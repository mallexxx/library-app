#!/usr/bin/env python3
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
from typing import Optional
import sqlite3, tomllib, asyncio, httpx, os, re
from datetime import datetime, timedelta
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
CFG_PATH = os.environ.get("CONFIG", "/data/config.toml")
with open(CFG_PATH, "rb") as f:
    CFG = tomllib.load(f)

BOOKS_DB   = CFG["library"]["books_db"]
LIBRARY_DB = CFG["library"]["library_db"]
INPXER_BASE     = CFG["download"]["inpxer_base"]
FILEBROWSER_BASE = CFG["download"]["filebrowser_base"]
ADULT_GENRES     = set(CFG["adult"]["genres"])
GENRE_LIMIT      = CFG["relations"]["genre_limit"]
CACHE_DAYS       = CFG["relations"]["cache_days"]
GOOGLE_BOOKS_KEY = CFG.get("google_books", {}).get("api_key", "")

# ── DB helpers ───────────────────────────────────────────────────────────────
def bconn():
    c = sqlite3.connect(BOOKS_DB, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def lconn():
    c = sqlite3.connect(LIBRARY_DB, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

async def in_thread(fn):
    """Run a blocking function in a thread pool so it doesn't block the event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, fn)

def book_to_dict(row) -> dict:
    d = dict(row)
    # Prettify author: "Фамилия,Имя,Отчество" → "Имя Фамилия"
    raw = d.get("author", "")
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if len(parts) >= 2:
        d["author_display"] = " ".join(reversed(parts[:2]))
    else:
        d["author_display"] = raw
    d["download_url"] = f"{INPXER_BASE}/{d['id']}"
    return d

# ── Google Books enrichment ──────────────────────────────────────────────────
# Google categories → INPX genre tokens (best-effort mapping)
GBOOKS_GENRE_MAP = {
    "fiction / classics":       ["prose_classic", "prose_rus_classic"],
    "fiction / literary":       ["prose_classic", "prose_contemporary"],
    "fiction / science fiction": ["sf", "sf_social", "sf_space"],
    "fiction / fantasy":        ["sf_fantasy", "fantasy"],
    "fiction / mystery":        ["detective", "thriller"],
    "fiction / thrillers":      ["thriller", "detective"],
    "fiction / horror":         ["horror", "thriller"],
    "fiction / historical":     ["prose_history", "historical"],
    "fiction / romance":        ["love_contemporary", "love_history"],
    "fiction / adventure":      ["adventure", "prose_military"],
    "fiction":                  ["prose_contemporary"],
    "juvenile fiction":         ["child_prose", "child_sf"],
    "biography & autobiography": ["biography"],
    "history":                  ["history_russia", "sci_history"],
    "psychology":               ["sci_psychology", "self_help"],
    "philosophy":               ["sci_philosophy"],
    "science":                  ["sci_popular", "sci_phys"],
    "social science":           ["sci_social_studies", "prose_contemporary"],
    "russian fiction":          ["prose_contemporary", "prose_classic"],
}

def map_google_categories(categories: list[str]) -> list[str]:
    """Map Google Books categories to INPX genre codes."""
    genres = []
    for cat in categories:
        key = cat.lower()
        # exact match
        if key in GBOOKS_GENRE_MAP:
            genres.extend(GBOOKS_GENRE_MAP[key])
            continue
        # prefix match (e.g. "Fiction / Classics" → try "fiction / classics")
        for pattern, mapped in GBOOKS_GENRE_MAP.items():
            if key.startswith(pattern) or pattern.startswith(key.split("/")[0].strip()):
                genres.extend(mapped)
                break
    return list(dict.fromkeys(genres))  # deduplicate, preserve order

async def fetch_google_books(title: str, author: str) -> dict | None:
    """Fetch book metadata from Google Books API. Returns cover, description, categories."""
    if not GOOGLE_BOOKS_KEY:
        return None
    try:
        parts = [p.strip() for p in author.split(",") if p.strip()]
        author_last = parts[0] if parts else author

        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(
                "https://www.googleapis.com/books/v1/volumes",
                params={"q": f"{title} {author_last}", "maxResults": 5, "key": GOOGLE_BOOKS_KEY},
            )
            data = r.json()

        items = data.get("items", [])
        if not items:
            return None

        # Prefer item where author_last appears in volumeInfo authors
        best = None
        for item in items:
            vi = item["volumeInfo"]
            if author_last.lower() in " ".join(vi.get("authors", [])).lower():
                best = vi
                break
        if best is None:
            best = items[0]["volumeInfo"]

        # Cover: prefer higher-res, upgrade to https
        img = best.get("imageLinks", {})
        cover_url = img.get("thumbnail") or img.get("smallThumbnail") or ""
        cover_url = cover_url.replace("http://", "https://")

        return {
            "categories": best.get("categories", []),
            "description": best.get("description", ""),
            "cover_url": cover_url,
        }
    except Exception:
        return None

async def _enrich_google_books(book_id: str, title: str, author: str):
    """Fire-and-forget: fetch Google Books data, save cover/description, find related books."""
    try:
        meta = await fetch_google_books(title, author)
        if not meta:
            return

        def _save():
            now = datetime.utcnow().isoformat()
            with lconn() as lc:
                # Always save cover + description regardless of categories
                lc.execute(
                    "INSERT OR REPLACE INTO google_meta(book_id,cover_url,description,categories,fetched_at) VALUES(?,?,?,?,?)",
                    (book_id, meta.get("cover_url",""), meta.get("description",""),
                     ",".join(meta.get("categories",[])), now)
                )
                lc.commit()

            # Build genre-based relations if categories found
            mapped = map_google_categories(meta.get("categories", []))
            if not mapped:
                return
            rows = []
            with bconn() as bc:
                for genre in mapped[:3]:
                    peers = bc.execute(
                        "SELECT id FROM books WHERE genre=? AND id!=? ORDER BY RANDOM() LIMIT 10",
                        (genre, book_id)
                    ).fetchall()
                    for p in peers:
                        rows.append((book_id, p["id"], "google_books", 1.2, now))
            if rows:
                with lconn() as lc:
                    lc.executemany(
                        "INSERT OR REPLACE INTO relations(book_id,related_id,type,weight,fetched_at) VALUES(?,?,?,?,?)",
                        rows
                    )
                    lc.commit()

        await in_thread(_save)
    except Exception as e:
        print(f"[google_books] {book_id}: {e}", flush=True)


async def build_relations(book_id: str):
    """Build and cache all relation types for a book. Awaitable — runs DB in thread pool."""
    def _build():
        bc = bconn()
        lc = lconn()
        try:
            book = bc.execute("SELECT * FROM books WHERE id=?", (book_id,)).fetchone()
            if not book:
                return None

            now = datetime.utcnow().isoformat()
            rows_to_insert = []

            # same_author
            if book["author"]:
                peers = bc.execute(
                    "SELECT id FROM books WHERE author=? AND id!=? LIMIT 50",
                    (book["author"], book_id)
                ).fetchall()
                for p in peers:
                    rows_to_insert.append((book_id, p["id"], "same_author", 1.0, now))

            # same_series
            if book["series"] and book["series"].strip():
                peers = bc.execute(
                    "SELECT id FROM books WHERE series=? AND id!=? LIMIT 30",
                    (book["series"], book_id)
                ).fetchall()
                for p in peers:
                    rows_to_insert.append((book_id, p["id"], "same_series", 2.0, now))

            # same_genre — use RANDOM() sample to avoid slow full-genre scans
            if book["genre"]:
                peers = bc.execute(
                    "SELECT id FROM books WHERE genre=? AND id!=? ORDER BY RANDOM() LIMIT ?",
                    (book["genre"], book_id, GENRE_LIMIT)
                ).fetchall()
                for p in peers:
                    rows_to_insert.append((book_id, p["id"], "same_genre", 0.5, now))

            if rows_to_insert:
                lc.executemany(
                    "INSERT OR REPLACE INTO relations(book_id,related_id,type,weight,fetched_at) VALUES(?,?,?,?,?)",
                    rows_to_insert
                )
                lc.commit()

            return dict(book)
        finally:
            bc.close()
            lc.close()

    book = await in_thread(_build)


def needs_refresh(book_id: str) -> bool:
    with lconn() as lc:
        row = lc.execute(
            "SELECT MIN(fetched_at) as oldest FROM relations WHERE book_id=?",
            (book_id,)
        ).fetchone()
        if not row or not row["oldest"]:
            return True
        oldest = datetime.fromisoformat(row["oldest"])
        return datetime.utcnow() - oldest > timedelta(days=CACHE_DAYS)

def needs_google_enrichment(book_id: str) -> bool:
    """True if book has no google_books relations yet."""
    with lconn() as lc:
        row = lc.execute(
            "SELECT COUNT(*) as n FROM relations WHERE book_id=? AND type='google_books'",
            (book_id,)
        ).fetchone()
        return row["n"] == 0

# ── App ──────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

app = FastAPI(lifespan=lifespan)

# ── Search ───────────────────────────────────────────────────────────────────
@app.get("/api/search")
def search(
    q: str = "",
    genre: str = "",
    lang: str = "",
    series: str = "",
    adult: bool = False,
    page: int = 0,
    limit: int = 40,
):
    conditions = ["1=1"]
    params: list = []

    if not adult:
        placeholders = ",".join("?" * len(ADULT_GENRES))
        conditions.append(f"genre NOT IN ({placeholders})")
        params.extend(ADULT_GENRES)

    if q:
        conditions.append("(title LIKE ? OR author LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]

    if genre:
        conditions.append("genre = ?")
        params.append(genre)

    if lang:
        conditions.append("lang = ?")
        params.append(lang)

    if series:
        conditions.append("series LIKE ?")
        params.append(f"%{series}%")

    where = " AND ".join(conditions)
    offset = page * limit

    with bconn() as bc:
        total = bc.execute(f"SELECT COUNT(*) FROM books WHERE {where}", params).fetchone()[0]
        rows = bc.execute(
            f"SELECT * FROM books WHERE {where} ORDER BY date DESC LIMIT ? OFFSET ?",
            params + [limit, offset]
        ).fetchall()

    books = [book_to_dict(r) for r in rows]

    # Enrich with read status from library.db
    if books:
        ids = [b["id"] for b in books]
        with lconn() as lc:
            status_rows = lc.execute(
                "SELECT book_id, status FROM read_log WHERE book_id IN (%s)" % ",".join("?"*len(ids)),
                ids
            ).fetchall()
        status_map = {r["book_id"]: r["status"] for r in status_rows}
        for b in books:
            b["status"] = status_map.get(b["id"])

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "books": books,
    }

# ── Genre groups ─────────────────────────────────────────────────────────────
GENRE_GROUPS = {
    "Фантастика и фэнтези": ["sf", "sf_social", "sf_space", "sf_history", "sf_action",
        "sf_fantasy", "sf_heroic", "sf_detective", "sf_horror", "sf_humor", "sf_epic",
        "sf_cyberpunk", "sf_postapocalyptic", "sf_litrpg", "fantasy", "fantasy_alt_hist",
        "sf_stimpank", "sf_mystic", "sf_technofantasy", "sf_irony"],
    "Детективы и триллеры": ["detective", "thriller", "detective_police", "detective_hard",
        "detective_irony", "detective_maniac", "detective_classic", "detective_hist",
        "detective_action", "detective_espionage", "det_espionage", "det_action",
        "det_classic", "det_irony", "det_history", "det_hard", "det_police", "det_crime",
        "det_mental", "det_political", "thriller", "horror"],
    "Проза": ["prose_contemporary", "prose_classic", "prose_rus_classic", "prose_counter",
        "prose_history", "prose_military", "prose_su_classics", "prose_magic",
        "prose_poem", "prose_abs", "antique_russian", "antique_east"],
    "Любовные романы": ["love_contemporary", "love_history", "love_short", "love_erotica",
        "love_sf", "love_det", "love_hard"],
    "Приключения": ["adventure", "adventure_western", "adventure_history", "adventure_marine",
        "adventure_geo", "adventure_animal", "adventure_modern"],
    "Для детей": ["child_prose", "child_sf", "child_det", "child_tale", "child_education",
        "child_adv", "child_humor", "child_classical", "children", "fairy_tales"],
    "Юмор": ["humor", "humor_prose", "humor_verse", "humor_satire", "humor_anecdote"],
    "Наука и образование": ["sci_popular", "sci_phys", "sci_math", "sci_chem", "sci_biology",
        "sci_medicine", "sci_history", "sci_philosophy", "sci_psychology", "sci_social_studies",
        "sci_linguistic", "sci_geo", "sci_tech", "sci_it", "sci_ecology", "sci_transport",
        "sci_juris", "sci_economy", "sci_politics", "sci_culture", "sci_religion",
        "sci_cosmos", "sci_state", "sci_pedagogy", "sci_veterinary"],
    "История": ["history_russia", "sci_history", "antique", "antique_myths",
        "antique_antic", "antique_oriental", "antique_european", "biography"],
    "Психология и саморазвитие": ["self_help", "sci_psychology", "popular_business",
        "org_behavior", "management", "marketing", "economics"],
    "Техника и IT": ["sci_it", "comp_www", "comp_soft", "comp_hard", "comp_programming",
        "comp_db", "comp_osnet", "comp_game", "sci_tech"],
    "Поэзия": ["poetry", "antique_poetry", "lyrics"],
    "Драматургия": ["dramaturgy", "antique_plays"],
    "Религия и эзотерика": ["religion", "religion_budda", "religion_christianity",
        "religion_islam", "religion_judaism", "religion_paganism", "religion_self",
        "religion_esoterics", "religion_hinduism"],
    "Спорт и здоровье": ["sport", "home", "health", "sci_medicine"],
    "Эротика 18+": ["love_erotica", "sex_sf", "sex_story", "sex_humor", "adv_geo"],
}

# invert: genre_code → group_name
GENRE_TO_GROUP = {}
for grp, codes in GENRE_GROUPS.items():
    for code in codes:
        if code not in GENRE_TO_GROUP:
            GENRE_TO_GROUP[code] = grp

# ── Genres ───────────────────────────────────────────────────────────────────
@app.get("/api/genres")
def genres(adult: bool = False, grouped: bool = False):
    with bconn() as bc:
        rows = bc.execute(
            "SELECT genre, COUNT(*) as n FROM books GROUP BY genre ORDER BY n DESC"
        ).fetchall()

    if grouped:
        # Return groups with total counts
        group_counts: dict[str, int] = {}
        ungrouped = []
        for r in rows:
            is_adult = r["genre"] in ADULT_GENRES
            if not adult and is_adult:
                continue
            if adult and not is_adult:
                continue
            grp = GENRE_TO_GROUP.get(r["genre"])
            if grp:
                group_counts[grp] = group_counts.get(grp, 0) + r["n"]
            else:
                ungrouped.append({"genre": r["genre"], "count": r["n"]})
        result = [{"genre": k, "count": v, "is_group": True}
                  for k, v in sorted(group_counts.items(), key=lambda x: -x[1])]
        # Append ungrouped genres that have significant count
        result += [g for g in ungrouped if g["count"] > 100]
        return result

    # Flat list (includes group field for frontend optgroup building)
    result = []
    for r in rows:
        is_adult = r["genre"] in ADULT_GENRES
        if adult and is_adult:
            result.append({"genre": r["genre"], "count": r["n"], "group": GENRE_TO_GROUP.get(r["genre"])})
        elif not adult and not is_adult:
            result.append({"genre": r["genre"], "count": r["n"], "group": GENRE_TO_GROUP.get(r["genre"])})
    return result

# ── Book detail ──────────────────────────────────────────────────────────────
@app.get("/api/book/{book_id}")
async def book_detail(book_id: str):
    with bconn() as bc:
        book = bc.execute("SELECT * FROM books WHERE id=?", (book_id,)).fetchone()
    if not book:
        raise HTTPException(404, "Book not found")

    # Build/refresh relations — await so first open already returns related books
    if needs_refresh(book_id):
        await build_relations(book_id)

    # Google Books enrichment — runs independently, fire-and-forget
    if needs_google_enrichment(book_id):
        asyncio.create_task(_enrich_google_books(book_id, dict(book)["title"], dict(book)["author"]))

    # Get status + google meta
    with lconn() as lc:
        status_row = lc.execute(
            "SELECT status, rating, notes, date_updated FROM read_log WHERE book_id=?",
            (book_id,)
        ).fetchone()

        gmeta_row = lc.execute(
            "SELECT cover_url, description, categories FROM google_meta WHERE book_id=?",
            (book_id,)
        ).fetchone()

        # Get relations (show only books that exist in books.db)
        rel_rows = lc.execute(
            "SELECT related_id, type, weight FROM relations WHERE book_id=? ORDER BY weight DESC LIMIT 60",
            (book_id,)
        ).fetchall()

    # Enrich related books
    related = []
    if rel_rows:
        ids = [r["related_id"] for r in rel_rows]
        id_type = {r["related_id"]: r["type"] for r in rel_rows}
        placeholders = ",".join("?" * len(ids))
        with bconn() as bc:
            peers = bc.execute(
                f"SELECT * FROM books WHERE id IN ({placeholders})", ids
            ).fetchall()
        for p in peers:
            d = book_to_dict(p)
            d["relation_type"] = id_type.get(p["id"], "")
            related.append(d)

    result = book_to_dict(book)
    result["status"] = dict(status_row) if status_row else None
    result["related"] = related
    if gmeta_row:
        result["cover_url"] = gmeta_row["cover_url"] or ""
        result["description"] = gmeta_row["description"] or ""
        result["google_categories"] = gmeta_row["categories"] or ""
    return result

# ── Status update ─────────────────────────────────────────────────────────────
@app.post("/api/status")
async def set_status(body: dict):
    book_id = body.get("book_id")
    status  = body.get("status")   # read | reading | want | null (remove)
    rating  = body.get("rating")
    notes   = body.get("notes", "")

    if not book_id:
        raise HTTPException(400, "book_id required")

    with lconn() as lc:
        if status is None:
            lc.execute("DELETE FROM read_log WHERE book_id=?", (book_id,))
        else:
            if status not in ("read", "reading", "want"):
                raise HTTPException(400, "Invalid status")
            lc.execute("""
                INSERT INTO read_log(book_id, status, rating, notes, date_updated)
                VALUES(?,?,?,?,datetime('now'))
                ON CONFLICT(book_id) DO UPDATE SET
                    status=excluded.status,
                    rating=excluded.rating,
                    notes=excluded.notes,
                    date_updated=excluded.date_updated
            """, (book_id, status, rating, notes))
        lc.commit()

    # Ensure relations are built when marking read
    if status == "read" and needs_refresh(book_id):
        asyncio.create_task(build_relations(book_id))

    return {"ok": True}

# ── My books ─────────────────────────────────────────────────────────────────
@app.get("/api/my")
def my_books(status: Optional[str] = None):
    with lconn() as lc:
        if status:
            rows = lc.execute(
                "SELECT * FROM read_log WHERE status=? ORDER BY date_updated DESC",
                (status,)
            ).fetchall()
        else:
            rows = lc.execute(
                "SELECT * FROM read_log ORDER BY date_updated DESC"
            ).fetchall()

    if not rows:
        return []

    ids = [r["book_id"] for r in rows]
    status_map = {r["book_id"]: dict(r) for r in rows}
    placeholders = ",".join("?" * len(ids))

    with bconn() as bc:
        books = bc.execute(
            f"SELECT * FROM books WHERE id IN ({placeholders})", ids
        ).fetchall()

    result = []
    for b in books:
        d = book_to_dict(b)
        d["status_info"] = status_map.get(b["id"])
        result.append(d)

    result.sort(key=lambda x: x["status_info"]["date_updated"] or "", reverse=True)
    return result

# ── Graph ────────────────────────────────────────────────────────────────────
@app.get("/api/graph")
def graph():
    with lconn() as lc:
        read_rows = lc.execute(
            "SELECT book_id FROM read_log WHERE status='read'"
        ).fetchall()
        if not read_rows:
            return {"nodes": [], "edges": []}
        read_ids = [r["book_id"] for r in read_rows]

        rel_rows = lc.execute("""
            SELECT r.book_id, r.related_id, r.type, r.weight
            FROM relations r
            WHERE r.book_id IN (%s) AND r.related_id IN (%s)
        """ % (",".join("?"*len(read_ids)), ",".join("?"*len(read_ids))),
            read_ids + read_ids
        ).fetchall()

    placeholders = ",".join("?" * len(read_ids))
    with bconn() as bc:
        books = bc.execute(
            f"SELECT id, title, author, genre FROM books WHERE id IN ({placeholders})",
            read_ids
        ).fetchall()

    nodes = [{"id": b["id"], "label": b["title"][:40], "author": b["author"], "genre": b["genre"]} for b in books]
    edges = [{"from": r["book_id"], "to": r["related_id"], "type": r["type"], "weight": r["weight"]} for r in rel_rows]

    return {"nodes": nodes, "edges": edges}

# ── SPA fallback ──────────────────────────────────────────────────────────────
# Serve index.html for root and all non-API paths (SPA routing)
# Static files served inline via FileResponse

from fastapi.responses import HTMLResponse, FileResponse as FR
import mimetypes

STATIC_DIR = "/app/static"

@app.get("/", include_in_schema=False)
async def root():
    with open(f"{STATIC_DIR}/index.html") as f:
        return HTMLResponse(f.read())

@app.get("/{full_path:path}", include_in_schema=False)
async def spa_fallback(full_path: str):
    # Serve actual static files if they exist
    file_path = f"{STATIC_DIR}/{full_path}"
    if os.path.isfile(file_path):
        mt, _ = mimetypes.guess_type(file_path)
        return FR(file_path, media_type=mt or "application/octet-stream")
    # Otherwise SPA fallback
    with open(f"{STATIC_DIR}/index.html") as f:
        return HTMLResponse(f.read())
