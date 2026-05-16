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

# ── OpenLibrary enrichment ───────────────────────────────────────────────────
async def fetch_openlibrary(title: str, author: str) -> list[str]:
    """Returns list of related book titles from OpenLibrary."""
    try:
        q = f"{title} {author}".strip()
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(
                "https://openlibrary.org/search.json",
                params={"q": q, "limit": 1, "fields": "key,subject"}
            )
            data = r.json()
            docs = data.get("docs", [])
            if not docs:
                return []
            subjects = docs[0].get("subject", [])[:10]
            return subjects
    except Exception:
        return []

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
    if book:
        # OpenLibrary — fire-and-forget, truly background
        asyncio.create_task(_enrich_openlibrary(book_id, book["title"], book["author"]))

async def _enrich_openlibrary(book_id: str, title: str, author: str):
    subjects = await fetch_openlibrary(title, author)
    if not subjects:
        return
    # Find books in our DB matching those subjects as genres/titles
    with bconn() as bc, lconn() as lc:
        now = datetime.utcnow().isoformat()
        rows = []
        for subj in subjects[:5]:
            peers = bc.execute(
                "SELECT id FROM books WHERE title LIKE ? AND id!=? LIMIT 3",
                (f"%{subj[:20]}%", book_id)
            ).fetchall()
            for p in peers:
                rows.append((book_id, p["id"], "openlibrary", 1.5, now))
        if rows:
            lc.executemany(
                "INSERT OR REPLACE INTO relations(book_id,related_id,type,weight,fetched_at) VALUES(?,?,?,?,?)",
                rows
            )
            lc.commit()

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

# ── Genres ───────────────────────────────────────────────────────────────────
@app.get("/api/genres")
def genres(adult: bool = False):
    with bconn() as bc:
        rows = bc.execute(
            "SELECT genre, COUNT(*) as n FROM books GROUP BY genre ORDER BY n DESC"
        ).fetchall()
    result = []
    for r in rows:
        is_adult = r["genre"] in ADULT_GENRES
        if adult and is_adult:
            result.append({"genre": r["genre"], "count": r["n"]})
        elif not adult and not is_adult:
            result.append({"genre": r["genre"], "count": r["n"]})
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

    # Get status
    with lconn() as lc:
        status_row = lc.execute(
            "SELECT status, rating, notes, date_updated FROM read_log WHERE book_id=?",
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
