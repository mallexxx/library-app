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
LOCAL_BOOKS_DIR = CFG["library"].get("local_books_dir", "")
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
                for genre in mapped:
                    peers = bc.execute(
                        "SELECT id FROM books WHERE genre=? AND id!=? ORDER BY RANDOM() LIMIT 50",
                        (genre, book_id)
                    ).fetchall()
                    for p in peers:
                        rows.append((book_id, p["id"], "google_books", 1.2, now))
            # Sanity cap: 50 unique relations
            seen = set(); deduped = []
            for r in rows:
                if r[1] not in seen: seen.add(r[1]); deduped.append(r)
            rows = deduped[:50]
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

            # same_series — only author's own cycle (same author in series)
            # Skip publisher series (multiple different authors in same series)
            if book["series"] and book["series"].strip():
                # Count distinct authors in this series
                series_authors = bc.execute(
                    "SELECT COUNT(DISTINCT author) as n FROM books WHERE series=?",
                    (book["series"],)
                ).fetchone()["n"]
                # Only treat as a real cycle if ≤3 distinct authors (likely same author multi-vol)
                if series_authors <= 3:
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

# ── Local books (non-INPX files) ─────────────────────────────────────────────
def init_db():
    """Ensure library.db has all needed tables."""
    with lconn() as lc:
        lc.execute("""
            CREATE TABLE IF NOT EXISTS read_log (
                book_id TEXT PRIMARY KEY,
                status TEXT NOT NULL CHECK(status IN ('read','reading','want','starred')),
                date_updated TEXT,
                rating INTEGER,
                notes TEXT,
                created_at TEXT
            )
        """)
        lc.execute("""
            CREATE TABLE IF NOT EXISTS local_books (
                id TEXT PRIMARY KEY,
                title TEXT,
                author TEXT,
                file_path TEXT,
                format TEXT,
                inpx_id TEXT
            )
        """)
        lc.commit()

def _inpx_id_from_filename(filename: str) -> str | None:
    """Extract INPX book id from fb2 filenames like 'Author_Title.XXXXXX.591341.fb2'"""
    m = re.search(r'\.(\d{5,7})\.(fb2|epub)$', filename, re.IGNORECASE)
    return m.group(1) if m else None

def _title_from_filename(filename: str) -> str:
    """Best-effort title from filename, strip known patterns."""
    name = Path(filename).stem
    # Remove trailing INPX id pattern like .591341
    name = re.sub(r'\.\d{5,7}$', '', name)
    # Remove random suffix like .EVEVZg
    name = re.sub(r'\.[A-Za-z0-9]{6}$', '', name)
    # Replace underscores/dashes
    name = name.replace('_', ' ').replace('-', ' ')
    return name.strip()

def sync_local_books():
    """Scan LOCAL_BOOKS_DIR, register new files in local_books + read_log."""
    if not LOCAL_BOOKS_DIR or not Path(LOCAL_BOOKS_DIR).exists():
        print(f"[local_books] dir not found: {LOCAL_BOOKS_DIR}")
        return

    SUPPORTED = {'.fb2', '.epub', '.pdf', '.djvu', '.mobi', '.azw3'}
    files = [p for p in Path(LOCAL_BOOKS_DIR).rglob('*')
             if p.is_file() and p.suffix.lower() in SUPPORTED]

    print(f"[local_books] scanning {len(files)} files in {LOCAL_BOOKS_DIR}")

    import hashlib
    conn = sqlite3.connect(LIBRARY_DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    existing = {r[0] for r in conn.execute("SELECT file_path FROM local_books").fetchall()}

    for fpath in files:
        str_path = str(fpath)
        inpx_id = _inpx_id_from_filename(fpath.name)
        fmt = fpath.suffix.lstrip('.').lower()

        if inpx_id:
            with bconn() as bc:
                exists_in_inpx = bc.execute("SELECT id FROM books WHERE id=?", (inpx_id,)).fetchone()
            if exists_in_inpx:
                conn.execute(
                    "INSERT OR IGNORE INTO local_books(id, title, author, file_path, format, inpx_id)"
                    " VALUES (?,?,?,?,?,?)",
                    (f"local:{inpx_id}", '', '', str_path, fmt, inpx_id)
                )
                conn.execute(
                    "INSERT OR IGNORE INTO read_log(book_id, status, date_updated, created_at)"
                    " VALUES (?, 'starred', datetime('now'), datetime('now'))",
                    (inpx_id,)
                )
                conn.commit()
                if str_path not in existing:
                    print(f"[local_books] matched INPX {inpx_id}: {fpath.name}")
                continue

        if str_path in existing:
            continue

        # No INPX match — create local-only record
        fhash = hashlib.md5(str_path.encode()).hexdigest()[:8]
        book_id = f"local:{fhash}"
        title = _title_from_filename(fpath.name)
        conn.execute(
            "INSERT OR IGNORE INTO local_books(id, title, author, file_path, format, inpx_id)"
            " VALUES (?,?,?,?,?,?)",
            (book_id, title, '', str_path, fmt, None)
        )
        conn.execute(
            "INSERT OR IGNORE INTO read_log(book_id, status, date_updated, created_at)"
            " VALUES (?, 'starred', datetime('now'), datetime('now'))",
            (book_id,)
        )
        print(f"[local_books] local-only: {book_id} → {fpath.name}")

    conn.commit()
    conn.close()
    print("[local_books] sync done")

def local_book_to_dict(row) -> dict:
    """Convert a local_books row to the same shape as book_to_dict."""
    d = dict(row)
    d["author_display"] = d.get("author") or "Неизвестен"
    d["title"] = d.get("title") or Path(d.get("file_path", "")).stem
    d["genre"] = ""
    d["series"] = ""
    d["lang"] = ""
    d["date"] = ""
    d["size"] = 0
    # Download via filebrowser — relative path under local_books dir
    rel = str(Path(d["file_path"]).relative_to(LOCAL_BOOKS_DIR)) if LOCAL_BOOKS_DIR else d["file_path"]
    d["download_url"] = f"{FILEBROWSER_BASE}/{rel}"
    d["is_local"] = True
    return d

# ── App ──────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if LOCAL_BOOKS_DIR:
        sync_local_books()
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
        if genre.startswith("group:"):
            group_name = genre[6:]
            codes = list(GENRE_GROUPS.get(group_name, {}).keys())
            if codes:
                placeholders = ",".join("?" * len(codes))
                # Match any genre code that starts with one of the group's codes
                # since INPX uses compound codes like "sf:network_literature"
                group_conditions = []
                for code in codes:
                    group_conditions.append("genre = ? OR genre LIKE ?")
                    params += [code, f"{code}:%"]
                conditions.append("(" + " OR ".join(group_conditions) + ")")
            else:
                conditions.append("genre = ?")
                params.append(genre)
        else:
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
# Structure: { "Категория": { "subgenre_code": "Читаемое название", ... }, ... }
# Codes match flibusta INPX genre field (may be compound: "sf:network_literature")
GENRE_GROUPS: dict[str, dict[str, str]] = {
    "Фантастика и фэнтези": {
        "sf":                   "Научная фантастика",
        "sf_space":             "Космическая фантастика",
        "sf_social":            "Социальная фантастика",
        "sf_history":           "Альтернативная история",
        "sf_action":            "Боевая фантастика",
        "sf_fantasy":           "Фэнтези",
        "sf_heroic":            "Героическое фэнтези",
        "sf_fantasy_city":      "Городское фэнтези",
        "sf_epic":              "Эпическое фэнтези",
        "sf_horror":            "Ужасы",
        "sf_humor":             "Юмористическая фантастика",
        "sf_cyberpunk":         "Киберпанк",
        "sf_postapocalyptic":   "Постапокалипсис",
        "sf_litrpg":            "ЛитРПГ",
        "sf_stimpank":          "Стимпанк",
        "sf_mystic":            "Мистика",
        "sf_technofantasy":     "Технофэнтези",
        "sf_detective":         "Фантастический детектив",
        "sf_irony":             "Иронческая фантастика",
        "sf_etc":               "Фантастика: прочее",
        "fantasy":              "Фэнтези",
        "fantasy_alt_hist":     "Альтернативная история",
        "popadancy":            "Попаданцы",
        "boyar_anime":          "Боярь-аниме",
        "network_literature":   "Самиздат / сетевая литература",
    },
    "Детективы и триллеры": {
        "detective":            "Детективы",
        "det_classic":          "Классический детектив",
        "det_police":           "Полицейский детектив",
        "det_crime":            "Криминальный детектив",
        "det_history":          "Исторический детектив",
        "det_hard":             "Крутой детектив",
        "det_lady":             "Дамский детектив",
        "det_irony":            "Иронический детектив",
        "det_espionage":        "Шпионский детектив",
        "det_political":        "Политический детектив",
        "det_su":               "Советский детектив",
        "det_artifact":         "Артефакт-детектив",
        "det_mental":           "Психологический детектив",
        "det_action":           "Боевик",
        "love_detective":       "Любовный детектив",
        "thriller":             "Триллер",
        "horror":               "Ужасы",
        "det_maniac":           "Про маньяков",
    },
    "Проза": {
        "prose_contemporary":   "Современная проза",
        "prose_classic":        "Классическая проза",
        "prose_rus_classic":    "Русская классика",
        "prose_su_classics":    "Советская классика",
        "prose_history":        "Историческая проза",
        "prose_military":       "Проза о войне",
        "prose_counter":        "Контркультура",
        "prose_magic":          "Магический реализм",
        "prose_poem":           "Поэма в прозе",
        "prose_abs":            "Абсурдистская проза",
        "antique_russian":      "Древнерусская литература",
        "antique_east":         "Восточная классика",
        "prose":                "Проза",
        "story":                "Рассказы и новеллы",
        "literature_20":        "Проза XX века",
        "fanfiction":           "Фанфикшн",
        "foreign_prose":        "Зарубежная проза",
    },
    "Любовные романы": {
        "love_sf":              "Любовное фэнтези",
        "love_contemporary":    "Современные любовные романы",
        "love_short":           "Короткие любовные романы",
        "love_history":         "Исторические любовные романы",
        "love_hard":            "Остросюжетные любовные романы",
        "love_det":             "Любовный детектив",
        "love_erotica":         "Эротическая литература",
        "love":                 "Любовные романы",
    },
    "Приключения": {
        "adv_history":          "Исторические приключения",
        "adv_geo":              "Путешествия и география",
        "adv_animal":           "Природа и животные",
        "adv_maritime":         "Морские приключения",
        "adv_indian":           "Вестерн, про индейцев",
        "adv_western":          "Вестерн",
        "adv_modern":           "Современные приключения",
        "adv_extreme":          "Экстремальные приключения",
        "adventure":            "Приключения",
        "adventure_history":    "Исторические приключения",
        "adventure_marine":     "Морские приключения",
        "adventure_geo":        "Путешествия",
        "adventure_animal":     "Животные",
        "adventure_western":    "Вестерн",
        "adventure_modern":     "Современные приключения",
    },
    "Для детей": {
        "child_prose":                  "Детская проза",
        "child_adv":                    "Детские приключения",
        "child_adv_animal":             "Детские книги о животных",
        "child_prose_history":          "Историческая детская проза",
        "child_prose_humor":            "Юмористическая детская проза",
        "child_prose_romantic":         "Детская проза о любви",
        "child_sf":                     "Детская фантастика",
        "child_sf_fantasy":             "Детское фэнтези",
        "child_sf_horror":              "Детские ужасы и мистика",
        "child_det":                    "Детские детективы",
        "child_det_children_detectives":"Дети-сыщики",
        "child_tale":                   "Сказки",
        "child_tale_russian_writers":   "Сказки отечественных писателей",
        "child_tale_foreign_writers":   "Сказки зарубежных писателей",
        "folk_tale":                    "Народные сказки",
        "child_verse":                  "Стихи для детей",
        "child_education":              "Детская образовательная литература",
        "child_classical":              "Классическая детская литература",
        "child_humor":                  "Детский юмор",
        "foreign_children":             "Зарубежная детская литература",
        "children":                     "Детская литература",
        "fairy_tales":                  "Сказки",
    },
    "Юмор": {
        "humor":                "Юмор",
        "humor_prose":          "Юмористическая проза",
        "humor_verse":          "Юмористические стихи и басни",
        "humor_satire":         "Сатира",
        "humor_anecdote":       "Анекдоты",
    },
    "Документальная": {
        "nonf_biography":       "Биографии и мемуары",
        "nonf_publicism":       "Публицистика",
        "nonf_criticism":       "Критика",
        "biography":            "Биографии",
        "nonfiction":           "Документальная литература",
        "sci_culture":          "Культурология",
        "nonf_biography_writers": "Биографии писателей и поэтов",
        "military_history":     "Военная документалистика",
    },
    "Наука и образование": {
        "sci_history":          "История",
        "sci_psychology":       "Психология",
        "sci_philosophy":       "Философия",
        "sci_politics":         "Политика",
        "sci_popular":          "Научно-популярная литература",
        "sci_linguistic":       "Языкознание",
        "sci_philology":        "Литературоведение",
        "sci_medicine":         "Медицина",
        "sci_math":             "Математика",
        "sci_phys":             "Физика",
        "sci_juris":            "Юриспруденция",
        "sci_biology":          "Биология",
        "sci_social_studies":   "Обществознание",
        "sci_economy":          "Экономика",
        "sci_chem":             "Химия",
        "sci_geo":              "География",
        "sci_tech":             "Техника",
        "sci_it":               "Информатика",
        "sci_ecology":          "Экология",
        "sci_cosmos":           "Астрономия",
        "sci_pedagogy":         "Педагогика",
        "sci_state":            "Государство и право",
        "sci_veterinary":       "Ветеринария",
        "sci_transport":        "Транспорт",
        "sci_religion":         "Религиоведение",
        "sci_theories":         "Научные теории",
        "sci_medicine_alternative": "Нетрадиционная медицина",
        "sci_psychology_popular": "Популярная психология",
        "science":              "Наука",
    },
    "Деловая литература": {
        "popular_business":     "Деловая литература",
        "management":           "Менеджмент",
        "marketing":            "Маркетинг и PR",
        "economics":            "Экономика",
        "org_behavior":         "Кадры и карьера",
        "banking":              "Финансы",
        "self_help":            "Саморазвитие",
        "economics_ref":        "Экономические справочники",
    },
    "Компьютеры и IT": {
        "comp_programming":     "Программирование",
        "comp_www":             "Интернет",
        "comp_soft":            "Программы и ОС",
        "comp_hard":            "Компьютерное железо",
        "comp_db":              "Базы данных",
        "comp_osnet":           "Сети и ОС",
        "comp_game":            "Игры",
        "computers":            "Компьютеры",
    },
    "Поэзия": {
        "poetry":               "Поэзия",
        "antique_poetry":       "Античная поэзия",
        "lyrics":               "Лирика",
        "humor_verse":          "Юмористические стихи",
    },
    "Драматургия": {
        "dramaturgy":           "Драматургия",
        "antique_plays":        "Античная драма",
        "drama":                "Драма",
        "comedy":               "Комедия",
        "screenplays":          "Сценарии",
    },
    "Религия и эзотерика": {
        "religion":             "Религия",
        "religion_christianity":"Христианство",
        "religion_islam":       "Ислам",
        "religion_budda":       "Буддизм",
        "religion_judaism":     "Иудаизм",
        "religion_paganism":    "Язычество",
        "religion_hinduism":    "Индуизм",
        "religion_esoterics":   "Эзотерика",
        "religion_self":        "Самосовершенствование",
        "religion_orthodoxy":   "Православие",
    },
    "Дом и семья": {
        "home_health":          "Здоровье",
        "home_cooking":         "Кулинария",
        "home_crafts":          "Хобби и ремесла",
        "home_sport":           "Спорт и боевые искусства",
        "home_pets":            "Домашние животные",
        "home_sex":             "Семья и секс",
        "home_garden":          "Сад и огород",
        "home_entertain":       "Развлечения",
        "home_diy":             "Сделай сам",
        "home":                 "Домоводство",
        "sport":                "Спорт",
        "health":               "Здоровье",
    },
    "Искусство и культура": {
        "sci_culture":          "Культурология",
        "design":               "Искусство и дизайн",
        "aphorisms":            "Афоризмы",
    },
    "Справочники": {
        "ref_encyc":            "Энциклопедии",
        "ref_dict":             "Словари",
        "ref_guide":            "Путеводители",
        "ref_almanac":          "Альманахи",
        "ref_ref":              "Справочники",
        "geo_guides":           "Географические справочники",
        "tbg_school":           "Учебники",
    },
}

# invert: genre_code → group_name
GENRE_TO_GROUP: dict[str, str] = {}
for grp, subgenres in GENRE_GROUPS.items():
    for code in subgenres:
        if code not in GENRE_TO_GROUP:
            GENRE_TO_GROUP[code] = grp

# flat: genre_code → readable name
GENRE_NAMES: dict[str, str] = {}
for grp, subgenres in GENRE_GROUPS.items():
    for code, name in subgenres.items():
        if code not in GENRE_NAMES:
            GENRE_NAMES[code] = name

def genre_to_group(genre_code: str) -> str | None:
    """Handle compound genre codes like 'love_sf:network_literature:popadancy'.
    Split on ':', return the group of the first component that matches."""
    for part in genre_code.split(":"):
        grp = GENRE_TO_GROUP.get(part)
        if grp:
            return grp
    return None

def genre_display_name(genre_code: str) -> str:
    """Return human-readable name for a genre code (first part of compound code)."""
    for part in genre_code.split(":"):
        name = GENRE_NAMES.get(part)
        if name:
            return name
    return genre_code

# ── Genres ───────────────────────────────────────────────────────────────────
@app.get("/api/genres")
def genres(adult: bool = False, grouped: bool = False):
    with bconn() as bc:
        rows = bc.execute(
            "SELECT genre, COUNT(*) as n FROM books GROUP BY genre ORDER BY n DESC"
        ).fetchall()

    # Build per-group counts from DB (actual book counts)
    group_counts: dict[str, int] = {}
    subgenre_counts: dict[str, int] = {}  # raw_code → count
    for r in rows:
        is_adult = r["genre"] in ADULT_GENRES
        if not adult and is_adult:
            continue
        if adult and not is_adult:
            continue
        grp = genre_to_group(r["genre"])
        if grp:
            group_counts[grp] = group_counts.get(grp, 0) + r["n"]
        # accumulate counts for canonical codes (first part of compound)
        first_code = r["genre"].split(":")[0]
        subgenre_counts[first_code] = subgenre_counts.get(first_code, 0) + r["n"]

    if grouped:
        # Return: list of { genre, count, is_group, subgenres?: [{code, name, count}] }
        result = []
        for grp_name, subgenres in GENRE_GROUPS.items():
            grp_count = group_counts.get(grp_name, 0)
            if grp_count == 0:
                continue
            subs = []
            seen_names: set[str] = set()
            for code, name in subgenres.items():
                cnt = subgenre_counts.get(code, 0)
                if cnt == 0 or name in seen_names:
                    continue
                seen_names.add(name)
                subs.append({"code": code, "name": name, "count": cnt})
            subs.sort(key=lambda x: -x["count"])
            result.append({
                "genre": grp_name,
                "count": grp_count,
                "is_group": True,
                "subgenres": subs,
            })
        result.sort(key=lambda x: -x["count"])
        return result

    # Flat list (for backwards compat)
    result = []
    for r in rows:
        is_adult = r["genre"] in ADULT_GENRES
        if adult and is_adult:
            result.append({"genre": r["genre"], "count": r["n"], "group": genre_to_group(r["genre"])})
        elif not adult and not is_adult:
            result.append({"genre": r["genre"], "count": r["n"], "group": genre_to_group(r["genre"])})
    return result

# ── Book detail ──────────────────────────────────────────────────────────────
@app.get("/api/book/{book_id:path}")
async def book_detail(book_id: str):
    # Local-only book (not in INPX)
    if book_id.startswith("local:"):
        with lconn() as lc:
            lb = lc.execute("SELECT * FROM local_books WHERE id=?", (book_id,)).fetchone()
            if not lb:
                raise HTTPException(404, "Local book not found")
            status_row = lc.execute(
                "SELECT status, rating, notes, date_updated FROM read_log WHERE book_id=?",
                (book_id,)
            ).fetchone()
        d = local_book_to_dict(lb)
        d["status"] = dict(status_row) if status_row else None
        d["related"] = []
        return d

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
            "SELECT related_id, type, weight FROM relations WHERE book_id=? ORDER BY weight DESC LIMIT 150",
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
            if status not in ("read", "reading", "want", "starred"):
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

    all_ids = [r["book_id"] for r in rows]
    status_map = {r["book_id"]: dict(r) for r in rows}

    # Split: INPX ids vs local-only ids
    local_ids = [i for i in all_ids if i.startswith("local:")]
    inpx_ids  = [i for i in all_ids if not i.startswith("local:")]

    result = []

    # INPX books
    if inpx_ids:
        placeholders = ",".join("?" * len(inpx_ids))
        with bconn() as bc:
            books = bc.execute(
                f"SELECT * FROM books WHERE id IN ({placeholders})", inpx_ids
            ).fetchall()
        for b in books:
            d = book_to_dict(b)
            d["status_info"] = status_map.get(b["id"])
            result.append(d)

    # Local-only books
    if local_ids:
        with lconn() as lc2:
            placeholders = ",".join("?" * len(local_ids))
            local_rows = lc2.execute(
                f"SELECT * FROM local_books WHERE id IN ({placeholders})", local_ids
            ).fetchall()
        for b in local_rows:
            d = local_book_to_dict(b)
            d["status_info"] = status_map.get(b["id"])
            result.append(d)

    result.sort(key=lambda x: (x.get("status_info") or {}).get("date_updated") or "", reverse=True)
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
