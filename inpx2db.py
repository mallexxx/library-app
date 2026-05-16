#!/usr/bin/env python3
"""Parse INPX file and build books.db SQLite database."""
import zipfile, sqlite3, sys, time, os

FIELDS = ["id", "author", "genre", "title", "series", "format", "date", "lang", "zip_file"]

def build(inpx_path: str, db_path: str):
    t0 = time.time()
    print(f"[inpx2db] Parsing {inpx_path}...")

    books = []
    with zipfile.ZipFile(inpx_path) as z:
        for name in z.namelist():
            if not name.endswith(".inp"):
                continue
            zip_name = name.split(".")[0]
            data = z.read(name).decode("utf-8", errors="replace")
            for line in data.strip().split("\n"):
                parts = line.strip().split(chr(4))
                if len(parts) < 12:
                    continue
                book_id = parts[5].strip()
                if not book_id:
                    continue
                books.append((
                    book_id,
                    parts[0].rstrip(":").strip(),   # author  "Фамилия,Имя,Отчество"
                    parts[1].rstrip(":").strip(),   # genre
                    parts[2].strip(),               # title
                    parts[3].strip(),               # series
                    parts[9].strip(),               # format
                    parts[10].strip(),              # date
                    parts[11].strip(),              # lang
                    zip_name,                       # zip archive prefix
                ))

    print(f"[inpx2db] Parsed {len(books)} books in {time.time()-t0:.1f}s")

    # Rebuild db
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE books (
            id TEXT PRIMARY KEY,
            author TEXT, genre TEXT, title TEXT,
            series TEXT, format TEXT, date TEXT,
            lang TEXT, zip_file TEXT
        )
    """)
    conn.executemany("INSERT OR IGNORE INTO books VALUES (?,?,?,?,?,?,?,?,?)", books)
    conn.commit()
    conn.execute("CREATE INDEX idx_author ON books(author)")
    conn.execute("CREATE INDEX idx_genre  ON books(genre)")
    conn.execute("CREATE INDEX idx_title  ON books(title)")
    conn.execute("CREATE INDEX idx_series ON books(series)")
    conn.commit()
    conn.close()

    total = time.time() - t0
    size_mb = os.path.getsize(db_path) / 1024 / 1024
    print(f"[inpx2db] Done in {total:.1f}s — {size_mb:.1f} MB")


if __name__ == "__main__":
    inpx = sys.argv[1] if len(sys.argv) > 1 else "/library/flibusta_fb2_local.inpx"
    db   = sys.argv[2] if len(sys.argv) > 2 else "/data/books.db"
    build(inpx, db)
