#!/usr/bin/env python3
"""Initialize persistent library.db (read_log + relations). Never drops existing data."""
import sqlite3, sys, os

def init(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS read_log (
            book_id     TEXT PRIMARY KEY,
            status      TEXT NOT NULL CHECK(status IN ('read','reading','want')),
            date_updated TEXT,
            rating      INTEGER,
            notes       TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS relations (
            book_id     TEXT NOT NULL,
            related_id  TEXT NOT NULL,
            type        TEXT NOT NULL,
            weight      REAL DEFAULT 1.0,
            fetched_at  TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (book_id, related_id, type)
        );

        CREATE INDEX IF NOT EXISTS idx_rel_book ON relations(book_id);
        CREATE INDEX IF NOT EXISTS idx_rel_type ON relations(book_id, type);
    """)
    conn.commit()
    conn.close()
    print(f"[init_db] {db_path} ready")

if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "/data/library.db"
    init(db)
