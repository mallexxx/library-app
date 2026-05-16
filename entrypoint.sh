#!/bin/sh
set -e

CONFIG=${CONFIG:-/data/config.toml}

echo "[entrypoint] Building books.db from INPX..."
python /app/inpx2db.py

echo "[entrypoint] Initializing library.db..."
python /app/init_db.py

echo "[entrypoint] Starting server..."
exec uvicorn main:app --host 0.0.0.0 --port 8080 --app-dir /app
