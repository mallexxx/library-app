# library-app

Self-hosted personal book library with mobile-first web UI.

## What it does

- Browse 687k+ books from Flibusta INPX archive
- Mark books as **Read / Reading / Want to read**
- Auto-builds relations between books (same author, series, genre, OpenLibrary co-reads)
- Interactive graph of your read books and their connections
- Separate adult section (hidden from main)
- Download via inpxer or FileBrowser links

## Stack

- **Backend**: Python + FastAPI
- **Frontend**: Vue 3 (CDN) + Tailwind CSS (CDN) + vis.js graph
- **Storage**: SQLite — `books.db` (rebuilt from INPX on start) + `library.db` (persistent)
- **Deploy**: Docker + Caddy reverse proxy

## Quick start

```bash
# 1. Clone
git clone https://github.com/mallexxx/library-app
cd library-app

# 2. Edit config
cp config.toml.example data/config.toml
# Set paths to your INPX file and books directory

# 3. Run
docker compose up -d
```

## Config

`data/config.toml` — edit without rebuilding the image:

```toml
[library]
inpx_file = "/library/flibusta_fb2_local.inpx"
local_books_path = "/books"

[download]
inpxer_base = "https://your-inpxer-domain/download"
filebrowser_base = "https://your-filebrowser-domain/books"

[adult]
genres = ["love_erotica", "erotik", ...]
```

## Data volumes

| Path | Description |
|------|-------------|
| `/library` (ro) | Directory with `.inpx` file |
| `/books` (ro) | Personal book files |
| `/data` (rw) | `config.toml`, `library.db` (persistent), `books.db` (rebuilt on start) |

## Startup time

~23 seconds to parse INPX and build SQLite index (687k books, 227MB).
`library.db` (your read status and relations) is never touched on restart.
