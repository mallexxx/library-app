FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn[standard] httpx

COPY inpx2db.py init_db.py main.py ./
COPY static/ ./static/

EXPOSE 8080
ENTRYPOINT ["/app/entrypoint.sh"]
