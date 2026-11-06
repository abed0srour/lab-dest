# Build context is the repository root (the node needs common/ for its metrics).
FROM python:3.12-slim

WORKDIR /app
COPY storage_node/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY storage_node/__init__.py storage_node/app.py storage_node/

ENV DATA_DIR=/data
VOLUME /data
EXPOSE 8000

CMD ["uvicorn", "storage_node.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "65"]
