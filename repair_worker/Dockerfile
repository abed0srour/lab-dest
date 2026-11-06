# Build context is the repository root (the worker needs common/).
FROM python:3.12-slim

WORKDIR /app
COPY repair_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY repair_worker/ repair_worker/

CMD ["python", "-m", "repair_worker.worker"]
