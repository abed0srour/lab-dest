# Build context is the repository root (the service needs common/).
FROM python:3.12-slim

WORKDIR /app
COPY metadata/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY metadata/ metadata/

EXPOSE 8000

CMD ["uvicorn", "metadata.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
