# Build context is the repository root (the gateway needs common/).
FROM python:3.12-slim

WORKDIR /app
COPY gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY gateway/ gateway/

EXPOSE 8000

CMD ["uvicorn", "gateway.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "65"]
