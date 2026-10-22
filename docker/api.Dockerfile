FROM python:3.11.9-slim-bookworm
WORKDIR /app
RUN pip install --no-cache-dir \
    fastapi==0.115.12 uvicorn[standard]==0.34.2 \
    pydantic==2.11.4 psycopg[binary,pool]==3.2.9 confluent-kafka==2.10.0 \
    PyYAML==6.0.2 orjson==3.10.18
COPY backend/src/ backend/src/
COPY frontend/ frontend/
ENV PYTHONPATH=/app/backend/src WEB_DIR=/app/frontend PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "ridesense.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
