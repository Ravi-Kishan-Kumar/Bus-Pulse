FROM python:3.11.9-slim-bookworm
WORKDIR /app
RUN pip install --no-cache-dir pydantic==2.11.4 PyYAML==6.0.2 confluent-kafka==2.10.0 psycopg[binary]==3.2.9
COPY backend/src/ backend/src/
COPY config/ config/
COPY data/blr/network_all.json.gz data/blr/network_all.json.gz
ENV PYTHONPATH=/app/backend/src PYTHONUNBUFFERED=1
CMD ["python", "-m", "ridesense.simulator.producer"]
