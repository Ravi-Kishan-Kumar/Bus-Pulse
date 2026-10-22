FROM maven:3.9.9-eclipse-temurin-17 AS kafka-connector
WORKDIR /build
COPY docker/pom.xml pom.xml
RUN mvn --batch-mode --no-transfer-progress -q org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    -DincludeScope=runtime -DoutputDirectory=/kafka-jars

FROM python:3.11.9-slim-bookworm
ENV DEBIAN_FRONTEND=noninteractive \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless libgomp1 \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.8 pyarrow==17.0.0 pandas==2.2.3 numpy==2.2.6 \
    psycopg[binary,pool]==3.2.9 confluent-kafka==2.10.0 pydantic==2.11.4 orjson==3.10.18 PyYAML==6.0.2 \
    lightgbm==4.6.0 scikit-learn==1.6.1
COPY --from=kafka-connector /kafka-jars/ /tmp/kafka-jars/
RUN cp /tmp/kafka-jars/*.jar "$SPARK_HOME/jars/" && rm -rf /tmp/kafka-jars
WORKDIR /app
COPY backend/src/ backend/src/
COPY config/ config/
COPY data/blr/network_all.json.gz data/blr/network_all.json.gz
COPY db/ db/
COPY scripts/ scripts/
ENV PYTHONPATH=/app/backend/src \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
EXPOSE 4040
CMD ["python", "-m", "ridesense.stream.spark.main"]
