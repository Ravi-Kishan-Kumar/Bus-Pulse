"""Phase-0 Spark/Kafka compatibility spike.

Run inside the processor image attached to the Compose network after Kafka is up.
This executable portion checks the connector, stateful pandas API, Kafka batch
sink, and event-time append windows. Crash-resume and deliberately late-row
checks are recorded as manual steps in docs/DECISIONS.md.
"""

from __future__ import annotations

import os
import tempfile
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_json, struct, window
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.streaming.state import GroupStateTimeout


def count_state(key, batches, state):
    count = state.get[0] if state.exists else 0
    for pdf in batches:
        count += len(pdf)
    state.update((count,))
    yield pd.DataFrame({"vehicle_id": [str(key[0])], "event_count": [count]})


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("bus-pulse-phase0-spike")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    jvm = spark.sparkContext._jvm
    jvm.java.lang.Class.forName("org.apache.spark.sql.kafka010.KafkaSourceProvider")

    schema = StructType([StructField("vehicle_id", StringType()), StructField("event_count", LongType())])
    state_schema = StructType([StructField("event_count", LongType())])
    source = (
        spark.readStream.format("rate").option("rowsPerSecond", 3).load()
        .select(col("timestamp").alias("event_time"), col("value"),
                concat(lit("V"), (col("value") % 3).cast("string")).alias("vehicle_id"))
    )

    stateful = (
        source.withWatermark("event_time", "3 seconds")
        .groupBy("vehicle_id")
        .applyInPandasWithState(
            count_state, outputStructType=schema, stateStructType=state_schema,
            outputMode="Update", timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )
    key_value = stateful.select(
        col("vehicle_id").cast("string").alias("key"),
        to_json(struct("vehicle_id", "event_count")).cast("string").alias("value"),
    )
    temp = tempfile.mkdtemp(prefix="bus-pulse-spark-spike-")
    q1 = (
        key_value.writeStream.outputMode("update").option("checkpointLocation", f"{temp}/q1")
        .foreachBatch(lambda batch, _: batch.write.format("kafka").option("kafka.bootstrap.servers", bootstrap)
                      .option("topic", "spark-spike-state").save())
        .start()
    )

    # A heartbeat-only source advances event-time and closes append windows.
    heartbeats = source.select(
        col("event_time"), col("value").cast("string").alias("event_id"),
        col("vehicle_id"), lit("heartbeat").alias("event_type"),
    )
    windowed = (
        heartbeats.withWatermark("event_time", "2 seconds")
        .dropDuplicates(["event_id", "event_time"])
        .groupBy(window("event_time", "2 seconds"), "vehicle_id")
        .count()
    )
    query_name = "bus_pulse_spike_windows"
    q2 = (
        windowed.writeStream.format("memory").queryName(query_name).outputMode("append")
        .option("checkpointLocation", f"{temp}/q2").start()
    )
    time.sleep(10)
    q1.stop()
    q2.stop()
    window_count = spark.table(query_name).count()
    if window_count == 0:
        raise RuntimeError("heartbeat-only input did not advance the watermark and close an append window")
    if not q1.isActive and not q2.isActive:
        print(f"PASS: Spark {spark.version}; Kafka connector loaded; applyInPandasWithState -> Kafka batch sink; "
              f"heartbeat event-time windows closed ({window_count} windows)")
    spark.stop()


if __name__ == "__main__":
    main()
