"""Kafka to TimescaleDB streaming analytics for simulated occupancy."""

from __future__ import annotations

import hashlib
import json
import os
import time
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, max as spark_max, to_timestamp, window
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)
from ridesense.ml.features import FEATURE_COLUMNS, make_features

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://ridesense:ridesense-local-only@timescaledb:5432/ridesense")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/app/checkpoints")
OCCUPANCY_THRESHOLD = float(os.getenv("OVERCROWD_THRESHOLD_PCT", "85"))
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))
ALLOWED_LATENESS = int(os.getenv("ALLOWED_LATENESS_SECONDS", "15"))
_LAST_INGEST_MONOTONIC: float | None = None
_LGBM_MODEL = None
_LGBM_MODEL_MTIME: float | None = None
_OUTPUT_PRODUCER = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "bus-pulse-spark-output",
                             "enable.idempotence": True, "acks": "all"})

EVENT_SCHEMA = StructType([
    StructField("schema_version", IntegerType()), StructField("event_id", StringType()),
    StructField("event_type", StringType()), StructField("vehicle_id", StringType()),
    StructField("route_id", StringType()), StructField("region_id", StringType()),
    StructField("direction_id", IntegerType()),
    StructField("event_ts", StringType()), StructField("produced_ts_ms", LongType()),
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
    StructField("progress_m", DoubleType()), StructField("speed_mps", DoubleType()),
    StructField("capacity", IntegerType()),
    StructField("seat_capacity", IntegerType()), StructField("stand_capacity", IntegerType()),
    StructField("support_capacity", IntegerType()), StructField("vehicle_type", StringType()),
    StructField("capacity_range_min", IntegerType()), StructField("capacity_range_max", IntegerType()),
    StructField("delay_s", IntegerType()),
    StructField("onboard_count", IntegerType()), StructField("seated_count", IntegerType()),
    StructField("standing_count", IntegerType()), StructField("supported_standing_count", IntegerType()),
    StructField("unsupported_standing_count", IntegerType()), StructField("occ_est_pct", DoubleType()),
    StructField("apc_missing", BooleanType()), StructField("advisory_extra", BooleanType()),
])


def _dt(value):
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def write_raw(batch, batch_id: int):
    global _LAST_INGEST_MONOTONIC
    rows = [r.asDict(recursive=True) for r in batch.collect()]
    if not rows:
        return
    ingest_monotonic = time.monotonic()
    interval = max(0.001, ingest_monotonic - _LAST_INGEST_MONOTONIC) if _LAST_INGEST_MONOTONIC else 1.0
    _LAST_INGEST_MONOTONIC = ingest_monotonic
    now = datetime.now(timezone.utc)
    event_times = [_dt(r["event_ts"]) for r in rows]
    produced_ages = [max(0.0, r["produced_ts_ms"] / 1000 - _dt(r["event_ts"]).timestamp())
                     for r in rows if r.get("produced_ts_ms")]
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT max(event_ts) FROM occupancy_state")
            prior_max = cur.fetchone()[0]
            max_seen = max(event_times + ([_dt(prior_max)] if prior_max else []))
            lateness_floor = max_seen.timestamp() - ALLOWED_LATENESS
            late_dropped = sum(ts.timestamp() < lateness_floor for ts in event_times)
            ooo_applied = sum(ts.timestamp() < max_seen.timestamp() and ts.timestamp() >= lateness_floor for ts in event_times)
            duplicates = 0
            for r in rows:
                event_ts = _dt(r["event_ts"])
                cur.execute(
                    """INSERT INTO occupancy_state
                       (event_ts,event_id,vehicle_id,route_id,region_id,direction_id,lat,lon,progress_m,
                        onboard_count,seated_count,standing_count,supported_standing_count,capacity,
                        occ_est_pct,apc_missing,seat_capacity,stand_capacity,support_capacity,
                        vehicle_type,capacity_range_min,capacity_range_max,advisory_extra)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (event_ts,event_id) DO NOTHING""",
                    (event_ts, r["event_id"], r["vehicle_id"], r["route_id"], r.get("region_id") or "whole_city", r["direction_id"],
                     r.get("lat"), r.get("lon"), r.get("progress_m"), r["onboard_count"],
                     r["seated_count"], r["standing_count"], r["supported_standing_count"],
                     r["capacity"], r["occ_est_pct"], bool(r.get("apc_missing", False)),
                     r.get("seat_capacity", 40), r.get("stand_capacity", 30), r.get("support_capacity", 24),
                     r.get("vehicle_type", "Standard city bus (simulation)"),
                     r.get("capacity_range_min", 60), r.get("capacity_range_max", 75),
                     bool(r.get("advisory_extra", False))),
                )
                if cur.rowcount == 0:
                    duplicates += 1
                cur.execute(
                    """INSERT INTO vehicle_latest
                       (vehicle_id,route_id,region_id,direction_id,event_ts,lat,lon,progress_m,speed_mps,occ_est_pct,
                        seated_count,standing_count,supported_standing_count,updated_at,onboard_count,capacity,
                        seat_capacity,stand_capacity,support_capacity,vehicle_type,capacity_range_min,capacity_range_max,
                        advisory_extra)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (vehicle_id) DO UPDATE SET route_id=EXCLUDED.route_id,
                        region_id=EXCLUDED.region_id,direction_id=EXCLUDED.direction_id,event_ts=EXCLUDED.event_ts,lat=EXCLUDED.lat,
                        lon=EXCLUDED.lon,progress_m=EXCLUDED.progress_m,speed_mps=EXCLUDED.speed_mps,
                        occ_est_pct=EXCLUDED.occ_est_pct,
                        seated_count=EXCLUDED.seated_count,standing_count=EXCLUDED.standing_count,
                        supported_standing_count=EXCLUDED.supported_standing_count,updated_at=EXCLUDED.updated_at,
                        onboard_count=EXCLUDED.onboard_count,capacity=EXCLUDED.capacity,
                        seat_capacity=EXCLUDED.seat_capacity,stand_capacity=EXCLUDED.stand_capacity,
                        support_capacity=EXCLUDED.support_capacity,vehicle_type=EXCLUDED.vehicle_type,
                        capacity_range_min=EXCLUDED.capacity_range_min,capacity_range_max=EXCLUDED.capacity_range_max,
                        advisory_extra=EXCLUDED.advisory_extra
                       WHERE vehicle_latest.event_ts <= EXCLUDED.event_ts""",
                    (r["vehicle_id"], r["route_id"], r.get("region_id") or "whole_city", r["direction_id"], event_ts, r.get("lat", 0),
                     r.get("lon", 0), r.get("progress_m", 0), r.get("speed_mps", 0), r["occ_est_pct"], r["seated_count"],
                     r["standing_count"], r["supported_standing_count"], now, r["onboard_count"],
                     r["capacity"], r.get("seat_capacity", 40), r.get("stand_capacity", 30),
                     r.get("support_capacity", 24), r.get("vehicle_type", "Standard city bus (simulation)"),
                     r.get("capacity_range_min", 60), r.get("capacity_range_max", 75),
                     bool(r.get("advisory_extra", False))),
                )
            cur.execute(
                """INSERT INTO dq_metrics
                   (ts,query_name,batch_id,events_in,malformed,duplicates,late_dropped,apc_missing,
                    ooo_applied,lateness_p95_s,rows_dropped_by_watermark,throughput_eps)
                   VALUES (%s,'occupancy_ingest',%s,%s,0,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                (now, batch_id, len(rows), duplicates, late_dropped,
                 sum(bool(r.get("apc_missing", False)) for r in rows), ooo_applied,
                 statistics.quantiles(produced_ages, n=100)[94] if len(produced_ages) > 1 else (produced_ages[0] if produced_ages else 0),
                 late_dropped, len(rows) / interval),
            )


def write_windows(batch, batch_id: int):
    rows = [r.asDict(recursive=True) for r in batch.collect()]
    if not rows:
        return
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for r in rows:
                start = _dt(r["window_start"])
                end = _dt(r["window_end"])
                cur.execute(
                    """INSERT INTO window_aggregates(window_start,vehicle_id,route_id,occ_mean,occ_max,n_events)
                       VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (vehicle_id,window_start) DO UPDATE SET
                       occ_mean=EXCLUDED.occ_mean,occ_max=EXCLUDED.occ_max,n_events=EXCLUDED.n_events""",
                    (start, r["vehicle_id"], r["route_id"], r["occ_mean"], r["occ_max"], r["n_events"]),
                )
                cur.execute(
                    """INSERT INTO route_windows(window_start,route_id,occ_mean,occ_max,n_buses)
                       SELECT %s,%s,avg(occ_est_pct),max(occ_est_pct),count(DISTINCT vehicle_id)
                       FROM occupancy_state WHERE route_id=%s AND event_ts >= %s AND event_ts < %s
                       ON CONFLICT (route_id,window_start) DO UPDATE SET occ_mean=EXCLUDED.occ_mean,
                       occ_max=EXCLUDED.occ_max,n_buses=EXCLUDED.n_buses""",
                    (start, r["route_id"], r["route_id"], start, end),
                )

                cur.execute(
                    "SELECT window_start,occ_mean,occ_max FROM window_aggregates WHERE vehicle_id=%s AND window_start<%s ORDER BY window_start DESC LIMIT 1",
                    (r["vehicle_id"], start),
                )
                previous = cur.fetchone()
                consecutive = bool(previous and start - _dt(previous[0]) == timedelta(seconds=WINDOW_SECONDS))
                delta = (r["occ_mean"] - float(previous[1])) if consecutive else 0.0
                cur.execute("SELECT occ_mean FROM route_windows WHERE route_id=%s AND window_start=%s",
                            (r["route_id"], start))
                route_row = cur.fetchone()
                route_mean = float(route_row[0]) if route_row else float(r["occ_mean"])
                features = make_features(float(r["occ_mean"]), float(r["occ_max"]), int(r["n_events"]),
                                         float(previous[1]) if previous else float(r["occ_mean"]), route_mean, start)
                model, model_version = _load_model()
                contributions = {"current_window_pct": float(r["occ_mean"]),
                                 "per_window_change_pct": delta, "horizon_minutes": 5}
                if model is not None:
                    values = [[features[name] for name in FEATURE_COLUMNS]]
                    predicted = float(model.predict(values)[0])
                    shap = model.predict(values, pred_contrib=True)[0]
                    contributions = {name: float(shap[i]) for i, name in enumerate(FEATURE_COLUMNS)}
                    contributions["bias"] = float(shap[-1])
                else:
                    predicted = float(r["occ_mean"]) + delta * 5
                    model_version = "streaming-trend-v1"
                predicted = max(0.0, min(120.0, predicted))
                made_at = end
                target_at = made_at + timedelta(minutes=5)
                contributions["target_ts"] = target_at.isoformat()
                cur.execute("SELECT pred_pct FROM predictions WHERE vehicle_id=%s AND made_at<%s AND target='plus_5m' ORDER BY made_at DESC LIMIT 1",
                            (r["vehicle_id"], made_at))
                previous_prediction = cur.fetchone()
                cur.execute(
                    """INSERT INTO predictions(made_at,vehicle_id,route_id,target,pred_pct,model_version,contributions)
                       VALUES (%s,%s,%s,'plus_5m',%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (made_at, r["vehicle_id"], r["route_id"], predicted, model_version, Jsonb(contributions)),
                )
                if float(r["occ_max"]) < OCCUPANCY_THRESHOLD:
                    cur.execute(
                        """UPDATE alerts SET resolved_at=COALESCE(resolved_at,%s)
                           WHERE vehicle_id=%s AND alert_type='OVERCROWD' AND resolved_at IS NULL
                             AND detected_at<=%s""",
                        (made_at, r["vehicle_id"], made_at),
                    )
                if predicted < OCCUPANCY_THRESHOLD:
                    cur.execute(
                        """UPDATE alerts SET resolved_at=COALESCE(resolved_at,%s)
                           WHERE vehicle_id=%s AND alert_type='PREDICTED_OVERCROWD' AND resolved_at IS NULL
                             AND detected_at<=%s""",
                        (made_at, r["vehicle_id"], made_at),
                    )
                prediction_event = {
                    "schema_version": 1,
                    "event_id": hashlib.sha256(f"{r['vehicle_id']}|{made_at.isoformat()}|plus_5m".encode()).hexdigest(),
                    "vehicle_id": r["vehicle_id"], "route_id": r["route_id"],
                    "made_at": made_at.isoformat(), "target_ts": target_at.isoformat(),
                    "horizon_minutes": 5, "pred_pct": predicted, "model_version": model_version,
                    "contributions": contributions,
                }
                _OUTPUT_PRODUCER.produce("predictions", key=r["vehicle_id"],
                                         value=json.dumps(prediction_event).encode())
                alert_types = []
                if consecutive and float(previous[2]) > OCCUPANCY_THRESHOLD and float(r["occ_max"]) > OCCUPANCY_THRESHOLD:
                    alert_types.append(("OVERCROWD", "critical", float(r["occ_max"])))
                if (predicted >= OCCUPANCY_THRESHOLD and float(r["occ_mean"]) <= OCCUPANCY_THRESHOLD
                        and consecutive and previous_prediction
                        and float(previous_prediction[0]) >= OCCUPANCY_THRESHOLD):
                    alert_types.append(("PREDICTED_OVERCROWD", "warning", predicted))
                for kind, severity, value in alert_types:
                    event_id = hashlib.sha256(f"{kind}|{r['vehicle_id']}|{start.isoformat()}".encode()).hexdigest()
                    cur.execute(
                        """INSERT INTO alerts(event_id,alert_type,severity,vehicle_id,route_id,detected_at,
                           window_starts,occ_pct,pred_pct,threshold) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT DO NOTHING RETURNING event_id""",
                        (event_id, kind, severity, r["vehicle_id"], r["route_id"], made_at,
                         Jsonb([start.isoformat()]), value if kind != "PREDICTED_OVERCROWD" else None,
                         predicted if kind == "PREDICTED_OVERCROWD" else None, OCCUPANCY_THRESHOLD),
                    )
                    if cur.fetchone() is None:
                        continue
                    alert_event = {
                        "schema_version": 1, "event_id": event_id, "alert_type": kind,
                        "severity": severity, "vehicle_id": r["vehicle_id"], "route_id": r["route_id"],
                        "detected_at": made_at.isoformat(), "window_starts": [start.isoformat()],
                        "occ_pct": value if kind != "PREDICTED_OVERCROWD" else None,
                        "pred_pct": predicted if kind == "PREDICTED_OVERCROWD" else None,
                        "threshold": OCCUPANCY_THRESHOLD,
                    }
                    _OUTPUT_PRODUCER.produce("alerts", key=r["route_id"],
                                             value=json.dumps(alert_event).encode())
    _OUTPUT_PRODUCER.flush(5)


def _load_model():
    global _LGBM_MODEL, _LGBM_MODEL_MTIME
    path = os.getenv("MODEL_PATH", str(Path(__file__).resolve().parents[5] / "models" / "occupancy_lgbm.txt"))
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return None, "streaming-trend-v1"
    if _LGBM_MODEL is None or _LGBM_MODEL_MTIME != mtime:
        import lightgbm as lgb
        _LGBM_MODEL = lgb.Booster(model_file=path)
        _LGBM_MODEL_MTIME = mtime
    return _LGBM_MODEL, f"lightgbm-{_LGBM_MODEL.current_iteration()}"


def main():
    spark = (SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[4]"))
             .appName("bus-pulse-occupancy-stream")
             .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "3"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.streaming.stateStore.providerClass",
                     "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", "occupancy-state").option("startingOffsets", "latest")
           .option("failOnDataLoss", "false").load())
    parsed = raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
    events = parsed.select("e.*").withColumn("event_ts", to_timestamp("event_ts"))
    valid = events.where(col("event_id").isNotNull() & col("vehicle_id").isNotNull()
                         & col("route_id").isNotNull() & col("event_ts").isNotNull())
    raw_query = (valid.writeStream.foreachBatch(write_raw).option("checkpointLocation", f"{CHECKPOINT}/raw")
                 .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "1 second")).start())
    windows = (valid.withWatermark("event_ts", f"{ALLOWED_LATENESS} seconds")
               .groupBy(window(col("event_ts"), f"{WINDOW_SECONDS} seconds"), "vehicle_id", "route_id")
               .agg(avg("occ_est_pct").alias("occ_mean"), spark_max("occ_est_pct").alias("occ_max"),
                    count("*").alias("n_events"))
               .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"),
                       "vehicle_id", "route_id", "occ_mean", "occ_max", "n_events"))
    agg_query = (windows.writeStream.outputMode("append").foreachBatch(write_windows)
                 .option("checkpointLocation", f"{CHECKPOINT}/windows")
                 .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "1 second")).start())
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
