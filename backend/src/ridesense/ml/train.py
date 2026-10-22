"""Train and report a LightGBM +5-minute occupancy model from persisted stream windows."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import psycopg
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error

from ridesense.ml.features import FEATURE_COLUMNS

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://ridesense:ridesense-local-only@timescaledb:5432/ridesense")
PROJECT_ROOT = Path(__file__).resolve().parents[4]
MODEL_DIR = Path(os.getenv("MODEL_DIR", str(PROJECT_ROOT / "models")))
REPORT_PATH = Path(os.getenv("ML_REPORT", str(PROJECT_ROOT / "reports" / "ml.json")))


def main():
    query = """SELECT w.window_start,w.vehicle_id,w.route_id,w.occ_mean,w.occ_max,w.n_events,
                      COALESCE(p.occ_mean,w.occ_mean) AS prev_occ_mean,
                      COALESCE(r.occ_mean,w.occ_mean) AS route_mean_pct,
                      f.occ_mean AS target_pct
               FROM window_aggregates w
               JOIN window_aggregates f ON f.vehicle_id=w.vehicle_id
                    AND f.window_start=w.window_start + interval '5 minutes'
               LEFT JOIN window_aggregates p ON p.vehicle_id=w.vehicle_id
                    AND p.window_start=w.window_start - interval '1 minute'
               LEFT JOIN route_windows r ON r.route_id=w.route_id AND r.window_start=w.window_start
               ORDER BY w.window_start"""
    with psycopg.connect(DATABASE_URL) as conn:
        frame = pd.read_sql_query(query, conn)
    if len(frame) < 100:
        raise SystemExit(f"Need at least 100 labeled stream windows; found {len(frame)}. Keep the simulator running, then retry.")
    frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True)
    hours = frame["window_start"].dt.hour + frame["window_start"].dt.minute / 60
    angle = 2 * np.pi * hours / 24
    frame["hour_sin"] = np.sin(angle)
    frame["hour_cos"] = np.cos(angle)
    frame["prev_occ_mean"] = frame["prev_occ_mean"].fillna(frame["occ_mean"])
    frame["route_mean_pct"] = frame["route_mean_pct"].fillna(frame["occ_mean"])
    cutoff = frame["window_start"].quantile(0.8)
    train = frame[frame["window_start"] <= cutoff]
    valid = frame[frame["window_start"] > cutoff]
    if len(valid) < 20:
        raise SystemExit("Need at least 20 temporally held-out windows; keep the simulator running for more data.")
    x_train, y_train = train[FEATURE_COLUMNS], train["target_pct"]
    x_valid, y_valid = valid[FEATURE_COLUMNS], valid["target_pct"]
    model = lgb.LGBMRegressor(n_estimators=160, num_leaves=15, learning_rate=0.04,
                              max_depth=-1, random_state=42, verbosity=-1)
    model.fit(x_train, y_train)
    pred = np.clip(model.predict(x_valid), 0, 120)
    persistence = np.clip(valid["occ_mean"].to_numpy(), 0, 120)
    baseline = LinearRegression().fit(x_train[["occ_mean", "prev_occ_mean", "route_mean_pct"]], y_train)
    linear = np.clip(baseline.predict(valid[["occ_mean", "prev_occ_mean", "route_mean_pct"]]), 0, 120)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "model": "LightGBMRegressor",
        "horizon_minutes": 5, "rows": int(len(frame)), "train_rows": int(len(train)),
        "validation_rows": int(len(valid)), "split": "temporal 80/20", "features": FEATURE_COLUMNS,
        "mae_pct_points": {"lightgbm": float(mean_absolute_error(y_valid, pred)),
                           "persistence": float(mean_absolute_error(y_valid, persistence)),
                           "linear": float(mean_absolute_error(y_valid, linear))},
        "trained_through": train["window_start"].max().isoformat(),
        "validated_from": valid["window_start"].min().isoformat(),
    }
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    model.booster_.save_model(str(MODEL_DIR / "occupancy_lgbm.txt"))
    (MODEL_DIR / "occupancy_lgbm.meta.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
