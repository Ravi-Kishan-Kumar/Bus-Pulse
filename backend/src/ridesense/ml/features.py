"""Shared, time-stable features for training and stream-time prediction."""

from __future__ import annotations

import math
from datetime import datetime

FEATURE_COLUMNS = ["occ_mean", "occ_max", "n_events", "prev_occ_mean", "route_mean_pct", "hour_sin", "hour_cos"]


def make_features(occ_mean: float, occ_max: float, n_events: int, prev_occ_mean: float,
                  route_mean_pct: float, window_start: datetime) -> dict[str, float]:
    hour = window_start.hour + window_start.minute / 60
    angle = 2 * math.pi * hour / 24
    return {
        "occ_mean": float(occ_mean), "occ_max": float(occ_max), "n_events": float(n_events),
        "prev_occ_mean": float(prev_occ_mean), "route_mean_pct": float(route_mean_pct),
        "hour_sin": math.sin(angle), "hour_cos": math.cos(angle),
    }
