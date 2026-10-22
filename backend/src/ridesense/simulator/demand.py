"""Synthetic stop demand; all demand parameters are assumptions."""

from __future__ import annotations

import math
import os
import random
from datetime import datetime


def demand_multiplier(local_time: datetime, demand_profile: str = "normal") -> float:
    hour = local_time.hour + local_time.minute / 60
    if 8 <= hour < 10 or 17 <= hour < 19:
        multiplier = 2.7 if (8 <= hour < 9 or 17 <= hour < 18) else 1.8
    else:
        multiplier = 0.65
    # Scenario profiles are synthetic multipliers; they are repeatable even when
    # the host clock is off-peak, which makes admin demonstrations predictable.
    if demand_profile == "moderate":
        return max(multiplier, 1.6)
    if demand_profile == "rush":
        return max(multiplier, 2.7) * 2.0
    return multiplier


def poisson(rng: random.Random, mean: float) -> int:
    if mean <= 0:
        return 0
    # Knuth's exact sampler is fine for the small stop-arrival means used here.
    limit = math.exp(-mean)
    product = 1.0
    count = 0
    while product > limit:
        product *= rng.random()
        count += 1
    return count - 1


def stop_flow(rng: random.Random, hub_score: int, headway_minutes: float, onboard: int,
              local_time: datetime, demand_profile: str = "normal") -> tuple[int, int]:
    # ASSUMPTION: hub score approximates transfers and peak hours amplify demand.
    hub_weight = 1.0 + 0.5 * math.log(max(1, hub_score))
    headway_weight = min(2.5, max(0.35, headway_minutes / 8.0))
    scale = max(0.0, float(os.getenv("DEMAND_SCALE", "1.0")))
    board_mean = 1.8 * hub_weight * headway_weight * demand_multiplier(local_time, demand_profile) * scale
    boarding = poisson(rng, board_mean)
    # ASSUMPTION: about 8% of riders alight at ordinary stops, more at larger hubs.
    alight_probability = min(0.45, 0.08 + 0.015 * math.log(max(1, hub_score)))
    alighting = sum(1 for _ in range(onboard) if rng.random() < alight_probability)
    return boarding, alighting
