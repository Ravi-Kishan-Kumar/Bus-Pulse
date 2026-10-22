"""One synthetic vehicle moving over a screened GTFS route direction."""

from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ridesense.common.schemas import PassengerFlow, VehicleTelemetry
from ridesense.simulator.demand import stop_flow
from ridesense.stream.core.occupancy import OccupancyLedger

_SHAPE_LENGTH_CACHE: dict[int, list[float]] = {}


def stable_event_id(vehicle_id: str, event_type: str, timestamp: datetime, ordinal: int = 0) -> str:
    raw = f"{vehicle_id}|{event_type}|{timestamp.isoformat()}|{ordinal}".encode()
    return hashlib.sha256(raw).hexdigest()


@dataclass
class BusTick:
    telemetry: VehicleTelemetry
    passenger_flow: PassengerFlow | None
    occupancy: dict[str, Any]


@dataclass
class BusSimulator:
    vehicle_id: str
    route_id: str
    route_short_name: str
    directions: list[dict[str, Any]]
    progress_m: float
    rng: random.Random
    region_id: str = "whole_city"
    capacity: int = 70
    seat_capacity: int = 40
    stand_capacity: int = 30
    support_capacity: int = 30
    vehicle_type: str = "Standard city bus (simulation)"
    capacity_range_min: int = 60
    capacity_range_max: int = 75
    advisory_extra: bool = False
    speed_mps: float = 7.0
    demand_profile: str = "normal"
    ledger: OccupancyLedger = field(init=False)
    direction_index: int = 0
    next_stop_index: int = 0
    dwell_remaining_s: float = 0.0
    _switch_after_dwell: bool = False
    _shape_cumulative: list[list[float]] = field(init=False)
    _last_departure: dict[str, datetime] = field(default_factory=dict)
    stop_hub_scores: dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if len(self.directions) == 1:
            original = self.directions[0]
            length = float(original["length_m"])
            self.directions.append({
                **original,
                "direction_id": int(original["direction_id"]) + 1,
                "shape": list(reversed(original["shape"])),
                "stops": [
                    {**stop, "seq": index, "progress_m": max(0.0, length-float(stop["progress_m"]))}
                    for index, stop in enumerate(reversed(original["stops"]))
                ],
            })
        if len(self.directions) < 2:
            raise ValueError(f"route {self.route_id} needs at least one usable direction")
        self.directions = sorted(self.directions, key=lambda d: int(d["direction_id"]))[:2]
        self.ledger = OccupancyLedger(self.capacity, self.seat_capacity, self.stand_capacity, self.support_capacity)
        self.direction_index = 0
        self._shape_cumulative = []
        for direction in self.directions:
            shape = direction["shape"]
            key = id(shape)
            cumulative = _SHAPE_LENGTH_CACHE.get(key)
            if cumulative is None:
                cumulative = self._shape_lengths(shape)
                _SHAPE_LENGTH_CACHE[key] = cumulative
            self._shape_cumulative.append(cumulative)
        self._seek_next_stop()

    @property
    def direction(self):
        return self.directions[self.direction_index]

    @property
    def direction_id(self):
        return int(self.direction["direction_id"])

    @property
    def route_length_m(self):
        return float(self.direction["length_m"])

    def _shape_lengths(self, shape):
        cumulative = [0.0]
        for a, b in zip(shape, shape[1:]):
            cumulative.append(cumulative[-1] + self._distance(a, b))
        return cumulative

    @staticmethod
    def _distance(a, b):
        lat1, lat2 = math.radians(a[1]), math.radians(b[1])
        dlat, dlon = lat2 - lat1, math.radians(b[0] - a[0])
        h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        return 12_742_000 * math.asin(math.sqrt(min(1.0, h)))

    def _position(self):
        shape = self.direction["shape"]
        cumulative = self._shape_cumulative[self.direction_index]
        if len(shape) < 2 or cumulative[-1] <= 0:
            raise ValueError(f"route {self.route_id} has no usable geometry")
        target = max(0.0, min(cumulative[-1], self.progress_m / max(1.0, self.route_length_m) * cumulative[-1]))
        low, high = 1, len(cumulative)-1
        while low < high:
            middle = (low + high) // 2
            if cumulative[middle] < target:
                low = middle + 1
            else:
                high = middle
        index = low
        segment = cumulative[index] - cumulative[index - 1]
        ratio = 0.0 if segment == 0 else (target - cumulative[index - 1]) / segment
        a, b = shape[index - 1], shape[index]
        return a[0] + ratio * (b[0] - a[0]), a[1] + ratio * (b[1] - a[1])

    def _seek_next_stop(self):
        stops = self.direction["stops"]
        self.next_stop_index = next(
            (i for i, stop in enumerate(stops) if stop["progress_m"] >= self.progress_m - 0.1),
            len(stops) - 1,
        )

    def _turn_at_terminal(self):
        self.direction_index = 1 - self.direction_index
        self.progress_m = 0.0
        self._seek_next_stop()

    def tick(self, event_time: datetime, dt_seconds: float = 3.0, produced_ts_ms: int | None = None,
             paused: bool = False) -> BusTick:
        produced_ts_ms = produced_ts_ms or int(datetime.now().timestamp() * 1000)
        flow = None
        if paused:
            pass
        elif self.dwell_remaining_s > 0:
            self.dwell_remaining_s = max(0.0, self.dwell_remaining_s - dt_seconds)
            if self.dwell_remaining_s == 0 and self._switch_after_dwell:
                self._turn_at_terminal()
                self._switch_after_dwell = False
        else:
            next_stop = self.direction["stops"][self.next_stop_index]
            target_progress = float(next_stop["progress_m"])
            moved_to = self.progress_m + self.speed_mps * dt_seconds
            if moved_to >= target_progress:
                self.progress_m = target_progress
                is_terminal = self.next_stop_index == len(self.direction["stops"]) - 1
                previous_departure = self._last_departure.get(next_stop["stop_id"])
                headway_minutes = 8.0 if previous_departure is None else max(
                    0.5, (event_time - previous_departure).total_seconds() / 60
                )
                boarding, alighting = stop_flow(
                    self.rng, int(self.stop_hub_scores.get(next_stop["stop_id"], 1)), headway_minutes,
                    self.ledger.onboard_count, event_time, self.demand_profile,
                )
                result = self.ledger.apply_flow(boarding, alighting, terminal=is_terminal)
                door_open_ms = 12_000 + int(1_500 * (result["boarded"] + result["alighted"]))
                self.dwell_remaining_s = door_open_ms / 1000
                self._last_departure[next_stop["stop_id"]] = event_time
                flow = PassengerFlow(
                    event_id=stable_event_id(self.vehicle_id, "flow", event_time),
                    vehicle_id=self.vehicle_id, route_id=self.route_id, direction_id=self.direction_id,
                    region_id=self.region_id,
                    event_ts=event_time, produced_ts_ms=produced_ts_ms, stop_id=next_stop["stop_id"],
                    boarding_count=result["boarded"], alighting_count=result["alighted"],
                    door_open_ms=door_open_ms, capacity=self.capacity, seat_capacity=self.seat_capacity,
                    stand_capacity=self.stand_capacity, standing_support_capacity=self.support_capacity,
                    terminal=is_terminal,
                )
                if is_terminal:
                    self._switch_after_dwell = True
                else:
                    self.next_stop_index = min(self.next_stop_index + 1, len(self.direction["stops"]) - 1)
            else:
                self.progress_m = min(moved_to, self.route_length_m)

        lon, lat = self._position()
        event_type = "heartbeat" if paused or self.dwell_remaining_s > 0 else "vehicle_telemetry"
        telemetry = VehicleTelemetry(
            event_id=stable_event_id(self.vehicle_id, event_type, event_time),
            event_type=event_type, vehicle_id=self.vehicle_id, route_id=self.route_id,
            region_id=self.region_id, direction_id=self.direction_id,
            event_ts=event_time, produced_ts_ms=produced_ts_ms,
            lat=lat, lon=lon, progress_m=self.progress_m,
            speed_mps=0.0 if paused or self.dwell_remaining_s > 0 else self.speed_mps,
            vehicle_type=self.vehicle_type, capacity_range_min=self.capacity_range_min,
            capacity_range_max=self.capacity_range_max, advisory_extra=self.advisory_extra,
        )
        return BusTick(telemetry, flow, self.ledger.snapshot())
