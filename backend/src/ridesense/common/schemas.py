"""Versioned Pydantic contracts shared by the simulator, Kafka and Spark."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    event_id: str = Field(min_length=8)
    event_type: str
    vehicle_id: str = Field(min_length=1)
    route_id: str = Field(min_length=1)
    region_id: str = Field(default="whole_city", min_length=2, max_length=32)
    direction_id: int = Field(ge=0)
    event_ts: datetime
    produced_ts_ms: int = Field(gt=0)


class VehicleTelemetry(Event):
    event_type: Literal["vehicle_telemetry", "heartbeat"] = "vehicle_telemetry"
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    progress_m: float = Field(ge=0)
    speed_mps: float = Field(ge=0, le=40)
    delay_s: int = Field(default=0, ge=0)
    headway_s: int = Field(default=0, ge=0)
    vehicle_type: str = "Standard city bus (simulation)"
    capacity_range_min: int = Field(default=60, ge=1)
    capacity_range_max: int = Field(default=75, ge=1)
    advisory_extra: bool = False


class PassengerFlow(Event):
    event_type: Literal["passenger_flow"] = "passenger_flow"
    stop_id: str
    boarding_count: int = Field(ge=0)
    alighting_count: int = Field(ge=0)
    door_open_ms: int = Field(ge=0)
    capacity: int = Field(gt=0)
    seat_capacity: int = Field(ge=0)
    stand_capacity: int = Field(ge=0)
    standing_support_capacity: int = Field(ge=0)
    terminal: bool = False

    @field_validator("standing_support_capacity")
    @classmethod
    def support_within_standing_capacity(cls, value, info):
        stand_capacity = info.data.get("stand_capacity")
        if stand_capacity is not None and value > stand_capacity:
            raise ValueError("standing support capacity cannot exceed stand capacity")
        return value


class OccupancyState(Event):
    event_type: Literal["occupancy_state"] = "occupancy_state"
    lat: float | None = None
    lon: float | None = None
    progress_m: float | None = None
    speed_mps: float = Field(default=0, ge=0, le=40)
    capacity: int = Field(gt=0)
    seat_capacity: int = Field(default=40, ge=0)
    stand_capacity: int = Field(default=30, ge=0)
    support_capacity: int = Field(default=24, ge=0)
    vehicle_type: str = "Standard city bus (simulation)"
    capacity_range_min: int = Field(default=60, ge=1)
    capacity_range_max: int = Field(default=75, ge=1)
    onboard_count: int = Field(ge=0)
    seated_count: int = Field(ge=0)
    standing_count: int = Field(ge=0)
    supported_standing_count: int = Field(ge=0)
    unsupported_standing_count: int = Field(ge=0)
    occ_est_pct: float = Field(ge=0, le=120)
    apc_missing: bool = False
    advisory_extra: bool = False


class PredictionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    event_id: str
    vehicle_id: str
    route_id: str
    made_at: datetime
    target_ts: datetime
    horizon_minutes: int = Field(gt=0)
    pred_pct: float = Field(ge=0, le=120)
    model_version: str
    contributions: dict[str, float] = Field(default_factory=dict)


class AlertEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    event_id: str
    alert_type: Literal["OVERCROWD", "PREDICTED_OVERCROWD", "ABNORMAL_CHANGE"]
    severity: Literal["info", "warning", "critical"]
    vehicle_id: str | None = None
    route_id: str
    detected_at: datetime
    window_starts: list[datetime] = Field(default_factory=list)
    occ_pct: float | None = None
    pred_pct: float | None = None
    delay_s: int | None = None
    threshold: float


class ControlCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    command_id: str
    action: Literal["start", "pause", "resume", "reset", "set_speed", "set_fleet", "set_region",
                    "rush_hour", "normal_hours", "moderate_hours", "route_demo"]
    issued_at: datetime
    requested_by: str
    speedup: Literal[1, 5] | None = None
    route_bus_counts: dict[str, int] = Field(default_factory=dict)
    region_id: str | None = Field(default=None, min_length=2, max_length=32)
    parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)

    @field_validator("route_bus_counts")
    @classmethod
    def nonnegative_fleet(cls, value):
        if any(count < 0 for count in value.values()):
            raise ValueError("route bus counts cannot be negative")
        return value
