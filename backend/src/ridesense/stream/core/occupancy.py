"""Deterministic bus occupancy accounting, including seated and standing riders."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class OccupancyLedger:
    capacity: int = 70
    seat_capacity: int = 40
    stand_capacity: int = 30
    standing_support_capacity: int = 30
    crush_ratio: float = 1.2
    seated_count: int = 0
    standing_count: int = 0
    supported_standing_count: int = 0

    def __post_init__(self):
        if min(self.capacity, self.seat_capacity, self.stand_capacity, self.standing_support_capacity) < 0:
            raise ValueError("capacities cannot be negative")
        if self.capacity == 0 or self.seat_capacity + self.stand_capacity < self.capacity:
            raise ValueError("seat + standing capacity must cover total capacity")
        if self.standing_support_capacity > self.stand_capacity:
            raise ValueError("support capacity cannot exceed standing capacity")
        if self.crush_ratio < 1.0:
            raise ValueError("crush ratio must be >= 1")
        self._normalize()

    @property
    def unsupported_standing_count(self) -> int:
        return self.standing_count - self.supported_standing_count

    @property
    def onboard_count(self) -> int:
        return self.seated_count + self.standing_count

    @property
    def occ_est_pct(self) -> float:
        return 100.0 * self.onboard_count / self.capacity

    @property
    def crowd_label(self) -> str:
        pct = self.occ_est_pct
        if pct < 50:
            return "Low"
        if pct < 70:
            return "Medium"
        if pct <= 85:
            return "High"
        return "Full"

    def apply_flow(self, boarding: int, alighting: int, *, terminal: bool = False) -> dict:
        if boarding < 0 or alighting < 0:
            raise ValueError("boarding and alighting counts cannot be negative")
        before = self.onboard_count
        if terminal:
            self.seated_count = 0
            self.standing_count = 0
            self.supported_standing_count = 0
            self._normalize()
            return {"boarded": 0, "alighted": before, "terminal_reset": True}

        actually_alighted = min(alighting, self.onboard_count)
        total_alighted = actually_alighted
        # Riders without a support position leave first, then supported standees, then seats.
        unsupported = min(actually_alighted, self.unsupported_standing_count)
        self.standing_count -= unsupported
        actually_alighted -= unsupported
        supported = min(actually_alighted, self.supported_standing_count)
        self.standing_count -= supported
        self.supported_standing_count -= supported
        actually_alighted -= supported
        seated = min(actually_alighted, self.seated_count)
        self.seated_count -= seated

        onboard_room = max(0, int(self.capacity * self.crush_ratio) - self.onboard_count)
        boarded = min(boarding, onboard_room)
        seaters = min(boarded, self.seat_capacity - self.seated_count)
        self.seated_count += seaters
        remaining = boarded - seaters
        safe_standers = min(
            remaining,
            self.stand_capacity - self.standing_count,
            self.standing_support_capacity - self.supported_standing_count,
        )
        self.standing_count += safe_standers
        self.supported_standing_count += safe_standers
        remaining -= safe_standers
        crush_standers = min(remaining, max(0, int(self.capacity * self.crush_ratio) - self.onboard_count))
        self.standing_count += crush_standers
        self._normalize()
        return {"boarded": seaters + safe_standers + crush_standers, "alighted": total_alighted,
                "terminal_reset": False}

    def snapshot(self) -> dict:
        result = asdict(self)
        result.update(
            onboard_count=self.onboard_count,
            unsupported_standing_count=self.unsupported_standing_count,
            occ_est_pct=round(self.occ_est_pct, 2),
            crowd_label=self.crowd_label,
            simulated=True,
        )
        return result

    def _normalize(self):
        self.seated_count = max(0, min(self.seat_capacity, int(self.seated_count)))
        self.standing_count = max(0, int(self.standing_count))
        self.supported_standing_count = max(0, min(self.standing_count, self.standing_support_capacity))
        max_total = int(self.capacity * self.crush_ratio)
        if self.onboard_count > max_total:
            extra = self.onboard_count - max_total
            removed = min(extra, self.unsupported_standing_count)
            self.standing_count -= removed
            extra -= removed
            removed = min(extra, self.supported_standing_count)
            self.standing_count -= removed
            self.supported_standing_count -= removed
            extra -= removed
            if extra:
                self.seated_count = max(0, self.seated_count - extra)
