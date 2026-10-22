"""Fleet lifecycle and administrator control semantics."""

from __future__ import annotations

import random
from typing import Any

from ridesense.common.schemas import ControlCommand
from ridesense.common.regions import REGIONS, allocate_fleet, routes_for_region
from ridesense.simulator.bus import BusSimulator, BusTick


class FleetManager:
    def __init__(self, network: dict[str, Any], fleet_config: dict[str, Any], *, seed: int = 42,
                 max_buses: int = 4000, capacity: int = 70, seat_capacity: int = 40,
                 stand_capacity: int = 30, support_capacity: int = 24,
                 vehicle_profiles: list[dict[str, Any]] | None = None):
        self.network = network
        self.seed = seed
        self.max_buses = max_buses
        self.capacity = capacity
        self.seat_capacity = seat_capacity
        self.stand_capacity = stand_capacity
        self.support_capacity = support_capacity
        self.vehicle_profiles = vehicle_profiles or [{
            "name": "Standard city bus (simulation)", "weight": 1, "capacity": capacity,
            "seats": seat_capacity, "standing": stand_capacity, "handholds": support_capacity,
            "range_min": 60, "range_max": 75,
        }]
        if any(int(profile["handholds"]) > int(profile["standing"]) for profile in self.vehicle_profiles):
            raise ValueError("profile handhold capacity cannot exceed standing capacity")
        self.paused = True
        self.speedup = 1
        self.rush_hour = False
        self.demand_profile = "normal"
        self.region_id = "whole_city"
        self._next_vehicle = 1
        self._rng = random.Random(seed)
        self._routes = {r["route_id"]: r for r in network["routes"]}
        self._hub_scores = {sid: int(stop["hub_score"]) for sid, stop in network["stops"].items()}
        self._route_directions = {}
        for route in network["routes"]:
            valid = [d for d in route["directions"] if d["usable"]]
            if route["usable"] and len(valid) >= 1:
                if len(valid) == 1:
                    original = valid[0]
                    length = float(original["length_m"])
                    valid.append({
                        **original,
                        "direction_id": int(original["direction_id"]) + 1,
                        "shape": list(reversed(original["shape"])),
                        "stops": [
                            {**stop, "seq": index,
                             "progress_m": max(0.0, length-float(stop["progress_m"]))}
                            for index, stop in enumerate(reversed(original["stops"]))
                        ],
                    })
                self._route_directions[route["route_id"]] = valid
        self.all_usable_routes = bool(fleet_config.get("all_usable_routes", False))
        self.minimum_per_route = int(fleet_config.get("minimum_per_route", 1 if self.all_usable_routes else 0))
        stop_routes: dict[str, set[str]] = {}
        for route_id, directions in self._route_directions.items():
            for direction in directions:
                for stop in direction.get("stops", []):
                    stop_routes.setdefault(str(stop["stop_id"]), set()).add(route_id)
        self._route_connections = {
            route_id: sum(1 for stop_id in {str(stop["stop_id"]) for direction in directions
                                           for stop in direction.get("stops", [])}
                          if len(stop_routes.get(stop_id, set())) > 1)
            for route_id, directions in self._route_directions.items()
        }
        self._region_routes: dict[str, set[str]] = {"whole_city": set(self._route_directions)}
        self._planned_counts: dict[str, dict[str, int]] = {}
        self._fleet: dict[str, list[BusSimulator]] = {}

    @property
    def bus_count(self):
        return sum(len(buses) for buses in self._fleet.values())

    @property
    def route_bus_counts(self):
        return {route_id: len(buses) for route_id, buses in sorted(self._fleet.items()) if buses}

    def _new_bus(self, route_id: str, position_index: int, route_count: int,
                 advisory_extra: bool = False):
        route = self._routes[route_id]
        directions = self._route_directions[route_id]
        length = float(directions[0]["length_m"])
        progress = length * ((position_index + 0.5) / max(1, route_count))
        vehicle_id = f"V{self._next_vehicle:03d}"
        self._next_vehicle += 1
        profile = self._rng.choices(self.vehicle_profiles,
                                    weights=[max(1, int(item.get("weight", 1))) for item in self.vehicle_profiles],
                                    k=1)[0]
        bus = BusSimulator(
            vehicle_id=vehicle_id, route_id=route_id, route_short_name=route["short_name"],
            region_id=self.region_id,
            directions=directions, progress_m=progress,
            rng=random.Random(self._rng.randrange(2**32)), capacity=int(profile["capacity"]),
            seat_capacity=int(profile["seats"]), stand_capacity=int(profile["standing"]),
            support_capacity=int(profile["handholds"]), vehicle_type=str(profile["name"]),
            capacity_range_min=int(profile["range_min"]), capacity_range_max=int(profile["range_max"]),
            advisory_extra=advisory_extra, demand_profile=self.demand_profile,
            stop_hub_scores=self._hub_scores,
        )
        return bus

    def set_route_bus_counts(self, counts: dict[str, int], advisory_extra_route_id: str | None = None):
        normalized = {str(route_id): int(count) for route_id, count in counts.items() if int(count) > 0}
        if any(count < 0 for count in counts.values()):
            raise ValueError("route bus counts cannot be negative")
        if advisory_extra_route_id:
            current_extra_count = len(self._fleet.get(advisory_extra_route_id, []))
            normalized[advisory_extra_route_id] = max(
                normalized.get(advisory_extra_route_id, 0), current_extra_count + 1)
        if sum(normalized.values()) > self.max_buses:
            raise ValueError(f"fleet request has {sum(normalized.values())} buses; limit is {self.max_buses}")
        unknown = [route_id for route_id in normalized if route_id not in self._route_directions]
        if unknown:
            raise ValueError(f"route ids are missing or do not have a usable direction: {unknown}")
        missing = self._region_routes.get(self.region_id, set()) - set(normalized)
        if missing:
            raise ValueError(f"The fleet must cover all usable routes in {self.region_id}")
        if any(normalized.get(route_id, 0) < (2 if self._route_connections.get(route_id, 0) <= 1 else 1)
               for route_id in self._region_routes.get(self.region_id, set())):
            raise ValueError("Every selected route needs at least one bus; isolated routes need two")

        next_fleet: dict[str, list[BusSimulator]] = {}
        for route_id, desired in normalized.items():
            existing = self._fleet.get(route_id, [])
            if desired <= len(existing):
                next_fleet[route_id] = existing[:desired]
            else:
                buses = list(existing)
                for index in range(len(existing), desired):
                    buses.append(self._new_bus(route_id, index, desired,
                                               advisory_extra=route_id == advisory_extra_route_id))
                next_fleet[route_id] = buses
        self._fleet = next_fleet
        for buses in self._fleet.values():
            for bus in buses:
                bus.demand_profile = self.demand_profile
                bus.region_id = self.region_id

    def recommended_fleet(self, region_id: str) -> dict[str, int]:
        if region_id not in self._region_routes:
            self._region_routes[region_id] = routes_for_region(self.network, region_id)
        routes = self._region_routes[region_id]
        metrics = [{"route_id": route_id,
                    "length_m": max(float(direction["length_m"]) for direction in self._route_directions[route_id]),
                    "isolated": self._route_connections.get(route_id, 0) <= 1}
                   for route_id in routes]
        if region_id not in self._planned_counts:
            self._planned_counts[region_id] = allocate_fleet(metrics, self.max_buses)
        return dict(self._planned_counts[region_id])

    def set_region(self, region_id: str):
        if region_id not in REGIONS:
            raise ValueError(f"unknown simulation region: {region_id}")
        if region_id not in self._region_routes:
            self._region_routes[region_id] = routes_for_region(self.network, region_id)

        region_routes = self._region_routes[region_id]
        # A commuter route demo can adjust one route's allocation.  If that
        # leaves a partial fleet, starting its region must restore the complete
        # regional plan instead of silently retaining only the demo route.
        if region_id == self.region_id and set(self._fleet) == region_routes:
            return
        self.region_id = region_id
        counts = self.recommended_fleet(region_id)
        self.set_route_bus_counts(counts)

    def set_route_demo(self, route_id: str, bus_count: int, demand_profile: str = "normal"):
        if demand_profile not in {"normal", "moderate", "high"}:
            raise ValueError("route demo crowding must be normal, moderate, or high")
        self.demand_profile = "rush" if demand_profile == "high" else demand_profile
        self.rush_hour = self.demand_profile == "rush"
        region_routes = self._region_routes.get(self.region_id)
        if region_routes is None:
            self._region_routes[self.region_id] = routes_for_region(self.network, self.region_id)
            region_routes = self._region_routes[self.region_id]
        if route_id not in region_routes or route_id not in self._route_directions:
            raise ValueError(f"route {route_id} is not part of the selected simulation region")
        minimum = 2 if self._route_connections.get(route_id, 0) <= 1 else 1
        desired = max(minimum, int(bus_count))
        if desired > self.max_buses:
            raise ValueError(f"route demo needs {desired} buses; fleet limit is {self.max_buses}")

        # A commuter demo runs within the administrator's regional fleet. The
        # previous implementation replaced every other route with one selected
        # route, leaving the admin table at zero for nearly all live buses.
        # Restore a complete regional fleet when necessary, then reallocate a
        # few flexible slots to the requested route while retaining the region
        # minimum for every other route.
        if set(self._fleet) != region_routes:
            self._fleet = {}
            self.set_route_bus_counts(self.recommended_fleet(self.region_id))
        counts = self.route_bus_counts
        counts[route_id] = desired
        overflow = sum(counts.values()) - self.max_buses
        if overflow > 0:
            candidates = sorted(
                (candidate for candidate in counts if candidate != route_id),
                key=lambda candidate: counts[candidate] - (2 if self._route_connections.get(candidate, 0) <= 1 else 1),
                reverse=True,
            )
            for candidate in candidates:
                minimum_for_candidate = 2 if self._route_connections.get(candidate, 0) <= 1 else 1
                removable = max(0, counts[candidate] - minimum_for_candidate)
                reduction = min(removable, overflow)
                counts[candidate] -= reduction
                overflow -= reduction
                if overflow == 0:
                    break
        if overflow > 0:
            raise ValueError("route demo cannot fit within the regional fleet limit")
        self.set_route_bus_counts(counts)
        self.paused = False

    def apply_command(self, command: ControlCommand | dict[str, Any]):
        if isinstance(command, dict):
            command = ControlCommand.model_validate(command)
        if command.action == "set_fleet" and command.region_id:
            self.region_id = command.region_id
            self.recommended_fleet(self.region_id)
        elif command.action == "route_demo":
            if command.region_id not in REGIONS:
                raise ValueError("route demo needs a supported Bengaluru region")
            self.region_id = command.region_id
            route_id = str(command.parameters.get("route_id", ""))
            bus_count = int(command.parameters.get("buses", 0))
            demand_profile = str(command.parameters.get("demand_profile", "normal"))
            self.set_route_demo(route_id, bus_count, demand_profile)
        elif command.action in {"set_region", "start", "resume", "rush_hour", "normal_hours",
                                "moderate_hours", "reset"} and command.region_id:
            self.set_region(command.region_id)
        if command.action == "pause":
            self.paused = True
        elif command.action in {"start", "resume"}:
            if not self._fleet:
                self.set_region(self.region_id)
            self.paused = False
        elif command.action == "set_speed":
            if command.speedup not in (1, 5):
                raise ValueError("speedup must be 1 or 5")
            self.speedup = command.speedup
        elif command.action == "set_fleet":
            extra_route = command.parameters.get("advisory_extra_route_id")
            self.set_route_bus_counts(command.route_bus_counts,
                                      advisory_extra_route_id=str(extra_route) if extra_route else None)
        elif command.action == "route_demo":
            pass
        elif command.action in {"rush_hour", "normal_hours", "moderate_hours"}:
            self.demand_profile = {"rush_hour": "rush", "moderate_hours": "moderate",
                                   "normal_hours": "normal"}[command.action]
            self.rush_hour = self.demand_profile == "rush"
            if not self._fleet:
                self.set_region(self.region_id)
            self.paused = False
            for buses in self._fleet.values():
                for bus in buses:
                    bus.demand_profile = self.demand_profile
        elif command.action == "reset":
            current = self.recommended_fleet(self.region_id)
            self._fleet = {}
            self.demand_profile = "normal"
            self.set_route_bus_counts(current)
            self.paused = True
            self.rush_hour = False
        elif command.action == "set_region":
            self.paused = True
        else:
            raise ValueError(f"unsupported simulator command: {command.action}")
        return self.status()

    def tick(self, event_time, dt_seconds: float = 3.0, produced_ts_ms: int | None = None) -> list[BusTick]:
        return [bus.tick(event_time, dt_seconds, produced_ts_ms,
                         paused=self.paused) for route_buses in self._fleet.values() for bus in route_buses]

    def status(self):
        return {
            "running": not self.paused,
            "paused": self.paused,
            "rush_hour": self.rush_hour,
            "demand_profile": self.demand_profile,
            "region_id": self.region_id,
            "region_name": REGIONS[self.region_id]["name"],
            "speedup": self.speedup,
            "total_buses": self.bus_count,
            "max_buses": self.max_buses,
            "route_bus_counts": self.route_bus_counts,
            "active_routes": len(self.route_bus_counts),
            "simulated": True,
        }
