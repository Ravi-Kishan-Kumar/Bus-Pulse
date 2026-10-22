"""Lightweight Bengaluru demo regions used to scope the synthetic fleet."""

from __future__ import annotations

from typing import Any


# Polygons are deliberately broad demo areas, not official ward boundaries.
REGIONS: dict[str, dict[str, Any]] = {
    "whole_city": {
        "name": "Whole Bengaluru", "center": [77.5946, 12.9716], "zoom": 10.2,
        "polygon": [[77.35, 12.73], [77.91, 12.73], [77.91, 13.25], [77.35, 13.25]],
    },
    "central": {
        "name": "Central Bengaluru", "center": [77.5946, 12.9716], "zoom": 12.2,
        "polygon": [[77.51, 12.92], [77.67, 12.92], [77.67, 13.04], [77.51, 13.04]],
    },
    "north": {
        "name": "North Bengaluru", "center": [77.59, 13.09], "zoom": 11.4,
        "polygon": [[77.47, 13.035], [77.75, 13.035], [77.75, 13.23], [77.47, 13.23]],
    },
    "east": {
        "name": "East Bengaluru · Whitefield", "center": [77.72, 12.98], "zoom": 11.5,
        "polygon": [[77.65, 12.84], [77.87, 12.84], [77.87, 13.12], [77.65, 13.12]],
    },
    "south": {
        "name": "South Bengaluru", "center": [77.59, 12.84], "zoom": 11.5,
        "polygon": [[77.48, 12.74], [77.72, 12.74], [77.72, 12.93], [77.48, 12.93]],
    },
    "west": {
        "name": "West Bengaluru", "center": [77.50, 12.97], "zoom": 11.5,
        "polygon": [[77.37, 12.84], [77.54, 12.84], [77.54, 13.11], [77.37, 13.11]],
    },
    "orr": {
        "name": "Outer Ring Road corridor", "center": [77.67, 12.94], "zoom": 11.2,
        "polygon": [[77.57, 12.83], [77.72, 12.83], [77.80, 12.91], [77.81, 13.02],
                    [77.73, 13.10], [77.67, 13.04], [77.61, 12.96]],
    },
}


def point_in_region(lon: float, lat: float, region_id: str) -> bool:
    polygon = REGIONS.get(region_id, REGIONS["whole_city"])["polygon"]
    inside = False
    previous = polygon[-1]
    for current in polygon:
        x1, y1 = previous
        x2, y2 = current
        if ((y1 > lat) != (y2 > lat)) and lon < (x2 - x1) * (lat - y1) / ((y2 - y1) or 1e-12) + x1:
            inside = not inside
        previous = current
    return inside


def routes_for_region(network: dict[str, Any], region_id: str) -> set[str]:
    """Return usable GTFS routes with at least one stop or shape point in a region."""
    if region_id == "whole_city":
        return {str(route["route_id"]) for route in network["routes"]
                if route.get("usable") and any(direction.get("usable") for direction in route.get("directions", []))}
    stops = network.get("stops", {})
    selected: set[str] = set()
    for route in network["routes"]:
        if not route.get("usable"):
            continue
        matched = False
        for direction in route.get("directions", []):
            if not direction.get("usable"):
                continue
            for stop in direction.get("stops", []):
                point = stops.get(str(stop.get("stop_id")), {})
                if point and point_in_region(float(point["lon"]), float(point["lat"]), region_id):
                    matched = True
                    break
            if not matched and any(point_in_region(float(point[0]), float(point[1]), region_id)
                                   for point in direction.get("shape", [])):
                matched = True
            if matched:
                break
        if matched:
            selected.add(str(route["route_id"]))
    return selected


def allocate_fleet(route_metrics: list[dict[str, Any]], max_buses: int) -> dict[str, int]:
    """Allocate one bus per route, two for isolated routes, then favor longer lines."""
    metrics = {str(row["route_id"]): row for row in route_metrics}
    counts = {route_id: (2 if bool(row.get("isolated")) else 1) for route_id, row in metrics.items()}
    if sum(counts.values()) > max_buses:
        raise ValueError("The selected region has more isolated routes than the configured fleet limit can cover")
    remaining = max_buses - sum(counts.values())
    while remaining:
        candidates = [route_id for route_id, count in counts.items()
                      if count < min(4, 1 + int(float(metrics[route_id].get("length_m", 0)) // 25000))]
        if not candidates:
            break
        route_id = max(candidates, key=lambda item: float(metrics[item].get("length_m", 0)) / (counts[item] ** 1.6))
        counts[route_id] += 1
        remaining -= 1
    return counts

