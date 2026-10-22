#!/usr/bin/env python3
"""Build a compact, quality-screened Bengaluru bus network from a GTFS zip.

GTFS is used for static routes, stops and geometry only. Schedule times and fares
are deliberately excluded from the runtime network.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import html
import json
import math
import statistics
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FEED = ROOT / "data/blr/raw/bmtc.zip"
DEFAULT_OUTPUT = ROOT / "data/blr/network_all.json.gz"
DEFAULT_REPORT = ROOT / "reports/network_quality.json"
DEFAULT_PREVIEW = ROOT / "reports/network_preview.html"
DEFAULT_PROVENANCE = ROOT / "data/blr/PROVENANCE.json"

BBOX = (77.3, 12.7, 78.0, 13.4)  # min_lon, min_lat, max_lon, max_lat
STOP_MAX_OFFSET_M = 100.0
MAX_SHAPE_KM = 80.0
SIMPLIFY_M = 15.0


def rows(zf: zipfile.ZipFile, name: str):
    with zf.open(name) as raw:
        import io

        yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))


def haversine(a: tuple[float, float], b: tuple[float, float]) -> float:
    lon1, lat1 = map(math.radians, a)
    lon2, lat2 = map(math.radians, b)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 12_742_000 * math.asin(math.sqrt(min(1.0, h)))


def point_segment_distance(point, start, end, cos_lat):
    # Local equirectangular projection is accurate enough for short Bengaluru segments.
    sx, sy = start[0] * 111_320 * cos_lat, start[1] * 110_574
    ex, ey = end[0] * 111_320 * cos_lat, end[1] * 110_574
    px, py = point[0] * 111_320 * cos_lat, point[1] * 110_574
    dx, dy = ex - sx, ey - sy
    den = dx * dx + dy * dy
    t = 0.0 if den == 0 else max(0.0, min(1.0, ((px - sx) * dx + (py - sy) * dy) / den))
    qx, qy = sx + t * dx, sy + t * dy
    return math.hypot(px - qx, py - qy), t


def project_stop(stop_xy, shape, cumulative, cos_lat):
    best_distance, best_progress = float("inf"), 0.0
    for i, (a, b) in enumerate(zip(shape, shape[1:])):
        distance, fraction = point_segment_distance(stop_xy, a, b, cos_lat)
        if distance < best_distance:
            best_distance = distance
            best_progress = cumulative[i] + fraction * (cumulative[i + 1] - cumulative[i])
    return best_progress, best_distance


def simplify(points, tolerance_m=SIMPLIFY_M):
    if len(points) <= 2:
        return points
    lat0 = math.radians(sum(p[1] for p in points) / len(points))
    cos_lat = math.cos(lat0)
    xy = [(p[0] * 111_320 * cos_lat, p[1] * 110_574) for p in points]
    keep = {0, len(points) - 1}
    stack = [(0, len(points) - 1)]
    while stack:
        start, end = stack.pop()
        ax, ay = xy[start]
        bx, by = xy[end]
        dx, dy = bx - ax, by - ay
        den = dx * dx + dy * dy
        max_dist, max_idx = -1.0, -1
        for i in range(start + 1, end):
            px, py = xy[i]
            t = 0.0 if den == 0 else max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / den))
            d = math.hypot(px - ax - t * dx, py - ay - t * dy)
            if d > max_dist:
                max_dist, max_idx = d, i
        if max_dist > tolerance_m:
            keep.add(max_idx)
            stack.extend(((start, max_idx), (max_idx, end)))
    return [points[i] for i in sorted(keep)]


def percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)]


def build(feed: Path):
    with zipfile.ZipFile(feed) as zf:
        info = next(rows(zf, "feed_info.txt"), {})
        attributions = list(rows(zf, "attributions.txt")) if "attributions.txt" in zf.namelist() else []
        stops = {}
        for row in rows(zf, "stops.txt"):
            try:
                stops[row["stop_id"]] = {
                    "name": row.get("stop_name", ""),
                    "lon": float(row["stop_lon"]),
                    "lat": float(row["stop_lat"]),
                    "hub_score": 0,
                }
            except (KeyError, ValueError):
                continue

        routes = {}
        for row in rows(zf, "routes.txt"):
            if row.get("route_type") == "3":
                routes[row["route_id"]] = {
                    "route_id": row["route_id"],
                    "short_name": row.get("route_short_name", ""),
                    "long_name": row.get("route_long_name", ""),
                    "usable": False,
                    "fail_reasons": [],
                    "trips_in_feed": 0,
                    "directions": [],
                }

        trips = {}
        trip_counts: Counter[str] = Counter()
        for row in rows(zf, "trips.txt"):
            if row.get("route_id") in routes:
                trip_id = row["trip_id"]
                trips[trip_id] = {
                    "route_id": row["route_id"],
                    "direction_id": int(row.get("direction_id") or 0),
                    "shape_id": row.get("shape_id", ""),
                }
                routes[row["route_id"]]["trips_in_feed"] += 1

        for row in rows(zf, "stop_times.txt"):
            if row.get("trip_id") in trips:
                trip_counts[row["trip_id"]] += 1

        representative = {}
        trips_by_direction: defaultdict[tuple[str, int], list[str]] = defaultdict(list)
        for trip_id, trip in trips.items():
            trips_by_direction[(trip["route_id"], trip["direction_id"])].append(trip_id)
        for key, ids in trips_by_direction.items():
            representative[key] = max(ids, key=lambda trip_id: trip_counts[trip_id])

        selected = set(representative.values())
        selected_stops: defaultdict[str, list[tuple[int, str]]] = defaultdict(list)
        for row in rows(zf, "stop_times.txt"):
            trip_id = row.get("trip_id", "")
            if trip_id in selected:
                try:
                    selected_stops[trip_id].append((int(row["stop_sequence"]), row["stop_id"]))
                except (KeyError, ValueError):
                    continue

        shape_ids = {trips[trip_id]["shape_id"] for trip_id in selected if trips[trip_id]["shape_id"]}
        shape_points: defaultdict[str, list[tuple[int, tuple[float, float]]]] = defaultdict(list)
        for row in rows(zf, "shapes.txt"):
            if row.get("shape_id") in shape_ids:
                try:
                    shape_points[row["shape_id"]].append(
                        (int(row["shape_pt_sequence"]), (float(row["shape_pt_lon"]), float(row["shape_pt_lat"])))
                    )
                except (KeyError, ValueError):
                    continue

    route_stop_sets: defaultdict[str, set[str]] = defaultdict(set)
    direction_failures: Counter[str] = Counter()
    offset_p90s = []
    simplified_point_count = 0
    original_point_count = 0
    directions = []

    for (route_id, direction_id), trip_id in sorted(representative.items()):
        trip = trips[trip_id]
        stop_rows = sorted(selected_stops[trip_id])
        stop_ids = [stop_id for _, stop_id in stop_rows if stop_id in stops]
        reasons = []
        if len(stop_ids) < 8:
            reasons.append("fewer_than_8_stops")

        shape_rows = sorted(shape_points.get(trip["shape_id"], []))
        shape = [xy for _, xy in shape_rows]
        original_point_count += len(shape)
        if len(shape) < 2:
            reasons.append("missing_shape")
            continue

        segment_lengths = [haversine(a, b) for a, b in zip(shape, shape[1:])]
        cumulative = [0.0]
        for length in segment_lengths:
            cumulative.append(cumulative[-1] + length)
        length_m = cumulative[-1]
        if length_m > MAX_SHAPE_KM * 1000:
            reasons.append("shape_over_80_km")

        offsets, progress_stops = [], []
        mean_lat = math.radians(sum(point[1] for point in shape) / len(shape))
        for seq, stop_id in stop_rows:
            if stop_id not in stops:
                continue
            stop = stops[stop_id]
            xy = (stop["lon"], stop["lat"])
            if not (BBOX[0] <= xy[0] <= BBOX[2] and BBOX[1] <= xy[1] <= BBOX[3]):
                reasons.append("stop_outside_bengaluru_box")
            progress, offset = project_stop(xy, shape, cumulative, math.cos(mean_lat))
            offsets.append(offset)
            progress_stops.append((seq, stop_id, progress, offset))

        if offsets:
            p90_offset = percentile(offsets, 0.90)
            offset_p90s.append(p90_offset)
            if p90_offset > STOP_MAX_OFFSET_M:
                reasons.append("stop_offset_over_100m")
        else:
            p90_offset = 0.0

        straight_gaps = sum(
            haversine((stops[a]["lon"], stops[a]["lat"]), (stops[b]["lon"], stops[b]["lat"]))
            for a, b in zip(stop_ids, stop_ids[1:])
        )
        ratio = length_m / straight_gaps if straight_gaps else 0.0
        if not 0.95 <= ratio <= 2.0:
            reasons.append("shape_to_stops_length_ratio")

        in_order = sum(a[2] <= b[2] + 50 for a, b in zip(progress_stops, progress_stops[1:]))
        order_ratio = in_order / max(1, len(progress_stops) - 1)
        if order_ratio < 0.95:
            reasons.append("stop_order_mismatch")

        simplified = simplify(shape)
        simplified_point_count += len(simplified)
        direction_usable = not reasons
        if direction_usable:
            for stop_id in stop_ids:
                stops[stop_id]["hub_score"] += 1
                route_stop_sets[route_id].add(stop_id)

        direction = {
            "direction_id": direction_id,
            "length_m": round(length_m, 1),
            "shape": [[round(lon, 6), round(lat, 6)] for lon, lat in simplified],
            "stops": [
                {"stop_id": stop_id, "seq": seq, "progress_m": round(progress, 1), "offset_m": round(offset, 1)}
                for seq, stop_id, progress, offset in progress_stops
            ],
            "usable": direction_usable,
            "fail_reasons": sorted(set(reasons)),
            "representative_trip_id": trip_id,
        }
        routes[route_id]["directions"].append(direction)

    for route in routes.values():
        route["directions"].sort(key=lambda d: d["direction_id"])
        usable_directions = [direction for direction in route["directions"] if direction["usable"]]
        route["usable"] = bool(usable_directions)
        route["fail_reasons"] = sorted({reason for d in route["directions"] for reason in d["fail_reasons"]})

    used_stops = {sid for route_id, ids in route_stop_sets.items() if routes[route_id]["usable"] for sid in ids}
    usable_routes = [route for route in routes.values() if route["usable"]]
    fail_counts = Counter(reason for route in routes.values() for reason in route["fail_reasons"])
    return {
        "version": 2,
        "source": {
            "feed": "mdb-2595",
            "feed_version": info.get("feed_version", "unknown"),
            "service_start_date": info.get("feed_start_date", ""),
            "service_end_date": info.get("feed_end_date", ""),
            "sha256": hashlib.sha256(feed.read_bytes()).hexdigest(),
            "producer_url": info.get("feed_publisher_url", ""),
            "attributions": attributions,
        },
        "bbox": list(BBOX),
        "stops": {sid: stops[sid] for sid in stops},
        "routes": list(routes.values()),
        "quality": {
            "route_count": len(routes),
            "usable_routes": len(usable_routes),
            "direction_count": sum(len(route["directions"]) for route in routes.values()),
            "usable_directions": sum(sum(d["usable"] for d in route["directions"]) for route in routes.values()),
            "covered_stops": len(used_stops),
            "fail_reasons": dict(fail_counts),
            "median_direction_p90_offset_m": round(statistics.median(offset_p90s), 1) if offset_p90s else 0,
            "original_shape_points": original_point_count,
            "simplified_shape_points": simplified_point_count,
        },
    }


def write_outputs(network: dict[str, Any], output: Path, report: Path, preview: Path, provenance: Path):
    output.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    preview.parent.mkdir(parents=True, exist_ok=True)
    provenance.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(network, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    with gzip.open(output, "wb", compresslevel=9) as handle:
        handle.write(encoded)
    quality = network["quality"]
    report.write_text(json.dumps(quality, indent=2) + "\n", encoding="utf-8")
    provenance_doc = {
        **network["source"],
        "source_license": "Open Database License (ODbL-1.0), as stated by feed producer; verify terms before redistribution",
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "route_quality": quality,
        "data_honesty": "Static route topology only. Passenger counts, vehicle positions, occupancy and incidents are simulated.",
    }
    provenance.write_text(json.dumps(provenance_doc, indent=2) + "\n", encoding="utf-8")
    preview.write_text(make_preview(network), encoding="utf-8")


def make_preview(network):
    routes = [
        {"id": route["route_id"], "name": route["short_name"], "usable": route["usable"],
         "directions": [d["shape"] for d in route["directions"] if d["shape"]]}
        for route in network["routes"]
    ]
    payload = json.dumps(routes, separators=(",", ":"))
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BMTC GTFS route quality preview</title><link href="https://unpkg.com/maplibre-gl@5.6.2/dist/maplibre-gl.css" rel="stylesheet"><style>html,body,#map{{height:100%;margin:0}}#status{{position:absolute;z-index:2;top:10px;left:10px;background:white;padding:10px;font:14px sans-serif;max-width:420px}}</style></head><body><div id="map"></div><div id="status">Loading network preview…</div><script src="https://unpkg.com/maplibre-gl@5.6.2/dist/maplibre-gl.js"></script><script>const routes={payload};const map=new maplibregl.Map({{container:'map',style:'https://tiles.openfreemap.org/styles/liberty',center:[77.5946,12.9716],zoom:10}});map.addControl(new maplibregl.NavigationControl());map.on('load',()=>{{const features=[];for(const r of routes)for(const c of r.directions)features.push({{type:'Feature',properties:{{usable:r.usable?1:0,route:r.id,name:r.name}},geometry:{{type:'LineString',coordinates:c}}}});map.addSource('routes',{{type:'geojson',data:{{type:'FeatureCollection',features}}}});map.addLayer({{id:'routes',type:'line',source:'routes',paint:{{'line-color':['case',['==',['get','usable'],1],'#2563eb','#ef4444'],'line-opacity':['case',['==',['get','usable'],1],0.35,0.5],'line-width':1.5}}}});document.getElementById('status').textContent=routes.length+' routes; blue = usable, red = failed quality gates. Review routes against the road network.';}});map.on('error',()=>document.getElementById('status').textContent='Map tiles unavailable; route preview needs network tiles.');</script></body></html>"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed", type=Path, default=DEFAULT_FEED)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--preview", type=Path, default=DEFAULT_PREVIEW)
    parser.add_argument("--provenance", type=Path, default=DEFAULT_PROVENANCE)
    args = parser.parse_args()
    if not args.feed.exists():
        parser.error(f"GTFS zip not found: {args.feed}; download the mdb-2595 feed first")
    network = build(args.feed)
    write_outputs(network, args.output, args.report, args.preview, args.provenance)
    print(json.dumps(network["quality"], indent=2))
    if network["quality"]["usable_routes"] < 3000 or network["quality"]["covered_stops"] < 8000:
        raise SystemExit("Network quality floor failed: need >=3000 usable routes and >=8000 covered stops")


if __name__ == "__main__":
    main()
