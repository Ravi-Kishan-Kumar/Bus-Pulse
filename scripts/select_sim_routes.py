#!/usr/bin/env python3
"""Choose a hub-connected demo fleet and assign a configurable count per route."""

from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_network(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def choose(network, route_count: int, fleet_size: int):
    routes = [r for r in network["routes"] if r["usable"]]
    route_stop_sets = {}
    route_lengths = {}
    stop_routes: defaultdict[str, set[str]] = defaultdict(set)
    for route in routes:
        usable_dirs = [d for d in route["directions"] if d["usable"]]
        if len(usable_dirs) < 2:
            continue
        route_stop_sets[route["route_id"]] = {
            stop["stop_id"] for direction in usable_dirs for stop in direction["stops"]
        }
        route_lengths[route["route_id"]] = sum(d["length_m"] for d in usable_dirs) / len(usable_dirs)
        for stop_id in route_stop_sets[route["route_id"]]:
            stop_routes[stop_id].add(route["route_id"])

    eligible = [r for r in routes if r["route_id"] in route_stop_sets and r["trips_in_feed"] >= 10]
    if len(eligible) < route_count:
        eligible = [r for r in routes if r["route_id"] in route_stop_sets]
    by_id = {r["route_id"]: r for r in eligible}
    hubs = sorted(stop_routes, key=lambda sid: (-len(stop_routes[sid]), network["stops"][sid]["name"]))
    selected = []
    selected_ids = set()

    def candidate_score(route_id):
        route = by_id[route_id]
        length_penalty = abs(route_lengths[route_id] - 15_000) / 15_000
        service_bonus = min(route["trips_in_feed"], 30) / 100
        shared = sum(len(route_stop_sets[route_id] & route_stop_sets[chosen_id]) >= 2 for chosen_id in selected_ids)
        return (-shared, length_penalty - service_bonus, route_id)

    for hub in hubs:
        if len(selected) >= route_count:
            break
        hub_candidates = [rid for rid in stop_routes[hub] if rid in by_id and rid not in selected_ids]
        for rid in sorted(hub_candidates, key=candidate_score):
            if len(selected) >= route_count:
                break
            if selected_ids and not any(len(route_stop_sets[rid] & route_stop_sets[other]) >= 2 for other in selected_ids):
                continue
            selected.append(rid)
            selected_ids.add(rid)

    if len(selected) < route_count:
        for route in sorted(eligible, key=lambda r: candidate_score(r["route_id"])):
            rid = route["route_id"]
            if rid in selected_ids:
                continue
            if selected_ids and not any(len(route_stop_sets[rid] & route_stop_sets[other]) >= 2 for other in selected_ids):
                continue
            selected.append(rid)
            selected_ids.add(rid)
            if len(selected) >= route_count:
                break

    if len(selected) != route_count:
        raise ValueError(f"Could select only {len(selected)} routes with >=2 usable directions and shared stops")
    if fleet_size < 3 * route_count:
        raise ValueError(f"fleet size {fleet_size} is too small: need at least 3 per route ({3 * route_count})")

    lengths = [route_lengths[rid] for rid in selected]
    weights = [length / sum(lengths) for length in lengths]
    allocation = [3] * route_count
    remaining = fleet_size - sum(allocation)
    fractions = []
    for index, weight in enumerate(weights):
        exact = remaining * weight
        allocation[index] += int(exact)
        fractions.append((exact - int(exact), index))
    for _, index in sorted(fractions, reverse=True)[: fleet_size - sum(allocation)]:
        allocation[index] += 1

    items = []
    for rid, count in zip(selected, allocation):
        route = by_id[rid]
        items.append({
            "route_id": rid,
            "short_name": route["short_name"],
            "buses": count,
            "directions": [d["direction_id"] for d in route["directions"] if d["usable"]],
            "length_m": round(route_lengths[rid]),
        })
    return {"routes": items, "total_buses": sum(allocation), "route_count": len(items)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", type=Path, default=ROOT / "data/blr/network_all.json.gz")
    parser.add_argument("--output", type=Path, default=ROOT / "config/sim_fleet.yaml")
    parser.add_argument("--routes", type=int, default=12)
    parser.add_argument("--buses", type=int, default=60)
    parser.add_argument("--per-route", default="", help="Overrides the allocation; comma-separated ROUTE_ID:COUNT pairs")
    args = parser.parse_args()
    network = load_network(args.network)
    fleet = choose(network, args.routes, args.buses)
    if args.per_route:
        counts = {}
        for item in args.per_route.split(","):
            route_id, raw_count = item.split(":", 1)
            counts[route_id.strip()] = int(raw_count)
        ids = {r["route_id"] for r in fleet["routes"]}
        if set(counts) != ids:
            raise SystemExit("--per-route must specify exactly the selected route ids")
        for route in fleet["routes"]:
            if counts[route["route_id"]] < 1:
                raise SystemExit("each selected route needs at least one bus")
            route["buses"] = counts[route["route_id"]]
        fleet["total_buses"] = sum(counts.values())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        handle.write(f"# Generated from mdb-2595; edit per-route buses in the admin console.\n")
        handle.write(f"total_buses: {fleet['total_buses']}\nroute_count: {fleet['route_count']}\nroutes:\n")
        for route in fleet["routes"]:
            handle.write(f"  - route_id: {json.dumps(route['route_id'])}\n")
            handle.write(f"    short_name: {json.dumps(route['short_name'], ensure_ascii=False)}\n")
            handle.write(f"    buses: {route['buses']}\n")
            handle.write(f"    directions: {json.dumps(route['directions'])}\n")
            handle.write(f"    length_m: {route['length_m']}\n")
    print(json.dumps(fleet, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
