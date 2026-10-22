#!/usr/bin/env python3
"""Idempotently load the screened GTFS extract into TimescaleDB/Postgres."""

from __future__ import annotations

import gzip
import json
import os
import time
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb
from ridesense.common.regions import REGIONS, routes_for_region


ROOT = Path(__file__).resolve().parents[1]
NETWORK_PATH = Path(os.getenv("NETWORK_PATH", ROOT / "data/blr/network_all.json.gz"))
FLEET_PATH = Path(os.getenv("FLEET_PATH", ROOT / "config/sim_fleet.yaml"))


def simulated_routes(network):
    import yaml

    fleet = yaml.safe_load(FLEET_PATH.read_text(encoding="utf-8"))
    if fleet.get("all_usable_routes"):
        return {str(route["route_id"]) for route in network["routes"]
                if route.get("usable") and any(direction.get("usable") for direction in route.get("directions", []))}
    return {str(route["route_id"]) for route in fleet["routes"]}


def connect_with_retry(url: str):
    last_error = None
    for _ in range(40):
        try:
            return psycopg.connect(url)
        except psycopg.OperationalError as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError("database was not ready after 80 seconds") from last_error


def main():
    if not NETWORK_PATH.exists():
        raise SystemExit(f"Network extract not found: {NETWORK_PATH}; run scripts/build_network.py first")
    with gzip.open(NETWORK_PATH, "rt", encoding="utf-8") as handle:
        network = json.load(handle)
    live_routes = simulated_routes(network) if FLEET_PATH.exists() else set()
    url = os.getenv("DATABASE_URL", "postgresql://ridesense:ridesense-local-only@timescaledb:5432/ridesense")
    with connect_with_retry(url) as conn:
        with conn.cursor() as cur:
            migration = ROOT / "db/migrations/002_auth.sql"
            cur.execute(migration.read_text(encoding="utf-8"))
            cur.execute((ROOT / "db/migrations/003_bus_profiles.sql").read_text(encoding="utf-8"))
            cur.execute((ROOT / "db/migrations/004_simulation_regions.sql").read_text(encoding="utf-8"))
            cur.execute((ROOT / "db/migrations/005_scenarios_and_advisory_buses.sql").read_text(encoding="utf-8"))
            cur.execute((ROOT / "db/migrations/006_route_stop_lookup_index.sql").read_text(encoding="utf-8"))
            cur.execute((ROOT / "db/migrations/007_unique_advisory_per_alert.sql").read_text(encoding="utf-8"))
            cur.execute((ROOT / "db/migrations/008_crowding_alert_deduplication.sql").read_text(encoding="utf-8"))
            cur.execute((ROOT / "db/migrations/009_local_runtime_retention.sql").read_text(encoding="utf-8"))
            cur.executemany(
                """INSERT INTO routes(route_id,short_name,long_name,usable,fail_reasons,trips_in_feed,simulated)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(route_id) DO UPDATE SET short_name=excluded.short_name,long_name=excluded.long_name,
                   usable=excluded.usable,fail_reasons=excluded.fail_reasons,trips_in_feed=excluded.trips_in_feed,
                   simulated=excluded.simulated""",
                [
                    (r["route_id"], r["short_name"], r["long_name"], r["usable"], Jsonb(r["fail_reasons"]),
                     r["trips_in_feed"], r["route_id"] in live_routes)
                    for r in network["routes"]
                ],
            )
            cur.executemany(
                """INSERT INTO stops(stop_id,name,lat,lon,hub_score) VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT(stop_id) DO UPDATE SET name=excluded.name,lat=excluded.lat,lon=excluded.lon,
                   hub_score=excluded.hub_score""",
                [(sid, s["name"], s["lat"], s["lon"], s["hub_score"]) for sid, s in network["stops"].items()],
            )
            region_rows = [(route_id, region_id)
                           for region_id in REGIONS
                           for route_id in routes_for_region(network, region_id)]
            cur.execute("DELETE FROM route_regions")
            cur.executemany("INSERT INTO route_regions(route_id,region_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                            region_rows)
            directions, route_stops = [], []
            for route in network["routes"]:
                for d in route["directions"]:
                    directions.append((route["route_id"], d["direction_id"], d["length_m"], Jsonb(d["shape"]),
                                       d["usable"], Jsonb(d["fail_reasons"])))
                    route_stops.extend((route["route_id"], d["direction_id"], s["seq"], s["stop_id"],
                                        s["progress_m"], s["offset_m"]) for s in d["stops"])
            cur.executemany(
                """INSERT INTO route_directions(route_id,direction_id,length_m,shape,usable,fail_reasons)
                   VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(route_id,direction_id) DO UPDATE SET
                   length_m=excluded.length_m,shape=excluded.shape,usable=excluded.usable,fail_reasons=excluded.fail_reasons""",
                directions,
            )
            cur.executemany(
                """INSERT INTO route_stops(route_id,direction_id,seq,stop_id,progress_m,offset_m)
                   VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(route_id,direction_id,seq) DO UPDATE SET
                   stop_id=excluded.stop_id,progress_m=excluded.progress_m,offset_m=excluded.offset_m""",
                route_stops,
            )
        conn.commit()
    print(json.dumps({"routes": len(network["routes"]), "stops": len(network["stops"]),
                      "directions": len(directions), "route_stops": len(route_stops),
                      "region_assignments": len(region_rows)}, indent=2))


if __name__ == "__main__":
    main()
