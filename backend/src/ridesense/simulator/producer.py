"""Kafka producer loop and live control-command consumer for the simulator."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import signal
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from confluent_kafka import Consumer, Producer
import psycopg

from ridesense.common.schemas import ControlCommand, OccupancyState
from ridesense.simulator.fleet import FleetManager

LOG = logging.getLogger("ridesense.simulator")
IST = ZoneInfo("Asia/Kolkata")


def load_json_gzip(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


class SimulatorRunner:
    def __init__(self):
        network_path = Path(os.getenv("NETWORK_PATH", "/app/data/blr/network_all.json.gz"))
        fleet_path = Path(os.getenv("FLEET_CONFIG", "/app/config/sim_fleet.yaml"))
        settings_path = Path(os.getenv("SIMULATOR_CONFIG", "/app/config/simulator.yaml"))
        if not network_path.exists():
            network_path = Path("data/blr/network_all.json.gz")
        network = load_json_gzip(network_path)
        fleet_config = yaml.safe_load(fleet_path.read_text(encoding="utf-8"))
        settings = yaml.safe_load(settings_path.read_text(encoding="utf-8")) if settings_path.exists() else {}
        os.environ.setdefault("DEMAND_SCALE", str(settings.get("demand_scale", 1.0)))
        self.beat_seconds = float(os.getenv("BEAT_S", settings.get("beat_seconds", 30)))
        self.speedup = int(os.getenv("SIM_SPEEDUP", settings.get("speedup", 1)))
        self.max_buses = int(os.getenv("MAX_SIM_BUSES", "4000"))
        self.database_url = os.getenv("DATABASE_URL", "postgresql://ridesense:ridesense-local-only@timescaledb:5432/ridesense")
        self.fleet = FleetManager(
            network, fleet_config, seed=int(os.getenv("SEED", settings.get("seed", 42))), max_buses=self.max_buses,
            capacity=int(os.getenv("BUS_CAPACITY", settings.get("capacity_per_bus", 70))),
            seat_capacity=int(os.getenv("SEAT_CAPACITY", settings.get("seat_capacity_per_bus", 40))),
            stand_capacity=int(os.getenv("STAND_CAPACITY", settings.get("standee_capacity_per_bus", 30))),
            support_capacity=int(os.getenv("HANDHOLD_CAPACITY", settings.get("standing_support_capacity_per_bus", 30))),
            vehicle_profiles=settings.get("vehicle_profiles"),
        )
        self.producer = Producer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            "client.id": "bus-pulse-simulator",
            "enable.idempotence": True,
            "acks": "all",
        })
        self.control = Consumer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            "group.id": "bus-pulse-simulator-control-v1",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self.control.subscribe(["control"])
        now = datetime.now(IST)
        start = os.getenv("SIM_START_LOCAL", settings.get("start_local", "now")).strip().lower()
        if start == "now":
            self.event_time = now.replace(microsecond=0)
        else:
            hour, minute = map(int, start.split(":"))
            self.event_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if self.event_time > now:
                self.event_time -= timedelta(days=1)
        self.running = True
        self.force_tick = False
        self.fleet.speedup = self.speedup

    def _send(self, topic: str, key: str, model):
        self.producer.produce(topic, key=key, value=model.model_dump_json().encode("utf-8"))
        self.producer.poll(0)

    def _ack(self, command: ControlCommand, status: str, error: str | None = None):
        fleet_status = self.fleet.status()
        active_routes = len(fleet_status.pop("route_bus_counts", {}))
        fleet_status["active_routes"] = active_routes
        ack = {
            "schema_version": 1, "command_id": command.command_id, "applied": error is None,
            "status": status, "error": error, "acknowledged_at": datetime.now(IST).isoformat(),
            "fleet": fleet_status,
        }
        self.producer.produce("control-acks", key=command.command_id, value=json.dumps(ack).encode())
        self.producer.poll(0)

    def _handle_controls(self):
        while True:
            message = self.control.poll(0)
            if message is None:
                return
            if message.error():
                LOG.warning("Control consumer error: %s", message.error())
                return
            try:
                command = ControlCommand.model_validate_json(message.value())
                if command.action == "set_speed" and command.speedup is not None:
                    self.speedup = command.speedup
                status = self.fleet.apply_command(command)
                self._ack(command, "applied")
                self._write_status()
                if command.action in {"set_fleet", "route_demo", "start", "resume", "rush_hour", "normal_hours", "moderate_hours"}:
                    self.force_tick = True
                LOG.info("Applied control %s: %s", command.command_id, status)
            except Exception as exc:
                LOG.exception("Rejected simulator control command")
                try:
                    fallback = ControlCommand.model_validate_json(message.value())
                    self._ack(fallback, "rejected", str(exc))
                except Exception:
                    LOG.warning("Could not acknowledge malformed control command")

    def run(self):
        fleet = self.fleet.status()
        LOG.info("Starting synthetic fleet: running=%s buses=%s routes=%s max_buses=%s",
                 fleet["running"], fleet["total_buses"], len(fleet["route_bus_counts"]), fleet["max_buses"])
        next_tick = time.monotonic()
        last_status = 0.0
        while self.running:
            self._handle_controls()
            if self.force_tick:
                next_tick = 0
                self.force_tick = False
            now_monotonic = time.monotonic()
            if now_monotonic >= next_tick:
                produced_ms = int(time.time() * 1000)
                ticks = self.fleet.tick(self.event_time, self.beat_seconds, produced_ms)
                for tick in ticks:
                    self._send("vehicle-telemetry", tick.telemetry.vehicle_id, tick.telemetry)
                    state = tick.occupancy
                    occupancy = OccupancyState(
                        event_id=hashlib.sha256(f"{tick.telemetry.event_id}|occupancy".encode()).hexdigest(),
                        vehicle_id=tick.telemetry.vehicle_id, route_id=tick.telemetry.route_id,
                        region_id=tick.telemetry.region_id, direction_id=tick.telemetry.direction_id,
                        event_ts=tick.telemetry.event_ts, produced_ts_ms=produced_ms,
                        lat=tick.telemetry.lat, lon=tick.telemetry.lon,
                        progress_m=tick.telemetry.progress_m, speed_mps=tick.telemetry.speed_mps,
                        capacity=state["capacity"], seat_capacity=state["seat_capacity"],
                        stand_capacity=state["stand_capacity"], support_capacity=state["standing_support_capacity"],
                        vehicle_type=tick.telemetry.vehicle_type,
                        capacity_range_min=tick.telemetry.capacity_range_min,
                        capacity_range_max=tick.telemetry.capacity_range_max,
                        advisory_extra=tick.telemetry.advisory_extra,
                        onboard_count=state["onboard_count"], seated_count=state["seated_count"],
                        standing_count=state["standing_count"],
                        supported_standing_count=state["supported_standing_count"],
                        unsupported_standing_count=state["unsupported_standing_count"],
                        occ_est_pct=state["occ_est_pct"],
                    )
                    self._send("occupancy-state", occupancy.vehicle_id, occupancy)
                    if tick.passenger_flow is not None:
                        self._send("passenger-flow", tick.passenger_flow.vehicle_id, tick.passenger_flow)
                self.producer.flush(5)
                self.event_time += timedelta(seconds=self.beat_seconds)
                next_tick = now_monotonic + self.beat_seconds / max(1, self.fleet.speedup)
            if now_monotonic - last_status >= 10:
                self._write_status()
                last_status = now_monotonic
            time.sleep(0.5)
        self.control.close()
        self.producer.flush(10)

    def _write_status(self):
        status = self.fleet.status()
        with psycopg.connect(self.database_url) as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO simulation_status(singleton_id,running,paused,rush_hour,demand_profile,region_id,speedup,
                   total_buses,active_routes,last_heartbeat,updated_at)
                   VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())
                   ON CONFLICT (singleton_id) DO UPDATE SET running=EXCLUDED.running,paused=EXCLUDED.paused,
                   rush_hour=EXCLUDED.rush_hour,demand_profile=EXCLUDED.demand_profile,
                   region_id=EXCLUDED.region_id,speedup=EXCLUDED.speedup,
                   total_buses=EXCLUDED.total_buses,active_routes=EXCLUDED.active_routes,
                   last_heartbeat=EXCLUDED.last_heartbeat,updated_at=EXCLUDED.updated_at""",
                (status["running"], status["paused"], status["rush_hour"], status["demand_profile"], status["region_id"],
                 status["speedup"], status["total_buses"], status["active_routes"]),
            )


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    runner = SimulatorRunner()
    signal.signal(signal.SIGTERM, lambda *_: setattr(runner, "running", False))
    signal.signal(signal.SIGINT, lambda *_: setattr(runner, "running", False))
    runner.run()


if __name__ == "__main__":
    main()
