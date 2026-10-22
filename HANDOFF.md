# Bus Pulse handoff

Snapshot for a developer continuing the project. This note contains no `.env` values or API keys.

## Project purpose

Bus Pulse is a Bengaluru BMTC transit demo. GTFS provides static routes, stops, and shapes; vehicle positions, passenger loads, crowding, and incidents are simulated. It is not a live BMTC tracking system.

## Current folder map

```text
backend/src/ridesense/   FastAPI, agent tools, simulator, streaming and ML code
frontend/                Static commuter and administrator dashboards
models/                  LightGBM model and metadata
data/blr/                Bengaluru route network built from GTFS
data/runtime/             Kafka logs, TimescaleDB files, Spark checkpoints
db/migrations/            Database schema migrations
docker/                   Service Dockerfiles and Kafka connector build files
config/                   Simulator and fleet configuration
scripts/                  Network builder, route selection, seeder and helpers
tests/                    Unit tests
reports/                  Model and route quality reports
.venv/                    Local Python 3.11.9 environment (Git-ignored)
```

The frontend is plain HTML/CSS/JavaScript; it has no Node package setup. Python packaging is configured in the root `pyproject.toml`. Dockerfiles and Compose mounts point at the new `backend/`, `frontend/`, and `models/` paths.

## Setup

From PowerShell at the project root:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
docker compose up --build -d
```

The local `.venv` is already present and about 425 MiB. It has the project and development dependencies installed. Spark/PySpark runs in its Docker service, which has a separate container environment.

Dashboard: <http://localhost:8000>. Local demo administrator defaults are `admin` / `admin`; use only for local demonstrations. The assistant defaults to `LLM_PROVIDER=mock`, so no LLM key is required.

## Data and model artifacts

- `data/blr/network_all.json.gz` is the screened route network used by the simulator; the source feed is MobilityDatabase GTFS feed `mdb-2595`.
- `models/occupancy_lgbm.txt` is the existing LightGBM occupancy model; `models/occupancy_lgbm.meta.json` has its report/metadata. The Spark processor reads this directory. The `ml-train` Compose profile writes updated model files here and writes its report to `reports/ml.json`.
- `data/runtime/` is persistent and fully reproducible local service state: Kafka logs, TimescaleDB files, and Spark checkpoints. It is Git-ignored. Compose can reseed routes and stops from the route network, so omit it when sharing the project unless a specific local simulation history is needed.
- `data/blr/raw/` is ignored source archive data. Route-network generation details are in `docs/DATA_SOURCES.md` and `README.md`.

## Stream and API architecture

Kafka topics created by Compose include `vehicle-telemetry`, `passenger-flow`, `occupancy-state`, `predictions`, `alerts`, `control`, `control-acks`, `dead`, and `spark-spike-state`. Spark streams occupancy windows to TimescaleDB and publishes predictions/alerts; the API serves commuter/admin pages and control actions. Passenger counts distinguish seated, standing, and handhold-support riders as simulation estimates.

## Checkpoint: current status

- The source tree was moved from `src/ridesense` to `backend/src/ridesense`; `web/` became `frontend/`; the model artifacts moved from `data/runtime/models/` to top-level `models/`.
- `.venv` was created using Python 3.11.9, and `pip install -e ".[dev]"` completed. `pip check` reported no broken requirements; imports resolved to the new frontend and model paths.
- `docker compose config -q` passes with the new paths. The Docker images have **not** been rebuilt since the move; rebuild them before relying on the reorganized paths in containers.
- At the last container inspection, the Docker Compose services were stopped. The processor log showed `BufferError: Local: Queue full` while publishing to Kafka predictions, then exited with code 137. Investigate producer backpressure/queue handling before expecting the stream processor to remain up.
- Before stopping, the API health endpoint reported the database ready and 4,434 routes. The simulator was paused with zero buses; an administrator must start a region/fleet for a live demo.
- No tests were run in this handoff. The checks above were environment/package/import and Compose-configuration checks.

## Suggested next steps

1. Inspect and fix the Kafka producer queue-full path in `backend/src/ridesense/stream/spark/main.py` (around the `write_windows` prediction publish). Add bounded retry/backpressure and ensure delivery callbacks are polled; avoid silently dropping predictions.
2. Rebuild and start Compose with `docker compose up --build -d`, then check `docker compose ps`, Kafka topics, and `http://localhost:8000/api/health`.
3. Start a region and fleet from the administrator dashboard, then confirm buses and predictions flow through Kafka to the commuter view.
4. Review the simulation and capacity assumptions in `docs/DECISIONS.md`; occupancy, seats, standees, handholds, and demand are estimates, not sensor data.

## Secrets and storage

- Do not share `.env`; it may contain local credentials and API keys. Share `.env.example` instead and create a private `.env` on the friend's machine.
- A Groq API key was previously pasted into the conversation. Rotate it if it is still active; do not put it in this handoff. Mock mode works without a key.
- The Docker WSL data disk is at `C:\Users\ravik\AppData\Local\Docker\wsl\disk\docker_data.vhdx` on this machine. It was about 17.69 GiB even after pruning 5.316 GiB of build cache and the unused `bus-pulse-processor:phase0` image (about 1.68 GiB unique layers). Windows may need the VHDX compacted to show the reclaimed space; never delete this disk file manually.
- The workspace does not currently have Git metadata (`git status` reported that this is not a Git repository). Make a clean source archive or initialize Git before sharing. Exclude `.env` and `.venv`; omit `data/runtime/` unless the friend needs that ~5.17 GiB of existing runtime state.

## Follow-up implementation checkpoint — 2026-10-22

- Advisories now show a route action that focuses its GTFS geometry on the admin map. Approving an advisory publishes a fleet update for one extra route bus and tags the created bus as `advisory_extra`; the marker flag flows through simulator telemetry, Kafka, Spark, TimescaleDB, REST, and WebSocket data. The map renders it as a purple diamond.
- Commuters can request a 1–25 bus demo for a selected simulated route. It adjusts that route within the active shared regional fleet, so both dashboards see the requested route allocation while other regional routes remain live. Isolated routes keep the two-bus minimum.
- Journey search accepts Bengaluru `latitude, longitude` pairs and resolves each to the nearest GTFS stop. Fastest and nearest-bus sort choices use the existing rough, simulated estimates.
- Admin simulation controls now offer normal, moderate, and rush-hour scenarios. These use synthetic demand multipliers and are not BMTC passenger observations.
- The dashboard header now shows region and run state without a total fleet count or an event-age value mislabeled as latency.
- Added `db/migrations/005_scenarios_and_advisory_buses.sql`; the init-data image and seeder apply it on startup. Rebuild containers before running these changes: `docker compose up --build -d`.
- Static checks passed: Python `compileall`, Node `--check`, and `docker compose config -q`. No test suite was run. A Docker container status check was blocked because this session could not access the Docker Desktop engine pipe. The earlier Spark producer `BufferError: Local: Queue full` remains a known runtime issue to inspect after startup.
