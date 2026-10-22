# Storage layout

## Source files and runtime files

The repository separates application code under `backend/src/`, the dashboard under `frontend/`, AI model files under `models/`, the GTFS network under `data/blr/`, database migrations under `db/migrations/`, and build/configuration files under `docker/` and `config/`.

The local Python environment is `.venv/` at the project root, with packages in `.venv/Lib/site-packages` on Windows. Docker services install dependencies separately into their images at `/usr/local/lib/python3.11/site-packages`, a Linux path inside each container. Docker Desktop stores image layers and container filesystems in its managed Linux disk, outside the repository. The frontend is static HTML/CSS/JavaScript and does not need `node_modules`.

Kafka logs, TimescaleDB files, and Spark checkpoints are stored under `data/runtime/`. AI model files are stored under `models/` and mounted into the stream processor. `data/runtime/` is ignored by Git and excluded from Docker build contexts. Docker image layers and build cache live in Docker Desktop's managed Linux disk and can include assets shared with other projects.

## Inspect storage

From PowerShell in the project root:

```powershell
# Create and activate the local Python 3.11 virtual environment
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"

# Source files in this working directory
$bytes = (Get-ChildItem -File -Recurse -Force | Measure-Object Length -Sum).Sum
[math]::Round($bytes / 1MB, 2)

# Runtime data copied into the working directory
$runtimeBytes = (Get-ChildItem data/runtime -File -Recurse -Force | Measure-Object Length -Sum).Sum
[math]::Round($runtimeBytes / 1GB, 2)

# Docker storage on this machine (all projects)
docker system df
docker system df -v

# Location of a Python package inside the API container
docker compose exec -T api python -c "import pathlib, fastapi; print(pathlib.Path(fastapi.__file__).resolve())"

# Package metadata and site-packages root
docker compose exec -T api python -m pip show fastapi

# Local virtual environment package location
.\.venv\Scripts\python.exe -m pip show fastapi
```

Replace `fastapi` with an importable module to locate another package file; for example, use `yaml` for the `PyYAML` distribution. The package-location command returns a path inside the container, not a Windows path.

## Machine snapshot from 2026-10-22

Before the runtime move, the source tree measured about 57.44 MB. `docker system df` initially reported approximately 17.12 GB across the entire Docker engine: 7.74 GB of images, 2.42 GB of container writable layers, 1.65 GB of volumes, and 5.32 GB of build cache. This includes other local projects and old image/cache layers; it is not a Bus Pulse-only total.

The runtime data was copied into the project and verified. Before the folder reorganization, `data/runtime/` measured about 5.17 GiB (5.55 GB): Kafka logs about 3.68 GiB, TimescaleDB about 1.48 GiB, Spark checkpoints about 19 MiB, and model files under 1 MiB. The model artifacts are now in the top-level `models/` folder; `data/runtime/` contains Kafka, TimescaleDB, and Spark state. Compose mounts these directories directly. The detached TimescaleDB, Spark-checkpoint, and model rollback volumes have been removed. A small Kafka volume remains attached at the image's unused `/var/lib/kafka/data` path; Kafka writes to the project bind mount at `/tmp/kafka-logs`.

The local `.venv/` was created with Python 3.11.9 and the project/development dependencies installed. It currently occupies about 425 MiB and is ignored by Git.

The source code and runtime state are now together in the working directory. Docker images remain in Docker Desktop's managed Linux disk because they are engine assets, not standalone project files. After the runtime move, the unused `bus-pulse-processor:phase0` image (about 1.68 GB unique layers) and the shared builder cache (5.316 GB) were removed to reclaim space. Docker can rebuild the cache on later image builds. The current `docker system df` report includes assets from other projects and should not be read as this repository's size.

## Storage maintenance — 2026-10-22

Generated `data/runtime/` was reset after it grew to about 9.33 GB. It contained only reproducible local Kafka logs, TimescaleDB state, and Spark checkpoints; `data/blr/`, the GTFS archive, source code, `.venv/`, and model artifacts were retained. Compose then reseeded the GTFS routes and stops and the full-city simulation was restarted.

The local Compose defaults now bound Kafka storage to one hour of retention, 64 MB log segments, 1 MB index files, and three internal offset partitions. High-volume TimescaleDB hypertables use 15-minute chunks and a one-hour retention policy checked every five minutes. This reduces the initial Kafka footprint substantially and prevents a continuous demo from accumulating a multi-gigabyte log directory. Override `KAFKA_LOG_RETENTION_HOURS` and `KAFKA_TOPIC_RETENTION_MS` in `.env` only when longer replay is necessary.

Docker build cache and the unused `bus-pulse-ml-train` image were also removed. Docker Desktop's VHDX can stay physically large after cleanup because Windows does not compact it automatically; never delete `docker_data.vhdx` manually.
