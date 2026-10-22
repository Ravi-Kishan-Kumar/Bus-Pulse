# Architecture decisions

## 2026-10-21 — Source selected for route topology

- **Decision:** use MobilityDatabase feed `mdb-2595` (BMTC Community GTFS), as requested by the project owner.
- **Use:** static route IDs, stop locations/order, and route shapes after local quality screening.
- **Do not infer:** real-time bus position, passenger occupancy, route timetables, or service availability from this feed. Those live/demo values are simulated.
- **Evidence:** catalog page lists 4,434 bus routes and shapes and links a GTFS validator report; its producer documents the source and its limitations. See `docs/DATA_SOURCES.md`.
- **Risk/mitigation:** a validator pass establishes structural validity, not field accuracy. Exclude poor geometry from simulation/map highlighting, retain failure reasons, and review the generated preview.
- **License:** producer states ODbL-1.0 with attribution/share-alike conditions and notes some BMTC content may be copyrighted. Keep source notices and seek course-staff guidance before redistribution.
- **Measured snapshot:** feed version `20260907` (44,392,885-byte zip); screening found 3,633 usable routes, 6,032 usable directions, and 8,771 covered stops. Meets the brief's network coverage floors.

## 2026-10-21 — Bus occupancy assumptions

- **Decision:** simulated buses have configurable seat, standing, and standing-support capacities. Boarding fills available seats first, then standing spaces with available rail/strap support.
- **Assumption:** vehicle capacity and handhold capacity are synthetic demo parameters, not BMTC vehicle measurements. The UI must label the resulting seated/standing split as simulated.

## 2026-10-21 — Local infrastructure image pins

- Apache Kafka image: `apache/kafka:4.1.1` (official Apache release image, KRaft).
- TimescaleDB image: `timescale/timescaledb:2.30.2-pg16` (official image tag observed in the Docker Hub tag listing).
- Stream runtime: Apache Spark / PySpark `3.5.8`, Scala connector `_2.12`, Java `17`; `applyInPandasWithState` is available in the selected Spark line. The connector dependencies are resolved at image build time and copied into Spark's jar directory.
- Compose is the orchestration interface; Kafka and database are not installed directly on Windows.

## 2026-10-21 — Phase-0 Spark spike progress

- **Passed in the pinned processor image:** Spark 3.5.8 starts with no runtime dependency download; the Kafka source provider class loads from baked connector jars; `applyInPandasWithState` feeding a Kafka batch sink runs; heartbeat-only event-time input advances the watermark and closes append windows (9 windows in the spike run).
- **Still to prove before Phase 0 exit:** deliberately late input reaches the state function and is counted; kill/restart from a persisted checkpoint resumes without gaps or duplicates. The implementation remains in Phase 0 until these checks are recorded.

## 2026-10-22 — Live simulation clock

- **Decision:** simulator event time starts at current Asia/Kolkata time by default (`SIM_START_LOCAL=now`), then advances with the simulation clock. A fixed local start such as `07:45` remains configurable for repeatable scenarios.
- **Reason:** using a fixed morning time from the previous day made a newly started demo stream appear stale and could move event-time watermarks incorrectly.
- **Checkpoint:** the corrected time basis uses `/app/checkpoints/live-v2`; previous checkpoint data is preserved rather than reset.

## 2026-10-22 — Stream and agent workflow

- Simulator publishes occupancy-state events to Kafka for every vehicle tick. Each snapshot separately reports seated, standing, handhold-supported, and unsupported standing passengers.
- Spark Structured Streaming reads Kafka, persists idempotent occupancy and latest vehicle snapshots to TimescaleDB, closes 60-second event-time windows with 15-second allowed lateness, creates a five-minute linear trend baseline, and publishes threshold/change alerts in the database.
- The commuter assistant selects bounded read-only tools for route search, route status, and least-crowded buses, then logs tool arguments and returned evidence. The operations advisor turns overcrowding/prediction alerts into pending recommendations. An administrator's approval dispatches one additional simulated bus for the advised route; the bus is tagged in telemetry and shown with a distinct map marker.
- Optional Groq mode uses model tool-calling against the same bounded read-only tool list; offline/mock mode requires no API key. The model cannot issue simulator control commands.
- A temporal training/evaluation command (`make train-ml`) fits LightGBM against persisted window labels and reports persistence/linear baselines. With 320 labeled windows, a local temporal 80/20 split measured MAE of 5.995 points for LightGBM, 10.723 for persistence, and 8.778 for linear regression (`reports/ml.json`). This is an early synthetic demo snapshot, not a generalization claim. The processor hot-reloaded the model and wrote `lightgbm-160` predictions with feature contributions. Clean installs use the transparent trend baseline until a model is trained.

## 2026-10-22 — Demo controls and advisory bus marker

- Administrator scenarios set persistent simulator demand profiles: normal follows the simulated local clock, moderate applies a synthetic demand floor, and rush hour applies the higher commute multiplier. These are reproducible demo scenarios, not observed BMTC passenger counts.
- A commuter can request a bounded 1–25 bus route-only demo. It replaces the active fleet for that shared region to avoid allocating the whole city while the commuter showcases one route. Administrator and commuter dashboards see the same route-only fleet. Isolated routes retain a minimum of two buses.
- GPS coordinates are matched to the nearest GTFS stop before single-route journey search. Fastest and nearest-bus sorting remain estimates based on the synthetic fleet and route geometry.
- The top status shows region and run state only. It omits total fleet size and the age of the last event, which had been labeled as latency and could display large stale values.
