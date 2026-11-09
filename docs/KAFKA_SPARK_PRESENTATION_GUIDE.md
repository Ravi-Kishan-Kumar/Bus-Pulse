# Kafka and Spark Presentation Guide

Use this file to explain the Stream Processing and Analytics part of Bus Pulse.
It describes the implementation in this repository. Bus positions and passenger
counts are simulated; BMTC GTFS data supplies the static route and stop network.

## One-minute explanation

> Bus Pulse is a real-time transit analytics prototype. A simulator moves
> synthetic buses across real Bengaluru GTFS route geometry and publishes
> telemetry and occupancy events to Apache Kafka. Spark Structured Streaming
> consumes the occupancy stream, validates the required fields, writes the
> latest state to TimescaleDB, creates event-time occupancy windows, predicts
> occupancy five minutes ahead, and raises crowding alerts. The FastAPI
> dashboard reads the processed data from TimescaleDB and shows it to commuters
> and administrators.

The important idea is that Kafka decouples the data producer from the analytics
engine. The simulator can keep producing records while Spark consumes and
processes them independently.

## Architecture to draw or explain

~~~text
BMTC GTFS static data                 Synthetic simulator
routes, stops, shapes                position + passenger events
        |                                      |
        +--------------------------------------+
                                               v
                                    Apache Kafka, KRaft mode
                   +-------------------+-------------------+
                   |                   |                   |
          vehicle-telemetry     passenger-flow      occupancy-state
                   |                   |                   |
                   |                   |                   v
                   |                   |        Spark Structured Streaming
                   |                   |            |              |
                   |                   |     current state     60-second windows
                   |                   |            |              |
                   |                   |            v              v
                   |                   |       TimescaleDB   predictions and alerts
                   |                   |            |              |
                   +-------------------+------------+--------------+
                                                        |
                                                        v
                                        FastAPI + WebSocket + Map dashboard
~~~

## 1. Kafka setup

### What Kafka does in this project

Kafka is the event broker between the simulator and the processing services.
Instead of the simulator calling the database or dashboard directly, it writes
events to named Kafka topics. Different services can consume those events
without tightly coupling to the simulator.

The Kafka service is defined in docker-compose.yml.

| Setting | Current value | Presentation meaning |
|---|---:|---|
| Kafka version | Apache Kafka 4.1.1 | The broker used for event transport. |
| Mode | KRaft | Kafka runs without ZooKeeper. The same local node acts as broker and controller. |
| Broker nodes | 1 | Suitable for a laptop demo; it does not demonstrate broker failover. |
| Bootstrap address inside Docker | kafka:9092 | Other containers use the Docker service name to connect. |
| Host port | 9092 | Available from the Windows host for inspection if needed. |
| Main-topic partitions | 3 | Allows records keyed by vehicle ID to be distributed across partitions. |
| Replication factor | 1 | One local copy only, appropriate for the single-broker demo. |
| Default retention | 1 hour | Limits disk use during repeated simulation demonstrations. |
| Kafka storage | data/runtime/kafka | Persisted local runtime data; ignored by Git. |

### Why KRaft was selected

Older Kafka installations required ZooKeeper for metadata coordination. KRaft is
Kafka's built-in metadata mode, so the laptop demo uses fewer containers and is
easier to explain:

> We use a single-node KRaft broker. It is sufficient to demonstrate topics,
> partitions, consumer groups, offsets, and stream processing. It is not a
> production high-availability configuration because replication factor one
> means there is no second broker to take over.

### Topic setup

The one-time init-kafka container waits until Kafka is healthy, creates the
topics, prints their names, then exits with code 0. That exit is successful
initialization, not a failure.

| Topic | Partitions | Producer | Consumer or purpose |
|---|---:|---|---|
| vehicle-telemetry | 3 | Simulator | Raw vehicle location, speed, delay, and heartbeat events. |
| passenger-flow | 3 | Simulator | Boarding and alighting at simulated stops. |
| occupancy-state | 3 | Simulator | Current synthetic occupancy per bus. Spark consumes this topic in the current implementation. |
| predictions | 3 | Spark processor | Five-minute occupancy forecast events. |
| alerts | 3 | Spark processor | Crowding and predicted-crowding events. |
| dead | 1 | Reserved diagnostics path | Malformed, late, or duplicate event diagnostics. |
| control | 1 | API / admin dashboard | Start, pause, reset, scenario, and fleet commands. |
| control-acks | 1 | Simulator | Acknowledgement of simulator control commands. |
| spark-spike-state | 1 | Phase-0 utility | Used for Spark feasibility experiments. |

### Partition keys

The simulator uses vehicle_id as the key for vehicle, passenger-flow, and
occupancy messages. Kafka sends records with the same key to the same partition.
This matters because each bus is an independent entity whose events need a
consistent order for stateful analysis.

For presentation:

> We partition bus events by vehicle ID. This gives one bus a stable Kafka
> partition, which helps preserve the event order needed to calculate its
> occupancy state.

### Kafka message example

An occupancy event carries fields similar to the following:

~~~json
{
  "event_id": "unique-event-id",
  "vehicle_id": "V104-01",
  "route_id": "104",
  "event_ts": "2026-11-02T14:30:00Z",
  "lat": 12.9716,
  "lon": 77.5946,
  "onboard_count": 46,
  "seated_count": 40,
  "standing_count": 6,
  "supported_standing_count": 6,
  "capacity": 70,
  "occ_est_pct": 65.7
}
~~~

The values above illustrate the message shape. In Bus Pulse, position and
occupancy values are generated by the simulator, not read from a live BMTC
system.

## 2. Spark Structured Streaming setup

### Runtime and configuration

The processor service runs the stream processor in its own Docker container.
It uses Spark 3.5.8, Java 17, Python 3.11, the Kafka connector jars, and
PySpark. These dependencies are baked into docker/processor.Dockerfile so the
processor does not need to download them every time it starts.

| Setting | Current value | Why it matters |
|---|---:|---|
| Spark master | local[4] | Uses up to four local execution threads on the laptop. |
| Trigger interval | 1 second | Spark checks for new data approximately once per second. |
| Shuffle partitions | 3 | Matches the main Kafka-topic partition count for this small demo. |
| Window size | 60 seconds | Computes one occupancy summary per bus per minute. |
| Allowed lateness | 15 seconds | Keeps a window open briefly for late event-time records. |
| Crowding threshold | 85 percent | Used by crowding and forecast alert rules. |
| Checkpoints | data/runtime/spark-checkpoints | Stores streaming progress and state so Spark can recover after restart. |
| Output database | TimescaleDB | Holds current state, window aggregates, predictions, alerts, and metrics. |

### What Spark reads

Spark reads the Kafka occupancy-state topic using the Docker address kafka:9092.
It starts from latest offsets when a new checkpoint does not exist. Each
message is parsed from JSON into an explicit Spark schema.

The required fields are checked before downstream processing:

- event_id
- vehicle_id
- route_id
- event_ts

This protects the pipeline from records that cannot be associated with a bus,
route, or time.

### The two streaming queries

The processor builds two queries from the validated occupancy stream.

#### Query 1: Current occupancy state

The first query runs a foreachBatch database write. For every micro-batch it:

1. Inserts occupancy events into occupancy_state.
2. Updates vehicle_latest, which stores the newest known simulated state per
   vehicle.
3. Uses event_ts and event_id as a unique key so replayed records do not
   create duplicate occupancy rows.
4. Writes data-quality counters such as duplicate counts, lateness, and
   throughput.

This is the fast path used by the dashboard to show current bus markers.

#### Query 2: Window analytics, prediction, and alerts

The second query applies a watermark and creates a 60-second tumbling window:

~~~text
group by window(event_ts, 60 seconds), vehicle_id, route_id
calculate avg(occ_est_pct), max(occ_est_pct), count(events)
~~~

At window completion it:

1. Writes a per-vehicle summary to window_aggregates.
2. Calculates a route-level aggregate from occupancy records in the same
   interval and writes it to route_windows.
3. Builds features from current occupancy, the previous window, route context,
   and time.
4. Uses the LightGBM model when a trained file is present. Otherwise, it uses
   the transparent trend baseline.
5. Stores the five-minute prediction in predictions.
6. Emits a prediction message to the Kafka predictions topic.
7. Evaluates crowding rules and stores/emits any resulting alert.

The processor skips an empty route-window database write. This avoids trying to
insert a null average when historical Kafka events have no matching retained
occupancy record.

### Why there are two paths

The map needs recent vehicle state as quickly as possible. Forecasts and alerts
need a complete time window, so they naturally arrive later.

> We separate the fast path from the analytics path. The current bus marker is
> based on the latest event. The forecast is based on a completed 60-second
> event-time window plus the lateness allowance. Therefore, a forecast should
> not be expected to update at the same instant as the map marker.

### Watermarking and late data

The processor uses a 15-second watermark on event time. Event time is when the
bus observation occurred, which can differ from when it reaches the server.
The watermark lets Spark wait briefly for delayed records without keeping every
old window in memory.

For presentation:

> Event time tells us when the bus observation happened. A watermark lets us
> wait briefly for delayed records while giving Spark a point at which old
> windows can be finalized and state can be released.

## 3. Storage and recovery

Kafka and Spark state are stored under data/runtime:

| Path | Stores |
|---|---|
| data/runtime/kafka | Kafka topic logs and offsets. |
| data/runtime/spark-checkpoints | Spark query progress and state checkpoints. |
| data/runtime/timescaledb | TimescaleDB database files. |

The folders are intentionally excluded from Git because they are generated
runtime state. Kafka uses one-hour retention and small segments; TimescaleDB
also has short retention policies for high-volume demo tables. This keeps a
continuous local simulation from using unlimited disk space.

When Spark restarts, it reads its checkpoint to determine what has already
been processed. Kafka retains recent events so the stream can continue from
stored offsets. Database upserts and unique keys reduce the impact of a
replayed micro-batch.

## 4. What happens when the project starts

Run:

~~~powershell
docker compose up --build
~~~

Docker Compose starts services in the following dependency order:

1. Kafka starts and its health check confirms the broker accepts requests.
2. TimescaleDB starts and becomes healthy.
3. init-kafka creates Kafka topics and exits successfully.
4. init-data applies database setup and loads GTFS network information.
5. simulator, processor, and api start after the dependencies are ready.
6. Open http://localhost:8000. The frontend is static HTML, CSS, and
   JavaScript served by the API container.
7. An administrator selects a region and starts the simulation. The simulator
   begins publishing events; Spark begins processing them.

The simulator intentionally starts paused. A log such as
running=False buses=0 routes=0 is expected before the administrator starts a
regional simulation.

## 5. Explaining common logs

| Log message | Meaning | What to do |
|---|---|---|
| init-kafka exited with code 0 | Topics were created successfully. | No action needed. |
| init-data exited with code 0 | Database and network initialization completed. | No action needed. |
| Kafka Server started | Kafka is ready. | No action needed. |
| Coordinator load in progress: retrying | A client connected while Kafka was finishing startup. | Usually resolves automatically in seconds. |
| NativeCodeLoader warning | Spark cannot use an optional native Hadoop library. | Normal for this local prototype. |
| spark.sql.adaptive.enabled disabled | Adaptive execution is not used for streaming queries. | Normal Spark behavior. |
| Current batch is falling behind | A Spark micro-batch took longer than the one-second trigger. | Let startup catch up; reduce fleet size or increase trigger interval if persistent. |
| NotNullViolation for occ_mean | Old issue caused by an empty route-window aggregate. | Rebuild after the route-window guard update. |

## 6. Short demonstration script

Use this when explaining the running application.

1. “The real part of the data is the BMTC GTFS network: routes, stops, and
   shapes. Bus movement and passengers are synthetic because this feed has no
   live occupancy data.”
2. “The simulator acts like a fleet of bus sensors. It publishes an event for
   each active bus with location, occupancy, and passenger information.”
3. “Kafka is the event backbone. It separates the simulator from the Spark
   processor and organizes data into topics and partitions.”
4. “Spark consumes occupancy-state, maintains latest bus state, then creates
   60-second event-time windows.”
5. “Spark writes the current state, aggregates, predictions, and alerts into
   TimescaleDB. The map reads processed data through FastAPI.”
6. “The map marker is the fast path. The prediction is delayed until the window
   closes because it is calculated from a complete interval.”
7. “When the estimated crowding crosses configured conditions, the system
   creates an alert. The administrator can inspect it and approve an extra
   simulated bus; no real vehicle is dispatched.”

## 7. Likely viva questions and concise answers

### Why use Kafka instead of calling the database directly?

Kafka acts as a durable event buffer between producers and consumers. It lets
the simulator, Spark processor, API, and future consumers evolve independently.
It also supports partitions, offsets, replay within retention, and consumer
groups.

### Why use Spark if the simulator already calculates occupancy?

The simulator generates observations. Spark is the analytics layer: it
validates data, stores current processed state, groups observations by event
time, produces aggregates, performs prediction, and applies crowding rules.
Separating generation and analytics makes the pipeline easier to test and
resembles a real sensor-to-analytics architecture.

### Is this real-time?

It is near-real-time streaming in a local demonstration environment. The map
updates from incoming simulated events. The analytical path uses one-second
micro-batch triggers and 60-second windows, so predictions and alerts are
delayed until sufficient event-time data is present.

### What is the difference between a topic and a partition?

A topic is a named event stream such as occupancy-state. A partition is one
ordered shard of that topic. This project uses three partitions for main
vehicle topics so buses can be distributed by vehicle_id.

### What makes the processing reliable?

Spark checkpoints record processing progress. Kafka retains recent records.
The database uses unique keys and upserts for important outputs, helping a
restarted query avoid duplicate stored results. The single local Kafka broker
does not provide broker-level high availability.

### Why is there a watermark?

Events can arrive later than their event timestamp. The watermark allows a
short delay for late data while giving Spark a point at which old windows can
be finalized and state can be released.

### Is the LightGBM accuracy a real BMTC result?

No. The available model evaluation is on synthetic data generated by this
prototype. It demonstrates the training and comparison workflow only. Real
accuracy needs permitted real-world occupancy and vehicle-location data.

## 8. Commands useful during the demo

~~~powershell
# Start every container and show logs
docker compose up --build

# In another terminal, confirm services and their state
docker compose ps

# Follow the Spark processor only
docker compose logs -f processor

# Follow Kafka only
docker compose logs -f kafka

# List topics from the Kafka container
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# Stop all containers when the demonstration is over
docker compose down
~~~

## 9. Honest limitations to state

- The current deployment uses one Kafka broker with replication factor one.
- GTFS data is static topology, not live BMTC bus location or passenger-count
  data.
- Passenger load, crowding, capacity, delays, incidents, and demand are
  simulation estimates.
- The configured one-second trigger is a target, not a guarantee; Spark may
  take longer during startup or when catching up on retained Kafka events.
- The early LightGBM comparison is based on synthetic windows and needs a
  larger evaluation before making real-world accuracy claims.
- Docker and Spark can use noticeable CPU and RAM on a laptop.

## 10. Files to mention if asked for implementation evidence

| File | What it demonstrates |
|---|---|
| docker-compose.yml | Kafka, Spark processor, database, simulator, dependency order, and runtime settings. |
| backend/src/ridesense/simulator/producer.py | Event production and simulator control consumer. |
| backend/src/ridesense/stream/spark/main.py | Kafka ingestion, micro-batch writes, windows, predictions, and alerts. |
| docker/processor.Dockerfile | Spark, Java, Python, Kafka connector, and model dependencies. |
| config/pipeline.yaml | Streaming window, lateness, threshold, and Spark configuration. |
| docs/DATA_SOURCES.md | GTFS provenance and the real-versus-synthetic data boundary. |
| docs/DECISIONS.md | Architecture decisions, versions, and known limitations. |
