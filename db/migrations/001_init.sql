CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS routes (
    route_id text PRIMARY KEY,
    short_name text NOT NULL DEFAULT '',
    long_name text NOT NULL DEFAULT '',
    usable boolean NOT NULL DEFAULT false,
    fail_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
    trips_in_feed integer NOT NULL DEFAULT 0,
    simulated boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS stops (
    stop_id text PRIMARY KEY,
    name text NOT NULL DEFAULT '',
    lat double precision NOT NULL,
    lon double precision NOT NULL,
    hub_score integer NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS route_directions (
    route_id text NOT NULL REFERENCES routes(route_id),
    direction_id integer NOT NULL,
    length_m double precision NOT NULL,
    shape jsonb NOT NULL,
    usable boolean NOT NULL DEFAULT false,
    fail_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (route_id, direction_id)
);

CREATE TABLE IF NOT EXISTS route_stops (
    route_id text NOT NULL,
    direction_id integer NOT NULL,
    seq integer NOT NULL,
    stop_id text NOT NULL REFERENCES stops(stop_id),
    progress_m double precision NOT NULL,
    offset_m double precision NOT NULL,
    PRIMARY KEY (route_id, direction_id, seq),
    FOREIGN KEY (route_id, direction_id) REFERENCES route_directions(route_id, direction_id)
);

CREATE INDEX IF NOT EXISTS route_stops_stop_id_route_id_idx
    ON route_stops (stop_id, route_id);

CREATE TABLE IF NOT EXISTS vehicle_latest (
    vehicle_id text PRIMARY KEY,
    route_id text NOT NULL,
    direction_id integer NOT NULL,
    event_ts timestamptz NOT NULL,
    lat double precision NOT NULL,
    lon double precision NOT NULL,
    progress_m double precision NOT NULL,
    speed_mps double precision NOT NULL,
    occ_est_pct double precision NOT NULL,
    seated_count integer NOT NULL DEFAULT 0,
    standing_count integer NOT NULL DEFAULT 0,
    supported_standing_count integer NOT NULL DEFAULT 0,
    delay_s integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS occupancy_state (
    event_ts timestamptz NOT NULL,
    event_id text NOT NULL,
    vehicle_id text NOT NULL,
    route_id text NOT NULL,
    direction_id integer NOT NULL,
    lat double precision,
    lon double precision,
    progress_m double precision,
    onboard_count integer,
    seated_count integer,
    standing_count integer,
    supported_standing_count integer,
    capacity integer,
    occ_est_pct double precision,
    apc_missing boolean NOT NULL DEFAULT false,
    PRIMARY KEY (event_ts, event_id)
);
SELECT create_hypertable('occupancy_state', 'event_ts', chunk_time_interval => INTERVAL '15 minutes', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS window_aggregates (
    window_start timestamptz NOT NULL,
    vehicle_id text NOT NULL,
    route_id text NOT NULL,
    occ_mean double precision NOT NULL,
    occ_max double precision NOT NULL,
    n_events integer NOT NULL,
    PRIMARY KEY (vehicle_id, window_start)
);
SELECT create_hypertable('window_aggregates', 'window_start', chunk_time_interval => INTERVAL '15 minutes', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS route_windows (
    window_start timestamptz NOT NULL,
    route_id text NOT NULL,
    occ_mean double precision NOT NULL,
    occ_max double precision NOT NULL,
    n_buses integer NOT NULL,
    PRIMARY KEY (route_id, window_start)
);
SELECT create_hypertable('route_windows', 'window_start', chunk_time_interval => INTERVAL '15 minutes', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS predictions (
    made_at timestamptz NOT NULL,
    vehicle_id text NOT NULL,
    route_id text NOT NULL,
    target text NOT NULL DEFAULT 'plus_5m',
    pred_pct double precision NOT NULL,
    model_version text NOT NULL,
    contributions jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (vehicle_id, made_at, target)
);
SELECT create_hypertable('predictions', 'made_at', chunk_time_interval => INTERVAL '15 minutes', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS alerts (
    event_id text PRIMARY KEY,
    alert_type text NOT NULL,
    severity text NOT NULL,
    vehicle_id text,
    route_id text NOT NULL,
    detected_at timestamptz NOT NULL,
    window_starts jsonb NOT NULL DEFAULT '[]'::jsonb,
    occ_pct double precision,
    pred_pct double precision,
    delay_s integer,
    threshold double precision NOT NULL,
    acknowledged_at timestamptz
);

CREATE TABLE IF NOT EXISTS dq_metrics (
    ts timestamptz NOT NULL,
    query_name text NOT NULL,
    batch_id bigint NOT NULL,
    events_in bigint NOT NULL,
    malformed bigint NOT NULL,
    duplicates bigint NOT NULL,
    late_dropped bigint NOT NULL,
    apc_missing bigint NOT NULL,
    ooo_applied bigint NOT NULL,
    lateness_p95_s double precision NOT NULL,
    rows_dropped_by_watermark bigint NOT NULL,
    throughput_eps double precision NOT NULL,
    PRIMARY KEY (ts, query_name, batch_id)
);
SELECT create_hypertable('dq_metrics', 'ts', chunk_time_interval => INTERVAL '15 minutes', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS advisories (
    advisory_id text PRIMARY KEY,
    event_id text NOT NULL REFERENCES alerts(event_id),
    route_id text NOT NULL,
    action text NOT NULL,
    rationale text NOT NULL,
    evidence_ids jsonb NOT NULL,
    source text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS agent_traces (
    call_id text PRIMARY KEY,
    trace_id text NOT NULL,
    session_id text NOT NULL,
    agent text NOT NULL,
    tool text NOT NULL,
    args jsonb NOT NULL,
    result jsonb NOT NULL,
    as_of_ts timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
