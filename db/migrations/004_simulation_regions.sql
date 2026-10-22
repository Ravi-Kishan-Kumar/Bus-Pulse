CREATE TABLE IF NOT EXISTS route_regions (
    route_id text NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE,
    region_id text NOT NULL,
    PRIMARY KEY (route_id, region_id)
);
CREATE INDEX IF NOT EXISTS route_regions_region_idx ON route_regions(region_id, route_id);

ALTER TABLE vehicle_latest ADD COLUMN IF NOT EXISTS region_id text NOT NULL DEFAULT 'whole_city';
ALTER TABLE occupancy_state ADD COLUMN IF NOT EXISTS region_id text NOT NULL DEFAULT 'whole_city';
CREATE INDEX IF NOT EXISTS vehicle_latest_region_updated_idx ON vehicle_latest(region_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS stops_lat_lon_idx ON stops(lat, lon);

CREATE TABLE IF NOT EXISTS simulation_status (
    singleton_id smallint PRIMARY KEY DEFAULT 1 CHECK (singleton_id = 1),
    running boolean NOT NULL DEFAULT false,
    paused boolean NOT NULL DEFAULT true,
    rush_hour boolean NOT NULL DEFAULT false,
    region_id text NOT NULL DEFAULT 'whole_city',
    speedup smallint NOT NULL DEFAULT 1,
    total_buses integer NOT NULL DEFAULT 0,
    active_routes integer NOT NULL DEFAULT 0,
    last_heartbeat timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO simulation_status(singleton_id) VALUES (1) ON CONFLICT DO NOTHING;

ALTER TABLE alerts ADD COLUMN IF NOT EXISTS resolved_at timestamptz;
CREATE INDEX IF NOT EXISTS alerts_active_detected_idx ON alerts(detected_at DESC)
    WHERE acknowledged_at IS NULL AND resolved_at IS NULL;
