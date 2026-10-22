CREATE INDEX IF NOT EXISTS route_stops_stop_id_route_id_idx
    ON route_stops (stop_id, route_id);
