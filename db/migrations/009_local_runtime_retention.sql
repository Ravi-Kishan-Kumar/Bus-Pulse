-- Keep the high-volume synthetic stream bounded on a local development
-- machine.  The dashboard reads current buses from vehicle_latest and only
-- needs recent trend and forecast data from these hypertables.
SELECT add_retention_policy('occupancy_state', INTERVAL '1 hour',
                            schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE);
SELECT add_retention_policy('window_aggregates', INTERVAL '1 hour',
                            schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE);
SELECT add_retention_policy('route_windows', INTERVAL '1 hour',
                            schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE);
SELECT add_retention_policy('predictions', INTERVAL '1 hour',
                            schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE);
SELECT add_retention_policy('dq_metrics', INTERVAL '1 hour',
                            schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE);
