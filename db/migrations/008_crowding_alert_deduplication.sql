-- Active crowding episodes have one open alert per vehicle and alert type.
-- Resolve legacy non-crowding alerts and collapse duplicate open rows before
-- creating the partial unique index used by the streaming processor.
UPDATE alerts
SET resolved_at = COALESCE(resolved_at, now())
WHERE alert_type = 'ABNORMAL_CHANGE'
  AND resolved_at IS NULL;

WITH ranked AS (
    SELECT ctid,
           row_number() OVER (
               PARTITION BY vehicle_id, alert_type
               ORDER BY detected_at DESC, event_id DESC
           ) AS position
    FROM alerts
    WHERE resolved_at IS NULL
      AND vehicle_id IS NOT NULL
)
UPDATE alerts AS a
SET resolved_at = now()
FROM ranked AS r
WHERE a.ctid = r.ctid
  AND r.position > 1;

CREATE UNIQUE INDEX IF NOT EXISTS alerts_one_open_crowding_episode_idx
    ON alerts (vehicle_id, alert_type)
    WHERE resolved_at IS NULL
      AND vehicle_id IS NOT NULL;
