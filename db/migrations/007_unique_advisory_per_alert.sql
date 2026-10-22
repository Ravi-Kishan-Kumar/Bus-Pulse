WITH ranked AS (
    SELECT advisory_id,
           row_number() OVER (
               PARTITION BY event_id
               ORDER BY (status = 'approved') DESC, created_at ASC, advisory_id
           ) AS duplicate_rank
    FROM advisories
)
DELETE FROM advisories a
USING ranked r
WHERE a.advisory_id = r.advisory_id
  AND r.duplicate_rank > 1;

CREATE UNIQUE INDEX IF NOT EXISTS advisories_event_id_uidx
    ON advisories (event_id);
