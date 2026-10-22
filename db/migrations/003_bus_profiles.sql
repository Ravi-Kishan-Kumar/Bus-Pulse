ALTER TABLE occupancy_state ADD COLUMN IF NOT EXISTS seat_capacity integer NOT NULL DEFAULT 40;
ALTER TABLE occupancy_state ADD COLUMN IF NOT EXISTS stand_capacity integer NOT NULL DEFAULT 30;
ALTER TABLE occupancy_state ADD COLUMN IF NOT EXISTS support_capacity integer NOT NULL DEFAULT 24;
ALTER TABLE occupancy_state ADD COLUMN IF NOT EXISTS vehicle_type text NOT NULL DEFAULT 'Standard city bus (simulation)';
ALTER TABLE occupancy_state ADD COLUMN IF NOT EXISTS capacity_range_min integer NOT NULL DEFAULT 60;
ALTER TABLE occupancy_state ADD COLUMN IF NOT EXISTS capacity_range_max integer NOT NULL DEFAULT 75;

ALTER TABLE vehicle_latest ADD COLUMN IF NOT EXISTS onboard_count integer NOT NULL DEFAULT 0;
ALTER TABLE vehicle_latest ADD COLUMN IF NOT EXISTS capacity integer NOT NULL DEFAULT 70;
ALTER TABLE vehicle_latest ADD COLUMN IF NOT EXISTS seat_capacity integer NOT NULL DEFAULT 40;
ALTER TABLE vehicle_latest ADD COLUMN IF NOT EXISTS stand_capacity integer NOT NULL DEFAULT 30;
ALTER TABLE vehicle_latest ADD COLUMN IF NOT EXISTS support_capacity integer NOT NULL DEFAULT 24;
ALTER TABLE vehicle_latest ADD COLUMN IF NOT EXISTS vehicle_type text NOT NULL DEFAULT 'Standard city bus (simulation)';
ALTER TABLE vehicle_latest ADD COLUMN IF NOT EXISTS capacity_range_min integer NOT NULL DEFAULT 60;
ALTER TABLE vehicle_latest ADD COLUMN IF NOT EXISTS capacity_range_max integer NOT NULL DEFAULT 75;
