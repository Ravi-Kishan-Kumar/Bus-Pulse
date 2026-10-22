ALTER TABLE simulation_status
    ADD COLUMN IF NOT EXISTS demand_profile text NOT NULL DEFAULT 'normal';

ALTER TABLE vehicle_latest
    ADD COLUMN IF NOT EXISTS advisory_extra boolean NOT NULL DEFAULT false;

ALTER TABLE occupancy_state
    ADD COLUMN IF NOT EXISTS advisory_extra boolean NOT NULL DEFAULT false;
