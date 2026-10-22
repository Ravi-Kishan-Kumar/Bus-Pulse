CREATE TABLE IF NOT EXISTS commuter_users (
    username text PRIMARY KEY,
    display_name text NOT NULL,
    password_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
