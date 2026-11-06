-- Durable owner leases for *matching control*, not for workflow execution.
-- Worker task leases, FIFO permits and fencing remain in hub_task.
CREATE TABLE IF NOT EXISTS wfc_scale.hub_matching_owner (
    hub_id text PRIMARY KEY,
    owner_id text NOT NULL,
    owner_url text NOT NULL,
    epoch bigint NOT NULL DEFAULT 1 CHECK (epoch > 0),
    lease_until timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_hub_matching_owner_expiry
    ON wfc_scale.hub_matching_owner (lease_until);
