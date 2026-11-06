-- Additive: legacy one-owner-per-Hub matching path remains available.
-- A matching partition is a *dispatch* shard; activity permits and FIFO
-- constraints remain authoritative in existing hub_task transactions.
CREATE TABLE IF NOT EXISTS wfc_scale.hub_matching_partition_config (
    hub_id text PRIMARY KEY,
    partition_count integer NOT NULL CHECK (partition_count BETWEEN 1 AND 64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS wfc_scale.hub_matching_partition_owner (
    hub_id text NOT NULL REFERENCES wfc_scale.hub_matching_partition_config(hub_id),
    matching_partition integer NOT NULL CHECK (matching_partition BETWEEN 0 AND 63),
    owner_id text NOT NULL,
    owner_url text NOT NULL,
    epoch bigint NOT NULL DEFAULT 1 CHECK (epoch > 0),
    lease_until timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (hub_id, matching_partition)
);

CREATE INDEX IF NOT EXISTS ix_matching_partition_owner_expiry
    ON wfc_scale.hub_matching_partition_owner(lease_until);
