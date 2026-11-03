-- Workflow Hub remote activity queue. Durable, independently deployable from workers.
-- Task leases themselves are active concurrency permits; all claims lock
-- resource rows so PostgreSQL serializes permit allocation across Hub replicas.
CREATE TABLE IF NOT EXISTS wfc_scale.hub_resource (
    resource_key text PRIMARY KEY,
    capacity integer NOT NULL CHECK (capacity > 0),
    strict_fifo boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS wfc_scale.hub_task (
    sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    id uuid PRIMARY KEY,
    hub_id text NOT NULL,
    partition_id integer NOT NULL CHECK (partition_id >= 0),
    task_type text NOT NULL,
    task_version integer NOT NULL CHECK (task_version > 0),
    workflow_id uuid,
    payload jsonb NOT NULL,
    state smallint NOT NULL DEFAULT 0 CHECK (state BETWEEN 0 AND 3),
    due_at timestamptz NOT NULL DEFAULT now(),
    owner text,
    last_owner text,
    epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    result_json jsonb,
    last_error text,
    request_key text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_hub_task_lease CHECK (state <> 1 OR (owner IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_hub_task_request
    ON wfc_scale.hub_task(hub_id, request_key) WHERE request_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_hub_task_ready
    ON wfc_scale.hub_task(hub_id, due_at, sequence) WHERE state = 0;
CREATE INDEX IF NOT EXISTS ix_hub_task_expired
    ON wfc_scale.hub_task(hub_id, lease_until, sequence) WHERE state = 1;
CREATE TABLE IF NOT EXISTS wfc_scale.hub_task_constraint (
    task_id uuid NOT NULL REFERENCES wfc_scale.hub_task(id) ON DELETE CASCADE,
    resource_key text NOT NULL REFERENCES wfc_scale.hub_resource(resource_key),
    PRIMARY KEY (task_id, resource_key)
);
CREATE INDEX IF NOT EXISTS ix_hub_constraint_resource
    ON wfc_scale.hub_task_constraint(resource_key, task_id);
-- Strict ordering is by hub_task.sequence (monotonic database sequence).
-- It is an admission order, not a promise about external side effect completion
-- when leases expire and an unresponsive worker continues running.
