-- New distributed runtime data model. This runtime requires PostgreSQL.
CREATE SCHEMA IF NOT EXISTS wfc_scale;
CREATE TABLE IF NOT EXISTS wfc_scale.work_item (
    id uuid PRIMARY KEY,
    queue_name text NOT NULL,
    item_key text NOT NULL,
    item_id uuid NOT NULL,
    due_at timestamptz NOT NULL,
    state smallint NOT NULL DEFAULT 0, -- 0 ready, 1 leased, 2 complete, 3 dead
    owner text,
    epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    attempts int NOT NULL DEFAULT 0,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_work_state CHECK (state BETWEEN 0 AND 3),
    CONSTRAINT ck_work_attempts CHECK (attempts >= 0),
    UNIQUE (queue_name, item_key)
);
CREATE INDEX IF NOT EXISTS ix_work_ready ON wfc_scale.work_item
    (queue_name, due_at, id) WHERE state = 0;
CREATE INDEX IF NOT EXISTS ix_work_expired ON wfc_scale.work_item
    (queue_name, lease_until, id) WHERE state = 1;

-- Only one worker can own a workflow at any instant. Every successful state
-- mutation advances revision. The epoch fences old owners after lease transfer.
CREATE TABLE IF NOT EXISTS wfc_scale.workflow_gate (
    workflow_id uuid PRIMARY KEY,
    owner text,
    epoch bigint NOT NULL DEFAULT 0,
    revision bigint NOT NULL DEFAULT 0,
    lease_until timestamptz
);
CREATE INDEX IF NOT EXISTS ix_gate_expiry ON wfc_scale.workflow_gate (lease_until)
    WHERE owner IS NOT NULL;

-- A control-plane trigger is durable even if the process dies before a worker
-- is dispatched. Events are replayed when subscriptions are persisted.
CREATE TABLE IF NOT EXISTS wfc_scale.signal_event (
    id uuid PRIMARY KEY,
    event_name text NOT NULL,
    event_key text NOT NULL,
    payload text NOT NULL,
    effective_at timestamptz NOT NULL,
    processed boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_signal_unprocessed
  ON wfc_scale.signal_event(event_name, event_key, effective_at, id)
  WHERE processed = false;
