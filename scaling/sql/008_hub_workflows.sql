-- Versioned durable workflow orchestration for Hub-owned remote activity chains.
-- This is deliberately separate from the legacy Workflow Core state tables.
CREATE TABLE IF NOT EXISTS wfc_scale.hub_workflow (
    id uuid PRIMARY KEY,
    hub_id text NOT NULL,
    definition_id text NOT NULL,
    definition_version integer NOT NULL CHECK (definition_version > 0),
    partition_id integer NOT NULL CHECK (partition_id >= 0),
    run_id text,
    input_json jsonb NOT NULL,
    results_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    state smallint NOT NULL DEFAULT 0 CHECK (state BETWEEN 0 AND 3),
    next_step integer NOT NULL DEFAULT 0,
    step_count integer NOT NULL CHECK (step_count > 0),
    idempotency_key text,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_hub_workflow_idempotency
    ON wfc_scale.hub_workflow (hub_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_hub_workflow_run
    ON wfc_scale.hub_workflow(run_id, state, created_at)
    WHERE run_id IS NOT NULL;
ALTER TABLE wfc_scale.hub_task
    ADD COLUMN IF NOT EXISTS workflow_step integer;
CREATE UNIQUE INDEX IF NOT EXISTS ux_hub_task_workflow_step
    ON wfc_scale.hub_task(workflow_id, workflow_step)
    WHERE workflow_id IS NOT NULL AND workflow_step IS NOT NULL;
CREATE TABLE IF NOT EXISTS wfc_scale.hub_workflow_history (
    workflow_id uuid NOT NULL REFERENCES wfc_scale.hub_workflow(id),
    step_index integer NOT NULL,
    task_id uuid NOT NULL REFERENCES wfc_scale.hub_task(id),
    event_type text NOT NULL,
    payload jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(workflow_id, task_id, event_type)
);
CREATE INDEX IF NOT EXISTS ix_hub_workflow_history
    ON wfc_scale.hub_workflow_history(workflow_id, occurred_at, step_index);
