-- Durable, append-only workflow execution journal.
-- history_id is a global insertion sequence, not a transactional commit order.
CREATE TABLE IF NOT EXISTS wfc_scale.execution_history (
    history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workflow_id uuid NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    event_type text NOT NULL,
    work_item_id uuid NULL,
    execution_pointer_id text NULL,
    step_id integer NULL,
    step_attempt_id uuid NULL,
    revision bigint NULL,
    details jsonb NULL,
    CONSTRAINT ck_execution_history_type CHECK (length(event_type) BETWEEN 1 AND 100)
);
-- Keep migration rerunnable for early development deployments of this branch.
ALTER TABLE wfc_scale.execution_history
    ADD COLUMN IF NOT EXISTS occurred_at timestamptz NOT NULL DEFAULT clock_timestamp();
CREATE INDEX IF NOT EXISTS ix_execution_history_workflow
    ON wfc_scale.execution_history (workflow_id, history_id);
CREATE INDEX IF NOT EXISTS ix_execution_history_pointer
    ON wfc_scale.execution_history (workflow_id, execution_pointer_id, history_id)
    WHERE execution_pointer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_execution_history_work
    ON wfc_scale.execution_history (work_item_id, history_id)
    WHERE work_item_id IS NOT NULL;
-- No UPDATE or DELETE is performed by runtime. Restrict these privileges for
-- application roles; retention/archival must be handled by a separate role.
