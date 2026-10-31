-- Append-only execution journal. No mutable workflow state is stored here.
CREATE TABLE IF NOT EXISTS wfc_scale.execution_history (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 workflow_id uuid NOT NULL,
 revision bigint NOT NULL,
 event_index int NOT NULL,
 event_type text NOT NULL,
 pointer_id text,
 step_id int,
 attempt int,
 worker_id text,
 details jsonb NOT NULL DEFAULT '{}'::jsonb,
 occurred_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(workflow_id,revision,event_index)
);
CREATE INDEX IF NOT EXISTS ix_execution_history_workflow ON wfc_scale.execution_history(workflow_id,id);
CREATE INDEX IF NOT EXISTS ix_execution_history_event ON wfc_scale.execution_history(event_type,occurred_at);
-- The journal is intentionally insert-only for the runtime role.
