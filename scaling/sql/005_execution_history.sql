-- Append-only execution journal. Revision identifies the state snapshot being observed;
-- multiple attempts can occur at the same revision, so (revision,event_index) is
-- intentionally not a unique key. The monotonic id is the timeline cursor.
CREATE TABLE IF NOT EXISTS wfc_scale.execution_history (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workflow_id uuid NOT NULL,
    revision bigint NOT NULL,
    event_index int NOT NULL DEFAULT 0,
    event_type text NOT NULL,
    event_key text,
    pointer_id text,
    step_id int,
    attempt int,
    worker_id text,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_history_event_type CHECK (length(event_type) BETWEEN 1 AND 80),
    CONSTRAINT ck_history_details_size CHECK (octet_length(details::text) <= 8192)
);
-- Older copies of this feature branch had a uniqueness constraint that
-- incorrectly ruled out retries at the same workflow state revision.
ALTER TABLE wfc_scale.execution_history
    DROP CONSTRAINT IF EXISTS execution_history_workflow_id_revision_event_index_key;
ALTER TABLE wfc_scale.execution_history
    ADD COLUMN IF NOT EXISTS event_key text;
-- Handle a previously applied draft that used an inline UNIQUE declaration.
ALTER TABLE wfc_scale.execution_history
    DROP CONSTRAINT IF EXISTS execution_history_event_key_key;
CREATE UNIQUE INDEX IF NOT EXISTS ux_execution_history_event_key
    ON wfc_scale.execution_history(event_key);
CREATE INDEX IF NOT EXISTS ix_execution_history_workflow
    ON wfc_scale.execution_history(workflow_id,id);
CREATE INDEX IF NOT EXISTS ix_execution_history_type
    ON wfc_scale.execution_history(event_type,occurred_at);
-- The application only INSERTs into this table. Restrict UPDATE/DELETE
-- grants for the runtime DB role separately from the migration role.
