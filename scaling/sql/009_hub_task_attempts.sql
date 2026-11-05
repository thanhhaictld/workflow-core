-- Durable lease/attempt timeline for Hub-managed workflow steps.
-- A new epoch represents a fenced claim, not necessarily a successful
-- remote activity start. Older workflows have no attempt rows and continue
-- to use the existing completion-only history.
CREATE TABLE IF NOT EXISTS wfc_scale.hub_task_attempt (
    task_id uuid NOT NULL REFERENCES wfc_scale.hub_task(id) ON DELETE CASCADE,
    epoch bigint NOT NULL CHECK (epoch > 0),
    workflow_id uuid NOT NULL REFERENCES wfc_scale.hub_workflow(id),
    step_index integer NOT NULL CHECK (step_index >= 0),
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    worker_id text NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    outcome text NOT NULL DEFAULT 'Leased'
        CHECK (outcome IN ('Leased', 'Completed', 'RetryScheduled', 'Failed', 'LeaseExpired')),
    PRIMARY KEY (task_id, epoch),
    CONSTRAINT ck_hub_attempt_finish CHECK (
        (outcome = 'Leased' AND finished_at IS NULL)
        OR (outcome <> 'Leased' AND finished_at IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS ix_hub_attempt_workflow
    ON wfc_scale.hub_task_attempt (workflow_id, step_index, claimed_at);
