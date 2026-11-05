-- Durable aggregate metrics. Apply after 010_hub_remote_scheduler.sql.
-- No workflow IDs, tenant identifiers or payloads are retained.
CREATE TABLE IF NOT EXISTS wfc_scale.hub_monitor_sample (
    bucket timestamptz NOT NULL,
    hub_id text NOT NULL, -- '' = global/all hubs
    ready_activities bigint NOT NULL,
    leased_activities bigint NOT NULL,
    runnable_workflows bigint NOT NULL,
    PRIMARY KEY(bucket, hub_id)
);
CREATE INDEX IF NOT EXISTS ix_hub_task_operations
    ON wfc_scale.hub_task(hub_id,partition_id,task_type,state,due_at);
CREATE INDEX IF NOT EXISTS ix_hub_workflow_operations
    ON wfc_scale.hub_workflow(hub_id,partition_id,state,created_at);
