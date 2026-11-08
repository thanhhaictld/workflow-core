-- Bound workflow retirement to active tasks.
CREATE INDEX IF NOT EXISTS ix_hub_task_active_workflow_lookup
  ON wfc_scale.hub_task (workflow_id)
  WHERE state IN (0,1) AND workflow_id IS NOT NULL;
