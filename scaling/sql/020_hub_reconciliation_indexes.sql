-- Keep periodic reconciliation proportional to active work, not retained history.
CREATE INDEX IF NOT EXISTS ix_hub_workflow_native_reconcile
  ON wfc_scale.hub_workflow (id) INCLUDE (hub_id)
  WHERE execution_mode = 'native' AND native_status IN ('Runnable', 'Suspended');

CREATE INDEX IF NOT EXISTS ix_hub_task_dead_workflow_step
  ON wfc_scale.hub_task (workflow_id)
  WHERE state = 3 AND workflow_step IS NOT NULL;
