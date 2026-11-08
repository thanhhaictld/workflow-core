-- Find exhausted native activity transports without scanning completed tasks.
CREATE INDEX IF NOT EXISTS ix_hub_task_native_reconcile
  ON wfc_scale.hub_task (workflow_id)
  WHERE state = 3 AND dispatch_key IS NOT NULL
    AND last_error IS DISTINCT FROM 'Native pointer cancelled';
