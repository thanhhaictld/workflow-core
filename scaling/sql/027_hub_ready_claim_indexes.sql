-- Separate ready work from leased rows when using split batch claims.
-- Retain the active indexes for the default and legacy claim paths.
CREATE INDEX IF NOT EXISTS ix_hub_task_ready_capability_sequence
  ON wfc_scale.hub_task ((hub_id || '|' || task_type || '|' || task_version::text), sequence)
  WHERE state = 0 AND attempts < max_attempts;

CREATE INDEX IF NOT EXISTS ix_hub_task_ready_capability_bucket_sequence
  ON wfc_scale.hub_task ((hub_id || '|' || task_type || '|' || task_version::text),
                         (partition_id % 64), sequence)
  WHERE state = 0 AND attempts < max_attempts;
