-- Retained historical index migration for databases where it was installed.
-- Active setup no longer applies this migration, and no rollback drops it.
-- Existing indexes remain available to all other claim paths and shard counts.
CREATE INDEX IF NOT EXISTS ix_hub_task_ready_capability_shard4_sequence
  ON wfc_scale.hub_task ((hub_id || '|' || task_type || '|' || task_version::text),
                         (partition_id % 4), sequence)
  WHERE state = 0 AND attempts < max_attempts;
