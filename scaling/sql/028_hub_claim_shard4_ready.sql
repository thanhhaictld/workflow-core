-- Optional support for the four-shard ordered capability merge experiment.
-- Compose/dev install only with WFC_HUB_INSTALL_SHARD4_READY_INDEX=true.
-- Disabling the query or installation flag does not drop an existing index.
-- Existing indexes remain available to all other claim paths and shard counts.
CREATE INDEX IF NOT EXISTS ix_hub_task_ready_capability_shard4_sequence
  ON wfc_scale.hub_task ((hub_id || '|' || task_type || '|' || task_version::text),
                         (partition_id % 4), sequence)
  WHERE state = 0 AND attempts < max_attempts;
