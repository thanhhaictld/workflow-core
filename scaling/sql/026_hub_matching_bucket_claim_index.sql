-- Expression index for 64 stable logical bucket IDs.
-- For matching shard counts dividing 64, (partition_id % 64) IN matching
-- buckets is exactly equivalent to partition_id % match_count = match_shard.
-- This changes neither persisted partition IDs nor lease/FIFO semantics.
-- The old capability/sequence index is retained for A/B comparison and
-- non-power-of-two matching shard counts.
CREATE INDEX IF NOT EXISTS ix_hub_task_claim_capability_bucket_sequence
  ON wfc_scale.hub_task (
    (hub_id || '|' || task_type || '|' || task_version::text),
    (partition_id % 64),
    sequence
  )
  WHERE state IN (0,1) AND attempts < max_attempts;
