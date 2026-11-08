-- Allow single-hub claimers to scan their active queue in sequence order.
-- Retain the global sequence index for multi-hub and dense queue scans.
CREATE INDEX IF NOT EXISTS ix_hub_task_claim_hub_sequence
  ON wfc_scale.hub_task (hub_id, sequence)
  WHERE state IN (0,1) AND attempts < max_attempts;
