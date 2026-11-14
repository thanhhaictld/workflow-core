-- Avoid scanning unrelated workflow tasks for activity-only claimers.
-- The expression matches the capability predicate used by both claim paths.
CREATE INDEX IF NOT EXISTS ix_hub_task_claim_capability_sequence
  ON wfc_scale.hub_task ((hub_id || '|' || task_type || '|' || task_version::text), sequence)
  WHERE state IN (0,1) AND attempts < max_attempts;
