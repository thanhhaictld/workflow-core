-- Restrict the ordered index to claimable attempt counts. Point updates and
-- retirement do not imply this predicate and should use their lookup indexes.
CREATE INDEX IF NOT EXISTS ix_hub_task_claim_sequence
  ON wfc_scale.hub_task (sequence)
  WHERE state IN (0,1) AND attempts < max_attempts;
DROP INDEX IF EXISTS wfc_scale.ix_hub_task_active_sequence;
DROP INDEX IF EXISTS wfc_scale.ix_hub_task_active_workflow;
