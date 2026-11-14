-- Partition-wide candidate streams; no payload or resource arrays are indexed.
-- A blocking build is suitable only in a maintenance window.
CREATE INDEX IF NOT EXISTS ix_hub_task_candidate_ready_bucket_sequence
  ON wfc_scale.hub_task (hub_id, (partition_id % 64), sequence)
  WHERE state = 0 AND attempts < max_attempts;
CREATE INDEX IF NOT EXISTS ix_hub_task_candidate_expired_bucket_deadline
  ON wfc_scale.hub_task (hub_id, (partition_id % 64), lease_until, sequence)
  WHERE state = 1 AND attempts < max_attempts;
-- Deadline-ordered recovery avoids scanning long runs of future-due ready tasks.
CREATE INDEX IF NOT EXISTS ix_hub_task_candidate_ready_bucket_due
  ON wfc_scale.hub_task (hub_id, (partition_id % 64), due_at, sequence)
  WHERE state = 0 AND attempts < max_attempts;
