-- Run with psql and autocommit before deploying the corresponding Hub.
-- Match the ready-due reader's predicate without making the hub-wide
-- ready and claim-sequence indexes eligible for that query.
SELECT pg_advisory_lock(12029, 38);

SELECT NOT EXISTS (
    SELECT 1 FROM pg_index
    WHERE indexrelid = to_regclass('wfc_scale.ix_hub_task_candidate_ready_bucket_due')
      AND indisvalid
      AND obj_description(indexrelid, 'pg_class') = '038: bounded ready-due candidate pages'
) AS rebuild_ready_due_bucket_index \gset

\if :rebuild_ready_due_bucket_index
-- Remove an invalid replacement left by an interrupted concurrent build.
DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_ready_due_bucket_new;
CREATE INDEX CONCURRENTLY ix_hub_task_ready_due_bucket_new
    ON wfc_scale.hub_task (hub_id, (partition_id % 64), due_at, sequence)
    WHERE (CASE state WHEN 0 THEN true ELSE false END)
      AND attempts < max_attempts AND admission = 1;

BEGIN;
ALTER INDEX IF EXISTS wfc_scale.ix_hub_task_candidate_ready_bucket_due
    RENAME TO ix_hub_task_ready_due_bucket_old;
ALTER INDEX wfc_scale.ix_hub_task_ready_due_bucket_new
    RENAME TO ix_hub_task_candidate_ready_bucket_due;
COMMENT ON INDEX wfc_scale.ix_hub_task_candidate_ready_bucket_due
    IS '038: bounded ready-due candidate pages';
COMMIT;
\endif

DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_ready_due_bucket_old;
SELECT pg_advisory_unlock(12029, 38);
