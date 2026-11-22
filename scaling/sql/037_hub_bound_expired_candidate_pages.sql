-- Run with psql and autocommit, before deploying the corresponding Hub.
-- The CASE matches the expired page reader while keeping older partial
-- indexes ineligible for that query. It still selects exactly state = 1.
-- Keep the public index name so replaying 029 does not rebuild the old index.
SELECT pg_advisory_lock(12029, 37);

SELECT NOT EXISTS (
    SELECT 1 FROM pg_index
    WHERE indexrelid = to_regclass('wfc_scale.ix_hub_task_candidate_expired_bucket_deadline')
      AND indisvalid
      AND obj_description(indexrelid, 'pg_class') = '037: bounded expired candidate pages'
) AS rebuild_expired_bucket_index \gset

\if :rebuild_expired_bucket_index
-- Retry a failed build without leaving an invalid index in the write path.
DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_expired_bucket_new;
CREATE INDEX CONCURRENTLY ix_hub_task_expired_bucket_new
    ON wfc_scale.hub_task (hub_id, (partition_id % 64), lease_until, sequence)
    WHERE (CASE state WHEN 1 THEN true ELSE false END)
      AND attempts < max_attempts AND admission = 1;

BEGIN;
ALTER INDEX IF EXISTS wfc_scale.ix_hub_task_candidate_expired_bucket_deadline
    RENAME TO ix_hub_task_expired_bucket_old;
ALTER INDEX wfc_scale.ix_hub_task_expired_bucket_new
    RENAME TO ix_hub_task_candidate_expired_bucket_deadline;
COMMENT ON INDEX wfc_scale.ix_hub_task_candidate_expired_bucket_deadline
    IS '037: bounded expired candidate pages';
COMMIT;
\endif

DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_expired_bucket_old;
SELECT pg_advisory_unlock(12029, 37);
