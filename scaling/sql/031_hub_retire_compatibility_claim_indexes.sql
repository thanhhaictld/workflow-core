-- Retire five compatibility claim indexes from the candidate-matching schema.
-- Run with autocommit: DROP INDEX CONCURRENTLY cannot run in a transaction block.
-- Current bootstrap paths skip the index-only migrations 023 and 025-027 so
-- repeated setup does not rebuild these indexes before dropping them again.
-- Keep the original migrations available for explicit compatibility rollback.
-- Task data, unique indexes, recovery and candidate discovery indexes are retained.
DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_claim_hub_sequence;
DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_claim_capability_sequence;
DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_claim_capability_bucket_sequence;
DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_ready_capability_sequence;
DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_ready_capability_bucket_sequence;
