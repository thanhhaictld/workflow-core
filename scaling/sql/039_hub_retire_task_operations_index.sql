-- Run with psql and autocommit. Remove the monitor index on existing databases.
DROP INDEX CONCURRENTLY IF EXISTS wfc_scale.ix_hub_task_operations;
