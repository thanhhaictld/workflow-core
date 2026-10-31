-- Optional indexes for the existing Workflow Core EF PostgreSQL schema.
-- Validate table/column names and enum values against the migration in this branch.
CREATE INDEX IF NOT EXISTS ix_wfc_workflow_runnable_due
 ON wfc."Workflow" ("NextExecution", "InstanceId")
 WHERE "Status" = 0 AND "NextExecution" IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_wfc_scheduled_due
 ON wfc."ScheduledCommand" ("ExecuteTime");
