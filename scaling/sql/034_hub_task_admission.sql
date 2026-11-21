-- MVP cutover: new writers no longer publish business-key concurrency policies.
-- 0 = unknown/compatibility, 1 = proven unconstrained, even values are permit flags.
-- This forward-incompatible MVP upgrade reclassifies legacy tasks without
-- permit rows; applications with permit-sensitive live work must drain/reset
-- before using this version.
BEGIN;
ALTER TABLE wfc_scale.hub_task
    ADD COLUMN IF NOT EXISTS admission smallint NOT NULL DEFAULT 1
    CHECK (admission BETWEEN 0 AND 126 AND (admission <= 1 OR admission % 2 = 0));

ALTER TABLE wfc_scale.hub_task ALTER COLUMN admission SET DEFAULT 1;
-- Migration is deliberately not backwards compatible: authority comes from
-- persisted constraint rows, not historical registrations.
UPDATE wfc_scale.hub_task t SET admission=0
WHERE admission=1 AND EXISTS (
    SELECT 1 FROM wfc_scale.hub_task_constraint c WHERE c.task_id=t.id
);
UPDATE wfc_scale.hub_task t SET admission=1
WHERE admission=0 AND NOT EXISTS (
    SELECT 1 FROM wfc_scale.hub_task_constraint c WHERE c.task_id=t.id
);

-- Normally a task and its constraints are inserted together with a matching
-- descriptor. If a residual constraint is attached to an unconstrained task,
-- invalidate its descriptor under the task row lock so a cached batch hint
-- cannot strand it or bypass the authoritative constraint check.
CREATE OR REPLACE FUNCTION wfc_scale.hub_guard_unconstrained_admission() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE wfc_scale.hub_task SET admission=0 WHERE id=NEW.task_id AND admission=1;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER hub_guard_unconstrained_admission
BEFORE INSERT OR UPDATE OF task_id ON wfc_scale.hub_task_constraint
FOR EACH ROW EXECUTE FUNCTION wfc_scale.hub_guard_unconstrained_admission();
COMMIT;
