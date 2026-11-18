-- Older writers omit admission and existing tasks retain conservative admission.
-- 0 = unknown/compatibility, 1 = unconstrained, even values are permit flags.
BEGIN;
ALTER TABLE wfc_scale.hub_task
    ADD COLUMN IF NOT EXISTS admission smallint NOT NULL DEFAULT 0
    CHECK (admission BETWEEN 0 AND 126 AND (admission <= 1 OR admission % 2 = 0));

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
