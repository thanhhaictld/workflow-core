-- Advisory cross-Hub wakeups. PostgreSQL tasks/leases remain authoritative.
-- Apply after 013_activity_policy_revision.sql; safe to re-apply.
-- pg_notify is transaction-coupled: listeners see hints only after commit.
-- Same-channel/same-payload notifications within a transaction are coalesced.
CREATE OR REPLACE FUNCTION wfc_scale.notify_hub_work_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- A newly created immediately eligible task warrants a wake-up.
        IF NEW.state = 0 AND NEW.due_at <= statement_timestamp() THEN
            PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
        END IF;
    ELSE
        -- A future task becoming due/ready warrants a wake-up. Also signal
        -- when a lease is released: older tasks blocked by a resource permit
        -- may now be eligible even though their rows have not changed.
        IF (NEW.state = 0 AND NEW.due_at <= statement_timestamp()
            AND (OLD.state <> 0 OR OLD.due_at > statement_timestamp()))
           OR (OLD.state = 1 AND NEW.state <> 1) THEN
            PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_hub_work_ready ON wfc_scale.hub_task;
CREATE TRIGGER trg_hub_work_ready
AFTER INSERT OR UPDATE OF state, due_at ON wfc_scale.hub_task
FOR EACH ROW EXECUTE FUNCTION wfc_scale.notify_hub_work_ready();
