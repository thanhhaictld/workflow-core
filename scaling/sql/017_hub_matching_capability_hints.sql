-- Transaction-committed, capability-aware wakeups for matching owners.
-- Keep wfc_hub_task_ready unchanged for legacy SSE consumers and rolling deploys.
-- Matching hints are advisory; PostgreSQL task rows remain authoritative.
CREATE OR REPLACE FUNCTION wfc_scale.notify_hub_work_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF octet_length(NEW.hub_id) > 80 THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state = 0 AND NEW.due_at <= statement_timestamp() THEN
            PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
            PERFORM pg_notify('wfc_hub_matching_ready',
                json_build_object('hub',NEW.hub_id,'type',NEW.task_type,
                                  'version',NEW.task_version)::text);
        END IF;
    ELSE
        IF NEW.state = 0 AND NEW.due_at <= statement_timestamp()
             AND (OLD.state <> 0 OR OLD.due_at > statement_timestamp()) THEN
            PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
            PERFORM pg_notify('wfc_hub_matching_ready',
                json_build_object('hub',NEW.hub_id,'type',NEW.task_type,
                                  'version',NEW.task_version)::text);
        ELSIF OLD.state = 1 AND NEW.state <> 1 THEN
            -- Completing one task can unblock another task type by releasing
            -- a global/keyed/partition permit. Notify every matching lane.
            PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
            PERFORM pg_notify('wfc_hub_matching_ready',
                json_build_object('hub',NEW.hub_id)::text);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- The trigger from migration 014 executes this replaced function.
