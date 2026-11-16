-- Exact task-ID readiness/invalidation hints for Hub-owned in-memory candidates.
-- They are transaction-committed but intentionally ephemeral: NOTIFY and the
-- existing outbox dispatcher can lose hints after dispatch; indexed recovery
-- remains required. Existing 014/030 behavior is replaced additively here.
CREATE OR REPLACE FUNCTION wfc_scale.notify_hub_work_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    payload text;
    task_changed boolean := false;
    ready_now boolean := false;
    permit_released boolean := false;
    candidate_ids_enabled boolean := COALESCE(current_setting('wfc.hub_candidate_task_id_hints', true) = 'on', false);
BEGIN
    IF octet_length(NEW.hub_id) > 80 THEN RETURN NEW; END IF;

    -- Preserve migration 030's producer behavior byte-for-byte when the new
    -- task-ID option is disabled. This keeps direct NOTIFY versus outbox
    -- delivery semantics unchanged for existing candidate-matching users.
    IF NOT candidate_ids_enabled THEN
        IF TG_OP = 'INSERT' THEN
            IF NEW.state = 0 AND NEW.due_at <= statement_timestamp() THEN
                payload := json_build_object('hub',NEW.hub_id,'type',NEW.task_type,
                    'version',NEW.task_version,'partition',NEW.partition_id)::text;
            END IF;
        ELSE
            IF NEW.state = 0 AND NEW.due_at <= statement_timestamp()
                 AND (OLD.state <> 0 OR OLD.due_at > statement_timestamp()) THEN
                payload := json_build_object('hub',NEW.hub_id,'type',NEW.task_type,
                    'version',NEW.task_version,'partition',NEW.partition_id)::text;
            ELSIF OLD.state = 1 AND NEW.state <> 1 THEN
                payload := json_build_object('hub',NEW.hub_id)::text;
            END IF;
        END IF;
        IF payload IS NOT NULL THEN
            IF current_setting('wfc.hub_notification_outbox', true) = 'on' THEN
                INSERT INTO wfc_scale.hub_notification_outbox(hub_id,matching_payload)
                VALUES (NEW.hub_id,payload);
            ELSE
                PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
                PERFORM pg_notify('wfc_hub_matching_ready', payload);
            END IF;
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        ready_now := NEW.state = 0 AND NEW.due_at <= statement_timestamp();
        task_changed := ready_now;
    ELSE
        ready_now := NEW.state = 0 AND NEW.due_at <= statement_timestamp()
            AND (OLD.state <> 0 OR OLD.due_at > statement_timestamp());
        -- Claim/terminal transitions invalidate cached readiness. Include epoch
        -- to cover retries/reclaims without turning lease heartbeats into hints.
        task_changed := ready_now OR NEW.state IS DISTINCT FROM OLD.state
            OR NEW.epoch IS DISTINCT FROM OLD.epoch
            OR (OLD.state = 0 AND NEW.due_at IS DISTINCT FROM OLD.due_at);
        permit_released := OLD.state = 1 AND NEW.state <> 1;
    END IF;

    IF task_changed THEN
        IF candidate_ids_enabled THEN
            payload := json_build_object('hub',NEW.hub_id,'taskId',NEW.id,
                'partition',NEW.partition_id,'type',NEW.task_type,
                'version',NEW.task_version,'kind','task')::text;
        ELSE
            payload := json_build_object('hub',NEW.hub_id,'partition',NEW.partition_id,
                'type',NEW.task_type,'version',NEW.task_version,'kind','task')::text;
        END IF;
        IF octet_length(payload) > 512 THEN
            payload := json_build_object('hub',NEW.hub_id,'kind','recovery')::text;
        END IF;
    ELSIF permit_released THEN
        -- Resource scopes may span matching partitions; route this hub-wide.
        payload := json_build_object('hub',NEW.hub_id,'kind','permit-release')::text;
    END IF;

    IF ready_now OR permit_released THEN
        IF current_setting('wfc.hub_notification_outbox', true) IS DISTINCT FROM 'on' THEN
            PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
        END IF;
    END IF;
    IF payload IS NOT NULL THEN
        IF current_setting('wfc.hub_notification_outbox', true) = 'on' THEN
            INSERT INTO wfc_scale.hub_notification_outbox(hub_id,matching_payload)
            VALUES (NEW.hub_id,payload);
        ELSE
            PERFORM pg_notify('wfc_hub_matching_ready',payload);
        END IF;
    END IF;
    -- Preserve the hub-wide release contract for old listeners and for
    -- resources whose keys cross matching partitions, even when an exact ID
    -- invalidation was also emitted for the changed task.
    IF task_changed AND permit_released THEN
        payload := json_build_object('hub',NEW.hub_id,'kind','permit-release')::text;
        IF current_setting('wfc.hub_notification_outbox', true) = 'on' THEN
            INSERT INTO wfc_scale.hub_notification_outbox(hub_id,matching_payload)
            VALUES (NEW.hub_id,payload);
        ELSE
            PERFORM pg_notify('wfc_hub_matching_ready',payload);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

-- 014 observes only INSERT/state/due_at; epoch changes must invalidate cached
-- task IDs too. Dropping/recreating the trigger preserves rows and indexes.
DROP TRIGGER IF EXISTS trg_hub_work_ready ON wfc_scale.hub_task;
CREATE TRIGGER trg_hub_work_ready
AFTER INSERT OR UPDATE OF state, due_at, epoch ON wfc_scale.hub_task
FOR EACH ROW EXECUTE FUNCTION wfc_scale.notify_hub_work_ready();
