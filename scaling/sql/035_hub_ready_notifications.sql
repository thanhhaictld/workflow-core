-- Deliver readiness and exact-ID invalidations without resource-release hints.
CREATE OR REPLACE FUNCTION wfc_scale.notify_hub_work_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    payload text;
    task_changed boolean := false;
    ready_now boolean := false;
    candidate_ids_enabled boolean := COALESCE(current_setting('wfc.hub_candidate_task_id_hints', true) = 'on', false);
BEGIN
    IF octet_length(NEW.hub_id) > 80 THEN RETURN NEW; END IF;

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
    END IF;

    IF task_changed THEN
        payload := json_build_object('hub',NEW.hub_id,'taskId',NEW.id,
            'partition',NEW.partition_id,'type',NEW.task_type,
            'version',NEW.task_version,'kind','task')::text;
        IF octet_length(payload) > 512 THEN
            payload := json_build_object('hub',NEW.hub_id,'kind','recovery')::text;
        END IF;
    END IF;

    IF ready_now THEN
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
    RETURN NEW;
END;
$$;

