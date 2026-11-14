-- Additive opt-in transactional notification outbox. Existing direct NOTIFY is
-- retained unless the database setting wfc.hub_notification_outbox is 'on'.
CREATE TABLE IF NOT EXISTS wfc_scale.hub_notification_outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hub_id text NOT NULL,
    matching_payload text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION wfc_scale.notify_hub_work_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE payload text;
BEGIN
    IF octet_length(NEW.hub_id) > 80 THEN RETURN NEW; END IF;
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
END;
$$;
