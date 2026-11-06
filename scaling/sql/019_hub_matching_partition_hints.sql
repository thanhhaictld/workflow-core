-- Transaction-committed, shard-targeted matching notifications.
-- Keep old wfc_hub_task_ready channel for SSE and pre-sharding processors.
-- Release of any resource permit can unblock another task in *any* shard.
CREATE OR REPLACE FUNCTION wfc_scale.notify_hub_work_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF octet_length(NEW.hub_id) > 80 THEN RETURN NEW; END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state = 0 AND NEW.due_at <= statement_timestamp() THEN
            PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
            PERFORM pg_notify('wfc_hub_matching_ready',
                json_build_object('hub',NEW.hub_id,'type',NEW.task_type,
                                  'version',NEW.task_version,
                                  'partition',NEW.partition_id)::text);
        END IF;
    ELSE
        IF NEW.state = 0 AND NEW.due_at <= statement_timestamp()
             AND (OLD.state <> 0 OR OLD.due_at > statement_timestamp()) THEN
            PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
            PERFORM pg_notify('wfc_hub_matching_ready',
                json_build_object('hub',NEW.hub_id,'type',NEW.task_type,
                                  'version',NEW.task_version,
                                  'partition',NEW.partition_id)::text);
        ELSIF OLD.state = 1 AND NEW.state <> 1 THEN
            -- A keyed resource permit can be shared across task partitions.
            PERFORM pg_notify('wfc_hub_task_ready', NEW.hub_id);
            PERFORM pg_notify('wfc_hub_matching_ready',
                json_build_object('hub',NEW.hub_id)::text);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
