-- Remove the experimental row-level exact task-ID producer. Keep the
-- statement-level bounded shard signals and the explicit legacy outbox path.
-- This is forward-only: migrations already applied to customer data are not
-- edited or reversed.
DROP TRIGGER IF EXISTS trg_hub_work_ready ON wfc_scale.hub_task;
DROP FUNCTION IF EXISTS wfc_scale.notify_hub_work_ready();

CREATE OR REPLACE FUNCTION wfc_scale.hub_signal_ready_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('wfc.hub_notification_outbox', true) = 'on' THEN
        INSERT INTO wfc_scale.hub_notification_outbox(hub_id, matching_payload)
        SELECT hub_id, json_build_object('hub',hub_id,'partition',partition_id,'kind','signal')::text
        FROM (
            SELECT DISTINCT hub_id,partition_id FROM hub_inserted
            WHERE state=0 AND due_at<=statement_timestamp() AND octet_length(hub_id)<=80
        ) AS hints;
    ELSE
        PERFORM pg_notify('wfc_hub_task_ready',hub_id),
                pg_notify('wfc_hub_matching_ready',
                    json_build_object('hub',hub_id,'partition',bucket,'kind','signal')::text)
        FROM (
            SELECT DISTINCT hub_id,(partition_id % 64) AS bucket FROM hub_inserted
            WHERE state=0 AND due_at<=statement_timestamp() AND octet_length(hub_id)<=80
        ) AS hints;
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION wfc_scale.hub_signal_ready_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('wfc.hub_notification_outbox', true) = 'on' THEN
        INSERT INTO wfc_scale.hub_notification_outbox(hub_id, matching_payload)
        SELECT hub_id, json_build_object('hub',hub_id,'partition',partition_id,'kind','signal')::text
        FROM (
            SELECT DISTINCT n.hub_id,n.partition_id
            FROM hub_updated n JOIN hub_previous p ON p.id=n.id
            WHERE n.state=0 AND n.due_at<=statement_timestamp()
              AND (p.state<>0 OR p.due_at>statement_timestamp())
              AND octet_length(n.hub_id)<=80
        ) AS hints;
    ELSE
        PERFORM pg_notify('wfc_hub_task_ready',hub_id),
                pg_notify('wfc_hub_matching_ready',
                    json_build_object('hub',hub_id,'partition',bucket,'kind','signal')::text)
        FROM (
            SELECT DISTINCT n.hub_id,(n.partition_id % 64) AS bucket
            FROM hub_updated n JOIN hub_previous p ON p.id=n.id
            WHERE n.state=0 AND n.due_at<=statement_timestamp()
              AND (p.state<>0 OR p.due_at>statement_timestamp())
              AND octet_length(n.hub_id)<=80
        ) AS hints;
    END IF;
    RETURN NULL;
END;
$$;
