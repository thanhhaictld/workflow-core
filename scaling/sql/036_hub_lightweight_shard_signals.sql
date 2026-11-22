-- 036: Default notifications are transactional, shard-bucket WAKE-UP SIGNALS.
-- One signal per (hub, 0..63 bucket) per statement; pg_notify further dedupes
-- identical (channel,payload) within a transaction. No task payloads or IDs.
-- A lost signal is harmless: candidate recovery remains PostgreSQL-authoritative.
-- Explicit experimental exact-ID producers retain the row-level trigger;
-- explicit outbox sessions still produce one outbox row per partition/statement.
--
-- AFTER STATEMENT transition tables avoid running JSON/pg_notify per inserted
-- task. UPDATE transition tables are filtered to newly-ready transitions.
-- The GUC guard makes the existing row trigger opt-in without rewriting it.
DROP TRIGGER IF EXISTS trg_hub_work_ready ON wfc_scale.hub_task;
CREATE TRIGGER trg_hub_work_ready
AFTER INSERT OR UPDATE OF state, due_at, epoch ON wfc_scale.hub_task
FOR EACH ROW
WHEN (current_setting('wfc.hub_candidate_task_id_hints', true) = 'on')
EXECUTE FUNCTION wfc_scale.notify_hub_work_ready();

CREATE OR REPLACE FUNCTION wfc_scale.hub_signal_ready_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('wfc.hub_candidate_task_id_hints', true) = 'on' THEN RETURN NULL; END IF;
    IF current_setting('wfc.hub_notification_outbox', true) = 'on' THEN
        -- Compatibility for explicitly selected outbox clients. The default
        -- lightweight path never inserts an outbox row.
        INSERT INTO wfc_scale.hub_notification_outbox(hub_id, matching_payload)
        SELECT hub_id, json_build_object('hub',hub_id,'partition',partition_id,'kind','signal')::text
        FROM (
            SELECT DISTINCT hub_id,partition_id FROM hub_inserted
            WHERE state=0 AND due_at<=statement_timestamp() AND octet_length(hub_id)<=80
        ) as hints;
    ELSE
        PERFORM pg_notify('wfc_hub_task_ready',hub_id),
                pg_notify('wfc_hub_matching_ready',
                    json_build_object('hub',hub_id,'partition',bucket,'kind','signal')::text)
        FROM (
            SELECT DISTINCT hub_id,(partition_id % 64) AS bucket FROM hub_inserted
            WHERE state=0 AND due_at<=statement_timestamp() AND octet_length(hub_id)<=80
        ) as hints;
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION wfc_scale.hub_signal_ready_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('wfc.hub_candidate_task_id_hints', true) = 'on' THEN RETURN NULL; END IF;
    IF current_setting('wfc.hub_notification_outbox', true) = 'on' THEN
        INSERT INTO wfc_scale.hub_notification_outbox(hub_id, matching_payload)
        SELECT hub_id, json_build_object('hub',hub_id,'partition',partition_id,'kind','signal')::text
        FROM (
            SELECT DISTINCT n.hub_id,n.partition_id
            FROM hub_updated n JOIN hub_previous p ON p.id=n.id
            WHERE n.state=0 AND n.due_at<=statement_timestamp()
              AND (p.state<>0 OR p.due_at>statement_timestamp())
              AND octet_length(n.hub_id)<=80
        ) as hints;
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
        ) as hints;
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_hub_work_ready_signal_insert ON wfc_scale.hub_task;
CREATE TRIGGER trg_hub_work_ready_signal_insert
AFTER INSERT ON wfc_scale.hub_task
REFERENCING NEW TABLE AS hub_inserted
FOR EACH STATEMENT EXECUTE FUNCTION wfc_scale.hub_signal_ready_insert();

DROP TRIGGER IF EXISTS trg_hub_work_ready_signal_update ON wfc_scale.hub_task;
CREATE TRIGGER trg_hub_work_ready_signal_update
AFTER UPDATE ON wfc_scale.hub_task
REFERENCING OLD TABLE AS hub_previous NEW TABLE AS hub_updated
FOR EACH STATEMENT EXECUTE FUNCTION wfc_scale.hub_signal_ready_update();
