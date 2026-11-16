-- Disposable PostgreSQL 17 query-shape comparison for stage 3.
-- Run only in a fresh isolated database with migrations through 032.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL wfc.hub_notification_outbox = 'on';
INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
SELECT gen_random_uuid(),'stage3-candidate-bench',0,'bench-activity',1,'{}'::jsonb
FROM generate_series(1,50000);
COMMIT;
INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
VALUES ('stage3-shared-resource',1,true);
INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
SELECT id,'stage3-shared-resource' FROM wfc_scale.hub_task
WHERE hub_id='stage3-candidate-bench' ORDER BY sequence LIMIT 128;
ANALYZE wfc_scale.hub_task;
ANALYZE wfc_scale.hub_task_constraint;

PREPARE baseline_page(text,int,bigint,timestamptz,timestamptz,bigint,int,int) AS
WITH page AS MATERIALIZED (
  SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,
         due_at AS deadline
  FROM wfc_scale.hub_task
  WHERE hub_id=$1 AND partition_id % 64=$2 AND attempts<max_attempts
    AND sequence<=$3 AND state=0 AND due_at<=$4 AND (due_at,sequence)>($5,$6)
  ORDER BY due_at,sequence LIMIT $7
), resource_sizes AS MATERIALIZED (
  SELECT c.task_id,sum(octet_length(c.resource_key)+24) AS bytes
  FROM wfc_scale.hub_task_constraint c JOIN page p ON p.id=c.task_id GROUP BY c.task_id
), resources AS (
  SELECT c.task_id,array_agg(c.resource_key ORDER BY c.resource_key) AS keys
  FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
  WHERE s.bytes<=$8 GROUP BY c.task_id
)
SELECT p.id,p.sequence,
       CASE WHEN length.too_large THEN NULL ELSE p.hub_id END,
       p.partition_id,CASE WHEN length.too_large THEN NULL ELSE p.task_type END,
       p.task_version,p.workflow_id,CASE WHEN length.too_large THEN NULL ELSE w.definition_id END,
       w.definition_version,p.epoch,p.deadline,COALESCE(r.keys,ARRAY[]::text[]),
       COALESCE(s.bytes,0),length.too_large
FROM page p LEFT JOIN wfc_scale.hub_workflow w ON w.id=p.workflow_id
CROSS JOIN LATERAL (SELECT (128+octet_length(p.hub_id)+octet_length(p.task_type)+
  COALESCE(octet_length(w.definition_id),0))>$8 AS too_large) length
LEFT JOIN resource_sizes s ON s.task_id=p.id LEFT JOIN resources r ON r.task_id=p.id
ORDER BY p.deadline,p.sequence;

PREPARE final_hydration(uuid[],text,int,int,int) AS
WITH requested AS MATERIALIZED (SELECT unnest($1::uuid[]) AS id),
page AS MATERIALIZED (
  SELECT t.id,t.sequence,t.hub_id,t.partition_id,t.task_type,t.task_version,t.workflow_id,
         t.epoch,t.due_at,t.lease_until,t.state
  FROM requested r JOIN wfc_scale.hub_task t ON t.id=r.id
  WHERE t.hub_id=$2 AND t.partition_id % $4=$3 AND t.attempts<t.max_attempts
    AND ((t.state=0 AND t.due_at<=clock_timestamp()) OR
         (t.state=1 AND t.lease_until<clock_timestamp()))
), budget AS MATERIALIZED (
  SELECT GREATEST(1,$5/GREATEST(count(*),1)) AS per_candidate FROM page
), resource_sizes AS MATERIALIZED (
  SELECT c.task_id,sum(octet_length(c.resource_key)+24) AS bytes
  FROM wfc_scale.hub_task_constraint c JOIN page p ON p.id=c.task_id GROUP BY c.task_id
), base_size AS MATERIALIZED (
  SELECT p.id,128+octet_length(p.hub_id)+octet_length(p.task_type)+
    COALESCE(octet_length(w.definition_id),0) AS bytes,b.per_candidate
  FROM page p LEFT JOIN wfc_scale.hub_workflow w ON w.id=p.workflow_id CROSS JOIN budget b
), resources AS (
  SELECT c.task_id,array_agg(c.resource_key ORDER BY c.resource_key) AS keys
  FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
  JOIN base_size b ON b.id=c.task_id
  WHERE b.bytes<=b.per_candidate AND b.bytes+s.bytes<=b.per_candidate GROUP BY c.task_id
)
SELECT p.id,p.sequence,CASE WHEN b.bytes<=b.per_candidate THEN p.hub_id END,p.partition_id,
       CASE WHEN b.bytes<=b.per_candidate THEN p.task_type END,p.task_version,p.workflow_id,
       CASE WHEN b.bytes<=b.per_candidate THEN w.definition_id END,w.definition_version,p.epoch,
       CASE WHEN p.lease_until<clock_timestamp() AND p.state=1 THEN p.lease_until ELSE p.due_at END,
       COALESCE(r.keys,ARRAY[]::text[]),COALESCE(s.bytes,0),b.bytes>b.per_candidate,
       COALESCE(r.keys IS NOT NULL OR s.bytes IS NULL OR s.bytes=0,FALSE)
FROM page p LEFT JOIN wfc_scale.hub_workflow w ON w.id=p.workflow_id
JOIN base_size b ON b.id=p.id LEFT JOIN resource_sizes s ON s.task_id=p.id
LEFT JOIN resources r ON r.task_id=p.id ORDER BY p.sequence,p.id;

SET plan_cache_mode='force_generic_plan';
SELECT array_agg(id)::uuid[] AS batch_ids FROM (
  SELECT id FROM wfc_scale.hub_task WHERE hub_id='stage3-candidate-bench'
  ORDER BY sequence LIMIT 128
) s \gset
SELECT id AS single_id FROM wfc_scale.hub_task WHERE hub_id='stage3-candidate-bench' ORDER BY sequence LIMIT 1 \gset
SELECT max(sequence) AS upper_sequence FROM wfc_scale.hub_task \gset
\echo baseline indexed discovery page: generic prepared parameters, 128-row page
EXPLAIN (ANALYZE,BUFFERS,SETTINGS,FORMAT JSON)
EXECUTE baseline_page('stage3-candidate-bench',0,:upper_sequence,clock_timestamp(),
  '-infinity'::timestamptz,0,128,65536);
\echo final committed-ID hydration: generic prepared parameters, 128 requested IDs
EXPLAIN (ANALYZE,BUFFERS,SETTINGS,FORMAT JSON)
EXECUTE final_hydration(:'batch_ids'::uuid[],'stage3-candidate-bench',0,64,65536);
\echo final committed-ID hydration: generic prepared parameters, one requested ID
EXPLAIN (ANALYZE,BUFFERS,SETTINGS,FORMAT JSON)
EXECUTE final_hydration(ARRAY[:'single_id'::uuid],'stage3-candidate-bench',0,64,65536);
-- Claim and completion produce invalidation/release hints; lease-only renewal
-- must not create an outbox row.
BEGIN;
SET LOCAL wfc.hub_notification_outbox = 'on';
UPDATE wfc_scale.hub_task SET state=1,owner='bench-worker',
  lease_until=clock_timestamp()+interval '1 minute',epoch=epoch+1 WHERE id=:'single_id';
COMMIT;
BEGIN;
SET LOCAL wfc.hub_notification_outbox = 'on';
UPDATE wfc_scale.hub_task SET lease_until=lease_until+interval '1 minute' WHERE id=:'single_id';
COMMIT;
BEGIN;
SET LOCAL wfc.hub_notification_outbox = 'on';
UPDATE wfc_scale.hub_task SET state=2,owner=NULL,lease_until=NULL WHERE id=:'single_id';
COMMIT;
SELECT count(*) AS outbox_hint_rows FROM wfc_scale.hub_notification_outbox WHERE hub_id='stage3-candidate-bench';
