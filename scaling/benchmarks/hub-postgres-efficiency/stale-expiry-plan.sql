-- Add a new expiry wave without ANALYZE, then plan the current expiry page.
ALTER TABLE wfc_scale.hub_task DISABLE TRIGGER trg_hub_work_ready;
INSERT INTO wfc_scale.hub_task
  (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,state,owner,lease_until)
SELECT gen_random_uuid(),'efficiency-lab',((n-1)%64)::int,'activity',1,
       '11111111-1111-1111-1111-111111111111','{}'::jsonb,1,'worker',
       statement_timestamp()-interval '30 seconds'
FROM generate_series(1,5000) n;
ALTER TABLE wfc_scale.hub_task ENABLE TRIGGER trg_hub_work_ready;
SELECT COALESCE(max(sequence),0) AS upper FROM wfc_scale.hub_task \gset

SELECT c.reltuples AS estimated_task_rows,s.last_analyze
FROM pg_class c JOIN pg_stat_user_tables s ON s.relid=c.oid
WHERE c.oid='wfc_scale.hub_task'::regclass;
SELECT count(*) AS actual_task_rows FROM wfc_scale.hub_task WHERE hub_id='efficiency-lab';

EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH page AS MATERIALIZED (
  SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,lease_until AS deadline
  FROM wfc_scale.hub_task
  WHERE hub_id='efficiency-lab' AND partition_id % 64=0 AND attempts<max_attempts
    AND sequence<=:upper AND state=1 AND lease_until<statement_timestamp()
    AND (lease_until,sequence)>(timestamp '0001-01-01',0)
  ORDER BY lease_until,sequence LIMIT 128
), resource_sizes AS MATERIALIZED (
  SELECT c.task_id,sum(octet_length(c.resource_key)+24) AS bytes
  FROM wfc_scale.hub_task_constraint c JOIN page p ON p.id=c.task_id GROUP BY c.task_id
), resources AS MATERIALIZED (
  SELECT c.task_id,array_agg(c.resource_key ORDER BY c.resource_key) AS keys
  FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
  JOIN page p ON p.id=c.task_id
  WHERE s.bytes+128+octet_length(p.hub_id)+octet_length(p.task_type)<=512
  GROUP BY c.task_id
)
SELECT p.id,p.sequence,p.hub_id,p.partition_id,p.task_type,p.task_version,p.workflow_id,p.epoch,p.deadline,
       COALESCE(r.keys,ARRAY[]::text[]),COALESCE(s.bytes,0)
FROM page p LEFT JOIN resource_sizes s ON s.task_id=p.id LEFT JOIN resources r ON r.task_id=p.id
ORDER BY p.deadline,p.sequence;
