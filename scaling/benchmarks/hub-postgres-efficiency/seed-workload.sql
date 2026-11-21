-- Run after migrations through 033 on a fresh isolated database.
ALTER TABLE wfc_scale.hub_task DISABLE TRIGGER trg_hub_work_ready;
ALTER TABLE wfc_scale.hub_task SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
INSERT INTO wfc_scale.hub_workflow
  (id,hub_id,definition_id,definition_version,partition_id,input_json,step_count,execution_mode,native_snapshot,native_status)
VALUES ('11111111-1111-1111-1111-111111111111','efficiency-lab','lab-definition',1,0,'{}'::jsonb,1,'native','{}'::jsonb,'Runnable');
INSERT INTO wfc_scale.hub_task
  (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,state,due_at,owner,lease_until,attempts,max_attempts)
SELECT gen_random_uuid(),'efficiency-lab',((n-1)%64)::int,'activity',1,
  '11111111-1111-1111-1111-111111111111','{}'::jsonb,
  CASE WHEN n<=100000 THEN 0 ELSE 1 END,
  CASE WHEN n<=50000 THEN statement_timestamp()+interval '30 days'
       WHEN n<=100000 THEN statement_timestamp()-interval '1 minute'
       ELSE statement_timestamp() END,
  CASE WHEN n>100000 THEN 'worker' END,
  CASE WHEN n>100000 AND n<=150000 THEN statement_timestamp()+interval '30 days'
       WHEN n>150000 THEN statement_timestamp()-interval '30 seconds' END,
  0,5
FROM generate_series(1,155000) n;
ALTER TABLE wfc_scale.hub_task ENABLE TRIGGER trg_hub_work_ready;
INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
SELECT 'lab-resource-'||g,10,false FROM generate_series(0,9) g;
INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
SELECT id,'lab-resource-'||(sequence%10) FROM wfc_scale.hub_task
WHERE hub_id='efficiency-lab' AND sequence%20=0;
ANALYZE wfc_scale.hub_task;
ANALYZE wfc_scale.hub_task_constraint;
ANALYZE wfc_scale.hub_workflow;
