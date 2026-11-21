\set request_a random(1,2147483647)
\set request_b random(1,2147483647)
INSERT INTO wfc_scale.hub_task
  (id,hub_id,partition_id,task_type,task_version,workflow_id,payload)
VALUES
  (md5(':request_a-:request_b')::uuid,'dispatch-before-lab',0,'native-activity',1,
   '22222222-2222-2222-2222-222222222222','{}'::jsonb);
UPDATE wfc_scale.hub_task
SET execution_pointer_id=gen_random_uuid()::text,
    dispatch_key=gen_random_uuid()::text,
    native_step_id=7,
    due_at=statement_timestamp()+interval '1 hour'
WHERE id=md5(':request_a-:request_b')::uuid;
