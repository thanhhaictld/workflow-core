\set request_a random(1,2147483647)
\set request_b random(1,2147483647)
INSERT INTO wfc_scale.hub_task
  (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,
   execution_pointer_id,dispatch_key,native_step_id,due_at)
VALUES
  (md5(':request_a-:request_b')::uuid,'dispatch-after-lab',0,'native-activity',1,
   '33333333-3333-3333-3333-333333333333','{}'::jsonb,
   gen_random_uuid()::text,gen_random_uuid()::text,7,
   statement_timestamp()+interval '1 hour');
