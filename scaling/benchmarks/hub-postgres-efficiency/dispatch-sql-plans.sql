\pset pager off
\echo before-insert-default-due
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT TEXT)
INSERT INTO wfc_scale.hub_task
  (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,request_key)
VALUES ('44444444-4444-4444-4444-444444444441','dispatch-plan-before',0,'native-activity',1,
        '44444444-4444-4444-4444-444444444440','{}'::jsonb,'dispatch-plan-before');
\echo before-metadata-update
EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT TEXT)
UPDATE wfc_scale.hub_task SET execution_pointer_id='pointer',dispatch_key='dispatch-plan-before',
  native_step_id=7,due_at=statement_timestamp()+interval '1 hour'
WHERE id='44444444-4444-4444-4444-444444444441';
ROLLBACK;

\echo after-single-insert-final-due
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT TEXT)
INSERT INTO wfc_scale.hub_task
  (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,request_key,
   execution_pointer_id,dispatch_key,native_step_id,due_at)
VALUES ('55555555-5555-5555-5555-555555555551','dispatch-plan-after',0,'native-activity',1,
        '55555555-5555-5555-5555-555555555550','{}'::jsonb,'dispatch-plan-after',
        'pointer','dispatch-plan-after',7,statement_timestamp()+interval '1 hour');
ROLLBACK;
