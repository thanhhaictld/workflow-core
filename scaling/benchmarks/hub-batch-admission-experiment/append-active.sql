-- psql variable :prefix must be unique in the stale-statistics database.
-- These 25k rows are active due-ready backlog, deliberately not analyzed after insert.
INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload,state,due_at,attempts,max_attempts,created_at,updated_at)
SELECT md5(:'prefix'||g)::uuid,'hub-'||(g%4),((g/4)%4)::int,'activity-'||(g%8),1+(g%3),NULL,
       jsonb_build_object('fixture','synthetic','class','active-backlog-extra','ordinal',g),0,
       now()-interval '1 minute',0,5,now()-interval '1 hour',now()-interval '1 minute'
FROM generate_series(1,25000) g;
