-- psql variable :prefix must be unique within its target database.
INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload,state,due_at,attempts,max_attempts,result_json,created_at,updated_at)
SELECT md5(:'prefix'||g)::uuid,'hub-'||(g%4),((g/4)%4)::int,'activity-'||(g%8),1+(g%3),NULL,
       jsonb_build_object('fixture','synthetic','class','historical-completed-extra','ordinal',g),2,
       now()-interval '1 day',1,5,jsonb_build_object('ok',true),now()-interval '2 days',now()-interval '1 day'
FROM generate_series(1,25000) g;
