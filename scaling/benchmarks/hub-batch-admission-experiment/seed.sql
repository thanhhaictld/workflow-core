-- Synthetic-only, deterministic benchmark fixture; applied only to the labeled disposable DB.
INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES ('synthetic',100,false);
INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload,state,due_at,owner,last_owner,epoch,lease_until,attempts,max_attempts,result_json,created_at,updated_at)
SELECT md5('completed-'||g)::uuid,
       'hub-'||(g%4), ((g/4)%4)::int,
       'activity-'||(g%8), 1+(g%3), NULL,
       jsonb_build_object('fixture','synthetic','class','historical-completed','ordinal',g),
       2, now()-interval '1 day', NULL,NULL,0,NULL,1,5,
       jsonb_build_object('ok',true,'fixture',true), now()-interval '2 days', now()-interval '1 day'
FROM generate_series(1,64000) g;
ANALYZE wfc_scale.hub_task;
INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload,state,due_at,owner,last_owner,epoch,lease_until,attempts,max_attempts,created_at,updated_at)
SELECT md5('backlog-'||g)::uuid,
       'hub-'||(g%4), ((g/4)%4)::int,
       'activity-'||(g%8), 1+(g%3), NULL,
       jsonb_build_object('fixture','synthetic','class','active-backlog','ordinal',g),
       CASE WHEN g%20=17 THEN 1 ELSE 0 END,
       CASE WHEN g%20=16 THEN now()+interval '1 hour' ELSE now()-interval '1 minute' END,
       CASE WHEN g%20=17 THEN 'live-worker' ELSE NULL END,
       CASE WHEN g%20=17 THEN 'live-worker' ELSE NULL END,
       CASE WHEN g%20=17 THEN 1 ELSE 0 END,
       CASE WHEN g%20=17 THEN now()+interval '10 minutes' ELSE NULL END,
       0,5,now()-interval '1 hour',now()-interval '1 minute'
FROM generate_series(1,50000) g;
INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
SELECT id,'synthetic' FROM wfc_scale.hub_task
WHERE payload->>'class'='active-backlog' AND (payload->>'ordinal')::int%20 IN (18,19);
INSERT INTO wfc_scale.hub_matching_partition_config(hub_id,partition_count)
SELECT 'hub-'||g,4 FROM generate_series(0,3) g;
INSERT INTO wfc_scale.hub_matching_partition_owner(hub_id,matching_partition,owner_id,owner_url,epoch,lease_until)
SELECT 'hub-'||h,p,'owner-'||h||'-'||p,'http://127.0.0.1',1,now()+interval '1 hour'
FROM generate_series(0,3) h CROSS JOIN generate_series(0,3) p;
