WITH picked AS MATERIALIZED (
                    SELECT t.id,t.sequence,s.worker FROM wfc_scale.hub_task t
                    JOIN jsonb_to_recordset(@selected) AS s(id uuid,epoch bigint,worker text,capabilities jsonb,pins jsonb) ON s.id=t.id AND s.epoch=t.epoch
                    WHERE t.hub_id=@hub AND t.partition_id % @count=@partition
                      AND EXISTS (SELECT 1 FROM jsonb_to_recordset(s.capabilities) AS c(hub text,task_type text,version integer)
                          WHERE c.hub=t.hub_id AND c.task_type=t.task_type AND c.version=t.task_version)
                      AND t.attempts<t.max_attempts AND ((t.state=0 AND t.due_at<=now()) OR (t.state=1 AND t.lease_until<now()))
                      AND NOT EXISTS (SELECT 1 FROM wfc_scale.hub_task_constraint c WHERE c.task_id=t.id)
                      AND NOT EXISTS (SELECT 1 FROM wfc_scale.hub_workflow w WHERE w.id=t.workflow_id AND w.execution_mode='native' AND w.native_status<>'Runnable')
                      AND (t.task_type NOT IN ('__workflow__','__native_workflow__') OR s.pins IS NULL OR s.pins = 'null'::jsonb OR EXISTS (
                          SELECT 1 FROM wfc_scale.hub_workflow w,jsonb_to_recordset(s.pins) AS p(hub text,definition text,version integer)
                          WHERE w.id=t.workflow_id AND w.hub_id=p.hub AND w.definition_id=p.definition AND w.definition_version=p.version))
                    ORDER BY t.sequence FOR UPDATE OF t SKIP LOCKED
                )
                UPDATE wfc_scale.hub_task t SET state=1,owner=p.worker,last_owner=p.worker,epoch=t.epoch+1,
                    attempts=t.attempts+1,lease_until=now()+@lease,updated_at=now()
                FROM picked p WHERE t.id=p.id
                RETURNING t.id,t.hub_id,t.partition_id,t.task_type,t.task_version,t.workflow_id,t.payload::text,
                    t.epoch,t.attempts,t.lease_until,COALESCE(t.workflow_step,t.native_step_id)
