-- psql -v run_id=RUN_ID -f hub-push-metrics.sql
WITH workflows AS (
  SELECT * FROM wfc_scale.hub_workflow WHERE run_id=:'run_id'
), tasks AS (
  SELECT t.* FROM wfc_scale.hub_task t JOIN workflows w ON w.id=t.workflow_id
), claim_events AS (
  SELECT e.task_id, min(e.occurred_at) AS claimed_at
  FROM wfc_scale.hub_workflow_event e JOIN workflows w ON w.id=e.workflow_id
  WHERE e.event_type='WorkflowTaskClaimed' GROUP BY e.task_id
), attempts AS (
  SELECT a.task_id, min(a.claimed_at) AS claimed_at
  FROM wfc_scale.hub_task_attempt a JOIN workflows w ON w.id=a.workflow_id GROUP BY a.task_id
), starts AS (
  SELECT t.id, t.created_at, LEAST(a.claimed_at, e.claimed_at) AS claimed_at
  FROM tasks t LEFT JOIN attempts a ON a.task_id=t.id LEFT JOIN claim_events e ON e.task_id=t.id
), delays AS (
  SELECT EXTRACT(epoch FROM (claimed_at-created_at))*1000 AS milliseconds
  FROM starts WHERE claimed_at IS NOT NULL
), durations AS (
  SELECT EXTRACT(epoch FROM (completed_at-created_at))*1000 AS milliseconds
  FROM workflows WHERE completed_at IS NOT NULL
)
SELECT json_build_object(
  'runId', :'run_id',
  'workflowCount', (SELECT count(*) FROM workflows),
  'completedWorkflows', (SELECT count(*) FROM workflows WHERE completed_at IS NOT NULL),
  'completedTasks', (SELECT count(*) FROM tasks WHERE state=2),
  'taskCount', (SELECT count(*) FROM tasks),
  'tasksWithStartTime', (SELECT count(*) FROM delays),
  'scheduleStartP50Ms', (SELECT percentile_cont(.5) WITHIN GROUP (ORDER BY milliseconds) FROM delays),
  'scheduleStartP95Ms', (SELECT percentile_cont(.95) WITHIN GROUP (ORDER BY milliseconds) FROM delays),
  'scheduleStartP99Ms', (SELECT percentile_cont(.99) WITHIN GROUP (ORDER BY milliseconds) FROM delays),
  'endToEndP95Ms', (SELECT percentile_cont(.95) WITHIN GROUP (ORDER BY milliseconds) FROM durations),
  'endToEndP99Ms', (SELECT percentile_cont(.99) WITHIN GROUP (ORDER BY milliseconds) FROM durations),
  'tasksRetried', (SELECT count(*) FROM tasks WHERE attempts>1),
  'extraClaims', (SELECT coalesce(sum(greatest(attempts-1,0)),0) FROM tasks),
  'leaseExpiries', (SELECT count(*) FROM wfc_scale.hub_task_attempt a JOIN tasks t ON t.id=a.task_id WHERE a.outcome='LeaseExpired'),
  'deadTasks', (SELECT count(*) FROM tasks WHERE state=3)
);
