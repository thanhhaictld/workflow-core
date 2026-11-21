-- Run inside the retained isolated wfc_hub_efficiency_20261118 database.
-- This is the current HubCandidateReader page shape with bounded enrichment.
\pset pager off
\timing on
SELECT COALESCE(max(sequence),0) AS upper FROM wfc_scale.hub_task \gset
\echo sparse-ready-empty-page
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH page AS MATERIALIZED (
  SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,due_at AS deadline
  FROM wfc_scale.hub_task
  WHERE hub_id='sparse-lab' AND partition_id % 64=0 AND attempts<max_attempts
    AND sequence<=:upper AND state=0 AND due_at<=statement_timestamp() AND sequence>0
  ORDER BY sequence LIMIT 128
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
ORDER BY p.sequence;

\echo active-ready-page
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH page AS MATERIALIZED (
  SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,due_at AS deadline
  FROM wfc_scale.hub_task
  WHERE hub_id='efficiency-lab' AND partition_id % 64=0 AND attempts<max_attempts
    AND sequence<=:upper AND state=0 AND due_at<=statement_timestamp() AND sequence>0
  ORDER BY sequence LIMIT 128
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
ORDER BY p.sequence;

\echo active-expiry-empty-page
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH page AS MATERIALIZED (
  SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,lease_until AS deadline
  FROM wfc_scale.hub_task
  WHERE hub_id='efficiency-lab' AND partition_id % 64=0 AND attempts<max_attempts
    AND sequence<=:upper AND state=1 AND lease_until<statement_timestamp()-interval '2 minutes'
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

\echo expired-lease-page
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

\echo current-indexes-and-table-statistics
SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='wfc_scale' AND tablename IN ('hub_task','hub_task_constraint') ORDER BY tablename,indexname;
SELECT s.relname,c.reltuples,c.relpages,s.last_analyze,s.last_autoanalyze FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid=s.relid
WHERE s.schemaname='wfc_scale' AND s.relname IN ('hub_task','hub_task_constraint');
SELECT count(*) AS rows, count(*) FILTER(WHERE state=0 AND due_at<=clock_timestamp()) AS due_ready,
       count(*) FILTER(WHERE state=0 AND due_at>clock_timestamp()) AS future_ready,
       count(*) FILTER(WHERE state=1 AND lease_until>=clock_timestamp()) AS live_leases,
       count(*) FILTER(WHERE state=1 AND lease_until<clock_timestamp()) AS expired_leases
FROM wfc_scale.hub_task WHERE hub_id='efficiency-lab';
