-- Run on the synthetic efficiency lab after seed-workload.sql.
\pset pager off
\echo sparse-ready-64-bucket-page-probes
WITH buckets AS (SELECT generate_series(0,63) AS bucket), pages AS (
  SELECT b.bucket,p.rows,p.resource_keys
  FROM buckets b CROSS JOIN LATERAL (
    WITH page AS MATERIALIZED (
      SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,due_at AS deadline
      FROM wfc_scale.hub_task
      WHERE hub_id='sparse-lab' AND partition_id%64=b.bucket AND attempts<max_attempts
        AND sequence<=999999999 AND state=0 AND due_at<=statement_timestamp() AND sequence>0
      ORDER BY sequence LIMIT 128
    ), resource_sizes AS MATERIALIZED (
      SELECT c.task_id,sum(octet_length(c.resource_key)+24) AS bytes
      FROM wfc_scale.hub_task_constraint c JOIN page p ON p.id=c.task_id GROUP BY c.task_id
    ), resources AS MATERIALIZED (
      SELECT c.task_id,array_agg(c.resource_key ORDER BY c.resource_key) AS keys
      FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
      JOIN page p ON p.id=c.task_id
      WHERE s.bytes+128+octet_length(p.hub_id)+octet_length(p.task_type)<=512 GROUP BY c.task_id
    )
    SELECT count(*) AS rows,sum(COALESCE(array_length(r.keys,1),0)) AS resource_keys
    FROM page p LEFT JOIN resource_sizes s ON s.task_id=p.id LEFT JOIN resources r ON r.task_id=p.id
  ) p
)
SELECT 'sparse-ready' AS stream,count(*) AS page_calls,count(*) FILTER(WHERE rows=0) AS empty_pages,
       sum(rows) AS rows,sum(resource_keys) AS resource_keys FROM pages;

\echo active-ready-64-bucket-page-probes
WITH buckets AS (SELECT generate_series(0,63) AS bucket), pages AS (
  SELECT b.bucket,p.rows,p.resource_keys
  FROM buckets b CROSS JOIN LATERAL (
    WITH page AS MATERIALIZED (
      SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,due_at AS deadline
      FROM wfc_scale.hub_task
      WHERE hub_id='efficiency-lab' AND partition_id%64=b.bucket AND attempts<max_attempts
        AND sequence<=999999999 AND state=0 AND due_at<=statement_timestamp() AND sequence>0
      ORDER BY sequence LIMIT 128
    ), resource_sizes AS MATERIALIZED (
      SELECT c.task_id,sum(octet_length(c.resource_key)+24) AS bytes
      FROM wfc_scale.hub_task_constraint c JOIN page p ON p.id=c.task_id GROUP BY c.task_id
    ), resources AS MATERIALIZED (
      SELECT c.task_id,array_agg(c.resource_key ORDER BY c.resource_key) AS keys
      FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
      JOIN page p ON p.id=c.task_id
      WHERE s.bytes+128+octet_length(p.hub_id)+octet_length(p.task_type)<=512 GROUP BY c.task_id
    )
    SELECT count(*) AS rows,sum(COALESCE(array_length(r.keys,1),0)) AS resource_keys
    FROM page p LEFT JOIN resource_sizes s ON s.task_id=p.id LEFT JOIN resources r ON r.task_id=p.id
  ) p
)
SELECT 'active-ready' AS stream,count(*) AS page_calls,count(*) FILTER(WHERE rows=0) AS empty_pages,
       sum(rows) AS rows,sum(resource_keys) AS resource_keys FROM pages;

\echo active-nonexpired-lease-setup
UPDATE wfc_scale.hub_task SET lease_until=statement_timestamp()+interval '30 days'
WHERE hub_id='efficiency-lab' AND state=1;
\echo active-nonexpired-expiry-64-bucket-page-probes
WITH buckets AS (SELECT generate_series(0,63) AS bucket), pages AS (
  SELECT b.bucket,p.rows,p.resource_keys
  FROM buckets b CROSS JOIN LATERAL (
    WITH page AS MATERIALIZED (
      SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,lease_until AS deadline
      FROM wfc_scale.hub_task
      WHERE hub_id='efficiency-lab' AND partition_id%64=b.bucket AND attempts<max_attempts
        AND sequence<=999999999 AND state=1 AND lease_until<statement_timestamp()
        AND (lease_until,sequence)>(timestamp '0001-01-01',0)
      ORDER BY lease_until,sequence LIMIT 128
    ), resource_sizes AS MATERIALIZED (
      SELECT c.task_id,sum(octet_length(c.resource_key)+24) AS bytes
      FROM wfc_scale.hub_task_constraint c JOIN page p ON p.id=c.task_id GROUP BY c.task_id
    ), resources AS MATERIALIZED (
      SELECT c.task_id,array_agg(c.resource_key ORDER BY c.resource_key) AS keys
      FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
      JOIN page p ON p.id=c.task_id
      WHERE s.bytes+128+octet_length(p.hub_id)+octet_length(p.task_type)<=512 GROUP BY c.task_id
    )
    SELECT count(*) AS rows,sum(COALESCE(array_length(r.keys,1),0)) AS resource_keys
    FROM page p LEFT JOIN resource_sizes s ON s.task_id=p.id LEFT JOIN resources r ON r.task_id=p.id
  ) p
)
SELECT 'active-nonexpired-expiry' AS stream,count(*) AS page_calls,count(*) FILTER(WHERE rows=0) AS empty_pages,
       sum(rows) AS rows,sum(resource_keys) AS resource_keys FROM pages;

\echo expired-lease-setup
WITH selected AS (
  SELECT id FROM wfc_scale.hub_task WHERE hub_id='efficiency-lab' AND state=1
  ORDER BY sequence LIMIT 5000
)
UPDATE wfc_scale.hub_task t SET lease_until=statement_timestamp()-interval '30 seconds'
FROM selected s WHERE s.id=t.id;
\echo expired-lease-64-bucket-page-probes
WITH buckets AS (SELECT generate_series(0,63) AS bucket), pages AS (
  SELECT b.bucket,p.rows,p.resource_keys
  FROM buckets b CROSS JOIN LATERAL (
    WITH page AS MATERIALIZED (
      SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,lease_until AS deadline
      FROM wfc_scale.hub_task
      WHERE hub_id='efficiency-lab' AND partition_id%64=b.bucket AND attempts<max_attempts
        AND sequence<=999999999 AND state=1 AND lease_until<statement_timestamp()
        AND (lease_until,sequence)>(timestamp '0001-01-01',0)
      ORDER BY lease_until,sequence LIMIT 128
    ), resource_sizes AS MATERIALIZED (
      SELECT c.task_id,sum(octet_length(c.resource_key)+24) AS bytes
      FROM wfc_scale.hub_task_constraint c JOIN page p ON p.id=c.task_id GROUP BY c.task_id
    ), resources AS MATERIALIZED (
      SELECT c.task_id,array_agg(c.resource_key ORDER BY c.resource_key) AS keys
      FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
      JOIN page p ON p.id=c.task_id
      WHERE s.bytes+128+octet_length(p.hub_id)+octet_length(p.task_type)<=512 GROUP BY c.task_id
    )
    SELECT count(*) AS rows,sum(COALESCE(array_length(r.keys,1),0)) AS resource_keys
    FROM page p LEFT JOIN resource_sizes s ON s.task_id=p.id LEFT JOIN resources r ON r.task_id=p.id
  ) p
)
SELECT 'expired-lease' AS stream,count(*) AS page_calls,count(*) FILTER(WHERE rows=0) AS empty_pages,
       sum(rows) AS rows,sum(resource_keys) AS resource_keys FROM pages;
