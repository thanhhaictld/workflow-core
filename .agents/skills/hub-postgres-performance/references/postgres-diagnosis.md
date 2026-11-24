# PostgreSQL diagnosis

## Rank interval statements

Verify database identity, PostgreSQL and extension versions, and permissions to see query fingerprints and text. Record `compute_query_id`, statement tracking scope and capacity, and `track_io_timing`. Use fresh read sessions or transactions for the before and after snapshots. The examples target the local PostgreSQL 17 stack. Check version and extension columns when adapting them. Use existing stats as available and preserve the shared counters.

```sql
SELECT clock_timestamp() AS captured_at, current_database() AS database_name,
       pg_postmaster_start_time() AS postmaster_started_at, stats_reset, dealloc
FROM pg_stat_statements_info;

SELECT dbid, userid, queryid, toplevel, stats_since, query,
       calls, total_exec_time, rows, shared_blks_hit, shared_blks_read,
       shared_blks_dirtied, shared_blks_written, temp_blks_read, temp_blks_written,
       shared_blk_read_time, shared_blk_write_time, wal_records, wal_fpi, wal_bytes
FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database());
```

- Diff by `(dbid, userid, queryid, toplevel)` within the same catalog identity. Inspect `stats_since`, global reset time, eviction count and monotonicity. Distinguish newly tracked statements from reset or evicted entries and flag incomplete deltas. Across rebuilt database copies, match normalized SQL and source where query IDs differ. Keep nested statements visible if tracked and avoid summing their elapsed time into a total that already includes the parent. [PostgreSQL 17 statement statistics](https://www.postgresql.org/docs/17/pgstatstatements.html)
- Rank by delta total execution time, calls, rows, logical block accesses, reads, temp activity and WAL. Calculate interval mean as delta execution time / delta calls. Use per-completion counters to find amplification and per-call counters to separate costly calls from high frequency. Preserve total impact and flag zero-row discovery calls. SQL execution time includes waits and concurrent statements can overlap. Buffer hits measure repeated logical accesses. Shared reads can come from the OS cache. An I/O timing column with tracking disabled provides no timing evidence.
- Attribute the SQL to deployed code. Start with [candidate discovery](../../../../scaling/src/WorkflowCore.Hub/HubCandidateReader.cs), [task store](../../../../scaling/src/WorkflowCore.Hub/HubTaskStore.cs), [workflow store](../../../../scaling/src/WorkflowCore.Hub/HubRemoteWorkflowStore.cs), [readiness](../../../../scaling/src/WorkflowCore.Hub.Gateway/HubReadinessBroadcaster.cs) or [operations reads](../../../../scaling/src/WorkflowCore.Hub/HubOperationsStore.cs), depending on the fingerprint. Separate monitoring, audit and periodic scans from required lease, state and history operations.

## Distinguish waits, plans and maintenance

- During the affected interval, take bounded `pg_stat_activity` samples for active sessions in the target database by query ID and wait type. Inspect `pg_blocking_pids(pid)` and transaction age when locks dominate, including idle-in-transaction blockers. Exclude the diagnostic session and separate idle clients. Samples can miss short waits, and a null wait event on an active session alone cannot establish CPU saturation. Cross-check with Hub admission, workflow row-lock and commit histograms and host or container CPU and I/O when available.
- Capture table row estimates, heap/index sizes, dead tuples, HOT updates and last vacuum/analyze for the hot relations. Compare interval `pg_stat_wal`, `pg_stat_checkpointer` and `pg_stat_io` where supported, with reset times and scope. PostgreSQL-wide counters can include unrelated workloads. A higher WAL byte count with fewer records can reflect more full-page images, so inspect `wal_fpi` and checkpoint timing before attributing WAL to indexes or application writes. [PostgreSQL 17 monitoring statistics](https://www.postgresql.org/docs/17/monitoring-stats.html)
- Start with plain `EXPLAIN (FORMAT JSON)` for the actual SQL shape and representative branch, predicates and parameters. Include hot and sparse buckets and a queue with active work. Compare custom and generic prepared plans if prepared reuse is in scope. Examine index bounds, filter versus index condition, order/sorts, scan amplification, estimates and selectivity. Keep plan-time evidence distinct from the actual plan during a recorded run. Capture actual rows/buffers with bounded read-only `EXPLAIN (ANALYZE, BUFFERS)` in an appropriate environment. Use an isolated workload for plans that execute locking or mutating SQL.
- In the ReadyDue investigation, the same due-ordered query used many fewer logical blocks after the partial index predicate matched the deployed SQL and the plan bounded the bucket and deadline. Total PostgreSQL CPU per workflow did not improve in those runs. Record both query-level and whole-cohort effects.

## Review `hub_task` indexes

```sql
SELECT s.indexrelid, s.indexrelname, pg_relation_size(s.indexrelid) AS index_bytes,
       s.idx_scan, s.last_idx_scan, s.idx_tup_read, s.idx_tup_fetch,
       i.indisvalid, i.indisready, i.indisunique, i.indisprimary, i.indisreplident,
       pg_get_indexdef(s.indexrelid) AS definition,
       ARRAY(SELECT c.conname::text FROM pg_constraint AS c
             WHERE c.conindid = s.indexrelid ORDER BY c.conname) AS constraints
FROM pg_stat_user_indexes AS s
JOIN pg_index AS i ON i.indexrelid = s.indexrelid
WHERE s.schemaname = 'wfc_scale' AND s.relname = 'hub_task'
ORDER BY index_bytes DESC, s.indexrelname;
```

- Interpret zero `idx_scan` as no recorded scans over the available statistics history on that server. Check reset history, index identity, reads on replicas and coverage of rare paths such as recovery, compatibility claims and selective monitor filters. Compare index order, keys, expressions, partial predicates, included columns, operator classes and unique semantics before calling indexes equivalent. A shared prefix alone is weak evidence.
- Check constraints, replica identity, `ON CONFLICT` and application write paths even when no catalog constraint owns the index. `ux_hub_task_request` protects keyed enqueue including legacy remote wake-ups. `ux_hub_task_workflow_step` protects legacy numbered steps. Check current migrations, supported workflows and clients before proposing retirement. Inspect representative plan coverage for the indexes that remain read candidates. Zero recorded scans leaves write enforcement and write maintenance costs unmeasured.
- For an authorized retirement, follow the migration/replay and data-preservation rules in `AGENTS.md`. Validate retained indexes and affected write/read semantics. Compare disk usage, statement and WAL work, latency distributions and throughput under matched conditions. A before/after throughput difference can point to a stage worth investigating even when the reason for the change remains open.
