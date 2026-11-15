# Hub task stale-statistics plan reproduction

`reproduce-stale-partial-index.py` starts its own uniquely named, labeled
PostgreSQL 17 container on `--network none` with 2 CPUs and 2 GiB memory. It
disables autovacuum only on the synthetic `hub_task` table, loads 144,000 rows,
analyzes the fully completed/empty partial-index state, then activates 44,000
rows without ANALYZE. The checked-in `before-selected-id.sql` is the exact
pre-fix query; the current SQL is extracted from `HubTaskStore.cs` at run time.

Run from repository root and place raw plans outside the worktree:

```powershell
python scaling/benchmarks/hub-task-spike-diagnosis/reproduce-stale-partial-index.py `
  --output C:/Users/<you>/AppData/Roaming/orca/chat-scratch/<task>/postgres-hub-task-fix
```

For stale-empty, fresh 44k, and post-ANALYZE churn states, the script compares
both SQL versions at batch sizes 1/8/16/32 under forced custom and generic
plans. Each `EXPLAIN ANALYZE` executes inside a transaction and rolls back. It
also checks empty selection, missing IDs, and stale epochs. The script fails if
the matched baseline no longer reproduces over one million scanned rows at
batch 32, or if the current statement exceeds four `hub_task` visits per
selected item. Raw JSON plans and a compact CSV summary are written only to the
requested scratch directory; the owned container is stopped and auto-removed.

This isolates query plan behavior. It is not an end-to-end Hub workload, does
not model all schema indexes or concurrent activity, and cannot support a CPU
or throughput-gain claim.
