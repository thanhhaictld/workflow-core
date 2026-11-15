# Completion and wakeup write diagnosis

`run_experiment.py` captures read-only statistics and cost-only plans from a
running Docker PostgreSQL instance. It never executes a write, DDL, ANALYZE,
statistics reset, or EXPLAIN ANALYZE on that source.

```powershell
python scaling/benchmarks/hub-completion-write-cost/run_experiment.py `
  --source-container <postgres-container> --user workflow --database workflow `
  --output <new-output-directory>
```

Add `--experiment --plans-only` to reproduce completion with stale/fresh
statistics using the source schema and synthetic rows in a newly created
PostgreSQL 17 container. The current SQL is extracted from
`HubNativeWorkflowStore.CompleteTaskSql`; the baseline is the original query.
Custom and generic plans each run five alternating pairs, with an unmeasured
warmup in each session. Every measured update rolls back. Assertions require
the baseline broad scan to reproduce and every current probe to visit one row
through the primary key.

Add `--experiment` without `--plans-only` to also run the write-attribution
matrix. Each arm/repetition gets its own clone of the synthetic fixture:
baseline, operations index removed, notification trigger disabled, and LZ4
result compression. These are diagnostic arms, not proposed migrations.
Disabling notifications would violate runtime wakeup/capacity behavior.
The write matrix commits each completion/wakeup pair and asserts one completed
and one inserted row per pair. Reported SQL execution times exclude COMMIT
latency; no application concurrency or end-to-end workflow behavior is measured.

The new container has no network access, published ports or shared volumes.
The script stops it in `finally` and retains its synthetic data for inspection.
It never starts/stops other containers. Existing output directories are refused.
Raw output directories are ignored by Git; compact evidence is in `evidence/`.

See [findings](../../docs/investigations/hub-completion-wakeup-query-optimization-20261115.md)
for results, qualification limits, and runtime validation.
