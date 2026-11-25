# Experiment: batch versus single PostgreSQL task commit

## Scope
Base: PR #41 source branch `thanhhaictld/hub-memory-candidates-luna` at `647c5452`, after merging `master`. This experiment is isolated on `experiment/task-completion-batch-commit-pg-io`.

Compare **one commit per native activity result** with **one commit per batch of 16 independent activity results**, with 8 concurrent callers. Both modes run exactly the same task/attempt/inbox/history/wake SQL from `HubNativeWorkflowStore` and retain owner+epoch fencing. The batch method limits calls to 32 entries and sorts by workflow ID before locking. Success is acknowledged only after the containing transaction commits. If any SQL error aborts a batch, none of its results is acknowledged, and the caller must retry with the same task epochs.

The new batch API is **not connected to the live gateway**. There is no queue, delayed completion or change to the default single-task REST completion route. The A/B tests call both production persistence methods directly in one benchmark harness.

## Why use this workload
Each fixture creates a distinct native workflow with one already-leased native activity task and an attempt record, using the current schema. During the timed section the store completes the task, persists attempt finish, inbox result, workflow event and wake-up task. All five durable effects are checked for exactly once. Preparation (fixture inserts, worker execution and candidate discovery) is excluded from throughput timing so the experiment isolates completion persistence instead of measuring ingestion or scheduling.

The same runner executes a paired, reversed sequence `single → batch → batch → single`, with new equal-sized fixtures, warm-up, identical PostgreSQL settings and concurrency. This helps reduce but cannot remove GitHub runner noise. Each of the four samples completes 600 activities; the batch size is 16.

## PostgreSQL measurements
- **Throughput / completion ACK p50, p95, p99:** timed completion calls, one latency observation per task including its containing batch commit, without producer queue delay.
- **PostgreSQL CPU:** `usage_usec` delta from the PostgreSQL service container's cgroup `cpu.stat`. Report CPU-ms/activity rather than total runner CPU.
- **Physical I/O:** PostgreSQL container cgroup `io.stat` rbytes/wbytes delta.
- **PostgreSQL buffer I/O:** `pg_stat_io` read/write/extend/fsync/hit counts and timings.
- **WAL:** `pg_stat_wal` bytes, records, writes/syncs and times.
- **Transactions / logical reads:** `pg_stat_database` commits, rollbacks, blocks read/hit, rows inserted/updated and I/O time. `track_io_timing` and `track_wal_io_timing` are enabled in the job.

All raw deltas, CPU, I/O and aggregate results are archived in `native-completion-commit-ab.json`; the GitHub step summary includes paired averages and percentage changes. No arbitrary performance gain threshold is used as a correctness gate.

## Interpretation
The batch path still issues per-task SQL statements. It reduces database **transaction commits**, not the number of task-row updates or WAL bytes necessarily. It intentionally differs from PR #43's data-modifying CTE (which regressed). Batching unrelated activities into one transaction increases transaction duration, commit blast radius and potential cross-workflow head-of-line blocking. Measure p95/p99 and deadlocks in addition to throughput.

This is a persistence microbenchmark, **not a 1,000-workflow end-to-end benchmark** and not proof PostgreSQL has reached its throughput ceiling. The production Gateway needs a separate, explicitly opt-in, durable-safe microbatch admission policy before these gains (if any) can apply to live worker traffic.

## To reproduce
Run the CI workflow `.github/workflows/native-task-commit-postgres-ab.yml` or apply the same runtime schema and run:

```sh
export WFC_TEST_PG='Host=localhost;Port=5432;Database=workflow;Username=workflow;Password=workflow_dev_only'
export BENCH_COUNT=600 BENCH_CONCURRENCY=8 BENCH_BATCH_SIZE=16
dotnet run -c Release --project scaling/benchmarks/native-completion-commit-ab/NativeCompletionCommitAb.csproj
```
