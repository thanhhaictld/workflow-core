# PostgreSQL batch-admission experiment

This is an isolated, synthetic-data experiment for `HubTaskStore.ClaimCandidateBatchAsync`. It does not modify runtime code or deploy a change. `baseline.sql` is extracted from the production raw string; `prototype.sql` is an experiment-only selected-ID-driven rewrite.

## Reproduce

From the repository root in PowerShell 7:

```powershell
pwsh -NoProfile -File ./scaling/benchmarks/hub-batch-admission-experiment/setup.ps1 -ExperimentName wfc-pg17-batch-yourname-yyyymmdd
$port = [int]((docker port wfc-pg17-batch-yourname-yyyymmdd 5432/tcp) -split ':')[-1]
python ./scaling/benchmarks/hub-batch-admission-experiment/extract_sql.py
$env:WFC_BATCH_RAW_DIR = Join-Path $env:APPDATA 'orca/chat-scratch/e05cc0fe1524a451e83ff74cdee008bf/postgres-4k-batch-experiment-luna-high/raw-plans/deployed-index-no028'
pwsh -NoProfile -File ./scaling/benchmarks/hub-batch-admission-experiment/run-matrix.ps1 -Port $port
python ./scaling/benchmarks/hub-batch-admission-experiment/verify-results.py --raw-root $env:WFC_BATCH_RAW_DIR
$env:WFC_BATCH_PORT = "$port"; $env:WFC_BATCH_DB = 'hub_batch_tests'
Push-Location ./scaling/benchmarks/hub-batch-admission-experiment
dotnet run --project BatchExperiment.csproj -- baseline.sql correctness raw/correctness prototype.sql
dotnet run --project BatchExperiment.csproj -- prototype.sql concurrency raw/concurrency
Pop-Location
$env:WFC_TEST_PG = "Host=127.0.0.1;Port=$port;Database=hub_batch_tests;Username=postgres;Pooling=false;Timeout=10;Command Timeout=60"
dotnet test ./scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release --filter 'FullyQualifiedName~HubCandidateBatchTests|FullyQualifiedName~HubUnconstrainedBatchClaimTests'
```

The setup refuses to reuse an existing container. It creates only the named disposable container, with a unique `wfc.experiment` label, `postgres:17`, `--cpus=2`, `--memory=2g`, `--memory-swap=2g`, and a random host port bound to `127.0.0.1`. PostgreSQL uses local trust authentication and no password is set. The commands do not touch the shared observability container. Large raw plans are written under `WFC_BATCH_RAW_DIR` in scratch; the owned benchmark folder keeps the harness, compact summaries and small correctness/resource metadata. When finished, retain the container/data and stop it with:

```powershell
pwsh -NoProfile -File ./scaling/benchmarks/hub-batch-admission-experiment/stop-container.ps1 -ExperimentName wfc-pg17-batch-luna-20261115 -ExperimentLabel postgres-batch-admission-luna-20261115
```

Use the actual experiment container name in the stop command. Do not remove the container if its database files are part of the evidence.

## Database and fixture

`setup.ps1` applies the production-matching deployed migration set 002–027, 029, and 030 by default, without inventing a task schema. Migration 028 is excluded because the observed deployed catalog does not contain its shard-4 ready capability index; it can be opted into only for a separately labeled nondeployment arm with `-Include028`. If running that optional arm, use a separate container name and point `WFC_BATCH_RAW_DIR` to a path named `index-arm-028-nondeployment`. The harness records the actual migration filenames in `raw/resource.json` and the run log. It loads `seed.sql` (64,000 completed and 50,000 active backlog tasks) plus 25,000 synthetic completed history rows, then analyzes the 139,000 row baseline. Hubs and task partitions are distributed across four IDs. The 50,000-row active set contains 80% due-ready tasks, 5% future-ready, 5% live-leased, and 10% constrained tasks; activity types and versions are mixed, and selected candidates advertise all 24 hub/type/version capability combinations. Workflow pins are null in the throughput matrix. `hub_batch_tests` is a separate database clone for correctness checks and the existing integration suite.

For stale pre-burst statistics, setup clones the analyzed baseline, disables table autovacuum only in that disposable clone, analyzes at 139,000 rows, then appends 25,000 active due-ready rows. The stale test therefore has 164,000 rows and `pg_class.reltuples=139,000`; active rows increase from 50,000 to 75,000. This is an explicit synthetic active-backlog stale-statistics condition, not the prior completed-history append. The analyzed arm remains 139,000 rows. The paired baseline and prototype in each stats condition use the same database, rows, buffers, and rollback-restored task state. Repeated rollbacks create dead tuple/index versions; autovacuum is disabled to prevent between-arm cleanup, and order is alternated, but accumulated MVCC versions, cache warming, and possible buffer-state drift remain limitations.

## Matrix and preparation behavior

The matrix tests batches 1/8/16/32, earliest/latest eligible IDs by `hub_task.sequence`, forced custom and generic PostgreSQL plans, and an unprepared Npgsql path. There are two warmups plus five measured executions per arm/cell. Baseline and prototype execute in alternating order for each cell and each transaction rolls back, so every sample leases the same starting rows. Plans are captured with `EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT JSON,TIMING OFF)` against the isolated container. Raw plan JSON is kept under `$WFC_BATCH_RAW_DIR/paired-*`; `cell-summary.csv` and `cell-summary.md` are generated in this benchmark directory.

The production `HubTaskStore` builds `NpgsqlDataSource` after only setting max pool size, and its batch command does not call `Prepare`/`PrepareAsync`. Npgsql 9.0.4 defaults `Max Auto Prepare` to zero unless the supplied connection string overrides it. The harness explicitly sets `Max Auto Prepare=0`; its application-mode calls stay unprepared, while custom/generic comparison arms explicitly prepare and force `plan_cache_mode`. Those two forced arms are controls, not claims about production's plan mode.

`correctness` takes explicit separate baseline and prototype paths and fails closed when either file hashes or normalized SQL hashes match. It compares both `RETURNING` sets on a synthetic edge set (worker ownership, separate per-item capability/pin failures, observed epoch, due-ready/expired/live/future lease conditions, attempts exhausted, native Runnable status, and constraints), checking rejected owner/state/attempt/epoch values remain unchanged. `concurrency` runs two overlapping prototype batches with opposite JSON order while the first transaction holds the lease locks. The filtered existing PostgreSQL tests run only against `hub_batch_tests` and exercise the production baseline store's owner fence, attempts/history, rollback, ambiguous commit and concurrent claims; they do not qualify prototype owner fencing/history/commit ambiguity or its store integration.

## Interpretation boundaries

The plan's server `Execution Time` is SQL elapsed time; `Planning Time`, Npgsql client roundtrip, shared buffer blocks, WAL, and rows returned by the update are reported separately. CPU was not measured. `EXPLAIN ANALYZE` preserves and evaluates the full SQL `RETURNING` expression, but does not send production task payload rows over the network. This is a single-container SQL microbenchmark, not workflow throughput or full HA/store qualification. Prior results captured with `-Include028` are retained in scratch under `raw-plans/index-arm-028-nondeployment/` and must not be read as deployment-matching evidence.
