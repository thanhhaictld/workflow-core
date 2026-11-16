# Hub task-ID hints stage 3 evidence

## Source and runtime

- Baseline source: `ca190584e1f84bf952223cff95731d914d0a5280`.
- Stage 2 reviewed source: `184fd8cd09472a828d445525a9e6fa3b9fb297c6`.
- Stage 3 source is recorded by the commit containing this report.
- PostgreSQL was a disposable `postgres:17` container; tests used an isolated schema migrated through 032, while query-plan work used fresh disposable databases copied from that schema. No shared database was reset.
- The DB tests were run with `WFC_TEST_PG` set to the disposable endpoint. No connection credentials are recorded here.

## Query-shape comparison

Reproduce against a fresh isolated PG17 database migrated through 032:

```powershell
Get-Content -Raw scaling/benchmarks/hub-task-id-hints-stage3/query-cost.sql |
  docker exec -i <pg17-container> psql -X -U workflow -d <fresh-db> -f - |
  Set-Content -Encoding utf8 scaling/benchmarks/hub-task-id-hints-stage3/pg17-query-cost.txt
```

The workload seeded 50,000 ready tasks in one Hub and discovery bucket, attached one shared strict-FIFO key to the 128-row page, analyzed the tables, and ran generic prepared statements with `EXPLAIN (ANALYZE,BUFFERS,SETTINGS,FORMAT JSON)`. The baseline statement is the indexed `BuildPageSql(ReadyDue)` shape from `ca190`; the final statement is the production ID hydration shape. Both returned 128 rows in the batched comparison, while a single-ID final hydration returned one row.

| Query | Rows | Execution time | Shared hit blocks | Access shape |
|---|---:|---:|---:|---|
| Baseline indexed discovery page | 128 | 5.517 ms | 11 | `ix_hub_task_candidate_ready_bucket_due` plus bounded metadata joins |
| Final ID hydration, 128 IDs | 128 | 9.048 ms | 388 | requested-ID materialization with `hub_task_pkey` probes and constraint joins |
| Final ID hydration, one ID | 1 | 0.234 ms | 7 | one `hub_task_pkey` probe and bounded joins |

The single-ID result is lower-cost for a sparse hint; a full 128-row hydration batch cost more than one indexed page in this run. This is a query-level observation, not an end-to-end speed claim. Candidate matching still runs the existing two-second ready-hub reconciliation and indexed recovery to preserve its previous missed-hint envelope. Therefore these measurements do **not** show a net system DB-call or CPU reduction; a production-shaped baseline/final trial with identical recovery, worker counts, and constrained workload remains an open performance acceptance item. Permit-release and policy hints intentionally remain hub-wide and can trigger scans.

The same disposable workload wrote 50,000 outbox rows for task inserts. A synthetic committed claim added one row, a lease-only heartbeat added zero, and completion added two (task invalidation plus permit release), for 50,003 total. This verifies trigger event volume and heartbeat exclusion, not dispatcher throughput. The raw plan output is `pg17-query-cost.txt`.

## Functional validation

Run with the same disposable `WFC_TEST_PG` setting:

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release --no-restore
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release --no-build --logger "console;verbosity=minimal"
pwsh -NoProfile -File scaling/benchmarks/hub-matching-failover-suite.ps1 -Output scaling/benchmarks/results/stage3-hub-candidate
git diff --check
```

Release solution build succeeded with zero errors and four existing analyzer warnings. The focused suite passed 51 tests, 0 failed, 0 skipped; the full PostgreSQL suite passed 332 tests, 0 failed, 0 skipped in 9m55s against a fresh PG17 schema applied through the CI migration sequence. After the full run, the final listener partition-64/4095 owner-routing test was rerun alone and passed (1/1) following an assertion-order-only change. The three owner/SSE failover windows passed: before claim (10.16s takeover, 20 tasks complete), after claim (7.34s takeover, 2 retries, stale heartbeat rejected with 409), and during execution (10.20s takeover, 20 tasks complete); all reported zero duplicate same-epoch starts, FIFO overlaps, and leaked reservations/uncertain claims. Raw failover results and execution/effect logs are in `scaling/benchmarks/results/stage3-hub-candidate/`.

Targeted regression coverage includes actual listener routing for task partitions 64 and 4095, old coarse typed hints at those partitions, duplicate IDs and Unicode/combined hydration byte limits, SQL suppression of oversized base metadata, recovery after hydration failure, exact/recovery page fairness, concurrent notification flood/overflow, local and periodic recovery wakeups, owner loss during hydration, same-ID rehint, terminal invalidation, no-notification due/expiry recovery, direct/outbox commit and rollback, policy update, resource-release hints, and the existing reservation/uncertainty/FIFO/unknown-constraint admission cases. The full PostgreSQL suite also exercises the existing lease and SSE delivery guarantees.

## Rollout and remaining qualification

Apply additive migration 032 after 031 through CI, development, and Compose setup before deploying the new listener. Keep periodic indexed recovery enabled; the outbox is transactionally dispatched but not replayable. Roll back by returning applications to old listeners while retaining 032; reverse the migration only after the old application is restored and only if required.

Acceptance remains open for the end-to-end five-run baseline/final benchmark matrix and measured latency bounds under disabled LISTEN/outbox, just-after-sweep arrivals, future due work, and expiry with both demand modes. The query plan comparison supports the bounded primary-key shape but does not qualify aggregate performance.
