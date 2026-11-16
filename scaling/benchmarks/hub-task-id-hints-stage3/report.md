# Hub task-ID hint qualification

## Decision

Keep the bounded task-ID path **experimental and off by default**. The five-run paired runtime comparison does not show a dependable end-to-end win: the opt-in candidate generated about 2.0–3.1× as many measured SQL statements per task in burst/steady/FIFO and approximately the same count in sparse, with higher median database execution time per task in every cell. It also doubled matching outbox writes from 2 to 4 per task and increased matching NOTIFY producer rows from 4 to 6 per task. Some sparse/FIFO latency repetitions improved, but other paired runs had large delays and the paired ranges overlap; these results do not justify enabling the feature by default. Keep indexed recovery, local readiness wakeups and the two-second reconciliation cadence unchanged.

`WFC_HUB_MATCHING_CANDIDATES_ENABLED=true` opts a Hub process and its `HubTaskStore` writer sessions into the feature. Normal Compose presets and Makefile commands leave it false. Migration 032 is additive and keeps old coarse payload behavior when the session flag is off. Rollout: apply 032 after 031, then opt in all Hub owners together; keep periodic recovery and outbox dispatch running. Rollback: set the flag false and restart Hub owners/writers; leave migration 032 installed. Do not reverse 032 while any new listener or writer remains deployed.

## Runtime comparison

Source baseline was `ca190584e1f84bf952223cff95731d914d0a5280`; candidate runtime source was built from `190c72b0c43bc4e2ed80c1c748df74d528b3b913` plus the working-tree qualification changes whose per-file hashes are recorded in [`manifest.json`](raw-runtime-qualified/manifest.json). Hub and worker binary hashes, migration 032 hash, runtime options, PostgreSQL settings, and exact database names are in that manifest. No credentials are included. Both database arms were isolated, began from equal 20,000-row terminal-task histories with zero active workflows, used PostgreSQL 17 with `pg_stat_statements.track=all` (including nested trigger SQL) and `track_io_timing=on`, two Hub owners, two matching partitions, four worker lane instances, push enabled, queue admission limit 3, batch size 32, and the same database-level outbox/dispatcher setup. A warmup preceded five interleaved baseline/candidate repetitions per scenario; all 40 measured invocations completed without workload/metrics failures, retries, lease expiries, dead tasks, or missing workflows. Raw observations including each repetition and warmups are in [`records.json`](raw-runtime-qualified/records.json); [`paired-summary.json`](paired-summary.json) preserves per-repetition values and paired order.

Values below are medians across five repetitions; `duration` and per-run workflow P95 ranges show min–max across those repetitions. P95/P99 are per-run workflow latency percentiles (40 samples for 8-workflow cells, 100 for 20-workflow cells), summarized by the median repetition. DB execution time is PostgreSQL statement execution, not CPU time. DB statements, buffer hits, and WAL are normalized by durably completed tasks. Candidate page/hydration query counts are separated; baseline's indexed claim/discovery SQL is in `task-other`, so its page count is not comparable by that classifier.

The accepted qualification set is only `raw-runtime-qualified`. Other retained `raw-*` directories are explicitly superseded setup/pilot evidence: `raw-runtime-interrupted-pre-gating` stopped when the old schema lacked the gate; `raw-runtime-interrupted-pre-outbox-config` stopped when the cloned DB lacked outbox configuration; `raw-runtime-full` predates complete SQL classification and feature-gated setup; `raw-query-profile` and `raw-query-profile-settled` are query-classification pilots; `raw-runtime` is a workload pilot; `raw-runtime-20workflow-diagnostic` is a two-run strict-FIFO size diagnostic with impractically long baseline durations. They are retained for audit but excluded from every table and conclusion here. The final failover directory includes one failed harness-configuration attempt and the three passing reruns described below.

| Workload | Arm | Duration s (range) | Durable tasks/s | Workflow P95 ms (range) / P99 ms | SQL statements/task | DB exec ms/task | Shared hits/task | WAL bytes/task | Candidate discovery calls/rows | ID hydration calls/rows |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Burst, 20 workflows / 200 tasks | Baseline | 9.22 (8.77–91.85) | 21.68 | 1,701 (1,529–30,690) / 1,781 | 64.8 | 5.72 | 764 | 14,076 | n/a | n/a |
| Burst | Candidate | 63.52 (10.22–104.37) | 3.15 | 33,261 (2,059–36,900) / 33,335 | 202.9 | 6.77 | 1,122 | 18,812 | 10,410 / 779 | 266 / 193 |
| Steady, 20 / 200, 400 ms stagger | Baseline | 11.71 (9.48–128.12) | 17.08 | 4,778 (1,504–33,215) / 4,965 | 66.4 | 4.73 | 926 | 17,670 | n/a | n/a |
| Steady | Candidate | 106.09 (11.26–170.28) | 1.89 | 35,836 (2,245–98,851) / 36,304 | 265.8 | 7.37 | 1,120 | 18,527 | 13,954 / 1,201 | 279 / 193 |
| Sparse, 8 / 80, 1.5 s stagger | Baseline | 127.29 (12.10–162.52) | 0.63 | 32,453 (1,221–33,044) / 32,467 | 247.7 | 5.92 | 922 | 24,999 | n/a | n/a |
| Sparse | Candidate | 13.41 (13.34–14.28) | 5.97 | 1,720 (1,591–3,353) / 1,829 | 252.0 | 6.79 | 1,011 | 16,930 | 6,605 / 39 | 150 / 72 |
| Shared-resource strict FIFO, 8 / 80 | Baseline | 5.36 (4.75–30.63) | 14.93 | 3,646 (1,547–29,740) / 3,749 | 78.1 | 7.47 | 1,899 | 17,362 | n/a | n/a |
| Shared-resource strict FIFO | Candidate | 5.77 (5.26–37.43) | 13.86 | 1,972 (1,838–33,276) / 1,976 | 130.1 | 8.94 | 1,781 | 17,946 | 2,135 / 62 | 120 / 76 |

The wide, alternating tails are real raw observations; they are retained rather than discarded as outliers. They make latency conclusions uncertain at five repetitions. `pg_stat_statements` did not expose PostgreSQL CPU, so no CPU-per-task claim is made. Sampled process working-set/private-byte maxima (not managed heap) were baseline Hub 122.3/67.0 MiB and worker 111.4/56.1 MiB; candidate Hub 124.6/66.7 MiB and worker 112.4/57.7 MiB. Candidate registry limits remain 2,048 entries and 4 MiB per partition, 32,768 process entries/IDs and 64 MiB process bytes; exact pending IDs have a 32,768 process-wide cap. These host-level samples do not establish managed allocation or steady-state memory bounds independently of those code limits.

The feature added 4,000 outbox rows and 6,000 matching NOTIFY producer rows for 1,000 burst/steady tasks per arm aggregate, versus 2,000 and 4,000 respectively in baseline. It performed 1,367 burst, 1,412 steady, 750 sparse, and 614 FIFO hydration calls across five repetitions; the corresponding candidate recovery page calls were 46,579, 57,815, 32,725, and 18,617. Exact hints therefore coexist with substantial recovery traffic and event write amplification. Aggregate DB calls, execution time, buffers, WAL, and per-query top rows are available in the `records.json` rows. No throughput advantage is claimed.

## Durable-claim recovery latency

The recovery harness measures from controlled arrival (or demand/eligibility) until `AcquireAsync` returned a task whose lease/epoch transaction had committed. It does not count a rescan callback as dispatch. Each row below has only two observations per arm, so report observed ranges, not percentiles. The test held the LISTEN consumer and outbox dispatcher stopped for all cells; the “outbox enabled” case left its rows pending. `arrival → claim` milliseconds:

| Recovery scenario | Demand | Baseline n=2 range | Final n=2 range |
|---|---|---:|---:|
| Arrival immediately after completed sweep | false | 1,725–1,733 | 1,718–3,718 |
| Arrival immediately after completed sweep | true | 1,774–1,800 | 1,781–1,836 |
| No delivered LISTEN hint | false | 1,981–2,007 | 1,980–1,995 |
| No delivered LISTEN hint | true | 153–162 | 163–193 |
| Outbox on, dispatcher disabled | false | 1,992–2,006 | 1,953–1,978 |
| Outbox on, dispatcher disabled | true | 167–198 | 162–168 |
| Future task becomes due | false | 3,996–4,001 | 3,996–4,005 |
| Future task becomes due | true | 3,315–4,006 | 3,302–3,996 |
| Lease expires without another event | false | 3,998–4,011 | 3,997–3,997 |
| Lease expires without another event | true | 3,990–3,992 | 3,992–4,000 |
| Demand appears after no demand | false | 2,524–2,531 | 2,519–2,523 |
| Demand appears after no demand | true | 2,683–2,768 | 2,672–2,761 |

Raw final/baseline observations are in [`recovery-final.json`](recovery-final.json) and [`recovery-baseline.json`](recovery-baseline.json), with the recomputed raw-preserving [`recovery-summary.json`](recovery-summary.json). The final just-after-sweep false-demand cell contains a 3.7 s observation; this small-N result is retained and is not evidence of a strict 2 s upper bound. Recovery tests use the real PostgreSQL acquisition and durable claim path with delivery disabled, but do not run a two-process remote worker deployment.

The current Release binaries passed all three owner/SSE failover windows in [`stage3-hub-candidate-final`](../results/stage3-hub-candidate-final/): before claim (10.65 s takeover, 20/20 tasks, 0 retries), after claim (10.08 s, 20/20, 2 retries and stale heartbeat rejected with 409), and during execution (7.63 s, 20/20, 0 retries). Each had zero duplicate same-epoch starts, FIFO overlaps, missing dispatch keys, and duplicate side effects. The first harness attempt timed out before Hub A readiness because the isolated test cluster's role password differed from the harness's fixed local development configuration; after aligning that task-owned disposable cluster, all three windows passed. No shared database was changed.

## Production hydration query plan

The old hand-copied `query-cost.sql` and its provisional comparison output were removed. [`HubCandidateHydrationPlanTests`](../../tests/WorkflowCore.Scaling.PostgreSQL.Tests/HubCandidateHydrationPlanTests.cs) prepares `HubCandidateReader.HydrationSql` itself using the same 128 requested IDs and production byte-budget construction (`requested 65,536`, SQL metadata budget `63,488`; 2,048 bytes reserved for ID accounting), then runs `EXPLAIN (ANALYZE,BUFFERS,SETTINGS,FORMAT JSON)` with a generic prepared statement. [`production-hydration-plan.json`](production-hydration-plan.json) records the production SQL SHA-256, exact plan, and hydrated row/metadata counts. On 20,000 constrained ready rows, PostgreSQL returned 128 rows in 6.766 ms, 1,408 shared hits and 0 shared reads, with 128 `hub_task_pkey` probes and no `hub_task` sequential scan. This validates the bounded lookup access shape; it does not show that a 128-row hydration batch is cheaper than indexed discovery or improve end-to-end throughput. The separate production method wall time in that artifact includes host/pool contention and is not used as query latency.

## Reproduction and verification

The paired runner needs baseline Release binaries built from `ca190584e1f84bf952223cff95731d914d0a5280`, final Release binaries, two isolated identically migrated PostgreSQL 17 databases seeded with equal 20,000-row terminal history, and connection strings supplied through `WFC_BENCH_PG_BASE` / `WFC_BENCH_PG_CAND` (do not print them). From the repository root:

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
python scaling/benchmarks/hub-task-id-hints-stage3/run-ab.py `
  --baseline-root <baseline-checkout-or-build-root> `
  --artifact-dir scaling/benchmarks/hub-task-id-hints-stage3/raw-runtime-qualified `
  --pg-container <isolated-postgres-17-container> `
  --baseline-database <isolated-baseline-db> `
  --candidate-database <isolated-candidate-db>
python scaling/benchmarks/hub-task-id-hints-stage3/summarize.py scaling/benchmarks/hub-task-id-hints-stage3/raw-runtime-qualified/records.json
python scaling/benchmarks/hub-task-id-hints-stage3/paired-summary.py
```

For the SQL plan and 24-cell durable-claim recovery matrix, set `WFC_TEST_PG` only in the test process to an isolated PG17 database migrated by the CI sequence through 032, and set `WFC_CANDIDATE_PLAN_OUTPUT` or `WFC_CANDIDATE_RECOVERY_OUTPUT` to an artifact path before filtering to `HubCandidateHydrationPlanTests` or `HubCandidateRecoveryLatencyTests`. Recovery output was captured separately for the baseline build and final candidate. Then run:

```powershell
python scaling/benchmarks/hub-task-id-hints-stage3/summarize-recovery.py `
  scaling/benchmarks/hub-task-id-hints-stage3/recovery-baseline.json `
  scaling/benchmarks/hub-task-id-hints-stage3/recovery-final.json
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release --no-build --logger 'console;verbosity=minimal'
pwsh -NoProfile -File scaling/benchmarks/hub-matching-failover-suite.ps1 -Output scaling/benchmarks/results/stage3-hub-candidate
git diff --check
```

Final checks: `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release` passed with 0 errors and 4 existing xUnit2031 warnings; the two focused notification regressions passed 2/2; the full suite on a fresh schema applied through the exact CI migration list passed 336/336, 0 failed, 0 skipped (10m04s); plan and recovery measurement tests passed 1/1 each; all three failover windows passed; `git diff --check` passed. The A/B raw output, process-memory samples, manifest, paired summary, plan, recovery raw data/summary, build/test/failover logs and outcomes are the qualification evidence; they contain no database credentials. Full-suite tests require `WFC_TEST_PG` and a fresh isolated schema. Existing upstream source/test behavior is unchanged.
