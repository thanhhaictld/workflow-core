# Hub task-ID hint qualification: corrected comparison

## Decision

Keep `WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED=false` by default and retain it as an experimental, opt-in enhancement. The original partition-owned in-memory candidate matcher remains the normal architecture: `WFC_HUB_MATCHING_CANDIDATES_ENABLED=true` in the standard Makefile and Compose presets. The controlled comparison holds that topology on in both arms and changes only exact task-ID producer/consumer behavior. The results are mixed and not robust enough to enable the additional path: burst and strict-FIFO medians improve on several runtime measures, while steady and sparse workloads regress materially; task-ID hints double relevant outbox and NOTIFY producer rows in every cell.

The prior `raw-runtime-qualified` measurement and its conclusions are retained unchanged for audit but excluded: its baseline disabled `WFC_HUB_MATCHING_CANDIDATES_ENABLED`, while its treatment enabled it. It compared different matching architectures and cannot attribute results to task-ID hints. The new evidence is in [`raw-runtime-controlled`](raw-runtime-controlled/), with both flags recorded separately and the existing candidate topology enabled in both arms.

## Runtime flags and migration behavior

- `WFC_HUB_MATCHING_CANDIDATES_ENABLED` controls the pre-existing partition-owned in-memory matcher and fenced candidate admission. Normal Makefile/Compose presets remain `true`.
- `WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED` controls only exact-ID production, listener routing, and hydration; normal presets default to `false`.
- Migration 032 retains the 030-equivalent direct-notify/outbox behavior when the per-session `wfc.hub_candidate_task_id_hints` setting is absent or off. In outbox mode it does not emit producer NOTIFY directly. In opt-in mode task-ID payloads are advisory and hydrated from PostgreSQL; claims and lease epochs remain database authority.
- Rollout: apply additive migration 032 after 031; keep the existing candidate topology enabled. Leave task-ID hints off unless deliberately testing the experiment. To trial it, set the new flag consistently on all Hub owners and HubTaskStore producer sessions. Roll back by setting only the task-ID flag false and restarting those processes. Do not reverse migration 032 while any new code/schema consumer remains deployed. Older listeners receive compatible coarse payloads from off-mode writers; when task-ID mode is off, exact payloads from newer writers are converted to coarse matching invalidations.

## Controlled PostgreSQL 17 runtime comparison

Baseline source is `ca190584e1f84bf952223cff95731d914d0a5280`. Treatment binaries were built from the child worktree at source HEAD `75144b93632093454ad45e54f702b97450fccfcb` plus the corrective uncommitted source changes present at run time; exact Hub/worker binary and source-file hashes are recorded in [`manifest.json`](raw-runtime-controlled/manifest.json). The baseline runtime has no task-ID feature. Both arms used candidate topology on, two Hub owners, two matching partitions, four worker lane instances, one workflow/activity worker per lane, push enabled, queue admission limit 3, candidate batch size 32, and the same PostgreSQL 17/outbox/dispatcher settings. Separate databases began with equal 20,000-row terminal histories and no active workflows. Each scenario had one warmup per arm followed by five interleaved measured repetitions; all 40 measured workloads and metrics captures completed, with zero failed runs, missing workflows, retries, expired leases, or dead tasks. Raw per-repetition records and run logs are retained.

Values below are medians of five runs; ranges retain all five, including latency outliers. P95/P99 are workflow-level percentiles measured within each run, and the displayed ranges are ranges across run percentiles rather than pooled percentiles. DB execution is PostgreSQL execution time, not CPU time.

| Workload | Arm | Duration s [range] | Durable tasks/s | Workflow P95 ms [range] | P99 ms [range] | SQL statements/task | DB execution ms/task | Shared hits/task | WAL bytes/task | Discovery calls/rows | Hydration calls/rows | Outbox producer rows/run | NOTIFY producer rows/run |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Burst, 20 workflows / 200 tasks | Baseline | 106.0 [19.3–142.4] | 1.89 | 36,658 [5,877–37,733] | 37,768 [6,108–39,235] | 329.8 | 6.47 | 987 | 13,888 | 21,030 / 579 | 0 / 0 | 400 | 400 |
|  | Task-ID on | 11.7 [10.7–104.4] | 17.05 | 2,603 [2,095–37,828] | 2,705 [2,283–37,875] | 107.1 | 6.17 | 985 | 17,449 | 4,261 / 24 | 282 / 191 | 800 | 800 |
| Steady, 20 / 200, 400 ms stagger | Baseline | 20.6 [19.6–27.7] | 9.71 | 5,477 [3,478–9,689] | 5,507 [3,644–9,739] | 161.9 | 5.60 | 937 | 19,501 | 9,706 / 204 | 0 / 0 | 400 | 400 |
|  | Task-ID on | 104.0 [11.3–111.7] | 1.92 | 35,288 [2,305–40,540] | 35,446 [3,919–40,827] | 264.9 | 7.24 | 1,105 | 16,838 | 14,230 / 352 | 263 / 193 | 800 | 800 |
| Sparse, 8 / 80, 1.5 s stagger | Baseline | 24.9 [23.8–25.9] | 3.21 | 4,215 [2,852–4,550] | 4,828 [2,876–5,258] | 394.7 | 7.88 | 1,094 | 14,350 | 12,177 / 80 | 0 / 0 | 160 | 160 |
|  | Task-ID on | 69.0 [14.2–128.5] | 1.16 | 30,026 [1,630–31,302] | 30,442 [1,709–31,375] | 435.1 | 9.18 | 1,165 | 16,227 | 11,002 / 46 | 149 / 71 | 320 | 320 |
| Shared-resource strict FIFO, 8 / 80 | Baseline | 12.3 [10.8–68.1] | 6.51 | 6,158 [4,331–35,685] | 6,230 [4,448–35,738] | 219.9 | 6.91 | 1,210 | 16,040 | 5,878 / 118 | 0 / 0 | 160 | 160 |
|  | Task-ID on | 5.7 [5.4–38.5] | 13.92 | 2,250 [2,185–36,912] | 2,327 [2,245–37,083] | 125.8 | 8.07 | 1,678 | 16,482 | 2,097 / 9 | 122 / 76 | 320 | 320 |

Aggregated totals across the five measured repetitions per scenario (warmups excluded):

| Workload | Arm | Durable tasks | All SQL statements | DB execution ms | Shared hits | WAL bytes | Discovery calls | Hydration calls | Outbox rows | NOTIFY producer rows |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Burst | Baseline | 1,000 | 271,775 | 6,624 | 1,145,006 | 15,187,503 | 85,843 | 0 | 2,000 | 2,000 |
| Burst | Task-ID on | 1,000 | 138,085 | 6,444 | 901,356 | 18,926,942 | 31,415 | 1,388 | 4,000 | 4,000 |
| Steady | Baseline | 1,000 | 161,526 | 5,370 | 961,888 | 17,817,728 | 48,410 | 0 | 2,000 | 2,000 |
| Steady | Task-ID on | 1,000 | 207,009 | 6,758 | 1,109,307 | 18,730,775 | 53,271 | 1,349 | 3,999 | 3,999 |
| Sparse | Baseline | 400 | 160,503 | 3,208 | 450,382 | 6,479,723 | 61,948 | 0 | 800 | 800 |
| Sparse | Task-ID on | 400 | 168,346 | 3,404 | 481,877 | 7,507,363 | 53,027 | 741 | 1,600 | 1,600 |
| Strict FIFO | Baseline | 400 | 114,887 | 3,029 | 534,886 | 8,177,582 | 38,327 | 0 | 800 | 800 |
| Strict FIFO | Task-ID on | 400 | 62,659 | 3,243 | 671,864 | 7,848,660 | 14,761 | 608 | 1,600 | 1,600 |

The task-ID path reduces discovery traffic in burst and strict-FIFO runs, but hydration traffic is additional and substantial. It adds 100% producer rows in the tested outbox configuration (outbox inserts and matching NOTIFY producer rows both double); this is expected feature amplification, not a default-mode change. Steady and sparse results show more SQL statements and higher DB execution per task, with much worse medians for completion and tail latency. Burst and FIFO improve on some medians but have wide run-to-run tails. Five repetitions do not establish stable tail behavior. Per-run raw records show requested versus completed workflows/tasks, retries, extra claims, lease expiries, and dead tasks; every measured run completed the full requested task count with zero retries, extra claims, expiries, or dead tasks. The benchmark used queue-mode admission with limit 3, and the database execution totals include its SQL cost; the in-process candidate-admission latency histogram was not exported because the run had no OTLP collector. PostgreSQL CPU was also unavailable from this harness, so no CPU-per-task claim is made. The run did not directly measure CLR managed heap; configured candidate registry caps remain in force, and `HubTaskStore` pending exact IDs use `_options.ProcessEntries` (default 32,768), not a hard-coded 16,384 cap.

### Raw evidence and reproduction

- [`records.json`](raw-runtime-controlled/records.json) contains every warmup and measured run, per-run metrics, and failures (none among measured runs).
- [`summary.json`](raw-runtime-controlled/summary.json) and [`paired-summary.json`](raw-runtime-controlled/paired-summary.json) preserve scenario/arm aggregates and all five repetition values.
- [`manifest.json`](raw-runtime-controlled/manifest.json) includes source/binary/migration hashes, both independent feature flags, workload dimensions, Postgres settings, and database isolation; [`controlled-evidence-integrity.json`](controlled-evidence-integrity.json) records hashes for key report artifacts and failover result JSON.
- The earlier cross-architecture experiment remains in `raw-runtime-qualified/` and is excluded from this decision.

From the repository root, with two task-owned disposable PG17 databases restored to the same terminal-history template and the baseline Release binaries available from an archive/build of `ca190584e1f84bf952223cff95731d914d0a5280`:

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
$env:WFC_BENCH_PG_BASE = '<isolated PG17 connection string>'
$env:WFC_BENCH_PG_CAND = '<isolated PG17 connection string>'
python scaling/benchmarks/hub-task-id-hints-stage3/run-ab.py `
  --baseline-root <baseline-build-root> `
  --artifact-dir scaling/benchmarks/hub-task-id-hints-stage3/raw-runtime-controlled `
  --pg-container <isolated-container> `
  --baseline-database <baseline-db> `
  --candidate-database <candidate-db>
python scaling/benchmarks/hub-task-id-hints-stage3/summarize.py scaling/benchmarks/hub-task-id-hints-stage3/raw-runtime-controlled/records.json
python scaling/benchmarks/hub-task-id-hints-stage3/paired-summary.py scaling/benchmarks/hub-task-id-hints-stage3/raw-runtime-controlled/records.json
```

No credentials are recorded in artifacts. The actual run used disposable container `wfc-luna-stage3b-20261117`, DBs `stage3baseid` and `stage3candid`, each with equal 20,000 terminal rows, outbox enabled, dispatcher enabled, `pg_stat_statements.track=all`, and `track_io_timing=on`.

## Durable claim recovery

The recovery matrix measures through `AcquireAsync` returning a durably committed lease. The LISTEN consumer is not running and outbox dispatcher is disabled; task rows in the outbox-enabled case remain pending. Existing baseline/final matrix observations used the candidate topology in both source trees but their raw `candidateIdHints` field was mislabeled: the baseline has no task-ID implementation; the final had it enabled. The raw observations are unchanged, and this is clarified in [`recovery-context.json`](recovery-context.json). These six non-arrival scenarios have only two samples per demand state, so ranges are reported rather than p95/p99. The newly controlled just-after-sweep focus has 12 measured repetitions per demand state, both with the new path off and on:

| Arrival immediately after completed sweep | New task-ID flag | RequireDemand=false | RequireDemand=true |
|---|---|---:|---:|
| Baseline source, candidate topology on | absent (off semantics) | 1,725–1,733 ms (n=2) | 1,774–1,800 ms (n=2) |
| Corrected final, task-ID off | off | 1,600–3,631 ms (n=12) | 1,732–1,894 ms (n=12) |
| Corrected final, task-ID on | on | 1,631–3,760 ms (n=12) | 1,745–1,850 ms (n=12) |

The two 3.6–3.8 s outliers are retained. This focused harness exercises the PostgreSQL task store and durable lease-return path; it does not recreate the complete multi-process worker/SSE deployment for every matrix cell. The prior six-scenario baseline/final observations remain available in `recovery-summary.json`: no-listen hint, outbox enabled with no dispatcher, future-due task, lease expiry without further notification, and demand appearing after an initially absent-demand observation. They measured ranges around 1.6–2.8 s for ready/demand wakeups and around 3.3–4.0 s for future due and expired leases. The entire matrix is not reinterpreted as a high-confidence percentile estimate because each of those cells has n=2.

Exact commands used for the controlled focus, with `WFC_TEST_PG` set only in the test process and no credentials in the artifact:

```powershell
$env:WFC_CANDIDATE_RECOVERY_REPETITIONS = '12'
$env:WFC_CANDIDATE_RECOVERY_FOCUS = 'arrival-after-sweep'
$env:WFC_CANDIDATE_RECOVERY_TASK_ID_HINTS = 'false' # run again with true
$env:WFC_CANDIDATE_RECOVERY_OUTPUT = '<recovery-output.json>'
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj `
  -c Release --filter FullyQualifiedName~HubCandidateRecoveryLatencyTests
```

## Verification

Release build: `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release` succeeded with 0 errors (4 pre-existing xUnit analyzer warnings). The corrected feature-gate, mixed-version, producer, listener, and recovery focused tests passed against PostgreSQL 17. The full PostgreSQL suite passed on the final fresh-schema run: 341 passed, 0 failed, 0 skipped. During earlier qualification attempts, the notification test was corrected to scope notifications to its own hub, and the flood test was corrected to explicitly enable task-ID routing and use a controlled slow subscriber; both targeted tests passed, and the final full suite includes those fixes. An unrelated native event-lane concurrency assertion failed once under the broader suite but passed when run alone; it passed in the final full run.

Failover checks for the existing candidate topology with the new exact task-ID flag explicitly off are recorded in [`stage3-candidate-topology-taskids-off`](../results/stage3-candidate-topology-taskids-off/). The current failover run passed before-claim, after-claim, and during-execution windows with the existing candidate topology enabled and task-ID hints explicitly disabled: owner takeover was 7.64 s, 10.15 s, and 10.24 s, respectively; all 20 workflows completed in each window with zero duplicate same-epoch starts, FIFO overlaps, missing dispatch keys, or duplicate side effects. The after-claim and during-execution cases each rejected a stale heartbeat with HTTP 409. Exact logs and JSON are in `../results/stage3-candidate-topology-taskids-off/`.
