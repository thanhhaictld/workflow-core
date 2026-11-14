# Hub matching scale implementation

Baseline SHA: 308fef892d6c4d74a543e07c960ae4d7e624918e
Status: Runtime implementation delivered; final Release build and 307 tests passed. Benchmark qualification and combined end-to-end failure stress remain incomplete. Updated 2026-11-14.

## Current status

GPT Terra implemented the runtime, migration 030/outbox, settings, metrics, tests, benchmark tooling and documentation in this child worktree. Changes are uncommitted. All new optimization switches default off. The final evidence audit at the end of this file contains commands, coverage limits and remaining risks. Earlier milestone entries are chronological history and may describe superseded states.

## Phased checklist

- [x] Phase 0: inspect baseline, AGENTS.md and required architecture docs.
- [x] Phase 1: implement opt-in bounded lazy demand/backpressure, cancellation and capacity wakeup.
- [x] Phase 2: implement mixed-worker unconstrained admission with per-item eligibility and uncertain-commit retention.
- [x] Phase 3: implement bounded partition/process admissions and batched retained-lease reconciliation.
- [x] Phase 4: implement hinted demand-gated discovery and additive transactional notification outbox.
- [x] Phase 5: implement settings, opt-in switches, metrics, documentation and benchmark tooling.
- [ ] Phase 6: correctness coverage partially verified; combined multi-session commit-loss/process-death/takeover scenario, prolonged saturation and outbox failover stress remain unverified.
- [ ] Phase 7: full 8/400/4000-lane benchmark matrix and acceptance targets remain unmeasured.
- [x] Phase 8: final Release build passed; fresh isolated PostgreSQL suite passed 307, failed 0, skipped 0 with WFC_TEST_PG set.
- [ ] Phase 9: implementation/evidence audit recorded; full approved-plan acceptance remains pending phases 6–7.

## Decisions and deviations

All new optimization switches default off pending benchmark qualification. Migration 030 and the outbox were absent from baseline and were added. No existing service was disrupted and no database was reset. Reports distinguish implemented behavior from verified behavior and performance evidence.

## Test commands and results

Final source: Release solution build succeeded with 0 errors and 4 existing analyzer warnings. Fresh isolated PostgreSQL 17 suite: 307 passed, 0 failed, 0 skipped (1m07s), with all three candidate optimization switches enabled and WFC_TEST_PG set. Exact commands and earlier failed/superseded runs are retained in the final evidence audit below. Coordinator checked this progress record; verification results were produced by GPT Terra.

## Benchmark evidence

No acceptance targets have been measured. Tooling and protocol are present. A separate active k6/16-worker workload occupied the shared 16-logical-processor host; an idle or dedicated environment is needed for attributable, non-disruptive measurements.

## Blockers and remaining work

- Run the full benchmark matrix with raw counter capture and five measured repetitions per cell.
- Verify combined cross-worker lost-commit-response, process death, takeover, reconciliation and stale completion in one end-to-end scenario.
- Verify concurrent ownership-loss stress, prolonged saturation and outbox failover/reconnect; evaluate the 8192-demand cap with mixed push/pull sessions.
- Measure CPU/fairness at 4000 lanes and default-path SQL query cost.

Implementation report: [scaling/docs/hub-matching-scale-implementation-report.md](scaling/docs/hub-matching-scale-implementation-report.md).

## Approved plan (complete scope)

1. Candidate overload: remove SQL claim fallback on credit exhaustion, keep sessions connected, bound demand to one record per session with available credits, reserve only when matching, wake fairly on capacity release and remove cancelled or expired demand. Preserve pull behavior when candidate mode is disabled.
2. Batch unconstrained reservations across workers within a Hub, partition and ownership epoch, at most 32 and without fill delay. Carry destination and capability/workflow eligibility; revalidate eligibility and absence of constraints in PostgreSQL, and handle stale items independently. Keep constrained admissions single with canonical locks and FIFO; record deliveries before send and retain all potentially committed reservations until authoritative reconciliation on uncertain commit.
3. Separate discovery, admission, and retained-lease reconciliation. Limit one admission per partition and default two per process within the existing database admission limit; round-robin pending partitions, one discovery query initially, and authoritative retained lease queries of at most 256. Preserve accounting through ownership cancellation.
4. Coalesce hints by owned bucket, prioritize hinted buckets with demand, and gate discovery on eligible demand and capacity. Keep finite horizons, sequence probes and periodic recovery for demand-bearing partitions. Maintain a transactional notification outbox; poll adaptively between 10 ms and 1 s with exponential jitter, reset on nonempty and drain full batches immediately. Migration 030 must precede dispatcher deployment; add indexes only for measured query-plan need.
5. Preserve REST/SSE contracts; add validated batch, concurrency and outbox-delay settings and independent opt-in backpressure, batching and concurrent-admission switches. Collect bounded-label metrics for waiting sessions, capacity waits, discovery rows/lease, batch fill, admission latency, reconciliation queries and outbox rows/oldest age. Measure statements, transactions, WAL and CPU per completed task alongside throughput and latency.
6. Test deterministic saturation without fallback, fairness, cancellation, ownership loss and memory bounds; PostgreSQL mixed-worker eligibility, stale items, constraint revalidation, atomic history and uncertain commits; injected post-commit response loss, mid-batch SSE failure and takeover; no early credit reuse or lost durable work and stale completion fencing.
7. Benchmark baseline versus tuned on the same isolated schema/workload with 8, 400 and 4000 logical lanes for idle, bursts, sustained backlog and a hot constrained resource, five measured repetitions following warmup. Targets: at least 50% fewer acquisition statements at 4000; idle query rate within 20% from 400 to 4000 at fixed partitions; at most 10% median throughput or p95 schedule-to-start regression at eight. Keep optimization opt-in unless supported by evidence. Do not disrupt services.
8. Run Release solution build and relevant tests with WFC_TEST_PG for actual database coverage; explicitly identify skipped tests and prerequisites. Provide reproducible benchmark tooling even when prerequisites block runs, fix failures, and distinguish implementation from measurements.

## Baseline inspection correction
HEAD matches the requested SHA. Git reports an untracked IMPLEMENTATION.md, so the worktree is not completely clean; preserve it. The previous Phase 0 completion claim is provisional until the required architecture documents are read in this session.

## Inspection and verification milestone

- Required Hub implementation and historical architecture documents were read. Migration 030 and the PostgreSQL notification dispatcher are absent from this baseline; migration 029 is the latest. An additive outbox and deployment wiring remain required.
- Candidate overload currently raises `HubCandidateCapacityException` and `HubPushSessions` falls back to an owner-fenced SQL claim. A preliminary retry-on-capacity experiment was discarded: it would wake many waiting sessions together, did not provide the required fairness or demand representation, and was not adequately tested. No runtime code change remains.
- Release build: `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release` succeeded with 7 warnings and 0 errors. This was run while the discarded experiment was present; the baseline source after its removal was not rebuilt.
- `dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release --no-build` exited 1: 258 passed, 39 failed, zero reported skipped. WFC_TEST_PG was unset; failures included tests explicitly requiring it, and some tests silently return without database coverage. This run used the build with the discarded experiment and is not verification of final source.
- Docker daemon and a postgres:17 image are available; an isolated database was not provisioned. Database integration testing and benchmarks are incomplete, not proven impossible. No service or database was reset or modified.
- Existing benchmark reference: `scaling/docs/hub-push-benchmark-2026-11-14.md` and `scaling/benchmarks/hub-push-load.py`; these do not establish the requested 8/400/4000 workload matrix or targets.
- All implementation phases, requested focused tests, migration, benchmark tooling/matrix and final verification remain incomplete. No optimization is approved for use based on this work.

## Resumed implementation milestone

- An independently opt-in `WFC_HUB_CANDIDATE_BACKPRESSURE=true` path now queues one waiting demand per acquisition, up to the existing 8192-session bound. It rotates waiting requests when registry credit is full, retries on the coordinator wake/tick, removes cancelled demands, checks the owner generation, and does not enter SQL fallback on credit exhaustion. This is partial: registry credits are still materialized before candidate matching, and deterministic saturation/fairness coverage is still required.
- The same flag gates discovery when no unreserved candidate credit is present; bounded horizon and recovery state are retained. Hinted-bucket prioritization remains unimplemented.
- A separate disposable PostgreSQL 17 container `wfc-codex-matching-test-25f99f40` on localhost port 55639 was created without touching existing services; EF and SQL 002–027/029 were applied. No database reset was performed. This test container contains only task-created test data and remains running for verification.
- `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release --no-restore` passed with 4 existing analyzer warnings before the latest discovery-gating edit. A full test run against the isolated database is in progress; no result is claimed yet.

## Database verification and outbox milestone

- PostgreSQL integration suite on the isolated database (migrations 002–027 and 029): 297 passed, 0 failed, 0 reported skipped; this was before the subsequent changes.
- Added additive migration 030, installed by dev/Compose migration paths. Default trigger behavior remains direct transactional NOTIFY; operators may enable outbox writes with database setting `wfc.hub_notification_outbox=on` after deploying the opt-in dispatcher. Dispatcher transactions delete locked rows and issue both notifications atomically, drain full batches immediately and back off with bounded exponential jitter; batch and delay settings are validated. Outbox is not measured and defaults off.
- Added deterministic demand-gating and real-PostgreSQL outbox commit/rollback and delivery tests; both passed (2/2) after applying migration 030 to the isolated database. Added batched authoritative retained-lease retirement (maximum 256 with backpressure enabled) and a PostgreSQL test; that test and final source have not yet been run.
- Cross-worker batches, independent concurrent admission, fair saturation tests, required benchmark matrix, and remaining observability are still open.

## Matching admission milestone

- Opt-in `WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH=true` combines already-reserved unconstrained candidates for one owner/partition without a fill delay; batch size defaults to 32 and validates `WFC_HUB_CANDIDATE_BATCH_SIZE` in 1–32. SQL carries each worker's capability and workflow pins, rechecks task state, epoch and absence of constraints, and writes the actual worker in task attempts and workflow history. Existing single-worker batch and injected unknown-commit tests pass; a new mixed-worker PostgreSQL test confirms distinct workers, mismatched eligibility and stale epoch isolation.
- Opt-in `WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION=true` uses at most one active admission per hub/partition and a process semaphore defaulting to two (validated `WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY=1..32`); the existing database gate remains authoritative. Shutdown awaits in-flight work. Deterministic concurrency/ownership-loss tests are still required; this is not yet measured.
- Opt-in backpressure now prioritizes coalesced hinted buckets while retaining finite discovery horizons and recovery probes. A new deterministic hint test has not yet been run.
- Focused PostgreSQL/deterministic command passed 7 tests following mixed-worker SQL, backpressure, retirement and outbox work, before concurrent admission and hinted-bucket edits. Benchmarks and complete failure injection remain outstanding.

## Focused validation and benchmark tooling milestone

- With all three candidate switches enabled, focused push-session, backpressure FIFO/cancellation, mixed-worker batch and coalesced hint tests passed: 6/6 on isolated PostgreSQL. A separate focused run passed 7/7 covering mixed batch, cancellation, retirement and outbox; these checks precede the latest telemetry edit.
- Added bounded-label waiting, capacity-wait, candidate row/lease, batch-fill, admission latency, reconciliation and outbox telemetry. Added `scaling/docs/hub-matching-scale-benchmark.md` and a fail-closed matrix analyzer; the existing load driver does not generate all four required scenarios, so workload generation and all measurements remain outstanding. `python scaling/benchmarks/hub-matching-scale-analyze.py --help` succeeded; acceptance gates have not been measured.
- Full PostgreSQL suite with all experimental switches enabled is running. Full end-to-end cross-worker grouping and concurrency failure injection, single-demand representation inside registry, migration failover stress and benchmark evidence remain open.

## Full-suite investigation

- Full tuned-flag suite on repeatedly reused isolated `workflow_test`: 302 passed, 1 failed. `HubNativeRuntimeTests.ExhaustedActivityTransportClosesAttemptAndDeliversNativeFailure` found no inbox message after a bounded 100-item sweep; this database had 440 ready exhausted tasks accumulated from earlier test runs, so cross-test database contamination is a plausible cause, not yet established as the sole cause.
- Created a second fresh database `workflow_verify2` inside the same task-owned container, applied EF and migrations 002–027/029/030, and started a full tuned-flag rerun. No database was reset, removed or modified outside the task-owned container.


## Latest validation

- Fresh isolated database `workflow_verify2` with all three candidate switches enabled: full suite passed 303/303 with 0 reported skips, before subsequent test-only concurrency instrumentation and dispatcher failure-hook changes. The earlier 302/303 run on `workflow_test` failed a bounded sweep against a database containing 440 previously exhausted tasks; the fresh run passed the same test.
- A later focused run of concurrency, mixed-worker commit uncertainty, and outbox rollback/delivery tests passed 5/5 on PostgreSQL; a prior combined focused run passed 10/10. Deterministic concurrent admission held two partitions at a barrier and confirmed a third waited, then all tasks were leased once.
- Latest `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release` passed with 4 warnings and 0 errors; `pwsh -NoProfile -File ./scripts/dev.ps1 help` passed. Analyzer rejected an empty matrix as intended (expected exit 1). No real performance trial has run.


## Lazy demand representation milestone

- The opt-in path now registers one waiting demand per acquisition with the partition and creates each registry credit only when an eligible cached candidate can be reserved; discovery recognizes waiting demand without consuming registry credit. Cancellation and generation loss remove waiting demand, and finite horizons remain intact. A partition with no remaining credit capacity stops discovery for queued demand.
- Focused tuned-flag registry, push, saturation/cancellation/takeover and concurrent admission tests passed 20/20 after this redesign; a new explicit zero-credit-before-match test and the latest small capacity-gating edit have not yet run.
- The measured 8/400/4000 matrix and several requested failure-injection scenarios remain open. The per-turn scan of waiting demands and cache compatibility has not been benchmarked and may need further optimization.

## Benchmark environment constraint

- `docker ps` showed a separate, actively running observability workload with 16 processor containers and an active k6 run on this 16-logical-processor host. Running 4000-lane measured trials now would contend with and potentially disrupt that workload, and would not yield attributable CPU, throughput or latency evidence. No existing container was stopped or changed. A dedicated or otherwise idle isolated benchmark environment and the documented missing workload generators remain needed for the full measured matrix; no acceptance gate is claimed.
- The new deterministic lazy-credit test and focused tuned-flag suite passed 26/26, and the injected mid-batch SSE failure test separately passed, preserving all four durable leases and delivery records while attempting remaining sends. A fresh-database full suite is currently running against the latest test build.

## Fresh end-to-end verification

- The complete suite passed 306/306 with all three candidate switches enabled on fresh isolated `workflow_verify3` with EF and migrations through 030; no tests reported skipped. This run included lazy-demand matching, mixed-worker uncertain commit, takeover/cancellation, separate admissions and mid-batch SSE failure; it preceded the final 128-demand-per-pass fairness bound.
- The benchmark load generator now offers a shared tenant key for the sample activity's capacity-one strict FIFO constraint, and the protocol specifies idle, burst and sustained backlog using the same driver. Both benchmark Python scripts passed `python -m py_compile`. The matrix remains unmeasured because the active separate k6/16-worker workload prevents isolated, non-disruptive performance trials on this host.

## Latest verification and remaining work

- After the 128-demand-per-pass bound, `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release` passed (4 existing xUnit analyzer warnings, 0 errors). The tuned-flag focused PostgreSQL suite passed 27/27 on the isolated database, including candidate registry, FIFO cancellation/takeover, concurrent admissions, mixed-worker batches, authoritative retirement, outbox rollback and mid-batch SSE failure. `git diff --check` passed with a line-ending warning only. The full 306-test run was immediately before the bounded-pass change.
- Benchmark analyzer and load generator compiled successfully and the analyzer rejects incomplete matrices. No benchmark measurements or target claims are available. Running the full matrix while another 16-worker k6 workload uses the same 16-CPU host would compromise isolation and potentially disrupt that workload; an idle/dedicated environment is required.
- The implementation is experimental: performance under thousands of waiting sessions and full end-to-end multi-worker commit-loss/takeover behavior still need the planned stress matrix. The shared-host benchmark constraint does not excuse any unverified correctness claim. No migration was deployed outside the isolated test container, no existing service was stopped, and no database was reset.
- The task-owned PostgreSQL container was stopped after verification; its container and database data were retained and can be restarted for follow-up. All other running containers were left untouched.

## Final evidence audit (2026-11-14; supersedes older milestone status)

**Implemented:** opt-in bounded lazy demand/backpressure, hinted and demand-gated discovery, mixed-worker unconstrained admission, bounded concurrent admission, batched retained-lease reconciliation, additive migration 030 with opt-in outbox dispatcher, telemetry and benchmark tooling. The new switches `WFC_HUB_CANDIDATE_BACKPRESSURE`, `WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH`, `WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION` and `WFC_HUB_OUTBOX_ENABLED` default off; database outbox writing also defaults off unless `wfc.hub_notification_outbox=on`. Existing candidate topology and push are separately opt-in. Numeric defaults: batch 32, process admission 2, outbox batch 128 and delays 10–1000 ms.

**Prior 306-test evidence:** yes, the fresh PostgreSQL 17 database `workflow_verify3` was real and `WFC_TEST_PG` was set. From the repository root, the exact test command was:

```powershell
$env:WFC_TEST_PG='Host=127.0.0.1;Port=55639;Database=workflow_verify3;Username=postgres;Password=isolated-test'
$env:WFC_HUB_CANDIDATE_BACKPRESSURE='true'
$env:WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH='true'
$env:WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION='true'
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release --no-build
```

Result: **306 passed, 0 failed, 0 skipped**, on the test build before the final 128-demand-per-pass scheduling bound. This was actual DB coverage: tests which return early when `WFC_TEST_PG` is unset received a configured connection; this does not imply that every test uses PostgreSQL.

**Final source verification:** a new database `workflow_verify4` was created inside the task-owned PostgreSQL 17 container, after restart assigned host port 53596; EF and migrations 002–027, 029 and 030 were applied, with optional 028 omitted as in default setup. No existing database was reset. Exact build/test commands from the repository root:

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
$env:WFC_TEST_PG='Host=127.0.0.1;Port=53596;Database=workflow_verify4;Username=postgres;Password=isolated-test'
$env:WFC_HUB_CANDIDATE_BACKPRESSURE='true'
$env:WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH='true'
$env:WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION='true'
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release --no-build
```

Build: **succeeded**, 0 errors and 4 existing xUnit analyzer warnings. Test: **307 passed, 0 failed, 0 skipped**, duration 1m07s. The final build and full test covered the final runtime code and the additional 129-ineligible-demand test; subsequent edits to this record and the report are documentation only. During the run a read-only query found 48,872 task rows and 534 workflow rows in the new isolated database, independently confirming actual PostgreSQL activity. The outbox test sets the DB option locally for its transaction; it was not enabled globally for the full suite.

**Covered, with scope:**

- SQL single-item and mixed-worker batch tests inject rollback before commit and a successful commit followed by a lost response; they check tentative/committed leases, individual eligibility and stale candidate isolation. This is store-level injection, not a multi-process transport crash.
- Registry tests retain uncertain/dispatched reservations on cancellation and ownership loss and prohibit early credit reuse; PostgreSQL tests cover expired-owner admission fencing, retained-lease retirement, resource FIFO/constraint revalidation and journal/attempt history.
- Push tests inject a failed send, check the durable lease survives owner takeover, then assert stale completion is fenced; the four-item mid-batch test fails the second send, checks later sends are attempted and all four delivery records/DB leases remain. That mid-batch test uses a single worker/session.
- Saturation tests cover waiting FIFO, cancellation, ownership takeover and credits only on matching; the new >128 test checks progress with 129 incompatible waiters. Concurrent-admission test holds two partition admissions at a barrier and verifies a third waits, then all three lease once. Tests do not establish fairness or bounded latency at thousands of sessions.
- Outbox test covers transactional task/outbox rollback and an injected dispatcher failure before commit, followed by successful notification delivery and removal. It does not simulate a process crash or connection loss after dispatcher commit.

**Unverified / unresolved correctness risks:** there is no end-to-end multi-session cross-worker batch with lost commit response followed by Hub process death, owner takeover, authoritative reconciliation and stale completion all in one scenario; no concurrent admission stress with ownership loss during a real batch commit; and no large-scale prolonged saturation or notification outbox failover/reconnect stress. SQL mixed-worker admission and individual registry/transport cases are covered separately and must not be presented as that combined proof. The 8192 waiting-demand cap is shared by candidate acquisitions; reaching it raises an exception rather than providing unlimited waiting, so behavior under a full mix of push and pull sessions needs an end-to-end capacity test. The matching queue scans/rotates waiting demand and candidate entries; CPU and fairness at 4000 lanes are not measured. Existing services must not be interpreted as production-ready; external side effects remain at least once.

**Benchmark blocked:** no 8/400/4000 matrix or acceptance metric has been measured. At inspection a separate active observability/k6 workload with 16 processor containers shared the 16-logical-processor host, making an isolated, non-disruptive performance comparison unavailable; this does not prevent the small correctness tests above. The benchmark driver, protocol and fail-closed analyzer are present, but an idle/dedicated environment, complete raw counter capture, five measured repetitions per cell and actual results are still needed. All optimizations remain off by default pending evidence.

**Working tree:** baseline HEAD remains `308fef892d6c4d74a543e07c960ae4d7e624918e`; runtime, tests, migration, documentation, development migration wiring and benchmark files are modified/untracked and uncommitted. No merge, deployment, PR publication, database reset or changes to other running containers were performed. Earlier in this record are chronological milestones and superseded in-progress status; use this audit for final state.
- The task-owned PostgreSQL container was stopped again after the audit; its databases and container were retained. Restarting it can assign a different localhost port, so re-query `docker port` before follow-up tests.
- The mixed-worker SQL refactor also changes the internal representation used by existing same-worker candidate batches; although behavior passed the database tests, its default-path query cost has not been separately benchmarked.


## Golden observability preset configuration

At the user's request, the Makefile golden observability targets explicitly enable candidate backpressure, cross-worker batches, and concurrent admission, with batch size 32 and concurrency 2. Both Hub Compose services receive the settings; ordinary Compose defaults remain off. The golden preset leaves the outbox dispatcher off pending its separate PostgreSQL session-setting rollout, while exposing its validated defaults; command-line Make overrides remain supported. This is an experimental lab configuration, not evidence that benchmark gates have passed.

Verification: `make --no-print-directory obs-config OBS_ENV_FILE=scaling/observability/.env.example OWNER_PROJECT=wfc-golden-config-check ...` rendered both Hubs with the expected values; a second render with `WFC_HUB_CANDIDATE_BACKPRESSURE=false WFC_HUB_CANDIDATE_BATCH_SIZE=8` verified overrides; direct Compose render verified the switches remain off without the golden preset. An initial direct Compose render omitted required `WFC_OBSERVABILITY_PROJECT` and failed; rerunning with a unique project name passed. These were configuration-only renders; no services were started or changed.
