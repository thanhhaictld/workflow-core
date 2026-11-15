# Hub PostgreSQL efficiency and latency: Luna implementation plan

Status: proposed work, not implemented or performance-qualified by this document.
Prepared 2026-11-15 against local HEAD `cec5d966`; revalidate source and working-tree changes before execution.

## Objective

Reduce PostgreSQL CPU and database work per completed task while preserving throughput, tail latency, durable history, fencing, resource concurrency, and bounded recovery. Diagnose queueing separately from database CPU: lower CPU alone is not success if workers wait longer.

Deliver independent, reviewable changes with reproducible before/after evidence. Do not implement every hypothesis simply because it appears in this plan. Measurement determines which experimental phases proceed.

## Read first and reconcile existing work

- Repository AGENTS.md, `scaling/README.md`, [current runtime](current-runtime.md), [implementation runbook](hub-remote-scheduler-implementation.md), and relevant sections of [historical scheduler plan](hub-embedded-scheduler-final-plan.md).
- [Initial PostgreSQL review](postgresql-4k-review-2026-11-15.md).
- [Stale-statistics correction](investigations/postgres-4k-stale-statistics-fix-luna.md).
- [Post-fix benchmark](investigations/postgres-4k-post-stale-fix-benchmark-luna.md) and [throughput variance investigation](investigations/postgres-4k-throughput-variance-luna.md), if present. At plan creation these two reports and `benchmarks/hub-audit-export-experiment/` were untracked existing work; preserve them.
- [Discovery/recovery investigation](investigations/postgres-4k-discovery-recovery-luna.md), [index investigation](investigations/postgres-4k-index-config-luna.md), and [matching implementation report](hub-matching-scale-implementation-report.md).

The earlier statement that the CASE fix still needed its first full workload run is superseded: the newer report records three successful post-fix cohorts, no old-query calls, and stable selected-ID statement cost. It does not establish a controlled speedup over master. Post-fix median CPU was approximately 137 CPU-s for 16,000 tasks; median throughput approximately 72 workflows/s. A later slow cohort used about 1.88 average PostgreSQL cores and had substantial workflow handoff delay. Neither observation establishes CPU saturation.

## Architecture and invariants

Readiness notifications -> bounded discovery -> advisory in-memory candidates/worker credits -> owner-fenced admission -> committed lease -> SSE delivery -> worker execution -> fenced REST result/commit -> snapshot/history/inbox/next task -> readiness.

Timers, expiry, inbox reconciliation, ambiguous-lease reconciliation, and readiness recovery run alongside this path. PostgreSQL remains authoritative. Remote workers and provisioners must never acquire database access.

Non-negotiable constraints:

1. Keep task epoch, owner epoch, expiry and capability checks authoritative under the existing transaction/lock boundaries. Preserve SKIP LOCKED semantics and deterministic lock ordering.
2. Preserve atomic task acknowledgement, workflow revision/snapshot, attempts, inbox and history. No asynchronous history shortcut.
3. Treat notifications and cached candidates as hints. Lost hints, cancellation, reconnect and owner loss must recover through durable state.
4. Preserve unknown-commit quarantine and authoritative reconciliation; never release uncertain capacity merely because a request timed out.
5. Constrained/FIFO work must not enter the unconstrained mixed-worker shortcut. Unknown resource metadata is not an empty resource set.
6. Preserve the existing configured recovery behavior and measure actual recovery lag. A 10-second next-sweep setting is not proof of a 10-second end-to-end bound when scans or capacity queues are involved.
7. Preserve upstream and fallback claim paths. No destructive database reset, speculative index removal, or weakened durability.
8. Do not overwrite unrelated edits or manipulate another worktree's running stack. Use identified isolated resources for mutating experiments; do not enable broad logging or run mutating EXPLAIN against shared data.

## Query inventory and ownership

| Family | Source | Why inspect |
| --- | --- | --- |
| Batch admission | `src/WorkflowCore.Hub/HubTaskStore.cs`, `CandidateBatchAdmissionSql`, `ClaimCandidateBatchAsync` | Selected-ID probes, fencing, per-worker metadata, batch fill and transaction count |
| Discovery | `HubCandidateReader.cs`, `HubPartitionCandidateRegistry.cs` | ReadyDue/Expired/Ready pages, horizon, repeated expansion, head rechecks, empty streams |
| Coordinator | `src/WorkflowCore.Hub.Gateway/HubCandidateAcquisition.cs` | Demand allocation, admission slots, cancellation, reconciliation and scheduling fairness |
| Native commit/completion | `HubNativeWorkflowStore.cs`, `HubTaskStore.cs` | Snapshot/result JSON, history, state transitions, next task insertion |
| Inbox recovery | `HubNativeWorkflowStore.SweepInboxAsync` | Event/exhausted-task UNION, DISTINCT/order/limit, global discovery and per-workflow transactions |
| Timer scheduling | `HubTimerStore.FireDueAsync`, `src/WorkflowCore.Hub.Scheduling/HubSchedulingExtensions.cs` | Inbox sweep currently precedes timer processing; nominal 500 ms ticks and overlapping batches |
| Other recovery | Gateway readiness broadcaster/listener, matching ownership, candidate retirement | Duplicate work, renewal starvation and lost-hint recovery |
| Notifications | `sql/030_hub_notification_outbox.sql`, Gateway dispatcher | Extra writes, queue age, direct versus outbox transaction behavior |
| Indexes/monitor | `sql/007*` through current migrations, monitor reader | Insert/update cost and compatibility with retained read paths |

Paths without a directory above are under `scaling/src/WorkflowCore.Hub/`; all table and query assumptions must be checked against current source. Query IDs are environment-specific evidence, not stable identifiers to hard-code into instrumentation.

## Phase 0 — Freeze a reproducible baseline

Deliver `scaling/docs/investigations/hub-efficiency-baseline-luna.md` and a small reproducible collector under `scaling/benchmarks/hub-postgres-efficiency/`.

- Record HEAD, source diff/hash, SQL hashes, compiled image identity, migration/index catalog, server version, preparation mode, all effective flags, topology, worker slots, pool limits, CPU quotas, scrape intervals and dataset sizes.
- Record master identity separately if comparing with master. Main implementation comparisons use current fixed source as control; master is an additional architecture comparison, not a substitute for isolating each change.
- Inventory existing collectors and reuse them. Distinguish workload, command/audit and idle windows. Retain failed/outlier runs, raw counters and boundary timestamps.
- Reproduce a normal and a slow cohort where practical. Capture task-ready -> claim, claim -> delivery/worker-start, execution, result -> commit, and activity-result -> next workflow claim. If timestamps do not exist, mark the interval unknown rather than infer it.
- Validate the CASE statement is active and the prior statement has no new calls. Preserve the stale-zero plan reproduction as a regression fixture.

Gate: no architecture change until source identity, measurement windows, and the dominant delay/work categories are explicit. Missing historical metrics justify instrumentation, not a root-cause claim.

## Phase 1 — Add low-overhead attribution

Owned implementation files: `HubTelemetry.cs`, `HubOpenTelemetryExtensions.cs`, candidate reader/registry/acquisition, store connection acquisition sites, scheduler, and relevant observability mappings. Keep metrics changes independent from behavior changes.

Implementation outline:

1. Add discovery counters tagged by bounded stream and activation reason: pages, empty pages, rows, bytes, head versus forward pass, horizon captures, skipped/no-demand turns and budget deferrals. Aggregate reasons from a fixed enum; do not label task, worker, workflow, arbitrary hub or resource IDs.
2. Record candidate reservation age, waiting-demand duration, batch fill, grant ratio and ambiguous-retirement queries. Existing discovery counts are not unique tasks: distinguish rediscovery using bounded existing registry membership, without an unbounded global ID set.
3. Record connection acquisition wait and operation duration for completion, admission, discovery, renewal, timer and reconciliation classes. Separate application-budget wait from pool wait and SQL transaction elapsed time.
4. Record scheduler batch overlap, timer due-to-fire delay, reconciliation candidates/processed/empty results, and owner renewal failures. Keep partial/missing measurements visible.
5. Capture interval deltas of pg_stat_statements calls/rows/time/buffers/WAL where supported; table/index stats; PostgreSQL CPU; waits and active sessions; host/container CPU pressure; worker occupancy and credits. No subtraction of SQL elapsed time from process CPU.
6. Audit exported metric names and histogram buckets against the collector and dashboard. Compare collector overhead with and without sampling in an isolated run.

Tests: metric tag cardinality and accounting where meaningful; cancellation/exception accounting must not leak gauges. Build and existing focused runtime tests. Avoid tests that only assert implementation text.

Gate: classify the bottleneck as excessive scans, excessive calls, write cost, lock/pool wait, worker/handoff delay, or unresolved. More than one can apply.

## Phase 2 — Stabilize admission and coordinator progress

The CASE optimization is already implemented: do not rewrite it without a reproduced failure. Integrate its stale-empty-index custom/generic/unprepared plan coverage into repeatable isolated verification, including fresh/churn states and batches 1/8/16/32. Keep eligibility and competing-update tests.

Investigate the coordinator default-path concern: concurrent admission is opt-in, but when disabled, `DrainAsync` awaits admission inside the discovery loop. Master previously separated discovery/admission/reaping. First write a focused test that holds one admission and observes another partition's discovery and uncertain-lease retirement progress.

If confirmed, separate admission dispatch from discovery/reconciliation while retaining bounded in-flight work. A disabled experimental flag should have a documented compatible behavior, not silently stall independent progress. Keep one tracked lifecycle for every admission, await shutdown, and preserve cancellation and owner-generation fencing. Do not launch untracked tasks.

Tests: slow constrained admission, another partition progress, flag combinations, cancellation before/during commit, shutdown, owner loss, mixed capabilities/pins, ambiguous result and retained credits. Do not change batch size and concurrency while measuring this fix.

Gate: regression test passes; no additional unbounded task/queue growth; unchanged durable correctness. Separate correctness fix from throughput claims.

## Phase 3 — Separate ready discovery from expiry recovery

Proceed only if Phase 1 identifies empty/repeated discovery as meaningful work. Own candidate registry/reader and coordinator wakeup integration.

Proposed state outline per owner generation:

```text
ReadyScan: horizon, cursors, hinted buckets, bounded head-recheck budget
ExpiryScan: horizon, cursors, next eligible recovery time, pending continuation
Generation: cancellation, capacity/demand state, rotation and bounded turn budget
```

- Ready hints prioritize ready work without restarting every expired stream.
- Recovery ticks schedule expiry work independently; continue finite scans fairly across turns.
- Preserve current demand gating semantics unless explicitly proved safe to change. Empty-stream reuse is scoped to the captured horizon/cutoff; elapsed time can make previously unexpired work eligible.
- Retain head rechecks for skipped/deferred/resource-blocked candidates, periodic sequence probes, and full lost-notification recovery. Same-horizon head queries cannot discover post-horizon inserts; schedule a fresh horizon when needed.
- Do not add one timer/task per bucket or an unbounded notification queue. Do not introduce a 250 ms polling loop as an assumed optimization over the existing recovery cadence.
- Optimize resource metadata expansion only if measured separately; unknown/oversized metadata must remain constrained on admission.

Tests: arrivals beyond horizon, expiry without notification, lost hints, many empty buckets, cancelled demand, incompatible capabilities, constrained head work, takeover, long paged sweep, fairness and bounded memory. Compare effective ready/expiry recovery p95/p99/max, not just scan count.

Gate: fewer buffer accesses/calls per grant without worse recovery or tail latency beyond the predeclared tolerance. Otherwise retain instrumentation and abandon the behavior change.

## Phase 4 — Bound background traffic and protect completion

Proceed if connection/CPU/lock pressure correlates with completion latency. Claim admission already has a local gate; discovery and recovery use the shared data source outside it. Local limits multiply across replicas.

Implementation outline:

- Inventory all users of each actual Npgsql pool before claiming reserved capacity. Design explicit bounded budgets for speculative discovery and background recovery; keep completion, heartbeat and owner renewal serviceable.
- Prefer limiting optional work over adding pools/connections. Separate pools are an experiment requiring a total connection budget, not a default solution.
- Budgets must support cancellation, bounded waiting, fairness and recovery progress. A discovery defer preserves its cursor/horizon. Never hold a task/workflow transaction while waiting for an application budget.
- Account for maximum Hub replicas, schedulers, monitor and administrative connections. Document the deployment-wide bound and overload behavior.
- Examine `FireDueAsync` coupling: slow inbox sweep currently delays timer work in the same batch. Separate scheduling lanes only with explicit concurrency limits and timer/reconciliation recovery tests.

Tests: saturated discovery with continuing completions/renewals, bounded waiter count, cancellation/shutdown, two Hubs, owner expiry, eventual recovery under sustained work. Sweep concurrency values 1/2/3 with fixed worker capacity; do not assume more is faster.

## Phase 5 — Reduce native reconciliation duplication

Own `SweepInboxAsync`, timer/reconciliation orchestration and dedicated tests. First capture exact plans for empty, active-event, consumed-event and exhausted-task datasets with retained history. The outer LIMIT bounds output, not necessarily inspected rows.

Consider separate event and exhausted-task discovery queries with independently measured bounded cursors and deduplication. Preserve visibility of new rows that sort before a cursor through periodic resets/recovery. A permanent high-water mark over mutable UUID/state eligibility is unsafe.

If duplicate cross-Hub scans are significant, propose recovery partition ownership with explicit acquisition, renewal, takeover and disabled-topology behavior. Do not assume matching ownership automatically covers timers or future events. Keep row/workflow locks and idempotency authoritative even with ownership.

Tests: future-effective events becoming due without a new notification, consumed/unconsumed inbox, multiple subscriptions, suspended workflows, exhausted activity without result, duplicate notifications, crash/takeover, changing eligibility behind cursor, bounded recovery on large history. Keep unsupported workflow behavior unchanged.

Gate: lower empty-sweep cost and duplicate work with equivalent event/activity recovery and timer latency. Ownership expansion requires a separate architecture review before implementation.

## Phase 6 — Controlled index and write-cost experiments

Only after query attribution. No live index removal in this phase.

- Map every index to uniqueness guarantees and active/fallback/monitor/recovery query plans. Compare broad claim-sequence and hub-sequence indexes individually, then together, on isolated clones.
- Preserve task/sequence identity, idempotency and native uniqueness indexes regardless of low scan counts. Keep candidate and recovery indexes unless an equivalent access path is demonstrated.
- Measure insertion, lease update, heartbeat, completion, retry and next-task insertion: buffers, WAL, CPU, throughput and tail latency. Include growing completed history and active burst after empty statistics.
- Inspect snapshot/history/result payload sizes and time inside locks. Preparing immutable serialization before the transaction is permissible only if revision-dependent validation and persisted semantics remain unchanged.
- Do not combine index deletion, snapshot format changes, outbox, pool tuning and vacuum settings in one arm. Outbox is a separate lock-contention experiment with write/age/failure metrics.

If an index change wins across supported paths, prepare a forward migration using the actual next available migration number, compatible rollout order, concurrent-DDL runner constraints, invalid-index recovery, rollback recreation, deployment scripts and documentation. Never edit historical migration meaning to remove an existing index silently.

Active/history table separation, snapshot delta storage and durable targeted recovery queues are deferred architectural projects requiring separate contracts and migration designs.

## Benchmark protocol and acceptance

Use an isolated stack with explicitly recorded resources. Keep source, topology, flags, dataset, preparation mode and collection settings constant within a comparison. Create equal-history fixtures for both arms; continued append-only history is a confounder, not an equivalent baseline. Do not reset shared databases or statistics.

- At least five measured repetitions per arm after warmup, alternating/bracketing A/B order. Report median, range and paired results; show every failed/outlier run. If variance overlaps the proposed gain, classify the result as inconclusive and extend measurement rather than declare success.
- Primary workload: 4,000 workflows, 100 VUs, two Hubs and 16 processors with recorded slot limits. Keep the durable audit but report its cost separately.
- Additional targeted cells: idle workers/empty queue; large active backlog; growing history; stale-empty statistics; skewed capabilities/buckets; constrained/FIFO work; expiry/retry; timers/events; cancelled/reconnected sessions; Hub failover; worker-count scaling. Select the cells relevant to each phase instead of running an indiscriminate full matrix for every edit.
- Primary metric: PostgreSQL CPU-ms per durably completed task. Supporting metrics: calls/buffers/WAL per completed task, workflow throughput and p95/p99, schedule-to-start, completion pool wait, timer/expiry recovery lag, fairness, memory, retries and durable audit correctness.
- Proposed performance gate, fixed before running: at least 10% median CPU/task improvement with no more than 5% throughput or tail-latency regression, supported by paired runs and uncertainty analysis. These are project targets, not existing measured guarantees. Smaller wins can be reported honestly but do not meet this gate automatically.
- Correctness: zero duplicate durable completion/effect records, zero lost tasks/history, no unexpected attempts in failure-free cohorts, preserved resource/FIFO invariants. Failure-injection cohorts may legitimately retry: verify fencing/idempotency and bounded eventual progress rather than demand zero retries.
- Recovery: no new missed deadlines or regression beyond configured guarantees/baseline tolerances; document actual p95/p99/max and clock/sampling uncertainty. Instrumentation overhead must be reported.

## Build, test and evidence requirements

Use .NET SDK 10 and PowerShell 7. From repository root:

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
# Set WFC_TEST_PG to an explicitly identified isolated migrated database first.
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release
```

Run affected focused tests first, then full PostgreSQL integration tests for final runtime changes. Verify WFC_TEST_PG is nonempty and record database/schema identity; a test silently returning for absent configuration is not database validation. Use existing plan reproduction commands from their READMEs. Do not run shared `dev.ps1 up` or Make refresh as an incidental test setup.

Each phase delivers: source/test diff, exact commands and exit codes, environment manifest, raw evidence location, compact results, correctness/failure coverage, unresolved limitations and a keep/revert/defer decision. Persist essential summaries and reproduction scripts in the repository; do not rely exclusively on ephemeral absolute scratch paths. No credentials in evidence.

## Work sequence and handoff

Dependency order: Phase 0 -> Phase 1 -> attribution decision. Phase 2 correctness investigation can follow baseline independently of performance attribution. Phases 3/4/5/6 are separate experiments selected from measurements; rebaseline after each accepted change. Do not start all four simultaneously.

Suggested review units:

1. Baseline collector, source manifest and measured report.
2. Low-cardinality metrics with exporter verification.
3. Coordinator progress regression test and minimal fix if confirmed.
4. Ready/expiry scheduling experiment and result.
5. Background capacity/completion protection experiment and result.
6. Reconciliation query change; ownership design separately if warranted.
7. Index experiment report; migration only after acceptance.
8. Final integrated correctness/failover run and repeat benchmark, with updated runbook.

Stop a hypothesis when evidence disproves it. Record negative results. Do not claim production readiness from a development benchmark, and do not call lower CPU a win if it merely reflects slower processing.

### Copyable Luna assignment

> Read `scaling/docs/hub-postgres-efficiency-luna-plan.md` and repository guidance. Execute Phases 0 and 1 first, preserving existing work and isolated database boundaries. Reconcile the post-CASE benchmark and throughput-variance reports before proposing runtime changes. Deliver a reproducible baseline, exported low-cardinality hot-path metrics, and an evidence-ranked next implementation choice. Investigate the coordinator progress regression with a focused test. Continue with the highest-supported bounded experiment, one reviewable change at a time, following this plan's correctness and performance gates. Do not reset shared data, remove live indexes, weaken fencing/history, or infer CPU starvation from PostgreSQL percentage alone. Report exact validation, raw evidence, uncertainties and keep/revert/defer decisions. No deployment or merge is requested.
