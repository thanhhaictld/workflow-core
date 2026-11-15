# PostgreSQL 4K discovery and recovery investigation (Luna)

Date: 2026-11-15. Scope: candidate discovery, demand/wakeup behavior, expired
lease recovery, and native inbox reconciliation in the two 4,000-workflow burst
window (2026-11-14 17:15–17:20 UTC). This is a source and archived-evidence
review. No runtime code was changed and no live SQL or profiling probe was run.

## Evidence boundary

The two bursts each completed 4,000 workflows / 16,000 tasks without extra
attempts. In the five-minute window, PostgreSQL CPU was estimated at 385.15
CPU-seconds, with a 3.29-core peak on a 16-CPU Docker VM. These totals include
background activity and post-run auditing; neither they nor statement elapsed
time isolate candidate CPU cost. Archived candidate counters report about
93.69k candidate discoveries versus 32.16k grants (about 2.91 recorded
discoveries/grant) and mean mixed-batch fill 12.19/32. The counters are not
unique task IDs or proof of worker utilization.

The due-ready discovery statement (query ID
`3737985904634580822`) accumulated 11.58 s execution time, expired discovery
(`-5021478638737671992`) 6.47 s, and native event/exhausted-task
reconciliation (`-5706908197263325917`) 3.78 s. These are execution elapsed
times, not CPU attribution. A separate cumulative `pg_stat_statements`
snapshot spans four runs: it reports over 27k expired discovery calls and over
8k inbox reconciliation calls, both with zero returned rows. Do not attribute
those cumulative counts to the two-run window. The baseline review gives the
full measurement caveats and query descriptions in
[`postgresql-4k-review-2026-11-15.md`](../postgresql-4k-review-2026-11-15.md).

## Ranked findings

### 1. Empty candidate streams fan out across each owned partition

**Observed in code:** `HubPartitionCandidateRegistry` defaults to a 10-second
recovery interval, 128-row pages and a single query scheduler. For each
matching owner it enumerates `64 / matchingPartitionCount` buckets and adds a
`ReadyDue` and `Expired` stream per bucket; every eighth sweep it also adds a
sequence-ordered `Ready` probe. With four matching partitions, one full sweep
can therefore issue up to 128 empty-capable due/expired page queries across
four owners, plus the periodic ready probes. Empty buckets still consume
bounded turns. Each owner also captures a horizon with a global `max(sequence)`
query before scanning its streams. The loop runs at 50 ms and allows up to 16
turns after an activation. Sources: [`HubCandidateReader.cs`](../../src/WorkflowCore.Hub/HubCandidateReader.cs#L29),
[`HubPartitionCandidateRegistry.cs`](../../src/WorkflowCore.Hub/HubPartitionCandidateRegistry.cs#L4),
especially stream construction/scheduling/end-of-sweep at lines 370–490 and
the `EndSweepLocked` recovery deadline at lines 525–528; [`HubCandidateAcquisition.cs`](../../src/WorkflowCore.Hub.Gateway/HubCandidateAcquisition.cs#L228).

**Measured:** the window and cumulative call/row counts above show repeated
discovery and empty expired probes. They do not establish that all empty calls
were avoidable: expired leases have no task-ready notification at the moment
their deadline passes.

**Actual demand and notification scheduling:** candidate pages are not
unconditionally queried every 50 ms. In backpressure mode, `RequireDemand`
skips a partition unless it has an unreserved/ungranted credit or waiting
demand with capacity to allocate. The first waiting demand marks a clean
partition dirty when no usable credit is already available; task-ready
notifications mark the bucket dirty, set
`HeadRequested`, and signal the one-slot wake channel. An activation drains up
to 16 registry turns and queues one coalesced continuation if the budget is
exhausted. After a full sweep, absent demand/hints, the next scan is delayed by
the current default 10-second recovery cadence. Thus the fixed 10-second
cadence is a recovery ceiling/idle recheck, not the only scan trigger. The
demand gate is at `RunOneTurnAsync` lines 403–406; first-waiting-demand dirties
the partition at lines 211–219; the default is `Recovery` at option lines 9–16.

**Conclusion:** withdraw the proposed 250 ms–5 s per-empty-bucket schedule.
Those intervals are shorter than the current 10-second recovery cadence and
could multiply empty expired-stream probes (up to 40 times at 250 ms) if a
generation remains demand-eligible. There is no per-stream empty-count and
time-series evidence to show that this would reduce work. Keep the existing
10-second recovery behavior for now. Any later backoff must be selected from
measured per-stream probe cadence, empty rate, and lease-recovery lag; never
exceed the current 10-second recovery bound. Empty-stream delay effects and
any CPU reduction remain hypotheses, not findings.

### 2. Readiness hints bound activations; live sweeps do not advance their horizon

**Observed in code:** the Hub listener coalesces typed PostgreSQL hints for a
short window in a bounded 512-item channel; the readiness broadcaster also
does periodic recovery. Candidate acquisition maps hints to owner/bucket,
sets `Dirty` and `HeadRequested`, and wakes a one-slot channel. Each activation
can run up to 16 registry turns and requeues itself when all 16 were used.
`ReadPageAsync` caps every stream at its captured `UpperSequence` and cutoff.
The rate-limited head catch-up runs after eight forward pages, chooses a hinted
bucket or rotating head bucket, and queries `ReadyDue` from the beginning
(null cursor) without moving the forward cursor. It can therefore recheck
rows ordered before the forward cursor under the same horizon. It is useful for
previously seen ready candidates and readiness/resource rechecks; it is not
only a way to find post-horizon inserts. Notifications do not reset the live
horizon/cursor. Once the sweep ends, `Dirty` can cause a new capture, subject to
the current demand gate. A fresh task whose sequence is above the current
horizon cannot appear in the same-horizon head query. Sources:
[`PostgresHubNotificationListener.cs`](../../src/WorkflowCore.Hub.Gateway/PostgresHubNotificationListener.cs#L42),
[`HubReadinessBroadcaster.cs`](../../src/WorkflowCore.Hub.Gateway/HubReadinessBroadcaster.cs#L84),
[`HubCandidateAcquisition.cs`](../../src/WorkflowCore.Hub.Gateway/HubCandidateAcquisition.cs#L86),
and [`HubPartitionCandidateRegistry.cs`](../../src/WorkflowCore.Hub/HubPartitionCandidateRegistry.cs#L366),
[`HubCandidateReader.cs`](../../src/WorkflowCore.Hub/HubCandidateReader.cs#L29).

**Measured:** the historical candidate-to-grant ratio is 2.91 by advisory
counters; there is no archived activation reason, turn/page yield, or horizon
age measurement to connect that ratio to notification work.

**Recommendation:** retain current head catch-up and forward-cursor behavior.
Do not suppress head queries or replace them with only a fresh-horizon scan:
within a live horizon the head query may rediscover ready rows ordered before
the cursor, and the registry's `Notify(resource: ...)` path clears
blocked/deferred in-memory candidates and immediately rematches them. That
resource-key path is exercised by registry tests; the production
`HubCandidateAcquisition.Hint` bridge passes a task bucket but no resource key.
A fresh horizon is not equivalent to that prompt in-memory/resource-release
path. The current unit test
`HeadCatchupIsRateLimitedAndDoesNotResetForwardCursor` explicitly checks head
re-reads and monotonic forward cursors.

The resource-specific wake behavior is also covered by
[`BlockedResourceDefersLaterPagesUntilWakeAndLeavesOtherWorkEligible`](../../tests/WorkflowCore.Scaling.PostgreSQL.Tests/HubPartitionCandidateRegistryTests.cs#L167).

The stale-horizon inefficiency remains a hypothesis: when task-ready hints
arrive during a sweep, a head query uses the captured sequence upper bound, so
it cannot include a newly inserted higher-sequence task. Hints carry no task
sequence, so the running code cannot tell whether a hint is stale for that
horizon. Do not change this behavior on current evidence. If hint activations
are later coalesced, keep the existing head reads and forward cursors, and
retain one dirty-after-sweep bit that causes a fresh horizon after the current
sweep. Preserve head-bucket rotation, its eight-page rate limit, and the demand
gate; do not repeatedly recapture horizons during a forward scan. This is a
constraint for a future optimization, not a demonstrated reduction.

### 3. Native inbox reconciliation is called on every scheduler batch

**Observed in code:** `HubTimerSchedulerService` ticks at configured
`PollIntervalMs` (default 500 ms) and may have two concurrent batches. Each
`HubTimerStore.FireDueAsync` first invokes `SweepInboxAsync`, then performs a
due-timer lookup even when the inbox scan found no candidates. `SweepInboxAsync`
queries the event/exhausted-task union, then opens a transaction per candidate
and uses event-lane locking, workflow `FOR UPDATE SKIP LOCKED`, idempotent
inbox inserts and durable wakeup insertion. Source refs:
[`HubSchedulingExtensions.cs`](../../src/WorkflowCore.Hub.Scheduling/HubSchedulingExtensions.cs#L13),
[`HubTimerStore.cs`](../../src/WorkflowCore.Hub/HubTimerStore.cs#L20),
[`HubNativeWorkflowStore.cs`](../../src/WorkflowCore.Hub/HubNativeWorkflowStore.cs#L458).

**Measured:** the specific inbox reconciliation statement cost 3.78 s in the
five-minute window; the separate four-run snapshot shows over 8k calls and no
rows. This supports recurring empty scans, but does not identify scheduler
replica count, time per empty scan, or share of CPU. The scheduler also scans
for timers, so a no-inbox result is not a no-op for the whole batch.

**Likely cause (hypothesis):** fixed 500 ms polling runs a recovery union
regardless of whether there are native subscriptions with eligible events or
exhausted native activity tasks. The current code returns no next-deadline
signal, so it cannot cheaply sleep until a future-effective event becomes
eligible.

**Recommendation:** withdraw the proposed 1–5 second empty-result interval.
It is longer than the current 500 ms scheduler tick and may reduce empty inbox
query count, but could increase event/exhausted-task recovery latency by up to
that interval. Preserve the configured 500 ms polling cadence until an
isolated test establishes an acceptable alternative; it is a nominal scan
cadence, not a hard upper bound when both batch slots are busy. Archived calls/zero rows do not show how many scans coincide
with future-effective events or newly exhausted tasks, and no inbox duration
histogram exists. Keep the current 500 ms poll cadence until an isolated
baseline measures empty scan frequency and event-to-wakeup / exhausted-task
recovery lag. Timer due scanning remains on its existing path; any future
inbox-only backoff must retain a finite recovery maximum and reset on relevant
signals.

### 4. Several counters do not reveal empty work or causal wake efficiency

`CandidateDiscoveryRows` increments only by returned item count, so it cannot
count empty page queries or page duration. `CandidateReconciliationQueries`
records the candidate lease-retirement lookup in `HubTaskStore.RetirableCandidateLeasesAsync`,
not registry page scans or native inbox reconciliation; it cannot be used as
the count of either of those families.
Current candidate snapshot gauges expose buffered entries/credits/reservations,
not stream cursors, empty scans, or delay to rediscovery. PostgreSQL hint
counters count received/forwarded notifications, not coalesced activations,
matched candidates, or SQL avoided. Inbox sweep has no dedicated call,
duration, returned-candidate, or empty-result metric.

**Minimal useful instrumentation:**

- Candidate page query count and duration by stream plus `empty|rows|error`,
  `head|forward` page kind, and returned rows. This reveals empty-probe cadence
  without IDs and confirms head rechecks still occur.
- Candidate activation count by trigger (`hint|demand|timer|continuation`),
  turns per activation, and dirty-horizon recaptures. This tests whether wake
  coalescing or continuation bursts are actually doing work.
- Native inbox sweep count, duration, and candidate count by
  `empty|rows|error`. This measures the current 500 ms path and recovery lag.

These are low-cardinality labels only; do not label by hub, workflow, task,
bucket, or owner. Existing hint receive/forward counters and candidate row/grant
counters remain useful context. Do not add an empty-stream timer or alter
`HeadRequested` as part of instrumentation.

## Proposed first step and experiment

No cadence or head-query code/config change is justified by the current
evidence. The concrete first change should be instrumentation only, with the
current registry `RecoveryCadence=10s`, demand gate, 50 ms acquisition timer,
16-turn bound, head catch-up, and scheduler 500 ms interval unchanged.

Run one isolated baseline experiment against a disposable PostgreSQL instance
and test Hub deployment: collect a 10-minute idle/no-backlog period, then run
the same 4,000-workflow / 16,000-task burst, and finally inject one expired
lease and one future-effective native event with notifications suppressed for
those recovery cases. Keep four matching partitions and the existing
configuration. Record candidate page empty rate and interval by stream,
activation triggers/turns, inbox empty scan rate/duration, and time from lease
expiry or event-effective time to durable rediscovery/wakeup. This is an
instrumentation-baseline experiment, not an optimization A/B; it establishes
the current measured cadence before any delay is selected. It cannot prove a
performance improvement.

## Correctness risks and safeguards

- Any future backoff delays recovery of a lease that expires without a
  notification. Bound it by the current 10-second recovery ceiling unless a
  separately approved SLO allows otherwise; verify task schedule-to-start and
  lease recovery lag, not just SQL calls.
- Future-effective events must become visible at/after `effective_at` even if
  their publication notification happened earlier. Retain the current
  configured scan cadence unless a durable timer/wakeup or separately tested
  recovery bound is proven for every event type.
- Any future horizon optimization must retain the head query's same-horizon
  rechecks for previously scanned rows and the rotating head bucket. Do not let
  continuous hints reset forward fairness indefinitely; every bucket must get
  a bounded probe, with current eight-forward-page head rate limit preserved.
- Do not clear uncertain reservations, retire candidate credits, or infer
  commit outcomes from a negative scan. Preserve owner-generation fencing,
  owner takeover, `SKIP LOCKED`, and exact lease-expiry reaping.
- Inbox reconciliation remains idempotent and under the existing event lane
  and workflow lock ordering. Never turn notifications into authority or
  bypass unique inbox keys.
- Measure timer fire lag separately; inbox backoff must not delay timer
  deadlines or the ordinary task candidate path.

## Exact follow-up tests (not run)

The first experiment above only gathers a baseline. Before any later code change,
the candidate registry tests should cover: an existing eligible row before the
forward cursor becoming discoverable via head catch-up; a notification during
a live horizon; the post-horizon sequence row appearing after the next capture;
resource release clearing matching blocked/deferred candidates; and the
existing `HeadCatchupIsRateLimitedAndDoesNotResetForwardCursor` behavior. Keep
the test's assertions for repeated null-cursor head reads and monotonically
increasing forward cursors.

On an isolated PostgreSQL test schema, add recovery checks for a dropped task
notification, due-at crossing during a sweep, silent lease expiry, owner
takeover, future-effective native event, exhausted activity transport attempt,
duplicate event publication, and two concurrent schedulers. Assert eventual
discovery/wakeup, exact owner/epoch fencing, no duplicate lease, event/inbox
idempotency, and latency bounds equal to the retained recovery maximum. These
are acceptance requirements for a future change, not work performed here.

## Limitations and commands

Reviewed repository guidance and current-runtime, Hub implementation, and
final-plan documentation; traced source using `rg -n` and PowerShell
`Get-Content` selections. `git status --short` showed pre-existing staged
changes to the baseline review, observability docs/configuration and telemetry
source; those were left untouched. Only this assigned report was added.

No live PostgreSQL command was attempted, so there was no probe failure or
nonzero exit to report. No tests/build were run: this is an investigation and
the proposed baseline experiment requires an isolated database/workload.
Archived telemetry is from the observed historical interval; no query savings,
CPU reduction, or cadence tradeoff has been measured. The source
links above describe the reviewed worktree state and should be rechecked if
runtime files change before implementation.
