# Event-gated matching drain and admission-wait tracing

Stacked on PR #21 (multi-shard matching) at
`perf/matching-event-gated-claim-drain`. This is an **opt-in experiment**
against PR #18 adaptive polling and the first PR #21 matching algorithm.

## What the prior trace proved

[Four-shard CI 42238342002](https://github.com/thanhhaictld/workflow-core/actions/runs/42238342002)
reported 4,029 matching claim RPCs / 2,788 empty (vs 2,340 / 1,221
adaptive) for 2,400 task leases. Activity-only filtering found 2,590 claim
RPCs for 600 leases; 2,259 were empty, 87.2% of activity claim RPCs.
The candidate-query p95 was tens of milliseconds, but pre-query time
included substantial (previously untraced) admission wait.

## Changes

**Flag:** `WFC_HUB_MATCHING_EVENT_GATED=true`, set on *both* Hub replicas,
with `WFC_HUB_MATCHING_ENABLED=true` and
`WFC_HUB_MATCHING_PARTITIONS=4`. Remote processors still use
`WFC_HUB_LONG_POLL_ENABLED=true` and the same partition count.

1. `HubEventGatedClaimDispatcher` keeps pending worker demand in memory,
   grouped by `(hub,matching_partition)`. A shard permits **one active SQL
   claim RPC at a time**. Additional readiness notifications are coalesced
   into capability-scoped pending flags, not assigned to every waiter.
2. A successful claim re-queues one matching capability token, so a second
   compatible waiter can drain additional persisted tasks. An **empty**
   claim consumes its token and stops draining until a new committed hint
   or bounded catch-up probe.
3. PostgreSQL `LISTEN/NOTIFY` remains the primary latency path.
   The existing Hub-wide 2-second recovery sweep still serves legacy SSE
   clients, but **does not wake the matching workers** when the experiment
   is enabled.
4. A single low-frequency 10-second PostgreSQL query groups ready tasks by
   `(hub,task_type,version,task_partition)` and emits at most 512
   capability/shard hints. This recovers missed notifications and expired
   leases. New workers additionally receive one initial, capability-scoped
   catch-up probe per 5 seconds.
5. Admission wait is now measured in the distinct
   `hub.task.claim.admission_wait` span and tagged on its parent
   `hub.task.claim` as `claim.admission_wait_seconds`, separately from
   `db.connection_acquire_seconds`, the candidate query event, and
   the surrounding `hub.matching.claim_drain` span.
6. `workflow_hub_matching_dispatch_attempts` and
   `workflow_hub_matching_dispatch_duration_seconds` distinguish empty
   drains and actual task-leasing drains without high-cardinality labels.

## Safety boundaries

- The dispatcher never owns durable task state. PostgreSQL task leases,
  fencing epochs, resource permits, FIFO sequence and execution journal
  are unchanged.
- One matching shard's owner is still controlled by PR #21's PostgreSQL
  owner lease and epoch; the dispatcher checks ownership before a claim.
- Worker capacity remains bounded by the existing worker. The HTTP poll
  returns a committed lease or waits up to the existing 4 seconds per shard.
- The direct hub-initiated readiness call is suppressed for matching only
  when event gating is enabled. Typed committed PostgreSQL notifications
  and the 10-second recovery scan provide actual wakeup signals.
- Missing notifications can increase Schedule-to-Start latency by as much
  as a recovery interval; this tradeoff must be quantified. A pending
  handoff cannot guarantee exactly-once activity execution if a Hub crashes
  after committing the lease but before returning HTTP.
- The 10-second scan currently has a 512-hint cap; deployments with
  more active capability/shard combinations need cursor pagination or
  per-Hub sharding of the recovery scan.
- No cross-shard forwarding is added. Workers still rotate shards.
- Existing multi-Hub workflow workers still use the adaptive path; this
  experiment improves activity matching first, not end-to-end task
  queue routing for those workers.

## Three-way benchmark and acceptance

CI workflow: `.github/workflows/workflow-hub-event-gated-performance.yml`
runs `adaptive`, `matching`, and `event-gated`, each at 8 and 16
processors for unordered, partition-FIFO and keyed-FIFO policies on the
same two-Hub/four-shard Compose topology.

The HA workflow additionally runs the new event-gated path during
`SIGKILL` of one Hub owner and verifies takeover, durable completion,
at-least-once task replay boundaries, and keyed/partition FIFO invariants.

**Do not enable by default** without a successful build/integration suite,
two-Hub failover, no FIFO regression, reduced empty claim RPCs, and improved
workflow/queue p95/p99 (or a documented latency tradeoff with acceptable
user-facing SLO). Repeat measurements on dedicated resources; CI uses
a shared four-vCPU runner.
