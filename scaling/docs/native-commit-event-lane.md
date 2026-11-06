# Native workflow commits: remove Hub-wide serialization where no events are involved

Branch: `feat/partition-free-native-commit-lock` (stacked on
`feat/native-commit-metrics-batch-history`).
Tested against PostgreSQL 17 and the Hub REST/DB-free processor architecture.

## Problem

Before this change, `HubNativeWorkflowStore.CommitAsync` always executed:

```sql
SELECT pg_advisory_xact_lock(hashtextextended(@hub,12012));
```

**before** acquiring the per-workflow row lock and before performing every
snapshot update, native event-journal batch insert and task acknowledgment.
A Hub with 64 logical partitions and eight separate processors therefore
still serialized **all native commits** for that Hub. No business activity,
per-resource, or FIFO policies are required to encounter this contention.

### Previous native commit path

```text
worker 1 -- \                         +--------------------------------------+
worker 2 ---- \                        | PostgreSQL Hub advisory transaction |
worker 3 ------> Hub REST commit ----> | lock ("offboarding")                |
worker 4 ---- /                        | workflow row + task fencing         |
worker 8 -- /                          | history INSERT + snapshot + ack     |
                                      +--------------------------------------+
                                            only one at a time for the Hub
```

## Changed path (default)

`WFC_HUB_NATIVE_EVENT_LANE_MODE=subscriptions` (default) separates two
classes of commits without changing the REST protocol, the native commit
contract, lease fencing, or the durable atomic transaction:

1. A **subscription-free** commit has no new subscriptions in the request
   and no existing `wfc_scale.hub_native_subscription` rows for the workflow.
   It skips the Hub advisory lock and uses
   `SELECT ... FROM wfc_scale.hub_workflow WHERE id=@id FOR UPDATE`,
   then the existing task-lease lock and fenced commit. Unrelated workflow
   row locks are independent.
2. A commit **registering or removing subscriptions** keeps the Hub advisory
   lock **before** acquiring workflow/task row locks. This preserves atomic
   synchronization with concurrent event publication. Old subscriptions must
   also be checked: checking the incoming subscription list alone is unsafe.
3. `PublishEventAsync` retains the same Hub advisory lock before inserting
   the event and matching/waking subscribed workflows. `SweepInboxAsync`
   also retains the original lane for reconciliation. Existing read/modify
   logic, `FOR UPDATE` and idempotent inbox inserts remain unchanged.

```text
                Native commit
                       |
          SQL: read Hub + EXISTS(old subscriptions)
                       |
               +-------+-------+
               |               |
       no old or new        has old or new
       subscriptions        subscriptions
               |               |
               |          LOCK Hub event lane
               |               |
               +-------+-------+
                       |
             LOCK workflow row
                       |
             LOCK fenced task row
                       |
            batch history + snapshot
             + durable task ack
                       |
                  COMMIT
```

### Ordering/correctness argument

For event-sensitive commits, lock ordering is unchanged:
`Hub event lane -> workflow row -> task row`. `PublishEventAsync` takes the
same lane before locking matching workflows. Therefore if an event races a
new subscription, either publication commits first and
`MatchEvents` in `CommitAsync` sees it, or the subscription commits
first and publication sees the subscriber. The event cannot fall between the
two uncommitted states and become permanently invisible.

Subscription-free commits never install or remove event subscriptions, so
a simultaneous publisher has no subscription transition to race on.
Task epoch and workflow revision fencing continue to reject stale/replayed
commits, and inbox writes remain durable across worker crashes.

There is **no additional SQL round trip** on the hot path: the original
query that fetched `hub_id` now also computes indexed
`EXISTS(SELECT 1 FROM hub_native_subscription WHERE workflow_id = @id)`.
Workflows that have ever used subscriptions but no longer have any are
eligible for bypass after the subscription rows are removed.

This is intentionally conservative: the Hub-wide lock remains for
event-sensitive transactions. Sharding by event key is a separate change,
requiring sorted multi-key locks, fanout and reconcile race testing.

### Rollback

```bash
# Restore the original "one native commit at a time per Hub" behavior:
WFC_HUB_NATIVE_EVENT_LANE_MODE=hub docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env up -d --no-deps --force-recreate hub
```

Default behavior is `subscriptions`. Both modes use batched history
by default: `WFC_HUB_BATCH_HISTORY_ENABLED=true`.

## PostgreSQL race regression tests

`HubNativeEventLaneTests` runs with `WFC_TEST_PG`:

- **Eight unrelated native workflows** must all commit and reach `Complete`
  while an external transaction holds the same Hub advisory lock. Before
  this change, they would wait for the Hub transaction to release the lock.
- **Publish concurrently with register**: both operations must wait on a
  manually held Hub lock; after release they may run in either order, but
  exactly one matching event must reach the durable inbox.
- **Remove an existing subscription**: a later commit whose *new*
  subscriptions are empty must still wait for the Hub event lock if old
  subscription rows exist.

Existing native routed, timer, retry, duplicate fencing, crash recovery and
durable history tests continue to run in CI.

## Measured 1,000-workflow same-runner A/B

Verified: [GitHub Actions lock A/B benchmark](https://github.com/thanhhaictld/workflow-core/actions/runs/42148738423).

Environment: 8 native processor containers, 100 k6 VUs, one Hub, one PostgreSQL
instance, identical synthetic three-step offboarding definitions. Each arm
runs 1,000 workflows on the same runner with batched history enabled.
Hub mode is read from the live container before and after each arm; k6
`--no-deps` prevents accidental Hub recreation. Both finished with zero
workflow failures and valid persisted native history and waterfall trace.

| Measurement | `hub` global lock | `subscriptions` optimized | Change |
| --- | ---: | ---: | ---: |
| Completed workflows/s | **14.827** | **23.336** | **+57.4%** |
| Workflow p95 | 62.312 s | 40.051 s | -35.7% |
| Workflow p99 | 62.517 s | 40.348 s | -35.5% |
| Claim p95 | 412 ms | 333 ms | -19.2% |
| Claim p99 | 1,723 ms | 1,087 ms | -36.9% |
| Hub advisory lock p95 | 261 ms | Not taken for event-free commits | Eliminated on this path |
| Hub advisory lock p99 | 466 ms | Not taken for event-free commits | Eliminated on this path |
| Workflow row-lock p95 | 3.65 ms | 7.50 ms | More independent concurrent commits |
| Workflow row-lock p99 | 7.70 ms | 14.58 ms | More independent concurrent commits |

These are results from **one sequential A/B pair**, not a statistical
capacity guarantee. PostgreSQL page-cache warming, GitHub runner contention,
and worker reconnection after the Hub restart may affect the ratio. Repeat
reversed-order trials with fixed CPU and pool settings before setting SLOs.
Workflow completion p95/p99 includes time spent queued; it is not the runtime
of a three-step body. Prometheus histogram p95/p99 are bucket estimates.

A 1,000,000-workflow test has **not** been run and its p95/p99 are unknown.

## Monitoring and next bottleneck

New metrics and Grafana panels:
- `workflow_hub_native_event_lane_bypass_total`, by bounded
  `event_lane_mode`, counts bypassed attempts (not successful workflow
  completions).
- `workflow_hub_native_workflow_row_lock_duration_seconds_bucket`, by mode.
- `workflow_hub_native_commit_duration_seconds_bucket` and
  `workflow_hub_native_event_lane_lock_wait_seconds_bucket`, now
  distinguish lock mode.

The `ClaimAsync` implementation still scans the first 96 eligible task
candidates repeatedly and uses per-candidate transactions. The pool,
hub REST traffic, PostgreSQL WAL and task claim path can now dominate.
The next P1 should be a set-based `SKIP LOCKED` fast claim for
**unconstrained** tasks while keeping business resource/FIFO admission
on the existing correctness-preserving path.
