# Offboarding baseline: native-only workflow, k6 and 8 processor containers

**Branch:** `feat/unconstrained-offboarding-k6` (based on
`feat/hub-postgres-notify-sse`).  
**Status:** Benchmark implementation; not a capacity guarantee.  
**Goal:** Determine whether configured Hub/activity FIFO and resource policies
explain low throughput, while measuring how well a bounded-connection Hub
scales when eight independent remote processor containers execute native steps.

## 1. Hypotheses

- H1: Removing resource-key FIFO/capacity admission improves throughput when
  the same Hub is used.
- H2: Increasing processor replicas from 1 -> 4 -> 8 improves throughput
  until Hub database transactions, workflow state commits or CPU dominate.
- H3: A 2-second polling loop may delay work *discovery*, but LISTEN/NOTIFY
  does not change the per-task claim SQL or database work needed to commit.
- H4: The code's **Hub-scoped EventLane advisory lock**
  (`pg_advisory_xact_lock(hashtextextended(@hub,12012))`) may serialize native
  commits even with no configured business activity policies.
- H5: The current `HubTaskStore.ClaimAsync` walks up to 96 ordered
  candidates per batch and attempts to admit individual tasks using
  `FOR UPDATE SKIP LOCKED`; query count and transaction overhead may
  dominate under sustained parallelism.

**Causal limitation:** Existing onboarding v3 is a three-activity routed
workload with a deliberate ~400 ms provisioning step and a strict per-tenant
resource policy. The new offboarding workflow uses three *local* native steps
with no such delay. Comparing absolute throughput is a useful upper bound,
**not proof that FIFO caused the difference**. Compare the same workload
across worker counts/pool sizes, and use future matched workloads to isolate
activity-transport and policy overhead separately.

## 2. Isolated synthetic offboarding workflow

- `HubId=offboarding` (isolated from `onboarding`, `backup`, `billing`).
- `DefinitionId=tenant-offboarding-fast`, `Version=1`, 64 logical partitions.
- Data: `TenantId`, `Validated`, `AccessRevoked`, `ResourcesRemoved`.
- Native steps: `ValidateOffboarding`, `RevokeOffboardingAccess`,
  `RemoveOffboardingResources`.
- Steps return immediately. **No external effects actually occur.**
- Processor registers **no** `AddActivity` handlers. Therefore no
  `hub_task_constraint` resource rows are created for its tasks: no
  `HubConcurrency` or `ActivityConcurrency` permit keys, no per-tenant
  semaphore, and no strict FIFO activity resource key.
- Workflow tasks nevertheless remain *durable* and require PostgreSQL
  claims, fencing, sequential state progression for each workflow, and
  a transactional history commit. The internal queue is still ordered by
  `hub_task.sequence`. Logical partitions do **not** currently shard
  databases or eliminate the native EventLane lock.

Do not interpret this synthetic workflow as a production tenant deletion
implementation. Real offboarding must call identity/infra services and
handle idempotency, audit, retries and unknown external outcomes.

## 3. Compose workload

The `offboarding-load` profile adds:

| Service | Role | Replicas |
| --- | --- | --- |
| `processor` | Native-only .NET processor, REST+SSE, no DB credentials | 8 |
| `k6-offboarding` | Submission-first k6 driver and run-summary verifier | on demand |
| `hub` | Existing gateway/scheduler, limited PostgreSQL pool | 1 |
| `postgres` | Existing durable queue/state/history | 1 |
| `grafana`, `prometheus`, `otel-collector` | Existing observability | 1 each |

The regular onboarding `provisioner` stays in the default profile and is
scaled to **zero** for a clean offboarding experiment. No `container_name` or
published port exists on `processor`, so it can be scaled to eight. Each
replica gets its own hostname and SSE stream; the Hub's database pool remains
bounded. The application host never reads `WFC_PG`.

## 4. Reproduce

```bash
cp scaling/observability/.env.example scaling/observability/.env

# Do not delete persistent named volumes; migrations are additive.
docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  --profile offboarding-load up -d --build \
  --scale processor=8 --scale provisioner=0

docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  --profile offboarding-load ps

docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  --profile offboarding-k6 run --rm \
  -e K6_OFFBOARD_COUNT=1000 -e K6_OFFBOARD_VUS=100 \
  -e K6_OFFBOARD_TIMEOUT_SECONDS=360 k6-offboarding
```

Equivalently: `make obs-offboarding-up`,
`make obs-offboarding-test`, `make obs-offboarding-ps`.

Do not use `--scale processor=8` alone without the profile: the service is
opt-in to protect the normal onboarding smoke test.

## 5. k6 measurement design

The previous onboarding script uses a VU to both submit and repeatedly poll
each individual workflow. That can make user-side VU availability and
monitoring HTTP traffic influence throughput. The offboarding script instead:

1. Verifies the Hub is ready and the offboarding v1 definition was published.
2. Submits exactly `K6_OFFBOARD_COUNT` workflows with
   `shared-iterations` using `K6_OFFBOARD_VUS` concurrent VUs.
3. Uses a unique run ID and idempotency key per iteration.
4. After submission, polls only `/api/hub/runs/{runId}/summary` until
   all instances complete, fail, or the configurable deadline expires.
5. Verifies one durable `native-runtime` snapshot has all three flags set.
6. Logs overall workflows/sec and server creation-to-last-completion
   workflows/sec, plus server p95/p99 completion latency.
7. Writes independent `results/k6-offboarding-summary.json` and publishes
   measurements to the existing Prometheus remote-write endpoint.

CI uses a small 8-replica smoke workload; run a much larger controlled
workload on your own fixed-size host to compare scaling. The
`maxDuration` of the k6 submit scenario is independent of the teardown
completion wait.

## 6. Measure the bottleneck

Capture the following in both 1- and 8-replica runs:

| Layer | Measurement |
| --- | --- |
| Request | Accepted workflow starts/s; start p95/p99 |
| Workflow | Completed workflows/s; completion age p95/p99 |
| Hub | Claim p95/p99, queue age, empty claims, task scans |
| PostgreSQL | Connection pool, active sessions, transaction throughput, WAL |
| Locks | `pg_stat_activity` wait_event_type = `Lock`, wait_event = `advisory` |
| Processor | Inflight workers, CPU, throttling, memory |
| Transport | LISTEN/NOTIFY hints, SSE connection count, reconnects |

Diagnostic SQL:

```sql
-- There must be zero business activity resource constraints for offboarding.
SELECT count(*) AS offboarding_resource_constraints
FROM wfc_scale.hub_task_constraint c
JOIN wfc_scale.hub_task t ON t.id = c.task_id
WHERE t.hub_id = 'offboarding';

-- Current Hub sessions and lock waits.
SELECT application_name, state, wait_event_type, wait_event, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY application_name, state, wait_event_type, wait_event
ORDER BY count(*) DESC;

-- Task state across the benchmark namespace.
SELECT state, count(*)
FROM wfc_scale.hub_task
WHERE hub_id = 'offboarding'
GROUP BY state;
```

Measure 1/4/8 processors, then 8/12/24 database connections if the physical
database can safely accommodate it. Keep the same benchmark input and CPU
allocation, and repeat each run several times. Vary processor worker
concurrency independently, not all knobs at once.

## 7. Acceptance and risks

- No `AddActivity` registration and no explicit per-hub, activity or resource
  policy on the offboarding sample.
- Exactly eight isolated `processor` replicas; zero normal onboarding
  `provisioner` replicas during the baseline experiment.
- All benchmark workflows reach `Complete`, no dead letters/failures.
- At least one completed native snapshot has `Validated`,
  `AccessRevoked` and `ResourcesRemoved` all true.
- No offboarding `hub_task_constraint` rows.
- Benchmark output is stored separately from onboarding and emitted to
  Prometheus, not confused with overall Hub throughput.
- CI builds the dedicated processor and checks k6 syntax and eight-container
  smoke test.
- Do not change the task claim ordering, Hub event-lane lock or transactional
  fencing as part of **this** experiment. Those require separate correctness
  and performance design, targeted concurrency tests, and before/after
  benchmarks.
