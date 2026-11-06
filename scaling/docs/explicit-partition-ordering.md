# Hub management, partition placement and explicit execution policies

Implementation: `feat/explicit-partition-execution-policies`, stacked on
`feat/partition-free-native-commit-lock`.

## Responsibility boundaries

- **Hub:** authentication/authorization, workflow namespace and definitions,
  management, monitoring, and *optional explicitly configured quotas*.
  An activity using the new policy modes does not acquire an implicit
  Hub-global concurrency resource.
- **Partition:** durable placement and scheduling-distribution key. It does
  **not** imply serialization.
- **Workflow:** workflow-row revision lock, task lease/epoch fencing, atomic
  durable native snapshot+history; separate from admission constraints.
- **Activity admission:** optional explicit policy. Durable PostgreSQL
  resource permits enforce mutual exclusion/FIFO/bounded concurrency across
  replicas and, for keyed policies, across different workflow partitions.

The deployment still runs one REST/SSE Hub Gateway with embedded scheduler.
This change separates **semantics**, not control/data plane processes.

```text
Hub (namespace / management / auth)
  |
  v
Partition = hash(workflowId) % N     <--- immutable after assignment
  |
  +--- Unordered: no permit          <--- parallel, even in same partition
  +--- PartitionSerial: 1 FIFO permit per Hub+partition
  +--- KeyedExclusive: 1 per business resource, no guaranteed FIFO
  +--- KeyedFifo: 1 FIFO permit per business resource
  +--- KeyedBounded: N concurrent tasks per business resource
  |
  v
Hub PostgreSQL queue / worker lease / atomic workflow commit
```

## Code-first usage

```csharp
services.AddHubWorkflowRuntime()
    .AddWorkflow<MyWorkflow, MyData>("tenant-operations", partitions: 64)
    .AddActivity<Validate>("tenant-operations", "validate", 1, o =>
    {
        o.Partitions = 64;
        o.Ordering = HubActivityOrdering.Unordered;
        o.Input(s => s.TenantId);
    })
    .AddActivity<IndexPartition>("tenant-operations", "index-partition", 1, o =>
    {
        o.Partitions = 64;
        o.Ordering = HubActivityOrdering.PartitionSerial;
        o.Input(s => s.TenantId);
    })
    .AddActivity<RevokeAccess>("tenant-operations", "revoke-access", 1, o =>
    {
        o.Partitions = 64;
        o.Ordering = HubActivityOrdering.KeyedFifo;
        o.ResourceName = "tenant";
        o.ResourceField = "tenantId";
        o.Input(s => s.TenantId);
    })
    .AddActivity<Deploy>("tenant-operations", "deploy", 1, o =>
    {
        o.Partitions = 64;
        o.Ordering = HubActivityOrdering.KeyedBounded;
        o.ResourceName = "subscription";
        o.ResourceField = "subscriptionId";
        o.ResourceCapacity = 5;
        o.Input(s => s.SubscriptionId);
    });
```

The example assumes the application has implemented these workflow and step
types, with public input properties. Use `using WorkflowCore.Hub;` and
`using WorkflowCore.RemoteExecution;`.

Legacy registrations intentionally default to
`HubActivityOrdering.Legacy` so their existing Hub and activity capacity
rules do not suddenly disappear on deployment. For new activities,
**explicitly choose `Unordered`** unless ordering or resource limits are
business requirements.

The `HubConcurrency` and `Concurrency` registration values retain the
older implicit Hub/activity admission semantics *only* in `Legacy`.
New ordering modes use no Hub-wide capacity permit. The Hub control plane
can implement explicit quotas separately without using business FIFO locks.

## Correctness and scale tradeoffs

- **PartitionSerial** creates a durable permit key
  `partition:<hubId>:<partitionId>`, capacity 1, FIFO. Activities in a
  different partition can run concurrently, but all explicitly partition-
  serial activity types in the same Hub partition share that permit.
- **KeyedFifo** uses a SHA-256 digest of the configured resource field.
  The permit key is scoped by Hub, resource name and digest, *not* by
  partition. Workflows on different partitions therefore cannot execute
  the same resource concurrently.
- **KeyedExclusive** limits concurrent execution to 1 but allows admission
  out of strict FIFO order. **KeyedBounded** admits up to
  `ResourceCapacity` active leases for the key.
- **Unordered** materializes no resource constraint rows, even when many
  workflows are assigned to the same partition.
- The PostgreSQL `FOR UPDATE SKIP LOCKED` claim algorithm remains
  shared and works on resource permits only when a task actually has them.
  It does not enforce a global Hub/partition ordering for unordered tasks.
- FIFO means increasing durable task `sequence` admission, not guaranteed
  side-effect completion order under retries or failed external systems.
  No framework can make arbitrary remote API side effects exactly once.
- Partition count is set per workflow definition/version; changing it for
  already running workflow instances requires explicit migration. Avoid
  using a partition ID as a tenant/business concurrency key.

### Migration

Apply additive
`scaling/sql/015_explicit_activity_ordering.sql` **after 014** before
deploying a Hub with the new registration payload. Existing rows get
`Legacy`; repeated schema application is safe. `db-schema` in
`scripts/dev.ps1`, Compose and the GitHub integration workflow now include
the migration.

## End-to-end k6 benchmark

[Successful 8-processor E2E benchmark](https://github.com/thanhhaictld/workflow-core/actions/runs/42154061585).

Three synthetic one-activity workflows with **identical 75 ms bodies**;
all routed through the same Hub and eight DB-free processor containers.
500 workflows per mode, 60 submission VUs; persistent PostgreSQL leases and
workflow records verified after every phase.

| Metric | Unordered | Partition FIFO | Keyed FIFO |
| --- | ---: | ---: | ---: |
| Workflows | 500 | 500 | 500 |
| Failed | 0 | 0 | 0 |
| Throughput workflows/sec | **15.875** | 12.018 | 13.812 |
| Completion p95 sec | **28.571** | 38.749 | 33.744 |
| Completion p99 sec | **28.617** | 39.775 | 34.308 |
| Activity task constraints | 0 | 500 | 500 |
| Lease overlaps within tested scope | 468 (permitted) | **0** | **0** |
| Resource keys spanning multiple partitions | N/A | N/A | **8** |

Lease overlap counts represent the number of activity attempts that overlap
with another attempt within the same partition/business key; not overlap
pair counts. For FIFO modes zero overlap is mandatory. Unordered overlap
is intentional and proves no partition-global serialization.

**Caveats:** One sequential k6 trial on a shared GitHub runner, not a
statistical capacity guarantee or an identical workload to the native-only
offboarding test. p95/p99 include time spent queued. Task attempt timestamps
measure lease intervals, not body CPU time. Repeat randomized run order on
dedicated runners to set performance SLOs.

### Reproduce locally

```bash
COMPOSE=scaling/observability/compose.yaml
ENVFILE=scaling/observability/.env.example

POLICY_BENCHMARK_ENABLED=true docker compose -f "$COMPOSE" --env-file "$ENVFILE" \
  up -d --build hub prometheus
POLICY_BENCHMARK_ENABLED=true OFFBOARD_PROCESSOR_WORKERS=4 \
  docker compose -f "$COMPOSE" --env-file "$ENVFILE" \
    --profile offboarding-load up -d --build --no-deps \
    --scale processor=8 processor

for mode in unordered partition keyed; do
  run="local-policy-$mode-$(date +%s)"
  POLICY_BENCHMARK_ENABLED=true K6_POLICY_MODE="$mode" \
    docker compose -f "$COMPOSE" --env-file "$ENVFILE" \
      --profile policy-k6 run --rm --no-deps \
      -e K6_POLICY_MODE="$mode" -e K6_POLICY_COUNT=500 \
      -e K6_POLICY_VUS=60 -e K6_POLICY_TIMEOUT_SECONDS=900 \
      -e K6_POLICY_RUN_ID="$run" k6-policy
  python3 scaling/observability/benchmark/verify_ordering.py \
    --mode "$mode" --run-id "$run" --count 500 \
    --output "scaling/observability/results/$mode.json"
done
```

For production use a real `WFC_HUB_AUTH_TOKEN` and appropriate environment
file. The CI test additionally collects artifacts for each mode.

### Next optimizations (not included)

1. Batched `SKIP LOCKED` claim fast path for unconstrained tasks.
2. Explicit Hub quota policy separate from task ordering permits.
3. Optional partition affinity/ownership, only if queue/DB contention demands it.
4. Repeated k6 benchmarks at 1k, 10k, 100k and eventually 1M scale with
   separate metrics and sustained-workload tests.
