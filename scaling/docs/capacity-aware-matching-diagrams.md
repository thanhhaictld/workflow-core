# Capacity-aware matching: architecture and sequence diagrams

**Scope:** [PR #28](https://github.com/thanhhaictld/workflow-core/pull/28), derived from PR #27 including PostgreSQL optimization migrations 020–022. These diagrams explain the **task delivery/matching path**, not a change to workflow definitions. **Fanout is experimental and default OFF**.

## 1. High-level topology

```mermaid
flowchart TB
    Client["Client / k6"] --> LB["Hub load balancer"]

    subgraph HubCluster["Hub Gateway replicas (2)"]
        HA["Hub A gateway"]
        HB["Hub B gateway"]
        Match["Per-hub / per-shard matching ownership
        Event-gated dispatchers"]
        HA --> Match
        HB --> Match
    end

    LB --> HA
    LB --> HB

    PG[("PostgreSQL
    Task queue / workflow state
    Matching ownership epochs
    FIFO permits / fenced leases / history")]

    Match <-->|"Validate owner, claim task"| PG
    PG -.->|"Committed matching-ready NOTIFY"| HA
    PG -.->|"Committed matching-ready NOTIFY"| HB

    subgraph ProcessorFleet["Remote processor replicas (8 by default)"]
        W["Workflow and activity worker lanes"]
        Planner["Local execution-slot reservation planner"]
        Polls["Concurrent REST long polls
        One per reserved shard; MaxTasks=1"]
        Executor["Run activity / workflow task
        Heartbeat / complete / fail"]
        W --> Planner --> Polls --> Executor
    end

    Polls -->|"POST /workers/tasks:poll"| LB
    LB -->|"Leased task response"| Polls
    Executor -->|"REST heartbeat / completion"| LB
```

*The matching box is a logical responsibility within Hub replicas, **not another standalone service or a single global lock**. Each matching shard has its own PostgreSQL-fenced owner, potentially a different replica. The load-balanced Gateway forwards polls to the current matching owner.*

## 2. Successful task dispatch and completion

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker lane
    participant C as Slot planner
    participant LB as Hub load balancer
    participant H as Hub / shard owner
    participant M as Event-gated matcher
    participant DB as PostgreSQL
    participant X as Task executor

    W->>C: TryReserve(running tasks)
    C-->>W: Up to 4 distinct shard reservations

    par One long poll for shard 0
        W->>LB: POST tasks:poll (shard=0, MaxTasks=1)
    and One long poll for shard 1
        W->>LB: POST tasks:poll (shard=1, MaxTasks=1)
    and One long poll for shards 2 and 3 each
        W->>LB: POST tasks:poll (shard=2/3, MaxTasks=1 each)
    end

    LB->>H: Route / forward each poll to its matching owner
    H->>M: Register in-memory waiter, bounded catch-up probe
    Note over DB,M: A task-ready or permit-release transaction commits
    DB-->>H: NOTIFY matching-ready (hint, not a durable queue)
    H->>M: PublishMatchingHint(...)
    M->>DB: Verify matching-shard owner lease
    DB-->>M: Ownership valid
    M->>DB: ClaimAsync with eligibility, FIFO, permit and epoch checks
    DB-->>M: Persisted fenced lease (at most one task)
    M-->>H: Leased task
    H-->>LB: Long-poll response
    LB-->>W: Leased task
    W->>C: Release completed poll reservation
    W->>X: Execute task asynchronously
    X->>LB: POST heartbeat while running
    LB->>H: Forward heartbeat
    H->>DB: Renew lease subject to fencing
    X->>LB: POST completion / failure
    LB->>H: Forward result
    H->>DB: Commit fenced result and workflow transition
    DB-->>H: Commit successful
    H-->>X: Completion accepted
    Note over DB,H: New eligible work may trigger another readiness notification
    X-->>W: Execution slot freed
    W->>C: Reserve another shard poll as capacity allows
```

Other polls can remain outstanding while the returned lease executes. Completion is not a separate delivery ACK: the worker already uses the existing HTTP heartbeat and completion protocol.

## 3. The worker's capacity invariant

```text
execution slots = 8
max concurrent shard polls = min(4, 8, matching_shards)
each outstanding poll reserves one slot and uses MaxTasks=1

running_tasks + reserved_polls <= execution_slots
```

Example: if 6 tasks are executing, at most 2 shard polls may remain outstanding. The local planner tracks which shards have a reserved poll, ensuring one outstanding poll per shard **per worker lane**. This is not a global per-shard concurrency limit; multiple worker replicas may poll the same matching owner.

| Running tasks | Reserved polls | Unreserved free slots |
|---:|---:|---:|
| 0 | 4 | 4 |
| 3 | 4 | 1 |
| 6 | 2 | 0 |
| 8 | 0 | 0 |

## 4. Timeouts, retries and Hub ownership changes

```mermaid
sequenceDiagram
    participant W as Worker lane
    participant P as Local planner
    participant LB as Load balancer
    participant H as Hub owner
    participant DB as PostgreSQL

    W->>P: Reserve a free slot
    W->>LB: Long poll shard 2
    LB->>H: Resolve / forward to owner
    alt Empty result or normal 4-second timeout
        H-->>LB: [] 
        LB-->>W: Empty response
        W->>P: Release reservation
        W->>P: TryReserve next available shard
    else Hub down or matching ownership lost
        H-->>LB: 503 / network failure
        LB-->>W: Failed poll
        Note over W: Bounded retry delay (~1 second)
        W->>P: Release reservation after reply completes
        W->>LB: Reconnect / new poll
        LB->>H: Route to valid new owner
        H->>DB: Verify fenced owner before a claim
    end
```

The diagram simplifies errors; in code, transport errors are logged and bounded by a one-second delay. A task already leased in PostgreSQL **does not become available immediately when its HTTP connection fails**. Lease expiry/fencing and existing recovery control reassignment. A missed NOTIFY is recovered by matching catch-up probes and periodic PostgreSQL readiness reconciliation.

## 5. Configuration and limitations

```powershell
make obs-setup
# Edit scaling/observability/.env to set suitable secrets.
make obs-offboarding-fanout-up
make obs-offboarding-fanout-ps
make obs-offboarding-fanout-logs
make obs-offboarding-fanout-test
make obs-offboarding-fanout-down
```

The Make targets combine `compose.yaml`, `compose.matching-ha.yaml` and `compose.matching-shards.yaml`, set the matching/event-gated/fanout flags, and use `pwsh` as Make's recipe shell. Fanout is active only for **single-Hub worker lanes with >1 matching shard**. Multi-Hub worker lanes continue the adaptive REST/SSE fallback. There is **no gRPC push, durable worker-credit service, delivery ACK protocol or new task queue**.

**Tradeoffs:** More simultaneous HTTP waiters and individual one-task claims can increase PostgreSQL transactions per workflow and host CPU. The first CI A/B benchmark found promising unordered and keyed-FIFO throughput, but shared runner CPU approached saturation; see [investigation and measured results](capacity-aware-matching-experiment.md). Do not infer dedicated-host scaling or promote the flag from one run.

**Relevant source:** `scaling/src/WorkflowCore.RemoteExecution/HubRuntimeWorkers.cs`, `HubShardCapacityPlanner.cs`, `scaling/src/WorkflowCore.Hub.Gateway/HubEventGatedClaimDispatcher.cs`, `HubTaskMatcher.cs`, `HubGatewayEndpoints.cs`, `PostgresHubNotificationListener.cs`, `HubReadinessBroadcaster.cs`.
