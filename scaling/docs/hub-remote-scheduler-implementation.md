# Embedded scheduler and remote Processor/Provisioner implementation

For the consolidated architecture, task-delivery protocol, Hub scaling modes
and current configuration baseline, start with [current runtime](current-runtime.md).
This runbook supplies detailed endpoints, setup and verification context.

Status: **experimental native fluent runtime and sequential compatibility path**.
Branch: `feat/hub-remote-processor-scheduler`; based on `refactor/scaling-project-layout`.
See [final architecture plan](hub-embedded-scheduler-final-plan.md).

For sharded SSE owner loss, see the [recovery validation](cadence-inspired-scaling-validation.md)
and [design](cadence-inspired-scaling-design.md). A preferred peer's connection
or header timeout now permits a conditional PostgreSQL takeover after expiry;
failure of a live owner's transport never authorizes stealing its lease.
Accepted SSE bodies have no three-second header deadline. Task execution leases
remain independent, so uncertain deliveries can still wait for normal expiry.
`WFC_HUB_OWNER_DIAGNOSTICS=true` enables route exception logs for investigation;
unset it afterward. Check owner epochs/DB time and trusted peer reachability when
diagnosing 503, and do not infer recovery from health checks alone.

## Implemented boundaries

- `scaling/src/WorkflowCore.Hub.Gateway` is generic REST/SSE. It registers no business definitions or concurrency policies.
- `scaling/src/WorkflowCore.Hub` contains `HubDefinitionStore` (PostgreSQL-persisted current workflow metadata, serializable concurrency policies), `HubRemoteWorkflowStore` (fenced state transitions and wakeups) and `HubTimerStore` (durable timer firing).
- `scaling/src/WorkflowCore.Hub.Scheduling` is an independent library with `AddHubScheduling` and a bounded `BackgroundService`. `scaling/src/WorkflowCore.Hub.Server/Program.cs` registers it **inside the Hub process**.
- `scaling/src/WorkflowCore.Hub.Client` provides REST transport. `scaling/src/WorkflowCore.RemoteExecution` reuses native builders, executor, mappings, pointers, error handlers, and middleware. The sequential registry remains for v1 runs.
- Experimental native push is opt-in on Hub and worker with `WFC_HUB_PUSH_ENABLED=true`, in addition to the Hub matching configuration. A single-Hub lane advertises its effective concurrency as credits, subject to its worker cap and session limits. It uses SSE for committed assignments and REST for lease heartbeat and result commits; see [current runtime](current-runtime.md#transport-selection) for routing, fallback and recovery limits.
- Push sessions reserve and claim up to four credits per acquisition turn. The Gateway records one delivery for every committed lease and attempts to send every assignment in the batch; constrained work still follows the single-candidate admission path. Candidate discovery is kicked by coalesced readiness/credit wakeups, drains bounded turns with periodic recovery, and keeps database admission owner-fenced. Successful completion or failure acknowledgements release retained candidate credits promptly; uncertain or unreported leases remain charged until authoritative lease retirement, so a disconnected worker can still occupy candidate capacity for the 45-second lease window.
- `workflow_hub_worker_inflight` now has a low-cardinality `task_kind` label (`activity` or `workflow`) for native worker occupancy. Candidate registry credits, reservations, cached entries, estimated bytes, and credit-capacity fallback counts are exported as process-wide gauges/counters; per-shard `oldestReadyAgeSeconds` remains in the Hub operations snapshot rather than a Prometheus label series.
- `scaling/sample/onboarding/WorkflowCore.Provisioner.Host` publishes registrations and runs separately bounded workflow/activity lanes using **REST/SSE only**.
- `scaling/sample/onboarding/WorkflowCore.Processor.Host` was historical v1-only tooling; the project has been removed. New deployments use the combined native workflow/activity lanes in `WorkflowCore.Provisioner.Host`. The reusable sequential runtime remains in `WorkflowCore.RemoteExecution` for v1 compatibility.
- Removing that sample host does not remove existing v1/v2 data. Unfinished runs require application code that registers their versions; the native Provisioner advertises v3 definitions and does not claim v1/v2 capability. Build fingerprints do not gate registration.
- `scaling/sql/010_hub_remote_scheduler.sql` is forward-only: it does not discard prior Hub executions or legacy workflow history.
- Apply `scaling/sql/020_hub_reconciliation_indexes.sql` after 019 to index active native workflows and dead workflow tasks for periodic reconciliation. The Compose and development migration paths apply it automatically; index creation on an existing large database can briefly block writes.
- Apply `scaling/sql/021_hub_active_task_indexes.sql` after 020 for workflow retirement; index creation can briefly block writes.
- Apply `scaling/sql/022_hub_claim_index_selectivity.sql` after 021 and before deploying the corresponding Hub query. It narrows the ordered claim index so unrelated point updates do not scan the active queue when statistics are stale.
- Apply `scaling/sql/023_hub_claim_hub_sequence.sql` after 022 to support hub-selective ordered claims. Creating the index on an existing large database can block writes; plan deployment accordingly.
- Apply `scaling/sql/024_hub_native_reconcile_tasks.sql` after 023 before deploying the revised native inbox sweep. It indexes exhausted native activity tasks; index creation on a large existing table can block writes.
- Apply `scaling/sql/025_hub_claim_capability_sequence.sql` after 024 to index active tasks by advertised capability for claim scans. Creating the index on an existing large table can block writes.
- Apply `scaling/sql/026_hub_matching_bucket_claim_index.sql` after 025 before deploying the default indexed matching claim path. It indexes capability, `partition_id % 64`, and sequence without changing stored task IDs or routing. Index creation on an existing large table can block writes. `scripts/dev.ps1 db-schema` and observability Compose apply it; set `WFC_HUB_MATCHING_INDEXED_BUCKET_CLAIMS=false` to opt out of the indexed query path.
- Apply `scaling/sql/027_hub_ready_claim_indexes.sql` after 026 before deploying split batch claims. When unconstrained batching is enabled, the default path separately locks ordered ready and expired candidates and merges them by sequence; set `WFC_HUB_SPLIT_BATCH_CLAIM_CANDIDATES=false` to revert to the original query. It can lock up to one batch per state even when fewer are leased; contention and performance must be evaluated under the target workload. The fallback for resource-constrained tasks is unchanged. Index creation on an existing large table can block writes.
- Optional migration `scaling/sql/028_hub_claim_shard4_ready.sql` adds a four-shard ready-task index. Compose and `scripts/dev.ps1 db-schema` install it only with `WFC_HUB_INSTALL_SHARD4_READY_INDEX=true` (default false). Existing indexes are retained. Index creation on a large table can block writes and adds write cost even when the experiment is disabled. Install it before enabling `WFC_HUB_ORDERED_CAPABILITY_MERGE=true`, with batch and split claims enabled, to experiment with an ordered per-capability ready scan for a single Hub, four matching shards, and at most eight distinct capabilities. Other requests use the previous split query. Each capability can lock up to the batch limit, plus one batch of expired rows, until commit; competitors may temporarily skip unselected rows. The normal constrained-task path, task fencing and journal transactions are unchanged. Disable the query flag to roll back query selection; evaluate contention and throughput under the intended workload.
- Migration `scaling/sql/029_hub_partition_candidate_indexes.sql` adds sequence-ordered ready and deadline-ordered ready/expired indexes for the unfinished candidate reader; it does not enable candidate matching. The normal migration uses blocking index builds and requires a maintenance window for a large existing table. Alternatively install equivalent indexes with `CREATE INDEX CONCURRENTLY` outside a transaction before running the migration. Check `pg_index.indisvalid` for interrupted builds and remove invalid indexes before retrying; retain existing indexes and data.
- `WFC_HUB_REUSE_SNAPSHOT_CONTRACTS=true` opts remote workers into per-catalog Json.NET contract metadata reuse (default false). Each encode/decode still uses a separate serializer and the same format and allow-list. It is independent of the matching SQL flags and requires no schema change. See the [throughput investigation](snapshot-contract-reuse-throughput.md) for measured scope and limitations.
- The existing Next.js unified workflow list and waterfall remain in place; durable `TimerScheduled` and `TimerFired` journal events create waiting bars. OpenTelemetry spans remain short-lived, rather than spanning long waits.

**Contract note:** native runs use execution-pointer snapshots and durable inboxes. Tested scenarios include mappings, branches, loops, parallel joins, delays, schedules, recurrence, events, activities, retries, cancellation, and compensation. Upstream's unimplemented `SubWorkflowStepBody` is rejected. See the [native runtime guide](hub-native-fluent-runtime.md) for contracts and limits. Existing sequential and inline runs retain their execution modes.

## Local startup: Docker Compose

From the repository root:

```powershell
Copy-Item scaling/observability/.env.example scaling/observability/.env
# Edit .env: set a strong WFC_HUB_AUTH_TOKEN and GRAFANA_PASSWORD
docker compose -f scaling/observability/compose.yaml --env-file scaling/observability/.env up -d --build --scale provisioner=4
```

Services include `hub` with its scheduler and the combined `provisioner`. The Provisioner runs native workflow and activity lanes (including onboarding v3 and v4); no separate processor is deployed. Application hosts have no `WFC_PG`. Set `WORKFLOW_WORKERS` and `ACTIVITY_WORKERS` independently.

- Next.js unified workflow list: http://localhost:3002/workflows
- Grafana performance and timer-lag panels: http://localhost:3001/d/workflow-hub-otel
- Hub readiness: http://localhost:5100/health/ready

The provisioner also registers `tenant-onboarding` v4 alongside v3. V4 adds
`configure-tenant-access` between provisioning and the welcome activity; this
sample activity simulates configuration and does not change real access. V3
remains available for existing runs. Select v4 explicitly when starting a Hub
workflow to exercise it. The monitor's `/definitions` YAML viewer currently
reads the separate legacy definition archive, not Hub provisioner registrations,
so these Hub versions are not available there for comparison.

## Workload and timer smoke

Default k6: 1,000 **three-activity** workflows (100 VUs). The timer workflow `tenant-onboarding-timer` has four steps, with a durable two-second delay between validation and provisioning.

```powershell
docker compose -f scaling/observability/compose.yaml --env-file scaling/observability/.env --profile load run --rm k6

# Run a timer-based load (1000 waiting workflows by default):
docker compose -f scaling/observability/compose.yaml --env-file scaling/observability/.env --profile load run --rm -e K6_DEFINITION=tenant-onboarding-timer k6
```

To stage 1,000 concurrently *pending* workflows rather than 1,000 submissions while workers are running, use `scaling/sample/onboarding/WorkflowCore.Hub.LoadTest` with `WFC_LOAD_PHASE=submit`, start the combined `WorkflowCore.Provisioner.Host` afterward, and then run `WFC_LOAD_PHASE=verify`. Provisioner accepts `--register-only` to publish metadata before staging.

## Hub REST contract (dev bearer auth)

| Endpoint | Description |
| --- | --- |
| `POST /api/hub/definitions` | Upsert current workflow metadata by ID/version |
| `POST /api/hub/activity-registrations` | Persist declarative resource concurrency constraints |
| `GET /api/hub/definitions` and `/policies` | Read published business metadata, including previous in-memory definitions |
| `POST /api/hub/workflows` | Idempotently create a remote workflow and its first workflow task |
| `GET /api/hub/workflows/{id}/runtime` | Processor retrieves revision, input, persisted results, state, definition identity |
| `POST /api/hub/workflows/{id}/transitions` | Commit `ScheduleActivity`, `ScheduleTimer` or `CompleteWorkflow` with expected revision and fenced lease |
| `GET /api/hub/workflows/{id}/native-runtime` | Protocol-v1 native snapshot, revision, status, and unread inbox |
| `POST /api/hub/workflows/{id}/native-commits` | Atomic fenced native snapshot/effect/inbox/history commit and task acknowledgement |
| `POST /api/hub/hubs/{hub}/events` | Idempotent native event publication, including future-effective events |
| `POST /api/hub/workflows/{id}/status` | Idempotent native suspend/resume/terminate with revision fencing |
| `POST /api/hub/workers/tasks:claim` | Claim activity or workflow task, filtered by capabilities |
| `POST /api/hub/tasks/{id}/complete` | Fenced completion; schedules a subsequent processor task |
| `GET /api/hub/workflows/{id}/history` | Unified persisted activity/processor/timer journal |
| `GET /api/hub/workflows/{id}/trace` | Attempt waterfall (claim/finish timestamps), timer spans added in Next.js |

All mutating workflow operations are SQL transactions. A workflow waits by persisting a timer, releasing the processor, and re-enqueueing work on a future due deadline. SSE readiness is best-effort and reconciled by periodic Hub sweeps.

Native commits return the already-read Hub ID to the gateway internally for
readiness publication, avoiding a post-commit snapshot/inbox reload. The REST
acknowledgement remains `HubTransitionResult`, including on idempotent replay.
Within one native commit, activity registration reads are reused by
`(hub, activity type, version)` under the existing shared policy locks. Each
dispatch still validates its input and derives its own resource keys; the
registration cache is discarded with the transaction so later commits observe
registration and capacity updates.

Workflow and activity registration updates metadata under the supplied identity
and version. Developers keep same-version changes backward compatible and define
a new code version for breaking changes, retaining earlier registrations while
their runs finish. Assembly fingerprints are informational. Registration leaves
existing run state and operator-controlled capacities unchanged.

## Scheduler controls and future extraction

```json
{
  "WorkflowHub": {
    "Scheduler": {
      "Enabled": true,
      "PollIntervalMs": 500,
      "BatchSize": 100,
      "MaxConcurrentBatches": 2
    }
  }
}
```

Configure through `WorkflowHub__Scheduler__Enabled` or `HUB_SCHEDULER_ENABLED` in Compose. The library makes **no HTTP call to the Hub itself**. Its `IHubTimerStore` shares Hub's bounded PostgreSQL data source and only enqueues durable tasks.

For a future dedicated host, reuse `WorkflowCore.Hub.Scheduling` and `IHubTimerStore` in a new `WorkflowCore.Hub.Scheduler.Host` and set `Enabled=false` in Gateway replicas. The dedicated scheduler is Hub infrastructure with DB access; the Processor and Provisioner must remain DB-free. **The dedicated host is not implemented in this branch** because it is intentionally deferred.

## Validation

- `scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/HubRemoteSchedulerTests.cs` covers registration updates, fenced workflow transitions and replay, remote activity completion, a two-scanner due-timer race, and persisted history.
- `HubRegistrationTests` covers same-version metadata/contract updates, concurrent publication, retained operator capacities, waiting-snapshot recovery, and code-defined versions.
- `HubNativeExecutionTests`, `HubNativeRoutedTests`, `HubNativeRuntimeTests`, and `HubNativeTransportTests` cover native/upstream semantic comparisons, typed/scalar mappings, pointer recovery, durable races, cancellation, and completion transport outages.
- `pwsh scripts/test-hub-native.ps1 -ManageHosts` force-restarts Hub and the combined Provisioner during a native wait and verifies native v3 recovery.
- `.github/workflows/workflow-hub-observability.yml` runs Compose, 20 real k6 workflows, a timer workflow, monitor history/waterfall assertions, metrics export.
- `.github/workflows/distributed-runtime.yml` includes a two-phase 1000-workflow CI test and the PostgreSQL test suite.
- Verify CI passes before reporting performance numbers. Relative to the earlier inline activity-chain benchmark, the remote path has additional processor task/REST overhead: remeasure rather than reusing historical throughput/latency.

## Remaining work before production

Production identity/scoped authorization, operator recovery workflows, live cross-version migration, scheduler-health readiness, a dedicated scheduler host (optional), and a full 1000-due-timer failover stress test remain. Native workflows use the upstream cancellation/error semantics. `SubWorkflowStepBody` remains unsupported because it is unimplemented upstream.

The system currently uses a shared **development bearer token**, local unauthenticated OTel/Prometheus remote write, and business activity handlers that simulate side effects. Do not expose this Compose stack publicly or infer production readiness from the smoke test.


## Claim and activity throughput telemetry

Worker-observed HTTP claim and poll metrics, their bounded labels, histogram
boundaries, durable completion semantics, and process/export limitations are
specified in the [observability guide](../observability/README.md#worker-claims-and-durable-activity-completions).
They are distinct from internal database claim scans and do not establish
exactly-once execution of external effects.
