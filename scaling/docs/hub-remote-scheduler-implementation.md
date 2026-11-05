# Embedded scheduler and remote Processor/Provisioner implementation

Status: **experimental native fluent runtime and sequential compatibility path**.
Branch: `feat/hub-remote-processor-scheduler`; based on `refactor/scaling-project-layout`.
See [final architecture plan](hub-embedded-scheduler-final-plan.md).

## Implemented boundaries

- `scaling/src/WorkflowCore.Hub.Gateway` is generic REST/SSE. It registers no business definitions or concurrency policies.
- `scaling/src/WorkflowCore.Hub` contains `HubDefinitionStore` (PostgreSQL-persisted current workflow metadata, serializable concurrency policies), `HubRemoteWorkflowStore` (fenced state transitions and wakeups) and `HubTimerStore` (durable timer firing).
- `scaling/src/WorkflowCore.Hub.Scheduling` is an independent library with `AddHubScheduling` and a bounded `BackgroundService`. `scaling/src/WorkflowCore.Hub.Server/Program.cs` registers it **inside the Hub process**.
- `scaling/src/WorkflowCore.Hub.Client` provides REST transport. `scaling/src/WorkflowCore.RemoteExecution` reuses native builders, executor, mappings, pointers, error handlers, and middleware. The sequential registry remains for v1 runs.
- `scaling/sample/onboarding/WorkflowCore.Provisioner.Host` publishes registrations and runs separately bounded workflow/activity lanes using **REST/SSE only**.
- `scaling/sample/onboarding/WorkflowCore.Processor.Host` was historical v1-only tooling; the project has been removed. New deployments use the combined native workflow/activity lanes in `WorkflowCore.Provisioner.Host`. The reusable sequential runtime remains in `WorkflowCore.RemoteExecution` for v1 compatibility.
- Removing that sample host does not remove existing v1/v2 data. Unfinished runs require application code that registers their versions; the native Provisioner advertises v3 definitions and does not claim v1/v2 capability. Build fingerprints do not gate registration.
- `scaling/sql/010_hub_remote_scheduler.sql` is forward-only: it does not discard prior Hub executions or legacy workflow history.
- The existing Next.js unified workflow list and waterfall remain in place; durable `TimerScheduled` and `TimerFired` journal events create waiting bars. OpenTelemetry spans remain short-lived, rather than spanning long waits.

**Contract note:** native runs use execution-pointer snapshots and durable inboxes. Tested scenarios include mappings, branches, loops, parallel joins, delays, schedules, recurrence, events, activities, retries, cancellation, and compensation. Upstream's unimplemented `SubWorkflowStepBody` is rejected. See the [native runtime guide](hub-native-fluent-runtime.md) for contracts and limits. Existing sequential and inline runs retain their execution modes.

## Local startup: Docker Compose

From the repository root:

```powershell
Copy-Item scaling/observability/.env.example scaling/observability/.env
# Edit .env: set a strong WFC_HUB_AUTH_TOKEN and GRAFANA_PASSWORD
docker compose -f scaling/observability/compose.yaml --env-file scaling/observability/.env up -d --build --scale provisioner=4
```

Services include `hub` with its scheduler and the combined `provisioner`. The Provisioner runs native v3 workflow and activity lanes; no separate processor is deployed. Application hosts have no `WFC_PG`. Set `WORKFLOW_WORKERS` and `ACTIVITY_WORKERS` independently.

- Next.js unified workflow list: http://localhost:3002/workflows
- Grafana performance and timer-lag panels: http://localhost:3001/d/workflow-hub-otel
- Hub readiness: http://localhost:5100/health/ready

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
