# Workflow Operations Dashboard

## Scope and architecture

The Hub gateway remains a **generic REST/SSE gateway** with PostgreSQL,
concurrency admission and the embedded scheduler. Remote Processor and
Provisioner remain independent hosts. No workflow runtime contract changes.

The Next.js monitor uses a server-only proxy to the authenticated
\`GET /api/hub/operations\` endpoint. It has seven views: Overview, Activities,
Partitions, Hub & scheduler, Concurrency, Workers and History.

\`\`\`mermaid
flowchart LR
    UI[Next.js monitoring UI] -->|same origin| Proxy[Next.js operations API]
    Proxy -->|REST bearer| Hub[Hub gateway]
    Hub --> Reader[HubOperationsStore]
    Reader --> PG[(PostgreSQL Hub state)]
    Sample[HubMetricsSampler] -->|once per minute| PG
    Hub --> OTel[OpenTelemetry]
    OTel --> Grafana[Prometheus / Grafana]
\`\`\`

**Global state is in PostgreSQL**, not a process-local dashboard cache.
SSE connections and runtime process telemetry remain in Grafana because they
are not database facts.

## Metrics and semantics

| Metric | Authoritative source | Definition |
|---|---|---|
| Active workflows | \`hub_workflow\` | State Ready or Running, including waiting |
| Runnable workflow backlog | \`hub_task\` | Distinct workflow IDs with due unclaimed or reclaimable expired tasks |
| Activity ready backlog | \`hub_task\` | Due, unclaimed, attempts remaining |
| Activity leased | \`hub_task\` | Leased with unexpired lease |
| Activity delayed | \`hub_task\` | Ready with future due time |
| Retry-due | \`hub_task\` | Due, ready, attempts greater than zero |
| Dead letters | \`hub_task\` | Terminal state 3 |
| Capacity/FIFO blocked | Resource and constraint tables | Distinct ready tasks blocked by active permits or older FIFO tasks |
| Oldest ready age | \`now()-due_at\` | Due-age, not created-age |
| Waiting workflows | \`hub_workflow.waiting_for\` | Active and waiting for external condition |
| Scheduled workflows | Pending \`hub_timer\` | Active workflow with future pending timer |
| Queue wait P95 | \`hub_task_attempt\` | Time from task creation to first claim, last hour |
| Lease duration P95 | \`hub_task_attempt\` | Claim-to-finish duration, not CPU time |
| Throughput | \`hub_workflow.completed_at\` | Completed workflows in last five minutes divided by five |
| Partition skew | Task backlog grouped by partition | Maximum ready tasks / average ready tasks among returned partitions |
| Concurrency occupancy | Leased constraints / configured resource slots | Aggregated by non-sensitive resource class |
| Worker activity | Active \`hub_task.owner\` | Currently active **lease owners** only |
| Scheduler backlog | \`hub_timer\` | Pending and overdue timer counts |
| Database connections | \`pg_stat_activity\` | Total/active connections to current database |
| Historical backlog | \`hub_monitor_sample\` | Aggregate minute samples; seven-day retention |

Important qualifiers: Active workflows are not runnable backlog. A task
can be ready **and** policy blocked. A queued task can be unclaimable due to
missing worker capabilities; that condition cannot be inferred from resource
capacity alone. Per-policy contention counts can count the same task more
than once because a task may require several resources. Missing trend samples
remain null rather than being rendered as healthy zeros.

First-claim queue P95 is **only available for persisted attempt rows**.
This does not represent every standalone task or a retry-specific wait.
The worker list excludes idle workers and is **not** an ownership/rebalancing
registry. Do not mislabel partition relative backlog as execution-slot
utilization. Future worker/partition registrations should be designed as a
separate durable heartbeat/ownership API.

## Deployment

Apply SQL migrations through
\`scaling/sql/011_hub_monitor_sample.sql\`. The dashboard adds no new .NET
project; its implementation is \`scaling/src/WorkflowCore.Hub/HubOperationsStore.cs\`.

The Hub host needs \`WFC_PG\` and \`WFC_HUB_AUTH_TOKEN\`; the Next.js server
needs \`WORKFLOW_HUB_URL\` and \`WORKFLOW_HUB_TOKEN\`. The Next.js backend
never forwards the bearer token to the browser.

The sampler stores global and per-business-Hub snapshots once per minute
using an idempotent upsert. A once-daily prune keeps seven days of samples.
Trends support 1h/6h/24h windows; incomplete periods have missing samples.
The endpoint is read-only but is intended for development until
authentication, RBAC, audit and tenant isolation are implemented.

## Validation

\`\`\`bash
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release
cd scaling/monitor && corepack pnpm run check-types && corepack pnpm run build
\`\`\`

The PostgreSQL acceptance test asserts queue/lease separation, resource
contention, worker identification and persisted trend reads. CI provisions
PostgreSQL and applies migration 011 before those tests.

**Suggested operational triage:** Runnable workflow backlog, oldest ready
age, activity backlog, blocked resources, partition skew, active lease owners,
first-claim latency and workflow execution history.


## Process-local OpenTelemetry metrics (optional)

Set \`WORKFLOW_PROMETHEUS_URL\` on the **Next.js server** to make the Hub
and Workers panels show live SSE sessions, Hub task claim P95, worker
execution P95, HTTP P95, timer fire lag P95, scheduler error counts and
worker in-flight counts. The Next.js API queries Prometheus server-side;
query names match the existing \`workflow-hub-runtime.json\` Grafana
dashboard. Metrics are global, even if a business Hub filter is active.
Missing or unavailable series are returned as null and rendered as N/A.
