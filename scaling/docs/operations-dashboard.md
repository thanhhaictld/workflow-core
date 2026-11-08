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

The observability Compose stack collects PostgreSQL database and table metrics
every 15 seconds with the OpenTelemetry PostgreSQL receiver. The local collector
uses the development PostgreSQL credentials and connects without TLS on the
private Compose network; deployments should use a dedicated monitoring role and
appropriate transport security. PostgreSQL process CPU is sampled separately
by a process exporter sharing the database container's PID namespace. It is a
one-minute rate where 100% represents one core, and may miss processes that
start and exit between scrapes. Recreate the exporter when the PostgreSQL
container is recreated so it joins the new PID namespace. It is not the
exporter's own CPU or a Docker container CPU measurement.

The Hub & scheduler view shows CPU, database size, estimated dead task rows,
and commit and rollback rates. Grafana's **Workflow Hub • PostgreSQL** dashboard
adds trends for these metrics, connections, and table block accesses. Values are
global to the configured database and are not filtered by business Hub. Missing
series, including insufficient samples for rates, remain N/A. Table row counts
are PostgreSQL estimates; dead rows are not necessarily immediately reclaimable.

The collector also reads `pg_stat_user_tables` every 15 seconds for the
`wfc_scale` schema. The Hub & scheduler view shows one-minute rates for
sequential scans, rows read by those scans, index scans, and live table rows
fetched by index scans for `hub_task` and `hub_workflow`; Grafana shows the
same rates by table. Index rows fetched exclude index entries examined and
index-only scan results. Counters may reset when PostgreSQL statistics are
reset; rates need at least two samples and are unavailable while collection
is down. The collector's SQL query receiver is experimental. The query reads
statistics views and does not scan workflow or task tables.


The collector samples `pg_stat_statements` every 15 seconds for the configured
database, exporting cumulative execution time by query ID and the first 500
characters of normalized query text to Prometheus. Grafana ranks the ten largest
estimated increases over the selected time range. The values include waits and
concurrent calls and are not CPU time. Increases are extrapolated estimates and
need at least two samples; new entries, eviction, resets, collection gaps and
short ranges can lead to missing or incomplete results. Historical data starts
when collection is enabled and is limited by Prometheus retention (seven days
in this stack). The dashboard also shows the current statistics reset time.
The query reads a statistics view, but exports a series for every tracked
statement: query text can contain sensitive data and statement churn increases
Prometheus cardinality and storage. Use this development setup only with access
and retention controls appropriate for the data. The local Compose stack
preloads and creates the extension during startup; other deployments must
install and preload it and provide a suitable collector database role.

The local Grafana server uses the existing database credentials for its
server-side datasource. These development credentials have broad database
access; Grafana users who can query the datasource can execute SQL and view
statement text, which may contain sensitive information. Restrict access and
use an appropriately limited database role outside this local stack.
