# Workflow Core Monitor

Read-only monitoring UI for the distributed PostgreSQL Workflow Core runtime.

The monitor and its .NET API host live under `scaling/monitor/`. The API host
also registers the onboarding and benchmark sample workflows and exposes demo
endpoints. It remains a development host and is not a production-ready API.

- **Admin shell:** Next.js 16, React 19, Tailwind CSS v4, Lucide, and
  a focused adaptation of the MIT-licensed [Shadcn Studio AdminCN Free](
  https://github.com/shadcnstudio/shadcn-nextjs-admincn-admin-template-free)
  color tokens and dashboard layout. The full upstream template and its
  unrelated CRM/mail/calendar pages are **not** copied.
- **Workflow runs:** a single sorted list of legacy Workflow Core and remote Hub
  workflows, filter by legacy/any Hub/individual business Hub, search, status,
  paginated results, manual/15-second refresh. Hub rows open a dedicated
  durable distributed-trace-style waterfall (with lease spans, retry attempts,
  queue wait, zoom and event inspector) in the same /workflows/{id} route.
- **Execution detail:** definition-based interactive workflow flow diagram, current workflow status, elapsed time, correlation IDs,
  retries/errors, a per-attempt waterfall similar to Temporal's trace view,
  step-span inspector, complete paginated journal, execution pointers, and
  leased/dead-letter work items.
- **Real API only:** never substitutes demo fixtures or fake metrics when the
  backend is unreachable.

NexaCRM's workflows page is the **visual/interaction reference only**; this
project does not include its paid source code.

## Prerequisites

- PostgreSQL with the upstream Workflow Core EF schema and
  `scaling/sql/002_distributed_runtime.sql` through
  `scaling/sql/005_execution_history.sql` applied in order
- .NET SDK 10 for `scaling/monitor/api`
- Node.js 22+ with npm
- PowerShell 7+ (`pwsh`) on Windows, macOS or Linux
- Docker Compose v2 (Docker Desktop on Windows/macOS) for local PostgreSQL

## Run locally

**Recommended: bootstrap from the repository root** using **PowerShell 7** on
Windows, macOS, or Linux. GNU Make is optional.

```powershell
pwsh -NoProfile -File ./scripts/dev.ps1 bootstrap
pwsh -NoProfile -File ./scripts/dev.ps1 up
# In a second terminal after the API starts:
pwsh -NoProfile -File ./scripts/dev.ps1 seed
```

If you have GNU Make installed, these are equivalent:


```bash
make bootstrap   # PostgreSQL 17 + EF migrations + SQL 002–005 + npm packages
make dev         # API on 127.0.0.1:5000, dashboard on 127.0.0.1:3000
# In another terminal, optionally create example executions:
make seed
```

The root [Makefile](../../Makefile) only delegates to
[the PowerShell script](../../scripts/dev.ps1); there are no Unix-only shell
recipes. Commands include `make help`, `make up`, `make test`, `make build`,
`make api`, `make ui`,
`make db-down`, and `make db-reset CONFIRM=reset` (deletes local data).
PostgreSQL listens on **127.0.0.1:55432** by default to avoid conflicts with
existing installations. The named Docker volume preserves data after shutdown.
Override `PG_PORT`, `API_PORT`, or `UI_PORT` in the Make invocation, e.g.
`make up PG_PORT=55433 API_PORT=5001 UI_PORT=3001`. Without Make, use
`pwsh ./scripts/dev.ps1 up -PgPort 55433 -ApiPort 5001 -UiPort 3001`.
Windows PowerShell (`powershell.exe`) version 5.1 is **not** supported; use
PowerShell 7 (`pwsh`). The script works without Git Bash or WSL.
Development credentials are **not suitable for production**. Docker is only
needed when using Make targets that provision the local database.

**Manual setup** (if not using the PowerShell runner):

From the repository root, first run the sample .NET API:

```bash
export WFC_PG='Host=localhost;Port=5432;Database=workflow;Username=workflow;Password=...'
dotnet run --project scaling/monitor/api --urls http://localhost:5000
```

In a separate shell:

```bash
cd scaling/monitor
cp .env.example .env.local
npm install
npm run dev
```

Open **http://localhost:3000/workflows**. Set `WORKFLOW_API_URL` in
`.env.local` if the .NET API is listening elsewhere. Next.js rewrites
`/backend/*` to the .NET API's `/api/*`. It does not require a CORS policy
during local development.

## Code-first definition metadata review

The **Workflow definitions** page at `/definitions` displays snapshots
derived from the registered C# fluent workflow definitions. Choose a definition
to compare any two archived versions in an **Added / Removed / Modified**
metadata diff, review individual step schemas, and copy normalized JSON.

The catalog is **read-only for operators**. It never publishes, edits,
deprecates, deploys or selects executable versions. Workflow Core's code-first
registry remains the sole authority for execution. Same-version metadata drift
is surfaced as a warning, without blocking workflow starts.

The `tenant-onboarding-versioned` C# sample defines v1 and v2; the second
adds `ConfigureAccessStep`. Use it to review a meaningful structural diff.
A separate demo API can start either C# version for runtime tests.

Only the separate **instance-level** suspend/resume/terminate controls are
gated by `WFC_ENABLE_MANAGEMENT_WRITES=true` in the private lab. This
environment flag is not production authentication or authorization.

See [code-first metadata review design](
../docs/definition-version-management.md).

## Onboarding sample lab (real workflow history)

Use **PowerShell 7** (Windows/macOS/Linux) from the repository root:

```powershell
pwsh ./scripts/dev.ps1 up      # PostgreSQL, control-plane API, separate worker, Next.js
```

The launcher starts **three distinct processes**: the .NET API
(`WFC_EMBEDDED_WORKER=false`), the standalone worker executable, and the
Next.js monitor. The API is responsible for workflow starts and approval events;
the worker claims fenced PostgreSQL work and executes the steps. Both register
the same workflow definitions from
[`WorkflowCore.Scaling.Samples`](../sample/onboarding/WorkflowCore.Scaling.Samples/TenantOnboarding.cs).

Open [the monitoring dashboard](http://127.0.0.1:3000/workflows)
and use **Onboarding workflow playground** to create three types of instances:

| Scenario | Workflow definition | Expected trace |
| --- | --- | --- |
| Happy path | `tenant-onboarding-auto` | Validate → identity → workspace → access → activate → welcome → Complete |
| Retry | `tenant-onboarding-auto` | Workspace throws once, retries with another attempt ID, then completes |
| Manual approval | `tenant-onboarding-approval` | Steps → WaitFor `OnboardingApproved` → approval signal → resume → Complete |
| Parallel fork/join | `tenant-onboarding-parallel` | Validate → three nested infrastructure/identity/billing lanes → Join → Activate → Welcome |

All steps are simulated in persisted workflow data. They intentionally **do
not create cloud resources, identities, tenants, or send emails**. The
artificial 120ms step latency makes the execution waterfall easier to review.
Each new playground run uses a unique tenant ID as its approval correlation key.

You can also create samples from a second terminal once the API is running:

```powershell
pwsh ./scripts/dev.ps1 seed
# For an approval-gated workflow, copy its UUID from the seed output:
pwsh ./scripts/dev.ps1 approve -WorkflowId <uuid>
# Verify the end-to-end paths, including immutable history and signal delivery:
pwsh ./scripts/test-onboarding.ps1
```

To develop independently, start `pwsh ./scripts/dev.ps1 api`,
`pwsh ./scripts/dev.ps1 worker`, and `pwsh ./scripts/dev.ps1 ui` in
three terminals. Multiple worker processes can use the same PostgreSQL queue
to demonstrate distributed claims and gating.

**Demo API endpoints** (private workstation only):

- `POST /api/demo/onboardings`: request JSON `tenantId`,
  `organizationName`, `requiresApproval`, `simulateTransientFailure`
- `POST /api/demo/onboardings/{workflowId}/approve`: publish a durable
  `OnboardingApproved` event keyed by the example's tenant ID

The monitor itself remains read-only for non-demo workflows. Demo start and
approve buttons are not a general-purpose administrative API. The sample API
has no authentication; never expose it to the public internet. Use unique
tenant IDs: approvals are keyed by tenant ID and would resume multiple waiting
samples if that key is shared.

### Interactive workflow flow diagram

The Trace tab now renders a **top-down workflow flow diagram** above the
existing execution waterfall whenever the exact-version definition topology
is registered. The graph shows ordinary steps, a three-lane fork for
`Parallel().Do(...)`, a logical `Join()` marker, and the post-join
continuation. Nested parallel blocks are supported.

- Edges come from `Sequence.Children` and each step's declared
  `Outcomes`; they are **definition paths**, not observed transitions.
- Node color/status comes from the persisted execution pointers, while
  attempt counts come from recorded attempts. Clicking an executed step
  selects its latest attempt in the existing span inspector.
- **Show/Hide diagram**, keyboard-accessible step selection, and
  **zoom in/out/reset** keep the view usable for large workflows.
- A Join marker never creates a fabricated journal span or duration.
  Unconnected/alternative definition paths are drawn separately without
  made-up transitions. Complex conditionals and loops are not yet represented
  as distinct decision/loop nodes; those need dedicated graph semantics.

The graph does not imply branch-level physical parallelism or replace
OpenTelemetry request traces. The time-axis waterfall below it remains the
source of actual duration/ordering information.

### Parallel trace indentation

For definitions registered with Workflow Core's fluent `Parallel().Do(...).Join()`,
the trace waterfall uses `GET /api/monitor/workflows/{id}/topology` to read
**the exact registered definition version**. `Sequence.Children` identifies
branch roots; `Sequence.Outcomes` identifies the continuation after all
branches have finished. The UI shows collapsible fork groups, indented
branches (including nested forks), actual step attempts/retries in those
branches, and a synthetic **join marker** with its status taken from the
persisted Sequence execution pointer. The marker has **no invented duration**.

This is a **logical hierarchy**, not an OpenTelemetry parent/child span
tree: branch steps currently execute through the Workflow Core executor
under its PostgreSQL workflow gate. Separate worker replicas do not imply
that each branch ran on a distinct machine. Durations come only from
journaled attempt timestamps. History from unknown/unregistered definitions
falls back to chronological attempts without guessed indentation.

## API contract

| GET route | Purpose |
| --- | --- |
| `/api/monitor/overview` | Global workflow counts, recent activity, historical errors |
| `/api/monitor/workflows?search=&status=All&page=1&take=20` | Paginated workflow snapshots |
| `/api/monitor/workflows/{id}` | Instance, pointers, errors and recent work ledger |
| `/api/monitor/workflows/{id}/topology` | Exact-version workflow definition steps, branch child roots and continuation IDs |
| `/api/workflows/{id}/history?after=0&take=300` | Existing append-only history read API |

Supported filters: `All`, `Runnable`, `Complete`, `Suspended`,
`Terminated`. UUID search matches exactly; text search checks definition
and reference (case insensitive). Entries returned by `history` are
**insertion ordered**. The UI draws the waterfall by `occurredAt` and
deduplicates history IDs when refreshing.

The trace view associates `StepAttemptStarted` events with
`StepBodyReturned`, `StepBodyYielded`, `StepBodyThrew`, and
`StepCompleted` by `stepAttemptId`. An unpaired start is shown as
**Uncertain**, never as a confirmed failure. The step body may already
have performed a remote side effect when the worker crashed; retries
require external idempotency/reconciliation.

## Security and production caveats

**This is a development sample, not an authenticated operations console.**
The existing API permits workflow starts and signal publications without
authentication and the new monitor endpoints can reveal operational
information across workflows. Deploy only on a trusted development network.

Before production:

1. Protect the .NET endpoints and the Next.js proxy with your existing
   identity provider; enforce authorization and tenant scoping at the
   query layer (not just in the frontend).
2. Keep error messages redacted. Setting
   `WFC_MONITOR_EXPOSE_ERROR_MESSAGES=true` is a **development-only**
   troubleshooting switch; error messages can contain secrets/PII.
3. Add a retention strategy, partition the journal for scale, implement
   audit-role permissions, and benchmark query indexes.
4. Provide proper incremental/history CDC semantics for high-throughput
   multi-worker deployments. PostgreSQL identity IDs are not a global
   commit-order stream; late-committing records could be missed by a
   naive high-watermark cursor.
5. Add tracing correlation to OpenTelemetry spans if request-to-service
   tracing (not only workflow step history) is required.

The UI is intentionally **read-only**: no unsafe reset/replay/terminate
controls are included. A deployment can configure data refresh but cannot
mutate workflow execution from this dashboard.

## Build

```bash
cd scaling/monitor
npm run check-types
npm run build
```

CI also builds the .NET API and executes the distributed PostgreSQL tests.

## Attribution

Shadcn Studio AdminCN Free, MIT License:
https://github.com/shadcnstudio/shadcn-nextjs-admincn-admin-template-free

This monitor copies/adapts the free template's public theme conventions.
It does not redistribute NexaCRM (paid) or imply affiliation with Shadcn
Studio.

## Unified Hub monitoring (observability Compose)

The observability Compose stack provides both the legacy API and the
authenticated Workflow Hub. Open /workflows at http://localhost:3002;
use the **Filter by hub** select for All hubs + legacy, Legacy Workflow
Core, All Workflow Hubs, or a specific Hub namespace such as onboarding.
The old /hub URL redirects to the same list with hub=hub selected.

Hub rows show the business Hub label and link to the same workflow detail
path with ?source=hub. The detail uses a **waterfall time ruler** comparable
to the legacy distributed trace: workflow duration, activity lanes, actual
fenced claim→commit lease spans, retry spans, queue waits, worker identities,
click-to-inspect attempt metadata, and 1x–8x zoom. Completion and failure
events remain in a separate journal tab. Activity claims are not precise
remote handler start timestamps: use Tempo for CPU/handler execution spans.
For pre-migration runs missing claim timing, display only honest terminal
event markers rather than fabricated duration bars. SQL migration 009 must
run for new attempts to be recorded. Legacy rows keep the existing
Temporal-style flow and paginated execution history.

Next.js server-side routes /api/workflow-feed and
/api/hub-workflows/{id} protect the Hub bearer token. Search, status and
pagination are resolved across the backends before merging. The current
aggregator fetches a bounded prefix of both lists for global pagination;
this is intended for the local lab, not unbounded production history.
Enable authentication, per-tenant authorization and keyset pagination
before exposing the monitor to external users.

See ../observability/README.md for Docker Compose and k6 usage.


## Hub Operations Dashboard

The monitor now opens at \`/operations\` and has Overview, Activities,
Partitions, Hub & scheduler, Concurrency, Workers and History pages.

Apply migrations through \`011_hub_monitor_sample.sql\` and configure the
monitor server with \`WORKFLOW_HUB_URL\` (Hub base URL) and
\`WORKFLOW_HUB_TOKEN\` (equal to the Hub server's \`WFC_HUB_AUTH_TOKEN\`).
Run the independent Hub Host with its PostgreSQL connection. The browser
only talks to the Next.js API proxy and never sees the Hub token.

The UI refreshes every 15 seconds, filters by business Hub, and offers
1h/6h/24h ranges. One-minute aggregate backlog snapshots are kept for seven
days. No generated demo metrics are ever returned by the API.

**Definitions:** Active workflow count includes waits; runnable workflow
backlog counts distinct workflow IDs with eligible tasks. Activity ready
backlog includes tasks blocked by capacity/FIFO; delayed work is separate.
Capacity-blocked is a snapshot estimate. First-claim P95 is based on durable
attempt history only. Worker list shows active lease owners, not idle workers
or actual partition ownership. For SSE, HTTP and process metrics, use the
existing OpenTelemetry/Grafana dashboards.

Full metric catalog, caveats and deployment:
[Operations dashboard design](../docs/operations-dashboard.md).


For process-local telemetry, optionally set \`WORKFLOW_PROMETHEUS_URL\`
to your internal Prometheus base URL (Compose uses
\`http://prometheus:9090\`). Without it, SQL counters still work, while
SSE/claim/HTTP/timer P95 show N/A. Do not expose Prometheus or the bearer
secret in client-side environment variables.
