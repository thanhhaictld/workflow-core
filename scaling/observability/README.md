# Workflow Hub observability and local k6 stress lab

This is an optional, private local stack for the **real Hub-backed three-step
onboarding activity chain**. The legacy Workflow Core IWorkflow<T> executor
is not yet remote; this dashboard complements the previous Next.js monitor.

## Architecture

~~~mermaid
flowchart LR
  K6["k6 workflow stress test"] -->|REST| H["Hub Gateway"]
  UI["Next.js monitor"] -->|server-side Hub API| H
  UI -->|legacy /backend proxy| API["Workflow Core monitor API"]
  API --> P
  H --> P[("PostgreSQL")]
  H -. SSE tasks available .-> W["C# Remote Workers"]
  W -->|REST claim/complete| H
  H -->|OTLP metrics/traces/logs| O["OpenTelemetry Collector"]
  W -->|OTLP| O
  O --> M["Prometheus"]
  O --> T["Tempo"]
  O --> L["Loki"]
  K6 -->|Prometheus remote write| M
  M --> G["Grafana"]
  T --> G
  L --> G
~~~

Hub PostgreSQL pool defaults to 12 sessions. Worker containers have NO
workflow-state database credentials. OTLP metrics export every 5 seconds,
and the Prometheus scrape interval is 5 seconds.

## Start everything locally

Requires Docker Desktop or Docker Engine + Compose v2.

~~~bash
# From the repository root:
cp scaling/observability/.env.example scaling/observability/.env
# Edit scaling/observability/.env to set strong local token/password.

docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  up -d --build --scale worker=4

docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env ps
~~~

PowerShell:

~~~powershell
Copy-Item scaling/observability/.env.example scaling/observability/.env
docker compose -f scaling/observability/compose.yaml --env-file scaling/observability/.env up -d --build --scale worker=4
~~~

The migrate container applies SQL migrations 002–009, followed by the
legacy Workflow Core EF migration in a separate one-shot monitor-api-migrate
container. The Hub and legacy monitor API use the same local PostgreSQL
instance but separate workflow state tables. The Next.js /backend rewrite targets the legacy API. The unified Workflow
runs page merges both persistence backends through server-side APIs (the
browser never sees the Hub bearer token).
The volume is isolated from the previous scaling/compose.monitor.yaml stack.

| Service | Local URL |
| --- | --- |
| Next.js workflow monitor | http://localhost:3002 |
| Unified Workflow runs (all sources) | http://localhost:3002/workflows |
| Only Hub workflows | http://localhost:3002/workflows?hub=hub |
| Onboarding business Hub only | http://localhost:3002/workflows?hub=onboarding |
| Legacy workflows only | http://localhost:3002/workflows?hub=legacy |
| Legacy Workflow Core monitor API | http://localhost:5000/health/ready |
| Grafana | http://localhost:3001 |
| Workflow Hub Grafana dashboard | http://localhost:3001/d/workflow-hub-otel |
| Prometheus | http://localhost:9090 |
| Tempo (Grafana Explore) | http://localhost:3200 |
| Hub health | http://localhost:5100/health/ready |
| OTLP HTTP receiver | http://localhost:4318 |

Grafana credentials: GRAFANA_USER and GRAFANA_PASSWORD from .env.

**One workflow list:** /workflows lists legacy and Hub executions together,
sorted newest-first with a Hub filter (All, Legacy, All Hubs, individual hub).
Each Hub row opens /workflows/{id}?source=hub, showing a zoomable
**distributed-trace-style waterfall** with true Hub lease/attempt timings,
worker identities, queue/scheduling waits, retries, failures and step results.
Legacy rows retain the existing Workflow Core execution waterfall.

New Hub claims are durably recorded in wfc_scale.hub_task_attempt
(SQL migration 009_hub_task_attempts.sql). Claim/finish timestamps are
transactional Hub lease boundaries rather than precise remote handler CPU
time. For a handler-level distributed span, inspect the OpenTelemetry traces
in Grafana Tempo. Old Hub runs without attempt records still show real
completion/failure event markers, but no invented start times or durations.
Hub-generated workflows are not invented or copied into legacy journals.
The old /hub URL redirects to /workflows?hub=hub.

The monitor API's embedded legacy worker is disabled by default so it
does not compete for database connections during Hub stress tests.
To run the legacy demo executor, set MONITOR_EMBEDDED_WORKER=true in .env.

## Run the 1,000-workflow k6 E2E stress test

~~~bash
docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  --profile load run --rm k6
~~~

Defaults: **1,000 durable workflows, 100 concurrent k6 virtual users**.
Each k6 iteration creates one real tenant-onboarding workflow and waits
for its three activities: validate-tenant, provision-tenant, send-welcome.
It checks results and returns a failure when a workflow fails or times out.

Override the load:

~~~bash
docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  --profile load run --rm \
  -e K6_WORKFLOWS=2000 -e K6_VUS=200 \
  -e K6_MAX_WAIT_SECONDS=300 k6
~~~

Results are persisted to scaling/observability/results/k6-summary.json.
On Linux, ensure the k6 process has write permission **only to the
results directory**, not the repository root.

A fresh K6_RUN_ID is generated on each run when unset. k6 sends metrics
to Prometheus via the experimental-prometheus-rw output and the Prometheus
remote-write receiver enabled in this local Compose stack.

**Concurrency nuance:** 1,000 workflow instances in one k6 run does not
guarantee 1,000 simultaneously pending; the workers are already processing
as the test submits them. To reproduce the stricter 1,000-pending test,
use the two-phase .NET harness from the previous Hub branch, or set
K6_MODE=submit with workers stopped, then start the workers and inspect
the run summary. See ../docs/workflow-hub-1000-performance.md.

## What Grafana shows

The provisioned Workflow Hub Stress Test & Runtime dashboard contains:

- Per-process resident memory (RSS), managed heap, .NET threadpool backlog
- Workflow started/completed/failed counters and rates (workflows/sec)
- k6-observed throughput and HTTP p95/p99 response time
- k6 end-to-end workflow p95/p99 latency
- Inflight worker activities, worker execution latency and outcomes
- Ready, leased and dead tasks; PostgreSQL total/active sessions
- Hub claim latency and HTTP server request rates
- SSE subscriber count

Built-in .NET runtime, ASP.NET Core, HttpClient and Npgsql telemetry is
also available in Explore. Metrics use low-cardinality labels only:
hub, activity_type, outcome, service_name and service_instance_id.
No tenant ID, workflow ID, task ID, auth header or payload is used in
Prometheus labels.

### Traces and logs

Grafana > Explore > Tempo: query services workflow-hub and
workflow-hub-worker. Trace context flows from HTTP through Hub and worker
REST calls; the never-ending SSE stream is excluded from server spans.
Grafana > Explore > Loki: inspect OTLP application logs.
WFC_OTEL_LOGS_ENABLED is enabled in this private lab.

Set OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 to export telemetry
from a native .NET process. Without this variable, OTLP export is disabled.

## Health and monitoring checks

~~~bash
docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env config --quiet

curl 'http://localhost:9090/api/v1/query?query=workflow_hub_workflows_completed_total'
curl 'http://localhost:9090/api/v1/query?query=workflow_hub_process_working_set_bytes'
docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env logs --tail=100 \
  hub worker monitor-api monitor otel-collector prometheus grafana tempo loki
~~~

All host-published ports bind to loopback. PostgreSQL, Loki and the worker
do not publish ports to the host. This is **not production security**:
Prometheus remote write and OTLP ingress are unauthenticated inside the
Compose network; the Hub uses a shared development bearer token.
Production needs TLS, worker identity, scoped authorization, limits,
sampling, secret management and retention policies.

## Stop and clean up

~~~bash
docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env down
~~~

This keeps all named data volumes. Only add -v to down if you **explicitly
intend to erase** local workflow, PostgreSQL, Prometheus and Grafana state.

## Caveats

- Hub is a bounded-pool single-node prototype; 12 DB connections protects
  pool exhaustion but does not guarantee throughput.
- Provisioning has a deliberate MaxActivityConcurrency(20) policy.
- The OTel Collector is an in-memory development pipeline with no durable
  log or trace buffer; traces/logs may be dropped during backend failure.
- k6 Prometheus remote write output is experimental and may change.
- Memory gauges represent processes (not total machine RAM or Kubernetes
  container limits). Use an infrastructure exporter if OS-wide metrics
  are required.
- Full remote IWorkflow<T> and subworkflow semantics are separate work.
