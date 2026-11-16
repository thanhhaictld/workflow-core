# Workflow Hub observability and local k6 stress lab

For current runtime boundaries, transport selection and Hub replica scaling,
start with the [current runtime guide](../docs/current-runtime.md).

For recovery testing on this same Grafana/Prometheus/Tempo/Loki stack, see the
[ownership recovery suite](owner-recovery.md). It layers HA/shard/recovery
settings on `compose.yaml`, reuses `k6-policy` for smoke/stress workloads, and
adds isolated PostgreSQL tests through `pwsh -NoProfile -File scripts/owner-recovery.ps1 db-test`.

This is an optional, private local stack for independently hosted C# workflow processors and activity provisioners.
The generic Hub includes an embedded durable timer scheduler. The combined Provisioner runs native `IWorkflow<T>` workflows and typed activities
over REST/SSE. See [native runtime contracts](../docs/hub-native-fluent-runtime.md).
Workflow code lives in a separate assembly so host policy edits do not alter its
implementation fingerprint. The onboarding host registers v3 and v4 definitions; the default workload uses v3.
Existing v1/v2 data is retained and unfinished old runs require hosts registering
those versions. Build fingerprints are informational, not an execution gate. If reusing an existing
`.env`, set `K6_VERSION=3` for new native runs.

## Architecture

~~~mermaid
flowchart LR
  K6["k6 workflow stress test"] -->|REST| H["Hub Gateway"]
  UI["Next.js monitor"] -->|server-side Hub API| H
  UI -->|legacy /backend proxy| API["Workflow Core monitor API"]
  API --> P
  H --> P[("PostgreSQL")]
  H -. SSE readiness hints .-> PR["Provisioner workflow lane"]
  H -. SSE readiness hints .-> W["Provisioner activity lane"]
  PR -->|REST claim or poll / native commit| H
  W -->|REST claim/complete| H
  H -->|local internal module| S["Embedded Timer Scheduler"]
  S -->|durable timer transactions| P
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

Hub PostgreSQL pool defaults to 12 sessions. Processor and provisioner containers have NO
workflow-state database credentials. OTLP metrics export every 5 seconds,
and the Prometheus scrape interval is 5 seconds.

## Start everything locally

Requires Docker Desktop or Docker Engine + Compose v2, and PowerShell 7.
The Compose launcher derives a stable project name from the worktree path, so
each worktree has its own PostgreSQL and observability volumes. Use the launcher
for every command, including commands with extra Compose files. Moving a
worktree changes its project name. For direct `docker compose` use, set
`WFC_OBSERVABILITY_PROJECT` to a unique, stable Compose project name first.
Host ports still need distinct values in each worktree's `.env` when running
stacks concurrently.

Existing `workflow-hub-observability_*` volumes are not migrated or deleted;
the new project starts with fresh volumes. Stop the old project explicitly with
`docker compose -p workflow-hub-observability ... down` before starting the new
stack if it is still running (set `WFC_OBSERVABILITY_PROJECT` to
`workflow-hub-observability` for that command). Back up and migrate any data
you need separately.

The primary startup command is `make ups-16`. It starts the owner-recovery Compose
stack with 16 processor containers, two Hubs and four matching shards. It builds
the application images, applies forward migrations, and waits for services and
policy definitions. The project name and existing volumes are shared with the
owner-recovery commands; shutdown retains volumes.

~~~powershell
make obs-setup                      # create .env only when missing
# Edit scaling/observability/.env to set a local token and database password.
make ups-16                         # primary startup, golden 16-processor preset
make obs-config                     # inspect resolved Compose settings
make obs-ps
make obs-logs
make obs-test-4k                    # 4,000 workflows / 100 VUs + durable audit
make obs-test                       # 16,000 workflows / 200 VUs / 1,800 seconds
make obs-matcher-failover
make obs-down                       # stop containers; retain named volumes
~~~

The only Make targets are `help`, `ups-16`, `obs-setup`, `obs-config`,
`obs-ps`, `obs-logs`, `obs-down`, `obs-test-4k`, `obs-test`, and
`obs-matcher-failover`. Use `pwsh -NoProfile -File scripts/dev.ps1 ...` for
the separate local development stack, and `scripts/owner-recovery.ps1` directly
for other owner-recovery actions.

The golden preset enables snapshot contract reuse, demand-aware claim admission,
split batch claim candidates, indexed matching buckets, completion fast paths,
batched history, the subscription-based native event lane, scheduler,
adaptive claims, matching candidates, candidate backpressure and cross-worker
batching. Candidate admission concurrency is 2. The dispatcher outbox setting
is `WFC_HUB_OUTBOX_ENABLED=true`; database outbox writes are configured
separately. The periodic SQL metrics sampler, task SSE push, and worker capacity
fanout are disabled in this preset. OTEL metrics, traces and logs remain enabled.

The retained task SSE and capacity-fanout modes are independently controlled by
`WFC_HUB_TASK_SSE_PUSH` and `WFC_HUB_WORKER_CAPACITY_FANOUT`; task SSE takes
precedence when both are enabled. Neither requires the removed single rotating
long-poll worker mode. Existing indexes and data are retained; migrations are
forward-only.

Make exports the golden flags for its short commands, overriding `.env` values;
explicit Make assignments can override them for a run. `OBS_ENV_FILE` and
`OBS_COMPOSE_FILE` select alternate files. Use `make obs-config` to review the
resolved configuration. No Make target removes volumes.

The sections below that describe old `obs-up`, `obs-offboarding-*`,
`obs-stress-test`, `obs-owner-recovery-*`, or `obs-clean` commands are historical
benchmark notes. Those Make aliases have been removed; use the current commands
above or the script and Compose launcher directly. `make ups-16` always keeps
the golden sampler setting false. For a sampler A/B, see
[owner-recovery sampler instructions](owner-recovery.md#start-test-and-stress).

## Co-hosted durable timer scheduler

`WorkflowCore.Hub.Scheduling` runs within Hub Host by default. Set
`HUB_SCHEDULER_ENABLED=false` only when you deploy a separate scheduler
host using the same scheduling module later. PostgreSQL owns timer deadlines;
SSE is a non-durable hint. The timer journal populates actual waiting bars
in the existing Next.js workflow waterfall.

To run a timer-based k6 workload, set `K6_DEFINITION=tenant-onboarding-timer`
in `scaling/observability/.env` and run k6 as below. Timer fire rate and
p50/p95/p99 fire lag are available in Grafana. To compare HTTP throughput,
run the default `tenant-onboarding` definition.

## Historical benchmark: 1,000-workflow k6 E2E stress test

This benchmark recipe documents an earlier Compose profile, not the current Make targets.

~~~bash
pwsh -NoProfile -File scaling/observability/compose.ps1 -f scaling/observability/compose.yaml `
  --env-file scaling/observability/.env `
  --profile load run --rm k6
~~~

Defaults: **1,000 durable workflows, 100 concurrent k6 virtual users**.
Each k6 iteration creates one real tenant-onboarding workflow and waits
for its three activities: validate-tenant, provision-tenant, send-welcome.
It checks results and returns a failure when a workflow fails or times out.

Override the load:

~~~bash
pwsh -NoProfile -File scaling/observability/compose.ps1 -f scaling/observability/compose.yaml `
  --env-file scaling/observability/.env `
  --profile load run --rm `
  -e K6_WORKFLOWS=2000 -e K6_VUS=200 `
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


## Historical benchmark: offboarding throughput baseline

This section records an earlier experiment. Its `obs-offboarding-*` Make aliases are removed; use the direct script/Compose guidance above for retained runtime modes.

This is a **separate, native-only** synthetic workflow for throughput diagnosis:

- Hub: `offboarding`
- Workflow: `tenant-offboarding-fast` version `1`
- Steps: `ValidateOffboarding` → `RevokeOffboardingAccess` → `RemoveOffboardingResources`
- None of the steps is registered through `AddActivity`; there is **no
  activity-level capacity, resource key, or strict FIFO policy**.
- The three steps are lightweight in-memory operations, without deliberately
  waiting for external APIs. This measures the runtime's native task path,
  not a realistic end-to-end tenant deprovisioning service.
- Native workflow tasks still use the database's authoritative claim,
  revision fencing, hub-scoped event-lane lock, sequence scan and durable
  history. There is no switch to bypass those correctness mechanisms.

Start **eight** Docker Compose replicas of the dedicated `processor`
service and stop the normal onboarding `provisioner` for a clean baseline:

~~~bash
cp scaling/observability/.env.example scaling/observability/.env
pwsh -NoProfile -File scaling/observability/compose.ps1 -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  --profile offboarding-load up -d --build \
  --scale processor=8 --scale provisioner=0

pwsh -NoProfile -File scaling/observability/compose.ps1 -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  --profile offboarding-load ps
~~~

Run the separate k6 workload (submits all instances without per-workflow
status polling, then polls only the run summary until all have completed):

~~~bash
pwsh -NoProfile -File scaling/observability/compose.ps1 -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  --profile offboarding-k6 run --rm \
  -e K6_OFFBOARD_COUNT=1000 \
  -e K6_OFFBOARD_VUS=100 \
  -e K6_OFFBOARD_TIMEOUT_SECONDS=360 \
  k6-offboarding
~~~

Historical Make shortcuts `obs-offboarding-up` and `obs-offboarding-test` were removed. The direct Compose commands above remain the documented way to run this optional profile. The old `OFFBOARD_PROCESSORS` Make override no longer applies; adjust `--scale processor=N` in the Compose command to compare counts. Each replica runs its own bounded native workflow worker lane; adjust `OFFBOARD_PROCESSOR_WORKERS` to tune per-pod concurrency.
Each processor has **no** `WFC_PG` credentials or database connection.

The benchmark writes `scaling/observability/results/k6-offboarding-summary.json`
without overwriting the original onboarding k6 results. On success it also
prints completed workflows/second over the full run, the server-observed
creation-to-completion window, p95/p99 workflow duration, and verifies a
persisted workflow snapshot has all three output flags set. Run IDs are unique
unless `K6_OFFBOARD_RUN_ID` is set explicitly.

For diagnosing a bottleneck, compare 1/4/8 processor replicas and
`WFC_HUB_DB_POOL` values separately. Inspect Hub claim duration, ready-work
age, PostgreSQL connection usage, transaction/advisory-lock wait time and
OTel workflow/worker metrics. **Do not attribute a throughput difference
solely to FIFO ordering**: onboarding adds remote activity round trips and an
intentional 400-ms provisioner delay, which are absent here. The Hub's
internal `EventLane` advisory transaction lock serializes some native
workflow commits per hub even though this sample has no business constraints.

See [offboarding benchmark design](../docs/offboarding-unconstrained-benchmark.md).

For the matching HA and SSE variants, clients wait for `hub-lb` to pass its
HTTP readiness check before starting. If k6 reports `lookup hub-lb` or
`Hub not healthy: 0`, check `hub-lb` startup with the same Compose files and
`compose.ps1` project used for the workload. A host port conflict can leave the
load balancer without a network attachment. Set `HUB_LB_PORT` in `.env` to an
unused port (for example, `5111` when another checkout owns `5110`), then
rerun the matching HA or SSE `up` target before retrying the test. Internal
clients continue to use `http://hub-lb:5100`.


### Finding offboarding under Workflow definitions

The **Workflow definitions** page (`http://localhost:3002/definitions`)
now displays both the legacy archived C# catalog and the Hub's persisted,
remote-published catalog. The offboarding definition appears as:

- Hub: `offboarding`
- Definition: `tenant-offboarding-fast`
- Version: `1`
- Execution mode: `native`

Select it to inspect published steps, metadata, and compare distinct versions.
The monitor uses its authenticated **server-side** `/api/hub-definitions`
proxy, backed by `GET /api/hub/definitions`; it does not place the Hub
bearer token in the browser or require the legacy monitoring API to know
about offboarding.

The offboarding processor is an **optional Compose profile**. An ordinary
`docker compose up` does not start it or register a new definition in an
empty database. Start the optional profile with the eight-replica direct Compose
command above before expecting it to appear. Registrations are persistent
and stay visible after those processors are stopped, until the database is
reset.

If the definition is missing, check `/api/hub-definitions` on the Next.js
monitor. A 503/502 indicates a Hub/token connection problem; a successful
empty array means this database has not received a processor registration.

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

Grafana > Explore > Tempo: query workflow-hub and
workflow-provisioner services. Trace context flows from HTTP through Hub and worker
REST calls; the never-ending SSE stream is excluded from server spans.
Grafana > Explore > Loki: inspect OTLP application logs.
WFC_OTEL_LOGS_ENABLED is enabled in this private lab.

Set OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 to export telemetry
from a native .NET process. Without this variable, OTLP export is disabled.

## Health and monitoring checks

~~~bash
pwsh -NoProfile -File scaling/observability/compose.ps1 -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env config --quiet

curl 'http://localhost:9090/api/v1/query?query=workflow_hub_workflows_completed_total'
curl 'http://localhost:9090/api/v1/query?query=workflow_hub_process_working_set_bytes'
pwsh -NoProfile -File scaling/observability/compose.ps1 -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env logs --tail=100 \
  hub provisioner monitor-api monitor otel-collector prometheus grafana tempo loki
~~~

All host-published ports bind to loopback. PostgreSQL, Loki and the worker
do not publish ports to the host. This is **not production security**:
Prometheus remote write and OTLP ingress are unauthenticated inside the
Compose network; the Hub uses a shared development bearer token.
Production needs TLS, worker identity, scoped authorization, limits,
sampling, secret management and retention policies.

## Historical stack shutdown and cleanup notes

~~~bash
pwsh -NoProfile -File scaling/observability/compose.ps1 -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env down
~~~

This keeps all named data volumes. Run `make obs-clean` (or add `--volumes`
to `down`) only when you intend to erase the Compose project’s named volumes,
including local workflow, PostgreSQL, Prometheus and Grafana state.

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


### Worker claims and durable activity completions

For a sampler A/B with it enabled, set `$env:WFC_HUB_METRICS_SAMPLER_ENABLED='true'` before directly running `pwsh -NoProfile -File scripts/owner-recovery.ps1 up -Processors 16`. The `make ups-16` golden preset always sets the sampler to false; clear the environment override and run `make ups-16` to restore that golden setting. Both Hub replicas receive the flag. Disabling the sampler removes the five-second database/queue sampler and its minute-level operations-history writes, while
retaining claim/execution metrics and OTEL export. Sampled queue/connection
series remain absent until a real sample is published, and operations history
has a gap. Collector SQL, on-demand monitor queries, and runtime scheduling
remain active. See [sampler A/B usage](owner-recovery.md#start-test-and-stress).

The `WorkflowCore.Hub` meter exports `workflow_claim_attempts_total`,
`workflow_claim_empty_total`, and `workflow_claim_success_total`, plus
`workflow_claim_duration_seconds` and `workflow_claim_batch_size` histograms.
Each attempt is one worker HTTP claim or long poll, including errors and
cancellation; it is distinct from Hub database claim scans. Empty counts only
successful responses with zero tasks. Success counts returned tasks, not requests.
The duration covers the entire HTTP call, including long-poll wait but excluding
error backoff; its `outcome` is `claimed`, `empty`, `error`, or `cancelled`.
Batch size records every successful response including zero; its explicit bounds
are 0, 1, 2, 4, 8, 16, 24, and 32. Duration bounds are 0.0005, 0.001,
0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
30, and 60 seconds. Both include the implicit +Inf bucket.

`workflow_claim_inflight` tracks pending worker HTTP requests, including held
long polls. It increases before sending and decreases when the request finishes,
including errors and cancellation. It uses `lane` and `transport` labels and
excludes task execution, retry backoff, and persistent SSE task delivery.
`workflow_claim_attempts_total` counts request starts; the duration histogram's
`workflow_claim_duration_seconds_count` counts request finishes with an outcome.
Finishing a request does not mean its returned tasks have completed execution.

Dashboard panels 37–40 show starts versus finishes per second, requests in flight,
finished outcomes, and p95/p99 HTTP latency for both workflow and activity lanes,
split by transport. Rates and percentiles use a one-minute window. In-flight is
summed directly across worker instances, rather than inferred by subtracting
counters. The default Compose stack exports and scrapes metrics every five
seconds; these are sampled aggregates, not individual request timelines.
Process termination can lose final measurements, and an in-flight series from a
terminated instance may persist until it becomes stale. Missing series remain
missing. Long-poll waits can legitimately produce high latency and nonzero
in-flight while no tasks are executing.

Claim labels are bounded: `lane=activity|workflow` and `transport=claim|poll`;
duration also has `outcome`. Workflow-lane tasks are processor work, not
business activities. Success uses the returned task type to classify its lane;
the other claim measurements describe the requesting lane. Activity panels
filter `lane="activity"`. The empty percentage divides by all attempts, so
errors and cancellations are in the denominator; the outcome panel shows them
separately. Percentiles aggregate histogram buckets across instances before
`histogram_quantile`. An absent series or zero denominator is not replaced by
invented data.

`workflow_activities_completed_total` counts newly accepted durable activity
completions in the Hub process, including standalone, managed, sequential remote,
and native paths. Replays, rejected or expired leases, failed transactions, and
workflow processor commits do not count. This is not a guarantee of exactly-once
business side effects. Metrics are process-local and best effort: a crash after
commit before recording, or before export, can lose counts. Use durable task and
workflow history for reconciliation. No identifiers or payload fields are labels
on these metrics. Existing `workflow_hub_*` diagnostics retain their meanings.


### HA load balancer and restarts

`make obs-down` includes the matching HA overlays, so it stops both Hub
replicas and the load balancer without removing named volumes. The HA nginx
configuration resolves Hub service names through Docker DNS at runtime;
recreated containers can receive different IP addresses. DNS refresh may take
up to five seconds. A successful single readiness request does not guarantee
all processors are registered or eliminate transient failures during restarts.
Do not reuse benchmark runs with failed workflow starts for throughput comparisons.
