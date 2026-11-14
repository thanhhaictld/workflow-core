# Hub push comparison, 14 November 2026

This is a single local experiment, not a production capacity claim. The same
native onboarding v3 workload, one Hub, PostgreSQL database, 100 virtual users,
and claim-admission configuration were used for the final four measurements.
One workflow runs three simulated activities and creates ten durable tasks.
Each VU starts and observes one workflow; 100 workflows were measured after a
10-workflow, 10-VU warmup. There was one measured repetition per cell. The
measurement interval begins before the 100 client iterations and ends when
their last status poll observes completion; warmup and measurement durations
are in the raw JSON. After an interruption restarted the Hub, the push-eight
cell used an additional 100-workflow run as its warmup. Clients poll at
0.5-second intervals.

A worker here means **one independently running logical lane in the single
Provisioner process**, with one execution slot. There are equal numbers of
workflow and activity lanes: 4+4 or 2000+2000. For push, each lane has its own
registered session, SSE stream and one credit; the 4,000 sessions were actually
connected in every sampled measurement. For adaptive pull, each lane uses a
separate readiness SSE stream and sends independent claims; all 4,000 streams
were likewise present in every sample. They are not 4,000 OS processes. Hub,
Provisioner and load generator ran as three processes on one Windows host;
PostgreSQL ran in one Docker container on that host.

| Transport | Live lanes | Completed workflows/s | Completed tasks/s | Schedule-to-start p50 / p95 / p99 (ms) | End-to-end p95 / p99 (ms) | Approx. PostgreSQL CPU ms/task |
| --- | ---: | ---: | ---: | --- | --- | ---: |
| Adaptive claim + readiness SSE | 8 | 5.03 | 50.29 | 1203 / 8079 / 9461 | 19894 / 19951 | 13.1 |
| Push | 8 | 4.36 | 43.57 | 1637 / 7170 / 7912 | 21794 / 21879 | 17.7 |
| Adaptive claim + readiness SSE | 4,000 | 1.18 | 11.84 | 3509 / 10364 / 11730 | 81700 / 83302 | 62.7 |
| Push | 4,000 | 5.05 | 50.51 | 1632 / 3488 / 5147 | 19683 / 19924 | 15.9 |

Each row completed 100 workflows and 1,000 tasks. Schedule-to-start uses each
task's database creation time and its first activity attempt or workflow claim
journal time; all 1,000 tasks per row had a recorded start. End-to-end uses
database workflow creation and completion timestamps. CPU/task is a **rough
sample-based estimate**: mean Docker container CPU percent sampled roughly
every 2–3 seconds, multiplied by measurement duration and divided by completed
tasks. It includes all PostgreSQL activity in that container, sampling error,
and idle/background costs; it is not measured statement CPU or a controlled
process counter.

The measured durations were 19.88, 22.95, 84.46 and 19.80 seconds in table
order. The corresponding warmups lasted 4.17, 22.42, 6.37 and 3.14 seconds.

| Transport and lanes | Sampled empty claims | Sampled failed pull claims | Extra task claims | Recorded activity lease expiries | Sampled ended push sessions | Client poll errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Adaptive 8 | 65 | 0 | 0 | 0 | n/a | 0 |
| Push 8 | 366 | n/a | 0 | 0 | 0 | 0 |
| Adaptive 4,000 | 23,600 | 3,400 | 17 | 5 | n/a | 12 |
| Push 4,000 | 37 | n/a | 0 | 0 | 0 | 0 |

Claim and ended-session figures are differences between the first and last
successful diagnostic samples, so omit work outside those samples; the failed
claim counter includes cancellation as well as errors. The lease-expiry count
is from activity attempt rows and does not establish a complete count of
workflow-task expiries. Extra task claims count durable `attempts - 1`, not
proven repeated business execution. There is no independent side-effect or
same-epoch execution observer, and worker-side retry/error totals are not
instrumented, so duplicate side effects, duplicate executions and total
worker retries are **unknown**, not zero. The sample activities simulate side
effects. Zero ended sessions between sampled endpoints is not a measurement of
every possible transient reconnect.

The results show a poorer push result at eight lanes in this run and a large
advantage at 4,000 on this shared machine. At 4,000 adaptive lanes the queue and
failed claims coincided with retries and markedly longer end-to-end latency.
The one-run sample, short warmup, fixed ordering, shared CPU/database and
sampled CPU accounting do not establish repeatability or isolate every
bottleneck. No conclusion rests on HTTP call count alone.

Before enabling admission, a separate 4,000-lane adaptive attempt reached
4,000 readiness streams but completed 0/100 workflows in 182.64 seconds; its
raw artifact is `base4000.json`. Hub logs reported Npgsql pool exhaustion at
the configured pool size of 12 and HTTP 500 claim failures. This failed run is
not substituted for the gated comparison above. The admission gate was then
enabled for **all** four final rows.

## Environment and reproducibility

- Base revision: `eef381e118cbf69b06667af478d555bee4215573`, with the
  uncommitted push implementation in this worktree. The tracked diff and
  SHA-256 hashes of the measured Hub/worker binaries and new contract/session
  source are in `../benchmarks/results/push-local/tracked-working-tree.patch`
  and `../benchmarks/results/push-local/hashes.json`. The later test-only edit
  adds a concurrency assertion and does not change those binaries.
- Intel Core i9-11950H, 8 cores / 16 logical processors, 68,408,107,008 bytes
  host RAM; Docker reports 16 CPUs and 33,501,859,840 bytes available. The
  PostgreSQL container had no explicit CPU or memory limit.
- .NET SDK 10.0.100; PostgreSQL 17.11, `max_connections=100`,
  `shared_buffers=128MB`; Hub data-source pool 12. One matching partition,
  matching enabled, `WFC_HUB_CLAIM_ADMISSION_ENABLED=true`, queue mode and
  limit 3; unconstrained batch claims and FIFO prefilter were left at defaults.
- Raw iteration, error, connection, counter and CPU samples are in
  [`../benchmarks/results/push-local/`](../benchmarks/results/push-local/),
  specifically `rev-base8.json`, `rev-push8d.json`, `rev-base4000.json`, and
  `rev-push4000.json`; their SQL metrics
  use the corresponding `*-measure-20261114-db.json` names. Warmup artifacts
  and earlier attempts are retained there too.

Commands were run from the repository root in PowerShell 7. The database was a
new, separate database; no existing database was reset. The following commands
show the exact configuration, with each server/worker command in its own
terminal; stop a worker before starting the next mode.

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
docker exec workflow-core-monitor-postgres-1 createdb -U workflow wfc_push_bench_revoked_20261114
docker exec workflow-core-monitor-postgres-1 sh -c 'pg_dump -U workflow --schema-only --schema=wfc_scale workflow | psql -q -v ON_ERROR_STOP=1 -U workflow -d wfc_push_bench_revoked_20261114'

# Hub terminal
$env:ASPNETCORE_URLS='http://127.0.0.1:54100'
$env:WFC_PG='Host=127.0.0.1;Port=55432;Database=wfc_push_bench_revoked_20261114;Username=workflow;Password=workflow_dev_only'
$env:WFC_HUB_AUTH_TOKEN='benchmark-local-token-20261114-only'
$env:WFC_HUB_MATCHING_ENABLED='true'
$env:WFC_HUB_MATCHING_NODE_ID='bench-hub'
$env:WFC_HUB_MATCHING_ADVERTISE_URL='http://127.0.0.1:54100'
$env:WFC_HUB_MATCHING_PEERS='http://127.0.0.1:54100'
$env:WFC_HUB_MATCHING_PARTITIONS='1'
$env:WFC_HUB_PUSH_ENABLED='true'
$env:WFC_HUB_DB_POOL='12'
$env:WFC_HUB_CLAIM_ADMISSION_ENABLED='true'
$env:WFC_HUB_CLAIM_ADMISSION_LIMIT='3'
$env:WFC_HUB_CLAIM_ADMISSION_MODE='queue'
$env:Logging__LogLevel__Default='Critical'
dotnet scaling/src/WorkflowCore.Hub.Server/bin/Release/net10.0/WorkflowCore.Hub.Server.dll

# Worker terminal: set PUSH and INSTANCES for each row: false/4, true/4,
# true/2000, false/2000, in that order.
$env:WFC_HUB_URL='http://127.0.0.1:54100'
$env:WFC_HUB_AUTH_TOKEN='benchmark-local-token-20261114-only'
$env:WFC_HUB_PUSH_ENABLED='<PUSH>'
$env:WFC_HUB_MATCHING_PARTITIONS='1'
$env:WFC_HUB_WORKER_LANE_INSTANCES='<INSTANCES>'
$env:WFC_HUB_WORKER_HUB='onboarding'
$env:WORKFLOW_WORKERS='1'
$env:ACTIVITY_WORKERS='1'
$env:WFC_HUB_ADAPTIVE_CLAIMS='true'
$env:Logging__LogLevel__Default='Critical'
dotnet scaling/sample/onboarding/WorkflowCore.Provisioner.Host/bin/Release/net10.0/WorkflowCore.Provisioner.Host.dll

# Load terminal, once for warmup and once for measurement per row.
$env:WFC_HUB_URL='http://127.0.0.1:54100'
$env:WFC_HUB_AUTH_TOKEN='benchmark-local-token-20261114-only'
$env:WFC_BENCH_TIMEOUT='180'
# For example, substitute base8/push8/base4000/push4000 for MODE:
$env:WFC_BENCH_RUN_ID='rev-<MODE>-warm-20261114'
$env:WFC_BENCH_OUTPUT='scaling/benchmarks/results/push-local/rev-<MODE>-warm.json'
$env:WFC_BENCH_VUS='10'; $env:WFC_BENCH_WORKFLOWS='10'
python scaling/benchmarks/hub-push-load.py
$env:WFC_BENCH_RUN_ID='rev-<MODE>-measure-20261114'
$env:WFC_BENCH_OUTPUT='scaling/benchmarks/results/push-local/rev-<MODE>.json'
$env:WFC_BENCH_VUS='100'; $env:WFC_BENCH_WORKFLOWS='100'
python scaling/benchmarks/hub-push-load.py
$run=$env:WFC_BENCH_RUN_ID
Get-Content -Raw scaling/benchmarks/hub-push-metrics.sql | docker exec -i workflow-core-monitor-postgres-1 psql -U workflow -d wfc_push_bench_revoked_20261114 -A -t -v "run_id=$run" | Set-Content -Encoding utf8 "scaling/benchmarks/results/push-local/$run-db.json"
```

The push-eight measurement was `rev-push8d-measure-20261114`, after
`rev-push8c-measure-20261114` warmed the restarted processes. The interrupted
`rev-push8b` attempt failed with connection refused and is retained as an
artifact; it is not included in the table.
