# Owner recovery with the existing observability stack

These targets reuse `compose.yaml`, its Grafana provisioning and dashboards,
Prometheus, Tempo, Loki, PostgreSQL, migrations, monitor, application builds and
`k6-policy` workload. They use the same worktree project as `compose.ps1` and
the same `OBS_ENV_FILE` as other `obs-*` targets. There is one runtime stack.

Compose layers, in order:

1. `scaling/observability/compose.yaml`
2. `compose.matching-ha.yaml` (two Hubs and `hub-lb`)
3. `compose.matching-shards.yaml` (matching shards)
4. `compose.owner-recovery.yaml` (four-shard SSE/credit settings and test image)

The last file is a small overlay. It defines no duplicate PostgreSQL, load
balancer, collector, Grafana, Prometheus or storage volumes. Hubs and processors
use the existing `Dockerfile.app`; database tests use `benchmark/OwnerTests.Dockerfile`.

## Start, test and stress

Run from repository root with Docker Compose, PowerShell 7 and GNU Make. Python
3 is used for durable SQL audits and fault tracing; .NET 10 runs inside the
test image. k6 runs in the existing Compose container.

```powershell
make obs-setup                        # preserve an existing .env
make obs-owner-recovery-up
make obs-owner-recovery-test          # full configured PostgreSQL suite + k6 smoke
make obs-owner-recovery-stress-test K6_POLICY_MODE=keyed K6_POLICY_COUNT=1000 K6_POLICY_VUS=100
make obs-owner-recovery-ps
make obs-owner-recovery-logs
make obs-owner-recovery-down          # keep all runtime/telemetry/test volumes
```

`obs-owner-recovery-setup` is also an alias for creating the same env file.
Configure `scaling/observability/.env` as with other suites. `OBS_ENV_FILE` and
`OBS_COMPOSE_FILE` overrides are supported. The launcher keeps the existing
PostgreSQL credentials and applies ordered forward migrations with
`ON_ERROR_STOP`; no schema reset occurs. Both Hubs become healthy before four
processors start. k6 begins only after benchmark definitions are registered.
Each DB-free processor has independent 8-slot workflow and activity lanes.

| Target | Action |
| --- | --- |
| `obs-owner-recovery-up` | Build/start observability, monitor and the recovery runtime profile |
| `obs-owner-recovery-test` | Full configured database tests, then k6 unordered/keyed cohorts of 40 each |
| `obs-owner-recovery-stress-test` | Run existing `k6-policy` with selected mode/count/VUs, then durable audit |
| `obs-owner-recovery-smoke-test` | k6 unordered/keyed cohorts only |
| `obs-owner-recovery-db-test` | Database tests only, isolated from runtime data |
| `obs-owner-recovery-fault-test` | Deliberate owner failures with continuous SQL/SSE/log tracing |
| `obs-owner-recovery-build` / `config` | Build images / show resolved Compose config |
| `obs-owner-recovery-ps` / `logs` | Inspect this worktree's observability and test projects |
| `obs-owner-recovery-down` | Stop/remove both projects' containers/networks; keep named volumes |

The workload supports `K6_POLICY_MODE=unordered`, `partition`, or `keyed`.
Defaults are `K6_POLICY_COUNT=300`, `K6_POLICY_VUS=60`, and
`K6_POLICY_TIMEOUT_SECONDS=600`. Keyed mode uses eight capacity-one resource
keys; partition mode uses the sample's partition-ordering policy. These are
synthetic 75 ms activities, not production capacity measurements.

Smoke/stress runs use `k6/partition-ordering.js`, its completion thresholds and
persisted business-snapshot check, and its Prometheus remote-write output.
After k6 completes, the launcher exports the exact cohort's workflow, task,
attempt and history rows and checks every durable result/completion. Keyed
cohorts must have no FIFO inversions or overlapping attempt intervals. External
side effects still require idempotency/reconciliation.

## Grafana and ports

The base stack's existing dashboards and data sources remain provisioned:

- Grafana: `http://localhost:3001` (credentials in the same `.env`).
- Runtime dashboard: `/d/workflow-hub-otel`.
- PostgreSQL dashboard: `/d/workflow-postgresql`.
- Owner recovery and policy-load dashboard: `/d/workflow-owner-recovery`.
- Prometheus: `http://localhost:9090`.
- Workflow monitor: `http://localhost:3002`.
- Hub LB: `http://localhost:5110`; direct Hubs: `5100`/`5101`.

The added recovery dashboard uses the existing `prometheus` data source and
dashboard provider. It shows k6 policy starts, durable workflow throughput,
owner tracking, resolution failures/fallbacks, task SSE sessions/reconnects,
first successful flush and database task snapshots. Missing series remain
missing. Local owner counters are advisory observations; SQL epochs identify
authoritative ownership. Session-to-first-flush includes idle time and is not
an exact delivery or failover latency measure. The existing general dashboard
also retains panels specific to other k6 workloads; those need their workload's
metrics to display data.

Ports follow `.env`. The overlay adds optional `HUB2_PORT=5101` and
`OWNER_METRICS_PORT=9464` for direct tracing/collector scraping; PostgreSQL stays
unpublished. All exposed ports bind to loopback. For another concurrent stack,
choose distinct ports in its env file or set `OWNER_PORT_BASE` to a nonzero
value: Hub A +0, Hub B +1, LB +10, Grafana +20, monitor +21, Prometheus +30,
Tempo +40, OTLP +50 and raw metrics +89. `OWNER_PORT_BASE=0` preserves `.env`.

`OWNER_PROJECT` optionally overrides the project name. Default identity exactly
matches `compose.ps1`, so ordinary `make obs-ps` sees the same runtime. Use the
recovery targets consistently for this four-shard topology; changing an existing
namespace's shard count requires a migration. A container from a different
Compose base is rejected before the launcher acts on the project.

## Database tests, output and faults

Database tests run in `<observability-project>-owner-tests`, using the **same
base `postgres` and `migrate` service definitions**, a separate PostgreSQL
volume, and the `tests` profile service. Only that project's database/migrations
and test runner start; it has no host ports and cannot clear the runtime DB.
`WFC_TEST_PG` is always configured. Every run writes a new TRX directory; a
missing TRX, zero executed tests or failing test returns a nonzero Make exit.
`OWNER_TEST_FILTER` supports targeted tests, which must not be reported as a
full-suite pass.

Outputs default to `artifacts/owner-recovery` (`OWNER_RESULTS` overrides it).
Each invocation uses a fresh UTC/GUID in run IDs and idempotency keys, so repeated
tests perform new work. Per-cohort k6 summaries, durable row exports and
`analysis.json` are retained in its run directory. k6 also remote-writes to the
base Prometheus, which persists samples for Grafana. Historical committed
evidence is not overwritten.

Normal tests do not inject failures. Explicit fault tests retain the specialized
Python tracer: SQL owner/epoch/expiry samples, zero-credit SSE observers and both
Hub/worker/LB logs. They require the victim to own live shards, keep it stopped
through the 85-second observation window, and restore it afterward. Idle cases
submit 40 workflows; busy cases submit 120. Exact project/service/replica and
Compose-base labels are checked before any fault operation. This target can
interrupt other work you submit to this same observability stack.

```powershell
make obs-owner-recovery-fault-test
make obs-owner-recovery-fault-test OWNER_FAULT_REPEAT=2
make obs-owner-recovery-fault-test OWNER_FAULT_VICTIM=hub-2 OWNER_FAULT_MODE=busy
```

`OWNER_RECOVERY_NOTIFY=false` disables Hub listeners for a missed-hint experiment;
restore it and rerun `up` afterward. `OWNER_RECOVERY_DIAGNOSTICS=true` enables
temporary routing exception logs. Without Make, use
`pwsh -NoProfile -File scripts/owner-recovery.ps1 <task>` with the corresponding
parameters. No deployment, source-branch merge or volume-delete target is added.

The superseded standalone `workflow-owner-recovery-*` lab's containers/images
and data volumes are retained for review. Its data is not silently moved into
the base observability database. The implementation's original recovery
[design](../docs/cadence-inspired-scaling-design.md) and
[validation](../docs/cadence-inspired-scaling-validation.md) remain historical
evidence with their recorded identities.

## Verified against the reused stack

On 2026-11-09, `obs-owner-recovery-test` passed the configured .NET 10 Release
suite (146/146, zero skipped) and both k6 smoke cohorts (40/40 each). A keyed
stress run passed 300/300 at 60 VUs; all eight keys spanned partitions, with no
FIFO inversions, overlapping attempts or duplicate durable results/completions.
The busy Hub B fault case completed 120/120 before restart and advanced both
affected owner epochs. This is a functional observation, not a recovery SLO.

Grafana's existing runtime/PostgreSQL dashboards and the added recovery dashboard
were provisioned through the same provider. Prometheus and PostgreSQL data-source
health checks passed, all three existing Prometheus scrape targets were up,
live k6/runtime series were present, and every recovery dashboard query was
accepted. `make obs-ps` inspected the same runtime project. A resolved-model
comparison confirmed unchanged base PostgreSQL, migration, Grafana, Prometheus,
Tempo, Loki and process-exporter definitions. A foreign-base container fixture
was rejected without mutation; historical archive reanalysis remained byte-identical.

The runtime contains 500 completed workflows from these runs; the earlier
standalone lab is stopped with its data retained. Logs, TRX, per-cohort k6 JSON,
durable exports and health/provenance receipts remain under `artifacts/`.
