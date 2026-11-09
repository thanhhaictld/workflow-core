# Ownership recovery Compose suite

This standalone development suite runs two Hubs, four matching shards, four
DB-free processors with separate 8-slot workflow/activity lanes, PostgreSQL,
nginx and an OTLP-to-Prometheus collector. It exercises the committed routing
fix described in the [design](../docs/cadence-inspired-scaling-design.md) and
[validation report](../docs/cadence-inspired-scaling-validation.md). It does not
include Terra's uncommitted resource-handoff changes, the monitor or Grafana.

Run from repository root with Docker Compose v2 or later and PowerShell 7.
Python 3 is required for smoke and fault tests; .NET 10 builds/tests run inside
Linux SDK containers. GNU Make is optional.

```powershell
make obs-owner-recovery-up
make obs-owner-recovery-test
make obs-owner-recovery-ps
make obs-owner-recovery-logs
make obs-owner-recovery-down
```

`up` builds current source, starts PostgreSQL/collector, applies ordered SQL
migrations with `ON_ERROR_STOP`, starts both Hubs, then starts four processors.
Build context excludes evidence, artifacts, local secrets and build outputs.
Repeated startup reapplies forward migrations without clearing persisted data.
The test target builds the same source, runs the full configured database suite
against **separate test PostgreSQL**, then runs 40 unordered and 40 eight-key
workflows against the two-Hub stack. All test failures propagate to Make.

| Target | Behavior |
| --- | --- |
| `obs-owner-recovery-help` | Commands and overrides |
| `obs-owner-recovery-config` | Print resolved Compose config, including development credentials |
| `obs-owner-recovery-build` | Build Hub, processor and .NET test images |
| `obs-owner-recovery-up` | Start the runtime lab; no load submission |
| `obs-owner-recovery-db-test` | Full configured PostgreSQL tests only |
| `obs-owner-recovery-smoke-test` | Start lab, execute both 40-workflow cohorts and audit durable results |
| `obs-owner-recovery-test` | Database tests plus both workflow cohorts |
| `obs-owner-recovery-fault-test` | Explicit disruptive owner-loss cases with continuous evidence |
| `obs-owner-recovery-ps` / `logs` | Inspect this project only |
| `obs-owner-recovery-down` | Remove suite containers/network; retain both database volumes and results |

Default ports bind to loopback only: Hub A `26200`, Hub B `26201`, LB `26210`,
metrics `26289`, PostgreSQL `26440`. Test PostgreSQL has no host port. Remote
processors receive REST/SSE credentials only, never a database connection string.
The collector exposes `http://127.0.0.1:26289/metrics`. These are fixed development
credentials, not production identity or authentication facilities.

## Isolation and configuration

The script derives `workflow-owner-recovery-<worktree-hash>` as its stable Compose
project, separate from the normal observability project and historical manual
lab. Images and both named data volumes belong to that project. Before acting,
the launcher checks existing project containers have its ownership-suite label;
the fault harness also resolves and validates exact project/service/replica
labels. It never adopts unrelated containers by a name prefix alone.

```powershell
make obs-owner-recovery-up OWNER_PORT_BASE=27200 OWNER_PROJECT=my-owner-lab
make obs-owner-recovery-test OWNER_PORT_BASE=27200 OWNER_PROJECT=my-owner-lab
make obs-owner-recovery-down OWNER_PORT_BASE=27200 OWNER_PROJECT=my-owner-lab
```

Always use the same overrides for one stack. Ports are base + 0/1/10/89/240.
Other options: `OWNER_RESULTS` (default `artifacts/owner-recovery`), `OWNER_PYTHON`
(default `python`) and `OWNER_TEST_FILTER` (optional dotnet test filter for a
targeted run). A filtered pass is not a full-suite pass. The launcher checks
TRX output and rejects a filter that executes zero tests. No generic `PG_*`,
observability `.env`, or `OBS_COMPOSE_FILE` settings are inherited into this suite.

Optional environment variables `OWNER_RECOVERY_TOKEN`, `OWNER_RECOVERY_PG_PASSWORD`,
`OWNER_RECOVERY_DIAGNOSTICS=true` and `OWNER_RECOVERY_NOTIFY=false` apply at
startup. Changing a retained database's password environment variable does not
change its persisted PostgreSQL password; keep the existing credential or change
it deliberately through PostgreSQL. Do not remove the volume to resolve it.
Restore the notification setting and rerun `up` after missed-hint experiments.
Collector settings live in `benchmark/owner-lab-collector.yaml`.

Without Make, the same launcher accepts direct tasks:

```powershell
pwsh -NoProfile -File scripts/owner-recovery.ps1 up
pwsh -NoProfile -File scripts/owner-recovery.ps1 test
pwsh -NoProfile -File scripts/owner-recovery.ps1 db-test -Filter 'FullyQualifiedName~HubSseOwnerRoutingTests'
pwsh -NoProfile -File scripts/owner-recovery.ps1 down
```

## Fault tests and evidence

Normal `test` does not kill Hubs. The explicit fault target stops processors
and both Hubs between independent cases to establish healthy preferred affinity,
then kills a live owner and keeps it stopped for the 85-second window. The
victim must own live shards. Idle cases submit 40 workflows after a 12-second
wait; busy cases submit 120 just before the kill. The victim is restarted only
after `end-before-restart`, including on collection failure. Faults target this
suite only and can interrupt work you have submitted to this suite.

```powershell
# Four cases: either owner, idle and busy. Around six minutes plus builds/startup.
make obs-owner-recovery-fault-test
# Repeat all four cases twice, matching the recovery validation matrix.
make obs-owner-recovery-fault-test OWNER_FAULT_REPEAT=2
# Shorter focused check:
make obs-owner-recovery-fault-test OWNER_FAULT_VICTIM=hub-2 OWNER_FAULT_MODE=busy
```

Each smoke/fault invocation uses a fresh UTC/GUID in its workflow run IDs and
idempotency keys, so repeated tests execute new work. It writes a directory under
`artifacts/owner-recovery`: source commit/status, resolved container identities,
raw DB ownership/task/wait samples, observer SSE frames, process logs, persisted
workflow/task/attempt/history exports and generated `analysis.json`. The audit
requires actual completed business snapshots and one durable result/completion;
keyed cohorts must have no FIFO inversions or overlapping attempts. Modified
fault cases must show new authoritative epochs and complete before restart.
This validates durable effects, not exactly-once external side effects.

Database runs write a fresh subdirectory of `artifacts/owner-recovery/tests`
with TRX results. Evidence and test outputs remain local ignored artifacts.
Historical committed evidence is never rewritten by these targets. Performance
results remain short shared-host measurements, not production capacity or an
eight-second recovery guarantee. The database and runtime tests can retain data
between invocations; no reset/volume-delete target is supplied.

## Verification on 2026-11-09

The Compose images built the .NET 10 Release solution successfully (seven
existing warnings, zero errors). The full `obs-owner-recovery-test` ran
146 configured tests with zero failures/skips, then completed 40 unordered and
40 keyed workflows. The focused routing target ran 7/7. A nonexistent filter
failed with a nonzero Make exit after the TRX recorded zero executed tests.

The default four-case fault target passed: each idle case completed 40/40 and
each busy case 120/120 before victim restart. Both affected shards advanced
epochs in every case; there were no extra activity attempts or duplicate durable
results/completions. These are local functional observations, not a recovery SLO.
The keyed smoke audit found eight keys spanning partitions, zero inversions and
zero overlapping attempt intervals.

`down` removed the suite containers/network while retaining both named volumes;
`up` reapplied migrations and returned ready with the same 400 completed workflow
rows. Configuration checks covered alternate project/ports, a results path with
spaces, separate test storage, four DB-free processors and loopback-only ports.
A live foreign-label fixture was rejected before mutation. Raw console logs,
TRX and persistence records remain under the ignored `artifacts/` directory.
Repeat-run validation then created 80 fresh smoke workflows and 120 fresh busy
fault workflows under unique run IDs; all completed and passed the durable audit,
bringing the retained runtime database to 600 completed workflows.
