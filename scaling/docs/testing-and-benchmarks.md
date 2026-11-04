# Testing evidence, benchmark plan and acceptance criteria

## Verified CI evidence

At source commit `b475e877141f0d8626c0a2ca5281302227928d4a`,
[GitHub Actions run 41207213239](https://github.com/thanhhaictld/workflow-core/actions/runs/41207213239)
completed successfully:

- .NET 10 build for the distributed runtime and development API sample.
- PostgreSQL 17 service container.
- SQL migrations `002`, `003`, `004` applied by psql.
- **8/8** xUnit integration tests passed.

Tests in `scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/DistributedRuntimeTests.cs`:
1. ConcurrentWorkersCannotClaimSameWork
2. ExpiredLeaseIsFencedFromNewGeneration
3. WorkflowGateRejectsCompetingAndStaleOwners
4. EngineCommitsWorkflowAndAcknowledgementTogether
5. EventPublishedBeforeWaitForIsDurablyReplayed
6. IdempotencyKeysPreventDuplicateWorkflowStartsAndSignals
7. DeadLetterIsVisibleAndRequiresExplicitOperatorReplay
8. HostedWorkerPoolProcessesNewWorkflowWithoutManualDispatch

Tests run under a single CI PostgreSQL instance; they are **not** a
long-running stress test, DR validation or proof of remote activity exactly-once.

## Reproduce CI locally

1. Install .NET 10 SDK and PostgreSQL 17.
2. Create a dedicated disposable test database.
3. Apply `scaling/sql/002_distributed_runtime.sql`,
   `003_signal_delivery.sql`, `004_request_dedupe.sql`.
4. Export `WFC_TEST_PG` connection string.
5. Run:

```bash
dotnet build scaling/monitor/api/WorkflowCore.Scaling.Api.csproj -c Release
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release
```

The smoke test runs existing Workflow Core EF migrations via
`PostgresContext.Database.MigrateAsync()` during test execution. Do not
point tests at an important database; tests create workflows and events.

## k6 API-ingestion benchmark

Start the sample from [benchmark README](../benchmarks/README.md), then:

```bash
k6 run scaling/benchmarks/k6-start-workflows.js \
  -e URL=http://localhost:5180 \
  -e RATE=100 \
  -e VUS=100 \
  -e MAX_VUS=800 \
  -e DURATION=5m \
  -e RUN_ID=trial-001
```

The script uses constant-arrival-rate, `POST /api/workflows`,
`Idempotency-Key`, and HTTP checks for status 202. Defaults: 100 requests/s,
5 minutes, 100 preallocated VUs, 800 max VUs, `http_req_failed < 1%`
and HTTP P99 under 1000 ms.

**Interpretation:** 202 only proves accepted workflow intent. It does not
measure the background execution rate. Count state-complete workflows and
successful work-item ACKs separately, with backlog age, transaction latency,
WAL volume, PostgreSQL CPU, lock waits and worker CPU.

## Required additional validation (not yet performed)

| Group | Scenario | Acceptance |
|---|---|---|
| Scale | 1, 2, 5, 10, 25+ replicas on fixed DB resources | Throughput scaling curve and knee documented |
| Stress | 100k+ queued, 1M+ completed history rows | Bounded query time, backlog drains without OOM |
| Long steps | Execution longer than lease duration | Heartbeat renews; stale worker cannot commit |
| Crash injection | Kill process before/after external action, transaction, ACK | No missing committed transitions |
| Event races | Two simultaneous events and late subscriber | No lost subscription wakeup; ordering policy verified |
| Partial fan-out | Crash mid-delivery to 1000 subscribers | Marker-based reprocessing correct |
| Retry | Timeouts, repeated transients, poison input, DLQ | Bound retry count and external effect reconciliation |
| HA | PostgreSQL failover, network interruption, partition | Known RPO/RTO and no conflicting active writer |
| Security | Untrusted payload/permissions/secret handling | Cannot execute unintended code or cross tenants |
| Shutdown | SIGTERM while active steps | Controlled cancellation + expected reclaim |
| Retention | Database grows to projected lifecycle volume | Cleanup preserves replay and idempotency horizon |

## Suggested performance report format

Record commit SHA, deployment mode, PostgreSQL specs/config, number of replicas,
worker concurrency, workflow definition and payload sizes, external API latency,
test duration, offered starts/s, acknowledged completions/s, P50/P95/P99
schedule delay, errors, dead letters and resource graphs. Compare **same
hardware and workload** against upstream; do not extrapolate claims from a
single local run.

## Provisional release gate

Do not claim production ready until the full race/failure matrix above passes,
benchmarks satisfy an agreed throughput and tail-latency SLO, the database
retention plan is in place, security review is completed, and rollback/
failover drills are documented.
