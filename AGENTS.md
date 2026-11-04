# Project guidance

This repository is a fork of Workflow Core. The fork develops a distributed
workflow runtime under `scaling/` and a React-based monitoring application under
`scaling/monitor/`. Preserve upstream behavior unless the task explicitly requires a
change to it.

## Where to look

- `src/` and `test/` contain the original Workflow Core engine, providers,
  extensions, samples, and tests. `WorkflowCore.sln` is the upstream solution.
- `scaling/README.md` is the entry point for the distributed runtime design.
  Use `scaling/docs/README.md` to find detailed architecture and operations
  documents. Check the relevant source and tests for current behavior; the
  documentation includes both implemented behavior and future plans.
- `scaling/WorkflowCore.Scaling.slnx` contains the new .NET projects. Add new
  .NET projects under `scaling/` and reference upstream assemblies as needed.
- `scaling/src/` contains the PostgreSQL runtime, Hub, gateway, scheduler,
  client, telemetry, and remote execution libraries. `scaling/sql/` contains
  ordered SQL migrations. `scaling/tests/` contains PostgreSQL integration
  tests. `scaling/sample/onboarding/` contains hosts and sample workflows.
- `scaling/monitor/` contains the Next.js, React, TypeScript, and Tailwind
  monitoring app and its .NET API host in `api/`. The host also supports the
  onboarding demo and references its sample workflows. Read
  `scaling/monitor/README.md` for local setup and limitations.
- `scripts/dev.ps1` manages the local development stack. The root `Makefile`
  provides optional shortcuts.

For Hub, remote processor, provisioner, or scheduler work, also read
`scaling/docs/hub-remote-scheduler-implementation.md` and the relevant sections
of `scaling/docs/hub-embedded-scheduler-final-plan.md`. The implementation is
experimental; do not describe planned capabilities as implemented.

## Runtime constraints

- The PostgreSQL runtime reuses Workflow Core's executor but has a separate
  durable work ledger. Do not run legacy background consumers alongside
  `DistributedWorkPump` or use legacy host start and event APIs for that runtime.
- Preserve transactional state changes, lease fencing, retry behavior, and
  durable history when changing execution or scheduling. External side effects
  are at least once and require idempotency or reconciliation.
- Keep the Hub gateway generic. The embedded scheduler is Hub infrastructure
  with database access; remote processors and provisioners use REST/SSE and do
  not access PostgreSQL directly.
- The remote execution library currently supports a sequential Activity/Delay
  slice, not arbitrary legacy workflows. Verify support in code before relying
  on branches, signals, subworkflows, or other workflow constructs.
- Treat the sample API, development bearer token, and observability stack as
  development facilities. Do not assume production authentication, tenant
  isolation, or production readiness.
- Preserve existing data when changing migrations. Review the migration order
  and the matching tests and deployment scripts; do not reset a database as a
  routine verification step.
- The monitor uses real backend data. Keep missing data, uncertain attempts,
  and backend failures visible rather than substituting invented results.

## Build and verify

Run commands from the repository root unless noted otherwise. Use .NET SDK 10
for the scaling projects, Node.js 22 or later for the monitor, and PowerShell 7
for the development script.

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release
pwsh -NoProfile -File ./scripts/dev.ps1 help
pwsh -NoProfile -File ./scripts/dev.ps1 up
```

The PostgreSQL integration tests require `WFC_TEST_PG` and a configured schema.
Some tests return without exercising the database when that variable is unset.
For the local Docker-backed setup, see `scaling/monitor/README.md` and `scripts/dev.ps1`;
`pwsh ./scripts/dev.ps1 test` provisions the local schema and sets the variable.
The `up` command starts local services and may install dependencies.

For monitor changes, run the relevant commands from `scaling/monitor/`:

```powershell
npm run check-types
npm run test:topology
npm run build
```

Choose tests that cover the behavior changed, including integration tests for
database and concurrency changes. Report any checks that could not run and
their prerequisites. Follow existing patterns in the affected code and keep
documentation aligned with changes to contracts or operational behavior.
