# Plan: Reusable monitoring backend and legacy demo removal

Status: planned; implementation has not started.

Historical note: this plan's proposed structure predates the native combined Provisioner. Its `WorkflowCore.Processor.Host` entry below is removed tooling; current deployments use `WorkflowCore.Provisioner.Host`, while sequential v1 compatibility remains in `WorkflowCore.RemoteExecution`.

## Goal

Make the monitoring backend a reusable building block under `scaling/src/`, with a thin standalone host. Remove the old PostgreSQL onboarding/benchmark demo and its UI and tooling dependencies. Keep monitoring independent of executable business workflows.

Scope assumption: “legacy onboarding” means `WorkflowCore.Scaling.Samples` and the direct-PostgreSQL demo mode of `WorkflowCore.Scaling.Worker`. Retain the native combined Provisioner sample, its fluent workflows, and existing Hub v1 compatibility. A broader removal of all onboarding examples would require revising this scope.

## Requirements

1. A host can register and map monitoring without referencing any sample assembly, registering workflow definitions, or starting execution consumers.
2. Existing workflow lists, details, history, topology, definition versions/diffs, and Hub waterfalls continue to show real persisted data. Historical legacy runs remain readable after removing their executable sample code.
3. The default monitor performs no workflow execution or business commands. Generic management remains an explicit optional feature, disabled by default, with existing fencing and write controls preserved.
4. Remove legacy onboarding/benchmark creation and approval flows across backend, UI, development commands, and CI. Retain equivalent generic runtime verification through test-owned fixtures and native scenarios.
5. Keep contracts, solution references, deployment paths, configuration, tests, and documentation consistent in each delivered increment. Successful compilation alone does not complete the change.

## Evidence from the current code

- `scaling/monitor/api/Program.cs` calls `AddDistributedWorkflowCore`, enables an embedded worker by default, registers onboarding and benchmark workflows, and mixes monitoring with business start/approval endpoints.
- `WorkflowMonitorReader.cs` reads persisted workflow state, work ledger, and execution history; these queries can serve historical runs without executing workflow code.
- `DefinitionCatalog.cs` contains metadata capture, persistence, and comparisons. Startup currently reconciles locally registered definitions. Topology prefers archived metadata but falls back to the executable registry.
- `WorkflowCore.Scaling.Worker/Program.cs` has both direct-PostgreSQL demo execution and a REST/SSE activity-worker mode. Removing the project wholesale would also remove a caller used by Hub compatibility tooling.
- The Next.js app contains onboarding creation and approval controls. Its server routes also aggregate Hub data; those routes are not the legacy demo.
- `scripts/dev.ps1`, both runtime/observability workflows, and `scaling/observability/compose.yaml` reference the current API or legacy worker paths.

## Proposed structure

```text
scaling/src/
  WorkflowCore.Monitoring/           # reusable services, read models, endpoints
  WorkflowCore.Monitoring.Host/      # thin standalone executable
scaling/monitor/                    # Next.js UI and existing server-side adapters
scaling/sample/onboarding/
  WorkflowCore.Provisioner.Host/    # retained native sample
  WorkflowCore.Processor.Host/      # historical entry; removed v1-only sample host
  WorkflowCore.Hub.Host/            # retained generic Hub host
  WorkflowCore.Hub.LoadTest/        # retained native workload
```

Root upstream `src/` is unchanged. “src” here means the distributed-runtime source directory, `scaling/src/`, following repository guidance.

### Reusable backend

Proposed public entry points are `AddWorkflowMonitoring(options)` and `MapWorkflowMonitoring()`. Validate connection/schema and error-disclosure options at registration. Keep process startup, environment-variable binding, telemetry configuration, health endpoint composition, and explicit migration mode in the host. The library must not call `Run`, register a workflow host, or add worker services.

Move existing readers and metadata DTOs with their current wire shapes. Keep existing read route paths and pagination/error behavior, including `/api/monitor/*`, definition/version/diff routes, and instance/history reads. Do not add interface layers unless a concrete hosting or testing need requires them.

### Metadata ownership and historical data

Separate metadata capture/publication from monitoring reads. Runtime/business hosts may explicitly publish their definitions; the monitor reads immutable stored versions and never needs their executable assemblies. Remove the registry fallback from topology. When a snapshot is absent, return an explicit unavailable response and show that state in the UI.

Before removing old registrations, inventory historical definitions and archive metadata through an explicit one-time tool or deployment preparation using the old definitions. Do not reconstruct missing metadata from guessed steps. Preserve stored hashes, versions, history, schemas, and rows. No destructive migration or database reset is part of this work.

### Commands and boundaries

Remove `/api/demo/onboardings`, its approval endpoint, `/api/demo/versioned-onboardings`, and the benchmark-specific `POST /api/workflows`.

Inventory callers of generic `/api/events` and `/api/manage/*`. Preserve needed legacy command routes through explicit optional registration separate from monitoring reads, with workers disabled and writes off by default. Document any unused command route intentionally retired. Hub start/event/status operations remain Hub client operations. Do not move business workflow registration into the monitoring library to retain an endpoint.

Keep Next.js Hub/Prometheus adapters and server-only credentials in place for this change. Consolidating every frontend server route into .NET is outside this extraction. Monitoring infrastructure may read PostgreSQL; remote Provisioner/Processor hosts remain PostgreSQL-free.

### Legacy sample removal

Remove the `WorkflowCore.Scaling.Samples` project after its production callers are migrated. Remove direct-PostgreSQL demo execution from `WorkflowCore.Scaling.Worker` and retire that executable after auditing its REST/SSE mode. Repoint v1 smoke tools to the retained combined Provisioner where capabilities match; otherwise retain a small compatibility host with the required handlers. Stable activity identities, capability declarations, resource policy, and v1 result shapes must survive this move. Reusable v1 infrastructure already in `WorkflowCore.RemoteExecution` remains.

Remove UI demo controls, including the creation panel in `workflow-list.tsx` and approval behavior in `workflow-trace.tsx`. Preserve generic workflow details, opt-in status management, historical legacy filtering, and real failure states.

## Delivery order

Detailed acceptance criteria and dependencies are in [todo.md](todo.md).

1. Inventory endpoint/deployment consumers and establish generic monitoring fixtures.
2. Extract reader services and metadata contracts into the reusable library.
3. Add reusable endpoint mapping and the thin host; preserve current read contracts.
4. Remove executable-registry dependence and verify archived topology recovery.
5. Update the UI to remove business demo flows and show unavailable metadata.
6. Switch local startup, Compose, migration invocation, and solution references.
7. Move any remaining v1 worker responsibilities to a retained host.
8. Replace legacy-demo CI and benchmark coupling with generic/native verification.
9. Delete unused legacy sample projects and business demo scripts after their callers have migrated.
10. Align documentation and run the complete acceptance checks.

Checkpoints follow tasks 3, 6, and 10. The checklist retains stable task IDs: execute 9a/9b before deletion tasks 8a/8b. Each slice must build and pass its focused behavioral checks before the next dependent slice. Remove temporary compatibility wiring before completion.

## Verification and completion gate

- Host the library from a test-owned ASP.NET application with no sample reference; query real PostgreSQL fixtures for overview, list, detail, history, versions/diffs, and topology.
- Assert monitoring registration adds neither `DistributedWorkPump` nor legacy Workflow Core consumers. Starting only the monitor must leave pending work unclaimed and workflow revisions unchanged.
- Archive fixture metadata, restart without its executable workflow registration, and verify historical topology and definition comparison. Missing metadata must be visible and backend failures must remain errors.
- Verify removed demo routes/controls are absent and generic management is disabled by default. If enabled, verify expected-status fencing and current error semantics.
- Run `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release` and PostgreSQL tests with a real `WFC_TEST_PG`; unset-variable skips do not satisfy acceptance.
- From `scaling/monitor`, run `npm run check-types`, `npm run test:topology`, and `npm run build`. Exercise the workflow list, legacy/native details, definitions, and backend failure states in a real browser.
- Run `pwsh -NoProfile -File scripts/dev.ps1 help`, validate PowerShell syntax, and resolve Compose configuration. On an existing local database, verify startup, migration mode, and shutdown without deleting data.
- Run `scripts/test-hub-native.ps1 -ManageHosts` and the retained v1 gateway smoke with their prerequisites. Verify native Provisioner and Hub scheduling remain independent of the monitoring host.
- Search active source/configuration for deleted project paths, demo endpoint callers, and workflow registration in monitoring. Historical documentation may mention retired paths only when clearly identified as historical.

## Risks and limits

| Risk | Mitigation |
| --- | --- |
| Removing sample code hides old topology | Archive known metadata before removal; explicit unavailable state where metadata was never archived. |
| Deleting the worker breaks Hub v1 smoke/capabilities | Audit its remote branch and migrate callers before deleting the executable. |
| Existing performance jobs depend on benchmark business endpoints | Move their workloads to test-owned fixtures or retained native load tools; preserve what the measurements assert. |
| API moves break Docker and bootstrap migration paths | Change project arguments, migration invocation, readiness checks, and CI together. |
| Extraction silently starts work consumers | Verify service registration and actual pending-work behavior in an isolated test host. |

This plan does not change execution modes, lease/commit contracts, resource policies, scheduler ownership, authentication design, or durable storage layout. It does not remove historical runs or promise production readiness. No implementation or verification run is claimed by this planning document.
