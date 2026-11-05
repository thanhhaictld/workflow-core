# Workflow registration fix checklist

Design and verification results: [plan.md](plan.md). All tasks are complete.

## 1. Replace immutable registration with upserts

- [x] Keep implementation identifiers as informational metadata and remove their use in registration acceptance checks.
- [x] Upsert workflow metadata and activity registrations under their existing identity/version; remove metadata, contract, and build-ID equality rejection.
- [x] Preserve basic validation, transactional resource-policy handling, and operator capacities. Never reset existing workflow execution state.
- [x] Return registration success for inserts and updates without falsely labeling changed metadata as an idempotent replay.

Dependencies: none.
Likely files: `HubDefinitionStore.cs`, `HubRemoteContracts.cs`, and
`HubGatewayEndpoints.cs`.
Verification: Release build and focused registration tests from task 2.

## 2. Verify developer-owned versioning

- [x] Test changed same-version workflow metadata and activity contracts against pre-existing rows, including stored build-ID fields; registration succeeds and publishes the new metadata.
- [x] Test repeated and concurrent registrations, retaining authoritative capacity settings and leaving snapshots, tasks, timers, and execution history unchanged by publication.
- [x] Test a compatible same-version update resumes an existing waiting run and allows a new run to complete.
- [x] Test two code-defined versions remain separate: an existing run finishes on its registered version and a new run uses the requested new version.
- [x] Update obsolete immutable-registration assertions. Keep lease/revision fencing and resource-policy tests.

Dependencies: 1.
Likely files: focused registration tests, `HubRemoteSchedulerTests.cs`, and
native runtime/execution test fixtures under `scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests`.
Verification with `WFC_TEST_PG` set to a configured test database:

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release
```

Checkpoint: registration updates succeed, durable execution tests pass, and
workflow versions remain chosen by application code.

## 3. Verify Compose and update guidance

- [x] Rebuild and redeploy Hub and provisioners using `scaling/observability/compose.yaml` with the existing PostgreSQL volume.
- [x] Confirm all provisioners stay running, a waiting test run resumes with the same identity, and a newly submitted workflow completes.
- [x] Document that developers own backward compatibility and register a new code-defined version for breaking changes while earlier runs finish.
- [x] Remove documentation that requires exact build fingerprints or immutable registration. Keep fingerprints informational; drift UI in the other branch serves reference and audit without gating execution.

Dependencies: 2.
Likely files: `scaling/docs/hub-native-fluent-runtime.md`,
`scaling/docs/hub-remote-scheduler-implementation.md`, and relevant registration
contract guidance. Compose itself needs no configuration change for this fix.
Verification: native recovery smoke and live Compose status/log checks.
Record any unavailable checks and their prerequisites.
