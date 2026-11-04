# Monitoring building block task list

Plan: [plan.md](plan.md). All items are implementation work; none is completed by writing this checklist.

## 1. Establish generic monitoring fixtures

- [ ] Inventory all read/command routes and their UI, script, CI, and benchmark callers; record intentionally removed routes.
- [ ] Add test-owned workflow/metadata fixtures independent of `WorkflowCore.Scaling.Samples`, covering current response shapes and persisted history.
- [ ] Identify historical metadata preparation and v1 worker capabilities needed before deletion.

Verification: run focused fixture tests against a real `WFC_TEST_PG`; record current endpoint contracts.

Dependencies: none. Likely files: monitoring test project, fixture/tests, endpoint inventory in the plan. Scope: medium, 3–5 files.

## 2. Extract monitoring services

- [ ] Create `scaling/src/WorkflowCore.Monitoring` and move reader/metadata contracts without changing JSON shapes.
- [ ] Add validated monitoring options and service registration without workflow definitions or worker services.
- [ ] Reference the library from the current API and solution during the extraction.

Verification: build the solution; run read-model/metadata tests; inspect registered hosted services.

Dependencies: 1. Likely files: new project, reader, catalog, existing API project, solution. Scope: medium, approximately 5 files; split metadata contracts if needed.

## 3. Host reusable endpoints

- [ ] Expose endpoint mapping for existing reads; create `WorkflowCore.Monitoring.Host` as a thin composition root under `scaling/src`.
- [ ] Separate optional generic management/event commands from default monitoring, preserving write controls and fence behavior where retained.
- [ ] Keep telemetry, health composition, environment configuration, and explicit migration mode in the executable.

Verification: host the library in a test-owned web app without sample registrations; test HTTP contracts and default command behavior; verify pending work remains unclaimed.

Dependencies: 2. Likely files: endpoint extensions, host project/Program, endpoint tests, old Program adaptation. Scope: medium, approximately 5 files.

## Checkpoint A: Independent backend

- [ ] New host reads real data with no business sample dependency.
- [ ] Existing read contracts pass and starting monitoring does not execute workflows.
- [ ] Both the extraction and the standalone host build.

## 4. Make metadata independent of executable workflows

- [ ] Separate explicit capture/publication from catalog reads; remove monitoring startup reconciliation and registry fallback.
- [ ] Provide documented one-time archive preparation for old definitions before deleting their code.
- [ ] Return explicit metadata-unavailable results when no archived version exists.

Verification: archive metadata, restart without executable definitions, and verify topology/diffs; test missing metadata and database failures.

Dependencies: 3. Likely files: catalog, topology endpoints, archive preparation tool, metadata tests. Scope: medium, 4–5 files.

## 5. Remove legacy business UI flows

- [ ] Remove `demo-onboarding.tsx` and its workflow-list integration.
- [ ] Remove sample-specific approval behavior from workflow trace; retain generic status controls.
- [ ] Render missing metadata and backend errors honestly; preserve legacy/native list and detail navigation.

Verification: frontend typecheck, topology tests, production build, and browser checks of list/detail/definitions with real services.

Dependencies: 4. Likely files: demo panel, workflow-list, workflow-trace, relevant metadata view/tests. Scope: medium, up to 5 files.

## 6. Switch development and deployment entry points

- [ ] Repoint solution/Compose API project paths and explicit migration invocation to the new host.
- [ ] Make local monitor startup independent of the direct-PostgreSQL sample worker; retain explicit native Hub/Provisioner commands.
- [ ] Retire legacy seed/approve command behavior and update Make wrappers/help consistently.

Verification: PowerShell syntax/help, Compose config, solution build, and local startup/migration/shutdown against preserved data.

Dependencies: 3–5. Likely files: solution, `scripts/dev.ps1`, Makefile, observability Compose, host configuration. Scope: medium, approximately 5 files.

## Checkpoint B: Deployment uses the new host

- [ ] UI reads the new backend, including historical runs and missing-metadata states.
- [ ] Monitor starts without execution workers; native workflows still execute through the combined Provisioner.
- [ ] Existing database contents survive migration/startup checks.

## 7. Migrate remaining v1 worker callers

- [ ] Compare legacy worker REST/SSE handler capabilities and result shapes with the retained Provisioner.
- [ ] Repoint gateway smoke/dev commands to an equivalent retained host, preserving activity identities and policies.
- [ ] Keep a minimal compatibility host only if required capabilities cannot be supplied by the existing host.

Verification: v1 gateway smoke and native/v1 coexistence checks; no PostgreSQL credentials in remote application hosts.

Dependencies: 6. Likely files: retained host handlers/registration, gateway smoke, dev commands. Scope: medium, 3–5 files.

## 8a. Retire old API and business scripts

- [ ] Remove `scaling/monitor/api` after all hosting/build/migration callers move.
- [ ] Remove legacy onboarding start/approval and benchmark start endpoints from the shipped backend.
- [ ] Remove `scripts/test-onboarding.ps1`; replace metadata/status checks in `test-definition-management.ps1` with generic fixtures before retiring its business setup.

Verification: endpoint tests assert retired routes are absent; search confirms no active callers of deleted API paths.

Dependencies: 4, 6, 7, 9a, 9b. Likely files: three old API source files/project and business scripts. Scope: split deletion into two small commits if over 5 files.

## 8b. Delete unused legacy sample projects

- [ ] Remove `WorkflowCore.Scaling.Samples` once no shipped host references it.
- [ ] Remove `WorkflowCore.Scaling.Worker` once its remote compatibility callers have migrated.
- [ ] Update solution membership and verify remaining project references resolve.

Verification: Release solution build and reference search; preserve test-owned fixtures, core runtime libraries, and stored data.

Dependencies: 7, 8a. Likely files: sample project/source deletions and solution. Scope: separate the two project deletions into verifiable commits.

## 9a. Replace legacy bootstrap CI

- [ ] Update runtime workflow path filters, build/start commands, and smoke setup for the new backend.
- [ ] Replace deleted demo smoke with generic monitor HTTP/metadata checks and native combined-host coverage.
- [ ] Ensure PostgreSQL-dependent jobs actually set `WFC_TEST_PG` and preserve useful failure logs.

Verification: workflow syntax, local equivalent commands, and CI runtime/bootstrap jobs.

Dependencies: 6, 7. Execute before deleting old projects in 8a/8b. Likely files: distributed-runtime workflow, generic monitor smoke script/test harness. Scope: small, 2–3 files.

## 9b. Align observability and load jobs

- [ ] Update observability workflow project paths and service checks.
- [ ] Replace legacy benchmark endpoint coupling with a test-owned or retained native workload, keeping assertions meaningful.
- [ ] Preserve Hub v1/native trace, timer, retry, and resource-policy checks.

Verification: Compose resolution, observability smoke, and changed load workload correctness checks; justify any changed performance baseline.

Dependencies: 9a. Likely files: observability workflow and affected load/script files. Scope: medium, 3–5 files; split if more workloads are found.

## 10. Document and verify the final layout

- [ ] Update root guidance, scaling/monitor READMEs, architecture/runbook references, and deployment commands in small document groups.
- [ ] Explain embedding, default read-only behavior, optional management, metadata publication/archive preparation, and removed demo endpoints.
- [ ] Run all acceptance checks in `plan.md`; record results and explicit prerequisites for any blocked check.

Verification: Release build; real PostgreSQL suite; frontend typecheck/topology/build/browser checks; generic monitor smoke; native crash/v1 compatibility smoke; stale-reference search; final diff review.

Dependencies: 8a, 8b, 9a, 9b. Likely files: AGENTS, READMEs, affected architecture/operations docs. Scope: split documentation updates into groups of at most 5 files.

## Checkpoint C: Complete

- [ ] Monitoring is embeddable from `scaling/src` and has no sample or consumer dependency.
- [ ] Legacy business demo is removed across backend, UI, tooling, and CI.
- [ ] Historical data/topology remain readable where metadata exists; unavailable data stays visible.
- [ ] Native workflows and required v1 compatibility pass with the retained hosts.
- [ ] Build, behavior, deployment, and documentation are consistent; verification evidence is recorded.
