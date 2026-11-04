# Hub Server / Management refactor checklist

Plan: [plan.md](plan.md). This updates the prior unfinished monitoring extraction
checklist for the proposed Hub.Management library and Hub.Server executable.
Items remain pending unless checked. Partial progress: the generic host has moved to
`scaling/src/WorkflowCore.Hub.Server`, its direct launch paths were updated, and
the monitor reader and catalog now build in a Management library used by the
old API. Endpoint extraction, Server composition, historical metadata preparation,
and the remaining deployment migration are not complete. The Release solution
build, Compose config, dev help, and recovery-script syntax passed; runtime
recovery and database-dependent checks have not yet been run. File groups are estimates; split any task
that grows beyond a focused, independently verifiable change.

## 1. Freeze routes and deployment consumers

- [ ] Inventory retained read routes, Hub protocol routes, generic commands, and business endpoints with every UI/script/CI caller.
- [ ] Record project/DLL paths, service names, migration jobs, ports, origins, credentials, pool settings and health responses affected by the two moves.
- [ ] Add test-owned read/metadata fixtures and baseline contract checks independent of business samples.

Acceptance: an explicit retain/migrate/retire decision exists for each route and
caller; fixtures exercise real persisted records and missing-data responses.
Verification: focused HTTP/read-model tests with configured PostgreSQL; reference search.
Dependencies: none. Likely files: route inventory, fixture/tests, test project. Scope: 3–5 files.

## 2a. Separate persisted metadata reads

- [ ] Separate catalog reads/comparison from executable metadata capture and startup reconciliation.
- [ ] Make topology read archived metadata without requiring an executable registry.
- [ ] Preserve metadata hashes and wire shapes; return explicit unavailable results for absent snapshots.

Acceptance: historical topology/diffs work without loading sample workflows;
missing metadata is distinguishable from a database failure.
Verification: persisted metadata, missing snapshot and failure tests.
Dependencies: 1. Likely files: catalog, topology endpoint, metadata tests. Scope: 3–4 files.

## 2b. Prepare historical metadata

- [ ] Inventory stored legacy definitions lacking snapshots.
- [ ] Provide an explicit archive preparation path using matching old definition binaries; avoid guessing missing metadata.
- [ ] Document which unfinished old runs still require old execution artifacts.

Acceptance: removing sample registration cannot silently remove available historic
metadata; irrecoverably unavailable metadata remains visible.
Verification: archive a fixture, restart without its definition, read topology/diffs;
confirm archive preparation does not start execution consumers.
Dependencies: 2a. Likely files: archive tool/entry point, tests, runbook. Scope: 3–4 files.

## 3a. Extract Management services

- [ ] Create `scaling/src/WorkflowCore.Hub.Management` as a library and move readers/DTOs/catalog reads with existing serialization shapes.
- [ ] Add validated `AddHubManagement(options)` registration with no business references, executable registry or worker services.
- [ ] Reference the library from the current API and solution so extraction can be verified before deployment changes.

Acceptance: library builds independently, resolves its readers, and adds no
`DistributedWorkPump` or upstream background consumers.
Verification: Release build, DI checks and focused read-model tests.
Dependencies: 2a. Likely files: project, readers, catalog, solution/API references. Scope: about 5 files; split if needed.

## 3b. Extract reusable endpoint mapping

- [ ] Add `MapHubManagement()` for retained read endpoints, keeping HTTP and JSON behavior.
- [ ] Separate any retained legacy command services/routes from default reads; writes remain opt-in and no worker starts.
- [ ] Use the module from the existing API while keeping demo registration outside it temporarily.

Acceptance: a test-owned web host with no sample assembly serves retained reads;
starting it leaves queued work and workflow revisions untouched.
Verification: HTTP contract tests, default command checks, actual pending-work test.
Dependencies: 3a. Likely files: mapping extensions, optional command mapping, old Program adaptation, hosting tests. Scope: 4–5 files.

## Checkpoint A: Reusable Management

- [ ] Management has no dependency on business samples or startup registry capture.
- [ ] Retained reads and unavailable/error behavior match the agreed contract.
- [ ] The current API still builds using the module while default Management never executes work.

## 4a. Move the generic Hub executable

- [ ] Move/rename `scaling/sample/onboarding/WorkflowCore.Hub.Host` to `scaling/src/WorkflowCore.Hub.Server` and update namespace/assembly identity where relevant.
- [ ] Update solution membership, project references, and direct release/run commands.
- [ ] Keep Gateway, scheduler configuration, health payloads and telemetry service identity stable during the host move.

Acceptance: the renamed generic Server starts and serves existing Hub routes with
no sample, Provisioner or remote-executor assembly reference.
Verification: solution build, readiness and authenticated Gateway smoke.
Dependencies: 1. Likely files: new host project/Program, solution and direct callers. Scope: split path groups into small related changes.

## 4b. Update process recovery paths

- [ ] Replace the sample-directory assumption in `scripts/test-hub-native.ps1` with explicit Server and Provisioner project/binary paths.
- [ ] Update Hub process identification and migration prerequisites to the new name.
- [ ] Preserve forced-death timing, existing worker capabilities and durable recovery assertions.

Acceptance: the recovery harness launches the new Server and unchanged Provisioner
and recovers a timer after both processes are force-killed.
Verification: Release build plus `scripts/test-hub-native.ps1 -ManageHosts` with real PostgreSQL and a development token.
Dependencies: 4a. Likely files: recovery script and directly associated CI invocation. Scope: 1–3 files.

## 5a. Compose Management into Server

- [ ] Reference/register/map Management from Server with an explicit enable/disable option.
- [ ] Resolve route overlap, existing authorization behavior and DB pool budget; retain Gateway transaction ownership.
- [ ] Verify retained legacy commands remain explicit and native commands use existing Hub stores.

Acceptance: one origin serves both API families; management registration never
starts a work consumer; management toggling does not disable Gateway or scheduling.
Verification: HTTP parity, route/auth checks, concurrent reads/activity/timer smoke,
Scheduler-enabled/disabled and Management-enabled/disabled startup tests.
Dependencies: 3b, 4b. Likely files: Server project/Program/config, composition tests. Scope: 3–5 files.

## 5b. Preserve explicit schema preparation

- [ ] Add an explicit Server migration-only invocation for required legacy EF migrations, preserving schema and migration history.
- [ ] Keep ordered Hub SQL through 013 in the existing one-shot job; no automatic migration loop on each Server replica.
- [ ] Ensure migration-only mode exits before listeners, scheduler/worker services, or business registration start.

Acceptance: existing-volume schema preparation is repeatable and non-destructive;
Server can read preserved historical rows afterward.
Verification: repeated migration invocation on a populated test database and process/hosted-service checks.
Dependencies: 5a. Likely files: Server migration entry point, focused tests, migration invocation configuration. Scope: 3–4 files.

## Checkpoint B: One generic backend

- [ ] Server hosts Gateway, Management and the configurable embedded scheduler.
- [ ] Native execution, archived legacy reads and migration-only mode pass.
- [ ] No business definition assembly is loaded to serve management requests.

## 6. Remove business coupling from the UI

- [ ] Remove legacy onboarding/benchmark creation and approval controls with their API calls.
- [ ] Preserve unified native/legacy list/detail/trace, definitions, and applicable generic status controls.
- [ ] Show missing topology, uncertain attempts, and backend failures explicitly.

Acceptance: monitor navigation needs no business demo endpoint and still presents
real records and errors from both persistence models.
Verification from `scaling/monitor`: typecheck, topology tests, production build,
and browser checks with real services and missing/error fixtures.
Dependencies: 2b, 5a. Likely files: demo panel, workflow list/trace, metadata view/tests. Scope: split UI groups if over 5 files.

## 7a. Switch observability Compose

- [ ] Use Hub.Server project/DLL for service `hub`; point both Next.js backend origins at it while preserving browser `/backend` URLs and server-only credentials.
- [ ] Switch the EF-only job to the verified Server migration invocation; remove the old monitor API service/build dependency.
- [ ] Keep named volumes, Hub address/port, provisioner settings and ordered SQL migration behavior intact.

Acceptance: existing data stays readable; four native Provisioners work with one
generic Server; monitor and native timer runs work after restart.
Verification: Compose config and existing-volume deployment smoke, UI read parity,
native workload correctness and recovery. Do not reset databases.
Dependencies: 5b, 6. Likely files: Compose, environment example, frontend upstream/proxy configuration. Scope: 3–5 files.

## 7b. Switch local development entry points

- [ ] Repoint `scripts/dev.ps1`, Make shortcuts/help and local origin defaults to Hub.Server.
- [ ] Remove default legacy demo worker startup and retired seed/approve behavior; retain application Provisioner commands.
- [ ] Verify Next.js build/runtime rewrite configuration reaches the configured Server.

Acceptance: documented local startup needs no changes to infrastructure code and
launches no legacy execution worker just to view workflows.
Verification: PowerShell syntax/help, local startup/readiness/shutdown, monitor reads and native start.
Dependencies: 7a. Likely files: dev script, Makefile, frontend upstream/proxy defaults. Scope: 3–5 files.

## 8a. Migrate runtime/bootstrap CI and smokes

- [ ] Update workflow filters/build paths, host/DLL names and generic monitoring readiness checks.
- [ ] Replace demo-based read/metadata/status setup with test-owned fixtures; audit old remote-worker capability callers before deleting them.
- [ ] Preserve real PostgreSQL setup, native recovery, and useful failure logs.

Acceptance: CI proves generic infrastructure independently of sample definitions;
retained compatibility tests use hosts/fixtures that actually advertise required capabilities.
Verification: workflow syntax, focused scripts and equivalent CI runtime/bootstrap commands.
Dependencies: 7b. Likely files: distributed-runtime workflow and generic smoke/fixture harnesses. Scope: 3–5 files.

## 8b. Migrate observability/load CI

- [ ] Update observability workflow paths, service readiness assertions and runtime topology checks.
- [ ] Remove benchmark/demo endpoint coupling using retained native tools or test fixtures.
- [ ] Preserve meaningful native trace/timer/retry/resource-policy assertions; state any changed performance workload explicitly.

Acceptance: observability smoke exercises the new Server topology; deleted business
routes and old binaries are not hidden prerequisites.
Verification: observability Compose smoke, workload correctness, metrics/history checks.
Dependencies: 8a. Likely files: observability workflow and affected load scripts. Scope: 2–5 files.

## Checkpoint C: All supported entry points migrated

- [ ] Compose, local scripts, recovery tools and CI use Server/Management.
- [ ] No active deployment requires the API under the frontend tree or the old Hub host path.
- [ ] Existing data survives the switch and native workloads still complete.

## 9a. Remove obsolete host/API files

- [ ] Delete `scaling/monitor/api` and remaining old Hub.Host files after caller migration.
- [ ] Remove temporary host forwarding/duplicate registration and retired demo endpoint definitions.
- [ ] Recheck source/project/solution/script/DLL references; classify any historical survivors.

Acceptance: exactly one default Hub infrastructure executable remains under `scaling/src`.
Verification: Release solution build, route inventory and stale-reference search.
Dependencies: 8b. Likely files: obsolete API/host sources and solution remnants. Scope: split directory deletion groups.

## 9b. Retire unused legacy business samples

- [ ] Delete Scaling.Samples and Scaling.Worker only after all required code/REST capabilities and callers have been audited/migrated.
- [ ] Retire business scripts or replace their setup with generic fixtures before deletion.
- [ ] Keep upstream/scaling runtime libraries, test-owned compatibility fixtures and historical persisted records.

Acceptance: Server/Management has zero sample references and no deleted tool remains
an active CI or developer prerequisite. Native v3 remains the default application path.
Verification: solution/project-reference search, relevant compatibility tests and generic/native smoke.
Dependencies: 2b, 8b, 9a. Likely files: sample project/source deletions, retired scripts, solution. Scope: one project/script group per slice.

## 10. Update guidance and record final evidence

- [ ] Update AGENTS, scaling/monitor READMEs, runtime/deployment docs and startup examples to the final project names and responsibilities.
- [ ] Explain standard Server configuration, application ownership, metadata archival, explicit migration mode and old pinned-run limitations.
- [ ] Run completion checks in plan.md and report failures, unavailable prerequisites and their actual scope.

Acceptance: a new application developer can deploy configured infrastructure and
work only on workflows/activities without editing Server or Management source.
Verification: final dependency review, Release build, real PostgreSQL suite,
frontend checks/browser flows, Compose/migration/recovery smokes and diff review.
Dependencies: 9a, 9b. Scope: documentation batches of at most 5 files plus recorded verification.

## Final checkpoint

- [ ] Hub.Management is reusable and has no business or default execution registration.
- [ ] Hub.Server is the standard generic backend for Gateway, Management and embedded scheduling.
- [ ] The Next.js monitor depends on stable HTTP contracts rather than its own .NET sample host.
- [ ] Application Provisioner/workflow artifacts and durable state are preserved.
- [ ] Legacy business coupling and obsolete active project paths are removed, with verification recorded.
