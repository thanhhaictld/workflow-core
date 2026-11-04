# Autonomous implementation handoff to GPT-6.1 Terra (high)

## Authority and workspace

The user explicitly requested implementation of the refactor plan in a new
worktree and said it does not need babysitting. You own implementation,
verification, routine design decisions, and the final report directly to the user.
Do not stop after replanning or ask for permission to perform reversible in-scope
work. Statements in older planning prose about implementation not having started
are historical, not an approval gate.

Worktree: `F:/Project/orca-workspaces/workflow-core/refactor-hub-server-management`
Branch: `thanhhaictld/refactor-hub-server-management`
Starting code: `e9d3d69f424b4fa77b421c402676acc69d077623`.
The new branch was created through Orca from the repository default and then
fast-forwarded to this required baseline, because the default branch has no
`scaling/` tree. The baseline includes native v3, policy revision/coordination
fixes, and removal of the obsolete standalone sequential Processor host.
The updated plan and checklist were copied into this worktree as uncommitted
files intentionally. Preserve them and update their progress as work is verified.

Read in order: `AGENTS.md`, `tasks/plan.md`, `tasks/todo.md`, relevant architecture
and runbooks named by AGENTS. Inspect current source; documentation may contain
future plans and stale paths. No coordinator or monitoring loop will manage you.

## Decisions already made

- `scaling/src/WorkflowCore.Hub.Management` is a reusable ASP.NET management/read
  module library; do not create another default Management executable.
- `scaling/src/WorkflowCore.Hub.Server` replaces the generic sample Hub host and
  hosts Gateway + Management + embedded Scheduling. It has no business sample
  reference or executable definition registration.
- Keep `scaling/monitor` as the Next.js UI. The normal deployment uses one backend
  origin; preserve existing useful HTTP paths and both native/legacy read models.
- Application developers own the native Provisioner and its workflow assembly.
  Keep their REST/SSE boundary and separate implementation fingerprints intact.
- Remove legacy business demo coupling using the checklist's caller-first order.
  Keep reusable runtimes and historical data; do not reintroduce the deleted old
  Processor host or falsely advertise v1/v2 execution in the native v3 sample.
- Generic management reads do not start workers. Retained legacy command support
  is explicit and disabled by default; native commands use the existing Hub APIs.
- Use persisted metadata for historical topology; missing metadata remains
  explicit. Archive known old metadata with matching definitions before deleting
  its sources. Do not invent schemas or timestamps.
- Preserve named volumes, schemas, migrations, lease epochs/revisions, policy
  ownership, native protocol and existing workflow code/version identities.

## Implementation route

Execute `tasks/todo.md` in dependency order. Each task includes acceptance,
verification and estimated files. Split large slices further if necessary; do
not turn every checkpoint into a human permission request. Use judgment for
routine mechanics; record material changes to the plan with reasons.

Do not merely rename folders: the old monitor Program enables an execution pump,
registers business samples, captures definition metadata from executable code and
mixes business starts/approvals with reads. Those responsibilities must be
separated to achieve the infrastructure boundary. Migration-only mode must exit
before starting application listeners or hosted services.

The old native recovery script assumes every binary is under sample/onboarding.
Replace that assumption with explicit project paths for Server and Provisioner.
Cohosting must keep `/api/hub` and legacy `/api/monitor`, `/api/definitions`, etc.
distinct; preserve source identity in the unified UI rather than treating old
PostgreSQL workflows as native Hub workflows.

## Verification and environment

Run the Release scaling build, focused tests as slices land, full PostgreSQL suite
with configured migrations through 013, frontend typecheck/topology/build and
real browser behavior where UI changed. Validate PowerShell syntax/help, Compose
configuration, migration-only mode, existing-data reads, native regular/timer
execution, forced process recovery and stale reference searches.

The original workspace has a live observability stack. Do not stop/recreate it
or use its database for destructive fixtures. Use isolated test containers and
unique Compose project names and host ports if starting another stack. Never
use `down -v`, truncate/reset user data, or delete another workspace's volumes.
A new worktree's changed build path may change deterministic MVID fingerprints;
use isolated registration fixtures and one published artifact per pinned version.
Do not rewrite stored implementation fingerprints to make a test pass.

Prior evidence on the baseline: 64 PostgreSQL tests passed when runner and DB
shared the Docker clock; all 53 Hub/native tests passed from Windows. Five older
DistributedRuntime immediate-claim tests failed on Windows/Docker clock skew.
Use a synchronized container runner for full verification if needed; report
actual failures and prerequisites, never weaken assertions. Native regular/timer,
forced Hub/Provisioner restart, coordinated capacity update/restart, workflow
assembly isolation and Compose 20-workflow functional smoke passed previously.
Those are baseline facts, not proof of your final refactor.

Report what changed, the final project/deployment shape, tests with actual
results, any limitations and remaining work. Leave a reviewable implementation
on this branch. Do not push, merge, or publish externally without user request.
