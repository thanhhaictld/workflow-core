# Plan: remove Hub-managed business resource policies

Status: partially implemented; retirement is not complete.
Date: 2026-11-18.

Partial implementation: the activity registration store rejects introduction or
restoration of business-resource registrations; existing registrations are frozen
against policy, routing, and contract drift. Identical republication and existing
capacity updates remain available. Migration 033 and dispatch guards have been
implemented but have not completed cutover and failure verification. Do not
treat retirement as complete until the configured compatibility checks and
controlled rollout pass. The sample now publishes new supported versions; old
executions require their original compatible worker binaries during drain.
The retirement is not complete and existing business permit enforcement remains
necessary. See the benchmark report for verification and deployment limitations.

Test-scope override (18 November): reuse established workloads and collectors.
Do not add benchmark tests, workloads, scenarios, targets, or a custom harness;
in particular, the proposed partition-independence benchmark lane below is not
authorized. Existing correctness checks and affected scripts may be adapted while
preserving durable audits and without asserting a retired keyed guarantee. The
performance qualification matrix below is aspirational where it calls for new
workloads; report the coverage actually available from existing workloads.

Scoped compatibility design: definitions do not describe every activity that
arbitrary workflow code might later select. Existing workflow executions at the
database rollout boundary retain compatibility eligibility; subsequent executions
do not. Supported starts on existing versions remain available. Known retired
contracts can be rejected at start, but unknown future dependencies cannot be
preflighted: a new workflow selecting a retired activity is rejected transactionally
at dispatch. Idempotent start replay must return the original execution and its
eligibility. Operators must quiesce old Hub writers for the migration and deploy
only enforcing writers before resuming; old binaries do not enforce the boundary.
For an authenticated, fenced workflow-task commit that tries a retired policy,
the Hub rejects all proposed effects and atomically records a terminal policy
reason: native uses its existing `Terminated` status and remote uses `Failed`.
The Hub retires previously persisted work as part of terminal cleanup. Replaying
the same accepted rejection returns the original outcome without new history;
stale or different commits cannot acquire that acknowledgement. These semantics
remain subject to the configured integration checks and rollout verification.

## Decision and scope

Confirmed scope: remove **all Hub admission policies based on business resource
keys**, including FIFO, mutual exclusion, and bounded concurrency. Applications
own business coordination across workflows, including durable ordering,
concurrency limits, retries, idempotency, and recovery.

Retire `KeyedExclusive`, `KeyedFifo`, `KeyedBounded`, legacy registrations with
`ResourceName` / `ResourceField` regardless of `StrictFifo`, and business-resource
builders `ExclusiveBy`, `ExclusiveGloballyBy`, `SequentialBy`,
`SequentialGloballyBy`, and `LimitBy`. Close direct `HubResourceRule` and inline
insertion entry points as well. Do not silently translate a retired policy into
another scheduling mode.

Preserve `Unordered`, `PartitionSerial`, workflow/task fencing, and existing
runtime quotas (`MaxHubConcurrency`, `MaxActivityConcurrency`,
`MaxWorkflowTaskConcurrency`, and non-keyed legacy Hub/activity capacities).
Runtime quotas may coordinate across partitions; they are operational admission
limits, not business-key policies. Removing these quotas is outside this scope.
Do not change the default `Legacy` registration to `Unordered` implicitly.

New work must not acquire a permit derived from a business input value. Once
compatibility work drains, acquisition no longer needs arbitrary business-key
aggregation, hashed business selectors, or cross-partition business-resource
checks. Partition and quota enforcement remain necessary. Existing shared tables
may remain in use for those internal permits; removing business policy is not
the same as dropping every table whose name contains `resource`.

Partition semantics remain as currently implemented: `PartitionSerial` admits
one opted-in activity task at a time per `(hub, persisted partition)`, in task
sequence order. It does not reserve a partition for an entire workflow while
that workflow waits for activities, timers, or events. Whole-workflow partition
serialization requires a separate design; it is not introduced by this removal.
The matching shard (`partition_id % matching_count`) and discovery bucket
(`partition_id % 64`) are routing units, not business ordering scopes.

## Current implementation and consequences

| Area | Observed implementation | Consequence for removal |
| --- | --- | --- |
| [Policies](../src/WorkflowCore.Hub/HubPolicies.cs) | Keyed enum values are 3, 4, and 5; builders offer FIFO, exclusive, and bounded business permits, including global scope. `PartitionSerial` creates `partition:<hub>:<partition>` with capacity 1 and FIFO enabled. | Retire every business-resource entry point, retain partition FIFO and non-keyed quotas. Do not delete every `StrictFifo` check. |
| [Registration](../src/WorkflowCore.RemoteExecution/HubWorkflowRegistration.cs), [wire contract](../src/WorkflowCore.Hub/HubRemoteContracts.cs), [persisted resolution](../src/WorkflowCore.Hub/HubDefinitionStore.cs) | Explicit keyed modes and legacy resource selectors produce business constraints, even with FIFO disabled. Registration can upsert routing fields under the same activity version. | Validate every entry point; prevent an old publisher from restoring a retired policy. Freeze compatibility registrations while draining. |
| [Enqueue and claim](../src/WorkflowCore.Hub/HubTaskStore.cs), [remote activity insertion](../src/WorkflowCore.Hub/HubRemoteWorkflowStore.cs), [inline workflow insertion](../src/WorkflowCore.Hub/HubWorkflowStore.cs) | Enqueues materialize resource rows and task constraints. Claims lock each resource and count active leases and earlier unfinished tasks across its scope. | Preserve admission for existing constrained work, including work scheduled later by an existing workflow. |
| [Candidate reader](../src/WorkflowCore.Hub/HubCandidateReader.cs) | Pages aggregate constraint sizes and resource-key arrays. Exact task-ID hydration was removed with the candidate notification fast path. | Policy retirement does not automatically simplify these queries. |
| [Acquisition](../src/WorkflowCore.Hub.Gateway/HubCandidateAcquisition.cs) | Only candidates with a complete, empty resource set use unconstrained batch admission. | A task remains constrained if partition FIFO or runtime quotas apply, or if it is draining a legacy business policy. |
| [Operations](../src/WorkflowCore.Hub/HubOperationsStore.cs) | Blocked-ready estimates include capacity and FIFO predecessors. | Keep truthful reporting for partition FIFO and draining compatibility work. |
| [Capacity update](../src/WorkflowCore.Hub/HubDefinitionStore.cs), `UpdateActivityCapacityAsync` | `ResourceCapacity` and shared-resource coordination update business limits alongside Hub/activity capacities. | Keep business capacity updates only for compatibility drain; preserve revision checks and runtime quota ownership. Retire business fields from the supported contract after compatibility ends. |
| [Schema](../sql/007_hub_gateway.sql), [activity registration](../sql/010_hub_remote_scheduler.sql), [ordering migration](../sql/015_explicit_activity_ordering.sql) | FIFO is persisted in resource and registration rows; policy names are constrained in SQL. | Preserve old values and history; do not edit applied migrations or renumber enum values. |

The [17 November discovery diagnosis](../benchmarks/hub-candidate-acquisition-simplification/investigation-20261117/diagnosis.md)
records repeated resource aggregation during an unordered workload. This supports
investigating discovery independently; it is not evidence of a measured speedup
from removing keyed policies.

## Intended public behavior

| Request or existing state | Intended behavior |
| --- | --- |
| New `KeyedExclusive`, `KeyedFifo`, or `KeyedBounded` registration, including numeric values 3, 4, and 5 | Reject with an actionable validation error. Do not reinterpret as `Unordered` or `PartitionSerial`. |
| Legacy registration with a business resource selector, with either FIFO flag value | Reject for new work. Reject partial selectors and non-default retired business options rather than silently ignoring them. |
| `ExclusiveBy`, `ExclusiveGloballyBy`, `SequentialBy`, `SequentialGloballyBy`, `LimitBy`, or a direct business `HubResourceRule` | Deprecate at the SDK boundary and reject new use server-side. Low-level routes must not bypass the decision by disguising keys as internal permits. |
| Existing retired-policy registration during drain | Permit an identical, frozen compatibility publication needed by old workers. Reject policy/contract/routing drift; keep operator capacity updates governed by the existing API. |
| Existing task or compatible existing workflow | Retain its FIFO, exclusive, or bounded constraints until it reaches a terminal state. |
| New version using a supported policy | Explicitly choose the replacement, publish compatible workers, and use it for new starts. No implicit Hub quota/default change. |
| Partition FIFO | Preserve its current activity-level ordering, failure/retry behavior, and independent execution across partitions. |
| Runtime quota | Retain cross-partition operational quota enforcement and operator revision checks without business input selectors. |
| Business `ResourceCapacity` update | Allow only for an existing draining policy under existing revision/shared-owner checks; reject creation or restoration of business policies through the capacity API. |

Keep persisted enum identities stable: retire values 3, 4, and 5 without reusing
them or renumbering supported values. During transition retain deprecated
representations for legacy reads; if public symbols are later removed, use an explicit compatibility
decoder for old data instead of relying on `Enum.Parse` to continue working.

## Delivery sequence

### 1. Inventory and freeze the transition contract

- Report all three keyed modes, every legacy resource selector regardless of
  FIFO flag, global and Hub-scoped builders, direct custom rules, shared-capacity
  owners, and business constraints linked to queued/leased tasks. Distinguish
  internal partition/quota permits from business resources using provenance and
  persisted constraints; a text prefix alone is not trusted classification.
- Find nonterminal workflows that can schedule a retired activity later,
  including suspended, timer-waiting, event-waiting, recurrent, and sequential
  compatibility workflows. Include in-memory policy registrations and inline
  workflow definitions; the activity-registration table is not a complete inventory.
- Identify the application versions that own them. Choose supported replacement
  policies for new versions and document removal of same-key cross-partition
  ordering, exclusion, and bounded concurrency. Do not turn `PartitionSerial`
  on globally as part of removal. Same-key inputs may overlap across partitions.
- Define a retirement manifest of affected Hub/definition/activity versions and
  freeze their publications. Capture source/image identity, migration/index
  catalog, and effective settings for the baseline. Preserve current uncommitted
  candidate-reader/registry changes; benchmark from a recorded source snapshot.

Deliverable: inventory, replacement mapping, and affected-version drain checklist.

### 2. Add compatibility-aware rejection before removing enforcement

- Implement matching validation in typed registration, Gateway/store registration,
  policy builders, and low-level task/inline insertion. Validate wire enum names
  and numbers according to the actual serializers. Return a stable error naming
  the retired policy and the application coordination responsibility.
- Allow frozen existing registrations to republish for recovery. This exception
  must not allow any new business-key registration or same-version mutation.
- Gate new starts for affected old workflow definitions; preserve idempotent
  replay of an already accepted start. Cover child starts, schedules, recurrence,
  and direct task producers. Continuations/retries of existing workflows must
  continue to schedule compatibility tasks under their original policy.
- Use a controlled rollout: quiesce affected producers/starts, update all Hubs
  and publishers, then resume supported new versions. Do not declare retirement
  enforced while an old Hub or publisher can still accept new retired work.
- Preserve claims, completion, heartbeat, retries, attempts, snapshots, history,
  timer/event arbitration, SSE credits, owner fencing, and ambiguous-commit
  quarantine for both supported and draining work.
- Keep resource capacity mutation for existing drain work separated from the
  supported runtime quota update contract. Preserve compare-and-swap revisions
  and shared-owner coordination; removing business fields must not remove the
  checks that still protect Hub/activity quotas.

Deliverable: new business-key work is rejected; existing workflows still recover
and complete with their original guarantees.

### 3. Move samples and active validation to supported behavior

- Update [onboarding](../sample/onboarding/WorkflowCore.Provisioner.Host/Program.cs)
  legacy FIFO and bounded activities (including billing/account capacity) and
  [offboarding policy samples](../sample/offboarding/WorkflowCore.Offboarding.Processor/Program.cs).
  Use new versions where old runs can still reference existing versions. Do not
  silently republish the same version with weaker ordering.
- Replace the active keyed-FIFO benchmark lane with a partition-independence
  scenario: equal business IDs across different persisted partitions may overlap;
  partition-serial tasks within one persisted partition must remain ordered.
- Update `scripts/owner-recovery.ps1`, `owner_recovery_lab.py`,
  `partition-ordering.js`, `verify_ordering.py`, failover suites, Make targets,
  and active `.github/workflows` callers together. Readiness must not wait for a
  removed definition, and analyzers must not require a removed keyed guarantee.
- Retain historical raw artifacts and label legacy keyed-policy fixtures as
  compatibility tests. Do not delete correctness coverage merely to make the suite pass.

Deliverable: supported presets start and their smoke/load/failover audits describe
the new contract correctly.

### 4. Separate internal admission from compatibility business resources

- Apply additive admission-descriptor schema before deploying readers/writers;
  keep its default conservative for older writers during rollout.
- Add a small Hub-owned, persisted admission descriptor for tasks: no permit,
  partition serial, runtime quota, or compatibility business resource. Represent
  supported combinations of internal permits without dropping any quota. If a
  business constraint exists, compatibility takes precedence and its full
  constraint set remains authoritative. Set classification atomically at enqueue from
  validated internal routing, never from a caller-provided key or a mutable
  current registration. Existing unclassified tasks stay conservative until a
  provenance-backed migration classifies them; unknown never means unconstrained.
- Change discovery to bounded indexed identity/routing pages with this compact
  admission descriptor. Supported work should not aggregate arbitrary resource
  keys or their sizes on every page. Derive internal partition/quota permit
  identities from validated Hub/task/partition metadata at authoritative claim.
  Hydrate/cache arbitrary key sets only for compatibility work, in bounded ID
  batches. Measure descriptor/schema and query changes separately: metadata
  writes and extra hydration round trips can offset read savings.
- Keep finite horizons, keyset/cursor progress, byte/entry budgets, metadata
  failure visibility, workflow pins, and periodic recovery. Exact-ID hint
  revalidation was retired separately; see
  [its decision record](decision-retire-exact-task-id-hints.md).
  Unknown admission metadata or constraints must never be treated as empty.
- In authoritative claims, evaluate the predecessor predicate only for FIFO
  resources, which still include partition FIFO and compatibility business FIFO.
  Evaluate an indexed `EXISTS` instead of counting all earlier tasks. Preserve
  the current predecessor definition unless a separate semantics change is made.
- Provide explicit authoritative paths for partition permits and runtime quotas;
  they may reuse existing permit tables and locks. Preserve current scope and
  lock ordering. A partition predecessor check must cover opted-in serial tasks,
  not accidentally serialize every task or workflow in that partition.
- Keep generic multi-resource claims only for draining compatibility work. Recheck
  stored admission kind and residual constraints transactionally so stale cached
  metadata cannot send a constrained task into unconstrained batching. Matching
  ownership alone does not serialize task execution.

Deliverable: supported discovery avoids arbitrary business-key aggregation;
supported claims perform only partition/runtime-quota checks. Compatibility uses
original resource checks. This does not promise zero SQL joins or zero
cross-partition runtime-quota checks.

### 5. Drain and retire compatibility entry points

- Keep original FIFO, exclusive, and bounded enforcement until affected tasks,
  their active or reclaimable leases, and workflows capable of producing more tasks have
  all drained. A momentarily empty ready queue is not sufficient.
- Do not delete queued constraints, flip `strict_fifo` on shared rows, reset a
  database, terminate workflows, or waive indefinitely waiting workflows as a
  routine migration. A live unconvertible workflow keeps compatibility necessary.
- After the drain, remove supported builders/options for all business-key policies.
  Retain legacy decoding/reporting as needed for historical registrations and data.
  The database `strict_fifo` field and generic FIFO enforcement remain required
  by `PartitionSerial` and must not be dropped under this plan.
- Remove business selector hashing/resolution, arbitrary business-resource
  locking/counting, business-key candidate caches/invalidation, and active
  business-resource metrics/UI controls after their compatibility consumers end.
  Retain truthful historical reporting, partition/quota contention, and internal
  permit storage. Do not drop constraints or indexes still used by internal
  permit admission, compatibility readers, or retained history tools.
- Use forward-only migrations for admission classification and any retirement
  metadata/schema enforcement. Allocate the next number after the then-current migration (currently
  032), retain historical policy values, and update Compose, development scripts,
  CI, and isolated fixture setup. Do not tighten a CHECK constraint so that
  preserved legacy rows become invalid.

Deliverable: no supported path creates business-key constraints or checks
business keys across partitions; old durable data remains readable. Keeping
internal partition/quota permit enforcement is expected.

## Verification and acceptance

Use the PostgreSQL suite with `WFC_TEST_PG` set and migrations actually applied.
Tests that return early without the variable do not qualify the change.
Compatibility upgrade fixtures additionally require `CREATEDB` on the test role:
they create uniquely named isolated databases, apply the real migrations before
and after seeding legacy state, and drop only their own successful fixtures.
Failed fixtures are retained and their names reported for diagnosis.

| Coverage | Required proof |
| --- | --- |
| Registration | Reject all three keyed modes by supported wire encoding, legacy selectors with FIFO true/false, all business builders, direct custom rules, and capacity-API bypasses. Frozen republish succeeds; policy drift and stale publishers cannot restore retired policies. |
| Migration/drain | Upgrade fixtures for FIFO, exclusive, and bounded policies, including global/shared resources, queued/leased tasks, and waiting workflows. Existing continuations, retries, and idempotent start replays work; new starts are blocked; history and epochs survive. |
| Partition ordering | Preserve increasing task-sequence admission and one valid active lease per opted-in persisted partition across two Hubs. Different partitions can progress with the same business ID. Cover retry delays, expiry, and exhausted/dead predecessors using existing semantics. |
| Application ownership | Same business ID across different partitions creates no business permit for supported work. With runtime quota headroom, both partitions can claim concurrently. Exclusive/bounded/FIFO guarantees remain only in compatibility fixtures. |
| Runtime limits | Hub/activity/workflow-task quotas retain current scopes, revision checks, and capacity-change behavior. No unknown or constrained task enters unconstrained batching; stale descriptors and hidden residual constraints fail closed. |
| Failure recovery | Exercise owner loss before admission, ambiguous commit, commit-before-SSE loss, worker disconnect/reconnect, stale completion, and timer/event recovery. No lost task, double durable commit, or leaked permanent permit. |
| Discovery | Supported page SQL avoids arbitrary business-key aggregation; compatibility metadata handling remains bounded. Preserve oversized/unknown handling, horizon/cursor progression, notification loss recovery, workflow pins, and stale-statistics plan regressions. |

Extend the relevant existing suites: `HubExplicitOrderingTests`,
`HubGatewayTests`, `HubRegistrationTests`, `HubNativeRoutedTests`,
`HubNativeCommitOptimizationTests`, candidate reader/admission/batch/coordinator
tests, `HubUnconstrainedBatchClaimTests`, and matching/SSE failover suites.
Include `HubOperationsStoreTests` and shared-capacity update coverage. Retain
positive FIFO, exclusive/global-exclusive, bounded, and multi-resource tests for
the compatibility path until it is retired; replace supported-policy expectations
with rejection and partition-independence tests.

Implementation checks, from repository root:

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release
pwsh -NoProfile -File ./scripts/dev.ps1 help
```

Use the configured integration stack/schema or the documented `dev.ps1 test`
setup. Run primary SSE and matching failover coverage with two Hubs. Run monitor
type/topology/build checks only if its code/contracts change. No application or
database checks are required just for adding this planning document.

For performance qualification, use identical warmed topology/data/flags with
at least three alternating baseline/candidate pairs. Separate policy changes
from SQL rewrites: measure supported unordered and partition-serial workloads
before/after on identical semantics, and measure legacy drain behavior separately.
Include supported runtime-quota and mixed compatibility cohorts. Do not call
relaxed business ordering or concurrency limits a query optimization speedup.

Collect durable completions/failures, attempts/history conservation, throughput,
completion p95/p99, task-ready-to-claim delay, batch fill, statement calls and
elapsed time, shared buffers, WAL, and process CPU seconds per completed workflow.
Include empty ready/expired streams and growing terminal history. Retain failed
and outlier runs; `pg_stat_statements` elapsed is not CPU time. Require correctness
to pass, lower measured acquisition cost, and no consistent throughput/latency
regression before promoting the query change; report gains with observed spread.

## Documentation, rollback, and application responsibility

Update the current runtime, explicit ordering, native authoring, policy-update,
candidate-metadata, operations, CI, and sample guides with the implemented result.
Retain this document as a plan until its individual deliverables are complete.

Before compatibility is removed, rollback to the recorded compatible binaries
with original rows and constraints intact; keep retired-version start restrictions
in effect. Do not reverse schema by deleting data. After supported work has run
without business-key coordination, a rollback cannot retroactively restore its
former ordering, exclusion, or concurrency limits. Rollback binaries must
understand any new admission classification and its fencing.

Applications that still need business ordering, exclusion, or per-key limits can
use a durable coordinator per business identity, an ordered broker consumer, or
a domain-owned transactional queue/lock/semaphore. They must define the sequence
or capacity boundary and handle failures, retries, idempotency, and expired
ownership. Partition assignment is not a replacement business lock, especially
across workflow definitions or partition layouts. A process-local lock or worker concurrency=1
does not provide a distributed ordering guarantee. Providing such a coordinator
is application work, not part of this Hub change.
