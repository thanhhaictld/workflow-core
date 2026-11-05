# Bug: Activity policy changes prevent provisioners from starting

**Status:** Native deployment fix implemented; see validation below
**Affected area:** Hub activity registration and remote provisioner  
**Observed:** 4 November 2026, local observability Compose stack

## Summary

The Hub rejects a changed activity policy when the hub, activity type, and version already exist. A provisioner publishes its policies during startup, so the rejection stops the host. Compose then repeatedly restarts it. Activity concurrency policies need to change as business demand changes without requiring a new activity version or losing existing workflow data.

## Observed behavior

In the local stack, all four provisioner replicas failed during startup with `HttpRequestException: 409 (Conflict)` from `HubRuntimePublisher.StartAsync`. Hub logs show the rejected request was `POST /api/hub/activity-registrations`. The provisioner client did not log the response body or the identity of the conflicting registration, so the particular activity and differing fields have not been confirmed.

The Hub and PostgreSQL were healthy. Compose restarted the provisioners under the `unless-stopped` policy. Consequently, their workflow and activity worker lanes were unavailable.

The current deployment uses the combined native Provisioner for both lanes.
The standalone sequential processor has been removed from Compose.

## Reproduction

1. Register an activity policy with the Hub.
2. Change a policy field in the provisioner while keeping the same hub, activity type, and version.
3. Start the provisioner against the same PostgreSQL data.
4. Observe a 409 response during registration and a failed host startup. With Compose, the container restarts repeatedly.

For the local stack, inspect the failure with:

```powershell
docker compose -f scaling/observability/compose.yaml logs --tail 50 provisioner
docker compose -f scaling/observability/compose.yaml logs --tail 100 hub
```

## Cause

[`HubDefinitionStore.RegisterActivityAsync`](../src/WorkflowCore.Hub/HubDefinitionStore.cs) inserts a registration once and compares all policy fields on subsequent registrations. It throws `Immutable activity policy/version mismatch` if any field differs. The gateway maps that exception to HTTP 409, and [`HubRuntimePublisher`](../src/WorkflowCore.RemoteExecution/HubRuntimeWorkers.cs) lets the failed registration stop the host.

Changing registration alone is insufficient. [`HubTaskStore.EnqueueAsync`](../src/WorkflowCore.Hub/HubTaskStore.cs) persists resource capacity and FIFO settings separately and rejects a mismatch when a task is enqueued. A complete fix must reconcile those settings with policy updates.

## Expected behavior and acceptance criteria

- Operators can change operational activity policy settings, including concurrency limits, without changing the activity version solely for that adjustment.
- The system applies an update consistently to future scheduling and admission, including resources already stored in PostgreSQL, without deleting workflow data.
- Updating a policy preserves transactional state, lease fencing, retry behavior, and durable history. Lowering a capacity does not revoke existing leases; new claims wait until capacity permits them.
- Concurrent updates and rolling deployments have defined conflict and ownership rules. An older replica must not silently restore a stale policy over a newer one.
- The behavior of queued tasks, active leases, partition changes, resource keys, and FIFO changes is explicitly defined and tested.
- Activity contracts and compatibility remain protected independently of operational policy changes. The implementation documents which fields can change and how incompatible changes are handled.
- Registration failures identify the activity and cause without exposing credentials. Integration tests cover updates, existing resources and tasks, and concurrent registration or update races.

The capacity update API and ownership rules below implement these criteria for operational limits. FIFO, partition and resource selector changes remain version migrations.

## Current recovery

Use a provisioner whose registrations match the policies already stored in the Hub, or restore the matching policy in the provisioner configuration and rebuild it. Preserve the PostgreSQL volume and existing workflow data. Rebuilding alone does not resolve a genuine policy mismatch.

## Capacity update fix

The fix separates startup registration from operator policy ownership. Operational capacities can be updated through the revision-checked API documented in [activity-policy-update.md](activity-policy-update.md), with a forward-only PostgreSQL migration. Existing task resource rows are updated in the same transaction as the policy and queued tasks continue using current admission settings. Active leases are not revoked. Immutable routing/contract compatibility still rejects incompatible startup declarations. Shared resource changes require coordinated ownership, and FIFO/partition/resource-key changes are intentionally not hot-swappable.

The new PostgreSQL regression tests cover active lease retention, capacity decreases, stale enqueue routes, racing revision updates, and shared-key conflicts. CI verification is required before merge.

## Confirmed native startup failure and correction

The live response body identified `onboarding/validate-tenant/v2` with
`Immutable activity routing/contract mismatch`. Its stored contract contained the
previous Host assembly MVID. All step bodies and startup policy declarations were
compiled into that assembly, so rebuilding after a policy-only source edit also
changed the immutable activity and workflow fingerprints. Capacity-only API
changes cannot repair an implementation mismatch.

The sample now separates workflow/step code into `WorkflowCore.Provisioner.Workflows`
and keeps operational declarations in the Host. It publishes native v3 and no
longer starts the sequential Processor in Compose. Existing v1/v2 data is retained;
unfinished old runs require the matching old binary. Fingerprint checks remain
intact, including rejection of incompatible contracts under the same version.

Compose escapes database variables consistently for migrations through 013.
The load and restart smoke scripts now target native v3. Shared capacity updates
are explicit and transactional; see [operator ownership](activity-policy-update.md).

## Validation (4 November 2026)

- Release solution build succeeds. All 64 PostgreSQL tests pass with runner and
  database sharing the Docker clock; all 53 Hub/native tests also pass from
  Windows. Five separate legacy-runtime immediate-claim tests fail on the
  Windows/Docker clock combination.
- The Compose k6 functional smoke completes 20 native workflows with zero failures.
- Compose migration and rebuilt Hub plus four native Provisioner replicas start
  against the existing PostgreSQL volume. Native v3 activity and timer runs complete.
- `scripts/test-hub-native.ps1 -ManageHosts` passes with forced process death and
  timer recovery, mapped data, idempotent starts and pointer/attempt history.
- A coordinated capacity decrease survives restarting all four provisioners; the
  original limit was restored with a revision-checked update after verification.
- Editing Host activity concurrency and rebuilding leaves the workflow assembly
  byte-for-byte unchanged. Rebuilding the Docker Host after host-only edits also
  registers successfully against the existing v3 metadata.

Implementation fingerprints identify build artifacts. Deploy the same published
workflow assembly to replicas; separately compiled Windows and Linux artifacts
can have different MVIDs and must not be mixed under one pinned version.
