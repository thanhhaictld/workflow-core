# Bug: Activity policy changes prevent provisioners from starting

**Status:** Open  
**Affected area:** Hub activity registration and remote provisioner  
**Observed:** 4 November 2026, local observability Compose stack

## Summary

The Hub rejects a changed activity policy when the hub, activity type, and version already exist. A provisioner publishes its policies during startup, so the rejection stops the host. Compose then repeatedly restarts it. Activity concurrency policies need to change as business demand changes without requiring a new activity version or losing existing workflow data.

## Observed behavior

In the local stack, all four provisioner replicas failed during startup with `HttpRequestException: 409 (Conflict)` from `HubRuntimePublisher.StartAsync`. Hub logs show the rejected request was `POST /api/hub/activity-registrations`. The provisioner client did not log the response body or the identity of the conflicting registration, so the particular activity and differing fields have not been confirmed.

The Hub and PostgreSQL were healthy. Compose restarted the provisioners under the `unless-stopped` policy. Consequently, their workflow and activity worker lanes were unavailable.

The separate `processor` service was also absent, but that is expected with a normal Compose startup: it is in the optional `sequential-processor` profile. The provisioner normally includes workflow and activity workers, including sequential compatibility.

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

These are requirements for a fix, not descriptions of currently implemented behavior. The policy authority and update API remain design decisions.

## Current recovery

Use a provisioner whose registrations match the policies already stored in the Hub, or restore the matching policy in the provisioner configuration and rebuild it. Preserve the PostgreSQL volume and existing workflow data. Rebuilding alone does not resolve a genuine policy mismatch.
