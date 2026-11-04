# Activity policy capacity changes

Operational capacity limits are mutable without changing activity version. The activity contract, partition count, resource identity/selector and strict FIFO mode remain immutable; change those only through a planned version migration. Existing tasks retain resource keys, while active leases retain their fencing epochs and are never revoked when a limit decreases.

## Ownership

Provisioners register the immutable activity contract and initial capacities. Repeated startup registration compares immutable fields, but **never** changes the current capacity. A stale provisioner therefore cannot restore previous limits.

The operator owns subsequent capacity updates through `PUT /api/hub/activity-registrations/capacity`. Read the current revision from `GET /api/hub/activity-registrations/{hub}/{activity}/{version}`; provide `expectedRevision` to the update request. The Hub serializes the compare-and-swap against registration writes and enqueues. Stale revisions return HTTP 409.

Example (use your existing bearer token; never put it in committed scripts):

```http
GET /api/hub/activity-registrations/onboarding/provision-tenant/2

PUT /api/hub/activity-registrations/capacity
Content-Type: application/json

{
  "hubId": "onboarding",
  "activityType": "provision-tenant",
  "version": 2,
  "expectedRevision": 1,
  "hubConcurrency": 8,
  "activityConcurrency": 3,
  "resourceCapacity": 1
}
```

The update transaction writes the registration and already-materialized `hub_resource` capacities together. Newly enqueued tasks use the current values. Claims consult current `hub_resource` rows. When reducing capacity below the current active lease count, outstanding leases continue until completion or expiry; further claims wait. Enqueue transactions that resolved stale policy before an update fail with a retryable 409 rather than reintroducing old capacities.

**Shared keys:** the Hub capacity key is common to all activities in the hub, activity capacity is common across versions of a type, and resource capacity may be common across activities by resource name. Uncoordinated changes to a shared key are rejected. Use a coordinated policy update/migration when multiple registrations own the same key. This interface changes capacity only and intentionally does not retrofit queued tasks for changed partition counts, resource keys, or FIFO semantics.

## Deployment

1. Apply `scaling/sql/013_activity_policy_revision.sql` to the existing PostgreSQL database (Compose migration handles this). Do not delete the database volume.
2. Deploy Hub and provisioner binaries together before applying updates.
3. Verify registration endpoints with an authenticated request.
4. Read the current revision, update capacities, then read it again. A stale attempt must receive HTTP 409.
5. Verify pending activity claims obey the new capacity while existing leases finish normally.

The update endpoint currently uses the existing Hub bearer-token scope. Restrict its exposure to trusted operators and add dedicated policy-admin authorization before granting broadly scoped worker credentials to untrusted hosts.

## Limitations

This fix does **not** support live FIFO changes, resource selector changes, partition rehashing or shared resource policy rewrites. Those require a separate, transactional migration and explicit queue semantics. It does not change workflow definitions, retry accounting or durable execution history.
