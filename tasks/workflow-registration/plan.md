# Plan: Let workflow developers own compatibility

Status: implemented and verified, 5 November 2026.
Checklist: [todo.md](todo.md).

## Behavior

Workflow and activity versions come from application code. Registration upserts
current metadata for the supplied identity and version. Rebuilding or changing
that metadata does not block provisioner startup.

Developers keep same-version changes backward compatible. For breaking changes,
they introduce a new version in code and keep the earlier version registered
while its workflows finish. Existing runs retain their version and durable state.
New runs use the version requested by the caller.

The drift UI is being developed in another branch for reference and audit.
Detected changes do not gate registration, provisioner startup, or execution.
Keep existing implementation identifiers as informational metadata for that work.

## Changes

1. Remove assembly build IDs from registration acceptance checks. Keep them as
   informational metadata without enforcing exact matches.
2. Replace immutable registration comparisons with database upserts for both
   workflow definitions and activity registrations. Accept metadata and contract
   updates under the same identity/version without compatibility analysis.
3. Keep input validation, transactional updates, lease fencing, workflow revision
   checks, and authoritative operator capacity settings. Registration changes
   metadata, not existing run snapshots, tasks, timers, or history.
4. Return successful registration for inserts and updates. Remove the misleading
   `idempotent=true` response for changed registrations. The existing REST client
   can continue accepting successful responses.
5. Update focused tests and documentation to describe developer-owned versioning.

Use the existing registration tables. The `implementation` field refreshes with
the rest of the metadata when the provisioner republishes a registration.
This fix adds no schema migration, metadata archive, revision API, compatibility
mode, or automatic workflow migration. Drift detection and audit presentation
belong to the other branch and are not prerequisites for this fix. Apply the
same registration rule without retaining the old immutable behavior for
particular execution modes.

## Why this fixes the current stack

The provisioner currently fails before its worker lanes start because activity
registration returns HTTP 409 for a changed assembly ID. Workflow registration
has the same exact-match restriction. Both must change. The observed restart
loop is in the provisioner processes; registration must never restart workflows.

## Verify and deploy

Use a populated test database to verify that same-version changes update metadata
and allow worker startup. Verify that existing runs resume from their snapshots,
new runs execute, and two explicitly registered versions can finish independently.
Check that concurrent provisioner registrations succeed without partial updates.

Build and test the scaling solution with real PostgreSQL, then rebuild and
redeploy Hub and provisioners through `scaling/observability/compose.yaml`.
Keep the existing database volume. Confirm the provisioners stay running and
queued workflows progress. No production deployment compatibility layer is needed.

## Verification results

- Release scaling solution build passed.
- All 67 PostgreSQL tests passed with `WFC_TEST_PG` configured, including three
  registration tests. The initial two regression tests failed against the old
  immutable-registration code before the fix.
- Native mapping, durable timer, pointer/history, and forced process restart
  smoke checks passed against the local test database.
- Rebuilt and redeployed Hub and four provisioners through observability Compose
  using the existing database volume. All four provisioners have zero restarts.
- All 300 previously queued workflows completed. All 1,300 original workflow IDs
  and creation times remain present. The 1,000 previously completed runs retained
  their snapshots and history counts.
- New workflow and timer smoke runs completed against the Compose deployment.
