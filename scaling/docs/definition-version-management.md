# Workflow definition version management

## Scope

This branch adds code-first workflow definition management, immutable schema
snapshots, version comparison, publication policy and fenced instance actions.

The React monitor provides:

- **/definitions** — searchable catalog, versions, default and lifecycle
- **/definitions/{id}** — compare v1 and v2 metadata, inspect schema,
  select default version, publish/deprecate/disable (lab opt-in only)
- **/workflows/{instance}** — suspend, resume and terminate using runtime gates

The sample named tenant-onboarding-versioned contains a v1 definition and a
v2 definition that inserts ConfigureAccessStep before ActivateTenantStep.

## Architecture

~~~mermaid
flowchart TD
  Code["Workflow Core fluent definitions"] --> Registry["IWorkflowRegistry"]
  Registry --> Normalize["Safe schema normalization"]
  Normalize --> SHA["Normalized JSON + SHA256"]
  SHA --> DB[("Postgres definition_version: immutable schema and mutable lifecycle")]
  DB --> API["Version catalog + diff REST API"]
  API --> UI["React management pages: versions + side-by-side diff"]
  DB --> Start["Governed new workflow starts"]
  Start --> Worker["DistributedWorkflowEngine, pinned version"]
  Worker --> Journal[("Postgres work queue and journal")]
~~~

The API registers all executable definitions, then reconciles their schemas
before HTTP requests are served. A new (definition ID, version) pair creates
an immutable snapshot. A same-version schema hash mismatch **fails API startup**.
The worker continues to register real C# workflow definitions; it does not
execute JSON, delegates or assemblies from the database.

## Normalized schema format 1

The metadata stores:

| Level | Captured fields |
| --- | --- |
| Definition | ID, version, description, data type, default error behavior and retry |
| Step | numeric ID, external ID, name, body type, kind (including parallel), child roots |
| Transitions | target ID, optional label, outcome implementation type |
| Inputs / outputs | ordinal, binding kind and safe source/target member names |
| Step policies | error behavior, retry interval, compensation ID, cancellation presence |

It **never serializes executable lambda expressions, captured constant values,
tenant state or credentials**. Source member names are captured only for direct
parameter member access. This metadata cannot guarantee that two C# method
bodies are semantically equivalent.

Parallel container joins are modeled as their Sequence parent plus children
and outcomes. The Join fluent method is not a separately executed runtime step.

## Structural version diff

The diff endpoint compares two persisted immutable schemas, returning a list of
Added, Removed and Modified field changes with paths and before/after JSON.
Step identity uses external ID when available, otherwise numeric step ID.
Insertion that shifts numeric IDs can therefore produce several related edits.
Use stable external step IDs for clean long-term diffs.

The UI supports filtering by change type and inspection/copying of normalized
metadata. Schema diff does not migrate running workflows or evaluate C# bodies.

## Lifecycle / safety

| Lifecycle | New governed starts | Existing instances | Default allowed |
| --- | --- | --- | --- |
| Published | Yes | unchanged | Yes |
| Deprecated | No | unchanged | No |
| Disabled | No | unchanged | No |

One default per definition ID is enforced by a partial unique index. Promotion
is transactional. A default version cannot be retired until another published
version is made default.

The sample start API selects the published catalog default and passes the exact
version to the runtime. The distributed engine also checks catalog policy with
a PostgreSQL row lock inside new-start transactions, so retirement cannot race
a start already being committed. Untracked definitions without catalog rows
retain their original registration behavior; integrate registry reconciliation
for every managed definition.

Instance status control supports Runnable to Suspended, Suspended to Runnable,
and Runnable/Suspended to Terminated. The runtime checks the expected current
status **under the workflow fence**. It does not undo already performed external
effects. Long-running workflows remain pinned to their original C# version.

All write endpoints are disabled unless the API process sets
WFC_ENABLE_MANAGEMENT_WRITES=true. This is only a **development switch**:
the sample API has no OIDC, RBAC, tenant authorization, or full administrative
audit records. Never publicly expose these operations until those controls
are implemented.

## API

| Method | Route | Description |
| --- | --- | --- |
| GET | /api/definitions | All snapshots and writes-enabled capability |
| GET | /api/definitions/{id}/versions | Version history |
| GET | /api/definitions/{id}/versions/{v} | Immutable schema |
| GET | /api/definitions/{id}/diff?from=1&to=2 | Structural schema diff |
| POST | /api/definitions/{id}/versions/{v}/default | Change published default |
| PUT | /api/definitions/{id}/versions/{v}/lifecycle | Change lifecycle |
| POST | /api/definitions/tenant-onboarding-versioned/start | Start typed demo v1/v2 |
| GET | /api/manage/capabilities | Management-write capability |
| POST | /api/manage/workflows/{uuid}/status | Suspend, Resume, Terminate |

## Runbook

Bootstrap applies scaling/sql/006_definition_versions.sql after SQL 005.

    pwsh ./scripts/dev.ps1 bootstrap
    $env:WFC_ENABLE_MANAGEMENT_WRITES = 'true' # private dev lab only
    pwsh ./scripts/dev.ps1 up

From a second terminal:

    pwsh ./scripts/test-onboarding.ps1
    pwsh ./scripts/test-definition-management.ps1

Open http://127.0.0.1:3002/definitions (or your configured UI port). CI
runs against real PostgreSQL, a dedicated worker, and the sample API.

## Future production work

Add OIDC and tenant-scoped RBAC, audited administrative change history,
compatibility checks before promotion, long-running workflow data migrations,
catalog reconciliation in all hosts, and versioned operational approvals.
Do not treat the schema hash as proof of step implementation compatibility.
