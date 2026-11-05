# Code-first workflow version metadata review

## Decision

**C# fluent definitions remain the only executable source of truth.** This
feature adds an observational metadata snapshot and structural diff, not a
workflow-schema editor, registration system, lifecycle manager or deployment
controller. The API and worker register Workflow Core definitions as before,
and the runtime selects the executable version through IWorkflowRegistry.

The version-review UI is at `/definitions` and `/definitions/{id}`.
It provides a searchable catalog of captured versions, a read-only YAML view,
and split or unified text comparison of archived snapshots. The JSON archive and
structural diff API remain available to existing clients.

## Data flow

~~~mermaid
flowchart TD
  CS["C# IWorkflow<TData>.Build()"] --> Registry["IWorkflowRegistry
Executable definitions"]
  Registry --> Runtime["DistributedWorkflowEngine
Workflow version is pinned at start"]
  Registry -. "Best-effort snapshot extraction" .-> Snapshot["Normalized metadata + SHA256"]
  Snapshot --> Postgres[("PostgreSQL definition_version
read-only archive")]
  Postgres --> Versions["GET /api/definitions/{id}/versions"]
  Versions --> UI["YAML viewer and text diff"]
  Postgres --> Diff["Structural diff API"]
~~~

**No edge from the metadata store to the runtime.** If capturing metadata
fails, the API logs the problem and continues to use the code-first registry.
The metadata archive is neither read nor locked when creating a workflow.

## Metadata format

`DefinitionSchema` contains the ID, version, description, data type, default
error policy, and an ordered collection of step metadata. Steps include their
numeric and optional external IDs, name, body type, kind, child roots,
outcome targets/labels, input-output binding descriptions, retry and error
settings, compensation ID, and cancellation flag.

Expression values, closures, executable delegates, tenant data and secrets
are intentionally not serialized. Source and target member names are used
only when they can be derived safely from simple property-member mappings.

The resulting schema is **descriptive**, not executable. It cannot recreate
the complete C# workflow, prove behavioral compatibility, or compare code
inside step-body methods.

## Immutable snapshots and drift

The catalog inserts one snapshot per `(definition_id, version)`. Once archived,
the snapshot and SHA256 are never overwritten. Re-registering a changed
schema with the *same version* logs a warning and marks that snapshot
`driftDetected` in the review API. The runtime is **not blocked**. Increment
the C# workflow version to get a new comparison baseline.

A previously archived schema remains accessible for review even if its old
executable version is no longer registered in the API process. **Running
instances still require their original executable definition**; an archived
JSON snapshot cannot execute a workflow.

The archive is populated from the registered definitions by an API startup
reconciliation. Only this snapshot capture writes to `definition_version`;
the UI and its endpoints are read-only.

## Comparison algorithm

The monitor serializes the complete archived schemas to deterministic YAML and
compares their lines. Numeric step ID shifts can produce additional changes;
text comparison does not match steps by ExternalId or establish behavioral
compatibility. YAML omits executable expressions, closures and step-body code.
The archive can differ from the currently registered C# metadata when a
same-version drift warning is shown.

The separate structural diff API continues to compare two archived versions are flattened into normalized metadata paths. Changes
are classified as `Added`, `Removed`, or `Modified`, with before/after
JSON values. Steps match by stable `ExternalId` if set, otherwise numeric
step ID. Inserting steps can shift IDs and create multiple related diff rows.
For long-lived workflows, use stable external step IDs where possible.

For `Parallel().Do(...).Join()`, a Sequence step contains branch roots and
continuation edges. `Join()` has no separate executable step or span.

## API and UI

| Method | Endpoint | Purpose |
| --- | --- | --- |
| GET | `/api/definitions` | Archived definition metadata catalog |
| GET | `/api/definitions/{id}/versions` | Archived versions |
| GET | `/api/definitions/{id}/versions/{version}` | One metadata snapshot |
| GET | `/api/definitions/{id}/diff?from=1&to=2` | Structural diff |

There are **no definition write endpoints**: no publish, default promotion,
deprecate/disable or JSON schema editing operations.

The sample-only endpoint `POST /api/demo/versioned-onboardings` starts a real
code-defined v1 or v2 sample by passing the requested version to Workflow Core.
This is an executable C# demo, **not** an action on the metadata catalog.

Instance-level suspend/resume/terminate commands remain available separately
in the Trace UI only when `WFC_ENABLE_MANAGEMENT_WRITES=true`. That flag must
never be regarded as RBAC; production requires authentication, authorization
and an audited operations API.

## Example and acceptance tests

The sample `tenant-onboarding-versioned` defines two C# workflows: v1
performs Validate → Identity → Activate → Welcome; v2 inserts
`ConfigureAccessStep` between Identity and Activate. Both are independently
registered executable implementations. The UI compares their extracted schema.

From the repo root:

~~~powershell
pwsh ./scripts/dev.ps1 bootstrap
pwsh ./scripts/dev.ps1 up
~~~

Visit `http://127.0.0.1:3002/definitions` (or the chosen UI port), select
the versioned onboarding definition, and choose v1 vs v2.

For the combined integration test, enable private-lab instance controls:

~~~powershell
$env:WFC_ENABLE_MANAGEMENT_WRITES = 'true'
pwsh ./scripts/dev.ps1 up
# Run separately once API and worker have started:
pwsh ./scripts/test-definition-management.ps1
~~~

The acceptance test validates immutable archived hashes, C# version-pinned
starts, metadata-only API access, the v1→v2 diff, and independent instance
status controls.
