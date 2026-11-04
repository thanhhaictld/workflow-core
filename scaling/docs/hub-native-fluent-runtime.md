# Native fluent workflows in the Provisioner

The combined Provisioner runs Workflow Core decisions and activity workers over
Hub REST/SSE. Hub stores opaque snapshots and durable work. It never loads workflow
assemblies or evaluates C# expressions. The application host does not open
PostgreSQL connections.

## Register workflows and typed activities

Keep upstream `IWorkflow<TData>`, `IWorkflowBuilder<TData>`, step bodies, mappings,
and error handlers. Register the Hub and partitions separately from the workflow's
code-defined ID and version:

```csharp
services.AddHubWorkflowRuntime(options =>
{
    options.HubUrl = "http://localhost:5100";
    options.Token = configuration["HubToken"]!;
    options.WorkflowConcurrency = 8;
    options.ActivityConcurrency = 16;
})
    .AddWorkflow<OnboardingWorkflow, OnboardingData>("onboarding", partitions: 32)
    .AddActivity<ProvisionTenant>("onboarding", "provision-tenant", version: 2, activity =>
    {
        activity.Partitions = 32;
        activity.Concurrency = 20;
        activity.ResourceName = "tenant";
        activity.ResourceField = "tenantId";
        activity.ResourceCapacity = 1;
        activity.StrictFifo = true;
        activity.Input(step => step.TenantId).Output(step => step.WorkspaceId);
    });
```

Ordinary steps run in the native executor. Registering a step type as a Hub
activity routes that type through durable activity work. Native mappings run
before dispatch and after successful results. The worker receives declared
properties, pointer context, persistence data, and a cancellation token. Workflow
data stays in the decision host. Put shared workflow-data mutations in ordinary
native steps.

One activity registration supplies publication, worker capability, property
contract, and Hub resource policy. Hub enforces resource limits across hosts.
Each host bounds its workflow and activity lanes independently. A waiting workflow
releases workflow worker capacity.

Input properties stay at the payload's top level for resource admission. Typed
values also travel in an adapter envelope so boxed numbers, DTOs, arrays, and
pointer persistence retain their CLR types. Fields beginning with `__hub` are
reserved for the adapter.
The typed-value envelope carries the marker `$hubValueFormat: v1`.

For native `.Activity(...)`, select a property for its entire parameters value
and a property for its result:

```csharp
runtime.AddActivity<Calculate>("billing", "calculate", 2, activity => activity
    .ActivityParameters(step => step.Request)
    .Output(step => step.Response)
    .ActivityResult(step => step.Response));
```

This supports scalar parameters and typed DTO results. Without
`ActivityParameters`, parameters are an object containing the declared input
property names. Without `ActivityResult`, the primitive receives a dictionary of
declared outputs. Typed routed steps always use their declared output properties.

## Use Hub operations

Use `HubWorkflowClient.StartAsync`, `PublishEventAsync`, and `ChangeStatusAsync`.
The typed client preserves CLR values in object-valued inputs and events.
`HubRestClient` also exposes raw JSON operations. Raw JSON events retain JSON value
types. Use the typed client when an unchanged mapping casts event data to a DTO.

Do not start `IWorkflowHost` or call legacy workflow start, event, or controller
APIs. Runtime registration removes legacy background consumers. It reuses upstream
builders, registry, pointer factory, executor, result processor, cancellation,
middleware, and error handlers without changing upstream source.

## Recover durable progression

One executor iteration produces a protocol-v1 commit containing the snapshot,
consumed inbox IDs, event subscriptions, live activity identities, dispatches,
next wake deadline, and history. Hub commits all effects and acknowledges the task
in one transaction. Worker identity, lease epoch, an unexpired lease, and expected
workflow revision fence the commit. Identical replays return the original
acknowledgement, even after later progress.
The initial task persists the genesis pointer before executing a user step.
Ordinary step retries therefore retain the pointer identity after a lost commit.

Activity completion writes an inbox result without changing the snapshot revision.
An event arriving during execution cannot invalidate or erase a result. After
acknowledgement, a commit checks unread inbox rows and creates another task when
needed. A partial unique index permits one active native workflow task per run.

Event publication and subscription commits share a short transaction-level event
lane per Hub. Events published before subscription registration remain available.
Each pointer receives a source event once. Repeated waits can consume later events.
Parallel pointers have separate subscriptions and dispatch identities.
The embedded scheduler reconciles future-effective events and exhausted activity
transport attempts. Sleep and retry deadlines use durable Hub timers.

Suspension retains dispatch intent and inbox results. Claims pause until resume.
Pointer cancellation fences outstanding activity leases and releases permits.
Complete or terminated workflows retire outstanding tasks and timers.

Workers renew leases and cancel execution on renewal failure or local lease
expiry. Completion transport retries resend the frozen result until acknowledged,
fenced, or cancelled. They do not rerun a successful body while retaining its lease.
Business failures enter the inbox and are thrown inside native execution, leaving
`.OnError(...)`, retries, suspension, and compensation authoritative.

External effects remain at least once. `__hubContext.dispatchKey` and the pointer
ID stay stable across transport retries and lease takeover. Use these identities
for downstream idempotency. A native workflow retry creates a new dispatch.
Cancellation cannot undo an external effect that already happened.

## Deploy without changing existing runs

Apply migrations in order through `012_hub_native_runtime.sql`. Migration 012 adds
native state, inbox/subscription/event tables, dispatch identities, and history
fields. It preserves existing rows and the inline and remote sequential modes.
Deploy the updated Hub code before starting native hosts or submitting native runs.
Older Gateway binaries do not understand native task completion and inbox delivery.

The sample publishes native workflows and typed activities as **v2**. It retains
immutable v1 definitions and derives v1 activity aliases from the same registrations.
The combined workflow lane handles both versions. V1 aliases require one target
implementation per activity name. The optional standalone processor runs v1 only.
The sample declares `SequentialOutput` projections to retain the exact v1 result
fields, including tenant identifiers, while v2 exposes the new typed outputs.

Registrations contain application assembly fingerprints and property contracts.
Workflow claims advertise definition/version capabilities, so a host cannot claim
a pinned version it has not registered during rollout.
Keep the exact registered code available for pinned runs. Increment versions for
workflow code or activity contract changes. Publishing a different registration
under an existing version fails. Live definition migration is not implemented.

Compose starts `provisioner` with `WORKFLOW_WORKERS` and `ACTIVITY_WORKERS`.
`processor` belongs to the optional `sequential-processor` profile.
`--register-only` publishes metadata and exits before claiming work.
`WFC_HUB_URL`, `WFC_HUB_AUTH_TOKEN`, and the local-development
`WFC_HUB_ALLOW_INSECURE_HTTP` configure transport. Only Hub receives `WFC_PG`.

Native history exposes execution pointer, predecessor, scope, and workflow retry
attempt. Activity trace rows retain task, dispatch, pointer, and lease epoch, so
repeated definition steps stay separate. The monitor shows the native snapshot
and status and pairs timer windows by timer identity.

## Verify behavior

Build the scaling solution and run the PostgreSQL tests with `WFC_TEST_PG` and
migrations through 012. Tests compare identical definitions under the adapter and
upstream executor: mappings, inline steps, branches, loops, `ForEach`, parallel
joins, delays, schedules, recurrence, events, retries, cancellation, and saga
compensation. Routed tests cover typed and scalar inputs/results, boxed values,
FIFO admission, duplicate results, and recovery after recreating application services.

Database tests exercise publication/commit races, event-before-subscription,
events during execution, repeated waits, future events, competing timer scanners,
stale owners, lease takeover, status fencing, suspension, and cancelled work.
A real HTTP fault-injection test verifies that completion outages resend a frozen
result without executing the body again.

Existing legacy queue tests assume synchronized application/database clocks and
an isolated work ledger. Database tests require a real database. Tests that return
early when `WFC_TEST_PG` is unset do not prove database behavior.

Run the process recovery smoke after a Release build:

```powershell
$env:WFC_TEST_PG = 'Host=localhost;Database=workflow;Username=workflow;Password=...'
$env:WFC_HUB_AUTH_TOKEN = 'a-development-token-with-at-least-24-characters'
pwsh -NoProfile -File scripts/test-hub-native.ps1 -ManageHosts -ApiUrl http://127.0.0.1:5102
```

The script publishes metadata, verifies v1/v2 coexistence and mapped data,
force-kills both processes during a durable wait, restarts them, and checks step
and attempt history. It never resets the database. Omit `-ManageHosts` to check
an existing combined deployment without restarting it.

Run `npm run check-types`, `npm run test:topology`, and `npm run build` from
`scaling/monitor` for monitoring changes.

## Boundaries

`SubWorkflowStepBody` is unimplemented upstream and is rejected at registration.
The adapter preserves implemented executor semantics for the tested scenarios.
These checks are not production certification or proof for every custom serializer,
middleware, or external integration.

Snapshots accept registered application types and supported value/container types.
Snapshots are limited to 4 MiB. Individual activity payloads/results and event
payloads are limited to 64 KiB. Keep workflow IDs/versions unique within a host's
upstream registry. Arbitrary CLR type loading is rejected.

Production authentication, tenant isolation, operator dead-letter workflows,
live cross-version migration, and multi-region operation remain outside this change.
