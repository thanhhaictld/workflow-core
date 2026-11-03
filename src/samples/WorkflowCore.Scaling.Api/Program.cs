using Microsoft.EntityFrameworkCore;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Primitives;
using WorkflowCore.Persistence.EntityFramework.Models;
using WorkflowCore.Persistence.PostgreSQL;
using WorkflowCore.Scaling.PostgreSQL;
using WorkflowCore.Scaling.Api;
using WorkflowCore.Scaling.Samples;

var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("Workflow")
    ?? Environment.GetEnvironmentVariable("WFC_PG")
    ?? throw new InvalidOperationException("Set ConnectionStrings:Workflow or WFC_PG");

// Makefile bootstrap runs EF migrations without starting the hosted work pump.
// This path has no HTTP listeners and performs no workflow execution.
if (Environment.GetEnvironmentVariable("WFC_MIGRATE_ONLY") == "1")
{
    await using var migrationDb = new PostgresContext(connectionString, "wfc");
    await migrationDb.Database.MigrateAsync();
    return;
}

builder.Services.AddDistributedWorkflowCore(connectionString, options =>
{
    options.Concurrency = int.TryParse(Environment.GetEnvironmentVariable("WORKERS"), out var workers)
        ? Math.Max(1, workers) : 16;
    // Run 'Worker' separately in the PowerShell sample harness; existing
    // stand-alone API behavior retains its embedded worker by default.
    options.StartWorker = Environment.GetEnvironmentVariable("WFC_EMBEDDED_WORKER") != "false";
});
builder.Services.AddBenchmarkProvisionDemo();
builder.Services.AddTenantOnboardingDemo();
if (Environment.GetEnvironmentVariable("WFC_HUB_ENABLED") == "true")
    builder.Services.AddHubGateway(connectionString);
builder.Services.AddSingleton(new WorkflowMonitorReader(connectionString));
builder.Services.AddSingleton(sp => new DefinitionCatalog(connectionString,
    sp.GetRequiredService<ILogger<DefinitionCatalog>>()));
var app = builder.Build();
if (Environment.GetEnvironmentVariable("WFC_HUB_ENABLED") == "true")
    app.MapHubGateway();

// The old workflow host is used for definition registration ONLY.
// Do not call host.Start(): the v2 runtime owns every scheduler and consumer.
app.Services.GetRequiredService<IWorkflowHost>().RegisterBenchmarkProvisionDemo();
app.Services.GetRequiredService<IWorkflowHost>().RegisterTenantOnboardingDemo();

// Archive descriptive metadata from code-first definitions. This is a
// best-effort review projection, NOT the runtime registry or a deployment gate.
var catalog = app.Services.GetRequiredService<DefinitionCatalog>();
try
{
    await catalog.ReconcileAsync(app.Services.GetRequiredService<IWorkflowRegistry>()
        .GetAllDefinitions(), CancellationToken.None);
}
catch (Exception error)
{
    app.Logger.LogWarning(error, "Workflow metadata capture failed; execution remains code-first");
}
var managementWritesEnabled = Environment.GetEnvironmentVariable("WFC_ENABLE_MANAGEMENT_WRITES") == "true";

// Demo-only control-plane endpoints for exercising real journal/workflow UI.
// They require authorization, tenancy checks and rate limiting if ever hosted
// beyond a private workstation. No actual external provisioning occurs.
app.MapPost("/api/demo/onboardings", async (
    DemoOnboardingRequest request, HttpContext http,
    DistributedWorkflowEngine engine, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.TenantId) || request.TenantId.Length > 100 ||
        string.IsNullOrWhiteSpace(request.OrganizationName) || request.OrganizationName.Length > 120)
        return Results.BadRequest(new { error = "tenantId and organizationName are required (max 100/120)" });

    var state = new TenantOnboardingState
    {
        TenantId = request.TenantId.Trim(),
        OrganizationName = request.OrganizationName.Trim(),
        RequiresApproval = request.RequiresApproval,
        SimulateTransientFailure = request.SimulateTransientFailure
    };
    if (request.Parallel && (request.RequiresApproval || request.SimulateTransientFailure))
        return Results.BadRequest(new { error = "Parallel demo cannot combine with approval or transient-failure scenarios" });
    var definitionId = request.Parallel ? "tenant-onboarding-parallel"
        : request.RequiresApproval ? "tenant-onboarding-approval" : "tenant-onboarding-auto";
    // Workflow Core's code registry selects the version; metadata cannot
    // govern or change which implementation the runtime executes.
    var id = await engine.StartAsync(definitionId, state, reference: state.TenantId,
        idempotencyKey: http.Request.Headers["Idempotency-Key"].FirstOrDefault(), ct: ct);
    return Results.Accepted($"/api/workflows/{id}", new
    {
        workflowId = id, state.TenantId, state.OrganizationName,
        definitionId, state.RequiresApproval, state.SimulateTransientFailure
    });
});

app.MapPost("/api/demo/onboardings/{id:guid}/approve", async (
    Guid id, DistributedWorkflowEngine engine, CancellationToken ct) =>
{
    await using var db = new PostgresContext(connectionString, "wfc");
    var stored = await db.Set<PersistedWorkflow>().AsNoTracking()
        .Where(w => w.InstanceId == id)
        .Select(w => new { w.WorkflowDefinitionId, w.Reference, w.Status })
        .FirstOrDefaultAsync(ct);
    if (stored is null) return Results.NotFound();
    if (stored.WorkflowDefinitionId != "tenant-onboarding-approval")
        return Results.BadRequest(new { error = "Workflow does not require approval" });
    if (stored.Status != WorkflowStatus.Runnable)
        return Results.Conflict(new { error = "Workflow is not runnable" });
    if (string.IsNullOrWhiteSpace(stored.Reference))
        return Results.BadRequest(new { error = "Workflow has no approval key" });
    // The demo uses unique tenantIds as event correlation keys. This avoids
    // exposing persisted state; don't reuse a tenantId across demo instances.
    var eventId = await engine.PublishEventAsync("OnboardingApproved",
        stored.Reference, "approved",
        idempotencyKey: $"onboarding-approve:{id}", ct: ct);
    return Results.Accepted($"/api/workflows/{id}", new { workflowId = id, eventId });
});

app.MapPost("/api/workflows", async (
    StartRequest request, HttpContext http, DistributedWorkflowEngine engine,
    CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.TenantId))
        return Results.BadRequest("tenantId is required");
    var idempotencyKey = http.Request.Headers["Idempotency-Key"].FirstOrDefault();
    var id = await engine.StartAsync("benchmark", new BenchmarkProvisionState
    {
        TenantId = request.TenantId
    }, reference: request.TenantId, idempotencyKey: idempotencyKey, ct: ct);
    return Results.Accepted($"/api/workflows/{id}", new { workflowId = id });
});

app.MapPost("/api/events", async (PublishRequest request, HttpContext http,
    DistributedWorkflowEngine engine, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.Name) || string.IsNullOrWhiteSpace(request.Key))
        return Results.BadRequest("name and key are required");
    var idempotencyKey = http.Request.Headers["Idempotency-Key"].FirstOrDefault();
    var id = await engine.PublishEventAsync(request.Name, request.Key, request.Value,
        idempotencyKey: idempotencyKey, ct: ct);
    return Results.Accepted($"/api/events/{id}", new { eventId = id });
});

app.MapGet("/api/workflows/{id:guid}", async (Guid id, CancellationToken ct) =>
{
    await using var db = new PostgresContext(connectionString, "wfc");
    var row = await db.Set<PersistedWorkflow>().AsNoTracking()
        .Where(w => w.InstanceId == id)
        .Select(w => new { w.InstanceId, w.WorkflowDefinitionId, w.Status, w.NextExecution })
        .FirstOrDefaultAsync(ct);
    return row is null ? Results.NotFound() : Results.Ok(row);
});

// Development API only: add authentication and per-tenant authorization before
// exposing workflow history in a real SaaS application.
app.MapGet("/api/workflows/{id:guid}/history", async (
    Guid id, long? after, int? take, ExecutionHistoryStore history, CancellationToken ct) =>
{
    if (after < 0 || take is < 1 or > 1000)
        return Results.BadRequest("after must be nonnegative; take must be 1..1000");
    await using var db = new PostgresContext(connectionString, "wfc");
    var exists = await db.Set<PersistedWorkflow>().AsNoTracking()
        .AnyAsync(w => w.InstanceId == id, ct);
    if (!exists) return Results.NotFound();

    var entries = await history.ReadAsync(id, after ?? 0, take ?? 100, ct);
    return Results.Ok(new
    {
        items = entries,
        nextAfter = entries.Count == 0 ? after ?? 0 : entries[^1].HistoryId
    });
});

// Read-only operational views. The sample API is intentionally not authenticated.
// Never expose these endpoints outside a trusted development network before adding
// an authorization policy that scopes *every* list/detail/history query by tenant.
app.MapGet("/api/monitor/overview", async (
    WorkflowMonitorReader monitor, CancellationToken ct) =>
    Results.Ok(await monitor.OverviewAsync(ct)));

app.MapGet("/api/monitor/workflows", async (
    string? search, string? status, int? page, int? take,
    WorkflowMonitorReader monitor, CancellationToken ct) =>
{
    var currentPage = page ?? 1;
    var pageSize = take ?? 25;
    if (currentPage < 1 || currentPage > 100000 || pageSize is < 1 or > 100 ||
        search?.Length > 200 ||
        (!string.IsNullOrWhiteSpace(status) && status != "All" &&
         !Enum.TryParse<WorkflowStatus>(status, true, out _)))
        return Results.BadRequest(new { error = "Invalid search, status or pagination parameters" });
    return Results.Ok(await monitor.ListAsync(search, status, currentPage, pageSize, ct));
});

app.MapGet("/api/monitor/workflows/{id:guid}", async (
    Guid id, WorkflowMonitorReader monitor, CancellationToken ct) =>
{
    // Messages may contain tokens or PII; the opt-in is for isolated diagnostics.
    var reveal = Environment.GetEnvironmentVariable("WFC_MONITOR_EXPOSE_ERROR_MESSAGES") == "true";
    var detail = await monitor.GetAsync(id, reveal, ct);
    return detail is null ? Results.NotFound() : Results.Ok(detail);
});

// Code-first workflow definitions have read-only metadata review endpoints.
// No schema mutations, lifecycle flags or default-version overrides exist.
app.MapGet("/api/definitions", async (
    DefinitionCatalog catalog, CancellationToken ct) =>
    Results.Ok(new
    {
        versions = await catalog.ListAsync(null, ct)
    }));

app.MapGet("/api/definitions/{id}/versions", async (
    string id, DefinitionCatalog catalog, CancellationToken ct) =>
{
    var versions = await catalog.ListAsync(id, ct);
    return versions.Count == 0 ? Results.NotFound() : Results.Ok(versions);
});

app.MapGet("/api/definitions/{id}/versions/{version:int}", async (
    string id, int version, DefinitionCatalog catalog, CancellationToken ct) =>
{
    if (version < 1) return Results.BadRequest();
    var item = await catalog.GetAsync(id, version, ct);
    return item is null ? Results.NotFound() : Results.Ok(item);
});

app.MapGet("/api/definitions/{id}/diff", async (
    string id, int from, int to, DefinitionCatalog catalog, CancellationToken ct) =>
{
    if (from < 1 || to < 1 || from == to)
        return Results.BadRequest(new { error = "Select two distinct positive versions" });
    var diff = await catalog.CompareAsync(id, from, to, ct);
    return diff is null ? Results.NotFound() : Results.Ok(diff);
});

// Sample start supports the two code-defined versioned onboarding schemas.
// Arbitrary user-supplied schema JSON is deliberately not executable.
app.MapPost("/api/demo/versioned-onboardings", async (
    ManagedOnboardingStart request, HttpContext http,
    IWorkflowRegistry registry, DistributedWorkflowEngine engine, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.TenantId) || request.TenantId.Length > 100 ||
        string.IsNullOrWhiteSpace(request.OrganizationName) || request.OrganizationName.Length > 120)
        return Results.BadRequest(new { error = "tenantId/organizationName are required (max 100/120)" });
    if (request.Version is < 1) return Results.BadRequest(new { error = "Invalid version" });
    // The actual executable C# definition is selected by Workflow Core.
    var definition = registry.GetDefinition("tenant-onboarding-versioned", request.Version);
    if (definition is null)
        return Results.NotFound(new { error = "Executable workflow version is not registered" });
    var version = definition.Version;
    var data = new TenantOnboardingState
    {
        TenantId = request.TenantId.Trim(), OrganizationName = request.OrganizationName.Trim()
    };
    var workflowId = await engine.StartAsync("tenant-onboarding-versioned", data,
        definitionVersion: version, reference: data.TenantId,
        idempotencyKey: http.Request.Headers["Idempotency-Key"].FirstOrDefault(), ct: ct);
    return Results.Accepted($"/api/workflows/{workflowId}",
        new { workflowId, definitionId = "tenant-onboarding-versioned", version });
});

// Dev-only instance management. The existing runtime's workflow fence owns
// state changes, and expectedCurrentStatus prevents stale/crossed operations.
app.MapGet("/api/manage/capabilities", () => Results.Ok(new { writesEnabled = managementWritesEnabled }));

app.MapPost("/api/manage/workflows/{id:guid}/status", async (
    Guid id, ManagedStatusRequest request, DistributedWorkflowEngine engine,
    CancellationToken ct) =>
{
    if (!managementWritesEnabled)
        return Results.Problem("Management writes disabled", statusCode: 403);

    await using var db = new PostgresContext(connectionString, "wfc");
    var current = await db.Set<PersistedWorkflow>().AsNoTracking()
        .Where(w => w.InstanceId == id).Select(w => (WorkflowStatus?)w.Status)
        .FirstOrDefaultAsync(ct);
    if (!current.HasValue) return Results.NotFound();

    var transition = (request.Action, current.Value) switch
    {
        ("Suspend", WorkflowStatus.Runnable) => WorkflowStatus.Suspended,
        ("Resume", WorkflowStatus.Suspended) => WorkflowStatus.Runnable,
        ("Terminate", WorkflowStatus.Runnable) => WorkflowStatus.Terminated,
        ("Terminate", WorkflowStatus.Suspended) => WorkflowStatus.Terminated,
        _ => (WorkflowStatus?)null
    };
    if (!transition.HasValue)
        return Results.Conflict(new { error = $"Cannot {request.Action} a {current.Value} workflow" });
    var applied = await engine.ChangeStatusAsync(id, transition.Value, ct,
        expectedCurrentStatus: current.Value);
    return applied ? Results.Ok(new { workflowId = id, status = transition.Value.ToString() })
        : Results.Conflict(new { error = "Workflow state changed or is currently leased. Refresh and retry." });
});

// Definition topology is the source of fork/branch/join indentation. The
// branch graph is static, but span timestamps and status remain journal-backed.
// Join() returns the Sequence container: it does not create a separate step.
app.MapGet("/api/monitor/workflows/{id:guid}/topology", async (
    Guid id, IWorkflowRegistry registry, CancellationToken ct) =>
{
    await using var db = new PostgresContext(connectionString, "wfc");
    var instance = await db.Set<PersistedWorkflow>().AsNoTracking()
        .Where(w => w.InstanceId == id)
        .Select(w => new { w.WorkflowDefinitionId, w.Version })
        .FirstOrDefaultAsync(ct);
    if (instance is null) return Results.NotFound();
    if (string.IsNullOrWhiteSpace(instance.WorkflowDefinitionId))
        return Results.NotFound(new { error = "Definition missing from workflow instance" });
    // Prefer the immutable persisted snapshot: old trace diagrams keep working
    // even after the API no longer registers their executable code version.
    var snapshot = await catalog.GetAsync(instance.WorkflowDefinitionId, instance.Version, ct);
    if (snapshot is not null)
    {
        var metadataSteps = snapshot.Schema.Steps.Select(step => new
        {
            id = step.Id, name = step.Name, kind = step.Kind,
            children = step.Children,
            nextStepIds = step.Outcomes.Select(outcome => outcome.TargetStepId).Distinct().ToArray()
        }).ToArray();
        return Results.Ok(new
        {
            definitionId = snapshot.DefinitionId, version = snapshot.Version, steps = metadataSteps
        });
    }

    var definition = registry.GetDefinition(instance.WorkflowDefinitionId, instance.Version);
    if (definition is null)
        return Results.NotFound(new { error = "Definition version is not registered or archived" });
    var steps = definition.Steps.Select(step => new
    {
        id = step.Id, name = step.Name ?? step.BodyType.Name,
        kind = step.BodyType == typeof(Sequence) && step.Children.Count > 1 ? "parallel" : "step",
        children = (IReadOnlyList<int>)step.Children.ToArray(),
        nextStepIds = step.Outcomes.Select(outcome => outcome.NextStep).Distinct().ToArray()
    }).OrderBy(step => step.id).ToArray();
    return Results.Ok(new { definitionId = definition.Id, version = definition.Version, steps });
});

app.MapGet("/health/ready", async (FencedWorkQueue queue, CancellationToken ct) =>
{
    try
    {
        await using var command = queue.DataSource.CreateCommand("SELECT 1");
        await command.ExecuteScalarAsync(ct);
        return Results.Ok(new { status = "healthy" });
    }
    catch { return Results.StatusCode(503); }
});

app.Run();

public sealed record DemoOnboardingRequest(
    string TenantId, string OrganizationName, bool RequiresApproval = false,
    bool SimulateTransientFailure = false, bool Parallel = false);
public sealed record StartRequest(string TenantId);
public sealed record PublishRequest(string Name, string Key, string? Value);

public sealed record ManagedOnboardingStart(string TenantId, string OrganizationName, int? Version = null);
public sealed record ManagedStatusRequest(string Action);
