using Microsoft.EntityFrameworkCore;
using WorkflowCore.Interface;
using WorkflowCore.Models;
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
builder.Services.AddTransient<NoopProvisionStep>();
builder.Services.AddTenantOnboardingDemo();
builder.Services.AddSingleton(new WorkflowMonitorReader(connectionString));
var app = builder.Build();

// The old workflow host is used for definition registration ONLY.
// Do not call host.Start(): the v2 runtime owns every scheduler and consumer.
app.Services.GetRequiredService<IWorkflowHost>()
    .RegisterWorkflow<BenchmarkProvisionWorkflow, BenchmarkProvisionState>();
app.Services.GetRequiredService<IWorkflowHost>().RegisterTenantOnboardingDemo();

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
    var definitionId = request.RequiresApproval
        ? "tenant-onboarding-approval" : "tenant-onboarding-auto";
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
        stored.Reference, new { approved = true },
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
    bool SimulateTransientFailure = false);
public sealed record StartRequest(string TenantId);
public sealed record PublishRequest(string Name, string Key, string? Value);
public sealed class BenchmarkProvisionState
{
    public string TenantId { get; set; } = "";
}
public sealed class BenchmarkProvisionWorkflow : IWorkflow<BenchmarkProvisionState>
{
    public string Id => "benchmark";
    public int Version => 1;
    public void Build(IWorkflowBuilder<BenchmarkProvisionState> builder) =>
        builder.StartWith<NoopProvisionStep>();
}
public sealed class NoopProvisionStep : StepBody
{
    public override ExecutionResult Run(IStepExecutionContext context) => ExecutionResult.Next();
}
