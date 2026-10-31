using Microsoft.EntityFrameworkCore;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Persistence.EntityFramework.Models;
using WorkflowCore.Persistence.PostgreSQL;
using WorkflowCore.Scaling.PostgreSQL;

var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("Workflow")
    ?? Environment.GetEnvironmentVariable("WFC_PG")
    ?? throw new InvalidOperationException("Set ConnectionStrings:Workflow or WFC_PG");

builder.Services.AddDistributedWorkflowCore(connectionString, options =>
{
    options.Concurrency = int.TryParse(Environment.GetEnvironmentVariable("WORKERS"), out var workers)
        ? Math.Max(1, workers) : 16;
});
builder.Services.AddTransient<NoopProvisionStep>();
var app = builder.Build();

// The old workflow host is used for definition registration ONLY.
// Do not call host.Start(): the v2 runtime owns every scheduler and consumer.
app.Services.GetRequiredService<IWorkflowHost>()
    .RegisterWorkflow<BenchmarkProvisionWorkflow, BenchmarkProvisionState>();

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
