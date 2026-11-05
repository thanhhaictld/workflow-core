using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using WorkflowCore.Hub.Telemetry;
using WorkflowCore.Hub.Scheduling;

// Standalone Hub storage/dispatcher deployment. This process never registers
// or executes Workflow Core C# workflow definitions.
var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("Workflow")
    ?? Environment.GetEnvironmentVariable("WFC_PG")
    ?? throw new InvalidOperationException("Standalone Hub requires WFC_PG");
builder.Services.AddWorkflowHubOpenTelemetry("workflow-hub", httpServer: true);
builder.Services.AddHubGateway(connectionString);
builder.Services.AddHubScheduling(builder.Configuration.GetSection("WorkflowHub:Scheduler"));
var app = builder.Build();
app.MapHubGateway();
app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
app.MapGet("/health/ready", async (HubTaskStore store, CancellationToken ct) =>
{
    try
    {
        await store.PingAsync(ct);
        return Results.Ok(new { status = "ready" });
    }
    catch { return Results.StatusCode(503); }
});
app.Run();
