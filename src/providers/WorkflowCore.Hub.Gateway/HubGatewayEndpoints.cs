using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Development Hub gateway. Worker access requires a bearer secret. Production
/// requires OIDC/mTLS, scoped hub authorization, network isolation and audit.
/// </summary>
public static class HubGatewayEndpoints
{
    public static IServiceCollection AddHubGateway(
        this IServiceCollection services, string connectionString)
    {
        var maxConnections = int.TryParse(Environment.GetEnvironmentVariable("WFC_HUB_DB_POOL"), out var max)
            ? Math.Clamp(max, 1, 100) : 12;
        services.AddSingleton(new HubTaskStore(connectionString, maxConnections));
        services.AddSingleton(new HubPolicyRegistry()
            .Register("onboarding", "validate-tenant", 1, 32, policy => policy
                .MaxHubConcurrency(100)
                .MaxActivityConcurrency(100))
            .Register("onboarding", "send-welcome", 1, 32, policy => policy
                .MaxHubConcurrency(100)
                .MaxActivityConcurrency(100))
            .Register("onboarding", "provision-tenant", 1, 32, policy => policy
                .MaxHubConcurrency(100)
                .MaxActivityConcurrency(20)
                .SequentialBy("tenant", body => StringKey(body, "tenantId")))
            .Register("backup", "restore-database", 1, 16, policy => policy
                .MaxHubConcurrency(16)
                .SequentialBy("database", body => StringKey(body, "databaseId")))
            .Register("billing", "capture-payment", 1, 32, policy => policy
                .MaxHubConcurrency(64)
                .LimitBy("account", 2, body => StringKey(body, "accountId"))));
        services.AddSingleton(new HubWorkflowRegistry()
            .Register("onboarding", "tenant-onboarding", 1, 32, builder => builder
                .Activity("validate-tenant")
                .Then("provision-tenant")
                .Then("send-welcome")));
        services.AddSingleton<HubWorkflowStore>();
        services.AddSingleton<HubReadinessBroadcaster>();
        services.AddHostedService(sp => sp.GetRequiredService<HubReadinessBroadcaster>());
        return services;
    }

    public static WebApplication MapHubGateway(this WebApplication app)
    {
        var token = Environment.GetEnvironmentVariable("WFC_HUB_AUTH_TOKEN");
        if (string.IsNullOrWhiteSpace(token) || token.Length < 24)
            throw new InvalidOperationException(
                "WFC_HUB_ENABLED requires WFC_HUB_AUTH_TOKEN with at least 24 characters.");

        var routes = app.MapGroup("/api/hub");
        routes.AddEndpointFilter(async (context, next) =>
        {
            var authorization = context.HttpContext.Request.Headers.Authorization.ToString();
            var candidate = authorization.StartsWith("Bearer ", StringComparison.Ordinal)
                ? authorization["Bearer ".Length..] : "";
            var expectedBytes = Encoding.UTF8.GetBytes(token);
            var actualBytes = Encoding.UTF8.GetBytes(candidate);
            if (actualBytes.Length != expectedBytes.Length ||
                !CryptographicOperations.FixedTimeEquals(actualBytes, expectedBytes))
                return Results.Unauthorized();
            return await next(context);
        });

        routes.MapGet("/policies", (HubPolicyRegistry registry) =>
            Results.Ok(registry.Describe()));
        routes.MapGet("/definitions", (HubWorkflowRegistry registry) =>
            Results.Ok(registry.Describe()));

        routes.MapPost("/workflows", async (
            HubWorkflowStartRequest request, HubWorkflowStore workflows,
            HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            try
            {
                var id = await workflows.StartAsync(request, ct);
                readiness.Publish(request.HubId);
                return Results.Accepted($"/api/hub/workflows/{id}", new { workflowId = id });
            }
            catch (KeyNotFoundException)
            {
                return Results.NotFound(new { error = "Unknown code-first Hub workflow" });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new { error = ex.Message });
            }
        });

        routes.MapGet("/workflows/{id:guid}", async (Guid id,
            HubWorkflowStore workflows, CancellationToken ct) =>
        {
            var workflow = await workflows.GetAsync(id, ct);
            return workflow is null ? Results.NotFound() : Results.Ok(workflow);
        });
        routes.MapGet("/workflows/{id:guid}/history", async (Guid id,
            HubWorkflowStore workflows, CancellationToken ct) =>
            Results.Ok(await workflows.HistoryAsync(id, ct)));

        routes.MapGet("/runs/{runId}/summary", async (string runId,
            HubWorkflowStore workflows, CancellationToken ct) =>
        {
            if (runId.Length is < 1 or > 100)
                return Results.BadRequest(new { error = "Invalid run identifier" });
            return Results.Ok(await workflows.SummaryAsync(runId, ct));
        });

        routes.MapPost("/tasks", async (HubEnqueueRequest request,
            HubPolicyRegistry registry, HubTaskStore store,
            HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            if (request.Payload.ValueKind != JsonValueKind.Object ||
                request.Payload.GetRawText().Length > 65536 ||
                request.IdempotencyKey?.Length > 128)
                return Results.BadRequest(new { error = "Invalid payload or idempotency key" });
            HubTaskRoute route;
            try { route = registry.Resolve(request.HubId, request.TaskType,
                request.Version, request.Payload); }
            catch (KeyNotFoundException) { return Results.NotFound(new { error = "Unknown code-first task" }); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }

            try
            {
                var id = await store.EnqueueAsync(route, request.Payload.GetRawText(),
                    request.WorkflowId, request.IdempotencyKey, ct);
                readiness.Publish(request.HubId);
                return Results.Accepted($"/api/hub/tasks/{id}", new { taskId = id });
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new { error = ex.Message });
            }
        });

        routes.MapGet("/tasks/{id:guid}", async (Guid id,
            HubTaskStore store, CancellationToken ct) =>
        {
            var task = await store.GetAsync(id, ct);
            return task is null ? Results.NotFound() : Results.Ok(task);
        });

        routes.MapPost("/workers/tasks:claim", async (HubClaimRequest request,
            HubTaskStore store, CancellationToken ct) =>
        {
            if (request.Hubs is null || request.Capabilities is null ||
                request.MaxTasks is < 1 or > 32 ||
                request.Hubs.Any(x => string.IsNullOrWhiteSpace(x) || x.Length > 80) ||
                request.Capabilities.Any(x => x.Version < 1 || string.IsNullOrWhiteSpace(x.HubId) ||
                    string.IsNullOrWhiteSpace(x.TaskType) || x.HubId.Length > 80 ||
                    x.TaskType.Length > 80))
                return Results.BadRequest(new { error = "Invalid worker capabilities" });
            try
            {
                var items = await store.ClaimAsync(request.WorkerId,
                    request.Hubs, request.Capabilities, request.MaxTasks,
                    TimeSpan.FromSeconds(45), ct);
                return Results.Ok(items);
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        routes.MapPost("/tasks/{id:guid}/heartbeat", async (
            Guid id, HubTaskHeartbeatRequest request, HubTaskStore store, CancellationToken ct) =>
            await store.HeartbeatAsync(id, request.WorkerId, request.Epoch,
                TimeSpan.FromSeconds(45), ct)
                ? Results.NoContent() : Results.Conflict(new { error = "Lease was lost" }));

        routes.MapPost("/tasks/{id:guid}/complete", async (
            Guid id, HubTaskResultRequest request, HubTaskStore store,
            HubWorkflowStore workflows, HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            if (request.Result.ValueKind is JsonValueKind.Undefined or
                JsonValueKind.Null || request.Result.GetRawText().Length > 65536)
                return Results.BadRequest(new { error = "Invalid activity result" });
            var managed = await store.IsManagedWorkflowStepAsync(id, ct);
            var done = managed
                ? await workflows.CompleteAsync(id, request.WorkerId, request.Epoch,
                    request.Result.GetRawText(), ct)
                : await store.CompleteAsync(id, request.WorkerId, request.Epoch,
                    request.Result.GetRawText(), ct);
            if (done)
            {
                var task = await store.GetAsync(id, ct);
                if (task is not null) readiness.Publish(task.HubId);
            }
            return done ? Results.NoContent() :
                Results.Conflict(new { error = "Task is stale or lease expired" });
        });

        routes.MapPost("/tasks/{id:guid}/fail", async (
            Guid id, HubTaskFailureRequest request, HubTaskStore store,
            HubWorkflowStore workflows, HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(request.Error) ||
                request.RetryDelaySeconds is < 0 or > 3600)
                return Results.BadRequest(new { error = "Invalid retry request" });
            var managed = await store.IsManagedWorkflowStepAsync(id, ct);
            var failed = managed
                ? await workflows.FailAsync(id, request.WorkerId, request.Epoch,
                    request.Error, TimeSpan.FromSeconds(request.RetryDelaySeconds), ct)
                : await store.FailAsync(id, request.WorkerId, request.Epoch,
                    request.Error, TimeSpan.FromSeconds(request.RetryDelaySeconds), ct);
            if (failed)
            {
                var task = await store.GetAsync(id, ct);
                if (task is not null) readiness.Publish(task.HubId);
            }
            return failed ? Results.NoContent() :
                Results.Conflict(new { error = "Task is stale or lease expired" });
        });

        routes.MapGet("/workers/events", async (
            HttpContext context, HubReadinessBroadcaster readiness) =>
        {
            var hubs = context.Request.Query["hub"].ToString()
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Distinct(StringComparer.Ordinal).ToArray();
            if (hubs.Length is < 1 or > 32 ||
                hubs.Any(h => h.Length > 80 || h.Length == 0))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }
            context.Response.ContentType = "text/event-stream";
            context.Response.Headers.CacheControl = "no-cache, no-transform";
            context.Response.Headers["X-Accel-Buffering"] = "no";
            await context.Response.StartAsync(context.RequestAborted);
            await foreach (var hub in readiness.Subscribe(hubs, context.RequestAborted))
            {
                var text = hub.Length == 0 ? ": keepalive\n\n" :
                    $"event: tasks.available\ndata: {JsonSerializer.Serialize(new { hubId = hub })}\n\n";
                await context.Response.WriteAsync(text, context.RequestAborted);
                await context.Response.Body.FlushAsync(context.RequestAborted);
            }
        });

        return app;
    }

    private static string? StringKey(JsonElement body, string name) =>
        body.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString() : null;
}
