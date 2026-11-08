using System.Diagnostics;
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
    // Optional: the matcher verifies the current PostgreSQL ownership epoch
    // immediately before every SQL task claim. The Gateway has already read
    // the live partition owner to route the HTTP poll, so another gateway-side
    // OwnsAsync query is redundant when the dispatcher enforces the check.
    // Default OFF until same-runner and crash-recovery A/B validation.
    private static readonly bool DeferInitialMatchingOwnerCheck =
        Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_DEFER_OWNER_CHECK") is "true" or "1" or "True";

    public static IServiceCollection AddHubGateway(
        this IServiceCollection services, string connectionString)
    {
        var maxConnections = int.TryParse(Environment.GetEnvironmentVariable("WFC_HUB_DB_POOL"), out var max)
            ? Math.Clamp(max, 1, 100) : 12;
        services.AddSingleton(new HubTaskStore(connectionString, maxConnections));
        services.AddSingleton<HubMatchingLeaseStore>();
        services.AddSingleton<HubMatchingPartitionLeaseStore>();
        services.AddSingleton<HubMatchingOwnership>();
        services.AddHostedService(sp => sp.GetRequiredService<HubMatchingOwnership>());
        services.AddSingleton<HubTaskMatcher>();
        // Generic infrastructure only: all business metadata is published
        // by independent processor/provisioner hosts through authenticated REST.
        // The empty in-memory registries retain the pre-existing inline-chain
        // API for compatibility with explicit integration-test registrations.
        services.AddSingleton(new HubPolicyRegistry());
        services.AddSingleton(new HubWorkflowRegistry());
        services.AddSingleton<HubDefinitionStore>();
        services.AddSingleton<HubRemoteWorkflowStore>();
        services.AddSingleton<HubNativeWorkflowStore>();
        services.AddSingleton<HubWorkflowStore>();
        services.AddSingleton<HubOperationsStore>();
        services.AddSingleton<HubReadinessBroadcaster>();
        services.AddHostedService(sp => sp.GetRequiredService<HubReadinessBroadcaster>());
        // Each Gateway pod holds one dedicated LISTEN session. The existing
        // PostgreSQL sweep remains authoritative if hints are lost or disabled.
        var notifyFlag = Environment.GetEnvironmentVariable("WFC_HUB_PG_NOTIFY_ENABLED");
        if (notifyFlag != "0" && !string.Equals(notifyFlag, "false", StringComparison.OrdinalIgnoreCase))
            services.AddHostedService(sp => new PostgresHubNotificationListener(
                Environment.GetEnvironmentVariable("WFC_HUB_PG_NOTIFY_CONNECTION") ?? connectionString,
                sp.GetRequiredService<HubReadinessBroadcaster>(),
                sp.GetRequiredService<ILogger<PostgresHubNotificationListener>>()));
        services.AddHostedService<HubMetricsSampler>();
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

        routes.MapGet("/policies", async (HubPolicyRegistry registry,
            HubDefinitionStore published, CancellationToken ct) =>
        {
            var external=await published.ActivitiesAsync(ct);
            return Results.Ok(registry.Describe().Cast<object>().Concat(
                external.Select(p=>(object)new {
                    p.HubId, TaskType=p.ActivityType,p.Version,
                    PartitionCount=p.Partitions,
                    Rules=new [] {
                        "hub: capacity="+p.HubConcurrency,
                        "activity: capacity="+p.ActivityConcurrency
                    }
                })));
        });
        routes.MapGet("/definitions", async (HubWorkflowRegistry registry,
            HubDefinitionStore published, CancellationToken ct) =>
        {
            var remote=await published.DefinitionsAsync(ct);
            return Results.Ok(registry.Describe().Cast<object>().Concat(
                remote.Select(d=> (object)new {
                    d.HubId,d.DefinitionId,d.Version,d.Partitions,d.StepCount,
                    ExecutionMode=d.Metadata.TryGetProperty("executionMode",out var mode)
                        ? mode.GetString() : "remote",
                    Metadata=d.Metadata,
                    Steps=d.Metadata.TryGetProperty("steps",out var steps)
                        ? steps : default(System.Text.Json.JsonElement)
                })));
        });
        routes.MapPost("/definitions", async (HubDefinitionRegistration definition,
            HubDefinitionStore store, CancellationToken ct) =>
        {
            try {
                var created=await store.RegisterAsync(definition,ct);
                return created?Results.Created("/api/hub/definitions",new{registered=true}):
                    Results.Ok(new{registered=true});
            }
            catch(ArgumentException ex){return Results.BadRequest(new{error=ex.Message});}
            catch(InvalidOperationException ex){return Results.Conflict(new{error=ex.Message});}
        });
        routes.MapPost("/activity-registrations", async (HubActivityRegistration policy,
            HubDefinitionStore store, CancellationToken ct) =>
        {
            try {
                var created=await store.RegisterActivityAsync(policy,ct);
                return created?Results.Created("/api/hub/activity-registrations",new{registered=true}):
                    Results.Ok(new{registered=true});
            }
            catch(ArgumentException ex){return Results.BadRequest(new{error=ex.Message});}
            catch(InvalidOperationException ex){return Results.Conflict(new{error=ex.Message});}
        });
        routes.MapGet("/activity-registrations/{hub}/{activity}/{version:int}",async (
            string hub,string activity,int version,HubDefinitionStore store,CancellationToken ct) =>
        {
            var current=await store.GetActivityPolicyAsync(hub,activity,version,ct);
            return current is null?Results.NotFound():Results.Ok(current);
        });
        routes.MapPut("/activity-registrations/capacity",async (
            HubActivityCapacityUpdate request,HubDefinitionStore store,CancellationToken ct) =>
        {
            try{return Results.Ok(new {revision=await store.UpdateActivityCapacityAsync(request,ct)});}
            catch(ArgumentException ex){return Results.BadRequest(new {error=ex.Message});}
            catch(KeyNotFoundException ex){return Results.NotFound(new {error=ex.Message});}
            catch(InvalidOperationException ex){return Results.Conflict(new {error=ex.Message});}
        });
        routes.MapGet("/operations", async (string? hub, int? windowMinutes,
            HubOperationsStore operations, CancellationToken ct) =>
        {
            try { return Results.Ok(await operations.SnapshotAsync(hub, windowMinutes ?? 60, ct)); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });
        routes.MapGet("/diagnostics", async (HubTaskStore store, CancellationToken ct) =>
            Results.Ok(await store.DiagnosticsAsync(ct)));

        routes.MapPost("/workflows", async (
            HubWorkflowStartRequest request, HubWorkflowStore workflows,
            HubRemoteWorkflowStore remote, HubDefinitionStore registry,
            HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            try
            {
                var def=await registry.GetAsync(request.HubId,request.DefinitionId,request.Version,ct);
                var id = def is null ? await workflows.StartAsync(request, ct)
                    : await remote.StartAsync(request,ct);
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

        routes.MapGet("/workflows/{id:guid}/native-runtime", async (Guid id,
            HubNativeWorkflowStore native, CancellationToken ct) =>
        {
            var runtime = await native.GetRuntimeAsync(id,ct);
            return runtime is null ? Results.NotFound() : Results.Ok(runtime);
        });
        routes.MapPost("/workflows/{id:guid}/native-commits", async (Guid id, HubNativeCommit request,
            HubNativeWorkflowStore native, HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            try
            {
                var outcome = await native.CommitWithRoutingAsync(id,request,ct);
                if(outcome is null)return Results.Conflict(new{error="Stale native lease or revision"});
                readiness.Publish(outcome.HubId);
                return Results.Ok(outcome.Result);
            }
            catch(ArgumentException ex){return Results.BadRequest(new{error=ex.Message});}
            catch(KeyNotFoundException ex){return Results.NotFound(new{error=ex.Message});}
            catch(InvalidOperationException ex){return Results.Conflict(new{error=ex.Message});}
        });
        routes.MapPost("/hubs/{hub}/events", async (string hub, HubNativeEvent request,
            HubNativeWorkflowStore native, HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            try { await native.PublishEventAsync(hub,request,ct); readiness.Publish(hub); return Results.NoContent(); }
            catch(ArgumentException ex){return Results.BadRequest(new{error=ex.Message});}
            catch(InvalidOperationException ex){return Results.Conflict(new{error=ex.Message});}
        });
        routes.MapPost("/workflows/{id:guid}/status", async (Guid id, HubNativeStatusChange request,
            HubNativeWorkflowStore native, HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            try
            {
                if(!await native.ChangeStatusAsync(id,request,ct))return Results.Conflict();
                var runtime=await native.GetRuntimeAsync(id,ct);
                if(runtime is not null)readiness.Publish(runtime.HubId);
                return Results.NoContent();
            }
            catch(ArgumentException ex){return Results.BadRequest(new{error=ex.Message});}
            catch(InvalidOperationException ex){return Results.Conflict(new{error=ex.Message});}
        });
        routes.MapGet("/workflows/{id:guid}/runtime", async (Guid id,
            HubRemoteWorkflowStore remote, CancellationToken ct) =>
        {
            var runtime=await remote.GetRuntimeAsync(id,ct);
            return runtime is null?Results.NotFound():Results.Ok(runtime);
        });
        routes.MapPost("/workflows/{id:guid}/transitions", async (Guid id,
            HubTransitionRequest request, HubRemoteWorkflowStore remote,
            HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            try
            {
                var result=await remote.CommitAsync(id,request,ct);
                if(result is null)return Results.Conflict(new{error="Stale task lease or workflow revision"});
                var runtime=await remote.GetRuntimeAsync(id,ct);
                if(runtime is not null)readiness.Publish(runtime.HubId);
                return Results.Ok(result);
            }
            catch(ArgumentException ex){return Results.BadRequest(new{error=ex.Message});}
            catch(KeyNotFoundException ex){return Results.NotFound(new{error=ex.Message});}
            catch(InvalidOperationException ex){return Results.Conflict(new{error=ex.Message});}
        });

        routes.MapGet("/workflows", async (string? hub, string? search,
            string? status, int? page, int? take,
            HubWorkflowStore workflows, CancellationToken ct) =>
        {
            try
            {
                return Results.Ok(await workflows.ListAsync(
                    hub, search, status, page ?? 1, take ?? 20, ct));
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        routes.MapGet("/workflow-overview", async (string? hub,
            HubWorkflowStore workflows, CancellationToken ct) =>
        {
            if (hub?.Length > 80)
                return Results.BadRequest(new { error = "Invalid hub" });
            return Results.Ok(await workflows.OverviewAsync(hub, ct));
        });

        routes.MapGet("/hubs", async (HubWorkflowStore workflows,
            HubWorkflowRegistry registry, CancellationToken ct) =>
        {
            var persisted = await workflows.HubIdsAsync(ct);
            return Results.Ok(persisted
                .Concat(registry.Describe().Select(x => x.HubId))
                .Distinct(StringComparer.Ordinal).OrderBy(x => x).ToArray());
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
        routes.MapGet("/workflows/{id:guid}/trace", async (Guid id,
            HubWorkflowStore workflows, CancellationToken ct) =>
        {
            if (await workflows.GetAsync(id, ct) is null) return Results.NotFound();
            return Results.Ok(await workflows.TraceAsync(id, ct));
        });

        routes.MapGet("/runs", async (int? take,
            HubWorkflowStore workflows, CancellationToken ct) =>
        {
            if (take is < 1 or > 100)
                return Results.BadRequest(new { error = "take must be 1..100" });
            return Results.Ok(await workflows.RecentRunsAsync(take ?? 20, ct));
        });

        routes.MapGet("/runs/{runId}/summary", async (string runId,
            HubWorkflowStore workflows, CancellationToken ct) =>
        {
            if (runId.Length is < 1 or > 100)
                return Results.BadRequest(new { error = "Invalid run identifier" });
            return Results.Ok(await workflows.SummaryAsync(runId, ct));
        });

        routes.MapPost("/tasks", async (HubEnqueueRequest request,
            HubPolicyRegistry registry, HubDefinitionStore published, HubTaskStore store,
            HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            if (request.Payload.ValueKind != JsonValueKind.Object ||
                request.Payload.GetRawText().Length > 65536 ||
                request.IdempotencyKey?.Length > 128)
                return Results.BadRequest(new { error = "Invalid payload or idempotency key" });
            HubTaskRoute route;
            try {
                route=await published.ResolveAsync(request.HubId,request.TaskType,
                    request.Version,request.Payload,ct)
                    ?? registry.Resolve(request.HubId, request.TaskType,
                        request.Version, request.Payload);
            }
            catch (KeyNotFoundException) { return Results.NotFound(new { error = "Unknown registered task" }); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }

            try
            {
                var id = await store.EnqueueAsync(route, request.Payload.GetRawText(),
                    request.WorkflowId, request.IdempotencyKey, ct);
                readiness.Publish(request.HubId, request.TaskType, request.Version);
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

        // Temporal-inspired demand-driven task matching. An incoming request
        // is routed to the active per-Hub owner, which parks the worker in memory
        // and only claims PostgreSQL tasks when demand and readiness intersect.
        routes.MapPost("/workers/tasks:poll", async (
            HttpContext context, HubClaimRequest request,
            HubMatchingOwnership ownership, HubTaskMatcher matcher, CancellationToken ct) =>
        {
            if (!ownership.Enabled) return Results.NotFound();
            // Multi-hub capability sets continue using the original claim API
            // until explicit fan-out to multiple matching owners is implemented.
            if (request.Hubs is not { Length: 1 } ||
                request.Capabilities is not { Length: > 0 and <= 128 } ||
                request.MaxTasks is < 1 or > 32 ||
                string.IsNullOrWhiteSpace(request.WorkerId) || request.WorkerId.Length > 128 ||
                string.IsNullOrWhiteSpace(request.Hubs[0]) || request.Hubs[0].Length > 80 ||
                request.Capabilities.Any(c => c.HubId != request.Hubs[0] ||
                    string.IsNullOrWhiteSpace(c.TaskType) || c.TaskType.Length > 80 ||
                    c.Version < 1))
                return Results.BadRequest(new { error = "Matching poll needs one Hub and valid capabilities" });
            if (!ownership.Sharded &&
                (request.MatchingPartitionCount is > 1 || request.MatchingPartition is > 0))
                return Results.Conflict(new { error = "Matching partition topology mismatch" });
            if (ownership.Sharded &&
                (request.MatchingPartitionCount != ownership.PartitionCount ||
                 request.MatchingPartition is null ||
                 request.MatchingPartition < 0 ||
                 request.MatchingPartition >= ownership.PartitionCount))
                return Results.Conflict(new { error = "Matching partition topology mismatch" });
            try
            {
                if (ownership.Sharded)
                {
                    var hub = request.Hubs[0];
                    var partition = request.MatchingPartition!.Value;
                    var forwarded = context.Request.Headers.ContainsKey("X-Wfc-Matching-Forwarded");
                    var current = await ownership.ResolvePartitionAsync(hub, partition, ct);
                    if (current is null)
                    {
                        // Unowned/expired partition: prefer a deterministic owner
                        // so healthy replicas share the matching workload.
                        if (!forwarded)
                        {
                            try
                            {
                                var routed = await ownership.ForwardToAsync(
                                    ownership.PreferredUrl(partition), request,
                                    context.Request.Headers.Authorization.ToString(), ct);
                                if (routed is not null) return Results.Ok(routed);
                            }
                            catch (HttpRequestException) { /* preferred peer unavailable */ }
                            catch (TaskCanceledException) when (!ct.IsCancellationRequested) { }
                        }
                        // Owner failed: elect locally using PostgreSQL's fenced
                        // UPSERT. Concurrent replicas cannot both win.
                        current = await ownership.AcquirePartitionAsync(hub, partition, ct);
                    }
                    if (!ownership.IsLocal(current))
                    {
                        if (forwarded) return Results.StatusCode(503);
                        var leases = await ownership.ForwardToAsync(current.OwnerUrl,
                            request, context.Request.Headers.Authorization.ToString(), ct);
                        return leases is null ? Results.StatusCode(503) : Results.Ok(leases);
                    }
                    // Valid local lease was observed in ResolvePartitionAsync.
                    // HubTaskMatcher still verifies DB ownership immediately
                    // before claiming, including after a long-poll wait.
                    if (!DeferInitialMatchingOwnerCheck &&
                        !await ownership.StillOwnsPartitionAsync(hub, partition, ct))
                        return Results.StatusCode(503);
                    return Results.Ok(await matcher.LongPollAsync(request, ct));
                }

                var owner = await ownership.ResolveAsync(request.Hubs[0], ct);
                if (!ownership.IsLocal(owner))
                {
                    // One forwarding hop only. An owner transition is retried
                    // by the worker through the load-balanced Gateway.
                    if (context.Request.Headers.ContainsKey("X-Wfc-Matching-Forwarded"))
                        return Results.StatusCode(503);
                    var leases = await ownership.ForwardAsync(owner, request,
                        context.Request.Headers.Authorization.ToString(), ct);
                    return leases is null ? Results.StatusCode(503) : Results.Ok(leases);
                }
                if (!DeferInitialMatchingOwnerCheck &&
                    !await ownership.StillOwnsAsync(request.Hubs[0], ct))
                    return Results.StatusCode(503);
                return Results.Ok(await matcher.LongPollAsync(request, ct));
            }
            catch (InvalidOperationException)
            {
                return Results.StatusCode(503);
            }
            catch (HttpRequestException)
            {
                return Results.StatusCode(503);
            }
            catch (TaskCanceledException) when (!ct.IsCancellationRequested)
            {
                return Results.StatusCode(503);
            }
        });

        routes.MapGet("/matching/owner/{hub}", async (string hub,
            HubMatchingOwnership ownership, CancellationToken ct) =>
        {
            if (!ownership.Enabled) return Results.NotFound();
            var lease = await ownership.ReadAsync(hub, ct);
            return lease is null ? Results.NotFound() :
                Results.Ok(new { lease.HubId, lease.OwnerId, lease.OwnerUrl,
                    lease.Epoch, lease.LeaseUntil, ThisNode = ownership.NodeId });
        });

        routes.MapGet("/matching/partitions/{hub}", async (string hub,
            HubMatchingOwnership ownership, CancellationToken ct) =>
        {
            if (!ownership.Sharded) return Results.NotFound();
            if (string.IsNullOrWhiteSpace(hub) || hub.Length > 80)
                return Results.BadRequest(new { error = "Invalid hub" });
            var leases = await ownership.ListPartitionsAsync(hub, ct);
            return Results.Ok(new { hub, partitions = ownership.PartitionCount,
                thisNode = ownership.NodeId,
                owners = leases.Select(l => new {
                    l.Partition, l.OwnerId, l.OwnerUrl, l.Epoch, l.LeaseUntil,
                    Active = l.LeaseUntil > DateTime.UtcNow
                }) });
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
                    TimeSpan.FromSeconds(45), ct, request.Workflows);
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
            HubWorkflowStore workflows, HubRemoteWorkflowStore remote, HubNativeWorkflowStore native,
            HubReadinessBroadcaster readiness, CancellationToken ct) =>
        {
            if (request.Result.ValueKind is JsonValueKind.Undefined or
                JsonValueKind.Null || request.Result.GetRawText().Length > 65536)
                return Results.BadRequest(new { error = "Invalid activity result" });
            using var completionSpan = HubTelemetry.Traces.StartActivity("hub.gateway.activity.complete");
            var routingStarted = Stopwatch.GetTimestamp();
            // Keep the legacy classification as an A/B control for the p95 experiment.
            var fastPath = Environment.GetEnvironmentVariable("WFC_HUB_COMPLETION_FAST_PATH_ENABLED")
                is not ("0" or "false" or "False");
            HubCompletionRoute? route;
            if (fastPath)
            {
                route = await store.GetCompletionRouteAsync(id, ct);
            }
            else
            {
                var remoteActivity = await remote.IsRemoteActivityAsync(id, ct);
                var managed = !remoteActivity && await store.IsManagedWorkflowStepAsync(id, ct);
                var nativeActivity = await native.IsActivityAsync(id, ct);
                var kind = nativeActivity ? HubCompletionKind.Native
                    : remoteActivity ? HubCompletionKind.Remote
                    : managed ? HubCompletionKind.Managed : HubCompletionKind.Standalone;
                route = new HubCompletionRoute(string.Empty, kind);
            }
            completionSpan?.AddEvent(new ActivityEvent("completion.route.resolved",
                tags: new ActivityTagsCollection {
                    { "elapsed.seconds", Stopwatch.GetElapsedTime(routingStarted).TotalSeconds },
                    { "route.kind", route?.Kind.ToString() ?? "NotFound" },
                    { "route.optimized", fastPath } }));
            if (route is null)
                return Results.Conflict(new { error = "Task is stale or lease expired" });
            var done = route.Kind switch
            {
                HubCompletionKind.Native => await native.CompleteActivityAsync(
                    id, request.WorkerId, request.Epoch, request.Result, ct),
                HubCompletionKind.Remote => await remote.CompleteActivityAsync(
                    id, request.WorkerId, request.Epoch, request.Result.GetRawText(), ct),
                HubCompletionKind.Managed => await workflows.CompleteAsync(
                    id, request.WorkerId, request.Epoch, request.Result.GetRawText(), ct),
                _ => await store.CompleteAsync(
                    id, request.WorkerId, request.Epoch, request.Result.GetRawText(), ct)
            };
            if (done)
            {
                if (fastPath) readiness.Publish(route.HubId);
                else
                {
                    var task = await store.GetAsync(id, ct);
                    if (task is not null) readiness.Publish(task.HubId);
                }
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

}
