using System.Diagnostics;
using System.Text.Json;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

internal static class HubMultiplexedPushEndpoints
{
    public static void MapMultiplexedPush(this RouteGroupBuilder workers)
    {
        var routes = workers.MapGroup("/workers/multiplexed-push");
        routes.MapGet("/topology", async (string hub, int partitionCount, HubMultiplexedPush push,
            HubMatchingOwnership ownership, CancellationToken ct) =>
        {
            if (!push.Enabled) return Results.NotFound();
            if (!ValidHub(hub) || partitionCount != ownership.PartitionCount) return Results.BadRequest();
            var owners = (await ownership.ListPartitionsAsync(hub, ct, activeOnly: true))
                .ToDictionary(lease => lease.Partition);
            return Results.Ok(Enumerable.Range(0, partitionCount).Select(partition =>
                new HubPushShardLocation(partition, ownership.PreferredUrl(partition), owners.GetValueOrDefault(partition))));
        });
        routes.MapPost("/topology/{partition:int}", async (string hub, int partition, int partitionCount,
            HubMultiplexedPush push, HubMatchingOwnership ownership, CancellationToken ct) =>
        {
            if (!push.Enabled) return Results.NotFound();
            if (!ValidHub(hub) || partitionCount != ownership.PartitionCount ||
                partition < 0 || partition >= partitionCount) return Results.BadRequest();
            var owner = await ownership.ResolvePartitionAsync(hub, partition, ct)
                ?? await ownership.AcquirePartitionAsync(hub, partition, ct);
            return Results.Ok(owner);
        });
        routes.MapPost("/sessions", (HubPushProcessorRegistration registration, HubMultiplexedPush push) =>
        {
            if (!push.Enabled) return Results.NotFound();
            try
            {
                var session = push.Register(registration);
                return session is null ? Results.Conflict() : Results.Ok(session.Identity);
            }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });
        routes.MapPut("/sessions/{id:guid}/grants", (Guid id, HubPushGrant[] grants, HubMultiplexedPush push) =>
            push.Offer(id, grants) ? Results.NoContent() : Results.Conflict());
        routes.MapDelete("/sessions/{id:guid}", (Guid id, HubMultiplexedPush push) =>
        {
            push.Close(id);
            return Results.Accepted();
        });
        routes.MapPost("/retired", async (HubPushGrantRecovery recovery, HubMultiplexedPush push, CancellationToken ct) =>
        {
            if (!push.Enabled) return Results.NotFound();
            try { return Results.Ok(await push.IsRetiredAsync(recovery, ct)); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });
        routes.MapGet("/sessions/{id:guid}/events", StreamAsync);
    }

    private static bool ValidHub(string hub) => !string.IsNullOrWhiteSpace(hub) && hub.Length <= 80;

    private static async Task StreamAsync(Guid id, HttpContext context, HubMultiplexedPush push,
        IHostApplicationLifetime lifetime, CancellationToken ct)
    {
        var session = push.Connect(id);
        if (session is null) { context.Response.StatusCode = 409; return; }
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct, session.Stop.Token, lifetime.ApplicationStopping);
        var connectedAt = Stopwatch.GetTimestamp();
        var firstDispatch = true;
        HubTelemetry.SseTaskSessions.Add(1);
        try
        {
            context.Response.ContentType = "text/event-stream";
            context.Response.Headers.CacheControl = "no-cache";
            context.Response.Headers["X-Accel-Buffering"] = "no";
            await WriteAsync(": connected\n\n", stop.Token);
            while (!stop.IsCancellationRequested && !push.IsExpired(session))
            {
                using var pulse = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
                pulse.CancelAfter(TimeSpan.FromSeconds(4));
                HubPushGrantResult frame;
                try { frame = await session.Results.Reader.ReadAsync(pulse.Token); }
                catch (OperationCanceledException) when (!stop.IsCancellationRequested)
                {
                    await WriteAsync(": keepalive\n\n", stop.Token);
                    continue;
                }
                await push.SendAsync(frame, async (result, token) =>
                {
                    await WriteAsync("event: grant.result\ndata: " + JsonSerializer.Serialize(result) + "\n\n", token);
                    if (result.Tasks.Length == 0) return;
                    HubTelemetry.SseTasksDispatched.Add(result.Tasks.Length);
                    if (firstDispatch)
                    {
                        firstDispatch = false;
                        HubTelemetry.SseSessionFirstDispatch.Record(Stopwatch.GetElapsedTime(connectedAt).TotalSeconds);
                    }
                }, stop.Token);
            }
        }
        catch (OperationCanceledException) { }
        catch (IOException) { }
        finally
        {
            HubTelemetry.SseTaskSessions.Add(-1);
            push.Close(id, streamEnded: true);
        }

        async Task WriteAsync(string frame, CancellationToken token)
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
            deadline.CancelAfter(TimeSpan.FromSeconds(5));
            await context.Response.WriteAsync(frame, deadline.Token);
            await context.Response.Body.FlushAsync(deadline.Token);
        }
    }
}
