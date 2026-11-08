using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Authenticated SSE delivery and REST credit-return routes. The matching
/// owner is determined by PostgreSQL, not by LB stickiness. Forwarding streams
/// is unbuffered and worker clients reconnect when the owner changes.
/// </summary>
public static class HubSseTaskStreamingEndpoints
{
    public static RouteGroupBuilder MapSseTaskStreaming(this RouteGroupBuilder routes)
    {
        routes.MapPost("/workers/tasks:stream", async (
            HttpContext context, HubSseTaskDistributor.Subscribe request,
            HubMatchingOwnership ownership, HubSseTaskDistributor distributor,
            CancellationToken ct) =>
        {
            var claim = request.Claim;
            if (!ownership.Sharded ||
                claim.Hubs is not { Length: 1 } ||
                claim.Capabilities is not { Length: > 0 and <= 128 } ||
                claim.WorkerId is not { Length: > 0 and <= 128 } ||
                claim.MaxTasks != 1 ||
                request.Capacity is < 1 or > 64 ||
                request.InitialCredits < 0 || request.InitialCredits > request.Capacity ||
                claim.MatchingPartition is null || claim.MatchingPartition < 0 ||
                claim.MatchingPartition >= ownership.PartitionCount ||
                claim.MatchingPartitionCount != ownership.PartitionCount ||
                claim.Hubs[0] is not { Length: > 0 and <= 80 } ||
                claim.Capabilities.Any(c => c.HubId != claim.Hubs[0] ||
                    c.TaskType is not { Length: > 0 and <= 80 } || c.Version < 1))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var hub = claim.Hubs[0];
            var shard = claim.MatchingPartition.Value;
            var forwarded = context.Request.Headers.ContainsKey("X-Wfc-Matching-Forwarded");
            try
            {
                var current = await ownership.ResolvePartitionAsync(hub, shard, ct);
                if (current is null && !forwarded &&
                    ownership.PreferredUrl(shard) != ownership.AdvertiseUrl)
                {
                    try
                    {
                        await ownership.ForwardSseToAsync(ownership.PreferredUrl(shard),
                            request, context.Request.Headers.Authorization.ToString(), context, ct);
                        return;
                    }
                    catch (HttpRequestException) { /* elect locally if preferred peer failed */ }
                }
                current ??= await ownership.AcquirePartitionAsync(hub, shard, ct);
                if (!ownership.IsLocal(current))
                {
                    if (forwarded)
                    {
                        context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                        return;
                    }
                    await ownership.ForwardSseToAsync(current.OwnerUrl, request,
                        context.Request.Headers.Authorization.ToString(), context, ct);
                    return;
                }
                if (!await ownership.StillOwnsPartitionAsync(hub, shard, ct))
                {
                    context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                    return;
                }
                context.Response.ContentType = "text/event-stream";
                context.Response.Headers.CacheControl = "no-cache, no-transform";
                context.Response.Headers["X-Accel-Buffering"] = "no";
                await context.Response.StartAsync(ct);
                await distributor.StreamAsync(request, current.Epoch,
                    token => ownership.StillOwnsPartitionEpochAsync(hub, shard, current.Epoch, token),
                    async (frame, token) =>
                    {
                        await context.Response.WriteAsync(frame, token);
                        await context.Response.Body.FlushAsync(token);
                    }, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            catch (IOException) when (ct.IsCancellationRequested) { }
            catch (Exception) when (context.Response.HasStarted)
            {
                // After SSE headers are sent an error cannot be an HTTP 503;
                // abort and let the client reconnect to the current owner.
                context.Abort();
            }
            catch (Exception)
            {
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            }
        });

        routes.MapPost("/workers/tasks:credit", async (
            HttpContext context, HubSseTaskDistributor.Credit credit,
            HubMatchingOwnership ownership, HubSseTaskDistributor distributor,
            CancellationToken ct) =>
        {
            if (!ownership.Sharded ||
                credit.SessionId == Guid.Empty || credit.CreditId == Guid.Empty ||
                credit.WorkerId is not { Length: > 0 and <= 128 } ||
                credit.HubId is not { Length: > 0 and <= 80 } ||
                credit.OwnerEpoch < 1 ||
                credit.MatchingPartition < 0 ||
                credit.MatchingPartition >= ownership.PartitionCount)
                return Results.BadRequest();
            try
            {
                var current = await ownership.ResolvePartitionAsync(
                    credit.HubId, credit.MatchingPartition, ct);
                if (current is null) return Results.StatusCode(503);
                // Never accept credits from a prior owner generation after
                // failover. The worker must reconnect with a new SSE session.
                if (credit.OwnerEpoch != current.Epoch) return Results.Conflict();
                if (!ownership.IsLocal(current))
                {
                    if (context.Request.Headers.ContainsKey("X-Wfc-Matching-Forwarded"))
                        return Results.StatusCode(503);
                    var forwarded = await ownership.ForwardSseCreditToAsync(current.OwnerUrl,
                        credit, context.Request.Headers.Authorization.ToString(), ct);
                    return forwarded ? Results.NoContent() : Results.NotFound();
                }
                if (!await ownership.StillOwnsPartitionEpochAsync(
                    credit.HubId, credit.MatchingPartition, credit.OwnerEpoch, ct))
                    return Results.Conflict();
                return distributor.Offer(credit)
                    ? Results.NoContent() : Results.Conflict();
            }
            catch (Exception) when (!ct.IsCancellationRequested)
            {
                return Results.StatusCode(503);
            }
        });
        return routes;
    }
}
