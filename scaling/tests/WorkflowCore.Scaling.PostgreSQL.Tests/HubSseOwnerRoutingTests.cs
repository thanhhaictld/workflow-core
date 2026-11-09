using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>Real HTTP and configured PostgreSQL: no mocked ownership authority.</summary>
public sealed class HubSseOwnerRoutingTests
{
    private static string Database => Environment.GetEnvironmentVariable("WFC_TEST_PG")
        ?? throw new InvalidOperationException("Owner routing tests require configured WFC_TEST_PG");

    private sealed class EnvironmentScope : IDisposable
    {
        private readonly Dictionary<string, string?> _old = new();
        public void Set(string key, string value)
        {
            _old.TryAdd(key, Environment.GetEnvironmentVariable(key));
            Environment.SetEnvironmentVariable(key, value);
        }
        public void Dispose()
        {
            foreach (var (key, value) in _old) Environment.SetEnvironmentVariable(key, value);
        }
    }

    private static WebApplication App()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseSetting("urls", "http://127.0.0.1:0");
        return builder.Build();
    }

    private static async Task<WebApplication> Gateway(string peer, EnvironmentScope environment)
    {
        environment.Set("WFC_HUB_MATCHING_ENABLED", "true");
        environment.Set("WFC_HUB_MATCHING_PARTITIONS", "4");
        environment.Set("WFC_HUB_MATCHING_NODE_ID", "routing-survivor");
        environment.Set("WFC_HUB_MATCHING_ADVERTISE_URL", "http://127.0.0.1:1");
        environment.Set("WFC_HUB_MATCHING_PEERS", "http://127.0.0.1:1," + peer);
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseSetting("urls", "http://127.0.0.1:0");
        builder.Services.AddHubGateway(Database);
        var app = builder.Build();
        app.MapGroup("/api/hub").MapSseTaskStreaming();
        await app.StartAsync();
        return app;
    }

    private static HubSseTaskDistributor.Subscribe Subscribe(string hub, int shard) =>
        new(new HubClaimRequest("routing-worker", [hub], [new(hub, "activity", 1)],
            MaxTasks: 1, MatchingPartition: shard, MatchingPartitionCount: 4), 1, 0);

    private static async Task<(HttpResponseMessage Response, long Epoch)> Open(
        HttpClient client, string url, HubSseTaskDistributor.Subscribe subscribe, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, url + "/api/hub/workers/tasks:stream")
            { Content = JsonContent.Create(subscribe) };
        var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        // Do not close the response body here: the caller owns the session.
        var reader = new StreamReader(await response.Content.ReadAsStreamAsync(ct));
        Assert.Equal("event: session", await reader.ReadLineAsync(ct));
        var line = await reader.ReadLineAsync(ct);
        using var data = JsonDocument.Parse(line!["data: ".Length..]);
        return (response, data.RootElement.GetProperty("ownerEpoch").GetInt64());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedPreferredPeerDoesNotStrandExpiredShard(bool slowHeaders)
    {
        await using var peer = App();
        peer.MapPost("/api/hub/workers/tasks:stream", async (HttpContext ctx) =>
        {
            if (slowHeaders) await Task.Delay(Timeout.InfiniteTimeSpan, ctx.RequestAborted);
            ctx.Response.StatusCode = 503;
        });
        await peer.StartAsync();
        using var environment = new EnvironmentScope();
        await using var app = await Gateway(peer.Urls.Single(), environment);
        var ownership = app.Services.GetRequiredService<HubMatchingOwnership>();
        var shard = ownership.PreferredUrl(0) == peer.Urls.Single() ? 0 : 1;
        var hub = "route-expired-" + Guid.NewGuid().ToString("N");
        var leases = app.Services.GetRequiredService<HubMatchingPartitionLeaseStore>();
        var old = await leases.AcquireOrReadAsync(hub, shard, 4, "failed-peer", peer.Urls.Single(),
            TimeSpan.FromSeconds(8), default);
        Assert.True(await leases.ReleaseAsync(hub, shard, old.OwnerId, old.Epoch, default));
        // Expire decisively using the DB clock, as the existing ownership tests
        // do. Host/VM clock skew must not make this a near-expiry routing race.
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var expire = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_matching_partition_owner
                SET lease_until=clock_timestamp()-interval '5 seconds'
                WHERE hub_id=@hub AND matching_partition=@shard
                """, db);
            expire.Parameters.AddWithValue("hub", hub);
            expire.Parameters.AddWithValue("shard", shard);
            Assert.Equal(1, await expire.ExecuteNonQueryAsync());
        }
        using var client = new HttpClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(12));
        var (response, epoch) = await Open(client, app.Urls.Single(), Subscribe(hub, shard), timeout.Token);
        using (response)
        {
            var winner = await leases.ReadAsync(hub, shard, default);
            Assert.Equal("routing-survivor", winner!.OwnerId);
            Assert.Equal(old.Epoch + 1, epoch);
            Assert.Equal(winner.Epoch, epoch);
            Assert.False(await leases.OwnsAsync(hub, shard, old.OwnerId, old.Epoch, default));
            Assert.False(await leases.RenewAsync(hub, shard, old.OwnerId, old.Epoch,
                TimeSpan.FromSeconds(8), default));
        }
    }

    [Fact]
    public async Task UnreachableLiveOwnerCannotBeStolen()
    {
        await using var peer = App();
        peer.MapPost("/api/hub/workers/tasks:stream", () => Results.StatusCode(503));
        await peer.StartAsync();
        using var environment = new EnvironmentScope();
        await using var app = await Gateway(peer.Urls.Single(), environment);
        var hub = "route-live-" + Guid.NewGuid().ToString("N");
        var leases = app.Services.GetRequiredService<HubMatchingPartitionLeaseStore>();
        var old = await leases.AcquireOrReadAsync(hub, 0, 4, "still-live", peer.Urls.Single(),
            TimeSpan.FromSeconds(8), default);
        using var client = new HttpClient();
        using var response = await client.PostAsJsonAsync(app.Urls.Single() +
            "/api/hub/workers/tasks:stream", Subscribe(hub, 0));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(old, await leases.ReadAsync(hub, 0, default));
    }

    [Fact]
    public async Task CancelledWorkerDoesNotElectAfterHeaderWait()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var peer = App();
        peer.MapPost("/api/hub/workers/tasks:stream", async (HttpContext ctx) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ctx.RequestAborted);
        });
        await peer.StartAsync();
        using var environment = new EnvironmentScope();
        await using var app = await Gateway(peer.Urls.Single(), environment);
        var ownership = app.Services.GetRequiredService<HubMatchingOwnership>();
        var shard = ownership.PreferredUrl(0) == peer.Urls.Single() ? 0 : 1;
        var hub = "route-cancel-" + Guid.NewGuid().ToString("N");
        using var client = new HttpClient();
        using var stop = new CancellationTokenSource();
        var request = client.PostAsJsonAsync(app.Urls.Single() +
            "/api/hub/workers/tasks:stream", Subscribe(hub, shard), stop.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await stop.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await request);
        // The endpoint must propagate caller cancellation, not turn it into a fallback.
        await Task.Delay(100);
        Assert.Null(await ownership.ReadPartitionAsync(hub, shard, default));
    }

    [Fact]
    public async Task ConcurrentFallbacksObserveOneGenerationAndTopologyStillRejectsMismatch()
    {
        using var environment = new EnvironmentScope();
        await using var app = await Gateway("http://127.0.0.1:2", environment);
        var ownership = app.Services.GetRequiredService<HubMatchingOwnership>();
        var hub = "route-race-" + Guid.NewGuid().ToString("N");
        var results = await Task.WhenAll(Enumerable.Range(0, 32)
            .Select(_ => ownership.AcquirePartitionAsync(hub, 2, default)));
        Assert.Single(results.Select(x => (x.OwnerId, x.Epoch)).Distinct());
        Assert.True(await ownership.StillOwnsPartitionEpochAsync(hub, 2, results[0].Epoch, default));
        Assert.False(await ownership.StillOwnsPartitionEpochAsync(hub, 2, results[0].Epoch + 1, default));
        var store = app.Services.GetRequiredService<HubMatchingPartitionLeaseStore>();
        var other = "route-topology-" + Guid.NewGuid().ToString("N");
        await store.EnsureConfigAsync(other, 8, default);
        await Assert.ThrowsAsync<InvalidOperationException>(() => ownership.AcquirePartitionAsync(other, 2, default));
    }

    [Fact]
    public async Task OwnerTurnoverPreservesExecutingTaskLeaseAndOneDurableResult()
    {
        await using var tasks = new HubTaskStore(Database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var owners = new HubMatchingPartitionLeaseStore(tasks);
        var hub = "route-executing-" + Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub, "native", 1, 4, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        await definitions.RegisterActivityAsync(new(hub, "activity", 1, 4, 10, 10), default);
        var id = await new HubRemoteWorkflowStore(tasks, definitions).StartAsync(
            new(hub, "native", 1, JsonSerializer.SerializeToElement(new { })), default);
        var workflow = Assert.Single(await tasks.ClaimAsync("wf", [hub],
            [new(hub, "__native_workflow__", 1)], 1, TimeSpan.FromSeconds(30), default));
        Assert.NotNull(await native.CommitAsync(id, new(1, "wf", workflow.Epoch, 0,
            workflow.Id, JsonSerializer.SerializeToElement(new { }), "Runnable", false, null,
            [], [], [new("dispatch", "pointer", 0, "activity", 1,
                JsonSerializer.SerializeToElement(new { }))], [], ["dispatch"]), default));
        var old = await owners.AcquireOrReadAsync(hub, workflow.PartitionId % 4, 4,
            "old-owner", "http://old:5100", TimeSpan.FromSeconds(8), default);
        var activity = Assert.Single(await tasks.ClaimAsync("executor", [hub],
            [new(hub, "activity", 1)], 1, TimeSpan.FromSeconds(30), default));
        Assert.True(await owners.ReleaseAsync(hub, old.Partition, old.OwnerId, old.Epoch, default));
        var next = await owners.AcquireOrReadAsync(hub, old.Partition, 4,
            "next-owner", "http://next:5100", TimeSpan.FromSeconds(8), default);
        Assert.Equal(old.Epoch + 1, next.Epoch);
        Assert.False(await owners.RenewAsync(hub, old.Partition, old.OwnerId, old.Epoch,
            TimeSpan.FromSeconds(8), default));
        Assert.True(await tasks.HeartbeatAsync(activity.Id, "executor", activity.Epoch,
            TimeSpan.FromSeconds(30), default));
        var result = JsonSerializer.SerializeToElement(new { businessResult = "done" });
        Assert.False(await native.CompleteActivityAsync(activity.Id, "executor", activity.Epoch + 1, result, default));
        Assert.True(await native.CompleteActivityAsync(activity.Id, "executor", activity.Epoch, result, default));
        await native.CompleteActivityAsync(activity.Id, "executor", activity.Epoch, result, default);
        var runtime = (await native.GetRuntimeAsync(id, default))!;
        Assert.Single(runtime.Inbox);
        var attempts = await new HubWorkflowStore(tasks, new HubPolicyRegistry(),
            new HubWorkflowRegistry()).TraceAsync(id, default);
        var attempt = Assert.Single(Assert.Single(attempts).Attempts);
        Assert.Equal("Completed", attempt.Outcome);
        Assert.NotNull(attempt.FinishedAt);
    }

    [Fact]
    public async Task OwnerDiesAfterClaimBeforeDeliveryKeepsUncertainExecutionLease()
    {
        await using var tasks = new HubTaskStore(Database);
        var owners = new HubMatchingPartitionLeaseStore(tasks);
        var hub = "route-undelivered-" + Guid.NewGuid().ToString("N");
        var policy = new HubPolicyRegistry().Register(hub, "activity", 1, 4,
            p => p.SequentialBy("resource", v => v.GetProperty("resourceId").GetString()));
        var payload = JsonSerializer.SerializeToElement(new { resourceId = "one" });
        var id = await tasks.EnqueueAsync(policy.Resolve(hub, "activity", 1, payload),
            payload.GetRawText(), null, null, default);
        var task = (await tasks.GetAsync(id, default))!;
        var shard = task.PartitionId % 4;
        var old = await owners.AcquireOrReadAsync(hub, shard, 4, "old-owner", "http://old:5100",
            TimeSpan.FromSeconds(8), default);
        HubLeasedTask? claimed = null;
        var distributor = new HubSseTaskDistributor(async (request, ct) =>
        {
            var leases = await tasks.ClaimAsync(request.WorkerId, request.Hubs, request.Capabilities,
                1, TimeSpan.FromSeconds(30), ct);
            claimed = Assert.Single(leases);
            Assert.True(await owners.ReleaseAsync(hub, shard, old.OwnerId, old.Epoch, ct));
            await owners.AcquireOrReadAsync(hub, shard, 4, "new-owner", "http://new:5100",
                TimeSpan.FromSeconds(8), ct);
            return leases;
        });
        var session = new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = false;
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var streaming = distributor.StreamAsync(Subscribe(hub, shard), old.Epoch,
            ct => owners.OwnsAsync(hub, shard, old.OwnerId, old.Epoch, ct), (frame, _) =>
            {
                if (frame.StartsWith("event: session"))
                {
                    using var data = JsonDocument.Parse(frame.Split("data: ")[1].Trim());
                    session.TrySetResult(data.RootElement.GetProperty("sessionId").GetGuid());
                }
                if (frame.StartsWith("event: task.leased")) delivered = true;
                return Task.CompletedTask;
            }, stop.Token);
        var sessionId = await session.Task.WaitAsync(stop.Token);
        Assert.True(distributor.Offer(new(sessionId, old.Epoch, "routing-worker", hub, shard, Guid.NewGuid())));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await streaming);
        Assert.False(delivered);
        Assert.NotNull(claimed);
        Assert.Equal("Leased", (await tasks.GetAsync(id, default))!.State);
        Assert.Empty(await tasks.ClaimAsync("another-worker", [hub], [new(hub, "activity", 1)],
            1, TimeSpan.FromSeconds(30), default));
        // Disconnect/owner turnover does not authorize recycling the lease.
        Assert.True(await tasks.HeartbeatAsync(id, "routing-worker", claimed.Epoch,
            TimeSpan.FromSeconds(30), default));
        Assert.False(distributor.Offer(new(sessionId, old.Epoch, "routing-worker", hub, shard, Guid.NewGuid())));
    }
}
