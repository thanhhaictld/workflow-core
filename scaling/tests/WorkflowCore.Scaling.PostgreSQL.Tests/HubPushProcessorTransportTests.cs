using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkflowCore.Hub;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubPushProcessorTransportTests
{
    private const string Push = "/api/hub/workers/multiplexed-push";
    private const string Token = "processor-test-with-at-least-24-characters";

    [Fact]
    public async Task SparseCreditsVisitEveryShardAndDoubleActivityThroughSameOwnerConnections()
    {
        await using var a = new Owner("a");
        await using var b = new Owner("b");
        var moved = false;
        Owner CurrentOwner(int shard) => !Volatile.Read(ref moved) && shard % 2 == 1 ? b : a;
        a.App.MapGet(Push + "/topology", (string hub) => Results.Ok(Enumerable.Range(0, 4)
            .Select(shard =>
            {
                var owner = CurrentOwner(shard);
                return new HubPushShardLocation(shard, owner.Url, new(hub, shard, owner.Name, owner.Url,
                    owner == a && shard % 2 == 1 ? 2 : 1, DateTime.UtcNow.AddSeconds(8)));
            })));
        var busy = false;
        JsonElement input = default;
        HubLeasedTask[] Assign(Owner owner, HubPushGrant grant) =>
            Volatile.Read(ref busy) && owner == CurrentOwner(grant.Partition) &&
            grant is { Kind: HubPushTaskKind.Activity, Partition: 3, HubId: "work" }
                ? Enumerable.Range(0, grant.Credits).Select(_ => new HubLeasedTask(Guid.NewGuid(), grant.HubId,
                    grant.Partition, "count", 2, Guid.NewGuid(), input, 1, 1, DateTime.UtcNow.AddSeconds(30))).ToArray()
                : [];
        a.Assign = grant => Assign(a, grant);
        b.Assign = grant => Assign(b, grant);

        var active = 0;
        var unblock = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        a.App.MapPost("/api/hub/tasks/{id}/complete", async (HttpContext context) =>
        {
            Interlocked.Increment(ref active);
            try { await unblock.Task.WaitAsync(context.RequestAborted); }
            finally { Interlocked.Decrement(ref active); }
            return Results.NoContent();
        });
        await a.App.StartAsync();
        await b.App.StartAsync();

        foreach (var capacity in new[] { 2, 4 })
        {
            using var host = Processor(a.Url, [a.Url, b.Url], capacity, includeWorkflows: true);
            var codec = host.Services.GetRequiredService<HubSnapshotCodec>();
            input = JsonSerializer.SerializeToElement(new
            {
                __hubContext = new { dispatchKey = "stable", workflowId = Guid.NewGuid(), definitionId = "flow",
                    version = 1, pointer = codec.Encode(new ExecutionPointer { Id = "step", StepId = 0 }) },
                __hubValues = codec.EncodeValue(new Dictionary<string, object?>())
            });
            a.Offers.Clear();
            b.Offers.Clear();
            unblock = new(TaskCreationOptions.RunContinuationsAsynchronously);
            await host.StartAsync();
            try
            {
                await Until(() => a.Offers.Concat(b.Offers)
                    .Select(g => (g.HubId, g.Partition, g.Kind)).Distinct().Count() == 12);
                Assert.Equal(1, a.Connected);
                Assert.Equal(1, b.Connected);
                Assert.Equal(capacity, a.LastRegistration!.ActivitySlots);
                Assert.Equal(1, a.LastRegistration.WorkflowSlots);
                var aSessions = a.StreamCount;
                Volatile.Write(ref busy, true);
                await Until(() => Volatile.Read(ref active) >= capacity);
                await Task.Delay(300);
                Assert.Equal(capacity, Volatile.Read(ref active));
                Assert.Equal(1, a.Connected);
                Assert.Equal(1, b.Connected);
                if (capacity == 4)
                {
                    Volatile.Write(ref moved, true);
                    unblock.TrySetResult();
                    await Until(() => b.Connected == 0 && a.Offers.Any(g => g.Partition == 3 && g.OwnerEpoch == 2));
                    Assert.Equal(1, a.Connected);
                    Assert.Equal(aSessions, a.StreamCount);
                }
            }
            finally
            {
                Volatile.Write(ref busy, false);
                unblock.TrySetResult();
                await host.StopAsync();
                await Until(() => a.Connected == 0 && b.Connected == 0 && Volatile.Read(ref active) == 0);
            }
        }
        Assert.Equal(1, a.PeakConnected);
        Assert.Equal(1, b.PeakConnected);
    }

    [Fact]
    public async Task WorkerRestartUsesFreshIncarnationWithoutRecoveringDiskState()
    {
        await using var owner = new Owner("a");
        owner.App.MapGet(Push + "/topology", (string hub) => Results.Ok(Enumerable.Range(0, 4)
            .Select(shard => new HubPushShardLocation(shard, owner.Url,
                new(hub, shard, owner.Name, owner.Url, 1, DateTime.UtcNow.AddSeconds(8))))));
        // Emulate the lost-SSE-response window: grants were accepted, but
        // the former worker cannot confirm receipt or execution.
        owner.WithholdResults = true;
        await owner.App.StartAsync();

        Guid firstProcessor;
        using (var first = Processor(owner.Url, [], 1, includeWorkflows: false))
        {
            await first.StartAsync();
            await Until(() => owner.Offers.Count >= 1);
            firstProcessor = owner.LastRegistration!.ProcessorId;
            await first.StopAsync();
        }
        await Until(() => owner.Connected == 0);
        using var restarted = Processor(owner.Url, [], 1, includeWorkflows: false);
        await restarted.StartAsync();
        try
        {
            await Until(() => owner.StreamCount == 2 && owner.Offers.Count >= 2);
            // Fresh ephemeral registration and independently offered capacity,
            // even though the previous grant's response was never observed.
            Assert.NotEqual(firstProcessor, owner.LastRegistration!.ProcessorId);
            Assert.Equal(1, Assert.Single(owner.Offers.Take(1)).Sequence);
            Assert.Equal(1, owner.Offers.Last().Sequence);
        }
        finally { await restarted.StopAsync(); }
        Assert.Equal(1, owner.PeakConnected);
    }

    [Fact]
    public async Task TwoWorkersOnSameHostDoNotShareAProcessJournalOrIdentity()
    {
        await using var owner = new Owner("a");
        owner.App.MapGet(Push + "/topology", (string hub) => Results.Ok(Enumerable.Range(0, 4)
            .Select(shard => new HubPushShardLocation(shard, owner.Url,
                new(hub, shard, owner.Name, owner.Url, 1, DateTime.UtcNow.AddSeconds(8))))));
        owner.WithholdResults = true;
        await owner.App.StartAsync();
        using var first = Processor(owner.Url, [], 1, includeWorkflows: false);
        using var second = Processor(owner.Url, [], 1, includeWorkflows: false);
        await first.StartAsync();
        await second.StartAsync();
        try
        {
            await Until(() => owner.Connected == 2 && owner.Registrations.Count >= 2);
            var ids = owner.Registrations.Select(r => r.ProcessorId).ToArray();
            Assert.Equal(2, ids.Distinct().Count());
            Assert.Equal(2, owner.PeakConnected);
        }
        finally
        {
            await second.StopAsync();
            await first.StopAsync();
        }
        await Until(() => owner.Connected == 0);
    }

    public sealed class TestFlow : IWorkflow<object>
    {
        public string Id => "flow";
        public int Version => 1;
        public void Build(IWorkflowBuilder<object> builder) => builder.StartWith(_ => ExecutionResult.Next());
    }

    private static IHost Processor(string url, string[] trusted, int slots, bool includeWorkflows) =>
        new HostBuilder().ConfigureServices(services =>
        {
            services.AddLogging();
            services.AddSingleton<HubNativeTransportTests.Effects>();
            var registrations = services.AddHubWorkflowRuntime(options =>
            {
                options.HubUrl = url;
                options.Token = Token;
                options.PushEnabled = true;
                options.PushTrustedOwners = trusted;
                options.MatchingPartitions = 4;
                options.ActivityConcurrency = slots;
                options.WorkflowConcurrency = 1;
            }).AddActivity<HubNativeTransportTests.CountEffect>("work", "count", 2, o => o.ActivityResult(s => s.Result));
            if (includeWorkflows)
            {
                registrations.AddWorkflow<TestFlow, object>("work");
                registrations.AddActivity<HubNativeTransportTests.CountEffect>("second/team", "count", 2, o => o.ActivityResult(s => s.Result));
            }
        }).Build();

    private static async Task Until(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(12));
        while (!condition()) await Task.Delay(25, timeout.Token);
    }

    private sealed class Owner : IAsyncDisposable
    {
        private readonly ConcurrentDictionary<Guid, Channel<HubPushGrant>> _sessions = new();
        private readonly Guid _instance = Guid.NewGuid();
        private int _connected, _streams, _peak;
        public WebApplication App { get; }
        public string Name { get; }
        public string Url => App.Urls.Single();
        public int Connected => Volatile.Read(ref _connected);
        public int StreamCount => Volatile.Read(ref _streams);
        public int PeakConnected => Volatile.Read(ref _peak);
        public HubPushProcessorRegistration? LastRegistration { get; private set; }
        public ConcurrentQueue<HubPushProcessorRegistration> Registrations { get; } = new();
        public ConcurrentQueue<HubPushGrant> Offers { get; } = new();
        public Func<HubPushGrant, HubLeasedTask[]> Assign { get; set; } = _ => [];
        public bool WithholdResults { get; set; }

        public Owner(string name)
        {
            Name = name;
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            App = builder.Build();
            App.MapPost("/api/hub/activity-registrations", () => Results.NoContent());
            App.MapPost("/api/hub/definitions", () => Results.NoContent());
            App.MapPost(Push + "/sessions", (HubPushProcessorRegistration registration) =>
            {
                LastRegistration = registration;
                Registrations.Enqueue(registration);
                var session = new HubPushOwnerSession(Guid.NewGuid(), _instance, Name);
                _sessions[session.SessionId] = Channel.CreateUnbounded<HubPushGrant>();
                return Results.Ok(session);
            });
            App.MapPut(Push + "/sessions/{id}/grants", (Guid id, HubPushGrant[] grants) =>
            {
                foreach (var grant in grants)
                {
                    Offers.Enqueue(grant);
                    _sessions[id].Writer.TryWrite(grant);
                }
                return Results.NoContent();
            });
            App.MapDelete(Push + "/sessions/{id}", (Guid id) =>
            {
                if (_sessions.TryRemove(id, out var grants)) grants.Writer.TryComplete();
                return Results.Accepted();
            });
            App.MapGet(Push + "/sessions/{id}/events", async (Guid id, HttpContext context) =>
            {
                var count = Interlocked.Increment(ref _connected);
                if (count > PeakConnected) Interlocked.Exchange(ref _peak, count);
                Interlocked.Increment(ref _streams);
                try
                {
                    context.Response.ContentType = "text/event-stream";
                    await context.Response.WriteAsync(": connected\n\n", context.RequestAborted);
                    await context.Response.Body.FlushAsync(context.RequestAborted);
                    await foreach (var grant in _sessions[id].Reader.ReadAllAsync(context.RequestAborted))
                    {
                        if (WithholdResults) continue;
                        var frame = new HubPushGrantResult(id, grant, Assign(grant));
                        await context.Response.WriteAsync($"data: {JsonSerializer.Serialize(frame)}\n\n", context.RequestAborted);
                        await context.Response.Body.FlushAsync(context.RequestAborted);
                    }
                }
                finally { Interlocked.Decrement(ref _connected); }
            });
        }

        public ValueTask DisposeAsync() => App.DisposeAsync();
    }
}
