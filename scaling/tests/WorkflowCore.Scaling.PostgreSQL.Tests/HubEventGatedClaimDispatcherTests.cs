using System.Collections.Concurrent;
using System.Text.Json;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// In-memory matcher tests do not require PostgreSQL. Durable admission and
/// FIFO/epoch rules remain covered by existing PostgreSQL integration tests.
/// </summary>
public sealed class HubEventGatedClaimDispatcherTests
{
    private static HubClaimRequest Request(string worker, int partition,
        string activity = "do-work") =>
        new(worker, ["test-hub"], [new HubCapability("test-hub", activity, 1)],
            MaxTasks: 4, MatchingPartition: partition, MatchingPartitionCount: 4);

    private static HubLeasedTask Lease(int partition) =>
        new(Guid.NewGuid(), "test-hub", partition, "do-work", 1, null,
            JsonDocument.Parse("{}").RootElement.Clone(), 1, 1,
            DateTime.UtcNow.AddSeconds(45));

    [Fact]
    public async Task HundredsOfReadyHintsCoalesceIntoOneShardClaimDrain()
    {
        var attempts = 0;
        var active = 0;
        var maxActive = 0;
        var dispatcher = new HubEventGatedClaimDispatcher(
            async (req, ct) =>
            {
                var current = Interlocked.Increment(ref active);
                var seen = Volatile.Read(ref maxActive);
                while (current > seen &&
                       Interlocked.CompareExchange(ref maxActive, current, seen) != seen)
                    seen = Volatile.Read(ref maxActive);
                try
                {
                    await Task.Delay(20, ct);
                    var claim = Interlocked.Increment(ref attempts);
                    return claim <= 6
                        ? (IReadOnlyList<HubLeasedTask>)[Lease(req.MatchingPartition!.Value)]
                        : Array.Empty<HubLeasedTask>();
                }
                finally { Interlocked.Decrement(ref active); }
            },
            (_, _) => Task.FromResult(true),
            sharded: true, partitionCount: 4,
            pollTimeout: TimeSpan.FromMilliseconds(600));

        var polls = Enumerable.Range(0, 12).Select(i =>
            dispatcher.LongPollAsync(Request("worker-" + i, 0), CancellationToken.None)).ToArray();
        for (var i = 0; i < 300; i++)
            dispatcher.NotifyReady("test-hub", "do-work", 1, 0);
        var results = await Task.WhenAll(polls);
        Assert.Equal(6, results.Sum(r => r.Count));
        Assert.Equal(1, maxActive);
        Assert.InRange(attempts, 7, 8); // six useful batches, then one empty drain
        Assert.Equal(0, dispatcher.PendingWaiters);
    }

    [Fact]
    public async Task TypedHintWakesOnlyItsMatchingShardAndCapability()
    {
        var attempts = new ConcurrentBag<(int Partition, string Type)>();
        var dispatcher = new HubEventGatedClaimDispatcher(
            (req, ct) =>
            {
                attempts.Add((req.MatchingPartition!.Value, req.Capabilities[0].TaskType));
                return Task.FromResult<IReadOnlyList<HubLeasedTask>>([]);
            },
            (_, _) => Task.FromResult(true),
            sharded: true, partitionCount: 4,
            pollTimeout: TimeSpan.FromMilliseconds(180));

        // Warm the bounded catch-up probe for each capability lane.
        await Task.WhenAll(
            dispatcher.LongPollAsync(Request("warm-0", 0), CancellationToken.None),
            dispatcher.LongPollAsync(Request("warm-1", 1), CancellationToken.None));
        var baselineZero = attempts.Count(x => x.Partition == 0);
        var baselineOne = attempts.Count(x => x.Partition == 1);
        var polls = new[] {
            dispatcher.LongPollAsync(Request("worker-0", 0), CancellationToken.None),
            dispatcher.LongPollAsync(Request("worker-1", 1), CancellationToken.None)
        };
        dispatcher.NotifyReady("test-hub", "do-work", 1, 0);
        await Task.WhenAll(polls);
        Assert.Equal(baselineZero + 1, attempts.Count(x => x.Partition == 0));
        Assert.Equal(baselineOne, attempts.Count(x => x.Partition == 1));
        Assert.Equal(0, dispatcher.PendingWaiters);
    }

    [Fact]
    public async Task ReadyHintSurvivesUntilNextWorkerRotatesIntoShard()
    {
        var count = 0;
        var dispatcher = new HubEventGatedClaimDispatcher(
            (req, ct) =>
            {
                var number = Interlocked.Increment(ref count);
                return Task.FromResult<IReadOnlyList<HubLeasedTask>>(
                    number == 2 ? [Lease(2)] : []);
            },
            (_, _) => Task.FromResult(true),
            sharded: true, partitionCount: 4,
            pollTimeout: TimeSpan.FromMilliseconds(140));

        // First poll probes SQL and finds no task. It leaves no waiters.
        Assert.Empty(await dispatcher.LongPollAsync(
            Request("first-worker", 2), CancellationToken.None));
        Assert.Equal(1, count);

        // A committed notification may arrive while every worker is polling
        // another matching shard. It must not be forgotten.
        dispatcher.NotifyReady("test-hub", "do-work", 1, 2);
        Assert.Single(await dispatcher.LongPollAsync(
            Request("rotated-worker", 2), CancellationToken.None));
        Assert.Equal(2, count);
        Assert.Equal(0, dispatcher.PendingWaiters);
    }

    [Fact]
    public async Task SuccessfulDrainHintSurvivesTheLastWaiterDisconnecting()
    {
        var count = 0;
        var dispatcher = new HubEventGatedClaimDispatcher(
            (req, ct) =>
            {
                Interlocked.Increment(ref count);
                return Task.FromResult<IReadOnlyList<HubLeasedTask>>([Lease(3)]);
            },
            (_, _) => Task.FromResult(true),
            sharded: true, partitionCount: 4,
            pollTimeout: TimeSpan.FromMilliseconds(180));

        // One worker receives a batch and immediately leaves the shard;
        // there is no other waiter to receive its follow-up drain signal.
        Assert.Single(await dispatcher.LongPollAsync(
            Request("worker-a", 3), CancellationToken.None));
        Assert.Equal(1, count);
        // A second worker arriving before its 5-second catch-up budget
        // expires must consume the retained drain token immediately.
        Assert.Single(await dispatcher.LongPollAsync(
            Request("worker-b", 3), CancellationToken.None));
        Assert.Equal(2, count);
        Assert.Equal(0, dispatcher.PendingWaiters);
    }

    [Fact]
    public async Task LostReadyHintIsRecoveredByInitialCapabilityProbe()
    {
        var called = 0;
        var dispatcher = new HubEventGatedClaimDispatcher(
            (req, ct) =>
            {
                Interlocked.Increment(ref called);
                return Task.FromResult<IReadOnlyList<HubLeasedTask>>([Lease(2)]);
            },
            (_, _) => Task.FromResult(true),
            sharded: true, partitionCount: 4,
            pollTimeout: TimeSpan.FromMilliseconds(300));

        // No waiters at notification time, so the in-memory hint can be
        // discarded. The first worker must still probe durable backlog.
        dispatcher.NotifyReady("test-hub", "do-work", 1, 2);
        var tasks = await dispatcher.LongPollAsync(Request("late-worker", 2),
            CancellationToken.None);
        Assert.Single(tasks);
        Assert.Equal(1, called);
        Assert.Equal(0, dispatcher.PendingWaiters);
    }

    [Fact]
    public async Task AdmissionDeferralRetainsHintAndRetriesWithoutAnotherNotification()
    {
        var attempts = 0;
        var dispatcher = new HubEventGatedClaimDispatcher(
            (req, ct) =>
            {
                if (Interlocked.Increment(ref attempts) == 1)
                    throw new HubClaimAdmissionDeferredException();
                return Task.FromResult<IReadOnlyList<HubLeasedTask>>([Lease(1)]);
            },
            (_, _) => Task.FromResult(true),
            sharded: true, partitionCount: 4,
            pollTimeout: TimeSpan.FromMilliseconds(600));

        var claimed = await dispatcher.LongPollAsync(Request("worker", 1),
            CancellationToken.None);
        Assert.Single(claimed);
        Assert.Equal(2, attempts);
        Assert.Equal(0, dispatcher.PendingWaiters);
    }

    [Fact]
    public async Task DeferredHintSurvivesPollTimeoutAndWorkerRotation()
    {
        var attempts = 0;
        var dispatcher = new HubEventGatedClaimDispatcher(
            (req, ct) =>
            {
                if (Interlocked.Increment(ref attempts) == 1)
                    throw new HubClaimAdmissionDeferredException();
                return Task.FromResult<IReadOnlyList<HubLeasedTask>>([Lease(2)]);
            },
            (_, _) => Task.FromResult(true),
            sharded: true, partitionCount: 4,
            pollTimeout: TimeSpan.FromMilliseconds(25));

        Assert.Empty(await dispatcher.LongPollAsync(
            Request("first-worker", 2), CancellationToken.None));
        Assert.Equal(1, attempts);

        // Initial 5-second recovery probe is suppressed for this capability.
        // Success therefore requires the first deferral to retain its hint.
        Assert.Single(await dispatcher.LongPollAsync(
            Request("rotated-worker", 2), CancellationToken.None));
        Assert.Equal(2, attempts);
        Assert.Equal(0, dispatcher.PendingWaiters);
    }


    [Fact]
    public async Task TypedHintScopesMultiCapabilityClaim_WithoutRestrictingGenericRecovery()
    {
        static HubClaimRequest MultiCapabilityRequest() =>
            new("multi-cap-worker", ["test-hub"],
                [new HubCapability("test-hub", "alpha", 1),
                 new HubCapability("test-hub", "beta", 1)],
                MaxTasks: 4, MatchingPartition: 0, MatchingPartitionCount: 4);

        static HubEventGatedClaimDispatcher Create(ConcurrentQueue<string[]> observed) =>
            new((request, _) =>
                {
                    observed.Enqueue(request.Capabilities.Select(c => c.TaskType).ToArray());
                    return Task.FromResult<IReadOnlyList<HubLeasedTask>>([]);
                },
                (_, _) => Task.FromResult(true),
                sharded: true, partitionCount: 4,
                pollTimeout: TimeSpan.FromMilliseconds(100),
                capabilityScopedClaims: true);

        var typed = new ConcurrentQueue<string[]>();
        var typedDispatcher = Create(typed);
        typedDispatcher.NotifyReady("test-hub", "beta", 1, 0);
        await typedDispatcher.LongPollAsync(MultiCapabilityRequest(), CancellationToken.None);
        Assert.True(typed.TryPeek(out var typedCapabilities));
        Assert.Equal(["beta"], typedCapabilities);

        var generic = new ConcurrentQueue<string[]>();
        var genericDispatcher = Create(generic);
        genericDispatcher.NotifyReady("test-hub", null, 0, 0);
        await genericDispatcher.LongPollAsync(MultiCapabilityRequest(), CancellationToken.None);
        Assert.True(generic.TryPeek(out var recoveryCapabilities));
        Assert.Equal(["alpha", "beta"], recoveryCapabilities);
    }

}
