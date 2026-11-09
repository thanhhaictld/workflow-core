using System.Text.Json;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubSseTaskDistributorTests
{
    private static HubSseTaskDistributor.Subscribe Subscribe(int capacity = 2,
        string worker = "worker-a", string type = "test-activity") =>
        new(new HubClaimRequest(worker, ["test-hub"],
            [new HubCapability("test-hub", type, 1)],
            MaxTasks: 1, MatchingPartition: 0, MatchingPartitionCount: 4),
            capacity, InitialCredits: 0);

    private static HubLeasedTask Lease() =>
        new(Guid.NewGuid(), "test-hub", 0, "test-activity", 1, null,
            JsonDocument.Parse("{}").RootElement.Clone(), 1, 1,
            DateTime.UtcNow.AddSeconds(45));

    [Fact]
    public async Task QueuedCreditsAreClaimedTogetherAndEachGetsOneResult()
    {
        var ready = new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var frames = new List<(Guid Credit, bool Leased)>();
        var lease = Lease();
        var batchSize = 0;
        var distributor = new HubSseTaskDistributor((request, _) =>
        {
            batchSize = request.MaxTasks;
            return Task.FromResult<IReadOnlyList<HubLeasedTask>>([lease]);
        });
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var stream = Task.Run(async () =>
        {
            try
            {
                await distributor.StreamAsync(Subscribe(capacity: 3, type: "__native_workflow__"), 5, _ => Task.FromResult(true),
                    async (frame, _) =>
                    {
                        using var data = ParseData(frame);
                        if (frame.StartsWith("event: session", StringComparison.Ordinal))
                        {
                            ready.TrySetResult(data.RootElement.GetProperty("sessionId").GetGuid());
                            await resume.Task;
                            return;
                        }
                        frames.Add((data.RootElement.GetProperty("creditId").GetGuid(),
                            frame.StartsWith("event: task.leased", StringComparison.Ordinal)));
                        if (frames.Count == 3) done.TrySetResult();
                    }, stop.Token);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        });
        var session = await ready.Task.WaitAsync(stop.Token);
        var credits = Enumerable.Range(0, 3).Select(_ => Guid.NewGuid()).ToArray();
        foreach (var credit in credits)
            Assert.True(distributor.Offer(new(session, 5, "worker-a", "test-hub", 0, credit)));
        Assert.False(distributor.Offer(new(session, 5, "worker-a", "test-hub", 0, Guid.NewGuid())));
        resume.TrySetResult();
        await done.Task.WaitAsync(stop.Token);
        Assert.Equal(3, batchSize);
        Assert.Equal(credits, frames.Select(f => f.Credit));
        Assert.Equal(new[] { true, false, false }, frames.Select(f => f.Leased));
        await stop.CancelAsync();
        await stream;
    }

    [Fact]
    public async Task ActivityCreditsRemainSingleClaimEvenWhenQueued()
    {
        var ready = new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sizes = new List<int>();
        var returned = 0;
        var distributor = new HubSseTaskDistributor((request, _) =>
        {
            sizes.Add(request.MaxTasks);
            return Task.FromResult<IReadOnlyList<HubLeasedTask>>([]);
        });
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var stream = distributor.StreamAsync(Subscribe(capacity: 2), 5, _ => Task.FromResult(true),
            async (frame, _) =>
            {
                if (frame.StartsWith("event: session", StringComparison.Ordinal))
                {
                    using var data = ParseData(frame);
                    ready.TrySetResult(data.RootElement.GetProperty("sessionId").GetGuid());
                    await resume.Task;
                }
                else if (frame.StartsWith("event: credit.unused", StringComparison.Ordinal) &&
                    Interlocked.Increment(ref returned) == 2)
                    done.TrySetResult();
            }, stop.Token);
        var session = await ready.Task.WaitAsync(stop.Token);
        Assert.True(distributor.Offer(new(session, 5, "worker-a", "test-hub", 0, Guid.NewGuid())));
        Assert.True(distributor.Offer(new(session, 5, "worker-a", "test-hub", 0, Guid.NewGuid())));
        resume.TrySetResult();
        await done.Task.WaitAsync(stop.Token);
        Assert.Equal(new[] { 1, 1 }, sizes);
        await stop.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stream);
    }

    [Fact]
    public async Task OwnerLossDuringBatchDoesNotDeliverRemainingLeases()
    {
        var ready = new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var distributor = new HubSseTaskDistributor((request, _) =>
            Task.FromResult<IReadOnlyList<HubLeasedTask>>(Enumerable.Range(0, request.MaxTasks)
                .Select(_ => Lease()).ToArray()));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var stream = distributor.StreamAsync(Subscribe(capacity: 2, type: "__native_workflow__"), 5,
            _ => Task.FromResult(Volatile.Read(ref delivered) == 0),
            async (frame, _) =>
            {
                if (frame.StartsWith("event: session", StringComparison.Ordinal))
                {
                    using var data = ParseData(frame);
                    ready.TrySetResult(data.RootElement.GetProperty("sessionId").GetGuid());
                    await resume.Task;
                }
                else if (frame.StartsWith("event: task.leased", StringComparison.Ordinal))
                    Interlocked.Increment(ref delivered);
            }, timeout.Token);
        var session = await ready.Task.WaitAsync(timeout.Token);
        Assert.True(distributor.Offer(new(session, 5, "worker-a", "test-hub", 0, Guid.NewGuid())));
        Assert.True(distributor.Offer(new(session, 5, "worker-a", "test-hub", 0, Guid.NewGuid())));
        resume.TrySetResult();
        await Assert.ThrowsAsync<InvalidOperationException>(() => stream.WaitAsync(timeout.Token));
        Assert.Equal(1, delivered);
    }

    [Fact]
    public async Task OneShotGrantPushesExactlyOneLeaseOrReturnsUnusedGrant()
    {
        var first = Lease();
        var attempts = 0;
        var sessionReady = new TaskCompletionSource<Guid>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = new TaskCompletionSource<Guid>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var returnedUnused = new TaskCompletionSource<Guid>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var distributor = new HubSseTaskDistributor((_, _) =>
        {
            var next = Interlocked.Increment(ref attempts);
            return Task.FromResult<IReadOnlyList<HubLeasedTask>>(
                next == 1 ? [first] : []);
        });
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var streaming = Task.Run(async () =>
        {
            try
            {
                await distributor.StreamAsync(Subscribe(), 5,
                    _ => Task.FromResult(true),
                    (frame, _) =>
                    {
                        if (frame.StartsWith("event: session", StringComparison.Ordinal))
                        {
                            using var data = ParseData(frame);
                            Assert.Equal(5, data.RootElement.GetProperty("ownerEpoch").GetInt64());
                            sessionReady.TrySetResult(
                                data.RootElement.GetProperty("sessionId").GetGuid());
                        }
                        if (frame.StartsWith("event: task.leased", StringComparison.Ordinal))
                        {
                            using var data = ParseData(frame);
                            Assert.Equal(first.Id,
                                data.RootElement.GetProperty("task").GetProperty("id").GetGuid());
                            delivered.TrySetResult(
                                data.RootElement.GetProperty("creditId").GetGuid());
                        }
                        if (frame.StartsWith("event: credit.unused", StringComparison.Ordinal))
                        {
                            using var data = ParseData(frame);
                            returnedUnused.TrySetResult(
                                data.RootElement.GetProperty("creditId").GetGuid());
                        }
                        return Task.CompletedTask;
                    }, stop.Token);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        });
        var session = await sessionReady.Task.WaitAsync(stop.Token);
        var credit1 = Guid.NewGuid();
        Assert.True(distributor.Offer(new(session, 5, "worker-a", "test-hub", 0, credit1)));
        Assert.True(distributor.Offer(new(session, 5, "worker-a", "test-hub", 0, credit1)));
        Assert.Equal(credit1, await delivered.Task.WaitAsync(stop.Token));
        var credit2 = Guid.NewGuid();
        Assert.True(distributor.Offer(new(session, 5, "worker-a", "test-hub", 0, credit2)));
        Assert.Equal(credit2, await returnedUnused.Task.WaitAsync(stop.Token));
        Assert.Equal(2, Volatile.Read(ref attempts));
        Assert.False(distributor.Offer(new(session, 4, "worker-a", "test-hub", 0, Guid.NewGuid())));
        Assert.False(distributor.Offer(new(session, 5, "wrong-worker", "test-hub", 0, Guid.NewGuid())));
        await stop.CancelAsync();
        await streaming;
        Assert.Equal(0, distributor.ActiveSessions);
    }

    [Fact]
    public async Task OwnerLossClosesZeroCreditStreamWithoutWaitingForTask()
    {
        var attempts = 0;
        var sessionReady = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var distributor = new HubSseTaskDistributor((_, _) =>
        {
            Interlocked.Increment(ref attempts);
            return Task.FromResult<IReadOnlyList<HubLeasedTask>>([]);
        });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var streaming = distributor.StreamAsync(Subscribe(capacity: 1), 12,
            _ => Task.FromResult(false), (frame, _) =>
            {
                if (frame.StartsWith("event: session", StringComparison.Ordinal))
                    sessionReady.TrySetResult(true);
                return Task.CompletedTask;
            }, timeout.Token);
        await sessionReady.Task.WaitAsync(timeout.Token);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await streaming.WaitAsync(timeout.Token));
        Assert.Equal(0, attempts); // no credits, no speculative claims
        Assert.Equal(0, distributor.ActiveSessions);
    }

    [Fact]
    public async Task NewGenerationRevokesOldSessionAndRejectsStaleCredit()
    {
        var ids = new List<Guid>();
        var ready = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var distributor = new HubSseTaskDistributor((_, _) =>
            Task.FromResult<IReadOnlyList<HubLeasedTask>>([]));
        using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(8));

        async Task ConnectAsync(Action<Guid> capture)
        {
            try
            {
                await distributor.StreamAsync(Subscribe(capacity: 1), 33,
                    _ => Task.FromResult(true), (frame, _) =>
                    {
                        if (frame.StartsWith("event: session", StringComparison.Ordinal))
                        {
                            using var data = ParseData(frame);
                            capture(data.RootElement.GetProperty("sessionId").GetGuid());
                        }
                        return Task.CompletedTask;
                    }, cancel.Token);
            }
            catch (OperationCanceledException) { }
        }

        var firstReady = new TaskCompletionSource<Guid>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondReady = new TaskCompletionSource<Guid>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var first = ConnectAsync(id => firstReady.TrySetResult(id));
        var id1 = await firstReady.Task.WaitAsync(cancel.Token);
        var second = ConnectAsync(id => secondReady.TrySetResult(id));
        var id2 = await secondReady.Task.WaitAsync(cancel.Token);
        Assert.NotEqual(id1, id2);
        Assert.False(distributor.Offer(new(id1, 33, "worker-a", "test-hub", 0, Guid.NewGuid())));
        Assert.True(distributor.Offer(new(id2, 33, "worker-a", "test-hub", 0, Guid.NewGuid())));
        await cancel.CancelAsync();
        await Task.WhenAll(first, second);
        Assert.Equal(0, distributor.ActiveSessions);
    }

    [Fact]
    public async Task OwnershipLossClosesStreamAfterCreditWasConsumedByRunningTask()
    {
        // The server has no free worker credits once it delivered the task.
        // It must still notice loss of the matching epoch, and stop pushing.
        var ownershipValid = 1;
        var leased = Lease();
        var sessionReady = new TaskCompletionSource<Guid>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var taskDelivered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var distributor = new HubSseTaskDistributor((_, _) =>
            Task.FromResult<IReadOnlyList<HubLeasedTask>>([leased]));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var streaming = distributor.StreamAsync(Subscribe(capacity: 1), 40,
            _ => Task.FromResult(Volatile.Read(ref ownershipValid) == 1),
            (frame, _) =>
            {
                if (frame.StartsWith("event: session", StringComparison.Ordinal))
                {
                    using var data = ParseData(frame);
                    sessionReady.TrySetResult(
                        data.RootElement.GetProperty("sessionId").GetGuid());
                }
                if (frame.StartsWith("event: task.leased", StringComparison.Ordinal))
                    taskDelivered.TrySetResult(true);
                return Task.CompletedTask;
            }, timeout.Token);
        var session = await sessionReady.Task.WaitAsync(timeout.Token);
        Assert.True(distributor.Offer(new(session, 40,
            "worker-a", "test-hub", 0, Guid.NewGuid())));
        await taskDelivered.Task.WaitAsync(timeout.Token);
        Interlocked.Exchange(ref ownershipValid, 0);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await streaming.WaitAsync(timeout.Token));
        Assert.Equal(0, distributor.ActiveSessions);
    }

    [Fact]
    public async Task InvalidInitialCreditsAndEpochAreRejected()
    {
        var distributor = new HubSseTaskDistributor((_, _) =>
            Task.FromResult<IReadOnlyList<HubLeasedTask>>([]));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            distributor.StreamAsync(Subscribe() with { InitialCredits = 1 }, 5,
                _ => Task.FromResult(true), (_, _) => Task.CompletedTask,
                CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            distributor.StreamAsync(Subscribe(), 0,
                _ => Task.FromResult(true), (_, _) => Task.CompletedTask,
                CancellationToken.None));
    }

    private static JsonDocument ParseData(string frame) =>
        JsonDocument.Parse(frame.Split("data: ", StringSplitOptions.None)[1].Trim());
}
