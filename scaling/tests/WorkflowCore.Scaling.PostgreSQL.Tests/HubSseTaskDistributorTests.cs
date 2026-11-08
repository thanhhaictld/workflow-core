using System.Text.Json;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Unit contract for server-originated leased-task SSE (no external DB).
/// Existing PostgreSQL integration tests cover FIFO, claim epochs and permits.
/// </summary>
public sealed class HubSseTaskDistributorTests
{
    private static HubSseTaskDistributor.Subscribe Subscribe(int credits = 1,
        string worker = "worker-a") =>
        new(new HubClaimRequest(worker, ["test-hub"],
            [new HubCapability("test-hub", "test-activity", 1)],
            MaxTasks: 1, MatchingPartition: 0, MatchingPartitionCount: 4),
            Capacity: 1, InitialCredits: credits);

    private static HubLeasedTask Lease() =>
        new(Guid.NewGuid(), "test-hub", 0, "test-activity", 1, null,
            JsonDocument.Parse("{}").RootElement.Clone(), 1, 1,
            DateTime.UtcNow.AddSeconds(45));

    [Fact]
    public async Task AReadyTaskIsPushedAndSecondTaskWaitsForReturnedCredit()
    {
        var first = Lease();
        var second = Lease();
        var attempts = 0;
        var sessionReady = new TaskCompletionSource<Guid>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondReady = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = new List<Guid>();
        var distributor = new HubSseTaskDistributor((req, ct) =>
        {
            var n = Interlocked.Increment(ref attempts);
            return Task.FromResult<IReadOnlyList<HubLeasedTask>>(n switch
            {
                1 => [first],
                2 => [second],
                _ => []
            });
        });
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var stream = Task.Run(async () =>
        {
            try
            {
                await distributor.StreamAsync(Subscribe(), (frame, _) =>
                {
                    if (frame.StartsWith("event: session", StringComparison.Ordinal))
                    {
                        using var data = JsonDocument.Parse(frame.Split("data: ")[1].Trim());
                        sessionReady.TrySetResult(data.RootElement.GetProperty("sessionId").GetGuid());
                    }
                    if (frame.StartsWith("event: task.leased", StringComparison.Ordinal))
                    {
                        using var data = JsonDocument.Parse(frame.Split("data: ")[1].Trim());
                        lock (delivered) delivered.Add(data.RootElement.GetProperty("id").GetGuid());
                        if (data.RootElement.GetProperty("id").GetGuid() == second.Id)
                            secondReady.TrySetResult(true);
                    }
                    return Task.CompletedTask;
                }, stop.Token);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        });
        var sessionId = await sessionReady.Task.WaitAsync(stop.Token);
        for (var i = 0; i < 50 && Volatile.Read(ref attempts) == 0; i++)
            await Task.Delay(10, stop.Token);
        await Task.Delay(40, stop.Token);
        Assert.Equal(1, Volatile.Read(ref attempts));
        Assert.False(secondReady.Task.IsCompleted);

        var returned = new HubSseTaskDistributor.Credit(sessionId, "worker-a",
            "test-hub", 0, first.Id, first.Epoch);
        Assert.True(distributor.Replenish(returned));
        Assert.True(distributor.Replenish(returned)); // retry must not double-credit
        await secondReady.Task.WaitAsync(stop.Token);
        Assert.Equal(2, Volatile.Read(ref attempts));
        lock (delivered) Assert.Equal(new[] { first.Id, second.Id }, delivered.ToArray());

        Assert.False(distributor.Replenish(returned with { TaskId = Guid.NewGuid() }));
        await stop.CancelAsync();
        await stream;
        Assert.Equal(0, distributor.ActiveSessions);
    }

    [Fact]
    public async Task StreamWithZeroInitialCreditsStaysIdleUntilCreditReturns()
    {
        var attempts = 0;
        var distributor = new HubSseTaskDistributor((_, _) =>
        {
            Interlocked.Increment(ref attempts);
            return Task.FromResult<IReadOnlyList<HubLeasedTask>>([]);
        });
        using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(120));
        try { await distributor.StreamAsync(Subscribe(credits: 0), (_, _) =>
            Task.CompletedTask, stop.Token); }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        Assert.Equal(0, attempts);
        Assert.Equal(0, distributor.ActiveSessions);
    }

    [Fact]
    public async Task RejectsInvalidCapacityRegistration()
    {
        var distributor = new HubSseTaskDistributor((_, _) =>
            Task.FromResult<IReadOnlyList<HubLeasedTask>>([]));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            distributor.StreamAsync(Subscribe(credits: 2), (_, _) =>
                Task.CompletedTask, CancellationToken.None));
        Assert.Equal(0, distributor.ActiveSessions);
    }
}
