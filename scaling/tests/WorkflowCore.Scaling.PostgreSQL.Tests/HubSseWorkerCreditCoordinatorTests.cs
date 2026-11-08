using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubSseWorkerCreditCoordinatorTests
{
    [Fact]
    public void ReservationsAndRunningTasksShareOneHardGlobalLimit()
    {
        var credits = new HubSseWorkerCreditCoordinator(capacity: 4, shards: 4);
        for (var shard = 0; shard < 4; shard++)
            credits.Connected(shard, Guid.NewGuid(), 7, () => { });
        var grants = Enumerable.Range(0, 4).Select(_ =>
        {
            Assert.True(credits.TryOffer(out var grant));
            return grant;
        }).ToArray();
        Assert.Equal(new[] { 0, 1, 2, 3 }, grants.Select(x => x.Shard));
        Assert.Equal(4, credits.ReservedOrRunning);
        Assert.False(credits.TryOffer(out _));

        var task = Guid.NewGuid();
        Assert.True(credits.Dispatched(grants[0].Shard, grants[0].SessionId,
            grants[0].CreditId, task));
        Assert.Equal(4, credits.ReservedOrRunning);
        Assert.False(credits.TryOffer(out _));
        credits.Finished(task);
        Assert.Equal(3, credits.ReservedOrRunning);
        Assert.True(credits.TryOffer(out var replacement));
        Assert.Equal(0, replacement.Shard); // hot shard gets a returned slot
    }

    [Fact]
    public void IdleGrantCanBeMovedToAnActiveShard()
    {
        var credits = new HubSseWorkerCreditCoordinator(capacity: 3, shards: 2);
        var sessions = new[] { Guid.NewGuid(), Guid.NewGuid() };
        credits.Connected(0, sessions[0], 5, () => { });
        credits.Connected(1, sessions[1], 6, () => { });
        Assert.True(credits.TryOffer(out var a));
        Assert.True(credits.TryOffer(out var b));
        Assert.True(credits.TryOffer(out var c));
        var hot = new[] { a, b, c }.First(x => x.Shard == 0);
        var cold = new[] { a, b, c }.First(x => x.Shard == 1);
        var task = Guid.NewGuid();
        Assert.True(credits.Dispatched(0, hot.SessionId, hot.CreditId, task));
        Assert.True(credits.Unused(1, cold.SessionId, cold.CreditId));
        Assert.True(credits.TryOffer(out var moved));
        Assert.Equal(0, moved.Shard);
        Assert.Equal(3, credits.ReservedOrRunning);
        Assert.False(credits.TryOffer(out _));
    }

    [Fact]
    public void ReconnectReleasesOnlyUndeliveredOffersAndRejectsStaleFrames()
    {
        var credits = new HubSseWorkerCreditCoordinator(capacity: 2, shards: 2);
        var oldSession = Guid.NewGuid();
        var newSession = Guid.NewGuid();
        var invalidated = false;
        credits.Connected(0, oldSession, 10, () => invalidated = true);
        Assert.True(credits.TryOffer(out var delivered));
        var running = Guid.NewGuid();
        Assert.True(credits.Dispatched(0, oldSession, delivered.CreditId, running));
        Assert.True(credits.TryOffer(out var offered));
        Assert.Equal(2, credits.ReservedOrRunning);
        credits.Connected(0, newSession, 11, () => { });
        Assert.True(invalidated);
        Assert.Equal(1, credits.ReservedOrRunning);
        Assert.False(credits.Dispatched(0, oldSession, offered.CreditId, Guid.NewGuid()));
        Assert.False(credits.Unused(0, oldSession, offered.CreditId));
        Assert.True(credits.TryOffer(out var replacement));
        Assert.Equal(newSession, replacement.SessionId);
        Assert.Equal(11, replacement.OwnerEpoch);
        Assert.False(credits.TryOffer(out _));
        credits.Finished(running);
        Assert.Equal(1, credits.ReservedOrRunning);
    }

    [Fact]
    public void StaleSessionCannotReclaimCreditsAfterNewGeneration()
    {
        var credits = new HubSseWorkerCreditCoordinator(capacity: 1, shards: 1);
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        credits.Connected(0, first, 1, () => { });
        Assert.True(credits.TryOffer(out var offered));
        credits.Connected(0, second, 2, () => { });
        credits.Disconnected(0, first);
        Assert.True(credits.TryOffer(out var next));
        Assert.False(credits.Unused(0, first, offered.CreditId));
        Assert.Equal(second, next.SessionId);
        Assert.Equal(1, credits.ReservedOrRunning);
    }

    [Fact]
    public void CanSubscribeMoreShardsThanAvailableExecutionSlots()
    {
        var credits = new HubSseWorkerCreditCoordinator(capacity: 1, shards: 4);
        for (var i = 0; i < 4; i++)
            credits.Connected(i, Guid.NewGuid(), 3, () => { });
        Assert.True(credits.TryOffer(out _));
        Assert.False(credits.TryOffer(out _));
        Assert.Equal(1, credits.ReservedOrRunning);
    }
}
