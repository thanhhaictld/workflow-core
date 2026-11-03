using System.Text.Json;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubOperationsStoreTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");

    [Fact]
    public async Task Snapshot_SeparatesReadyLeasedAndBlockedTasks_AndPersistsTrend()
    {
        if (Database is null) return;
        var hub = "ops-" + Guid.NewGuid().ToString("N")[..12];
        await using var tasks = new HubTaskStore(Database, 4);
        var policy = new HubPolicyRegistry().Register(hub, "test-activity", 1, 4, p =>
            p.LimitBy("scope", 1, json => json.GetProperty("resource").GetString()));
        var payload = JsonSerializer.SerializeToElement(new { resource = "same" });
        var route = policy.Resolve(hub, "test-activity", 1, payload);
        await tasks.EnqueueAsync(route, payload.GetRawText(), null, null, default);
        await tasks.EnqueueAsync(route, payload.GetRawText(), null, null, default);
        var leased = Assert.Single(await tasks.ClaimAsync(
            "ops-test-worker", [hub], [new HubCapability(hub, "test-activity", 1)],
            1, TimeSpan.FromSeconds(30), default));

        var operations = new HubOperationsStore(tasks);
        var snapshot = JsonSerializer.SerializeToElement(
            await operations.SnapshotAsync(hub, 60, default));
        Assert.Equal(1, snapshot.GetProperty("tasks").GetProperty("ready").GetInt64());
        Assert.Equal(1, snapshot.GetProperty("tasks").GetProperty("leased").GetInt64());
        Assert.Equal(1, snapshot.GetProperty("contention").GetProperty("blockedReady").GetInt64());
        Assert.NotEmpty(snapshot.GetProperty("partitions").EnumerateArray());
        Assert.NotEmpty(snapshot.GetProperty("policies").EnumerateArray());
        Assert.NotEmpty(snapshot.GetProperty("workers").EnumerateArray());
        Assert.Equal(0, snapshot.GetProperty("tasks").GetProperty("runnableWorkflows").GetInt64());
        Assert.True(snapshot.GetProperty("database").GetProperty("connections").GetInt64() > 0);

        await operations.RecordSampleAsync(default);
        snapshot = JsonSerializer.SerializeToElement(
            await operations.SnapshotAsync(hub, 60, default));
        Assert.Contains(snapshot.GetProperty("trends").EnumerateArray(),
            point => point.GetProperty("ready").ValueKind == JsonValueKind.Number);

        Assert.True(await tasks.CompleteAsync(leased.Id, "ops-test-worker",
            leased.Epoch, "{}", default));
        snapshot = JsonSerializer.SerializeToElement(
            await operations.SnapshotAsync(hub, 60, default));
        Assert.Equal(0, snapshot.GetProperty("contention").GetProperty("blockedReady").GetInt64());
        Assert.Equal(0, snapshot.GetProperty("tasks").GetProperty("leased").GetInt64());
    }

    [Fact]
    public async Task RejectsInvalidFilterBeforeAttemptingConnection()
    {
        await using var tasks = new HubTaskStore(
            "Host=localhost;Database=unused;Username=unused;Password=unused");
        var operations = new HubOperationsStore(tasks);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            operations.SnapshotAsync(null, 10, default));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            operations.SnapshotAsync(new string('h', 81), 60, default));
    }
}