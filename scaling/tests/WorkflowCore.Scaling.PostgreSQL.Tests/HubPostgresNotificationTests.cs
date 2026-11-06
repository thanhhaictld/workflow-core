using Npgsql;
using NpgsqlTypes;
using Microsoft.Extensions.Logging.Abstractions;
using WorkflowCore.Hub.Gateway;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Real PostgreSQL integration checks for transaction-coupled cross-pod hints.
/// The database schema must include migration 014; no application worker claims
/// a task because of a notification alone.
/// </summary>
public sealed class HubPostgresNotificationTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");
    private static string Hub() => "notify-" + Guid.NewGuid().ToString("N")[..12];

    private static async Task<NpgsqlConnection> ListenAsync(string connectionString,
        Action<string> onMessage)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        connection.Notification += (_, e) =>
        {
            if (e.Channel == "wfc_hub_task_ready") onMessage(e.Payload);
        };
        await using var command = new NpgsqlCommand("LISTEN wfc_hub_task_ready", connection);
        await command.ExecuteNonQueryAsync();
        return connection;
    }

    private static async Task WaitForHubAsync(NpgsqlConnection listener, string hub,
        List<string> received)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!received.Contains(hub))
            await listener.WaitAsync(timeout.Token);
    }

    private static async Task<Guid> InsertTaskAsync(NpgsqlConnection db,
        string hub, DateTime? due = null, NpgsqlTransaction? tx = null)
    {
        var id = Guid.NewGuid();
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task
                (id, hub_id, partition_id, task_type, task_version, payload, due_at)
            VALUES (@id, @hub, 0, 'notify-test', 1, '{}'::jsonb, coalesce(@due, now()))
            """, db, tx);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.Add("due", NpgsqlDbType.TimestampTz).Value = (object?)due ?? DBNull.Value;
        await cmd.ExecuteNonQueryAsync();
        return id;
    }

    [Fact]
    public async Task CommittedMatchingHintsCarryCapabilityAcrossTwoHubReplicas()
    {
        if (Database is null) return;
        var hub = Hub();
        using var lifetime = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var storeA = new HubTaskStore(Database, 3);
        await using var storeB = new HubTaskStore(Database, 3);
        using var readyA = new HubReadinessBroadcaster(storeA,
            new HubWorkflowStore(storeA, new HubPolicyRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        using var readyB = new HubReadinessBroadcaster(storeB,
            new HubWorkflowStore(storeB, new HubPolicyRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        var a = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var b = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        readyA.TaskReady += (observed, type, version) =>
        {
            if (observed == hub && type == "notify-test" && version == 1)
                a.TrySetResult();
        };
        readyB.TaskReady += (observed, type, version) =>
        {
            if (observed == hub && type == "notify-test" && version == 1)
                b.TrySetResult();
        };
        using var listenerA = new PostgresHubNotificationListener(Database, readyA,
            NullLogger<PostgresHubNotificationListener>.Instance);
        using var listenerB = new PostgresHubNotificationListener(Database, readyB,
            NullLogger<PostgresHubNotificationListener>.Instance);
        await listenerA.StartAsync(lifetime.Token);
        await listenerB.StartAsync(lifetime.Token);
        try
        {
            while (!listenerA.IsListening || !listenerB.IsListening)
                await Task.Delay(25, lifetime.Token);
            await using var writer = new NpgsqlConnection(Database);
            await writer.OpenAsync(lifetime.Token);
            await InsertTaskAsync(writer, hub);
            await Task.WhenAll(a.Task.WaitAsync(lifetime.Token), b.Task.WaitAsync(lifetime.Token));
        }
        finally
        {
            await listenerA.StopAsync(CancellationToken.None);
            await listenerB.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task CommitNotifiesAllHubReplicaListeners()
    {
        if (Database is null) return;
        var hub = Hub();
        var a = new List<string>();
        var b = new List<string>();
        await using var one = await ListenAsync(Database, a.Add);
        await using var two = await ListenAsync(Database, b.Add);
        await using var writer = new NpgsqlConnection(Database);
        await writer.OpenAsync();
        await InsertTaskAsync(writer, hub);
        await Task.WhenAll(WaitForHubAsync(one, hub, a), WaitForHubAsync(two, hub, b));
        Assert.Contains(hub, a);
        Assert.Contains(hub, b);
    }

    [Fact]
    public async Task RolledBackInsertDoesNotNotifyOrCreateReadyWork()
    {
        if (Database is null) return;
        var hub = Hub();
        var received = new List<string>();
        await using var listener = await ListenAsync(Database, received.Add);
        await using var writer = new NpgsqlConnection(Database);
        await writer.OpenAsync();
        Guid task;
        await using (var transaction = await writer.BeginTransactionAsync())
        {
            task = await InsertTaskAsync(writer, hub, tx: transaction);
            await transaction.RollbackAsync();
        }
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        try
        {
            while (!timeout.IsCancellationRequested)
                await listener.WaitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested) { }
        Assert.DoesNotContain(hub, received);
        await using var verify = new NpgsqlCommand(
            "SELECT count(*) FROM wfc_scale.hub_task WHERE id = @id", writer);
        verify.Parameters.AddWithValue("id", task);
        Assert.Equal(0, Convert.ToInt32(await verify.ExecuteScalarAsync()));
    }

    [Fact]
    public async Task FutureTaskNotifiesWhenTransitionedToReady()
    {
        if (Database is null) return;
        var hub = Hub();
        var received = new List<string>();
        await using var listener = await ListenAsync(Database, received.Add);
        await using var writer = new NpgsqlConnection(Database);
        await writer.OpenAsync();
        var id = await InsertTaskAsync(writer, hub, DateTime.UtcNow.AddMinutes(3));
        // The insert of a future task is deliberately not an immediate wake-up.
        await using (var cmd = new NpgsqlCommand(
            "UPDATE wfc_scale.hub_task SET due_at = now() WHERE id = @id", writer))
        {
            cmd.Parameters.AddWithValue("id", id);
            await cmd.ExecuteNonQueryAsync();
        }
        await WaitForHubAsync(listener, hub, received);
    }

    [Fact]
    public async Task ReleasingLeaseNotifiesWaitingWork()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var writer = new NpgsqlConnection(Database);
        await writer.OpenAsync();
        var id = await InsertTaskAsync(writer, hub);
        // Set up a leased task; completing it releases potentially blocked permits.
        await using (var lease = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task
            SET state = 1, owner = 'worker', lease_until = now() + interval '1 minute'
            WHERE id = @id
            """, writer))
        {
            lease.Parameters.AddWithValue("id", id);
            await lease.ExecuteNonQueryAsync();
        }
        var received = new List<string>();
        await using var listener = await ListenAsync(Database, received.Add);
        await using (var complete = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task
            SET state = 2, owner = NULL, lease_until = NULL WHERE id = @id
            """, writer))
        {
            complete.Parameters.AddWithValue("id", id);
            await complete.ExecuteNonQueryAsync();
        }
        await WaitForHubAsync(listener, hub, received);
    }
    [Fact]
    public async Task TwoHubPodsForwardCommittedHintToTheirOwnSseSubscribers()
    {
        if (Database is null) return;
        var hub = Hub();
        using var lifetime = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var storeOne = new HubTaskStore(Database, 2);
        await using var storeTwo = new HubTaskStore(Database, 2);
        using var readinessOne = new HubReadinessBroadcaster(storeOne,
            new HubWorkflowStore(storeOne, new HubPolicyRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        using var readinessTwo = new HubReadinessBroadcaster(storeTwo,
            new HubWorkflowStore(storeTwo, new HubPolicyRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        using var listenerOne = new PostgresHubNotificationListener(Database, readinessOne,
            NullLogger<PostgresHubNotificationListener>.Instance);
        using var listenerTwo = new PostgresHubNotificationListener(Database, readinessTwo,
            NullLogger<PostgresHubNotificationListener>.Instance);
        await listenerOne.StartAsync(lifetime.Token);
        await listenerTwo.StartAsync(lifetime.Token);
        try
        {
            while (!listenerOne.IsListening || !listenerTwo.IsListening)
                await Task.Delay(25, lifetime.Token);

            await using var one = readinessOne.Subscribe(new[] { hub }, lifetime.Token)
                .GetAsyncEnumerator(lifetime.Token);
            await using var two = readinessTwo.Subscribe(new[] { hub }, lifetime.Token)
                .GetAsyncEnumerator(lifetime.Token);
            Assert.True(await one.MoveNextAsync()); // initial catch-up hint
            Assert.True(await two.MoveNextAsync());
            await using var writer = new NpgsqlConnection(Database);
            await writer.OpenAsync(lifetime.Token);
            await InsertTaskAsync(writer, hub);

            var delivered = await Task.WhenAll(
                one.MoveNextAsync().AsTask(), two.MoveNextAsync().AsTask());
            Assert.All(delivered, Assert.True);
            Assert.Equal(hub, one.Current);
            Assert.Equal(hub, two.Current);
        }
        finally
        {
            await listenerOne.StopAsync(CancellationToken.None);
            await listenerTwo.StopAsync(CancellationToken.None);
        }
    }

}
