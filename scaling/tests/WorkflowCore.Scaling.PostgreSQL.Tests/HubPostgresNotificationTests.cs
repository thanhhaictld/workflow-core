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
            new HubWorkflowStore(storeA, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        using var readyB = new HubReadinessBroadcaster(storeB,
            new HubWorkflowStore(storeB, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        var a = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var b = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        readyA.TaskReady += (observed, type, version, partition) =>
        {
            if (observed == hub && partition == 0)
                a.TrySetResult();
        };
        readyB.TaskReady += (observed, type, version, partition) =>
        {
            if (observed == hub && partition == 0)
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
    public async Task LightweightSignalsCoalesceMultirowStatementsByBucketAndWakeOncePerBucket()
    {
        if (Database is null) return;
        var hub = Hub();
        var matching = new List<string>();
        var legacy = new List<string>();
        await using var listener = new NpgsqlConnection(Database);
        await listener.OpenAsync();
        listener.Notification += (_, e) =>
        {
            if (!e.Payload.Contains(hub, StringComparison.Ordinal)) return;
            if (e.Channel == "wfc_hub_matching_ready") matching.Add(e.Payload);
            if (e.Channel == "wfc_hub_task_ready") legacy.Add(e.Payload);
        };
        await using (var listen = new NpgsqlCommand(
            "LISTEN wfc_hub_matching_ready; LISTEN wfc_hub_task_ready", listener))
            await listen.ExecuteNonQueryAsync();
        await using var writer = new NpgsqlConnection(Database);
        await writer.OpenAsync();
        await using (var tx = await writer.BeginTransactionAsync())
        {
            await using var insert = new NpgsqlCommand("""
                SET LOCAL wfc.hub_notification_outbox='off';
                INSERT INTO wfc_scale.hub_task
                    (id,hub_id,partition_id,task_type,task_version,payload)
                SELECT gen_random_uuid(),@hub,
                       CASE WHEN n%2=0 THEN 64 ELSE 1 END,'bulk-ready',1,'{}'::jsonb
                FROM generate_series(1,100) n
                """, writer, tx);
            insert.Parameters.AddWithValue("hub", hub);
            await insert.ExecuteNonQueryAsync();
            // PostgreSQL only delivers a notification after commit.
            Assert.Empty(matching);
            await tx.CommitAsync();
        }
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (matching.Count < 2 || legacy.Count < 1)
            await listener.WaitAsync(timeout.Token);
        Assert.Equal(2, matching.Count);
        Assert.Single(legacy);
        Assert.Equal(hub, legacy[0]);
        var buckets = matching.Select(payload =>
        {
            using var json = System.Text.Json.JsonDocument.Parse(payload);
            Assert.Equal(hub, json.RootElement.GetProperty("hub").GetString());
            Assert.Equal("signal", json.RootElement.GetProperty("kind").GetString());
            Assert.False(json.RootElement.TryGetProperty("taskId", out _));
            return json.RootElement.GetProperty("partition").GetInt32();
        }).Order().ToArray();
        Assert.Equal(new[] {0,1}, buckets);

        matching.Clear(); legacy.Clear();
        await using (var lease = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task SET state=1,owner='bulk-worker',
                lease_until=now()+interval '1 minute',epoch=epoch+1
            WHERE hub_id=@hub
            """, writer))
        {
            lease.Parameters.AddWithValue("hub",hub);
            await lease.ExecuteNonQueryAsync();
        }
        using (var quiet = new CancellationTokenSource(TimeSpan.FromMilliseconds(200)))
        {
            try { await listener.WaitAsync(quiet.Token); }
            catch (OperationCanceledException) when (quiet.IsCancellationRequested) { }
        }
        Assert.Empty(matching);
        await using (var retry = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task SET state=0,due_at=now(),
                owner=NULL,lease_until=NULL WHERE hub_id=@hub
            """, writer))
        {
            retry.Parameters.AddWithValue("hub",hub);
            await retry.ExecuteNonQueryAsync();
        }
        using var resume = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (matching.Count < 2)
            await listener.WaitAsync(resume.Token);
        Assert.Equal(2, matching.Count);
    }

    [Fact]
    public async Task LegacyTaskIdSessionSettingCannotReenablePerTaskNotifications()
    {
        if (Database is null) return;
        var hub = Hub();
        var payload = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var listener = new NpgsqlConnection(Database);
        await listener.OpenAsync();
        listener.Notification += (_, e) =>
        {
            if (e.Channel == "wfc_hub_matching_ready" && e.Payload.Contains(hub, StringComparison.Ordinal))
                payload.TrySetResult(e.Payload);
        };
        await using (var listen = new NpgsqlCommand("LISTEN wfc_hub_matching_ready", listener))
            await listen.ExecuteNonQueryAsync();
        await using var writer = new NpgsqlConnection(Database);
        await writer.OpenAsync();
        await using (var insert = new NpgsqlCommand("SET wfc.hub_candidate_task_id_hints='on'; INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES(gen_random_uuid(),@hub,64,'legacy-setting-test',1,'{}')", writer))
        {
            insert.Parameters.AddWithValue("hub", hub);
            await insert.ExecuteNonQueryAsync();
        }
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!payload.Task.IsCompleted) await listener.WaitAsync(timeout.Token);
        using var json = System.Text.Json.JsonDocument.Parse(await payload.Task.WaitAsync(timeout.Token));
        Assert.Equal("signal", json.RootElement.GetProperty("kind").GetString());
        Assert.Equal(0, json.RootElement.GetProperty("partition").GetInt32());
        Assert.False(json.RootElement.TryGetProperty("taskId", out _));
    }

    [Fact]
    public async Task LegacyExactHintIsDowngradedToCoarseCandidateRescan()
    {
        if (Database is null) return;
        var hub = Hub();
        using var lifetime = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var store = new HubTaskStore(Database);
        using var readiness = new HubReadinessBroadcaster(store,
            new HubWorkflowStore(store, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        using var listener = new PostgresHubNotificationListener(Database, readiness,
            NullLogger<PostgresHubNotificationListener>.Instance);
        var coarse = new TaskCompletionSource<(string Hub, string? Type, int Version, int? Partition)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        readiness.CandidateRescanRequested += (observed, type, version, partition) =>
        {
            if (observed == hub && type == "mixed-version")
                coarse.TrySetResult((observed, type, version, partition));
        };
        await listener.StartAsync(lifetime.Token);
        try
        {
            while (!listener.IsListening) await Task.Delay(25, lifetime.Token);
            await using var writer = new NpgsqlConnection(Database);
            await writer.OpenAsync(lifetime.Token);
            var id = Guid.NewGuid();
            await using (var send = new NpgsqlCommand("""
                SELECT pg_notify('wfc_hub_matching_ready',
                    json_build_object('hub',@hub,'taskId',@id,'partition',64,
                                      'type','mixed-version','version',1,'kind','task')::text)
                """, writer))
            {
                send.Parameters.AddWithValue("hub", hub);
                send.Parameters.AddWithValue("id", id);
                await send.ExecuteNonQueryAsync(lifetime.Token);
            }
            Assert.Equal((hub, "mixed-version", 1, 64), await coarse.Task.WaitAsync(lifetime.Token));
        }
        finally { await listener.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task MalformedAndOversizedMatchingPayloadsRequestRecoveryWithoutTearingDownListener()
    {
        if (Database is null) return;
        var hub = Hub();
        using var lifetime = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var store = new HubTaskStore(Database);
        using var readiness = new HubReadinessBroadcaster(store,
            new HubWorkflowStore(store, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        using var listener = new PostgresHubNotificationListener(Database, readiness,
            NullLogger<PostgresHubNotificationListener>.Instance);
        var recovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var coarse = new TaskCompletionSource<(string, string?, int, int?)>(TaskCreationOptions.RunContinuationsAsynchronously);
        readiness.CandidateRecoveryRequired += () => recovery.TrySetResult();
        readiness.CandidateRescanRequested += (observed, type, version, partition) =>
        {
            if (observed == hub && partition == 0)
                coarse.TrySetResult((observed, type, version, partition));
        };
        await listener.StartAsync(lifetime.Token);
        try
        {
            while (!listener.IsListening) await Task.Delay(25, lifetime.Token);
            await using var writer = new NpgsqlConnection(Database); await writer.OpenAsync(lifetime.Token);
            await using (var malformed = new NpgsqlCommand("SELECT pg_notify('wfc_hub_matching_ready','{\\\"hub\\\":42}')", writer))
                await malformed.ExecuteNonQueryAsync(lifetime.Token);
            await recovery.Task.WaitAsync(lifetime.Token);
            await using (var oversized = new NpgsqlCommand("SELECT pg_notify('wfc_hub_matching_ready',repeat('x',513))", writer))
                await oversized.ExecuteNonQueryAsync(lifetime.Token);
            await InsertTaskAsync(writer, hub);
            Assert.Equal((hub, null, 0, 0), await coarse.Task.WaitAsync(lifetime.Token));
            Assert.True(listener.IsListening);
        }
        finally { await listener.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task NotificationFloodPublishesBoundedBatchesAndRetainsRecoveryIntent()
    {
        if (Database is null) return;
        var hub = Hub();
        using var lifetime = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var store = new HubTaskStore(Database);
        using var readiness = new HubReadinessBroadcaster(store,
            new HubWorkflowStore(store, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        using var listener = new PostgresHubNotificationListener(Database, readiness,
            NullLogger<PostgresHubNotificationListener>.Instance);
        var published = 0; var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        readiness.CandidateRescanRequested += (observed, _, _, _) =>
        {
            if (observed != hub) return;
            Interlocked.Increment(ref published);
            // Keep the consumer slower than PostgreSQL's notification callback
            // so this test exercises the bounded channel's overflow path.
            Thread.Sleep(1);
        };
        readiness.CandidateRecoveryRequired += () => recovered.TrySetResult();
        await listener.StartAsync(lifetime.Token);
        try
        {
            while (!listener.IsListening) await Task.Delay(25, lifetime.Token);
            await using var writer = new NpgsqlConnection(Database); await writer.OpenAsync(lifetime.Token);
            await using var flood = new NpgsqlCommand("""
                SELECT pg_notify('wfc_hub_matching_ready',
                    json_build_object('hub',@hub,'partition',64,
                        'type','flood-' || n::text,'version',1,'kind','signal')::text)
                FROM generate_series(1,4096) n
                """, writer);
            flood.Parameters.AddWithValue("hub", hub);
            await flood.ExecuteNonQueryAsync(lifetime.Token);
            await recovered.Task.WaitAsync(lifetime.Token);
            using var publishedTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            while (Volatile.Read(ref published) == 0) await Task.Delay(10, publishedTimeout.Token);
            Assert.InRange(Volatile.Read(ref published), 1, 4096);
            Assert.True(listener.IsListening);
        }
        finally { await listener.StopAsync(CancellationToken.None); }
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData("FaLsE", false)]
    [InlineData("0", false)]
    public async Task UnpartitionedLocalFallbackDefersCandidateDiscoveryWhenNotificationsAreEnabled(
        string? notifyFlag, bool notificationsEnabled)
    {
        if (Database is not { } database) return;
        const string setting = "WFC_HUB_PG_NOTIFY_ENABLED";
        var previous = Environment.GetEnvironmentVariable(setting);
        try
        {
            Environment.SetEnvironmentVariable(setting, notifyFlag);
            await VerifyLocalFallbackAsync(database, notificationsEnabled);
        }
        finally { Environment.SetEnvironmentVariable(setting, previous); }
    }

    private static async Task VerifyLocalFallbackAsync(string database, bool notificationsEnabled)
    {
        var hub = Hub();
        await using var store = new HubTaskStore(database);
        using var readiness = new HubReadinessBroadcaster(store,
            new HubWorkflowStore(store, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        var rescans = new List<(string Hub, int? Partition)>();
        readiness.CandidateRescanRequested += (observed, _, _, partition) => rescans.Add((observed, partition));
        var legacyHints = new List<(string Hub, string? Type, int Version, int? Partition)>();
        readiness.TaskReady += (observed, type, version, partition) => legacyHints.Add((observed, type, version, partition));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var subscriber = readiness.Subscribe([hub], timeout.Token).GetAsyncEnumerator();
        Assert.True(await subscriber.MoveNextAsync()); // Initial SSE wakeup.

        readiness.PublishFallback(hub, "local", 1);
        Assert.Equal((hub, "local", 1, (int?)null), Assert.Single(legacyHints));
        Assert.True(await subscriber.MoveNextAsync());
        Assert.Equal(hub, subscriber.Current);
        if (notificationsEnabled) Assert.Empty(rescans);
        else Assert.Equal((hub, (int?)null), Assert.Single(rescans));

        rescans.Clear();
        readiness.PublishFallback(hub, "local", 1, 64);
        Assert.Equal((hub, (int?)64), Assert.Single(rescans));
        rescans.Clear();
        await using var writer = new NpgsqlConnection(database); await writer.OpenAsync();
        await InsertTaskAsync(writer, hub);
        await readiness.ReconcileReadyAsync(default);
        Assert.Contains(rescans, hint => hint == (hub, null));
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
    public async Task ImmediateRetryNotifiesWaitingWork()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var writer = new NpgsqlConnection(Database);
        await writer.OpenAsync();
        var id = await InsertTaskAsync(writer, hub);
        // Set up a leased task; a retry can make it ready again.
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
            SET state = 0, owner = NULL, lease_until = NULL, due_at = now() WHERE id = @id
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
            new HubWorkflowStore(storeOne, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
            NullLogger<HubReadinessBroadcaster>.Instance);
        using var readinessTwo = new HubReadinessBroadcaster(storeTwo,
            new HubWorkflowStore(storeTwo, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
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
