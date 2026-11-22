using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNotificationOutboxTests
{
    [Fact]
    public async Task CoarseSignalUsesOutboxUntilDispatcherRuns()
    {
        var connection = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "outbox-legacy-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(connection);
        await db.OpenAsync();
        await using var listener = new NpgsqlConnection(connection);
        await listener.OpenAsync();
        await using (var listen = new NpgsqlCommand("LISTEN wfc_hub_task_ready; LISTEN wfc_hub_matching_ready", listener))
            await listen.ExecuteNonQueryAsync();
        var channels = new System.Collections.Concurrent.ConcurrentQueue<string>();
        var payload = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        listener.Notification += (_, e) =>
        {
            if (e.Payload.Contains(hub, StringComparison.Ordinal))
            {
                channels.Enqueue(e.Channel);
                if (e.Channel == "wfc_hub_matching_ready") payload.TrySetResult(e.Payload);
            }
        };
        var id = Guid.NewGuid();
        await using (var tx = await db.BeginTransactionAsync())
        {
            await using var insert = new NpgsqlCommand("""
                SET LOCAL wfc.hub_notification_outbox='on';
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
                VALUES (@id,@hub,64,'legacy-outbox',1,'{}');
                """, db, tx);
            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("hub", hub);
            await insert.ExecuteNonQueryAsync();
            await tx.CommitAsync();
        }
        using (var noEarlyNotification = new CancellationTokenSource(TimeSpan.FromMilliseconds(200)))
        {
            try { await listener.WaitAsync(noEarlyNotification.Token); }
            catch (OperationCanceledException) when (noEarlyNotification.IsCancellationRequested) { }
        }
        Assert.Empty(channels);
        await using (var row = new NpgsqlCommand("SELECT matching_payload FROM wfc_scale.hub_notification_outbox WHERE hub_id=@hub", db))
        {
            row.Parameters.AddWithValue("hub", hub);
            var saved = (string?)await row.ExecuteScalarAsync();
            Assert.NotNull(saved);
            using var json = System.Text.Json.JsonDocument.Parse(saved!);
            Assert.Equal(64, json.RootElement.GetProperty("partition").GetInt32());
            Assert.False(json.RootElement.TryGetProperty("taskId", out _));
        }
        await using var store = new HubTaskStore(connection);
        var dispatcher = new PostgresHubNotificationDispatcher(store,
            NullLogger<PostgresHubNotificationDispatcher>.Instance);
        Assert.True(await dispatcher.DispatchAsync(default) >= 1);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!payload.Task.IsCompleted) await listener.WaitAsync(timeout.Token);
        using var delivered = System.Text.Json.JsonDocument.Parse(await payload.Task);
        Assert.Equal(hub, delivered.RootElement.GetProperty("hub").GetString());
        Assert.Equal(64, delivered.RootElement.GetProperty("partition").GetInt32());
        Assert.Equal(new[] { "wfc_hub_task_ready", "wfc_hub_matching_ready" }, channels.ToArray());
    }

    [Fact]
    public async Task OutboxIsTransactionalAndDispatchPublishesCommittedHints()
    {
        var connection = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "outbox-" + Guid.NewGuid().ToString("N");
        var taskId = Guid.NewGuid();
        await using var db = new NpgsqlConnection(connection);
        await db.OpenAsync();
        await using var listener = new NpgsqlConnection(connection);
        await listener.OpenAsync();
        await using (var listen = new NpgsqlCommand("LISTEN wfc_hub_matching_ready", listener)) await listen.ExecuteNonQueryAsync();
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        listener.Notification += (_, e) => { if (e.Payload.Contains(hub, StringComparison.Ordinal)) received.TrySetResult(e.Payload); };
        async Task Insert(NpgsqlTransaction tx)
        {
            await using var cmd = new NpgsqlCommand("""
                SET LOCAL wfc.hub_notification_outbox = 'on';
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
                VALUES (@id,@hub,0,'work',1,'{}');
                """, db, tx);
            cmd.Parameters.AddWithValue("id", taskId); cmd.Parameters.AddWithValue("hub", hub);
            await cmd.ExecuteNonQueryAsync();
        }
        await using (var rolledBack = await db.BeginTransactionAsync()) { await Insert(rolledBack); await rolledBack.RollbackAsync(); }
        await using (var check = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_notification_outbox WHERE hub_id=@hub", db))
        {
            check.Parameters.AddWithValue("hub", hub); Assert.Equal(0L, (long)(await check.ExecuteScalarAsync())!);
        }
        await using (var committed = await db.BeginTransactionAsync()) { await Insert(committed); await committed.CommitAsync(); }
        await using var store = new HubTaskStore(connection);
        var dispatcher = new PostgresHubNotificationDispatcher(store, NullLogger<PostgresHubNotificationDispatcher>.Instance);
        await Assert.ThrowsAsync<IOException>(() => dispatcher.DispatchAsync(default,
            (_, _) => throw new IOException("failure before commit")));
        await using (var retained = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_notification_outbox WHERE hub_id=@hub", db))
        {
            retained.Parameters.AddWithValue("hub", hub); Assert.Equal(1L, (long)(await retained.ExecuteScalarAsync())!);
        }
        Assert.True(await dispatcher.DispatchAsync(default) >= 1);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!received.Task.IsCompleted) await listener.WaitAsync(timeout.Token);
        using (var json = System.Text.Json.JsonDocument.Parse(await received.Task))
        {
            Assert.Equal("signal", json.RootElement.GetProperty("kind").GetString());
            Assert.Equal(0, json.RootElement.GetProperty("partition").GetInt32());
            Assert.False(json.RootElement.TryGetProperty("taskId", out _));
        }
        await using var remaining = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_notification_outbox WHERE hub_id=@hub", db);
        remaining.Parameters.AddWithValue("hub", hub); Assert.Equal(0L, (long)(await remaining.ExecuteScalarAsync())!);
    }
}
