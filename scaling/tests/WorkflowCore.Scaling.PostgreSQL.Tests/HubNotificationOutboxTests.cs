using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNotificationOutboxTests
{
    [Fact]
    public async Task OutboxIsTransactionalAndDispatchPublishesCommittedHints()
    {
        var connection = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "outbox-" + Guid.NewGuid().ToString("N");
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
            cmd.Parameters.AddWithValue("id", Guid.NewGuid()); cmd.Parameters.AddWithValue("hub", hub);
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
        Assert.Contains(hub, await received.Task);
        await using var remaining = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_notification_outbox WHERE hub_id=@hub", db);
        remaining.Parameters.AddWithValue("hub", hub); Assert.Equal(0L, (long)(await remaining.ExecuteScalarAsync())!);
    }
}
