using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeActivityCompletionRoundtripTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ActivityCompletionPersistsExactlyOnceWithOriginalFencing(bool combined)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions)
            { CombinedActivityCompletionEnabled = combined };
        var remote = new HubRemoteWorkflowStore(tasks, definitions);
        var hub = "completion-roundtrip-" + Guid.NewGuid().ToString("N");
        var payload = JsonSerializer.SerializeToElement(new { result = "ok" });
        await definitions.RegisterAsync(new(hub, "native", 1, 4, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        Assert.True(await definitions.RegisterActivityAsync(new(hub, "activity", 1, 4), default));
        var workflowId = await remote.StartAsync(new(hub, "native", 1,
            JsonSerializer.SerializeToElement(new { })), default);
        var initial = Assert.Single(await tasks.ClaimAsync("workflow-worker", [hub],
            [new(hub, "__native_workflow__", 1)], 1, TimeSpan.FromSeconds(60), default));
        var dispatch = new HubNativeDispatch("dispatch-1", "pointer-1", 0, "activity", 1,
            JsonSerializer.SerializeToElement(new { }));
        var commit = new HubNativeCommit(1, "workflow-worker", initial.Epoch, 0, initial.Id,
            JsonSerializer.SerializeToElement(new { }), "Runnable", false, null,
            [], [], [dispatch], []);
        Assert.NotNull(await native.CommitAsync(workflowId, commit, default));
        var activity = Assert.Single(await tasks.ClaimAsync("activity-worker", [hub],
            [new(hub, "activity", 1)], 1, TimeSpan.FromSeconds(60), default));

        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        async Task<long> Count(string query)
        {
            await using var cmd = new NpgsqlCommand(query, db);
            cmd.Parameters.AddWithValue("workflow", workflowId);
            cmd.Parameters.AddWithValue("task", activity.Id);
            return (long)(await cmd.ExecuteScalarAsync() ?? throw new InvalidOperationException());
        }

        var eventsBefore = await Count("""
            SELECT count(*) FROM wfc_scale.hub_workflow_event
            WHERE workflow_id=@workflow AND task_id=@task AND event_type='ActivityResultReceived'
            """);
        var inboxBefore = await Count("""
            SELECT count(*) FROM wfc_scale.hub_native_inbox
            WHERE workflow_id=@workflow AND kind='Activity' AND source_key='dispatch-1'
            """);
        var wakesBefore = await Count("""
            SELECT count(*) FROM wfc_scale.hub_task
            WHERE workflow_id=@workflow AND task_type='__native_workflow__' AND state=0
            """);

        Assert.False(await native.CompleteActivityAsync(activity.Id, "activity-worker",
            activity.Epoch+1, payload, default));
        Assert.Equal(eventsBefore, await Count("""
            SELECT count(*) FROM wfc_scale.hub_workflow_event
            WHERE workflow_id=@workflow AND task_id=@task AND event_type='ActivityResultReceived'
            """));
        Assert.True(await native.CompleteActivityAsync(activity.Id, "activity-worker",
            activity.Epoch, payload, default));
        Assert.True(await native.CompleteActivityAsync(activity.Id, "activity-worker",
            activity.Epoch, payload, default));
        Assert.False(await native.CompleteActivityAsync(activity.Id, "different-worker",
            activity.Epoch, payload, default));
        Assert.False(await native.CompleteActivityAsync(activity.Id, "activity-worker",
            activity.Epoch, JsonSerializer.SerializeToElement(new { result = "different" }), default));

        Assert.Equal(eventsBefore+1, await Count("""
            SELECT count(*) FROM wfc_scale.hub_workflow_event
            WHERE workflow_id=@workflow AND task_id=@task AND event_type='ActivityResultReceived'
            """));
        Assert.Equal(inboxBefore+1, await Count("""
            SELECT count(*) FROM wfc_scale.hub_native_inbox
            WHERE workflow_id=@workflow AND kind='Activity' AND source_key='dispatch-1'
            """));
        Assert.Equal(wakesBefore+1, await Count("""
            SELECT count(*) FROM wfc_scale.hub_task
            WHERE workflow_id=@workflow AND task_type='__native_workflow__' AND state=0
            """));
        await using var attempt = new NpgsqlCommand("""
            SELECT a.outcome, a.finished_at IS NOT NULL, t.state, t.epoch
            FROM wfc_scale.hub_task_attempt a JOIN wfc_scale.hub_task t ON t.id=a.task_id
            WHERE a.task_id=@task AND a.epoch=@epoch
            """, db);
        attempt.Parameters.AddWithValue("task", activity.Id);
        attempt.Parameters.AddWithValue("epoch", activity.Epoch);
        await using var reader = await attempt.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("Completed", reader.GetString(0));
        Assert.True(reader.GetBoolean(1));
        Assert.Equal(2, reader.GetInt16(2));
        Assert.Equal(activity.Epoch, reader.GetInt64(3));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExpiredLeaseDoesNotInsertInboxHistoryOrWake(bool combined)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions)
            { CombinedActivityCompletionEnabled = combined };
        var hub = "expired-completion-" + Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub, "native", 1, 4, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        Assert.True(await definitions.RegisterActivityAsync(new(hub,"activity",1,4),default));
        var id = await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(
            new(hub,"native",1,JsonSerializer.SerializeToElement(new{})),default);
        var workflow = Assert.Single(await tasks.ClaimAsync("worker",[hub],
            [new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(60),default));
        var commit = new HubNativeCommit(1,"worker",workflow.Epoch,0,workflow.Id,
            JsonSerializer.SerializeToElement(new{}),"Runnable",false,null,[],[],
            [new HubNativeDispatch("expired-dispatch","expired-pointer",0,"activity",1,
                JsonSerializer.SerializeToElement(new{}))],[]);
        Assert.NotNull(await native.CommitAsync(id,commit,default));
        var activity = Assert.Single(await tasks.ClaimAsync("worker",[hub],
            [new(hub,"activity",1)],1,TimeSpan.FromSeconds(60),default));
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using(var expire = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task SET lease_until=clock_timestamp()-interval '1 second'
            WHERE id=@task
            """,db))
        {
            expire.Parameters.AddWithValue("task",activity.Id);
            Assert.Equal(1,await expire.ExecuteNonQueryAsync());
        }
        Assert.False(await native.CompleteActivityAsync(activity.Id,"worker",activity.Epoch,
            JsonSerializer.SerializeToElement(new{ result=1 }),default));
        await using var check = new NpgsqlCommand("""
            SELECT
                (SELECT count(*) FROM wfc_scale.hub_workflow_event
                 WHERE task_id=@task AND event_type='ActivityResultReceived'),
                (SELECT count(*) FROM wfc_scale.hub_native_inbox
                 WHERE workflow_id=@id AND source_key='expired-dispatch'),
                (SELECT count(*) FROM wfc_scale.hub_task
                 WHERE workflow_id=@id AND task_type='__native_workflow__' AND state=0)
            """,db);
        check.Parameters.AddWithValue("id",id);
        check.Parameters.AddWithValue("task",activity.Id);
        await using var r = await check.ExecuteReaderAsync();
        Assert.True(await r.ReadAsync());
        Assert.Equal(0L,r.GetInt64(0));
        Assert.Equal(0L,r.GetInt64(1));
        // Native commit may schedule a workflow turn; completion must not add another.
        Assert.InRange(r.GetInt64(2),0,1);
    }
}
