using Npgsql;
using NpgsqlTypes;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubClaimCandidateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TaskFiltersPreserveGlobalSequenceAfterIneligibleBacklog(bool multipleHubs)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var hub = "candidate-" + Guid.NewGuid().ToString("N");
        var secondHub = hub + "-second";
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using (var backlog = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task
                (id,hub_id,partition_id,task_type,task_version,payload,
                 state,due_at,attempts,owner,lease_until)
            SELECT gen_random_uuid(),CASE WHEN bucket=0 THEN @hub||'-other' ELSE @hub END,
                   CASE WHEN bucket=3 THEN 1 ELSE 0 END,
                   CASE WHEN bucket=1 THEN 'unsupported' ELSE 'activity' END,
                   CASE WHEN bucket=2 THEN 2 ELSE 1 END,'{}',
                   CASE WHEN bucket=6 THEN 1 WHEN bucket=7 THEN 2 ELSE 0 END,
                   now()+CASE WHEN bucket=4 THEN interval '1 hour' ELSE interval '0' END,
                   CASE WHEN bucket=5 THEN 5 ELSE 0 END,
                   CASE WHEN bucket=6 THEN 'busy' ELSE NULL END,
                   CASE WHEN bucket=6 THEN now()+interval '1 hour' ELSE NULL END
            FROM generate_series(0,7) bucket CROSS JOIN generate_series(1,110) item
            """, db))
        {
            backlog.Parameters.AddWithValue("hub", hub);
            await backlog.ExecuteNonQueryAsync();
        }
        var first = await InsertTask(db, hub, "activity", mode: null, workflowReference: false);
        var second = await InsertTask(db, secondHub, "activity", mode: null, workflowReference: false);
        var third = await InsertTask(db, hub, "activity", mode: null, workflowReference: false);
        await using var store = new HubTaskStore(database,
            enableUnconstrainedBatchClaims: false, enableFifoHeadPrefilter: false);
        var claimed = await store.ClaimAsync("worker", multipleHubs ? [hub, secondHub] : [hub],
            [new(hub, "activity", 1), new(secondHub, "activity", 1)],
            3, TimeSpan.FromSeconds(30), default,
            matchingPartition: 0, matchingPartitionCount: 8);
        Guid[] expected = multipleHubs ? [first, second, third] : [first, third];
        Assert.Equal(expected, claimed.Select(task => task.Id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WorkflowFiltersRunBeforeCandidateLimit(bool fifoHeadFilter)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var hub = "candidate-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using (var backlog = new NpgsqlCommand("""
            WITH workflows AS (
                INSERT INTO wfc_scale.hub_workflow
                    (id,hub_id,definition_id,definition_version,partition_id,
                     input_json,step_count,execution_mode,native_status)
                SELECT gen_random_uuid(),@hub,'candidate',CASE WHEN i<=110 THEN 1 ELSE 2 END,
                       0,'{}',1,'native',CASE WHEN i<=110 THEN 'Suspended' ELSE 'Runnable' END
                FROM generate_series(1,220) i
                RETURNING id
            )
            INSERT INTO wfc_scale.hub_task
                (id,hub_id,partition_id,task_type,task_version,workflow_id,payload)
            SELECT gen_random_uuid(),@hub,0,'__native_workflow__',1,id,'{}' FROM workflows
            """, db))
        {
            backlog.Parameters.AddWithValue("hub", hub);
            await backlog.ExecuteNonQueryAsync();
        }
        var expected = await InsertTask(db, hub, "__native_workflow__");
        await using var store = new HubTaskStore(database,
            enableUnconstrainedBatchClaims: false, enableFifoHeadPrefilter: fifoHeadFilter);
        var claimed = await store.ClaimAsync("worker", [hub], [new(hub, "__native_workflow__", 1)],
            1, TimeSpan.FromSeconds(30), default, [new(hub, "candidate", 1)]);
        Assert.Equal(expected, Assert.Single(claimed).Id);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SharedWorkflowLookupPreservesStatusPinsAndMissingReferences(bool usePins)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var hub = "candidate-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        var expected = new List<Guid>();
        expected.Add(await InsertTask(db, hub, "__native_workflow__"));
        var otherVersion = await InsertTask(db, hub, "__native_workflow__", definitionVersion: 2);
        if (!usePins) expected.Add(otherVersion);
        await InsertTask(db, hub, "__native_workflow__", status: "Suspended");
        await InsertTask(db, hub, "activity", status: "Complete");
        expected.Add(await InsertTask(db, hub, "__workflow__", mode: "inline", status: "Suspended"));
        expected.Add(await InsertTask(db, hub, "activity", definitionVersion: 2));
        expected.Add(await InsertTask(db, hub, "activity", mode: null, workflowReference: false));
        var orphan = await InsertTask(db, hub, "__workflow__", mode: null);
        if (!usePins) expected.Add(orphan);
        expected.Add(await InsertTask(db, hub, "activity", mode: null));

        await using var store = new HubTaskStore(database,
            enableUnconstrainedBatchClaims: false, enableFifoHeadPrefilter: false);
        HubWorkflowCapability[]? pins = usePins ? [new(hub, "candidate", 1)] : null;
        var claimed = await store.ClaimAsync("worker", [hub],
            [new(hub, "__native_workflow__", 1), new(hub, "__workflow__", 1), new(hub, "activity", 1)],
            32, TimeSpan.FromSeconds(30), default, pins);
        Assert.Equal(expected, claimed.Select(task => task.Id));
    }

    [Fact]
    public async Task OrderedCapabilityMerge_RespectsWorkflowPinsAndNativeStatus()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var hub = "merge-pins-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        var accepted = await InsertTask(db, hub, "__native_workflow__");
        await InsertTask(db, hub, "__native_workflow__", definitionVersion: 2);
        await InsertTask(db, hub, "__native_workflow__", status: "Suspended");
        await InsertTask(db, hub, "activity", status: "Complete");
        var activity = await InsertTask(db, hub, "activity", definitionVersion: 2);
        await using var store = new HubTaskStore(database, enableUnconstrainedBatchClaims: true,
            enableOrderedCapabilityMerge: true);
        var claimed = await store.ClaimAsync("worker", [hub],
            [new(hub, "__native_workflow__", 1), new(hub, "activity", 1)],
            8, TimeSpan.FromSeconds(30), default, [new(hub, "candidate", 1)],
            matchingPartition: 0, matchingPartitionCount: 4);
        Assert.True(new[] { accepted, activity }.ToHashSet().SetEquals(claimed.Select(t => t.Id)));
    }

    private static async Task<Guid> InsertTask(NpgsqlConnection db, string hub, string taskType,
        string? mode = "native", string status = "Runnable", int definitionVersion = 1,
        bool workflowReference = true)
    {
        Guid? workflow = workflowReference ? Guid.NewGuid() : null;
        if (mode is not null && workflow.HasValue)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_workflow
                    (id,hub_id,definition_id,definition_version,partition_id,
                     input_json,step_count,execution_mode,native_status)
                VALUES (@id,@hub,'candidate',@version,0,'{}',1,@mode,@status)
                """, db);
            insert.Parameters.AddWithValue("id", workflow.Value);
            insert.Parameters.AddWithValue("hub", hub);
            insert.Parameters.AddWithValue("version", definitionVersion);
            insert.Parameters.AddWithValue("mode", mode);
            insert.Parameters.AddWithValue("status", status);
            await insert.ExecuteNonQueryAsync();
        }
        var id = Guid.NewGuid();
        await using var task = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task
                (id,hub_id,partition_id,task_type,task_version,workflow_id,payload)
            VALUES (@id,@hub,0,@type,1,@workflow,'{}')
            """, db);
        task.Parameters.AddWithValue("id", id);
        task.Parameters.AddWithValue("hub", hub);
        task.Parameters.AddWithValue("type", taskType);
        task.Parameters.Add("workflow", NpgsqlDbType.Uuid).Value = (object?)workflow ?? DBNull.Value;
        await task.ExecuteNonQueryAsync();
        return id;
    }
}
