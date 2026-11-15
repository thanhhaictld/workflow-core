using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateBatchPlanTests
{
    [Fact]
    public async Task SelectedIdBatchSqlUsesPrimaryKeyProbesForRepresentativeActiveBacklog()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub="candidate-batch-plan-"+Guid.NewGuid().ToString("N");
        await using var db=new NpgsqlConnection(database); await db.OpenAsync();
        try
        {
        await using (var seed=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            SELECT gen_random_uuid(),@hub,0,'activity',1,'{}' FROM generate_series(1,25000);
            ANALYZE wfc_scale.hub_task;
            """,db))
        { seed.Parameters.AddWithValue("hub",hub); await seed.ExecuteNonQueryAsync(); }

        Guid[] ids;
        await using (var read=new NpgsqlCommand("SELECT id FROM wfc_scale.hub_task WHERE hub_id=@hub ORDER BY sequence LIMIT 32",db))
        { read.Parameters.AddWithValue("hub",hub); var list=new List<Guid>(); await using var rows=await read.ExecuteReaderAsync(); while(await rows.ReadAsync()) list.Add(rows.GetGuid(0)); ids=list.ToArray(); }

        foreach(var mode in new[]{"force_custom_plan","force_generic_plan"})
        foreach(var size in new[]{1,8,16,32})
        {
            await using var tx=await db.BeginTransactionAsync();
            await using(var set=new NpgsqlCommand($"SET LOCAL plan_cache_mode={mode}",db,tx)) await set.ExecuteNonQueryAsync();
            var selected=JsonSerializer.Serialize(ids.Take(size).Select(id=>new { id,epoch=0L,worker="worker",capabilities=new[]{new{hub,task_type="activity",version=1}},pins=(object?)null }));
            await using var explain=new NpgsqlCommand("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) "+HubTaskStore.CandidateBatchAdmissionSql,db,tx);
            explain.Parameters.Add("selected",NpgsqlDbType.Jsonb).Value=selected;
            explain.Parameters.AddWithValue("hub",hub); explain.Parameters.AddWithValue("count",4); explain.Parameters.AddWithValue("partition",0);
            explain.Parameters.Add("lease",NpgsqlDbType.Interval).Value=TimeSpan.FromSeconds(30);
            await explain.PrepareAsync();
            using var plan=JsonDocument.Parse((string)(await explain.ExecuteScalarAsync())!);
            var nodes=EnumerateNodes(plan.RootElement).ToArray();
            var planRoot=plan.RootElement[0].GetProperty("Plan");
            Assert.Equal(size,planRoot.GetProperty("Actual Rows").GetInt32());
            Assert.Contains(nodes,node=>node.TryGetProperty("Node Type",out var type)&&type.GetString()=="LockRows");
            Assert.Contains(nodes,node=>node.TryGetProperty("Node Type",out var type)&&type.GetString()=="Sort");
            Assert.Contains(nodes,node=>node.TryGetProperty("Node Type",out var type)&&type.GetString()=="ModifyTable");
            var primaryKeyProbes=nodes.Where(node=>node.TryGetProperty("Index Name",out var index)&&index.GetString()=="hub_task_pkey").ToArray();
            Assert.True(primaryKeyProbes.Length>=2,
                $"Expected stable selected-ID primary-key probes in {mode}, batch {size}.");
            var probeLoops=primaryKeyProbes.Sum(node=>node.GetProperty("Actual Loops").GetInt32());
            Assert.InRange(probeLoops,size*2,size*3);
            var taskScans=nodes.Where(node=>node.TryGetProperty("Relation Name",out var relation)&&relation.GetString()=="hub_task"&&
                node.TryGetProperty("Node Type",out var type)&&type.GetString() is "Seq Scan" or "Index Scan" or "Index Only Scan" or "Bitmap Heap Scan").ToArray();
            var observedTaskRows=0;
            foreach(var scan in taskScans)
            {
                Assert.True(scan.TryGetProperty("Index Name",out var index)&&index.GetString()=="hub_task_pkey",
                    $"hub_task queue scan detected in {mode}, batch {size}: {scan.GetRawText()}");
                var rows=scan.GetProperty("Actual Rows").GetInt32();
                var loops=scan.GetProperty("Actual Loops").GetInt32();
                var filtered=scan.TryGetProperty("Rows Removed by Filter",out var removed)?removed.GetInt32():0;
                observedTaskRows+=(rows+filtered)*loops;
            }
            Assert.InRange(observedTaskRows,size*2,size*3);
            await tx.RollbackAsync();
        }
        }
        finally
        {
            // The synthetic backlog is committed to give the planner realistic
            // active statistics; clean only this uniquely named test hub.
            await using var cleanup=new NpgsqlCommand("DELETE FROM wfc_scale.hub_task WHERE hub_id=@hub",db);
            cleanup.Parameters.AddWithValue("hub",hub); await cleanup.ExecuteNonQueryAsync();
        }
    }

    private static IEnumerable<JsonElement> EnumerateNodes(JsonElement value)
    {
        if(value.ValueKind==JsonValueKind.Object)
        {
            if(value.TryGetProperty("Node Type",out _)) yield return value;
            foreach(var property in value.EnumerateObject())
                foreach(var child in EnumerateNodes(property.Value)) yield return child;
        }
        else if(value.ValueKind==JsonValueKind.Array)
            foreach(var child in value.EnumerateArray())
                foreach(var nested in EnumerateNodes(child)) yield return nested;
    }
}
