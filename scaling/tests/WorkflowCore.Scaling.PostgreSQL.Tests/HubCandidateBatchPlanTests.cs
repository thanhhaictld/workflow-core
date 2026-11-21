using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateBatchPlanTests
{
    [Theory]
    [InlineData(4000, false)]
    [InlineData(4000, true)]
    [InlineData(25000, true)]
    public async Task SelectedIdBatchSqlUsesBoundedPrimaryKeyProbes(int backlog, bool freshStatistics)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using var tx = await db.BeginTransactionAsync();

        // A few thousand ready rows are enough to expose a full-table update plan.
        // Temporary tables give each case its own indexes and planner statistics.
        await ExecuteAsync("CREATE TEMP TABLE hub_task (LIKE wfc_scale.hub_task INCLUDING INDEXES)");
        await SeedAsync(1, freshStatistics ? backlog : 100);
        await ExecuteAsync("ANALYZE pg_temp.hub_task");
        if (!freshStatistics)
            await SeedAsync(101, backlog);

        var sql = HubTaskStore.CandidateBatchAdmissionSql.Replace("wfc_scale.hub_task", "pg_temp.hub_task");
        foreach (var mode in new[] { "force_custom_plan", "force_generic_plan" })
        foreach (var size in new[] { 1, 8, 16, 32 })
        {
            await tx.SaveAsync("claim");
            await ExecuteAsync($"SET LOCAL plan_cache_mode={mode}");
            var selected = JsonSerializer.Serialize(Enumerable.Range(1, size).Select(sequence => new
            {
                id = new Guid(sequence, 0, 0, new byte[8]),
                epoch = 0L,
                worker = "worker",
                capabilities = new[] { new { hub = "plan", task_type = "activity", version = 1 } },
                pins = (object?)null
            }));
            await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + sql, db, tx);
            explain.Parameters.Add("selected", NpgsqlDbType.Jsonb).Value = selected;
            explain.Parameters.AddWithValue("hub", "plan");
            explain.Parameters.AddWithValue("count", 4);
            explain.Parameters.AddWithValue("partition", 0);
            explain.Parameters.Add("lease", NpgsqlDbType.Interval).Value = TimeSpan.FromSeconds(30);
            await explain.PrepareAsync();
            using var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync())!);
            var root = plan.RootElement[0].GetProperty("Plan");
            var nodes = EnumerateNodes(root).ToArray();

            Assert.Equal(size, root.GetProperty("Actual Rows").GetInt32());
            Assert.Equal("ModifyTable", root.GetProperty("Node Type").GetString());
            Assert.Contains(nodes, node => node.GetProperty("Node Type").GetString() == "LockRows");
            Assert.Contains(nodes, node => node.GetProperty("Node Type").GetString() == "Sort");

            var taskScans = nodes.Where(node =>
                node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "hub_task" &&
                node.GetProperty("Node Type").GetString()!.EndsWith("Scan", StringComparison.Ordinal));
            var observedTaskRows = 0;
            foreach (var scan in taskScans)
            {
                var scanType = scan.GetProperty("Node Type").GetString();
                var indexScan = scanType == "Bitmap Heap Scan" ? scan.GetProperty("Plans")[0] : scan;
                Assert.True(indexScan.TryGetProperty("Index Name", out var index) && index.GetString() == "hub_task_pkey",
                    $"Unbounded task scan in {mode}, batch {size}: {scan.GetRawText()}");
                Assert.True(indexScan.TryGetProperty("Index Cond", out var condition) &&
                    condition.GetString()!.Contains("id =", StringComparison.Ordinal));

                var rows = scan.GetProperty("Actual Rows").GetInt32();
                var loops = scan.GetProperty("Actual Loops").GetInt32();
                var filtered = scan.TryGetProperty("Rows Removed by Filter", out var removed) ? removed.GetInt32() : 0;
                var rechecked = scan.TryGetProperty("Rows Removed by Index Recheck", out var rejected) ? rejected.GetInt32() : 0;
                observedTaskRows += (rows + filtered + rechecked) * loops;
            }
            Assert.InRange(observedTaskRows, size * 2, size * 3);
            await tx.RollbackAsync("claim");
            await tx.ReleaseAsync("claim");
        }

        async Task ExecuteAsync(string commandText)
        {
            await using var command = new NpgsqlCommand(commandText, db, tx);
            await command.ExecuteNonQueryAsync();
        }

        async Task SeedAsync(int first, int last)
        {
            await using var command = new NpgsqlCommand("""
                INSERT INTO pg_temp.hub_task
                    (id,sequence,hub_id,partition_id,task_type,task_version,payload,state,attempts,max_attempts,due_at,created_at,updated_at,epoch)
                SELECT (lpad(to_hex(sequence),8,'0') || '-0000-0000-0000-000000000000')::uuid,
                       sequence,'plan',0,'activity',1,'{}',0,0,5,now(),now(),now(),0
                FROM generate_series(@first,@last) AS rows(sequence)
                """, db, tx);
            command.Parameters.AddWithValue("first", first);
            command.Parameters.AddWithValue("last", last);
            await command.ExecuteNonQueryAsync();
        }
    }

    private static IEnumerable<JsonElement> EnumerateNodes(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
            foreach (var child in children.EnumerateArray())
                foreach (var descendant in EnumerateNodes(child))
                    yield return descendant;
    }
}
