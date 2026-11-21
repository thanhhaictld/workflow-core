using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeCompletionPlanTests
{
    [Theory]
    [InlineData("force_custom_plan", false)]
    [InlineData("force_generic_plan", false)]
    [InlineData("force_custom_plan", true)]
    [InlineData("force_generic_plan", true)]
    public async Task ActivityCompletionLocksWorkflowAfterBoundedTaskLookup(string planMode, bool freshStatistics)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using var tx = await db.BeginTransactionAsync();
        await using (var seed = new NpgsqlCommand("""
            CREATE TEMP TABLE hub_workflow (
                id uuid PRIMARY KEY, hub_id text, partition_id integer, native_status text, execution_mode text
            );
            CREATE TEMP TABLE hub_task (
                id uuid PRIMARY KEY, workflow_id uuid, execution_pointer_id text, dispatch_key text,
                native_step_id integer, payload text
            );
            CREATE UNIQUE INDEX hub_task_dispatch ON hub_task(workflow_id,dispatch_key) WHERE dispatch_key IS NOT NULL;
            INSERT INTO pg_temp.hub_workflow VALUES ('00000000-0000-0000-0000-000000000001','plan',0,'Runnable','native');
            INSERT INTO pg_temp.hub_task(id,workflow_id,payload)
            SELECT (lpad(to_hex(sequence),8,'0') || '-0000-0000-0000-000000000000')::uuid,
                   '00000000-0000-0000-0000-000000000001', repeat('x',1200)
            FROM generate_series(1,2000) AS rows(sequence);
            ANALYZE pg_temp.hub_task;
            INSERT INTO pg_temp.hub_task(id,workflow_id,execution_pointer_id,dispatch_key,native_step_id,payload)
            SELECT (lpad(to_hex(sequence),8,'0') || '-0000-0000-0000-000000000000')::uuid,
                   '00000000-0000-0000-0000-000000000001', 'pointer',
                   sequence::text, 1, repeat('x',1200)
            FROM generate_series(2001,6000) AS rows(sequence);
            """, db, tx))
            await seed.ExecuteNonQueryAsync();
        if (freshStatistics)
        {
            await using var analyze = new NpgsqlCommand("ANALYZE pg_temp.hub_task", db, tx);
            await analyze.ExecuteNonQueryAsync();
        }
        await using (var setMode = new NpgsqlCommand($"SET LOCAL plan_cache_mode={planMode}", db, tx))
            await setMode.ExecuteNonQueryAsync();

        var sql = HubNativeWorkflowStore.LockActivityWorkflowSql.Replace("wfc_scale.", "pg_temp.");
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + sql, db, tx);
        explain.Parameters.AddWithValue("task", new Guid(2001, 0, 0, new byte[8]));
        await explain.PrepareAsync();
        using var json = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync())!);
        var root = json.RootElement[0].GetProperty("Plan");
        Assert.Equal("LockRows", root.GetProperty("Node Type").GetString());
        Assert.Equal(1, root.GetProperty("Actual Rows").GetInt32());
        var task = Assert.Single(EnumerateNodes(root), node =>
            node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "hub_task");
        Assert.Equal("Index Scan", task.GetProperty("Node Type").GetString());
        Assert.Equal("hub_task_pkey", task.GetProperty("Index Name").GetString());
        Assert.Contains("id =", task.GetProperty("Index Cond").GetString());
        Assert.Equal(1, task.GetProperty("Actual Rows").GetInt32());
        Assert.False(task.TryGetProperty("Rows Removed by Filter", out var removed) && removed.GetInt32() > 0);
    }

    private static IEnumerable<JsonElement> EnumerateNodes(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
            foreach (var child in children.EnumerateArray())
                foreach (var descendant in EnumerateNodes(child))
                    yield return descendant;
    }

    [Theory]
    [InlineData("force_custom_plan", false)]
    [InlineData("force_generic_plan", false)]
    [InlineData("force_custom_plan", true)]
    [InlineData("force_generic_plan", true)]
    public async Task CompletionUsesBoundedProbeAndPreservesFencing(string mode, bool freshStatistics)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        var schema = "completion_plan_" + Guid.NewGuid().ToString("N");
        async Task Execute(string sql)
        {
            await using var command = new NpgsqlCommand(sql, db);
            await command.ExecuteNonQueryAsync();
        }
        await Execute($"CREATE SCHEMA {schema}");
        try
        {
            // Analyze an empty leased index, then introduce a backlog without
            // refreshing it: this reproduces the observed production plan.
            await Execute($"""
                CREATE TABLE {schema}.hub_task (
                  id uuid PRIMARY KEY, hub_id text NOT NULL, sequence bigint NOT NULL,
                  state smallint NOT NULL,owner text,epoch bigint NOT NULL,
                  lease_until timestamptz,result_json jsonb,updated_at timestamptz);
                ALTER TABLE {schema}.hub_task SET (autovacuum_enabled=false);
                CREATE INDEX expired ON {schema}.hub_task(hub_id,lease_until,sequence) WHERE state=1;
                INSERT INTO {schema}.hub_task(id,hub_id,sequence,state,owner,epoch,lease_until)
                SELECT md5(g::text)::uuid,'test',g,2,'worker',1,clock_timestamp()+interval '1 hour'
                FROM generate_series(1,20000) g;
                ANALYZE {schema}.hub_task;
                UPDATE {schema}.hub_task SET state=1 WHERE sequence<=15000;
                SET plan_cache_mode={mode};
                """);
            if (freshStatistics) await Execute($"ANALYZE {schema}.hub_task");
            var completion = HubNativeWorkflowStore.CompleteTaskSql
                .Replace("wfc_scale.hub_task", $"{schema}.hub_task")
                .Replace("@result", "$1").Replace("@task", "$2")
                .Replace("@owner", "$3").Replace("@epoch", "$4");
            await Execute($"PREPARE complete_task(text,uuid,text,bigint) AS {completion}");
            var target = "(SELECT id FROM " + schema + ".hub_task WHERE sequence=15000)";
            await using var targetCommand = new NpgsqlCommand("SELECT " + target, db);
            var taskId = (Guid)(await targetCommand.ExecuteScalarAsync())!;
            await using (var tx = await db.BeginTransactionAsync())
            {
                await using var explain = new NpgsqlCommand($$"""
                    EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)
                    EXECUTE complete_task('{"ok":true}','{{taskId}}','worker',1)
                    """, db, tx);
                using var json = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync())!);
                var scan = json.RootElement[0].GetProperty("Plan").GetProperty("Plans")[0];
                Assert.Equal("Index Scan", scan.GetProperty("Node Type").GetString());
                Assert.Equal("hub_task_pkey", scan.GetProperty("Index Name").GetString());
                Assert.Equal(1, scan.GetProperty("Actual Rows").GetInt32());
                Assert.True(!scan.TryGetProperty("Rows Removed by Filter", out var removed) || removed.GetInt32()==0);
                await tx.RollbackAsync();
            }
            // Exercise the exact production predicate after every rejection,
            // retaining the same target row through transaction rollback.
            foreach (var rejection in new[] { "worker", "epoch", "expired", "ready", "completed", "dead", "missing" })
            {
                await using var tx = await db.BeginTransactionAsync();
                var mutation = rejection switch
                {
                    "expired" => "lease_until=clock_timestamp()-interval '1 second'",
                    "ready" => "state=0",
                    "completed" => "state=2",
                    "dead" => "state=3",
                    _ => null
                };
                if (mutation is not null)
                {
                    await using var mutate = new NpgsqlCommand($"UPDATE {schema}.hub_task SET {mutation} WHERE id='{taskId}'", db, tx);
                    await mutate.ExecuteNonQueryAsync();
                }
                var id = rejection=="missing" ? Guid.NewGuid() : taskId;
                var owner = rejection=="worker" ? "other" : "worker";
                var epoch = rejection=="epoch" ? 2 : 1;
                await using var ack = new NpgsqlCommand($"EXECUTE complete_task('{{}}','{id}','{owner}',{epoch})", db, tx);
                Assert.Equal(0, await ack.ExecuteNonQueryAsync());
                await tx.RollbackAsync();
            }
        }
        finally
        {
            await Execute("DEALLOCATE ALL; RESET plan_cache_mode;");
            await Execute($"DROP SCHEMA {schema} CASCADE");
        }
    }
}
