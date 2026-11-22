using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidatePagePlanTests
{
    [Theory]
    [InlineData("force_custom_plan", false)]
    [InlineData("force_generic_plan", false)]
    [InlineData("force_custom_plan", true)]
    [InlineData("force_generic_plan", true)]
    public async Task CandidatePageUsesBucketAndDeadlineBoundsAfterStaleStatisticsBurst(string planMode, bool ready)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using var tx = await db.BeginTransactionAsync();
        await using (var seed = new NpgsqlCommand("""
            CREATE TEMP TABLE hub_task
                (LIKE wfc_scale.hub_task INCLUDING DEFAULTS INCLUDING IDENTITY);
            -- Copy the migrated index and its real competitors into an isolated fixture.
            DO $$
            DECLARE definition text;
            BEGIN
                FOR definition IN SELECT indexdef FROM pg_indexes
                    WHERE schemaname='wfc_scale' AND tablename='hub_task'
                LOOP
                    EXECUTE replace(definition,' ON wfc_scale.hub_task ',' ON pg_temp.hub_task ');
                END LOOP;
            END $$;
            INSERT INTO pg_temp.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state)
            SELECT gen_random_uuid(),'plan',g%64,'activity',1,'{}',2 FROM generate_series(1,20000) g;
            ANALYZE pg_temp.hub_task;
            INSERT INTO pg_temp.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,due_at,lease_until)
            SELECT gen_random_uuid(),'plan',g%64,'activity',1,'{}',@state,@owner,'2026-01-02T00:00:00Z',
                   CASE WHEN @state=1 THEN '2026-01-02T00:00:00Z'::timestamptz END
            FROM generate_series(1,16000) g;
            INSERT INTO pg_temp.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,due_at,lease_until)
            VALUES (gen_random_uuid(),'plan',6,'activity',1,'{}',@state,@owner,'2025-12-31T00:00:00Z',
                    CASE WHEN @state=1 THEN '2025-12-31T00:00:00Z'::timestamptz END);
            """, db, tx))
        {
            seed.Parameters.AddWithValue("state", (short)(ready ? 0 : 1));
            seed.Parameters.Add("owner", NpgsqlTypes.NpgsqlDbType.Text).Value = ready ? DBNull.Value : "worker";
            await seed.ExecuteNonQueryAsync();
        }
        await using (var setMode = new NpgsqlCommand($"SET LOCAL plan_cache_mode={planMode}", db, tx))
            await setMode.ExecuteNonQueryAsync();

        var stream = ready ? HubCandidateStream.ReadyDue : HubCandidateStream.Expired;
        var expectedIndex = ready ? "ix_hub_task_candidate_ready_bucket_due" : "ix_hub_task_candidate_expired_bucket_deadline";
        var expectedDeadlineBound = ready ? "(due_at <=" : "(lease_until <";
        var sql = HubCandidateReader.BuildPageSql(stream).Replace("wfc_scale.", "pg_temp.");
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE,BUFFERS,TIMING OFF,FORMAT JSON) " + sql, db, tx);
        explain.Parameters.AddWithValue("hub", "plan");
        var bucket = explain.Parameters.AddWithValue("bucket", 0);
        explain.Parameters.AddWithValue("upper", 36_001L);
        explain.Parameters.AddWithValue("cutoff", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        explain.Parameters.AddWithValue("afterDeadline", DateTime.MinValue);
        explain.Parameters.AddWithValue("after", 0L);
        explain.Parameters.AddWithValue("limit", 128);
        explain.Parameters.AddWithValue("byteLimit", 65_536);
        await explain.PrepareAsync();

        foreach (var (partition, expectedRows) in new[] { (0, 0), (6, 1) })
        {
            bucket.Value = partition;
            var result = (string)(await explain.ExecuteScalarAsync())!;
            using var plan = JsonDocument.Parse(result);
            var root = plan.RootElement[0].GetProperty("Plan");
            Assert.Equal(expectedRows, root.GetProperty("Actual Rows").GetInt32());
            Assert.Contains(expectedIndex, result);
            Assert.Contains("((partition_id % 64) =", result);
            Assert.Contains(expectedDeadlineBound, result);
            var blocks = root.GetProperty("Local Hit Blocks").GetInt64() + root.GetProperty("Local Read Blocks").GetInt64();
            Assert.InRange(blocks, 1, 32);
        }
    }

    [Fact]
    public async Task GenericPreparedFullPageUsesBucketIndexes()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-plan-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var seed = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,due_at)
            SELECT gen_random_uuid(),@hub,0,'activity',1,'{}',CASE WHEN g%2000=0 THEN now()-interval '1 minute' ELSE now()+interval '1 day' END FROM generate_series(1,20000) g;
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,lease_until)
            SELECT gen_random_uuid(),@hub,0,'activity',1,'{}',1,'worker',CASE WHEN g%2000=0 THEN now()-interval '1 minute' ELSE now()+interval '1 day' END FROM generate_series(1,20000) g;
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            SELECT gen_random_uuid(),@hub,2,'activity',1,'{}' FROM generate_series(1,20);
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count)
            VALUES (@workflow,@hub,'plan-definition',1,0,'{}',1);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload)
            VALUES (@task,@hub,0,'activity',1,@workflow,'{}');
            ANALYZE wfc_scale.hub_task;
            SET plan_cache_mode=force_generic_plan;
            """,db)) { seed.Parameters.AddWithValue("hub",hub); seed.Parameters.AddWithValue("workflow",Guid.NewGuid());
          seed.Parameters.AddWithValue("task",Guid.NewGuid()); await seed.ExecuteNonQueryAsync(); }
        var output = new StringBuilder();
        foreach (var stream in new[] { HubCandidateStream.Ready, HubCandidateStream.ReadyDue, HubCandidateStream.Expired })
        {
            var sql = HubCandidateReader.BuildPageSql(stream);
            string[] names = ["hub","bucket","upper","cutoff","after","limit","byteLimit","afterDeadline"];
            for (var i = 0; i < names.Length; i++)
                sql = Regex.Replace(sql, "@" + names[i] + @"\b", "$" + (i+1), RegexOptions.CultureInvariant);
            var deadline = stream != HubCandidateStream.Ready;
            var types = "text,integer,bigint,timestamp with time zone,bigint,integer,integer" + (deadline ? ",timestamp with time zone" : "");
            var prepare = $"PREPARE candidate_page({types}) AS {sql}";
            await using (var cmd = new NpgsqlCommand(prepare,db)) await cmd.ExecuteNonQueryAsync();
            output.AppendLine($"--- {stream}: actual reader SQL, generic prepared parameters ({types}) ---").AppendLine(prepare).AppendLine(";");
            foreach (var bucket in new[] { 0, 1, 2 })
            {
                // Production reserves 16 ID bytes per effective row before
                // passing the SQL-side base-metadata budget.
                var productionPageBudget = 1048576 - 256 * 16;
                var call = $"EXPLAIN (ANALYZE, BUFFERS) EXECUTE candidate_page('{hub}',{bucket},9223372036854775807,now(),0,256,{productionPageBudget}" + (deadline ? ",'2000-01-01 00:00:00+00'" : "") + ")";
                var plan = new StringBuilder(); await using var cmd = new NpgsqlCommand(call,db);
                await using (var r = await cmd.ExecuteReaderAsync()) while (await r.ReadAsync()) plan.AppendLine(r.GetString(0));
                output.AppendLine($"--- bucket {bucket} ---").AppendLine(call).AppendLine(plan.ToString());
                var expected = stream switch { HubCandidateStream.Ready => "ix_hub_task_candidate_ready_bucket_sequence", HubCandidateStream.ReadyDue => "ix_hub_task_candidate_ready_bucket_due", _ => "ix_hub_task_candidate_expired_bucket_deadline" };
                Assert.Contains(expected,plan.ToString());
            }
            await using var deallocate = new NpgsqlCommand("DEALLOCATE candidate_page",db); await deallocate.ExecuteNonQueryAsync();
        }
        var path = Environment.GetEnvironmentVariable("WFC_CANDIDATE_PLAN_ARTIFACT");
        if (!string.IsNullOrWhiteSpace(path)) await File.WriteAllTextAsync(path,output.ToString());
    }
}
