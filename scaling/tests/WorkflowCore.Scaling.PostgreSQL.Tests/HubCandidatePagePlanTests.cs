using System.Text;
using System.Text.RegularExpressions;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidatePagePlanTests
{
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
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@resource,1,false);
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@task,@resource);
            ANALYZE wfc_scale.hub_task;
            SET plan_cache_mode=force_generic_plan;
            """,db)) { seed.Parameters.AddWithValue("hub",hub); seed.Parameters.AddWithValue("workflow",Guid.NewGuid());
          seed.Parameters.AddWithValue("task",Guid.NewGuid()); seed.Parameters.AddWithValue("resource",hub+"-resource"); await seed.ExecuteNonQueryAsync(); }
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
                var call = $"EXPLAIN (ANALYZE, BUFFERS) EXECUTE candidate_page('{hub}',{bucket},9223372036854775807,now(),0,256,1048576" + (deadline ? ",'2000-01-01 00:00:00+00'" : "") + ")";
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
