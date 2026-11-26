using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateHydrationPlanTests
{
    [Fact]
    public async Task ProductionHydrationSqlPlanUsesTheProductionIdAndByteParameters()
    {
        // This is an opt-in evidence test because it seeds a realistic 20k-row
        // backlog and writes an EXPLAIN artifact. Ordinary suite runs stay small.
        var output = Environment.GetEnvironmentVariable("WFC_CANDIDATE_PLAN_OUTPUT");
        if (string.IsNullOrWhiteSpace(output)) return;
        var connection = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for plan reproduction");
        var hub = "candidate-plan-" + Guid.NewGuid().ToString("N");
        await using var store = new HubTaskStore(connection);
        Guid[] ids;
        await using (var db = new NpgsqlConnection(connection))
        {
            await db.OpenAsync();
            await using (var seed = new NpgsqlCommand("""
                BEGIN;
                SET LOCAL wfc.hub_notification_outbox='off';
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,due_at)
                SELECT gen_random_uuid(),@hub,0,'plan-work',1,'{}',0,clock_timestamp()
                FROM generate_series(1,20000);
                ANALYZE wfc_scale.hub_task;
                COMMIT;
                """, db))
            {
                seed.Parameters.AddWithValue("hub", hub);
                await seed.ExecuteNonQueryAsync();
            }
            await using var pick = new NpgsqlCommand("SELECT id FROM wfc_scale.hub_task WHERE hub_id=@hub ORDER BY sequence LIMIT 128", db);
            pick.Parameters.AddWithValue("hub", hub);
            await using var reader = await pick.ExecuteReaderAsync();
            var selected = new List<Guid>();
            while (await reader.ReadAsync()) selected.Add(reader.GetGuid(0));
            ids = selected.ToArray();
            Assert.Equal(128, ids.Length);
        }

        const int requestedByteLimit = 65536;
        var sqlBudget = HubCandidateReader.HydrationMetadataBudget(requestedByteLimit, ids.Length);
        var sql = HubCandidateReader.HydrationSql
            .Replace("@ids", "$1", StringComparison.Ordinal)
            .Replace("@hub", "$2", StringComparison.Ordinal)
            .Replace("@matchingPartition", "$3", StringComparison.Ordinal)
            .Replace("@partitionCount", "$4", StringComparison.Ordinal)
            .Replace("@perCandidateByteLimit", "$5", StringComparison.Ordinal);
        var literalIds = "ARRAY[" + string.Join(',', ids.Select(id => $"'{id:D}'::uuid")) + "]::uuid[]";
        string plan;
        await using (var db = new NpgsqlConnection(connection))
        {
            await db.OpenAsync();
            await using (var mode = new NpgsqlCommand("SET plan_cache_mode='force_generic_plan'", db))
                await mode.ExecuteNonQueryAsync();
            await using (var prepare = new NpgsqlCommand(
                $"PREPARE candidate_hydration(uuid[],text,integer,integer,integer) AS {sql}", db))
                await prepare.ExecuteNonQueryAsync();
            await using var explain = new NpgsqlCommand($"""
                EXPLAIN (ANALYZE,BUFFERS,SETTINGS,FORMAT JSON)
                  EXECUTE candidate_hydration({literalIds},'{hub}',0,1,{Math.Max(0, sqlBudget / ids.Length)})
                """, db);
            await using var rows = await explain.ExecuteReaderAsync();
            Assert.True(await rows.ReadAsync());
            plan = rows.GetString(0);
            await rows.CloseAsync();
            await using var deallocate = new NpgsqlCommand("DEALLOCATE candidate_hydration", db);
            await deallocate.ExecuteNonQueryAsync();
        }
        using (var parsedPlan = JsonDocument.Parse(plan))
        {
            Assert.True(ContainsPlanNode(parsedPlan.RootElement,
                node => node.TryGetProperty("Relation Name", out var relation) &&
                        relation.GetString() == "hub_task" &&
                        node.TryGetProperty("Index Name", out var index) &&
                        index.GetString() == "hub_task_pkey"),
                "Hydration must use bounded primary-key probes for requested task IDs");
            Assert.False(ContainsPlanNode(parsedPlan.RootElement,
                node => node.TryGetProperty("Node Type", out var kind) && kind.GetString() == "Seq Scan" &&
                        node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "hub_task"),
                "Hydration must not scan the hub_task backlog");
        }

        var stopwatch = Stopwatch.StartNew();
        var hydrated = await new HubCandidateReader(store).HydrateAsync(hub, 0, 1,
            ids, requestedByteLimit, CancellationToken.None);
        stopwatch.Stop();
        var artifact = new
        {
            hub,
            requestedIdCount = ids.Length,
            requestedByteLimit,
            productionSqlByteLimit = sqlBudget,
            productionSqlSha256 = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(HubCandidateReader.HydrationSql))),
            hydrationRows = hydrated.Items.Count,
            hydrationMetadataBytes = hydrated.MetadataBytes,
            productionHydrationElapsedMs = stopwatch.Elapsed.TotalMilliseconds,
            genericPreparedPlan = JsonDocument.Parse(plan).RootElement.Clone()
        };
        await File.WriteAllTextAsync(output, JsonSerializer.Serialize(artifact,
            new JsonSerializerOptions { WriteIndented = true }));
        await using (var cleanup = store.DataSource.CreateCommand("DELETE FROM wfc_scale.hub_task WHERE hub_id=@hub"))
        {
            cleanup.Parameters.AddWithValue("hub", hub);
            await cleanup.ExecuteNonQueryAsync();
        }
        Assert.Equal(128, hydrated.Items.Count);
    }

    private static bool ContainsPlanNode(JsonElement value, Func<JsonElement, bool> predicate)
    {
        if (value.ValueKind == JsonValueKind.Object)
        {
            if (predicate(value)) return true;
            foreach (var property in value.EnumerateObject())
                if (ContainsPlanNode(property.Value, predicate)) return true;
        }
        else if (value.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in value.EnumerateArray())
                if (ContainsPlanNode(item, predicate)) return true;
        }
        return false;
    }
}
