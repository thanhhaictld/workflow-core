using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

[Collection("CandidateAcquisitionRuntime")]
public sealed class HubTaskStoreFeatureGateTests
{
    [Fact]
    public async Task TaskIdProducerSessionSettingTracksTheCandidateFeatureGateBothWays()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var prior = Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_CANDIDATES_ENABLED");
        try
        {
            foreach (var (flag, expected) in new[] { ("false", false), ("true", true), ("false", false) })
            {
                Environment.SetEnvironmentVariable("WFC_HUB_MATCHING_CANDIDATES_ENABLED", flag);
                await using var store = new HubTaskStore(database);
                await using var query = store.DataSource.CreateCommand(
                    "SELECT current_setting('wfc.hub_candidate_task_id_hints',true)='on'");
                Assert.Equal(expected, (bool)(await query.ExecuteScalarAsync())!);
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable("WFC_HUB_MATCHING_CANDIDATES_ENABLED", prior);
        }
    }
}
