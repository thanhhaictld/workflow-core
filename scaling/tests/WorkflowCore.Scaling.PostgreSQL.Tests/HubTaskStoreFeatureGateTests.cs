using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

[Collection("CandidateAcquisitionRuntime")]
public sealed class HubTaskStoreFeatureGateTests
{
    [Fact]
    public async Task RemovedTaskIdHintEnvironmentVariableDoesNotChangeConnectionOptions()
    {
        var prior = Environment.GetEnvironmentVariable("WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED");
        try
        {
            foreach (var flag in new[] { "false", "true" })
            {
                Environment.SetEnvironmentVariable("WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED", flag);
                await using var store = new HubTaskStore("Host=localhost;Database=workflow;Username=workflow;Password=workflow");
                Assert.DoesNotContain("hub_candidate_task_id_hints", store.DataSource.ConnectionString,
                    StringComparison.OrdinalIgnoreCase);
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable("WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED", prior);
        }
    }
}
