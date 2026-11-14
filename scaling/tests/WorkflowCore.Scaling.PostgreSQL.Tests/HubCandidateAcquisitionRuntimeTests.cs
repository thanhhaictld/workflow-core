using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

// Environment switches are process-scoped; keep this test isolated from other collections.
[CollectionDefinition("CandidateAcquisitionRuntime", DisableParallelization = true)]
public sealed class CandidateAcquisitionRuntimeCollection { }

[Collection("CandidateAcquisitionRuntime")]
public sealed class HubCandidateAcquisitionRuntimeTests
{
    [Fact]
    public async Task ConfirmedMatchingCreditsAreRetiredWithoutWaitingForFortyFiveSecondLeases()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var keys = new[] { "WFC_HUB_MATCHING_ENABLED", "WFC_HUB_MATCHING_CANDIDATES_ENABLED",
            "WFC_HUB_MATCHING_NODE_ID", "WFC_HUB_MATCHING_ADVERTISE_URL",
            "WFC_HUB_MATCHING_PEERS", "WFC_HUB_MATCHING_PARTITIONS",
            "WFC_HUB_MATCHING_ADMISSION_CONCURRENCY" };
        var original = keys.ToDictionary(k => k, Environment.GetEnvironmentVariable);
        try
        {
            Environment.SetEnvironmentVariable(keys[0], "true");
            Environment.SetEnvironmentVariable(keys[1], "true");
            Environment.SetEnvironmentVariable(keys[2], "candidate-credit-test");
            Environment.SetEnvironmentVariable(keys[3], "http://localhost:5100");
            Environment.SetEnvironmentVariable(keys[4], "http://localhost:5100");
            // One candidate stream/bucket, reducing test time and noise.
            Environment.SetEnvironmentVariable(keys[5], "64");
            Environment.SetEnvironmentVariable(keys[6], "3");

            var hub = "candidate-credit-" + Guid.NewGuid().ToString("N");
            await using var tasks = new HubTaskStore(database, 12);
            var policies = new HubPolicyRegistry().Register(hub, "work", 1, 1, _ => { });
            var ownership = new HubMatchingOwnership(new HubMatchingLeaseStore(tasks),
                new HubMatchingPartitionLeaseStore(tasks),
                NullLogger<HubMatchingOwnership>.Instance);
            var readiness = new HubReadinessBroadcaster(tasks,
                new HubWorkflowStore(tasks, policies, new HubWorkflowRegistry()),
                NullLogger<HubReadinessBroadcaster>.Instance);
            var matcher = new HubCandidateAcquisition(tasks, ownership, readiness,
                NullLogger<HubCandidateAcquisition>.Instance);
            await ownership.StartAsync(default);
            await matcher.StartAsync(default);
            try
            {
                var owner = await ownership.AcquirePartitionAsync(hub, 0, default);
                Assert.True(ownership.IsLocal(owner));
                Assert.True(matcher.Enabled);
                var capability = new HubCapability(hub, "work", 1);
                var payload = JsonSerializer.SerializeToElement(new { message = "credit-lifetime" });
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(40));
                for (var i = 0; i < 12; i++)
                {
                    var expected = await tasks.EnqueueAsync(policies.Resolve(hub, "work", 1, payload),
                        payload.GetRawText(), null, null, deadline.Token);
                    readiness.PublishMatchingHint(hub, "work", 1, 0);
                    var leased = Assert.Single(await matcher.AcquireAsync(
                        new HubClaimRequest("worker", [hub], [capability], 1,
                            MatchingPartition: 0, MatchingPartitionCount: 64),
                        deadline.Token));
                    Assert.Equal(expected, leased.Id);
                    Assert.True(await tasks.CompleteAsync(leased.Id, "worker", leased.Epoch,
                        "{}", deadline.Token));

                    // The transport has already received the lease. Candidate
                    // bookkeeping must not retain its 45-second matching credit.
                    for (var wait = 0; wait < 100 && matcher.CandidateCredits != 0; wait++)
                        await Task.Delay(10, deadline.Token);
                    Assert.Equal(0, matcher.CandidateCredits);
                    Assert.Equal(0, matcher.CandidateUncertain);
                }
            }
            finally
            {
                await matcher.StopAsync(default);
                matcher.Dispose();
                await ownership.StopAsync(default);
                ownership.Dispose();
            }
        }
        finally
        {
            foreach (var key in keys) Environment.SetEnvironmentVariable(key, original[key]);
        }
    }
}
