using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateBackpressureTests
{
    [Fact]
    public async Task MoreThanOnePassOfIneligibleDemandDoesNotStarveCompatibleWorker()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        string[] names = ["WFC_HUB_MATCHING_ENABLED", "WFC_HUB_MATCHING_CANDIDATES_ENABLED",
            "WFC_HUB_MATCHING_NODE_ID", "WFC_HUB_MATCHING_ADVERTISE_URL", "WFC_HUB_MATCHING_PEERS", "WFC_HUB_MATCHING_PARTITIONS"];
        var old = names.ToDictionary(x => x, Environment.GetEnvironmentVariable);
        try
        {
            string[] values = ["true", "true", "fairness-test", "http://localhost:5100", "http://localhost:5100", "64"];
            for (var i = 0; i < names.Length; i++) Environment.SetEnvironmentVariable(names[i], values[i]);
            var hub = "bounded-pass-" + Guid.NewGuid().ToString("N"); var id = Guid.NewGuid();
            await using (var db = new NpgsqlConnection(database))
            {
                await db.OpenAsync();
                await using var insert = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'target',1,'{}')", db);
                insert.Parameters.AddWithValue("id", id); insert.Parameters.AddWithValue("hub", hub); await insert.ExecuteNonQueryAsync();
            }
            await using var store = new HubTaskStore(database);
            var ownership = new HubMatchingOwnership(new HubMatchingLeaseStore(store), new HubMatchingPartitionLeaseStore(store), NullLogger<HubMatchingOwnership>.Instance);
            var readiness = new HubReadinessBroadcaster(store, new HubWorkflowStore(store, new HubTaskRouteRegistry(), new HubWorkflowRegistry()), NullLogger<HubReadinessBroadcaster>.Instance);
            var candidate = new HubCandidateAcquisition(store, ownership, readiness, NullLogger<HubCandidateAcquisition>.Instance, null, true);
            await ownership.StartAsync(default); await candidate.StartAsync(default); await ownership.AcquirePartitionAsync(hub, 0, default);
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var ineligible = Enumerable.Range(0, 129).Select(i => candidate.AcquireAsync(
                new("other-" + i, [hub], [new(hub, "other", 1)], 1, null, 0, 64), stop.Token)).ToArray();
            var eligible = candidate.AcquireAsync(new("target-worker", [hub], [new(hub, "target", 1)], 1, null, 0, 64), stop.Token);
            var lease = Assert.Single(await eligible.WaitAsync(stop.Token)); Assert.Equal(id, lease.Id);
            Assert.Equal(129, candidate.DemandSnapshot().Waiting);
            stop.Cancel();
            foreach (var request in ineligible) await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request);
            Assert.Equal(0, candidate.DemandSnapshot().Waiting);
            await candidate.StopAsync(default); await ownership.StopAsync(default);
        }
        finally { foreach (var name in names) Environment.SetEnvironmentVariable(name, old[name]); }
    }

    [Fact]
    public async Task SaturationQueuesWithoutFallbackAndWakesInOrderAfterCancellation()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        string[] names = ["WFC_HUB_MATCHING_ENABLED", "WFC_HUB_MATCHING_CANDIDATES_ENABLED",
            "WFC_HUB_MATCHING_NODE_ID", "WFC_HUB_MATCHING_ADVERTISE_URL", "WFC_HUB_MATCHING_PEERS", "WFC_HUB_MATCHING_PARTITIONS",
            "WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION"];
        var old = names.ToDictionary(x => x, Environment.GetEnvironmentVariable);
        try
        {
            string[] values = ["true", "true", "backpressure-test", "http://localhost:5100", "http://localhost:5100", "64", "true"];
            for (int i=0; i<names.Length; i++) Environment.SetEnvironmentVariable(names[i], values[i]);
            var hub = "backpressure-" + Guid.NewGuid().ToString("N");
            await using (var db = new NpgsqlConnection(database))
            {
                await db.OpenAsync();
                await using var insert = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'work',1,'{}')", db);
                insert.Parameters.AddWithValue("id", Guid.NewGuid()); insert.Parameters.AddWithValue("hub", hub);
                await insert.ExecuteNonQueryAsync();
            }
            await using var store = new HubTaskStore(database);
            var ownership = new HubMatchingOwnership(new HubMatchingLeaseStore(store), new HubMatchingPartitionLeaseStore(store),
                NullLogger<HubMatchingOwnership>.Instance);
            var readiness = new HubReadinessBroadcaster(store,
                new HubWorkflowStore(store, new HubTaskRouteRegistry(), new HubWorkflowRegistry()), NullLogger<HubReadinessBroadcaster>.Instance);
            var options = new HubCandidateCoordinatorOptions(PartitionCredits: 1, ProcessCredits: 1);
            var admissionBarrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var candidate = new HubCandidateAcquisition(store, ownership, readiness, NullLogger<HubCandidateAcquisition>.Instance, options, true,
                (_, ct) => admissionBarrier.Task.WaitAsync(ct));
            await ownership.StartAsync(default); await candidate.StartAsync(default);
            var owner = await ownership.AcquirePartitionAsync(hub, 0, default);
            async Task Until(Func<bool> condition)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                while (!condition()) await Task.Delay(10, timeout.Token);
            }
            HubClaimRequest Request(string worker) => new(worker, [hub], [new(hub,"work",1)], 1, null, 0, 64);
            using var firstCancel = new CancellationTokenSource(); using var secondCancel = new CancellationTokenSource();
            using var thirdCancel = new CancellationTokenSource();
            var first = candidate.AcquireAsync(Request("first"), firstCancel.Token);
            await Until(() => candidate.AllocatedDemandWorkers().Contains("first"));
            var second = candidate.AcquireAsync(Request("second"), secondCancel.Token);
            var third = candidate.AcquireAsync(Request("third"), thirdCancel.Token);
            await Until(() => candidate.DemandSnapshot().Waiting == 2);
            Assert.Equal((2, 1), candidate.DemandSnapshot());
            Assert.False(second.IsCompleted); Assert.False(third.IsCompleted);
            firstCancel.Cancel(); await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
            await Until(() => candidate.AllocatedDemandWorkers().Contains("second"));
            Assert.False(third.IsCompleted);
            secondCancel.Cancel(); await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second);
            await Until(() => candidate.AllocatedDemandWorkers().Contains("third"));
            thirdCancel.Cancel(); await Assert.ThrowsAnyAsync<OperationCanceledException>(() => third);
            await Until(() => candidate.DemandSnapshot() == (0, 0));
            using var lostFirstCancel = new CancellationTokenSource();
            using var lostWaitingCancel = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var lostFirst = candidate.AcquireAsync(Request("lost-first"), lostFirstCancel.Token);
            await Until(() => candidate.AllocatedDemandWorkers().Contains("lost-first"));
            var lostWaiting = candidate.AcquireAsync(Request("lost-waiter"), lostWaitingCancel.Token);
            await Until(() => candidate.DemandSnapshot().Waiting == 1);
            var leases = new HubMatchingPartitionLeaseStore(store);
            Assert.True(await leases.ReleaseAsync(hub, 0, owner.OwnerId, owner.Epoch, default));
            var takeover = await leases.AcquireOrReadAsync(hub, 0, 64, "takeover", "http://localhost:5101", TimeSpan.FromSeconds(20), default);
            Assert.True(takeover.Epoch > owner.Epoch);
            await Assert.ThrowsAsync<InvalidOperationException>(() => lostWaiting);
            lostFirstCancel.Cancel(); await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lostFirst);
            await Until(() => candidate.DemandSnapshot() == (0, 0));
            await candidate.StopAsync(default); await ownership.StopAsync(default);
        }
        finally { foreach (var name in names) Environment.SetEnvironmentVariable(name, old[name]); }
    }
}
