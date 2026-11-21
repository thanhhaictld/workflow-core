using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateConcurrentAdmissionTests
{
    [Fact]
    public async Task TwoPartitionsMayAdmitTogetherButTheThirdWaitsForCapacity()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        string[] names = ["WFC_HUB_MATCHING_ENABLED", "WFC_HUB_MATCHING_CANDIDATES_ENABLED", "WFC_HUB_MATCHING_NODE_ID",
            "WFC_HUB_MATCHING_ADVERTISE_URL", "WFC_HUB_MATCHING_PEERS", "WFC_HUB_MATCHING_PARTITIONS",
            "WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION", "WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY"];
        var old = names.ToDictionary(x => x, Environment.GetEnvironmentVariable);
        try
        {
            string[] values = ["true", "true", "admission-test", "http://localhost:5100", "http://localhost:5100", "64", "true", "2"];
            for (var i=0; i<names.Length; i++) Environment.SetEnvironmentVariable(names[i],values[i]);
            var hub = "concurrent-admission-" + Guid.NewGuid().ToString("N");
            await using var db = new NpgsqlConnection(database); await db.OpenAsync();
            await using (var insert = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
                SELECT gen_random_uuid(),@hub,p,'work',1,'{}' FROM generate_series(0,2) p
                """,db)) { insert.Parameters.AddWithValue("hub",hub); await insert.ExecuteNonQueryAsync(); }
            await using var store = new HubTaskStore(database);
            var ownership = new HubMatchingOwnership(new HubMatchingLeaseStore(store), new HubMatchingPartitionLeaseStore(store), NullLogger<HubMatchingOwnership>.Instance);
            var ready = new HubReadinessBroadcaster(store,new HubWorkflowStore(store,new HubTaskRouteRegistry(),new HubWorkflowRegistry()),NullLogger<HubReadinessBroadcaster>.Instance);
            var entered = new ConcurrentQueue<int>();
            var two = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var candidate = new HubCandidateAcquisition(store,ownership,ready,NullLogger<HubCandidateAcquisition>.Instance,null,true,
                async (owner,ct) => { entered.Enqueue(owner.Partition); if (entered.Count >= 2) two.TrySetResult(); await release.Task.WaitAsync(ct); });
            await ownership.StartAsync(default); await candidate.StartAsync(default);
            for (int i=0;i<3;i++) await ownership.AcquirePartitionAsync(hub,i,default);
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var requests = Enumerable.Range(0,3).Select(i => candidate.AcquireAsync(new("worker-"+i,[hub],[new(hub,"work",1)],1,null,i,64),stop.Token)).ToArray();
            await two.Task.WaitAsync(stop.Token);
            Assert.Equal(2,entered.Count); Assert.Equal(2,entered.Distinct().Count());
            await Task.Delay(150,stop.Token); Assert.Equal(2,entered.Count);
            release.TrySetResult();
            var results = await Task.WhenAll(requests).WaitAsync(stop.Token);
            Assert.All(results, result => Assert.Single(result));
            Assert.Equal(3,results.SelectMany(x => x).Select(x => x.Id).Distinct().Count());
            await candidate.StopAsync(default); await ownership.StopAsync(default);
        }
        finally { foreach (var name in names) Environment.SetEnvironmentVariable(name,old[name]); }
    }
}
