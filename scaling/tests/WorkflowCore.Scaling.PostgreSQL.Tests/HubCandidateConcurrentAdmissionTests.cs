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
    public async Task SameShardCanAdmitTwoDistinctTasksWhileBothSlotsAreOccupied()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ??
            throw new InvalidOperationException("WFC_TEST_PG is required");
        string[] names = ["WFC_HUB_MATCHING_ENABLED", "WFC_HUB_MATCHING_CANDIDATES_ENABLED",
            "WFC_HUB_MATCHING_NODE_ID", "WFC_HUB_MATCHING_ADVERTISE_URL",
            "WFC_HUB_MATCHING_PEERS", "WFC_HUB_MATCHING_PARTITIONS",
            "WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION",
            "WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY",
            "WFC_HUB_CANDIDATE_ADMISSION_PER_PARTITION",
            "WFC_HUB_CANDIDATE_BATCH_SIZE", "WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH"];
        var old = names.ToDictionary(x => x, Environment.GetEnvironmentVariable);
        try
        {
            string[] values = ["true", "true", "same-shard-test", "http://localhost:5100",
                "http://localhost:5100", "64", "true", "2", "2", "1", "false"];
            for (var i = 0; i < names.Length; i++)
                Environment.SetEnvironmentVariable(names[i], values[i]);
            var hub = "same-shard-" + Guid.NewGuid().ToString("N");
            await using var db = new NpgsqlConnection(database);
            await db.OpenAsync();
            await using (var insert = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
                SELECT gen_random_uuid(),@hub,0,'work',1,'{}' FROM generate_series(1,2)
                """, db))
            {
                insert.Parameters.AddWithValue("hub", hub);
                await insert.ExecuteNonQueryAsync();
            }
            await using var store = new HubTaskStore(database);
            var ownership = new HubMatchingOwnership(new HubMatchingLeaseStore(store),
                new HubMatchingPartitionLeaseStore(store), NullLogger<HubMatchingOwnership>.Instance);
            var ready = new HubReadinessBroadcaster(store,
                new HubWorkflowStore(store, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
                NullLogger<HubReadinessBroadcaster>.Instance);
            var entered = new ConcurrentQueue<int>();
            var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var candidate = new HubCandidateAcquisition(store, ownership, ready,
                NullLogger<HubCandidateAcquisition>.Instance, null, true,
                async (owner, ct) =>
                {
                    entered.Enqueue(owner.Partition);
                    if (entered.Count >= 2) both.TrySetResult();
                    await release.Task.WaitAsync(ct);
                });
            await ownership.StartAsync(default);
            await candidate.StartAsync(default);
            try
            {
                await ownership.AcquirePartitionAsync(hub, 0, default);
                using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
                var claims = Enumerable.Range(0, 2).Select(i =>
                    candidate.AcquireAsync(new("worker-" + i, [hub], [new(hub, "work", 1)], 1,
                        null, 0, 64), stop.Token)).ToArray();
                await both.Task.WaitAsync(stop.Token);
                Assert.Equal(2, entered.Count);
                Assert.All(entered, partition => Assert.Equal(0, partition));
                release.TrySetResult();
                var leases = (await Task.WhenAll(claims).WaitAsync(stop.Token)).SelectMany(x => x).ToArray();
                Assert.Equal(2, leases.Length);
                Assert.Equal(2, leases.Select(x => x.Id).Distinct().Count());
            }
            finally
            {
                release.TrySetResult();
                await candidate.StopAsync(default);
                await ownership.StopAsync(default);
            }
        }
        finally
        {
            foreach (var name in names) Environment.SetEnvironmentVariable(name, old[name]);
        }
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(2, true)]
    public async Task OneDemandReturnsEveryLeaseCommittedForIt(int batchSize, bool crossWorkerBatch)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ??
            throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "single-demand-" + Guid.NewGuid().ToString("N");
        var settings = new Dictionary<string, string>
        {
            ["WFC_HUB_MATCHING_ENABLED"] = "true",
            ["WFC_HUB_MATCHING_CANDIDATES_ENABLED"] = "true",
            ["WFC_HUB_MATCHING_NODE_ID"] = "same-shard-test",
            ["WFC_HUB_MATCHING_ADVERTISE_URL"] = "http://localhost:5100",
            ["WFC_HUB_MATCHING_PEERS"] = "http://localhost:5100",
            ["WFC_HUB_MATCHING_PARTITIONS"] = "64",
            ["WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION"] = "true",
            ["WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY"] = "2",
            ["WFC_HUB_CANDIDATE_ADMISSION_PER_PARTITION"] = "2",
            ["WFC_HUB_CANDIDATE_BATCH_SIZE"] = batchSize.ToString(),
            ["WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH"] = crossWorkerBatch ? "true" : "false",
        };
        var original = settings.Keys.ToDictionary(x => x, Environment.GetEnvironmentVariable);
        try
        {
            foreach (var (name, value) in settings) Environment.SetEnvironmentVariable(name, value);
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var ct = stop.Token;
            var taskCount = batchSize * 2;
            await using var db = new NpgsqlConnection(database);
            await db.OpenAsync(ct);
            await using (var insert = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
                SELECT gen_random_uuid(),@hub,0,'work',1,'{}' FROM generate_series(1,@count)
                """, db))
            {
                insert.Parameters.AddWithValue("hub", hub);
                insert.Parameters.AddWithValue("count", taskCount);
                await insert.ExecuteNonQueryAsync(ct);
            }
            var matcherDatabase = new NpgsqlConnectionStringBuilder(database) { ApplicationName = hub };
            await using var store = new HubTaskStore(matcherDatabase.ConnectionString, 12);
            using var ownership = new HubMatchingOwnership(new HubMatchingLeaseStore(store),
                new HubMatchingPartitionLeaseStore(store), NullLogger<HubMatchingOwnership>.Instance);
            var ready = new HubReadinessBroadcaster(store,
                new HubWorkflowStore(store, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
                NullLogger<HubReadinessBroadcaster>.Instance);
            var bothDrainsStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseDrains = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var entered = 0;
            using var candidate = new HubCandidateAcquisition(store, ownership, ready,
                NullLogger<HubCandidateAcquisition>.Instance, null, true,
                async (_, token) =>
                {
                    if (Interlocked.Increment(ref entered) == 2) bothDrainsStarted.TrySetResult();
                    await releaseDrains.Task.WaitAsync(token);
                });
            await ownership.StartAsync(ct);
            await candidate.StartAsync(ct);
            try
            {
                await ownership.AcquirePartitionAsync(hub, 0, ct);
                Task<IReadOnlyList<HubLeasedTask>> claim;
                await using (var fence = new NpgsqlConnection(database))
                {
                    await fence.OpenAsync(ct);
                    await using var tx = await fence.BeginTransactionAsync(ct);
                    // Keep either drain from committing before the other can try admission.
                    await using var lockOwner = new NpgsqlCommand("""
                        SELECT epoch FROM wfc_scale.hub_matching_partition_owner
                        WHERE hub_id=@hub AND matching_partition=0 FOR UPDATE
                        """, fence, tx);
                    lockOwner.Parameters.AddWithValue("hub", hub);
                    Assert.NotNull(await lockOwner.ExecuteScalarAsync(ct));
                    claim = candidate.AcquireAsync(new("worker", [hub], [new(hub, "work", 1)],
                        taskCount, null, 0, 64), ct);
                    await bothDrainsStarted.Task.WaitAsync(ct);
                    releaseDrains.TrySetResult();

                    await using var blocked = new NpgsqlCommand("""
                        SELECT count(*) FROM pg_stat_activity
                        WHERE application_name=@hub AND wait_event_type='Lock'
                          AND query LIKE '%WITH locked_owner AS MATERIALIZED%'
                        """, db);
                    blocked.Parameters.AddWithValue("hub", hub);
                    while ((long)(await blocked.ExecuteScalarAsync(ct))! < 2 && candidate.AdmissionActive > 1)
                        await Task.Delay(20, ct);
                    await tx.CommitAsync(ct);
                }
                var returned = await claim.WaitAsync(ct);
                while (candidate.AdmissionActive != 0) await Task.Delay(20, ct);

                await using var leased = new NpgsqlCommand("""
                    SELECT id FROM wfc_scale.hub_task WHERE hub_id=@hub AND state=1 AND owner='worker'
                    """, db);
                leased.Parameters.AddWithValue("hub", hub);
                await using var rows = await leased.ExecuteReaderAsync(ct);
                var persisted = new List<Guid>();
                while (await rows.ReadAsync(ct)) persisted.Add(rows.GetGuid(0));
                Assert.Equal(returned.Select(lease => lease.Id).Order(), persisted.Order());
                Assert.Equal(batchSize, returned.Count);

                var remaining = await candidate.AcquireAsync(new("next-worker", [hub], [new(hub, "work", 1)],
                    batchSize, null, 0, 64), ct);
                Assert.Equal(taskCount, returned.Concat(remaining).Select(lease => lease.Id).Distinct().Count());
            }
            finally
            {
                releaseDrains.TrySetResult();
                await candidate.StopAsync(default);
                await ownership.StopAsync(default);
            }
        }
        finally
        {
            foreach (var (name, value) in original) Environment.SetEnvironmentVariable(name, value);
        }
    }

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
