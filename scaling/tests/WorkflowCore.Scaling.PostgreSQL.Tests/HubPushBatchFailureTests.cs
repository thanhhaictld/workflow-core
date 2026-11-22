using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubPushBatchFailureTests
{
    [Fact]
    public async Task MidBatchSendFailureKeepsEveryCommittedDeliveryAndAttemptsRemainingSends()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        string[] names = ["WFC_HUB_PUSH_ENABLED", "WFC_HUB_MATCHING_ENABLED", "WFC_HUB_MATCHING_CANDIDATES_ENABLED",
            "WFC_HUB_MATCHING_NODE_ID", "WFC_HUB_MATCHING_ADVERTISE_URL", "WFC_HUB_MATCHING_PEERS", "WFC_HUB_MATCHING_PARTITIONS",
            "WFC_HUB_CANDIDATE_BACKPRESSURE", "WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH"];
        var old = names.ToDictionary(x => x, Environment.GetEnvironmentVariable);
        try
        {
            string[] values = ["true", "true", "true", "batch-failure", "http://localhost:5100", "http://localhost:5100", "64", "true", "true"];
            for (var i = 0; i < names.Length; i++) Environment.SetEnvironmentVariable(names[i], values[i]);
            var hub = "send-failure-" + Guid.NewGuid().ToString("N");
            await using var db = new NpgsqlConnection(database); await db.OpenAsync();
            await using (var insert = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
                SELECT gen_random_uuid(),@hub,0,'work',1,'{}' FROM generate_series(1,4)
                """, db)) { insert.Parameters.AddWithValue("hub",hub); await insert.ExecuteNonQueryAsync(); }
            await using var store = new HubTaskStore(database);
            var owner = new HubMatchingOwnership(new HubMatchingLeaseStore(store),new HubMatchingPartitionLeaseStore(store),NullLogger<HubMatchingOwnership>.Instance);
            var ready = new HubReadinessBroadcaster(store,new HubWorkflowStore(store,new HubTaskRouteRegistry(),new HubWorkflowRegistry()),NullLogger<HubReadinessBroadcaster>.Instance);
            var candidate = new HubCandidateAcquisition(store,owner,ready,NullLogger<HubCandidateAcquisition>.Instance);
            await owner.StartAsync(default); await candidate.StartAsync(default);
            var sessions = new HubPushSessions(store,owner,ready,NullLogger<HubPushSessions>.Instance,candidate);
            var (session, _) = await sessions.RegisterAsync(new("worker",hub,0,64,[new(hub,"work",1)],null,4),default);
            Assert.NotNull(session); Assert.True(sessions.Connect(session));
            var sent = new List<HubPushAssignment>();
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var run = sessions.RunAsync(session,(assignment,_) => {
                sent.Add(assignment); if (sent.Count == 2) throw new IOException("injected SSE failure");
                return Task.CompletedTask;
            },stop.Token);
            Assert.True(sessions.Update(session,new(1,4)));
            await run.WaitAsync(stop.Token);
            // Candidate acquisition may durably admit fewer than the four offered credits.
            // A failed SSE send must still attempt every committed assignment in its batch,
            // and every such assignment must remain tracked for lease expiry/recovery.
            Assert.InRange(sent.Count, 2, 4);
            Assert.Equal(sent.Count, session.Deliveries.Count);
            Assert.Equal(sent.Count, sent.Select(x => x.Task.Id).Distinct().Count());
            await using var check = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_task WHERE hub_id=@hub AND state=1 AND owner='worker'",db);
            check.Parameters.AddWithValue("hub",hub);
            Assert.Equal((long)sent.Count,(long)(await check.ExecuteScalarAsync())!);
            await using var pending = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_task WHERE hub_id=@hub AND state=0",db);
            pending.Parameters.AddWithValue("hub",hub);
            Assert.Equal(4L-sent.Count,(long)(await pending.ExecuteScalarAsync())!);
            await candidate.StopAsync(default); await owner.StopAsync(default);
        }
        finally { foreach (var name in names) Environment.SetEnvironmentVariable(name,old[name]); }
    }
}
