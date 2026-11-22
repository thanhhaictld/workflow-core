using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubMultiplexedPushTests
{
    [Fact]
    public async Task OneSessionAdmitsDifferentShardsAndKindsAndRejectsReplayedOrStaleDemand()
    {
        await using var fixture = await Fixture.StartAsync();
        var hub = fixture.Hub;
        var registration = fixture.Registration;
        var push = fixture.Push;
        var session = push.Register(registration)!;
        Assert.Same(session, push.Connect(session.Identity.SessionId));
        Assert.Null(push.Connect(session.Identity.SessionId));
        Assert.Null(push.Register(registration));
        Assert.Equal(1, push.ConnectedCount);

        await fixture.SqlAsync("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,
                input_json,step_count,execution_mode,native_status)
            VALUES (@workflow,@hub,'definition',1,1,'{}',1,'native','Runnable');
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload)
            VALUES (gen_random_uuid(),@hub,0,'activity',1,NULL,'{}'),
                   (gen_random_uuid(),@hub,2,'activity',1,NULL,'{}'),
                   (gen_random_uuid(),@hub,1,'__native_workflow__',1,@workflow,'{}');
            """);
        var first = fixture.Grant(1, 0, HubPushTaskKind.Activity);
        var second = fixture.Grant(2, 1, HubPushTaskKind.Workflow);
        var third = fixture.Grant(3, 2, HubPushTaskKind.Activity);
        var id = session.Identity.SessionId;
        Assert.False(push.Offer(id, [first with { Credits = 3 }]));
        Assert.False(push.Offer(id, [second with { Credits = 2 }]));
        Assert.True(push.Offer(id, [first, second, third]));
        Assert.False(push.Offer(id, [first, second, third]));

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var results = new List<HubPushGrantResult>();
        for (var i = 0; i < 3; i++) results.Add(await session.Results.Reader.ReadAsync(timeout.Token));
        foreach (var result in results)
        {
            var task = Assert.Single(result.Tasks);
            Assert.Equal(result.Grant.Partition, task.PartitionId);
            Assert.Equal(result.Grant.Kind == HubPushTaskKind.Workflow, task.TaskType == "__native_workflow__");
            Assert.True(await fixture.Store.IsCandidateLeaseCurrentAsync(task,
                new HubPushGrantRecovery(session.Identity, result.Grant).LeaseOwner, timeout.Token));
        }

        Assert.True(push.Offer(id, [first with { Sequence = 4, OwnerEpoch = first.OwnerEpoch + 1 }]));
        var stale = await session.Results.Reader.ReadAsync(timeout.Token);
        Assert.Empty(stale.Tasks);
        Assert.False(stale.Uncertain);
        Assert.Equal(1, push.ConnectedCount);
        push.Close(id, streamEnded: true);
    }

    [Fact]
    public async Task AmbiguousCommitAndDisconnectHoldCreditAcrossOwnerIncarnationUntilLeaseExpires()
    {
        var committed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var loseResponse = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var fixture = await Fixture.StartAsync(new HubCandidateClaimTestHooks(Commit: async (tx, ct) =>
        {
            await tx.CommitAsync(ct);
            committed.TrySetResult();
            await loseResponse.Task.WaitAsync(ct);
            throw new IOException("Lost commit response");
        }));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var push = fixture.Push;
        var session = push.Register(fixture.Registration)!;
        Assert.Same(session, push.Connect(session.Identity.SessionId));
        await fixture.SqlAsync("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@workflow,@hub,0,'activity',1,'{}')
            """);
        var grant = fixture.Grant(1, 0, HubPushTaskKind.Activity);
        var recovery = new HubPushGrantRecovery(session.Identity, grant);
        Assert.True(push.Offer(session.Identity.SessionId, [grant]));
        try
        {
            await committed.Task.WaitAsync(timeout.Token);
            Assert.False(await push.IsRetiredAsync(recovery, timeout.Token));
        }
        finally { loseResponse.TrySetResult(); }
        var uncertain = await session.Results.Reader.ReadAsync(timeout.Token);
        Assert.True(uncertain.Uncertain);
        Assert.Empty(uncertain.Tasks);
        push.Close(session.Identity.SessionId, streamEnded: true);
        Assert.False(push.Offer(session.Identity.SessionId, [grant with { Sequence = 2 }]));
        Assert.False(await push.IsRetiredAsync(recovery, timeout.Token));

        var replacement = fixture.CreateOwnership();
        Assert.NotEqual(fixture.Ownership.NodeId, replacement.NodeId);
        var replacementPush = fixture.CreatePush(replacement);
        var unaccepted = recovery with { Grant = grant with { Sequence = 2 } };
        Assert.False(await replacementPush.IsRetiredAsync(unaccepted, timeout.Token));
        var initial = fixture.Owners[0];
        Assert.True(await fixture.Partitions.ReleaseAsync(fixture.Hub, 0, initial.OwnerId, initial.Epoch, timeout.Token));
        var takeover = await fixture.Partitions.AcquireOrReadAsync(fixture.Hub, 0, 4,
            replacement.NodeId, replacement.AdvertiseUrl, TimeSpan.FromSeconds(8), timeout.Token);
        Assert.True(takeover.Epoch > initial.Epoch);
        Assert.True(await replacementPush.IsRetiredAsync(unaccepted, timeout.Token));
        Assert.False(await replacementPush.IsRetiredAsync(recovery, timeout.Token));
        await fixture.SqlAsync("UPDATE wfc_scale.hub_task SET lease_until=clock_timestamp() WHERE id=@workflow");
        Assert.True(await replacementPush.IsRetiredAsync(recovery, timeout.Token));
        Assert.False(await fixture.Store.HeartbeatAsync(fixture.Workflow, recovery.LeaseOwner, 1,
            TimeSpan.FromSeconds(45), timeout.Token));
        var retried = Assert.Single(await fixture.Store.ClaimAsync("replacement", [fixture.Hub],
            [new(fixture.Hub, "activity", 1)], 1, TimeSpan.FromSeconds(45), timeout.Token,
            matchingPartition: 0, matchingPartitionCount: 4, ownerFence: new(takeover.OwnerId, takeover.Epoch)));
        Assert.Equal(2, retried.Attempts);
        Assert.False(await fixture.Store.CompleteAsync(retried.Id, recovery.LeaseOwner, 1, "{}", timeout.Token));
    }

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly Dictionary<string, string?> _environment;
        private readonly string _database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        private readonly HubReadinessBroadcaster _readiness;
        private readonly HubCandidateAcquisition _candidate;
        public string Hub { get; } = "mux-" + Guid.NewGuid().ToString("N");
        public Guid Workflow { get; } = Guid.NewGuid();
        public HubMatchingPartitionLease[] Owners { get; } = new HubMatchingPartitionLease[4];
        public HubTaskStore Store { get; }
        public HubMatchingPartitionLeaseStore Partitions { get; }
        public HubMatchingOwnership Ownership { get; }
        public HubMultiplexedPush Push { get; }
        public HubPushProcessorRegistration Registration => new(Guid.NewGuid(), 4, 2, 1,
            [new(Hub, HubPushTaskKind.Activity, [new(Hub, "activity", 1)]),
             new(Hub, HubPushTaskKind.Workflow, [new(Hub, "__native_workflow__", 1)], [new(Hub, "definition", 1)])]);

        private Fixture(HubCandidateClaimTestHooks? hooks)
        {
            var settings = new Dictionary<string, string>
            {
                ["WFC_HUB_PUSH_ENABLED"] = "true",
                ["WFC_HUB_MATCHING_ENABLED"] = "true",
                ["WFC_HUB_MATCHING_CANDIDATES_ENABLED"] = "true",
                ["WFC_HUB_MATCHING_NODE_ID"] = "mux-test",
                ["WFC_HUB_MATCHING_ADVERTISE_URL"] = "http://localhost:5100",
                ["WFC_HUB_MATCHING_PEERS"] = "http://localhost:5100",
                ["WFC_HUB_MATCHING_PARTITIONS"] = "4"
            };
            _environment = settings.Keys.ToDictionary(k => k, Environment.GetEnvironmentVariable);
            foreach (var setting in settings) Environment.SetEnvironmentVariable(setting.Key, setting.Value);
            Store = new HubTaskStore(_database);
            Partitions = new HubMatchingPartitionLeaseStore(Store);
            Ownership = CreateOwnership();
            _readiness = new HubReadinessBroadcaster(Store,
                new HubWorkflowStore(Store, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
                NullLogger<HubReadinessBroadcaster>.Instance);
            _candidate = new HubCandidateAcquisition(Store, Ownership, _readiness,
                NullLogger<HubCandidateAcquisition>.Instance, null, true, admissionTestHooks: hooks);
            Push = CreatePush(Ownership);
        }

        public static async Task<Fixture> StartAsync(HubCandidateClaimTestHooks? hooks = null)
        {
            var fixture = new Fixture(hooks);
            try
            {
                await fixture.Ownership.StartAsync(default);
                await fixture._candidate.StartAsync(default);
                for (var shard = 0; shard < 4; shard++)
                    fixture.Owners[shard] = await fixture.Ownership.AcquirePartitionAsync(fixture.Hub, shard, default);
                return fixture;
            }
            catch { await fixture.DisposeAsync(); throw; }
        }

        public HubMatchingOwnership CreateOwnership() =>
            new(new HubMatchingLeaseStore(Store), Partitions, NullLogger<HubMatchingOwnership>.Instance);

        public HubMultiplexedPush CreatePush(HubMatchingOwnership ownership) =>
            new(new HubPushSessions(Store, ownership, _readiness, NullLogger<HubPushSessions>.Instance, _candidate),
                ownership, Partitions, Store, _candidate, NullLogger<HubMultiplexedPush>.Instance);

        public HubPushGrant Grant(int sequence, int partition, HubPushTaskKind kind) =>
            new(sequence, Hub, partition, Owners[partition].Epoch, kind, 1);

        public async Task SqlAsync(string sql)
        {
            await using var db = new NpgsqlConnection(_database);
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, db);
            cmd.Parameters.AddWithValue("hub", Hub);
            cmd.Parameters.AddWithValue("workflow", Workflow);
            await cmd.ExecuteNonQueryAsync();
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await _candidate.StopAsync(default);
                await Ownership.StopAsync(default);
                await Store.DisposeAsync();
            }
            finally { foreach (var (key, value) in _environment) Environment.SetEnvironmentVariable(key, value); }
        }
    }
}
