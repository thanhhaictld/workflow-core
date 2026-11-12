using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Opt-in batch-claim integration tests against the same PostgreSQL schema as
/// the Hub. No mock can establish row-lock, permit, or fencing correctness.
/// </summary>
public sealed class HubUnconstrainedBatchClaimTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");
    private static string Hub() => "batch-claim-" + Guid.NewGuid().ToString("N")[..14];
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);
    private static JsonElement Payload(string key) => JsonSerializer.SerializeToElement(new { resourceId = key });

    private static async Task<Guid> Enqueue(HubTaskStore store, HubPolicyRegistry policies,
        string hub, string type, string resource)
    {
        var payload = Payload(resource);
        return await store.EnqueueAsync(policies.Resolve(hub, type, 1, payload),
            payload.GetRawText(), null, null, CancellationToken.None);
    }

    private static Task<IReadOnlyList<HubLeasedTask>> Claim(
        HubTaskStore store, string hub, string owner, int max = 16,
        params string[] types) =>
        store.ClaimAsync(owner, new[] { hub },
            types.Select(type => new HubCapability(hub, type, 1)).ToArray(),
            max, Lease, CancellationToken.None);

    [Fact]
    public async Task OrderedCapabilityMerge_MergesTypesAndExpiredRows_AndSkipsLockedHead()
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry().Register(hub, "a", 1, 1, _ => { })
            .Register(hub, "b", 1, 1, _ => { }).Register(hub, "c", 1, 1, _ => { });
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true,
            enableOrderedCapabilityMerge: true);
        var a = await Enqueue(store, registry, hub, "a", "a");
        var b = await Enqueue(store, registry, hub, "b", "b");
        var expired = await Enqueue(store, registry, hub, "c", "expired");
        var later = await Enqueue(store, registry, hub, "a", "later");
        await using var db = new NpgsqlConnection(Database);
        await db.OpenAsync();
        await using (var update = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task SET state=1,owner='old',epoch=1,attempts=1,
              lease_until=now()-interval '1 minute' WHERE id=@id
            """, db))
        {
            update.Parameters.AddWithValue("id", expired);
            await update.ExecuteNonQueryAsync();
        }
        var capabilities = new[] { new HubCapability(hub, "a", 1), new(hub, "b", 1),
            new(hub, "c", 1), new(hub, "a", 1) };
        Task<IReadOnlyList<HubLeasedTask>> ClaimShard(int count) => store.ClaimAsync(
            "worker", [hub], capabilities, count, Lease, CancellationToken.None,
            matchingPartition: 0, matchingPartitionCount: 4);
        await using (var tx = await db.BeginTransactionAsync())
        {
            await using var locked = new NpgsqlCommand(
                "SELECT id FROM wfc_scale.hub_task WHERE id=@id FOR UPDATE", db, tx);
            locked.Parameters.AddWithValue("id", a);
            await locked.ExecuteScalarAsync();
            var result = await ClaimShard(2);
            Assert.True(new[] { b, expired }.ToHashSet().SetEquals(result.Select(t => t.Id)));
            Assert.Equal(2L, Assert.Single(result, t => t.Id == expired).Epoch);
            await tx.CommitAsync();
        }
        var remaining = await ClaimShard(2);
        Assert.True(new[] { a, later }.ToHashSet().SetEquals(remaining.Select(t => t.Id)));
        Assert.False(await store.CompleteAsync(expired, "old", 1, "{}", CancellationToken.None));
    }

    [Fact]
    public async Task OrderedCapabilityMerge_ConcurrentClaimsRemainUniqueAndDrain()
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry().Register(hub, "a", 1, 1, _ => { })
            .Register(hub, "b", 1, 1, _ => { }).Register(hub, "c", 1, 1, _ => { });
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true,
            enableOrderedCapabilityMerge: true);
        var types = new[] { "a", "b", "c" };
        var expected = new HashSet<Guid>();
        for (var i = 0; i < 48; i++) expected.Add(await Enqueue(store, registry, hub, types[i % 3], i.ToString()));
        var capabilities = types.Select(t => new HubCapability(hub, t, 1)).ToArray();
        Task<IReadOnlyList<HubLeasedTask>> ClaimShard(string owner) => store.ClaimAsync(
            owner, [hub], capabilities, 8, Lease, CancellationToken.None,
            matchingPartition: 0, matchingPartitionCount: 4);
        var claims = (await Task.WhenAll(Enumerable.Range(0, 6).Select(i => ClaimShard("worker-" + i))))
            .SelectMany(batch => batch).ToList();
        for (var i = 0; i < 6 && claims.Count < expected.Count; i++)
            claims.AddRange(await ClaimShard("drain-" + i));
        Assert.Equal(expected.Count, claims.Count);
        Assert.True(expected.SetEquals(claims.Select(t => t.Id)));
        Assert.All(claims, t => Assert.Equal(1L, t.Epoch));
    }

    [Fact]
    public async Task OrderedCapabilityMerge_LeavesConstrainedTasksToPermitAdmission()
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry()
            .Register(hub, "free", 1, 1, _ => { })
            .Register(hub, "serial", 1, 1,
                p => p.SequentialBy("resource", x => x.GetProperty("resourceId").GetString()));
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true,
            enableOrderedCapabilityMerge: true);
        var first = await Enqueue(store, registry, hub, "serial", "key");
        var second = await Enqueue(store, registry, hub, "serial", "key");
        var free = await Enqueue(store, registry, hub, "free", "free");
        Task<IReadOnlyList<HubLeasedTask>> ClaimShard(string worker) => store.ClaimAsync(worker, [hub],
            [new(hub, "serial", 1), new(hub, "free", 1)], 8, Lease, CancellationToken.None,
            matchingPartition: 0, matchingPartitionCount: 4);
        var initial = await ClaimShard("first");
        Assert.True(new[] { first, free }.ToHashSet().SetEquals(initial.Select(t => t.Id)));
        Assert.Empty(await ClaimShard("second"));
        var lease = Assert.Single(initial, t => t.Id == first);
        Assert.True(await store.CompleteAsync(first, "first", lease.Epoch, "{}", CancellationToken.None));
        Assert.Equal(second, Assert.Single(await ClaimShard("second")).Id);
    }

    [Fact]
    public async Task LegacyClaims_FilterHubsAndPreserveGlobalSequence()
    {
        if (Database is null) return;
        var firstHub = Hub();
        var secondHub = Hub();
        var registry = new HubPolicyRegistry()
            .Register(firstHub, "work", 1, 1, _ => { })
            .Register(secondHub, "work", 1, 1, _ => { });
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: false);
        var first = await Enqueue(store, registry, firstHub, "work", "a");
        var second = await Enqueue(store, registry, secondHub, "work", "b");
        var third = await Enqueue(store, registry, firstHub, "work", "c");

        var single = await Claim(store, secondHub, "single", 1, "work");
        Assert.Equal(second, Assert.Single(single).Id);
        var both = await store.ClaimAsync("both", [firstHub, secondHub],
            [new(firstHub, "work", 1), new(secondHub, "work", 1)],
            2, Lease, CancellationToken.None);
        Assert.Equal(new[] { first, third }, both.Select(task => task.Id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BatchClaims_AreUniqueAcrossCompetingWorkers_AndFenced(bool split)
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry()
            .Register(hub, "unconstrained", 1, 16, _ => { });
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true,
            enableSplitBatchClaimCandidates: split);

        var expected = new HashSet<Guid>();
        for (var i = 0; i < 48; i++)
            expected.Add(await Enqueue(store, registry, hub, "unconstrained", i.ToString()));

        // Six independent claim calls race on one shared database; each row
        // must be leased at most once. The bulk path uses SKIP LOCKED.
        var batches = await Task.WhenAll(Enumerable.Range(0, 6)
            .Select(i => Claim(store, hub, "batch-worker-" + i, 16, "unconstrained")));
        var actual = batches.SelectMany(items => items).ToArray();
        Assert.Equal(48, actual.Length);
        Assert.True(expected.SetEquals(actual.Select(t => t.Id)));
        Assert.All(actual, t => Assert.Equal(1L, t.Epoch));

        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand("""
                SELECT count(*) FROM wfc_scale.hub_task_constraint c
                JOIN wfc_scale.hub_task t ON t.id=c.task_id
                WHERE t.hub_id=@hub
                """, db);
            cmd.Parameters.AddWithValue("hub", hub);
            Assert.Equal(0L, (long)(await cmd.ExecuteScalarAsync() ?? -1L));
        }

        foreach (var (owner, tasks) in batches.Select((tasks, index) =>
                     ("batch-worker-" + index, tasks)))
            foreach (var task in tasks)
                Assert.True(await store.CompleteAsync(task.Id, owner, task.Epoch,
                    "{\"ok\":true}", CancellationToken.None));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BatchClaims_MergeReadyAndExpiredBySequence_AndFenceOldOwner(bool split)
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry().Register(hub, "work", 1, 1, _ => { });
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true,
            enableSplitBatchClaimCandidates: split);
        var expired = await Enqueue(store, registry, hub, "work", "expired");
        var ready = await Enqueue(store, registry, hub, "work", "ready");
        var busy = await Enqueue(store, registry, hub, "work", "busy");
        var future = await Enqueue(store, registry, hub, "work", "future");
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_task SET state=1,owner='old',epoch=1,attempts=1,
                    lease_until=now()+CASE WHEN id=@expired THEN interval '-1 minute' ELSE interval '1 hour' END
                    WHERE id IN (@expired,@busy);
                UPDATE wfc_scale.hub_task SET due_at=now()+interval '1 hour' WHERE id=@future
                """, db);
            cmd.Parameters.AddWithValue("expired", expired);
            cmd.Parameters.AddWithValue("busy", busy);
            cmd.Parameters.AddWithValue("future", future);
            await cmd.ExecuteNonQueryAsync();
        }
        var first = Assert.Single(await Claim(store, hub, "new", 1, "work"));
        Assert.Equal(expired, first.Id);
        Assert.Equal(2, first.Epoch);
        Assert.False(await store.CompleteAsync(expired, "old", 1, "{}", CancellationToken.None));
        Assert.Equal(ready, Assert.Single(await Claim(store, hub, "new", 1, "work")).Id);
        Assert.Empty(await Claim(store, hub, "new", 1, "work"));
    }

    [Fact]
    public async Task SplitBatchClaims_CompeteAcrossReadyAndExpiredWithoutDuplicateLeases()
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry().Register(hub, "work", 1, 1, _ => { });
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true,
            enableSplitBatchClaimCandidates: true);
        var ids = new List<Guid>();
        for (var i = 0; i < 48; i++)
            ids.Add(await Enqueue(store, registry, hub, "work", i.ToString()));
        var expired = ids.Where((_, index) => index % 2 == 0).ToArray();
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_task SET state=1,owner='old',epoch=1,attempts=1,
                    lease_until=now()-interval '1 minute' WHERE id=ANY(@ids)
                """, db);
            cmd.Parameters.AddWithValue("ids", expired);
            await cmd.ExecuteNonQueryAsync();
        }
        var batches = await Task.WhenAll(Enumerable.Range(0, 6)
            .Select(i => Claim(store, hub, "worker-" + i, 16, "work")));
        var claimed = batches.SelectMany(batch => batch).ToList();
        // A concurrent claim may skip rows temporarily locked by another
        // branch but not selected in its merged batch; subsequent polls drain.
        for (var i = 0; i < 3 && claimed.Count < ids.Count; i++)
            claimed.AddRange(await Claim(store, hub, "drain-" + i, 16, "work"));
        Assert.Equal(ids.Count, claimed.Count);
        Assert.True(ids.ToHashSet().SetEquals(claimed.Select(task => task.Id)));
        var expiredSet = expired.ToHashSet();
        Assert.All(claimed, task => Assert.Equal(expiredSet.Contains(task.Id) ? 2L : 1L, task.Epoch));
    }

    [Theory]
    [InlineData(3)]
    [InlineData(4)]
    public async Task SplitBatchClaims_RespectMatchingPartitionAndCapabilities(int partitionCount)
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry()
            .Register(hub, "work", 1, 1, _ => { })
            .Register(hub, "other", 1, 1, _ => { });
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true,
            enableSplitBatchClaimCandidates: true, enableOrderedCapabilityMerge: true);
        var matching = await Enqueue(store, registry, hub, "work", "matching");
        var otherPartition = await Enqueue(store, registry, hub, "work", "partition");
        await Enqueue(store, registry, hub, "other", "capability");
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "UPDATE wfc_scale.hub_task SET partition_id=1 WHERE id=@id", db);
            cmd.Parameters.AddWithValue("id", otherPartition);
            await cmd.ExecuteNonQueryAsync();
        }
        var result = await store.ClaimAsync("worker", [hub], [new(hub, "work", 1)],
            8, Lease, CancellationToken.None, matchingPartition: 0, matchingPartitionCount: partitionCount);
        Assert.Equal(matching, Assert.Single(result).Id);
    }

    [Fact]
    public async Task NativeBatchDoesNotMixAtomicAndIndividuallyCommittedClaims()
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry().Register(hub, "__native_workflow__", 1, 8, _ => { });
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true);
        var free = await Enqueue(store, registry, hub, "__native_workflow__", "free");
        var constrained = await Enqueue(store, registry, hub, "__native_workflow__", "constrained");
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
                    VALUES (@key,1,true);
                INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
                    VALUES (@task,@key)
                """, db);
            cmd.Parameters.AddWithValue("key", hub + "-resource");
            cmd.Parameters.AddWithValue("task", constrained);
            await cmd.ExecuteNonQueryAsync();
        }
        Assert.Equal(free, Assert.Single(await Claim(store, hub, "native-a", 8, "__native_workflow__")).Id);
        Assert.Equal(constrained, Assert.Single(await Claim(store, hub, "native-b", 8, "__native_workflow__")).Id);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MixedClaims_BatchOnlyUnconstrained_AndPreserveStrictFifo(bool split)
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry()
            .Register(hub, "free", 1, 8, _ => { })
            .Register(hub, "serialized", 1, 8,
                policy => policy.SequentialBy("resource",
                    value => value.GetProperty("resourceId").GetString()));
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true,
            enableSplitBatchClaimCandidates: split);

        var constrained = new List<Guid>();
        for (var i = 0; i < 3; i++)
            constrained.Add(await Enqueue(store, registry, hub, "serialized", "same-key"));
        var free = new List<Guid>();
        for (var i = 0; i < 8; i++)
            free.Add(await Enqueue(store, registry, hub, "free", i.ToString()));

        var leased = await Claim(store, hub, "A", 16, "serialized", "free");
        Assert.Equal(9, leased.Count);
        Assert.True(free.ToHashSet().SetEquals(leased.Where(t => t.TaskType == "free")
            .Select(t => t.Id)));
        Assert.Equal(constrained[0], Assert.Single(leased.Where(t =>
            t.TaskType == "serialized")).Id);
        Assert.Empty(await Claim(store, hub, "B", 16, "serialized", "free"));

        var first = Assert.Single(leased.Where(t => t.TaskType == "serialized"));
        Assert.True(await store.CompleteAsync(first.Id, "A", first.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        var second = Assert.Single(await Claim(store, hub, "B", 16, "serialized", "free"));
        Assert.Equal(constrained[1], second.Id);
        Assert.True(await store.CompleteAsync(second.Id, "B", second.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        var third = Assert.Single(await Claim(store, hub, "C", 16, "serialized", "free"));
        Assert.Equal(constrained[2], third.Id);
    }
}
