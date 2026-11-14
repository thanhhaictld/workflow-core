using Npgsql;
using System.Text.Json;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Two independent HubTaskStore instances share PostgreSQL. Tests prove
/// per-shard owner fencing, independent takeover, and topology consistency.
/// </summary>
public sealed class HubMatchingPartitionTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");
    private static string Hub() => "matching-shards-" + Guid.NewGuid().ToString("N");

    [Fact]
    public async Task AdmissionLock_RechecksExpiryAfterWaitingForUnchangedOwnerRow()
    {
        if (Database is null) return;
        await using var store = new HubTaskStore(Database, 4);
        var owners = new HubMatchingPartitionLeaseStore(store);
        var owner = await owners.AcquireOrReadAsync(Hub(), 0, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(3), default);
        await using var blocker = new NpgsqlConnection(Database);
        await blocker.OpenAsync();
        await using var blocking = await blocker.BeginTransactionAsync();
        await using (var row = new NpgsqlCommand("""
            SELECT 1 FROM wfc_scale.hub_matching_partition_owner
            WHERE hub_id=@hub FOR UPDATE
            """, blocker, blocking))
        {
            row.Parameters.AddWithValue("hub", owner.HubId);
            await row.ExecuteScalarAsync();
        }
        await using var admission = new NpgsqlConnection(Database);
        await admission.OpenAsync();
        await using var tx = await admission.BeginTransactionAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var pending = HubMatchingPartitionLeaseStore.LockOwnerForAdmissionAsync(
            admission, tx, owner, 4, timeout.Token);
        await using (var blocked = new NpgsqlCommand(
            "SELECT cardinality(pg_blocking_pids(@pid))>0", blocker, blocking))
        {
            blocked.Parameters.AddWithValue("pid", admission.ProcessID);
            while (!(bool)(await blocked.ExecuteScalarAsync(timeout.Token))!)
                await Task.Delay(10, timeout.Token);
        }
        // Expire the lease while admission is blocked, without updating the row.
        // A predicate evaluated before LockRows would otherwise remain true.
        await using (var expire = new NpgsqlCommand(
            "SELECT pg_sleep(GREATEST(0,extract(epoch FROM (@expiry-clock_timestamp())))+0.1)",
            blocker, blocking))
        {
            expire.Parameters.AddWithValue("expiry", owner.LeaseUntil);
            await expire.ExecuteNonQueryAsync(timeout.Token);
        }
        await blocking.CommitAsync(timeout.Token);
        Assert.False(await pending);
        await tx.RollbackAsync();
    }

    [Fact]
    public async Task AdmissionLock_SerializesTakeover_AndRejectsOldEpoch()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var store = new HubTaskStore(Database, 4);
        var owners = new HubMatchingPartitionLeaseStore(store);
        var old = await owners.AcquireOrReadAsync(hub, 0, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(30), default);
        await using var admission = new NpgsqlConnection(Database);
        await admission.OpenAsync();
        await using var tx = await admission.BeginTransactionAsync();
        Assert.True(await HubMatchingPartitionLeaseStore.LockOwnerForAdmissionAsync(
            admission, tx, old, 4, default));

        await using (var competitor = new NpgsqlConnection(Database))
        {
            await competitor.OpenAsync();
            await using var change = new NpgsqlCommand("""
                SET lock_timeout='250ms';
                UPDATE wfc_scale.hub_matching_partition_owner
                SET lease_until=clock_timestamp()-interval '1 second'
                WHERE hub_id=@hub AND matching_partition=0
                """, competitor);
            change.Parameters.AddWithValue("hub", hub);
            var error = await Assert.ThrowsAsync<PostgresException>(() => change.ExecuteNonQueryAsync());
            Assert.Equal(PostgresErrorCodes.LockNotAvailable, error.SqlState);
        }
        await tx.CommitAsync();
        Assert.True(await owners.ReleaseAsync(hub, 0, "hub-a", old.Epoch, default));
        var replacement = await owners.AcquireOrReadAsync(hub, 0, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(30), default);
        Assert.True(replacement.Epoch > old.Epoch);
        await using var next = await admission.BeginTransactionAsync();
        Assert.False(await HubMatchingPartitionLeaseStore.LockOwnerForAdmissionAsync(
            admission, next, old, 4, default));
        Assert.False(await HubMatchingPartitionLeaseStore.LockOwnerForAdmissionAsync(
            admission, next, replacement, 3, default));
        Assert.True(await HubMatchingPartitionLeaseStore.LockOwnerForAdmissionAsync(
            admission, next, replacement, 4, default));
        await next.RollbackAsync();
    }

    [Fact]
    public async Task OwnerFencedBatch_RejectsStaleGeneration_AndDoesNotClaimConstrainedFallback()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var store = new HubTaskStore(Database, 4, enableUnconstrainedBatchClaims: true);
        var owners = new HubMatchingPartitionLeaseStore(store);
        var old = await owners.AcquireOrReadAsync(hub, 0, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(30), default);
        var free = Guid.NewGuid();
        var constrained = Guid.NewGuid();
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
                VALUES (@free,@hub,0,'work',1,'{}'),(@constrained,@hub,0,'work',1,'{}');
                INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
                VALUES (@hub,1,true);
                INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
                VALUES (@constrained,@hub)
                """, db);
            cmd.Parameters.AddWithValue("free", free);
            cmd.Parameters.AddWithValue("constrained", constrained);
            cmd.Parameters.AddWithValue("hub", hub);
            await cmd.ExecuteNonQueryAsync();
        }
        async Task<IReadOnlyList<HubLeasedTask>> Claim(HubMatchingPartitionLease owner) =>
            await store.ClaimAsync("worker", [hub], [new(hub, "work", 1)], 8,
                TimeSpan.FromSeconds(30), default, matchingPartition: 0,
                matchingPartitionCount: 4, matchingOwner: owner);
        Assert.True(await owners.ReleaseAsync(hub, 0, "hub-a", old.Epoch, default));
        var current = await owners.AcquireOrReadAsync(hub, 0, 4, "hub-b", "http://hub-2:5100",
            TimeSpan.FromSeconds(30), default);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Claim(old));
        Assert.Equal(free, Assert.Single(await Claim(current)).Id);
        Assert.Empty(await Claim(current));
        var legacy = await store.ClaimAsync("legacy", [hub], [new(hub, "work", 1)], 1,
            TimeSpan.FromSeconds(30), default);
        Assert.Equal(constrained, Assert.Single(legacy).Id);
    }

    [Fact]
    public async Task ReconciliationPagesReachHintsBeyondTheFirst512()
    {
        if (Database is null) return;
        var hub = "zz-push-page-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(Database);
        await db.OpenAsync();
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            SELECT gen_random_uuid(),@hub,0,'type-' || lpad(n::text,4,'0'),1,'{}'::jsonb
            FROM generate_series(1,520) n
            """, db))
        {
            insert.Parameters.AddWithValue("hub", hub);
            Assert.Equal(520, await insert.ExecuteNonQueryAsync());
        }
        await using var tasks = new HubTaskStore(Database, 4);
        (string Hub, string TaskType, int Version, int Partition)? cursor = null;
        var found = new HashSet<string>();
        for (var page = 0; page < 100; page++)
        {
            var hints = await tasks.ReadyMatchingHintsAsync(default, cursor);
            foreach (var hint in hints.Where(h => h.Hub == hub)) found.Add(hint.TaskType);
            if (hints.Count < 512) break;
            cursor = hints[^1];
        }
        Assert.Equal(520, found.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ClaimOwnerFenceRejectsTakeoverForBothClaimPaths(bool batch)
    {
        if (Database is null) return;
        var hub = Hub();
        await using var tasks = new HubTaskStore(Database, 4, enableUnconstrainedBatchClaims: batch);
        var owners = new HubMatchingPartitionLeaseStore(tasks);
        var old = await owners.AcquireOrReadAsync(hub, 0, 1, "old", "http://hub:5100",
            TimeSpan.FromSeconds(30), default);
        var policies = new HubPolicyRegistry().Register(hub, "work", 1, 1, _ => { });
        var payload = JsonSerializer.SerializeToElement(new { value = 1 });
        var id = await tasks.EnqueueAsync(policies.Resolve(hub, "work", 1, payload),
            payload.GetRawText(), null, null, default);
        Assert.True(await owners.ReleaseAsync(hub, 0, old.OwnerId, old.Epoch, default));
        var current = await owners.AcquireOrReadAsync(hub, 0, 1, "new", "http://hub:5100",
            TimeSpan.FromSeconds(30), default);
        Task<IReadOnlyList<HubLeasedTask>> Claim(HubClaimOwnerFence fence) =>
            tasks.ClaimAsync("worker", [hub], [new(hub, "work", 1)], 1,
                TimeSpan.FromSeconds(30), default, matchingPartition: 0,
                matchingPartitionCount: 1, ownerFence: fence);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Claim(new(old.OwnerId, old.Epoch)));
        var leased = Assert.Single(await Claim(new(current.OwnerId, current.Epoch)));
        Assert.Equal(id, leased.Id);
        Assert.Equal(1L, leased.Epoch);
        Assert.Equal(1, leased.Attempts);
    }

    [Fact]
    public async Task ClaimRacingTakeoverWaitsAndRejectsTheOldEpoch()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var tasks = new HubTaskStore(Database, 4, enableUnconstrainedBatchClaims: true);
        var owners = new HubMatchingPartitionLeaseStore(tasks);
        var old = await owners.AcquireOrReadAsync(hub, 0, 1, "old", "http://hub:5100",
            TimeSpan.FromSeconds(30), default);
        var policies = new HubPolicyRegistry().Register(hub, "work", 1, 1, _ => { });
        var payload = JsonSerializer.SerializeToElement(new { value = 1 });
        await tasks.EnqueueAsync(policies.Resolve(hub, "work", 1, payload),
            payload.GetRawText(), null, null, default);
        await using var db = new NpgsqlConnection(Database);
        await db.OpenAsync();
        await using var tx = await db.BeginTransactionAsync();
        await using (var takeover = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_matching_partition_owner
            SET owner_id='new', epoch=epoch+1, lease_until=clock_timestamp()+interval '30 seconds'
            WHERE hub_id=@hub AND matching_partition=0
            """, db, tx))
        {
            takeover.Parameters.AddWithValue("hub", hub);
            Assert.Equal(1, await takeover.ExecuteNonQueryAsync());
        }
        var claim = tasks.ClaimAsync("worker", [hub], [new(hub, "work", 1)], 1,
            TimeSpan.FromSeconds(30), default, matchingPartition: 0,
            matchingPartitionCount: 1, ownerFence: new(old.OwnerId, old.Epoch));
        await tx.CommitAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => claim);
        var newClaim = await tasks.ClaimAsync("worker", [hub], [new(hub, "work", 1)], 1,
            TimeSpan.FromSeconds(30), default, matchingPartition: 0,
            matchingPartitionCount: 1, ownerFence: new("new", old.Epoch + 1));
        Assert.Equal(1L, Assert.Single(newClaim).Epoch);
    }

    [Fact]
    public async Task DifferentMatchingPartitionsCanBeOwnedByDifferentHubReplicas()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        await using var b = new HubTaskStore(Database, 4);
        var first = new HubMatchingPartitionLeaseStore(a);
        var second = new HubMatchingPartitionLeaseStore(b);
        var p0 = await first.AcquireOrReadAsync(hub, 0, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(8), default);
        var p1 = await second.AcquireOrReadAsync(hub, 1, 4, "hub-b", "http://hub-2:5100",
            TimeSpan.FromSeconds(8), default);
        var p2 = await first.AcquireOrReadAsync(hub, 2, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(8), default);
        var p3 = await second.AcquireOrReadAsync(hub, 3, 4, "hub-b", "http://hub-2:5100",
            TimeSpan.FromSeconds(8), default);
        Assert.Equal(new[] { "hub-a", "hub-b", "hub-a", "hub-b" },
            (await first.ListAsync(hub, default)).Select(x => x.OwnerId).ToArray());
        Assert.Equal(p0.Epoch, (await second.AcquireOrReadAsync(
            hub, 0, 4, "hub-b", "http://hub-2:5100", TimeSpan.FromSeconds(8), default)).Epoch);
        Assert.True(await first.OwnsAsync(hub, 0, "hub-a", p0.Epoch, default));
        Assert.False(await second.OwnsAsync(hub, 0, "hub-b", p0.Epoch, default));
        Assert.True(await second.OwnsAsync(hub, 1, "hub-b", p1.Epoch, default));
        Assert.True(await first.OwnsAsync(hub, 2, "hub-a", p2.Epoch, default));
        Assert.True(await second.OwnsAsync(hub, 3, "hub-b", p3.Epoch, default));
    }

    [Fact]
    public async Task ExpiredPartitionTakeoverDoesNotAffectHealthyNeighbors()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        await using var b = new HubTaskStore(Database, 4);
        var first = new HubMatchingPartitionLeaseStore(a);
        var second = new HubMatchingPartitionLeaseStore(b);
        var failed = await first.AcquireOrReadAsync(hub, 0, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(8), default);
        var healthy = await second.AcquireOrReadAsync(hub, 1, 4, "hub-b", "http://hub-2:5100",
            TimeSpan.FromSeconds(8), default);
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var command = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_matching_partition_owner
                SET lease_until=clock_timestamp()-interval '1 second'
                WHERE hub_id=@hub AND matching_partition=0
                """, db);
            command.Parameters.AddWithValue("hub", hub);
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
        }
        var newOwner = await second.AcquireOrReadAsync(hub, 0, 4,
            "hub-b", "http://hub-2:5100", TimeSpan.FromSeconds(8), default);
        Assert.Equal("hub-b", newOwner.OwnerId);
        Assert.True(newOwner.Epoch > failed.Epoch);
        Assert.False(await first.RenewAsync(
            hub, 0, "hub-a", failed.Epoch, TimeSpan.FromSeconds(8), default));
        Assert.False(await first.ReleaseAsync(hub, 0, "hub-a", failed.Epoch, default));
        Assert.True(await second.OwnsAsync(hub, 1, "hub-b", healthy.Epoch, default));
        Assert.True(await second.OwnsAsync(hub, 0, "hub-b", newOwner.Epoch, default));
    }

    [Fact]
    public async Task GracefulReleaseEnablesImmediateTakeoverWithNewEpoch()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        await using var b = new HubTaskStore(Database, 4);
        var first = new HubMatchingPartitionLeaseStore(a);
        var second = new HubMatchingPartitionLeaseStore(b);
        var oldOwner = await first.AcquireOrReadAsync(hub, 3, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(8), default);
        Assert.True(await first.ReleaseAsync(hub, 3, "hub-a", oldOwner.Epoch, default));
        var recovered = await second.AcquireOrReadAsync(hub, 3, 4, "hub-b",
            "http://hub-2:5100", TimeSpan.FromSeconds(8), default);
        Assert.Equal("hub-b", recovered.OwnerId);
        Assert.Equal(oldOwner.Epoch + 1, recovered.Epoch);
        Assert.False(await first.OwnsAsync(hub, 3, "hub-a", oldOwner.Epoch, default));
    }

    [Fact]
    public async Task PartitionCountMismatchFailsClosed()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        var leases = new HubMatchingPartitionLeaseStore(a);
        await leases.EnsureConfigAsync(hub, 4, default);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            leases.EnsureConfigAsync(hub, 8, default));
    }

    [Fact]
    public async Task ParallelElectionOfSameShardHasExactlyOneOwner()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        await using var b = new HubTaskStore(Database, 4);
        var first = new HubMatchingPartitionLeaseStore(a);
        var second = new HubMatchingPartitionLeaseStore(b);
        var elected = await Task.WhenAll(
            first.AcquireOrReadAsync(hub, 2, 4, "hub-a", "http://hub:5100",
                TimeSpan.FromSeconds(8), default),
            second.AcquireOrReadAsync(hub, 2, 4, "hub-b", "http://hub-2:5100",
                TimeSpan.FromSeconds(8), default));
        Assert.Equal(elected[0].OwnerId, elected[1].OwnerId);
        Assert.Equal(elected[0].Epoch, elected[1].Epoch);
    }
}
