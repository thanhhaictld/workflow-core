using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubOptimisticOwnerAdmissionTests
{
    private static string Database => Environment.GetEnvironmentVariable("WFC_TEST_PG")
        ?? throw new InvalidOperationException("WFC_TEST_PG is required");

    [Fact]
    public async Task OptimisticPullAdmissionDoesNotTakeShareLockOnOwnerRow()
    {
        var hub = "optimistic-pull-" + Guid.NewGuid().ToString("N");
        var taskId = Guid.NewGuid();
        await using var store = new HubTaskStore(Database,
            enableUnconstrainedBatchClaims: true, enableOptimisticOwnerAdmission: true);
        var owners = new HubMatchingPartitionLeaseStore(store);
        var owner = await owners.AcquireOrReadAsync(hub, 0, 4, "first", "http://localhost:5100",
            TimeSpan.FromSeconds(30), default);
        await using var db = new NpgsqlConnection(Database);
        await db.OpenAsync();
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@id,@hub,0,'work',1,'{}')
            """, db))
        {
            insert.Parameters.AddWithValue("id", taskId);
            insert.Parameters.AddWithValue("hub", hub);
            await insert.ExecuteNonQueryAsync();
        }

        await using var tx = await db.BeginTransactionAsync();
        await using (var lockOwner = new NpgsqlCommand("""
            SELECT 1 FROM wfc_scale.hub_matching_partition_owner
            WHERE hub_id=@hub AND matching_partition=0 FOR UPDATE
            """, db, tx))
        {
            lockOwner.Parameters.AddWithValue("hub", hub);
            Assert.NotNull(await lockOwner.ExecuteScalarAsync());
        }

        // A strict admission would block on the FOR SHARE owner lock here.
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var claim = store.ClaimAsync("worker", [hub], [new(hub, "work", 1)],
            1, TimeSpan.FromSeconds(30), timeout.Token,
            matchingPartition: 0, matchingPartitionCount: 4, matchingOwner: owner);
        var leases = await claim.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(taskId, Assert.Single(leases).Id);
        await tx.RollbackAsync();
    }

    [Fact]
    public async Task TakeoverWhileOldAdmissionIsUncommittedUsesTaskEpochToFenceStaleWorker()
    {
        var hub = "optimistic-takeover-" + Guid.NewGuid().ToString("N");
        var firstTask = Guid.NewGuid();
        var secondTask = Guid.NewGuid();
        await using var store = new HubTaskStore(Database,
            enableOptimisticOwnerAdmission: true);
        var owners = new HubMatchingPartitionLeaseStore(store);
        var oldOwner = await owners.AcquireOrReadAsync(hub, 0, 4, "old",
            "http://localhost:5100", TimeSpan.FromSeconds(2), default);
        await using var db = new NpgsqlConnection(Database);
        await db.OpenAsync();
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@first,@hub,0,'work',1,'{}'),(@second,@hub,0,'work',1,'{}')
            """, db))
        {
            insert.Parameters.AddWithValue("first", firstTask);
            insert.Parameters.AddWithValue("second", secondTask);
            insert.Parameters.AddWithValue("hub", hub);
            await insert.ExecuteNonQueryAsync();
        }

        var enteredCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var oldRequest = new HubCandidateClaim(firstTask, 0, "old-worker", hub, 0, 4,
            [new(hub, "work", 1)], null, oldOwner, TimeSpan.FromSeconds(30));
        var oldClaim = store.ClaimCandidateAsync(oldRequest, timeout.Token,
            new HubCandidateClaimTestHooks(Commit: async (tx, ct) =>
            {
                enteredCommit.TrySetResult();
                await releaseCommit.Task.WaitAsync(ct);
                await tx.CommitAsync(ct);
            }));

        try
        {
            await enteredCommit.Task.WaitAsync(timeout.Token);
            // Owner expiry and takeover must not wait on the uncommitted
            // task claim. This intentionally permits a late old-owner commit.
            var remaining = oldOwner.LeaseUntil - DateTime.UtcNow + TimeSpan.FromMilliseconds(400);
            if (remaining > TimeSpan.Zero) await Task.Delay(remaining, timeout.Token);
            var newOwner = await owners.AcquireOrReadAsync(hub, 0, 4, "new",
                "http://localhost:5200", TimeSpan.FromSeconds(30), timeout.Token)
                .WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(newOwner.Epoch > oldOwner.Epoch);

            var newRequest = oldRequest with {
                TaskId = secondTask, Owner = newOwner, WorkerId = "new-worker"
            };
            var secondClaim = await store.ClaimCandidateAsync(newRequest, timeout.Token);
            Assert.Equal(HubCandidateClaimStatus.Leased, secondClaim.Status);
            Assert.Equal(secondTask, secondClaim.Lease!.Id);
            releaseCommit.TrySetResult();

            var oldResult = await oldClaim.WaitAsync(timeout.Token);
            Assert.Equal(HubCandidateClaimStatus.Leased, oldResult.Status);
            Assert.Equal(firstTask, oldResult.Lease!.Id);
            Assert.Equal(1, oldResult.Lease.Epoch);

            // The new owner cannot also claim the active old-owner task.
            Assert.Equal(HubCandidateClaimStatus.Unavailable,
                (await store.ClaimCandidateAsync(newRequest with {
                    TaskId = firstTask, ObservedEpoch = 0
                }, timeout.Token)).Status);

            // Simulate lease expiry to test authoritative task-epoch fencing.
            await using (var expire = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_task SET lease_until=clock_timestamp()-interval '1 second'
                WHERE id=@id AND epoch=1
                """, db))
            {
                expire.Parameters.AddWithValue("id", firstTask);
                Assert.Equal(1, await expire.ExecuteNonQueryAsync(timeout.Token));
            }
            var reclaimed = await store.ClaimCandidateAsync(newRequest with {
                TaskId = firstTask, ObservedEpoch = 1
            }, timeout.Token);
            Assert.Equal(HubCandidateClaimStatus.Leased, reclaimed.Status);
            Assert.Equal(2, reclaimed.Lease!.Epoch);
            Assert.Equal(2, reclaimed.Lease.Attempts);
            Assert.False(await store.CompleteAsync(firstTask, "old-worker", 1, "{}", timeout.Token));
        }
        finally
        {
            releaseCommit.TrySetResult();
            await oldClaim.WaitAsync(timeout.Token);
        }
    }
}
