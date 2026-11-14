using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubPushSessionTests
{
    [Fact]
    public async Task CreditIsNotRecreatedByRefreshUntilDeliveryIsReleased()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var keys = new[] { "WFC_HUB_PUSH_ENABLED", "WFC_HUB_MATCHING_ENABLED",
            "WFC_HUB_MATCHING_NODE_ID", "WFC_HUB_MATCHING_ADVERTISE_URL",
            "WFC_HUB_MATCHING_PEERS", "WFC_HUB_MATCHING_PARTITIONS" };
        var original = keys.ToDictionary(k => k, Environment.GetEnvironmentVariable);
        try
        {
            Environment.SetEnvironmentVariable(keys[0], "true");
            Environment.SetEnvironmentVariable(keys[1], "true");
            Environment.SetEnvironmentVariable(keys[2], "test-push-owner");
            Environment.SetEnvironmentVariable(keys[3], "http://localhost:5100");
            Environment.SetEnvironmentVariable(keys[4], "http://localhost:5100");
            Environment.SetEnvironmentVariable(keys[5], "1");
            var hub = "push-" + Guid.NewGuid().ToString("N");
            await using var tasks = new HubTaskStore(database, 4);
            var policies = new HubPolicyRegistry().Register(hub, "work", 1, 1, _ => { });
            var payload = JsonSerializer.SerializeToElement(new { value = 1 });
            var ids = new List<Guid>();
            for (var i = 0; i < 2; i++)
                ids.Add(await tasks.EnqueueAsync(policies.Resolve(hub, "work", 1, payload),
                    payload.GetRawText(), null, null, default));
            var ownership = new HubMatchingOwnership(new HubMatchingLeaseStore(tasks),
                new HubMatchingPartitionLeaseStore(tasks), NullLogger<HubMatchingOwnership>.Instance);
            await ownership.StartAsync(default);
            var broadcaster = new HubReadinessBroadcaster(tasks,
                new HubWorkflowStore(tasks, policies, new HubWorkflowRegistry()),
                NullLogger<HubReadinessBroadcaster>.Instance);
            var sessions = new HubPushSessions(tasks, ownership, broadcaster, NullLogger<HubPushSessions>.Instance);
            var (session, _) = await sessions.RegisterAsync(new("worker", hub, 0, 1,
                [new(hub, "work", 1)], null), default);
            Assert.NotNull(session);
            Assert.False(sessions.Update(session, new(1, 1)));
            Assert.True(sessions.Connect(session));
            Assert.False(sessions.Connect(session));
            await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.RegisterAsync(
                new("worker", hub, 0, 1, [new(hub, "work", 1)], null), default));
            var delivered = Channel.CreateUnbounded<HubPushAssignment>();
            var failedSend = new TaskCompletionSource<HubPushAssignment>(TaskCreationOptions.RunContinuationsAsynchronously);
            var failSend = false;
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var loop = sessions.RunAsync(session, (a, _) =>
            {
                if (failSend) { failedSend.TrySetResult(a); throw new IOException("lost send"); }
                delivered.Writer.TryWrite(a); return Task.CompletedTask;
            }, stop.Token);
            Assert.True(sessions.Update(session, new(1, 1)));
            var first = await delivered.Reader.ReadAsync(stop.Token);
            Assert.Contains(first.Task.Id, ids);
            Assert.False(sessions.Update(session, new(1, 1)));
            Assert.True(sessions.Update(session, new(2, 1)));
            using (var wait = new CancellationTokenSource(TimeSpan.FromMilliseconds(300)))
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await delivered.Reader.ReadAsync(wait.Token));
            Assert.True(await tasks.CompleteAsync(first.Task.Id, "worker", first.Task.Epoch, "{}", default));
            Assert.True(sessions.Update(session, new(3, 1, first.DeliveryId)));
            var second = await delivered.Reader.ReadAsync(stop.Token);
            Assert.NotEqual(first.Task.Id, second.Task.Id);
            Assert.True(await tasks.CompleteAsync(second.Task.Id, "worker", second.Task.Epoch, "{}", default));
            failSend = true;
            var third = await tasks.EnqueueAsync(policies.Resolve(hub, "work", 1, payload),
                payload.GetRawText(), null, null, default);
            broadcaster.PublishMatchingHint(hub, "work", 1, 0);
            Assert.True(sessions.Update(session, new(4, 1, second.DeliveryId)));
            var uncertain = await failedSend.Task.WaitAsync(stop.Token);
            await loop;
            Assert.Equal(third, uncertain.Task.Id);
            Assert.Equal("Leased", (await tasks.GetAsync(third, default))!.State);
            var leases = new HubMatchingPartitionLeaseStore(tasks);
            Assert.True(await leases.ReleaseAsync(hub, 0, session.Owner.OwnerId, session.Owner.Epoch, default));
            var takeover = await leases.AcquireOrReadAsync(hub, 0, 1, "other", "http://localhost:5101",
                TimeSpan.FromSeconds(8), default);
            async Task<IReadOnlyList<HubLeasedTask>> Claim() => await tasks.ClaimAsync("other-worker", [hub],
                [new(hub, "work", 1)], 1, TimeSpan.FromSeconds(30), default,
                matchingPartition: 0, matchingPartitionCount: 1,
                ownerFence: new(takeover.OwnerId, takeover.Epoch));
            Assert.Empty(await Claim());
            await using (var db = new NpgsqlConnection(database))
            {
                await db.OpenAsync();
                await using var expire = new NpgsqlCommand("""
                    UPDATE wfc_scale.hub_task SET lease_until=clock_timestamp()-interval '1 second' WHERE id=@id
                    """, db);
                expire.Parameters.AddWithValue("id", third);
                Assert.Equal(1, await expire.ExecuteNonQueryAsync());
            }
            var retried = Assert.Single(await Claim());
            Assert.Equal(2, retried.Attempts);
            Assert.True(retried.Epoch > uncertain.Task.Epoch);
            Assert.False(await tasks.CompleteAsync(third, "worker", uncertain.Task.Epoch, "{}", default));

            // Independent sessions compete for the same committed backlog.
            // Repeated capacity refreshes must not assign a second lease to
            // any session while its first delivery is outstanding.
            var concurrentHub = "push-race-" + Guid.NewGuid().ToString("N");
            var racePolicies = new HubPolicyRegistry().Register(concurrentHub, "work", 1, 1, _ => { });
            for (var i = 0; i < 16; i++)
                await tasks.EnqueueAsync(racePolicies.Resolve(concurrentHub, "work", 1, payload),
                    payload.GetRawText(), null, null, default);
            var raceAssignments = Channel.CreateUnbounded<HubPushAssignment>();
            var raceSessions = new List<HubPushSessions.Session>();
            var raceLoops = new List<Task>();
            for (var i = 0; i < 16; i++)
            {
                var (s, _) = await sessions.RegisterAsync(new("race-" + i, concurrentHub, 0, 1,
                    [new(concurrentHub, "work", 1)], null), default);
                Assert.NotNull(s);
                Assert.True(sessions.Connect(s));
                raceSessions.Add(s);
                raceLoops.Add(sessions.RunAsync(s, (a, _) => { raceAssignments.Writer.TryWrite(a); return Task.CompletedTask; }, stop.Token));
                Assert.True(sessions.Update(s, new(1, 1)));
            }
            foreach (var s in raceSessions) Assert.True(sessions.Update(s, new(2, 1)));
            var results = new List<HubPushAssignment>();
            for (var i = 0; i < 16; i++) results.Add(await raceAssignments.Reader.ReadAsync(stop.Token));
            Assert.Equal(16, results.Select(a => a.Task.Id).Distinct().Count());
            Assert.Equal(16, results.Select(a => a.SessionId).Distinct().Count());
            Assert.All(results, a => Assert.Equal(1L, a.Task.Epoch));
            var revokedSession = raceSessions[0];
            var revokedDelivery = Assert.Single(results, a => a.SessionId == revokedSession.Id);
            Assert.False(sessions.Revoke(revokedSession));
            Assert.False(sessions.Update(revokedSession, new(3, 1)));
            Assert.True(sessions.Update(revokedSession, new(3, 0, revokedDelivery.DeliveryId)));
            Assert.True(sessions.Revoke(revokedSession));
            Assert.False(sessions.Update(revokedSession, new(4, 1)));

            var multiHub = "push-multicredit-" + Guid.NewGuid().ToString("N");
            var multiPolicies = new HubPolicyRegistry().Register(multiHub, "work", 1, 1, _ => { });
            for (var i = 0; i < 8; i++)
                await tasks.EnqueueAsync(multiPolicies.Resolve(multiHub, "work", 1, payload),
                    payload.GetRawText(), null, null, default);
            var (multi, _) = await sessions.RegisterAsync(new("multi-worker", multiHub, 0, 1,
                [new(multiHub, "work", 1)], null, 4), default);
            Assert.NotNull(multi);
            Assert.True(sessions.Connect(multi));
            var multiAssignments = Channel.CreateUnbounded<HubPushAssignment>();
            var multiLoop = sessions.RunAsync(multi, (a, _) =>
            { multiAssignments.Writer.TryWrite(a); return Task.CompletedTask; }, stop.Token);
            Assert.True(sessions.Update(multi, new(1, 4)));
            var batch = new List<HubPushAssignment>();
            for (var i = 0; i < 4; i++) batch.Add(await multiAssignments.Reader.ReadAsync(stop.Token));
            Assert.Equal(4, batch.Select(a => a.Task.Id).Distinct().Count());
            Assert.True(sessions.Update(multi, new(2, 4))); // stale local view cannot duplicate credit
            using (var wait = new CancellationTokenSource(TimeSpan.FromMilliseconds(300)))
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await multiAssignments.Reader.ReadAsync(wait.Token));
            Assert.True(await tasks.CompleteAsync(batch[0].Task.Id, "multi-worker", batch[0].Task.Epoch, "{}", default));
            Assert.True(sessions.Update(multi, new(3, 1,
                SeenDeliveryIds: batch.Skip(1).Select(a => a.DeliveryId).ToArray(),
                ReleasedDeliveryIds: [batch[0].DeliveryId])));
            var next = await multiAssignments.Reader.ReadAsync(stop.Token);
            Assert.DoesNotContain(batch, a => a.Task.Id == next.Task.Id);
            Assert.False(sessions.Revoke(multi));
            Assert.True(sessions.Update(multi, new(4, 0, ReleasedDeliveryIds:
                batch.Skip(1).Select(a => a.DeliveryId).Append(next.DeliveryId).ToArray())));
            Assert.True(sessions.Revoke(multi));
            await stop.CancelAsync();
            await Task.WhenAll(raceLoops);
            await multiLoop;
            await ownership.StopAsync(default);
            ownership.Dispose();
        }
        finally { foreach (var key in keys) Environment.SetEnvironmentVariable(key, original[key]); }
    }
}
