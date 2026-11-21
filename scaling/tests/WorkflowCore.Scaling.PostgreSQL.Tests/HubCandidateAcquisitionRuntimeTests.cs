using System.Text.Json;
using Npgsql;
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
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExistingCandidateMatcherClaimsWorkWithTaskIdHintsOffOrOn(bool taskIdHintsEnabled)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var keys = new[] { "WFC_HUB_MATCHING_ENABLED", "WFC_HUB_MATCHING_CANDIDATES_ENABLED",
            "WFC_HUB_MATCHING_NODE_ID", "WFC_HUB_MATCHING_ADVERTISE_URL",
            "WFC_HUB_MATCHING_PEERS", "WFC_HUB_MATCHING_PARTITIONS",
            "WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY", "WFC_HUB_CANDIDATE_BACKPRESSURE",
            "WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED" };
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
            Environment.SetEnvironmentVariable(keys[8], taskIdHintsEnabled ? "true" : "false");

            var hub = "candidate-credit-" + Guid.NewGuid().ToString("N");
            await using var tasks = new HubTaskStore(database, 12);
            var policies = new HubTaskRouteRegistry().Register(hub, "work", 1, 1);
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
                await using (var hintSetting = tasks.DataSource.CreateCommand(
                    "SELECT current_setting('wfc.hub_candidate_task_id_hints',true)='on'"))
                    Assert.Equal(taskIdHintsEnabled, (bool)(await hintSetting.ExecuteScalarAsync())!);
                var capability = new HubCapability(hub, "work", 1);
                var payload = JsonSerializer.SerializeToElement(new { message = "credit-lifetime" });
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(40));
                for (var i = 0; i < 12; i++)
                {
                    var expected = await tasks.EnqueueAsync(policies.Resolve(hub, "work", 1),
                        payload.GetRawText(), null, null, deadline.Token);
                    readiness.PublishMatchingHint(hub, "work", 1, 0);
                    var leased = Assert.Single(await matcher.AcquireAsync(
                        new HubClaimRequest("worker", [hub], [capability], 1,
                            MatchingPartition: 0, MatchingPartitionCount: 64),
                        deadline.Token));
                    Assert.Equal(expected, leased.Id);
                    Assert.True(await tasks.CompleteAsync(leased.Id, "worker", leased.Epoch,
                        "{}", deadline.Token));
                    // The HTTP completion routes call this only after their
                    // durable completion succeeds; direct store completion
                    // alone intentionally cannot release transport accounting.
                    matcher.ReleaseCompletedLease(leased.Id, leased.Epoch);

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

    [Fact]
    public async Task ListenerRoutesActualPartitionsAboveDiscoveryBucketRangeToTheirOwner()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var keys = new[] { "WFC_HUB_MATCHING_ENABLED", "WFC_HUB_MATCHING_CANDIDATES_ENABLED",
            "WFC_HUB_MATCHING_NODE_ID", "WFC_HUB_MATCHING_ADVERTISE_URL", "WFC_HUB_MATCHING_PEERS",
            "WFC_HUB_MATCHING_PARTITIONS", "WFC_HUB_CANDIDATE_BACKPRESSURE" };
        var original = keys.ToDictionary(k => k, Environment.GetEnvironmentVariable);
        try
        {
            Environment.SetEnvironmentVariable(keys[0], "true"); Environment.SetEnvironmentVariable(keys[1], "true");
            Environment.SetEnvironmentVariable(keys[2], "candidate-partition-route"); Environment.SetEnvironmentVariable(keys[3], "http://localhost:5100");
            Environment.SetEnvironmentVariable(keys[4], "http://localhost:5100"); Environment.SetEnvironmentVariable(keys[5], "64");
            Environment.SetEnvironmentVariable(keys[6], "false");
            var hub = "candidate-part-route-" + Guid.NewGuid().ToString("N");
            using var lifetime = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await using var tasks = new HubTaskStore(database, 12);
            var readiness = new HubReadinessBroadcaster(tasks,
                new HubWorkflowStore(tasks, new HubTaskRouteRegistry(), new HubWorkflowRegistry()),
                NullLogger<HubReadinessBroadcaster>.Instance);
            var ownership = new HubMatchingOwnership(new HubMatchingLeaseStore(tasks),
                new HubMatchingPartitionLeaseStore(tasks), NullLogger<HubMatchingOwnership>.Instance);
            var matcher = new HubCandidateAcquisition(tasks, ownership, readiness,
                NullLogger<HubCandidateAcquisition>.Instance);
            var listener = new PostgresHubNotificationListener(database, readiness,
                NullLogger<PostgresHubNotificationListener>.Instance, taskIdHintsEnabled: true);
            var exact = new System.Collections.Concurrent.ConcurrentDictionary<int, Guid>();
            var coarse = new System.Collections.Concurrent.ConcurrentDictionary<int, byte>();
            var recovery = 0;
            long hydrationCalls = 0;
            using var meter = new System.Diagnostics.Metrics.MeterListener();
            meter.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == HubTelemetry.MeterName &&
                    instrument.Name == "workflow_hub_candidate_hydration_calls")
                    listener.EnableMeasurementEvents(instrument);
            };
            meter.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
            {
                if (instrument.Name == "workflow_hub_candidate_hydration_calls")
                    Interlocked.Add(ref hydrationCalls, value);
            });
            meter.Start();
            readiness.CandidateTaskChanged += (observed, id, partition, _, _) =>
            { if (observed == hub) exact[partition] = id; };
            readiness.CandidateRescanRequested += (observed, _, _, partition) =>
            { if (observed == hub && partition is int value) coarse[value] = 0; };
            readiness.CandidateRecoveryRequired += () => Interlocked.Increment(ref recovery);
            await ownership.StartAsync(lifetime.Token); await matcher.StartAsync(lifetime.Token);
            try
            {
                var owner = await ownership.AcquirePartitionAsync(hub, 0, lifetime.Token);
                var highOwner = await ownership.AcquirePartitionAsync(hub, 63, lifetime.Token);
                Assert.True(ownership.IsLocal(owner));
                Assert.True(ownership.IsLocal(highOwner));
                await listener.StartAsync(lifetime.Token);
                while (!listener.IsListening) await Task.Delay(25, lifetime.Token);
                var ids = new Dictionary<int, Guid> { [64] = Guid.NewGuid(), [4095] = Guid.NewGuid() };
                await using var db = new NpgsqlConnection(database); await db.OpenAsync(lifetime.Token);
                await using (var enableIds = new NpgsqlCommand(
                    "SET wfc.hub_candidate_task_id_hints='on'", db))
                    await enableIds.ExecuteNonQueryAsync(lifetime.Token);
                foreach (var (partition, id) in ids)
                {
                    await using var insert = new NpgsqlCommand("""
                        INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
                        VALUES (@id,@hub,@partition,'route-test',1,'{}')
                        """, db);
                    insert.Parameters.AddWithValue("id", id); insert.Parameters.AddWithValue("hub", hub);
                    insert.Parameters.AddWithValue("partition", partition); await insert.ExecuteNonQueryAsync(lifetime.Token);
                }
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(8));
                while (!ids.All(pair => exact.TryGetValue(pair.Key, out var observed) && observed == pair.Value))
                    await Task.Delay(25, timeout.Token);
                while (matcher.CandidateBuffered < 2) await Task.Delay(25, timeout.Token);
                using var hydrationTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                while (Volatile.Read(ref hydrationCalls) == 0) await Task.Delay(25, hydrationTimeout.Token);
                Assert.Equal(ids[64], exact[64]); Assert.Equal(ids[4095], exact[4095]);
                Assert.True(Volatile.Read(ref hydrationCalls) > 0,
                    "Committed task-ID notifications must reach the batched candidate hydration query");
                Assert.Equal(2, matcher.GenerationCount);
                Assert.Equal(0, Volatile.Read(ref recovery));
                // Old 014/030 typed payloads remain valid with task partitions
                // above the 0..63 candidate discovery bucket range.
                foreach (var partition in ids.Keys)
                {
                    await using var oldHint = new NpgsqlCommand("SELECT pg_notify('wfc_hub_matching_ready',json_build_object('hub',@hub,'type','route-test','version',1,'partition',@partition)::text)", db);
                    oldHint.Parameters.AddWithValue("hub", hub); oldHint.Parameters.AddWithValue("partition", partition);
                    await oldHint.ExecuteNonQueryAsync(lifetime.Token);
                }
                using var coarseTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                while (ids.Keys.Any(partition => !coarse.ContainsKey(partition))) await Task.Delay(25, coarseTimeout.Token);
            }
            finally
            {
                await listener.StopAsync(CancellationToken.None); listener.Dispose();
                await matcher.StopAsync(CancellationToken.None); matcher.Dispose();
                await ownership.StopAsync(CancellationToken.None); ownership.Dispose();
            }
        }
        finally { foreach (var key in keys) Environment.SetEnvironmentVariable(key, original[key]); }
    }
}
