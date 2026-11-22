using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

[Collection("CandidateAcquisitionRuntime")]
public sealed class HubCandidateRecoveryLatencyTests
{
    private sealed record Observation(string Arm, bool RequireDemand, string Scenario, int Repetition,
        bool Claimed, double? ArrivalToClaimMs, double? EligibleToClaimMs,
        string? ArrivedAtUtc, string? EligibleAtUtc, string? DemandAtUtc,
        string? ClaimedAtUtc, int PendingOutboxRows, string? Error);

    [Fact]
    public async Task MeasureDurableClaimLatencyAcrossMissedHintRecoveryMatrix()
    {
        var output = Environment.GetEnvironmentVariable("WFC_CANDIDATE_RECOVERY_OUTPUT");
        if (string.IsNullOrWhiteSpace(output)) return;
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for recovery measurements");
        var arm = Environment.GetEnvironmentVariable("WFC_CANDIDATE_RECOVERY_ARM") ?? "candidate";
        var repetitions = int.TryParse(Environment.GetEnvironmentVariable("WFC_CANDIDATE_RECOVERY_REPETITIONS"), out var configuredRepetitions)
            ? Math.Clamp(configuredRepetitions, 1, 100) : 2;
        var focus = Environment.GetEnvironmentVariable("WFC_CANDIDATE_RECOVERY_FOCUS");
        var keys = new[] { "WFC_HUB_MATCHING_ENABLED", "WFC_HUB_MATCHING_CANDIDATES_ENABLED",
            "WFC_HUB_MATCHING_NODE_ID", "WFC_HUB_MATCHING_ADVERTISE_URL", "WFC_HUB_MATCHING_PEERS",
            "WFC_HUB_MATCHING_PARTITIONS", "WFC_HUB_CANDIDATE_BACKPRESSURE", "WFC_HUB_PUSH_ENABLED" };
        var original = keys.ToDictionary(k => k, Environment.GetEnvironmentVariable);
        var observations = new List<Observation>();
        try
        {
            Environment.SetEnvironmentVariable(keys[0], "true");
            Environment.SetEnvironmentVariable(keys[1], "true");
            Environment.SetEnvironmentVariable(keys[2], "candidate-recovery-latency");
            Environment.SetEnvironmentVariable(keys[3], "http://localhost:5100");
            Environment.SetEnvironmentVariable(keys[4], "http://localhost:5100");
            Environment.SetEnvironmentVariable(keys[5], "1");
            Environment.SetEnvironmentVariable(keys[7], "true");
            foreach (var requireDemand in new[] { false, true })
            {
                Environment.SetEnvironmentVariable(keys[6], requireDemand ? "true" : "false");
                var hub = "recovery-latency-" + Guid.NewGuid().ToString("N");
                await using var tasks = new HubTaskStore(database, 12);
                var policies = new HubTaskRouteRegistry().Register(hub, "recovery-work", 1, 1);
                var ownership = new HubMatchingOwnership(new HubMatchingLeaseStore(tasks),
                    new HubMatchingPartitionLeaseStore(tasks), NullLogger<HubMatchingOwnership>.Instance);
                var readiness = new HubReadinessBroadcaster(tasks,
                    new HubWorkflowStore(tasks, policies, new HubWorkflowRegistry()),
                    NullLogger<HubReadinessBroadcaster>.Instance);
                var matcher = new HubCandidateAcquisition(tasks, ownership, readiness,
                    NullLogger<HubCandidateAcquisition>.Instance,
                    new HubCandidateCoordinatorOptions(RecoveryCadence: TimeSpan.FromSeconds(10)), requireDemand);
                using var lifetime = new CancellationTokenSource(TimeSpan.FromMinutes(20));
                await ownership.StartAsync(lifetime.Token);
                await matcher.StartAsync(lifetime.Token);
                await readiness.StartAsync(lifetime.Token);
                try
                {
                    var owner = await ownership.AcquirePartitionAsync(hub, 0, lifetime.Token);
                    Assert.True(ownership.IsLocal(owner));
                    var allScenarios = new[] { "arrival-after-sweep", "no-listen-hint",
                        "outbox-no-dispatcher", "future-due", "lease-expiry", "demand-after-absent" };
                    var scenarios = string.IsNullOrWhiteSpace(focus) ? allScenarios : allScenarios.Where(x => x == focus).ToArray();
                    if (scenarios.Length == 0) throw new ArgumentException($"Unknown recovery focus '{focus}'");
                    for (var scenarioIndex = 0; scenarioIndex < scenarios.Length; scenarioIndex++)
                    for (var repetition = 1; repetition <= repetitions; repetition++)
                    {
                        var scenario = scenarios[scenarioIndex];
                var observation = await MeasureOne(arm, database, hub, tasks, matcher,
                            requireDemand, scenario, repetition, lifetime.Token);
                        observations.Add(observation);
                    }
                }
                finally
                {
                    await readiness.StopAsync(CancellationToken.None);
                    readiness.Dispose();
                    await matcher.StopAsync(CancellationToken.None);
                    matcher.Dispose();
                    await ownership.StopAsync(CancellationToken.None);
                    ownership.Dispose();
                }
            }
        }
        finally
        {
            foreach (var key in keys) Environment.SetEnvironmentVariable(key, original[key]);
            await File.WriteAllTextAsync(output, JsonSerializer.Serialize(new
            {
                measuredUtc = DateTimeOffset.UtcNow,
                sourceCommitOrLabel = Environment.GetEnvironmentVariable("WFC_EVIDENCE_SOURCE_COMMIT") ?? "uncommitted",
                testAssemblySha256 = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(
                    Path.Combine(AppContext.BaseDirectory, "WorkflowCore.Scaling.PostgreSQL.Tests.dll")))),
                featureFlags = new { candidateTopologyEnabled = true,
                    outboxDispatcher = false,
                    listenConsumer = false, pushRecoverySweep = true },
                repetitions,
                focus = focus ?? "all",
                claimMeansDurablyLeasedTaskReturnedByHubCandidateAcquisition,
                notificationListenerStarted = false,
                recoveryCadenceSeconds = 10,
                observations
            }, new JsonSerializerOptions { WriteIndented = true }));
        }
        var expectedObservations = (string.IsNullOrWhiteSpace(focus) ? 6 : 1) * 2 * repetitions;
        Assert.Equal(expectedObservations, observations.Count);
        Assert.All(observations, row => Assert.True(row.Claimed, row.Error));
    }

    private const string claimMeansDurablyLeasedTaskReturnedByHubCandidateAcquisition =
        "Claim timestamp is after AcquireAsync returned a task whose DB state/epoch was committed; no event-only rescan is counted.";

    private static async Task<Observation> MeasureOne(string arm, string database, string hub, HubTaskStore tasks,
        HubCandidateAcquisition matcher, bool requireDemand, string scenario, int repetition,
        CancellationToken ct)
    {
        var id = Guid.NewGuid();
        DateTimeOffset arrival = default;
        DateTimeOffset? eligibleAt = null;
        DateTimeOffset? demandAt = null;
        Task<IReadOnlyList<HubLeasedTask>>? pendingClaim = null;
        var request = new HubClaimRequest("recovery-measure-worker", [hub],
            [new HubCapability(hub, "recovery-work", 1)], 1,
            MatchingPartition: 0, MatchingPartitionCount: 1);
        try
        {
            if (scenario == "arrival-after-sweep")
            {
                // With RequireDemand the registry intentionally does not scan
                // without a worker. Establish demand before the sweep boundary,
                // then introduce work immediately after that completed sweep.
                if (requireDemand) pendingClaim = matcher.AcquireAsync(request, ct);
                await WaitForNextSweep(matcher, ct);
            }
            arrival = DateTimeOffset.UtcNow;
            var future = scenario == "future-due";
            var expired = scenario == "lease-expiry";
            var outbox = scenario == "outbox-no-dispatcher";
            await using (var db = new NpgsqlConnection(database))
            {
                await db.OpenAsync(ct);
                await using var tx = await db.BeginTransactionAsync(ct);
                await using (var settings = new NpgsqlCommand("SELECT set_config('wfc.hub_notification_outbox',@outbox,true)", db, tx))
                {
                    settings.Parameters.AddWithValue("outbox", outbox ? "on" : "off");
                    await settings.ExecuteNonQueryAsync(ct);
                }
                await using (var insert = new NpgsqlCommand("""
                    INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,
                        state,due_at,owner,lease_until,epoch,attempts,max_attempts)
                    VALUES (@id,@hub,0,'recovery-work',1,'{}',@state,
                        CASE WHEN @future THEN now()+interval '2 seconds' ELSE now() END,
                        CASE WHEN @expired THEN 'previous-owner' ELSE NULL END,
                        CASE WHEN @expired THEN now()+interval '2 seconds' ELSE NULL END,
                        CASE WHEN @expired THEN 1 ELSE 0 END,
                        CASE WHEN @expired THEN 1 ELSE 0 END,5)
                    """, db, tx))
                {
                    insert.Parameters.AddWithValue("id", id);
                    insert.Parameters.AddWithValue("hub", hub);
                    insert.Parameters.AddWithValue("state", expired ? (short)1 : (short)0);
                    insert.Parameters.AddWithValue("future", future);
                    insert.Parameters.AddWithValue("expired", expired);
                    await insert.ExecuteNonQueryAsync(ct);
                }
                await tx.CommitAsync(ct);
            }
            if (future || expired) eligibleAt = arrival.AddSeconds(2);
            if (scenario == "demand-after-absent")
            {
                await Task.Delay(TimeSpan.FromMilliseconds(2500), ct);
                demandAt = DateTimeOffset.UtcNow;
            }
            else demandAt = DateTimeOffset.UtcNow;
            var leases = await (pendingClaim ?? matcher.AcquireAsync(request, ct))
                .WaitAsync(TimeSpan.FromSeconds(45), ct);
            var claimAt = DateTimeOffset.UtcNow;
            var lease = Assert.Single(leases);
            Assert.Equal(id, lease.Id);
            Assert.True(await tasks.CompleteAsync(id, "recovery-measure-worker", lease.Epoch, "{}", ct));
            matcher.ReleaseCompletedLease(id, lease.Epoch);
            var outboxRows = await OutboxRows(tasks, hub, ct);
            return new(arm, requireDemand, scenario, repetition, true,
                (claimAt - arrival).TotalMilliseconds,
                eligibleAt is null ? null : (claimAt - eligibleAt.Value).TotalMilliseconds,
                arrival.ToString("O"), eligibleAt?.ToString("O"), demandAt?.ToString("O"),
                claimAt.ToString("O"), outboxRows, null);
        }
        catch (Exception ex)
        {
            return new(arm, requireDemand, scenario, repetition, false, null, null,
                arrival == default ? null : arrival.ToString("O"), eligibleAt?.ToString("O"),
                demandAt?.ToString("O"), null, await OutboxRows(tasks, hub, ct), ex.ToString());
        }
    }

    private static async Task WaitForNextSweep(HubCandidateAcquisition matcher, CancellationToken ct)
    {
        var before = CompletedSweeps(matcher);
        var limit = Stopwatch.StartNew();
        while (CompletedSweeps(matcher) <= before)
        {
            if (limit.Elapsed > TimeSpan.FromSeconds(30)) throw new TimeoutException("No completed candidate recovery sweep observed");
            await Task.Delay(25, ct);
        }
    }

    private static long CompletedSweeps(HubCandidateAcquisition matcher)
    {
        var field = typeof(HubCandidateAcquisition).GetField("_registry",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return ((HubPartitionCandidateRegistry)field.GetValue(matcher)!).Snapshot().CompletedSweeps;
    }

    private static async Task<int> OutboxRows(HubTaskStore tasks, string hub, CancellationToken ct)
    {
        await using var command = tasks.DataSource.CreateCommand(
            "SELECT count(*) FROM wfc_scale.hub_notification_outbox WHERE hub_id=@hub");
        command.Parameters.AddWithValue("hub", hub);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct));
    }
}
