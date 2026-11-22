using WorkflowCore.Hub;
using Xunit;
using Xunit.Abstractions;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Repeatable CPU profile for indexed matcher queues. Durations are diagnostic
/// output only; correctness is asserted from the reservations, never from time.
/// </summary>
public sealed class HubCandidateMatcherProfileTests(ITestOutputHelper output)
{
    private sealed class Discovery(string hub, HubTaskCandidate[] candidates) : IHubCandidateDiscovery
    {
        public Task<HubCandidateHorizon> CaptureHorizonAsync(string requestedHub, CancellationToken ct) =>
            Task.FromResult(new HubCandidateHorizon(candidates.Length, DateTime.UtcNow));

        public Task<HubCandidatePage> ReadPageAsync(string requestedHub, int bucket,
            HubCandidateStream stream, HubCandidateHorizon horizon, HubCandidateCursor? cursor,
            int pageSize, int byteLimit, CancellationToken ct)
        {
            if (requestedHub != hub || bucket != 0 || stream != HubCandidateStream.ReadyDue)
                return Task.FromResult(new HubCandidatePage([], cursor, true, 0));
            var items = candidates.Where(x => x.Sequence > (cursor?.Sequence ?? 0))
                .Take(pageSize).ToArray();
            var next = items.Length == 0 ? cursor : new HubCandidateCursor(items[^1].Sequence);
            return Task.FromResult(new HubCandidatePage(items, next,
                items.Length < pageSize, items.Length * 256));
        }
    }

    [Theory]
    [InlineData(400)]
    [InlineData(4000)]
    public async Task ProfilesCompatibleAndWorkflowPinMismatchQueues(int lanes)
    {
        if (Environment.GetEnvironmentVariable("WFC_RUN_HUB_MATCHER_PROFILE") is not ("true" or "1" or "True"))
            return;
        foreach (var compatible in new[] { true, false })
        {
            var hub = $"matcher-profile-{lanes}-{compatible}";
            var candidates = Enumerable.Range(1, lanes).Select(i => new HubTaskCandidate(
                Guid.NewGuid(), i, hub, 0, "__workflow__", 1, Guid.NewGuid(),
                "target-flow", 1, 1, DateTime.UtcNow.AddHours(1))).ToArray();
            var pageSize = 256;
            var options = new HubCandidateCoordinatorOptions(
                PageSize: pageSize, PageWireBytes: 1024 * 1024,
                PartitionEntries: lanes + pageSize, ProcessEntries: lanes + pageSize,
                PartitionBytes: 64 * 1024 * 1024, ProcessBytes: 64 * 1024 * 1024,
                PartitionCredits: 16, ProcessCredits: 16, ProcessPartitions: 1,
                RecoveryCadence: TimeSpan.FromHours(1));
            using var registry = new HubPartitionCandidateRegistry(new Discovery(hub, candidates), options);
            // One matching partition maps to one deterministic candidate bucket.
            registry.StartGeneration(new HubMatchingPartitionLease(hub, 0, "profile", "http://localhost",
                1, DateTime.UtcNow.AddHours(1)), 64);

            var discoveryTimer = System.Diagnostics.Stopwatch.StartNew();
            for (var i = 0; i < lanes / pageSize + 8 && registry.Snapshot().Entries < lanes; i++)
                await registry.RunOneTurnAsync(DateTimeOffset.UtcNow, CancellationToken.None);
            discoveryTimer.Stop();
            Assert.Equal(lanes, registry.Snapshot().Entries);

            var matchTimer = System.Diagnostics.Stopwatch.StartNew();
            // Keep a fixed 16-credit cohort so the 4,000 case measures queue
            // scan growth without turning the profile into an N-squared soak.
            for (var i = 0; i < 16; i++)
            {
                var credit = new HubCandidateCredit($"credit-{i}", $"worker-{i}",
                    [new HubCapability(hub, "__workflow__", 1)],
                    [new HubWorkflowCapability(hub, compatible ? "target-flow" : "other-flow", 1)]);
                Assert.True(registry.AddCredit(hub, 0, 1, credit));
            }
            matchTimer.Stop();
            var reservations = registry.Snapshot().Reservations;
            Assert.Equal(compatible ? 16 : 0, reservations);
            output.WriteLine($"lanes={lanes}, workflowPins={(compatible ? "compatible" : "mismatched")}, " +
                $"discovery_ms={discoveryTimer.Elapsed.TotalMilliseconds:F2}, " +
                $"matching_ms={matchTimer.Elapsed.TotalMilliseconds:F2}, reservations={reservations}");
        }
    }
}
