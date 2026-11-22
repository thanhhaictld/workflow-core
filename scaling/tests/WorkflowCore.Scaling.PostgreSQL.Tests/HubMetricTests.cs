using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

[CollectionDefinition("Metric isolation", DisableParallelization = true)]
public sealed class MetricIsolationCollection { }

[Collection("Metric isolation")]
public sealed class HubMetricTests
{
    [Theory]
    [InlineData(null, true)]
    [InlineData("", true)]
    [InlineData("true", true)]
    [InlineData("1", true)]
    [InlineData("false", false)]
    [InlineData("False", false)]
    [InlineData("FALSE", false)]
    [InlineData("0", false)]
    public async Task MetricsSamplerCanBeDisabledWithoutRemovingGatewayStores(string? flag, bool enabled)
    {
        const string flagName = "WFC_HUB_METRICS_SAMPLER_ENABLED";
        var previous = Environment.GetEnvironmentVariable(flagName);
        try
        {
            Environment.SetEnvironmentVariable(flagName, flag);
            var services = new ServiceCollection();
            services.AddHubGateway("Host=localhost;Database=sampler-registration;Username=test;Password=unused");
            await using var store = Assert.IsType<HubTaskStore>(
                services.Single(d => d.ServiceType == typeof(HubTaskStore)).ImplementationInstance);
            Assert.Equal(enabled, services.Any(d => d.ServiceType == typeof(IHostedService) &&
                d.ImplementationType == typeof(HubMetricsSampler)));
            Assert.Contains(services, d => d.ServiceType == typeof(HubOperationsStore));
        }
        finally
        {
            Environment.SetEnvironmentVariable(flagName, previous);
        }
    }

    [Fact]
    public void DatabaseGaugesPublishActualSnapshotValuesIncludingZero()
    {
        var expected = new Dictionary<string, long> {
            ["workflow_hub_database_connections"] = 11,
            ["workflow_hub_database_active_connections"] = 3,
            ["workflow_hub_queue_ready"] = 7,
            ["workflow_hub_queue_leased"] = 2,
            ["workflow_hub_queue_dead"] = 0
        };
        var actual = new Dictionary<string, long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) => {
            if (instrument.Meter.Name == HubTelemetry.MeterName && expected.ContainsKey(instrument.Name))
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) => actual[instrument.Name] = value);
        listener.Start();
        HubTelemetry.PublishDatabaseSnapshot(new(11, 3, 7, 2, 0));
        listener.RecordObservableInstruments();
        Assert.Equal(expected.Count, actual.Count);
        Assert.All(expected, pair => Assert.Equal(pair.Value, actual[pair.Key]));
    }

    private sealed record Measurement(string Name, double Value, Dictionary<string, object?> Tags);
    private sealed class Capture : IDisposable
    {
        public readonly ConcurrentQueue<Measurement> Values = new();
        private readonly MeterListener listener = new();
        public Capture(params string[] instruments)
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == HubTelemetry.MeterName && instruments.Contains(instrument.Name))
                    l.EnableMeasurementEvents(instrument);
            };
            listener.SetMeasurementEventCallback<long>((i,v,t,_) => Add(i,v,t));
            listener.SetMeasurementEventCallback<int>((i,v,t,_) => Add(i,v,t));
            listener.SetMeasurementEventCallback<double>((i,v,t,_) => Add(i,v,t));
            listener.Start();
        }
        private void Add(Instrument i, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags) =>
            Values.Enqueue(new(i.Name, value, tags.ToArray().ToDictionary(x => x.Key, x => x.Value)));
        public void Dispose() => listener.Dispose();
    }

    [Fact]
    public async Task DurableCompletionCountsOnlyNewActivityCommitsAcrossStores()
    {
        if (Environment.GetEnvironmentVariable("WFC_TEST_PG") is null) return;
        using var capture = new Capture("workflow_activities_completed");
        double Count() => capture.Values.Where(x => x.Name == "workflow_activities_completed").Sum(x => x.Value);
        await new HubGatewayTests().CompletionIsIdempotent_AndRejectsStaleEpoch();
        Assert.Equal(1, Count());
        await new HubWorkflowE2ETests().Start_CompleteAndTransitionTwoSteps_Atomically();
        Assert.Equal(3, Count());
        await new HubRemoteSchedulerTests().RemoteWorkflow_ActivityTimerActivity_ResumesWithDurableWaterfall();
        Assert.Equal(5, Count());
        await new HubNativeRoutedTests().RoutedStepsRecoverFromSnapshotsAndApplyOutputsOnce("scalar");
        Assert.Equal(6, Count());
    }

    [Fact]
    public async Task CandidateAdmissionPublishesClaimLatencyAndCountsConfirmedLeases()
    {
        using var capture = new Capture("workflow_hub_tasks_claimed", "workflow_hub_claim_duration_seconds",
            "workflow_hub_candidate_admission_seconds", "workflow_hub_claim_admission_active");
        var admission = new HubCandidateAdmissionTests();
        await admission.CandidateAdmission_RevalidatesEpochCapabilityTopologyAndOwner();
        await admission.CommitFaultsPreserveTentativeLeaseForDurableReconciliation();

        Assert.Equal(1, capture.Values.Where(m => m.Name == "workflow_hub_tasks_claimed").Sum(m => m.Value));
        var durations = capture.Values.Where(m => m.Name == "workflow_hub_claim_duration_seconds").ToArray();
        Assert.All(durations, m => Assert.Equal("candidate", m.Tags["claim_strategy"]));
        foreach (var outcome in new[] { "claimed", "empty", "owner_lost", "commit_unknown", "error" })
            Assert.Contains(durations, m => Equals(outcome, m.Tags["outcome"]));
        Assert.Equal(durations.Length, capture.Values.Count(m => m.Name == "workflow_hub_candidate_admission_seconds"));
        Assert.Equal(0, capture.Values.Where(m => m.Name == "workflow_hub_claim_admission_active").Sum(m => m.Value));
    }
}
