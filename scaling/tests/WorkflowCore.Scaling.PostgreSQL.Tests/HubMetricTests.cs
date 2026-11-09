using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json;
using WorkflowCore.Hub;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

[CollectionDefinition("Metric isolation", DisableParallelization = true)]
public sealed class MetricIsolationCollection { }

[Collection("Metric isolation")]
public sealed class HubMetricTests
{
    private sealed record Measurement(string Name, double Value, Dictionary<string, object?> Tags);
    private sealed class Capture : IDisposable
    {
        public readonly ConcurrentQueue<Measurement> Values = new();
        private readonly MeterListener listener = new();
        public Capture()
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == HubTelemetry.MeterName && (instrument.Name.StartsWith("workflow_claim_") || instrument.Name == "workflow_activities_completed"))
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

    [Theory]
    [InlineData("claim")]
    [InlineData("poll")]
    public async Task ClaimMeasurementsCaptureAllOutcomesAndTaskLanes(string transport)
    {
        using var capture = new Capture();
        var lane = new KeyValuePair<string, object?>("lane", "activity");
        var endpoint = "tasks:" + transport;
        HubLeasedTask TaskOf(string type) => new(Guid.NewGuid(), "test", 0, type, 1,
            null, JsonSerializer.SerializeToElement(new {}), 1, 1, DateTime.UtcNow.AddMinutes(1));
        await HubRuntimeWorker.InstrumentClaimAsync(endpoint, lane, () => Task.FromResult(Array.Empty<HubLeasedTask>()));
        await HubRuntimeWorker.InstrumentClaimAsync(endpoint, lane, () => Task.FromResult(new[] {
            TaskOf("activity"), TaskOf("activity"), TaskOf("__workflow__"), TaskOf("__native_workflow__") }));
        await Assert.ThrowsAsync<HttpRequestException>(() => HubRuntimeWorker.InstrumentClaimAsync(endpoint, lane,
            () => Task.FromException<HubLeasedTask[]>(new HttpRequestException())));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => HubRuntimeWorker.InstrumentClaimAsync(endpoint, lane,
            () => Task.FromCanceled<HubLeasedTask[]>(new CancellationToken(true))));
        var values = capture.Values.ToArray();
        Assert.Equal(4, values.Count(x => x.Name == "workflow_claim_attempts"));
        Assert.Single(values, x => x.Name == "workflow_claim_empty");
        Assert.Equal(new double[] {0, 4}, values.Where(x => x.Name == "workflow_claim_batch_size").Select(x => x.Value));
        var success = values.Where(x => x.Name == "workflow_claim_success").ToArray();
        Assert.Equal(2, success.Single(x => (string)x.Tags["lane"]! == "activity").Value);
        Assert.Equal(2, success.Single(x => (string)x.Tags["lane"]! == "workflow").Value);
        var duration = values.Where(x => x.Name == "workflow_claim_duration_seconds").ToArray();
        Assert.Equal(new[] {"cancelled", "claimed", "empty", "error"}, duration.Select(x => (string)x.Tags["outcome"]!).Order());
        Assert.All(values, x => Assert.Equal(transport, x.Tags["transport"]));
    }

    [Fact]
    public async Task DurableCompletionCountsOnlyNewActivityCommitsAcrossStores()
    {
        if (Environment.GetEnvironmentVariable("WFC_TEST_PG") is null) return;
        using var capture = new Capture();
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
    public async Task NormalWorkerLoopEmitsClaimMetricsForRealHttpCalls()
    {
        using var capture = new Capture();
        await new HubNativeTransportTests().CompletionOutageResendsFrozenResultWithoutFailingOrExecutingBodyAgain();
        var attempts = capture.Values.Where(x => x.Name == "workflow_claim_attempts").ToArray();
        Assert.NotEmpty(attempts);
        Assert.All(attempts, x => { Assert.Equal("activity", x.Tags["lane"]); Assert.Equal("claim", x.Tags["transport"]); });
        Assert.Contains(capture.Values, x => x.Name == "workflow_claim_success" && x.Value == 1);
    }
}
