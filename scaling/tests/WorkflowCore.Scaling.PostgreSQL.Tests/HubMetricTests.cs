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
    [InlineData("activity", "claim")]
    [InlineData("activity", "poll")]
    [InlineData("workflow", "claim")]
    [InlineData("workflow", "poll")]
    public async Task ClaimMeasurementsCaptureAllOutcomesAndTaskLanes(string laneName, string transport)
    {
        using var capture = new Capture();
        var lane = new KeyValuePair<string, object?>("lane", laneName);
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
        var inflight = values.Where(x => x.Name == "workflow_claim_inflight").ToArray();
        Assert.Equal(new double[] {1, -1, 1, -1, 1, -1, 1, -1}, inflight.Select(x => x.Value));
        Assert.All(inflight, x => Assert.Equal(laneName, x.Tags["lane"]));
        Assert.All(values, x => Assert.Equal(
            x.Name == "workflow_claim_duration_seconds" ? 3 : 2, x.Tags.Count));
    }

    [Theory]
    [InlineData("activity", "claim", "claimed")]
    [InlineData("activity", "claim", "empty")]
    [InlineData("activity", "claim", "error")]
    [InlineData("activity", "claim", "cancelled")]
    [InlineData("activity", "poll", "claimed")]
    [InlineData("activity", "poll", "empty")]
    [InlineData("activity", "poll", "error")]
    [InlineData("activity", "poll", "cancelled")]
    [InlineData("workflow", "claim", "claimed")]
    [InlineData("workflow", "claim", "empty")]
    [InlineData("workflow", "claim", "error")]
    [InlineData("workflow", "claim", "cancelled")]
    [InlineData("workflow", "poll", "claimed")]
    [InlineData("workflow", "poll", "empty")]
    [InlineData("workflow", "poll", "error")]
    [InlineData("workflow", "poll", "cancelled")]
    public async Task PendingClaimRemainsInflightUntilRequestEnds(string laneName, string transport, string outcome)
    {
        using var capture = new Capture();
        var response = new TaskCompletionSource<HubLeasedTask[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = HubRuntimeWorker.InstrumentClaimAsync("tasks:" + transport,
            new KeyValuePair<string, object?>("lane", laneName), () => response.Task);

        Assert.False(pending.IsCompleted);
        Assert.Single(capture.Values, x => x.Name == "workflow_claim_attempts");
        Assert.Equal(1, capture.Values.Where(x => x.Name == "workflow_claim_inflight").Sum(x => x.Value));
        Assert.DoesNotContain(capture.Values, x => x.Name == "workflow_claim_duration_seconds");

        if (outcome == "error")
        {
            response.SetException(new HttpRequestException());
            await Assert.ThrowsAsync<HttpRequestException>(() => pending);
        }
        else if (outcome == "cancelled")
        {
            response.SetCanceled();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        }
        else
        {
            response.SetResult(outcome == "empty" ? [] : [new HubLeasedTask(
                Guid.NewGuid(), "test", 0, laneName == "workflow" ? "__native_workflow__" : "activity", 1,
                null, JsonSerializer.SerializeToElement(new {}), 1, 1, DateTime.UtcNow.AddMinutes(1))]);
            await pending;
        }

        Assert.Equal(0, capture.Values.Where(x => x.Name == "workflow_claim_inflight").Sum(x => x.Value));
        var finished = Assert.Single(capture.Values, x => x.Name == "workflow_claim_duration_seconds");
        Assert.Equal(outcome, finished.Tags["outcome"]);
        Assert.True(finished.Value >= 0);
        Assert.All(capture.Values, x => {
            Assert.Equal(laneName, x.Tags["lane"]);
            Assert.Equal(transport, x.Tags["transport"]);
        });
    }

    [Theory]
    [InlineData("activity", "claim")]
    [InlineData("activity", "poll")]
    [InlineData("workflow", "claim")]
    [InlineData("workflow", "poll")]
    public async Task InflightTracksConcurrentClaimsEndingOutOfOrder(string laneName, string transport)
    {
        using var capture = new Capture();
        var responses = Enumerable.Range(0, 3).Select(_ =>
            new TaskCompletionSource<HubLeasedTask[]>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        var pending = responses.Select(response => HubRuntimeWorker.InstrumentClaimAsync("tasks:" + transport,
            new KeyValuePair<string, object?>("lane", laneName), () => response.Task)).ToArray();
        double Inflight() => capture.Values.Where(x => x.Name == "workflow_claim_inflight").Sum(x => x.Value);

        Assert.Equal(3, Inflight());
        responses[1].SetResult([]);
        await pending[1];
        Assert.Equal(2, Inflight());
        responses[2].SetResult([]);
        await pending[2];
        Assert.Equal(1, Inflight());
        responses[0].SetResult([]);
        await pending[0];
        Assert.Equal(0, Inflight());
        Assert.Equal(3, capture.Values.Count(x => x.Name == "workflow_claim_attempts"));
        Assert.Equal(3, capture.Values.Count(x => x.Name == "workflow_claim_duration_seconds"));
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
