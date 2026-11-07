using System.Diagnostics;
using System.Text.Json;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubTraceCorrelationTests
{
    [Fact]
    public void ShortExecutionContinuesTrace_RetryAndLongWaitUseLinkedTraces()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == HubTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) =>
                ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);

        using var schedule = HubTelemetry.Traces.StartActivity("test.schedule", ActivityKind.Producer);
        Assert.NotNull(schedule);
        var payload = JsonDocument.Parse("{}").RootElement.Clone();
        var task = new HubLeasedTask(Guid.NewGuid(), "test", 0, "Activity", 1,
            Guid.NewGuid(), payload, 1, 1, DateTime.UtcNow.AddSeconds(30),
            DateTime.UtcNow, schedule.Id, schedule.TraceStateString);

        using (var first = HubTraceCorrelation.StartWorkerExecution("test.execute", task))
        {
            Assert.NotNull(first);
            Assert.Equal(schedule.TraceId, first.TraceId);
            Assert.Equal(schedule.SpanId, first.ParentSpanId);
            Assert.Equal("continued", first.GetTagItem("workflow.trace_segment"));
        }

        using (var retry = HubTraceCorrelation.StartWorkerExecution("test.retry",
            task with { Attempts = 2 }))
        {
            Assert.NotNull(retry);
            Assert.NotEqual(schedule.TraceId, retry.TraceId);
            Assert.Equal("linked_retry", retry.GetTagItem("workflow.trace_segment"));
            Assert.Contains(retry.Links, link => link.Context.TraceId == schedule.TraceId);
        }

        using (var old = HubTraceCorrelation.StartWorkerExecution("test.timer",
            task with { CreatedAt = DateTime.UtcNow.AddMinutes(-10) }))
        {
            Assert.NotNull(old);
            Assert.NotEqual(schedule.TraceId, old.TraceId);
            Assert.Equal("linked_wait", old.GetTagItem("workflow.trace_segment"));
            Assert.Contains(old.Links, link => link.Context.TraceId == schedule.TraceId);
        }
    }
}
