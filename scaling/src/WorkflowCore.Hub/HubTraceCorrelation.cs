using System.Diagnostics;

namespace WorkflowCore.Hub;

/// <summary>
/// Propagates causal context through the durable queue. A bounded first attempt
/// continues the scheduling trace; old tasks and lease retries start a new trace
/// linked to the scheduling context. PostgreSQL workflow history is authoritative.
/// </summary>
public static class HubTraceCorrelation
{
    // Keep short, causally-connected executions in one trace. Do not use a
    // long-lived root span for timers or retries. Configured per worker process.
    private static readonly TimeSpan MaxSegmentAge = ReadMaxAge();

    public static string? TraceParent =>
        Activity.Current is { IdFormat: ActivityIdFormat.W3C } a ? a.Id : null;
    public static string? TraceState => Activity.Current?.TraceStateString;
    public static string? TraceId =>
        Activity.Current is { IdFormat: ActivityIdFormat.W3C } a ? a.TraceId.ToString() : null;
    public static string? SpanId =>
        Activity.Current is { IdFormat: ActivityIdFormat.W3C } a ? a.SpanId.ToString() : null;

    public static Activity? StartWorkerExecution(string name, HubLeasedTask task)
    {
        var hasOrigin = ActivityContext.TryParse(task.TraceParent, task.TraceState, out var origin);
        var retry = task.Attempts > 1;
        var longWait = task.CreatedAt != default &&
            DateTime.UtcNow - task.CreatedAt > MaxSegmentAge;
        var split = retry || longWait;

        // Detached execution must not accidentally inherit an ambient polling
        // span. Explicitly clear it for a new linked segment or root.
        var ambient = Activity.Current;
        if (split || !hasOrigin) Activity.Current = null;
        Activity? span = hasOrigin && split
            ? HubTelemetry.Traces.StartActivity(name, ActivityKind.Consumer,
                default(ActivityContext), links: [new ActivityLink(origin)])
            : hasOrigin
                ? HubTelemetry.Traces.StartActivity(name, ActivityKind.Consumer, origin)
                : HubTelemetry.Traces.StartActivity(name, ActivityKind.Consumer);
        if (span is null && (split || !hasOrigin)) Activity.Current = ambient;
        span?.SetTag("workflow.trace_segment", !hasOrigin ? "new" :
            retry ? "linked_retry" : longWait ? "linked_wait" : "continued");
        return span;
    }

    private static TimeSpan ReadMaxAge()
    {
        var value = Environment.GetEnvironmentVariable("WFC_HUB_TRACE_SEGMENT_MAX_AGE_SECONDS");
        return int.TryParse(value, out var seconds) && seconds is >= 1 and <= 86400
            ? TimeSpan.FromSeconds(seconds) : TimeSpan.FromMinutes(2);
    }
}
