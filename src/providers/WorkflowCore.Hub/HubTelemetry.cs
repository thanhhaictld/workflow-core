using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace WorkflowCore.Hub;

/// <summary>
/// Low-cardinality application telemetry. Never use workflow IDs, tenants,
/// request keys or payloads as metric labels. Trace spans may carry IDs but
/// must not include input/output payloads or auth tokens.
/// </summary>
public static class HubTelemetry
{
    public const string MeterName = "WorkflowCore.Hub";
    public const string ActivitySourceName = "WorkflowCore.Hub";
    private static readonly Meter Meter = new(MeterName, "1.0.0");
    public static readonly ActivitySource Traces = new(ActivitySourceName, "1.0.0");

    public static readonly Counter<long> WorkflowsStarted =
        Meter.CreateCounter<long>("workflow_hub_workflows_started", "{workflow}");
    public static readonly Counter<long> WorkflowsCompleted =
        Meter.CreateCounter<long>("workflow_hub_workflows_completed", "{workflow}");
    public static readonly Counter<long> WorkflowsFailed =
        Meter.CreateCounter<long>("workflow_hub_workflows_failed", "{workflow}");
    public static readonly Histogram<double> WorkflowDuration =
        Meter.CreateHistogram<double>("workflow_hub_workflow_duration_seconds", "s");
    public static readonly Counter<long> TasksClaimed =
        Meter.CreateCounter<long>("workflow_hub_tasks_claimed", "{task}");
    public static readonly Counter<long> TasksCompleted =
        Meter.CreateCounter<long>("workflow_hub_tasks_completed", "{task}");
    public static readonly Counter<long> TasksFailed =
        Meter.CreateCounter<long>("workflow_hub_tasks_failed", "{task}");
    public static readonly Histogram<double> ClaimDuration =
        Meter.CreateHistogram<double>("workflow_hub_claim_duration_seconds", "s");
    public static readonly Histogram<double> StoreOperationDuration =
        Meter.CreateHistogram<double>("workflow_hub_store_duration_seconds", "s");
    public static readonly Counter<long> SseNotifications =
        Meter.CreateCounter<long>("workflow_hub_sse_notifications", "{notification}");
    public static readonly UpDownCounter<long> SseConnections =
        Meter.CreateUpDownCounter<long>("workflow_hub_sse_connections", "{connection}");
    public static readonly UpDownCounter<long> WorkerInflight =
        Meter.CreateUpDownCounter<long>("workflow_hub_worker_inflight", "{task}");
    public static readonly Counter<long> WorkerExecutions =
        Meter.CreateCounter<long>("workflow_hub_worker_executions", "{task}");
    public static readonly Histogram<double> WorkerExecutionDuration =
        Meter.CreateHistogram<double>("workflow_hub_worker_execution_duration_seconds", "s");

    // These observable instruments are process-local, not estimates of
    // total machine / container memory. Resource service.instance.id disambiguates.
    private static readonly Meter ProcessMeter = new("WorkflowCore.Hub.Process", "1.0.0");
    private static readonly ObservableGauge<long> ProcessWorkingSet =
        ProcessMeter.CreateObservableGauge("workflow_hub_process_working_set_bytes",
            () => Environment.WorkingSet, "By", "Resident process memory");
    private static readonly ObservableGauge<long> ManagedHeap =
        ProcessMeter.CreateObservableGauge("workflow_hub_managed_heap_bytes",
            () => GC.GetTotalMemory(false), "By", "Approximate live managed heap");
    private static readonly ObservableGauge<long> ThreadPoolPending =
        ProcessMeter.CreateObservableGauge("workflow_hub_threadpool_pending",
            () => ThreadPool.PendingWorkItemCount, "{work_item}");

    public static KeyValuePair<string, object?> HubTag(string hub) => new("hub", hub);
    public static KeyValuePair<string, object?> ActivityTag(string type) =>
        new("activity_type", type);
}
