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
    private static long _pendingTimers;
    private static long _dueTimers;
    private static readonly ObservableGauge<long> TimerPending =
        Meter.CreateObservableGauge("workflow_hub_timer_pending",
            () => Volatile.Read(ref _pendingTimers), "{timer}");
    private static readonly ObservableGauge<long> TimerDue =
        Meter.CreateObservableGauge("workflow_hub_timer_due",
            () => Volatile.Read(ref _dueTimers), "{timer}");
    public static void PublishTimerSnapshot(HubRemoteTimerMetrics metrics)
    {
        Interlocked.Exchange(ref _pendingTimers, metrics.Pending);
        Interlocked.Exchange(ref _dueTimers, metrics.Due);
    }

    public static readonly Counter<long> TimerFired =
        Meter.CreateCounter<long>("workflow_hub_timer_fired", "{timer}");
    public static readonly Histogram<double> TimerFireLag =
        Meter.CreateHistogram<double>("workflow_hub_timer_fire_lag_seconds", "s");
    public static readonly Histogram<double> TimerBatchDuration =
        Meter.CreateHistogram<double>("workflow_hub_timer_batch_duration_seconds", "s");
    public static readonly Counter<long> TimerSchedulerErrors =
        Meter.CreateCounter<long>("workflow_hub_timer_scheduler_errors", "{error}");
    public static readonly Counter<long> TasksClaimed =
        Meter.CreateCounter<long>("workflow_hub_tasks_claimed", "{task}");
    public static readonly Counter<long> TasksCompleted =
        Meter.CreateCounter<long>("workflow_hub_tasks_completed", "{task}");
    public static readonly Counter<long> TasksFailed =
        Meter.CreateCounter<long>("workflow_hub_tasks_failed", "{task}");
    // PostgreSQL hub-scoped advisory lock wait; tracks implicit native-runtime
    // coordination even if no activity-level FIFO/resource policy is configured.
    public static readonly Histogram<double> NativeEventLaneLockWait =
        Meter.CreateHistogram<double>("workflow_hub_native_event_lane_lock_wait_seconds", "s");
    public static readonly Histogram<double> ClaimDuration =
        Meter.CreateHistogram<double>("workflow_hub_claim_duration_seconds", "s");
    // P0 diagnostics: low-cardinality mode and operation only. No workflow,
    // worker, tenant or unbounded hub labels on histograms.
    public static readonly Histogram<double> NativeCommitDuration =
        Meter.CreateHistogram<double>("workflow_hub_native_commit_duration_seconds", "s");
    // Bounded cardinality diagnostic for the native, event-free fast path.
    public static readonly Counter<long> NativeEventLaneBypass =
        Meter.CreateCounter<long>("workflow_hub_native_event_lane_bypass", "{commit}");
    public static readonly Histogram<double> NativeWorkflowRowLockDuration =
        Meter.CreateHistogram<double>("workflow_hub_native_workflow_row_lock_duration_seconds", "s");
    public static readonly Histogram<double> NativeHistoryWriteDuration =
        Meter.CreateHistogram<double>("workflow_hub_native_history_write_duration_seconds", "s");
    public static readonly Histogram<int> NativeHistoryEventsPerCommit =
        Meter.CreateHistogram<int>("workflow_hub_native_history_events_per_commit", "{event}");
    public static readonly Histogram<int> ClaimCandidateCount =
        Meter.CreateHistogram<int>("workflow_hub_claim_candidates_scanned", "{task}");
    public static readonly Histogram<int> ClaimReturnedCount =
        Meter.CreateHistogram<int>("workflow_hub_claim_tasks_returned", "{task}");
    public static readonly Histogram<double> StoreOperationDuration =
        Meter.CreateHistogram<double>("workflow_hub_store_duration_seconds", "s");
    public static readonly Counter<long> SseNotifications =
        Meter.CreateCounter<long>("workflow_hub_sse_notifications", "{notification}");
    // Low-cardinality transport health; no hub/tenant identifiers on these instruments.
    public static readonly Counter<long> PostgresHintsReceived =
        Meter.CreateCounter<long>("workflow_hub_pg_notify_received", "{notification}");
    public static readonly Counter<long> PostgresHintsForwarded =
        Meter.CreateCounter<long>("workflow_hub_pg_notify_forwarded", "{notification}");
    public static readonly Counter<long> PostgresListenerFailures =
        Meter.CreateCounter<long>("workflow_hub_pg_notify_failures", "{failure}");
    public static readonly UpDownCounter<long> PostgresListenersActive =
        Meter.CreateUpDownCounter<long>("workflow_hub_pg_notify_listeners", "{connection}");
    public static readonly UpDownCounter<long> SseConnections =
        Meter.CreateUpDownCounter<long>("workflow_hub_sse_connections", "{connection}");
    public static readonly UpDownCounter<long> WorkerInflight =
        Meter.CreateUpDownCounter<long>("workflow_hub_worker_inflight", "{task}");
    public static readonly Counter<long> WorkerExecutions =
        Meter.CreateCounter<long>("workflow_hub_worker_executions", "{task}");
    public static readonly Histogram<double> WorkerExecutionDuration =
        Meter.CreateHistogram<double>("workflow_hub_worker_execution_duration_seconds", "s");
    public static readonly Histogram<double> WorkerBodyDuration =
        Meter.CreateHistogram<double>("workflow_hub_worker_body_duration_seconds", "s");
    public static readonly Histogram<double> WorkerCompletionDuration =
        Meter.CreateHistogram<double>("workflow_hub_worker_completion_http_duration_seconds", "s");
    public static readonly Histogram<double> ClaimConnectionWait =
        Meter.CreateHistogram<double>("workflow_hub_claim_db_connection_acquire_seconds", "s");
    public static readonly Histogram<double> ClaimCandidateQuery =
        Meter.CreateHistogram<double>("workflow_hub_claim_candidate_query_seconds", "s");
    public static readonly Histogram<double> ClaimPermitCheck =
        Meter.CreateHistogram<double>("workflow_hub_claim_permit_check_seconds", "s");

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

    private static long _databaseConnections;
    private static long _activeDatabaseConnections;
    private static long _queueReady;
    private static long _queueLeased;
    private static long _queueDead;
    private static readonly ObservableGauge<long> DatabaseConnections =
        Meter.CreateObservableGauge("workflow_hub_database_connections",
            () => Volatile.Read(ref _databaseConnections), "{connection}");
    private static readonly ObservableGauge<long> ActiveDatabaseConnections =
        Meter.CreateObservableGauge("workflow_hub_database_active_connections",
            () => Volatile.Read(ref _activeDatabaseConnections), "{connection}");
    private static readonly ObservableGauge<long> QueueReady =
        Meter.CreateObservableGauge("workflow_hub_queue_ready",
            () => Volatile.Read(ref _queueReady), "{task}");
    private static readonly ObservableGauge<long> QueueLeased =
        Meter.CreateObservableGauge("workflow_hub_queue_leased",
            () => Volatile.Read(ref _queueLeased), "{task}");
    private static readonly ObservableGauge<long> QueueDead =
        Meter.CreateObservableGauge("workflow_hub_queue_dead",
            () => Volatile.Read(ref _queueDead), "{task}");

    public static void PublishDatabaseSnapshot(HubDatabaseDiagnostics stats)
    {
        Interlocked.Exchange(ref _databaseConnections, stats.DatabaseConnections);
        Interlocked.Exchange(ref _activeDatabaseConnections, stats.ActiveDbConnections);
        Interlocked.Exchange(ref _queueReady, stats.ReadyTasks);
        Interlocked.Exchange(ref _queueLeased, stats.LeasedTasks);
        Interlocked.Exchange(ref _queueDead, stats.DeadTasks);
    }

    public static KeyValuePair<string, object?> HubTag(string hub) => new("hub", hub);
    public static KeyValuePair<string, object?> ActivityTag(string type) =>
        new("activity_type", type);
}
