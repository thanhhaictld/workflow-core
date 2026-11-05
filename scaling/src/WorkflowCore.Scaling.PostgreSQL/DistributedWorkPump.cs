using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WorkflowCore.Scaling.PostgreSQL;

public sealed class DistributedRuntimeOptions
{
    public bool StartWorker { get; set; } = true;
    public int Concurrency { get; set; } = 8;
    public int MaxAttempts { get; set; } = 12;
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(60);
    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromMilliseconds(200);
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(2);
}

/// <summary>
/// Runs independently on every worker replica. Each slot has one leased
/// work item at a time; a heartbeat renews the queue lease and workflow gate.
/// </summary>
public sealed class DistributedWorkPump : BackgroundService
{
    private static readonly Meter Meter = new("WorkflowCore.Distributed", "1.0.0");
    private static readonly ActivitySource Tracing = new("WorkflowCore.Distributed", "1.0.0");
    private static readonly Counter<long> Claimed = Meter.CreateCounter<long>("workflowcore.work.claimed");
    private static readonly Counter<long> Succeeded = Meter.CreateCounter<long>("workflowcore.work.succeeded");
    private static readonly Counter<long> Failed = Meter.CreateCounter<long>("workflowcore.work.failed");
    private static readonly Counter<long> Deferred = Meter.CreateCounter<long>("workflowcore.work.deferred");
    private static readonly Histogram<double> ExecutionDuration = Meter.CreateHistogram<double>(
        "workflowcore.work.duration", unit: "s");

    private readonly DistributedWorkflowEngine _engine;
    private readonly FencedWorkQueue _queue;
    private readonly DistributedRuntimeOptions _options;
    private readonly ILogger<DistributedWorkPump> _logger;
    private readonly string _ownerPrefix = $"{Environment.MachineName}:{Guid.NewGuid():N}";

    public DistributedWorkPump(DistributedWorkflowEngine engine, FencedWorkQueue queue,
        DistributedRuntimeOptions options, ILogger<DistributedWorkPump> logger)
    {
        if (options.Concurrency < 1 || options.MaxAttempts < 1 ||
            options.LeaseDuration < TimeSpan.FromSeconds(10))
            throw new ArgumentOutOfRangeException(nameof(options));
        _engine = engine;
        _queue = queue;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var slots = Enumerable.Range(0, _options.Concurrency)
            .Select(index => RunSlotAsync(index, stoppingToken)).ToArray();
        await Task.WhenAll(slots);
    }

    private async Task RunSlotAsync(int slot, CancellationToken ct)
    {
        var owner = $"{_ownerPrefix}:{slot}";
        long iteration = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var first = (++iteration % 4 == 0) ? "event" : "workflow";
                var second = first == "workflow" ? "event" : "workflow";
                var items = await _queue.ClaimAsync(first, owner, 1,
                    _options.LeaseDuration, _options.MaxAttempts, ct);
                if (items.Count == 0)
                    items = await _queue.ClaimAsync(second, owner, 1,
                        _options.LeaseDuration, _options.MaxAttempts, ct);
                if (items.Count == 0)
                {
                    if (slot == 0 && iteration % 100 == 0)
                        await _queue.DeadLetterExhaustedAsync(_options.MaxAttempts, ct);
                    await Task.Delay(_options.IdleDelay, ct);
                    continue;
                }

                Claimed.Add(1, new KeyValuePair<string, object?>("queue", items[0].Queue));
                await ProcessAsync(items[0], owner, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception error)
            {
                _logger.LogError(error, "Distributed queue slot {Slot} failed", slot);
                await Task.Delay(_options.RetryDelay, ct);
            }
        }
    }

    private async Task ProcessAsync(LeasedWork work, string owner, CancellationToken shutdown)
    {
        using var activity = Tracing.StartActivity("distributed-work.execute");
        activity?.SetTag("work.id", work.Id.ToString());
        activity?.SetTag("workflow.id", work.ItemId.ToString());
        activity?.SetTag("queue", work.Queue);
        var stopwatch = Stopwatch.StartNew();
        WorkflowFence? fence = null;
        // A queue claim is NOT a workflow ownership claim. They are independent:
        // multiple pending event/step dispatches may target the same workflow.
        if (work.Queue == "workflow")
        {
            fence = await _queue.AcquireWorkflowAsync(work.ItemId, owner,
                _options.LeaseDuration, shutdown);
            if (fence is null)
            {
                await _queue.DeferAsync(work, owner,
                    TimeSpan.FromMilliseconds(250 + Random.Shared.Next(250)), shutdown);
                return;
            }
        }

        using var execution = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
        using var heartbeatStop = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
        var heartbeat = HeartbeatAsync(work, fence, owner, execution, heartbeatStop.Token);
        try
        {
            if (work.Queue == "workflow")
                await _engine.ExecuteWorkflowAsync(work, fence!, owner, execution.Token);
            else if (work.Queue == "event")
                await _engine.ExecuteEventAsync(work, owner, execution.Token);
            else
                throw new InvalidOperationException($"Unknown work type: {work.Queue}");
            Succeeded.Add(1, new KeyValuePair<string, object?>("queue", work.Queue));
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            // Leave the work leased; another replica reclaims it when expired.
        }
        catch (DeferredWorkException)
        {
            Deferred.Add(1, new KeyValuePair<string, object?>("queue", work.Queue));
            // Contention and event ordering must not burn the failure budget.
            await _queue.DeferAsync(work, owner,
                TimeSpan.FromMilliseconds(200 + Random.Shared.Next(300)), shutdown);
        }
        catch (Exception error)
        {
            Failed.Add(1, new KeyValuePair<string, object?>("queue", work.Queue));
            activity?.SetStatus(ActivityStatusCode.Error, error.Message);
            _logger.LogError(error, "Work {Id} attempt {Attempt} failed", work.Id, work.Attempts);
            // Retry is intentionally conditional on the original lease, so a
            // stale worker cannot reschedule work owned by a newer generation.
            var factor = Math.Min(64, Math.Pow(2, Math.Max(0, work.Attempts - 1)));
            var delay = TimeSpan.FromMilliseconds(_options.RetryDelay.TotalMilliseconds *
                factor * (0.75 + Random.Shared.NextDouble() / 2));
            try
            {
                await _queue.RetryAsync(work, owner, delay, _options.MaxAttempts,
                    error.ToString()[..Math.Min(4000, error.ToString().Length)], shutdown);
            }
            catch (Exception retryError) when (!shutdown.IsCancellationRequested)
            {
                _logger.LogError(retryError, "Retry update failed; lease expiry will recover {Id}", work.Id);
            }
        }
        finally
        {
            stopwatch.Stop();
            ExecutionDuration.Record(stopwatch.Elapsed.TotalSeconds,
                new KeyValuePair<string, object?>("queue", work.Queue));
            await heartbeatStop.CancelAsync();
            try { await heartbeat; }
            catch (OperationCanceledException) { }
            if (fence is not null)
            {
                try { await _queue.ReleaseGateAsync(fence, CancellationToken.None); }
                catch (Exception error) { _logger.LogWarning(error, "Gate release failed for {Id}", work.ItemId); }
            }
        }
    }

    private async Task HeartbeatAsync(LeasedWork work, WorkflowFence? fence, string owner,
        CancellationTokenSource execution, CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromTicks(_options.LeaseDuration.Ticks / 3));
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                if (!await _queue.RenewAsync(work, fence, owner, _options.LeaseDuration, ct))
                {
                    execution.Cancel();
                    _logger.LogWarning("Work {Id} lost its lease; stopping execution", work.Id);
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception error)
        {
            _logger.LogError(error, "Heartbeat failed for {Id}", work.Id);
            execution.Cancel();
        }
    }
}
