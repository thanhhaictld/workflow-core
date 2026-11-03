using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Samples database/queue state once per Hub instance, not per worker or
/// subscribed SSE client. Metrics are snapshots and may be stale for 5 seconds.
/// </summary>
public sealed class HubMetricsSampler(
    HubTaskStore store, ILogger<HubMetricsSampler> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var snapshot = await store.DiagnosticsAsync(ct);
                HubTelemetry.PublishDatabaseSnapshot(snapshot);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Hub queue/database telemetry sampling failed");
            }

            try { if (!await timer.WaitForNextTickAsync(ct)) break; }
            catch (OperationCanceledException) { break; }
        }
    }
}
