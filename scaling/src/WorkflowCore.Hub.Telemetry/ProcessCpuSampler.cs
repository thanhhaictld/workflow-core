using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Telemetry;

/// <summary>
/// Samples process CPU utilization every five seconds. The current gauge value
/// is exported with service.name and service.instance.id resource attributes.
/// </summary>
internal sealed class ProcessCpuSampler : BackgroundService
{
    private static readonly TimeSpan SampleInterval = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var process = Process.GetCurrentProcess();
        process.Refresh();
        var previousCpu = process.TotalProcessorTime;
        var previousTimestamp = Stopwatch.GetTimestamp();
        var processorCount = Math.Max(1, Environment.ProcessorCount);

        using var timer = new PeriodicTimer(SampleInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            process.Refresh();
            var currentCpu = process.TotalProcessorTime;
            var currentTimestamp = Stopwatch.GetTimestamp();
            var wallSeconds = Stopwatch.GetElapsedTime(previousTimestamp, currentTimestamp).TotalSeconds;
            var cpuSeconds = (currentCpu - previousCpu).TotalSeconds;

            if (wallSeconds > 0 && cpuSeconds >= 0)
            {
                var cpuPercent = cpuSeconds / wallSeconds / processorCount * 100d;
                HubTelemetry.PublishProcessCpuPercent(cpuPercent);
            }

            previousCpu = currentCpu;
            previousTimestamp = currentTimestamp;
        }
    }
}
