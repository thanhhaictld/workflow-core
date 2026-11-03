using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Scheduling;

/// <summary>
/// One deployable Hub Host today; the same module can later be loaded by
/// a headless Scheduler Host without changing any REST/processor contract.
/// </summary>
public sealed class HubSchedulingOptions
{
    public bool Enabled { get; set; } = true;
    public int PollIntervalMs { get; set; } = 500;
    public int BatchSize { get; set; } = 100;
    public int MaxConcurrentBatches { get; set; } = 2;
}

public static class HubSchedulingExtensions
{
    public static IServiceCollection AddHubScheduling(this IServiceCollection services,
        IConfiguration configuration)
    {
        var options = new HubSchedulingOptions();
        configuration.Bind(options);
        if (options.PollIntervalMs is < 100 or > 60000 ||
            options.BatchSize is < 1 or > 1000 ||
            options.MaxConcurrentBatches is < 1 or > 16)
            throw new ArgumentException("Invalid WorkflowHub:Scheduler options");
        services.AddSingleton(options);
        services.AddSingleton<IHubTimerStore,HubTimerStore>();
        if (options.Enabled)
            services.AddHostedService<HubTimerSchedulerService>();
        return services;
    }
}

public sealed class HubTimerSchedulerService(
    IHubTimerStore store, HubSchedulingOptions options,
    ILogger<HubTimerSchedulerService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // The timer is a scan trigger, NOT a workflow deadline store.
        // Deadlines and claimed state always remain durable in PostgreSQL.
        using var timer=new PeriodicTimer(TimeSpan.FromMilliseconds(options.PollIntervalMs));
        using var gate=new SemaphoreSlim(options.MaxConcurrentBatches);
        var pending=new HashSet<Task>();
        while(!ct.IsCancellationRequested)
        {
            try
            {
                if (!await timer.WaitForNextTickAsync(ct))break;
                if (!await gate.WaitAsync(0,ct))continue;
                var work=RunBatchAsync(ct);
                pending.Add(work);
                pending.RemoveWhere(x=>x.IsCompleted);
            }
            catch(OperationCanceledException) when(ct.IsCancellationRequested){break;}
        }
        try{await Task.WhenAll(pending);}catch(OperationCanceledException){}

        async Task RunBatchAsync(CancellationToken token)
        {
            var start=Stopwatch.GetTimestamp();
            try
            {
                var fired=await store.FireDueAsync(options.BatchSize,token);
                if(fired>0)logger.LogDebug("Hub scheduler fired {Fired} timers",fired);
            }
            catch(OperationCanceledException) when(token.IsCancellationRequested){}
            catch(Exception ex)
            {
                HubTelemetry.TimerSchedulerErrors.Add(1);
                logger.LogError(ex,"Hub timer scheduler scan failed");
            }
            finally
            {
                HubTelemetry.TimerBatchDuration.Record(
                    Stopwatch.GetElapsedTime(start).TotalSeconds);
                gate.Release();
            }
        }
    }
}
