using Microsoft.Extensions.DependencyInjection;
using WorkflowCore.Models;

namespace WorkflowCore.Scaling.PostgreSQL;

public static class DistributedWorkflowServiceCollection
{
    /// <summary>
    /// Register ONLY the new PostgreSQL execution runtime, never the legacy
    /// RunnablePoller / WorkflowConsumer / EventConsumer. Apply scaling/sql
    /// migrations before starting this service.
    /// </summary>
    public static IServiceCollection AddDistributedWorkflowCore(
        this IServiceCollection services, string connectionString,
        Action<DistributedRuntimeOptions>? configure = null)
    {
        var settings = new DistributedRuntimeOptions();
        configure?.Invoke(settings);
        services.AddWorkflow(options =>
        {
            options.EnableWorkflows = false;
            options.EnableEvents = false;
            options.EnablePolling = false;
            options.EnableIndexes = false;
            options.EnableLifeCycleEventsPublisher = false;
            options.UsePostgreSQL(connectionString, canCreateDB: false, canMigrateDB: false);
        });

        services.AddSingleton(settings);
        services.AddSingleton(new FencedWorkQueue(connectionString));
        services.AddSingleton(sp => new DistributedWorkflowEngine(
            connectionString,
            sp.GetRequiredService<FencedWorkQueue>(),
            sp.GetRequiredService<WorkflowCore.Interface.IWorkflowRegistry>(),
            sp.GetRequiredService<WorkflowCore.Interface.IExecutionPointerFactory>(),
            sp.GetRequiredService<IServiceScopeFactory>()));
        services.AddHostedService<DistributedWorkPump>();
        return services;
    }
}
