using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WorkflowCore.Interface;
using WorkflowCore.Scaling.PostgreSQL;
using WorkflowCore.Scaling.Samples;
using WorkflowCore.Scaling.Worker;

// Remote mode uses REST+SSE and has no workflow-state database credentials.
if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("WFC_HUB_URL")))
{
    var remote = Host.CreateApplicationBuilder(args);
    remote.Services.AddSingleton<IHubActivityHandler, ProvisionTenantHandler>();
    remote.Services.AddSingleton<IHubActivityHandler, RestoreDatabaseHandler>();
    remote.Services.AddSingleton<IHubActivityHandler, CapturePaymentHandler>();
    remote.Services.AddHostedService<HubRemoteWorker>();
    await remote.Build().RunAsync();
    return;
}

var connectionString = Environment.GetEnvironmentVariable("WFC_PG")
    ?? throw new InvalidOperationException("WFC_PG connection string is required.");

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddDistributedWorkflowCore(connectionString, options =>
{
    options.Concurrency = int.TryParse(Environment.GetEnvironmentVariable("WORKERS"), out var workers)
        ? Math.Clamp(workers, 1, 64) : 4;
    options.IdleDelay = TimeSpan.FromMilliseconds(100);
});
builder.Services.AddTenantOnboardingDemo();
builder.Services.AddBenchmarkProvisionDemo();

// A worker registers the exact same workflow definitions as the API. Workflow
// instances can move between replicas because their state is in PostgreSQL.
using var host = builder.Build();
host.Services.GetRequiredService<IWorkflowHost>().RegisterTenantOnboardingDemo();
host.Services.GetRequiredService<IWorkflowHost>().RegisterBenchmarkProvisionDemo();
await host.RunAsync();
