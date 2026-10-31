using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WorkflowCore.Interface;
using WorkflowCore.Scaling.PostgreSQL;
using WorkflowCore.Scaling.Samples;

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

// A worker registers the exact same workflow definitions as the API. Workflow
// instances can move between replicas because their state is in PostgreSQL.
using var host = builder.Build();
host.Services.GetRequiredService<IWorkflowHost>().RegisterTenantOnboardingDemo();
await host.RunAsync();
