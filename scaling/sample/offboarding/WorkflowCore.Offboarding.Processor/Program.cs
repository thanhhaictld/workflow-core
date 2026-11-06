using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WorkflowCore.Hub.Telemetry;
using WorkflowCore.Offboarding.Processor;
using WorkflowCore.RemoteExecution;

// Dedicated DB-free processor for measuring Hub/native workflow throughput.
// No AddActivity(...) registrations: all steps execute in-process, without
// per-activity transport or Hub resource-capacity/FIFO constraints.
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddWorkflowHubOpenTelemetry("workflow-offboarding-processor");

var registerOnly = args.Contains("--register-only", StringComparer.Ordinal);
builder.Services.AddHubWorkflowRuntime(options =>
{
    options.RegisterOnly = registerOnly;
    options.WorkflowConcurrency = int.TryParse(
        Environment.GetEnvironmentVariable("WORKFLOW_WORKERS"), out var workers)
        ? workers : 16;
    // There are no registered activities, so the activity worker lane exits.
    options.ActivityConcurrency = 1;
})
.AddWorkflow<OffboardingWorkflow, OffboardingData>("offboarding", partitions: 64);

await builder.Build().RunAsync();
