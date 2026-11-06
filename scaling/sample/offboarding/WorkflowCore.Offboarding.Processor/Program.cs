using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WorkflowCore.Hub.Telemetry;
using WorkflowCore.Offboarding.Processor;
using WorkflowCore.RemoteExecution;

// Dedicated DB-free processor for measuring Hub/native workflow throughput.
// Offboarding remains native-only. Optional policy benchmarks register routed
// activities without affecting the original throughput workload.
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddWorkflowHubOpenTelemetry("workflow-offboarding-processor");

var registerOnly = args.Contains("--register-only", StringComparer.Ordinal);
var registrations = builder.Services.AddHubWorkflowRuntime(options =>
{
    options.RegisterOnly = registerOnly;
    options.WorkflowConcurrency = int.TryParse(
        Environment.GetEnvironmentVariable("WORKFLOW_WORKERS"), out var workers)
        ? workers : 16;
    options.ActivityConcurrency = Environment.GetEnvironmentVariable("POLICY_BENCHMARK_ENABLED") == "true"
        ? int.TryParse(Environment.GetEnvironmentVariable("ACTIVITY_WORKERS"), out var n) ? n : 8
        : 1;
});
registrations.AddWorkflow<OffboardingWorkflow, OffboardingData>("offboarding", partitions: 64);
if (Environment.GetEnvironmentVariable("POLICY_BENCHMARK_ENABLED") == "true")
{
    // Partition is placement, not ordering. Only opted-in activities get permits.
    registrations
        .AddWorkflow<PolicyUnorderedWorkflow, PolicyBenchmarkData>("policy-benchmark", 8)
        .AddWorkflow<PolicyPartitionWorkflow, PolicyBenchmarkData>("policy-benchmark", 8)
        .AddWorkflow<PolicyKeyedWorkflow, PolicyBenchmarkData>("policy-benchmark", 8)
        .AddActivity<PolicyUnorderedStep>("policy-benchmark", "policy-unordered-step", 1, o =>
        {
            o.Partitions = 8;
            o.Ordering = WorkflowCore.Hub.HubActivityOrdering.Unordered;
            o.Input(s => s.ResourceId).Output(s => s.Completed);
        })
        .AddActivity<PolicyPartitionStep>("policy-benchmark", "policy-partition-step", 1, o =>
        {
            o.Partitions = 8;
            o.Ordering = WorkflowCore.Hub.HubActivityOrdering.PartitionSerial;
            o.Input(s => s.ResourceId).Output(s => s.Completed);
        })
        .AddActivity<PolicyKeyedStep>("policy-benchmark", "policy-keyed-step", 1, o =>
        {
            o.Partitions = 8;
            o.Ordering = WorkflowCore.Hub.HubActivityOrdering.KeyedFifo;
            o.ResourceName = "resource";
            o.ResourceField = "resourceId";
            o.Input(s => s.ResourceId).Output(s => s.Completed);
        });
}

await builder.Build().RunAsync();
