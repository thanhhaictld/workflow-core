using Microsoft.Extensions.DependencyInjection;
using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace WorkflowCore.Scaling.Samples;

/// <summary>Existing one-step smoke workflow, shared across the API and worker.</summary>
public sealed class BenchmarkProvisionState
{
    public string TenantId { get; set; } = "";
}

public sealed class BenchmarkProvisionWorkflow : IWorkflow<BenchmarkProvisionState>
{
    public string Id => "benchmark";
    public int Version => 1;
    public void Build(IWorkflowBuilder<BenchmarkProvisionState> builder) =>
        builder.StartWith<NoopProvisionStep>();
}

public sealed class NoopProvisionStep : StepBody
{
    public override ExecutionResult Run(IStepExecutionContext context) =>
        ExecutionResult.Next();
}

public static class BenchmarkProvisionRegistration
{
    public static IServiceCollection AddBenchmarkProvisionDemo(this IServiceCollection services)
    {
        return services.AddTransient<NoopProvisionStep>();
    }

    public static void RegisterBenchmarkProvisionDemo(this IWorkflowHost host)
    {
        host.RegisterWorkflow<BenchmarkProvisionWorkflow, BenchmarkProvisionState>();
    }
}
