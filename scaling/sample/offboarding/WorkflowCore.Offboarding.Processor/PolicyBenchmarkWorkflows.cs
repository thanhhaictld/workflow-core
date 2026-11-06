using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace WorkflowCore.Offboarding.Processor;

/// <summary>
/// Synthetic, side-effect-free routed-activity workload. All workflows have
/// identical step duration; only their declared admission policy differs.
/// </summary>
public sealed class PolicyBenchmarkData
{
    public string ResourceId { get; set; } = "";
    public bool Completed { get; set; }
}

public sealed class PolicyUnorderedWorkflow : IWorkflow<PolicyBenchmarkData>
{
    public string Id => "policy-unordered";
    public int Version => 1;
    public void Build(IWorkflowBuilder<PolicyBenchmarkData> builder) => builder
        .StartWith<PolicyUnorderedStep>()
        .Input(s => s.ResourceId, d => d.ResourceId)
        .Output(d => d.Completed, s => s.Completed);
}

public sealed class PolicyPartitionWorkflow : IWorkflow<PolicyBenchmarkData>
{
    public string Id => "policy-partition-fifo";
    public int Version => 1;
    public void Build(IWorkflowBuilder<PolicyBenchmarkData> builder) => builder
        .StartWith<PolicyPartitionStep>()
        .Input(s => s.ResourceId, d => d.ResourceId)
        .Output(d => d.Completed, s => s.Completed);
}

public sealed class PolicyKeyedWorkflow : IWorkflow<PolicyBenchmarkData>
{
    public string Id => "policy-keyed-fifo";
    public int Version => 1;
    public void Build(IWorkflowBuilder<PolicyBenchmarkData> builder) => builder
        .StartWith<PolicyKeyedStep>()
        .Input(s => s.ResourceId, d => d.ResourceId)
        .Output(d => d.Completed, s => s.Completed);
}

public abstract class PolicyBenchmarkStep : StepBodyAsync
{
    public string ResourceId { get; set; } = "";
    public bool Completed { get; set; }
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        if (string.IsNullOrWhiteSpace(ResourceId))
            throw new ArgumentException("ResourceId missing");
        // Models a short external operation without touching a real tenant.
        await Task.Delay(75, context.CancellationToken);
        Completed = true;
        return ExecutionResult.Next();
    }
}

public sealed class PolicyUnorderedStep : PolicyBenchmarkStep { }
public sealed class PolicyPartitionStep : PolicyBenchmarkStep { }
public sealed class PolicyKeyedStep : PolicyBenchmarkStep { }
