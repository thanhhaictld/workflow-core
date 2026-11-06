using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace WorkflowCore.Offboarding.Processor;

/// <summary>
/// Synthetic, side-effect-free throughput baseline; not real tenant deletion.
/// Every instance has a unique tenant ID, and each of the three steps runs
/// inside the processor without an activity registration, lock or FIFO policy.
/// </summary>
public sealed class OffboardingData
{
    public string TenantId { get; set; } = "";
    public bool Validated { get; set; }
    public bool AccessRevoked { get; set; }
    public bool ResourcesRemoved { get; set; }
}

public sealed class OffboardingWorkflow : IWorkflow<OffboardingData>
{
    public string Id => "tenant-offboarding-fast";
    public int Version => 1;

    public void Build(IWorkflowBuilder<OffboardingData> builder) => builder
        .StartWith<ValidateOffboarding>().Input(s => s.TenantId, d => d.TenantId)
            .Output(d => d.Validated, s => s.Validated)
        .Then<RevokeOffboardingAccess>().Input(s => s.TenantId, d => d.TenantId)
            .Output(d => d.AccessRevoked, s => s.AccessRevoked)
        .Then<RemoveOffboardingResources>().Input(s => s.TenantId, d => d.TenantId)
            .Output(d => d.ResourcesRemoved, s => s.ResourcesRemoved);
}

public sealed class ValidateOffboarding : StepBodyAsync
{
    public string TenantId { get; set; } = "";
    public bool Validated { get; set; }

    public override Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        Validated = !string.IsNullOrWhiteSpace(TenantId);
        if (!Validated) throw new ArgumentException("TenantId is required");
        return Task.FromResult(ExecutionResult.Next());
    }
}

public sealed class RevokeOffboardingAccess : StepBodyAsync
{
    public string TenantId { get; set; } = "";
    public bool AccessRevoked { get; set; }

    public override Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        // Deliberately no external API call, timer, artificial delay or shared key.
        AccessRevoked = !string.IsNullOrWhiteSpace(TenantId);
        return Task.FromResult(ExecutionResult.Next());
    }
}

public sealed class RemoveOffboardingResources : StepBodyAsync
{
    public string TenantId { get; set; } = "";
    public bool ResourcesRemoved { get; set; }

    public override Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        ResourcesRemoved = !string.IsNullOrWhiteSpace(TenantId);
        return Task.FromResult(ExecutionResult.Next());
    }
}
