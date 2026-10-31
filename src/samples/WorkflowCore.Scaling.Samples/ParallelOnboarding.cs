using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace WorkflowCore.Scaling.Samples;

/// <summary>
/// Three concurrent logical branches converging at Workflow Core's Sequence
/// container join. No cloud resources are created: each step only delays.
/// The Sequence container is both the fork and the join; Join() does not
/// create a separate runtime execution step.
/// </summary>
public sealed class ParallelTenantOnboardingWorkflow : IWorkflow<TenantOnboardingState>
{
    public string Id => "tenant-onboarding-parallel";
    public int Version => 1;

    public void Build(IWorkflowBuilder<TenantOnboardingState> builder)
    {
        builder
            .StartWith<ValidateTenantStep>()
            .Parallel()
                .Do(branch => branch
                    .StartWith<CreateResourceGroup>()
                        .Input(s => s.TenantId, d => d.TenantId)
                    .Then<DeployDatabase>()
                        .Input(s => s.TenantId, d => d.TenantId))
                .Do(branch => branch
                    .StartWith<RegisterApplication>()
                        .Input(s => s.TenantId, d => d.TenantId)
                    .Then<ConfigureRoles>()
                        .Input(s => s.TenantId, d => d.TenantId))
                .Do(branch => branch
                    .StartWith<CreateBillingAccount>()
                        .Input(s => s.TenantId, d => d.TenantId)
                    .Then<ConfigureQuota>()
                        .Input(s => s.TenantId, d => d.TenantId))
            .Join()
            .Then<ActivateTenantStep>()
            .Then<SendWelcomeStep>();
    }
}

/// <summary>
/// Step bodies are synthetic, idempotent and have no side effects. Individual
/// delays let the trace show distinguishable branch activity.
/// </summary>
public abstract class ParallelOnboardingStep : StepBodyAsync
{
    public string TenantId { get; set; } = "";

    protected virtual int DelayMilliseconds => 150;

    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        if (string.IsNullOrWhiteSpace(TenantId))
            throw new InvalidOperationException("TenantId is required for demo branch.");
        await Task.Delay(DelayMilliseconds, context.CancellationToken);
        return ExecutionResult.Next();
    }
}

public sealed class CreateResourceGroup : ParallelOnboardingStep
{
    protected override int DelayMilliseconds => 180;
}
public sealed class DeployDatabase : ParallelOnboardingStep
{
    protected override int DelayMilliseconds => 320;
}
public sealed class RegisterApplication : ParallelOnboardingStep
{
    protected override int DelayMilliseconds => 220;
}
public sealed class ConfigureRoles : ParallelOnboardingStep
{
    protected override int DelayMilliseconds => 180;
}
public sealed class CreateBillingAccount : ParallelOnboardingStep
{
    protected override int DelayMilliseconds => 260;
}
public sealed class ConfigureQuota : ParallelOnboardingStep
{
    protected override int DelayMilliseconds => 140;
}
