using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace WorkflowCore.Provisioner.Host;

// A separate Hub workflow so existing sequential runs keep their definition.
public sealed class ParallelOnboardingWorkflow : IWorkflow<ParallelOnboardingData>
{
    public string Id => "tenant-onboarding-parallel";
    public int Version => 1;

    public void Build(IWorkflowBuilder<ParallelOnboardingData> builder) => builder
        .StartWith<ValidateTenant>().Input(s => s.TenantId, d => d.TenantId)
            .Output(d => d.Validated, s => s.Validated)
        .Parallel()
            .Do(branch => branch.StartWith<CreateResourceGroup>().Input(s => s.TenantId, d => d.TenantId)
                .Then<DeployDatabase>().Input(s => s.TenantId, d => d.TenantId).Output(d => d.DatabaseReady, s => s.Completed))
            .Do(branch => branch.StartWith<RegisterApplication>().Input(s => s.TenantId, d => d.TenantId)
                .Then<ConfigureRoles>().Input(s => s.TenantId, d => d.TenantId).Output(d => d.RolesReady, s => s.Completed))
            .Do(branch => branch.StartWith<CreateBillingAccount>().Input(s => s.TenantId, d => d.TenantId)
                .Then<ConfigureQuota>().Input(s => s.TenantId, d => d.TenantId).Output(d => d.QuotaReady, s => s.Completed))
        .Join()
        .Then<ActivateParallelTenant>().Input(s => s.TenantId, d => d.TenantId)
            .Input(s => s.DatabaseReady, d => d.DatabaseReady).Input(s => s.RolesReady, d => d.RolesReady)
            .Input(s => s.QuotaReady, d => d.QuotaReady).Output(d => d.Activated, s => s.Activated)
        .Then<SendWelcome>().Input(s => s.TenantId, d => d.TenantId).Output(d => d.WelcomeSent, s => s.WelcomeSent);
}

public sealed class ParallelOnboardingData
{
    public string TenantId { get; set; } = "";
    public bool Validated { get; set; }
    public bool DatabaseReady { get; set; }
    public bool RolesReady { get; set; }
    public bool QuotaReady { get; set; }
    public bool Activated { get; set; }
    public bool WelcomeSent { get; set; }
}

public abstract class ParallelOnboardingActivity : StepBodyAsync
{
    public string TenantId { get; set; } = "";
    public bool Completed { get; set; }
    protected abstract int DelayMilliseconds { get; }
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        if (string.IsNullOrWhiteSpace(TenantId)) throw new ArgumentException("Missing tenantId");
        await Task.Delay(DelayMilliseconds, context.CancellationToken);
        Completed = true;
        return ExecutionResult.Next();
    }
}
public sealed class CreateResourceGroup : ParallelOnboardingActivity { protected override int DelayMilliseconds => 180; }
public sealed class DeployDatabase : ParallelOnboardingActivity { protected override int DelayMilliseconds => 320; }
public sealed class RegisterApplication : ParallelOnboardingActivity { protected override int DelayMilliseconds => 220; }
public sealed class ConfigureRoles : ParallelOnboardingActivity { protected override int DelayMilliseconds => 180; }
public sealed class CreateBillingAccount : ParallelOnboardingActivity { protected override int DelayMilliseconds => 260; }
public sealed class ConfigureQuota : ParallelOnboardingActivity { protected override int DelayMilliseconds => 140; }

public sealed class ActivateParallelTenant : StepBodyAsync
{
    public string TenantId { get; set; } = "";
    public bool DatabaseReady { get; set; }
    public bool RolesReady { get; set; }
    public bool QuotaReady { get; set; }
    public bool Activated { get; set; }
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        if (string.IsNullOrWhiteSpace(TenantId) || !DatabaseReady || !RolesReady || !QuotaReady)
            throw new InvalidOperationException("All onboarding branches must complete before activation.");
        await Task.Delay(15, context.CancellationToken);
        Activated = true;
        return ExecutionResult.Next();
    }
}
