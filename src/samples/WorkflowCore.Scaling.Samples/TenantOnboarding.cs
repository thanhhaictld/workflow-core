using Microsoft.Extensions.DependencyInjection;
using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace WorkflowCore.Scaling.Samples;

/// <summary>
/// Demonstration-only workflow: all side effects are simulated in workflow state.
/// Never reuse these fake provisioning steps to create real tenants.
/// </summary>
public sealed class TenantOnboardingState
{
    public string TenantId { get; set; } = "";
    public string OrganizationName { get; set; } = "";
    public bool RequiresApproval { get; set; }
    public bool SimulateTransientFailure { get; set; }
    public int ProvisionAttempts { get; set; }
    public bool IdentityCreated { get; set; }
    public bool WorkspaceProvisioned { get; set; }
    public bool AccessConfigured { get; set; }
    public bool Activated { get; set; }
    public bool WelcomeSent { get; set; }
}

/// <summary>Happy path: several visible steps, completion without an external signal.</summary>
public sealed class AutomaticTenantOnboardingWorkflow : IWorkflow<TenantOnboardingState>
{
    public string Id => "tenant-onboarding-auto";
    public int Version => 1;
    public void Build(IWorkflowBuilder<TenantOnboardingState> builder)
    {
        builder.StartWith<ValidateTenantStep>()
            .Then<CreateIdentityStep>()
            .Then<ProvisionWorkspaceStep>()
                .OnError(WorkflowErrorHandling.Retry, TimeSpan.FromSeconds(1))
            .Then<ConfigureAccessStep>()
            .Then<ActivateTenantStep>()
            .Then<SendWelcomeStep>();
    }
}

/// <summary>
/// Approval path: pauses for a tenant-specific 'OnboardingApproved' signal.
/// Used to demonstrate waiting pointers, signal delivery, and resumption.
/// </summary>
public sealed class ApprovalTenantOnboardingWorkflow : IWorkflow<TenantOnboardingState>
{
    public string Id => "tenant-onboarding-approval";
    public int Version => 1;
    public void Build(IWorkflowBuilder<TenantOnboardingState> builder)
    {
        builder.StartWith<ValidateTenantStep>()
            .Then<CreateIdentityStep>()
            .Then<ProvisionWorkspaceStep>()
                .OnError(WorkflowErrorHandling.Retry, TimeSpan.FromSeconds(1))
            .Then<ConfigureAccessStep>()
            .WaitFor("OnboardingApproved",
                (data, context) => data.TenantId,
                data => DateTime.UtcNow.AddDays(-1))
            .Then<ActivateTenantStep>()
            .Then<SendWelcomeStep>();
    }
}

public static class TenantOnboardingRegistration
{
    public static IServiceCollection AddTenantOnboardingDemo(this IServiceCollection services)
    {
        services.AddTransient<ValidateTenantStep>();
        services.AddTransient<CreateIdentityStep>();
        services.AddTransient<ProvisionWorkspaceStep>();
        services.AddTransient<ConfigureAccessStep>();
        services.AddTransient<ActivateTenantStep>();
        services.AddTransient<SendWelcomeStep>();
        services.AddTransient<CreateResourceGroup>();
        services.AddTransient<DeployDatabase>();
        services.AddTransient<RegisterApplication>();
        services.AddTransient<ConfigureRoles>();
        services.AddTransient<CreateBillingAccount>();
        services.AddTransient<ConfigureQuota>();
        return services;
    }

    public static void RegisterTenantOnboardingDemo(this IWorkflowHost host)
    {
        host.RegisterWorkflow<AutomaticTenantOnboardingWorkflow, TenantOnboardingState>();
        host.RegisterWorkflow<ApprovalTenantOnboardingWorkflow, TenantOnboardingState>();
    }
}

public abstract class OnboardingStep : StepBodyAsync
{
    protected static TenantOnboardingState State(IStepExecutionContext context) =>
        (TenantOnboardingState)context.Workflow.Data;

    // A small artificial delay makes the waterfall visible. No external calls.
    protected static Task SimulateWork(IStepExecutionContext context) =>
        Task.Delay(TimeSpan.FromMilliseconds(120), context.CancellationToken);
}

public sealed class ValidateTenantStep : OnboardingStep
{
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        var state = State(context);
        if (string.IsNullOrWhiteSpace(state.TenantId) ||
            string.IsNullOrWhiteSpace(state.OrganizationName))
            throw new InvalidOperationException("Demo onboarding requires tenant and organization.");
        await SimulateWork(context);
        return ExecutionResult.Next();
    }
}

public sealed class CreateIdentityStep : OnboardingStep
{
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        var state = State(context);
        await SimulateWork(context);
        state.IdentityCreated = true;
        return ExecutionResult.Next();
    }
}

public sealed class ProvisionWorkspaceStep : OnboardingStep
{
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        var state = State(context);
        await SimulateWork(context);
        // Persisted state drives the one-time failure; a retry yields another
        // distinct StepAttemptStarted/StepBodyThrew history pair.
        state.ProvisionAttempts++;
        if (state.SimulateTransientFailure && state.ProvisionAttempts == 1)
            throw new InvalidOperationException("Intentional demo transient provisioning failure");
        state.WorkspaceProvisioned = true;
        return ExecutionResult.Next();
    }
}

public sealed class ConfigureAccessStep : OnboardingStep
{
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        State(context).AccessConfigured = true;
        await SimulateWork(context);
        return ExecutionResult.Next();
    }
}

public sealed class ActivateTenantStep : OnboardingStep
{
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        State(context).Activated = true;
        await SimulateWork(context);
        return ExecutionResult.Next();
    }
}

public sealed class SendWelcomeStep : OnboardingStep
{
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        State(context).WelcomeSent = true;
        await SimulateWork(context);
        return ExecutionResult.Next();
    }
}
