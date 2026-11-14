using WorkflowCore.Interface;
using WorkflowCore.Models;
using System.Text.Json;

namespace WorkflowCore.Provisioner.Host;

// Benchmark-only observation at the simulated side-effect site. The stable
// native dispatch key, not a lease epoch, identifies a repeated invocation.
internal static class SampleSideEffectObservation
{
    private static readonly object Gate = new();
    private static readonly string? Path = Environment.GetEnvironmentVariable("WFC_HUB_SIDE_EFFECT_OBSERVATION_LOG");
    public static void Record(IStepExecutionContext context, string activity)
    {
        if (string.IsNullOrWhiteSpace(Path)) return;
        context.ExecutionPointer.ExtensionAttributes.TryGetValue("hubDispatchKey", out var key);
        var line = JsonSerializer.Serialize(new { kind = "side_effect", activity,
            workflowId = context.Workflow.Id, dispatchKey = key?.ToString(), observedAt = DateTime.UtcNow });
        lock (Gate) File.AppendAllText(Path, line + Environment.NewLine);
    }
}

public sealed class OnboardingData
{
    public string TenantId { get; set; }="";
    public bool Validated { get; set; }
    public string? WorkspaceId { get; set; }
    public bool WelcomeSent { get; set; }
    public bool AccessConfigured { get; set; }
}

public sealed class OnboardingWorkflow : IWorkflow<OnboardingData>
{
    public string Id=>"tenant-onboarding";
    public int Version=>3;
    public void Build(IWorkflowBuilder<OnboardingData> builder)=>builder
        .StartWith<ValidateTenant>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.Validated,s=>s.Validated)
        .Then<ProvisionTenant>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.WorkspaceId,s=>s.WorkspaceId)
        .OnError(WorkflowErrorHandling.Retry,TimeSpan.FromSeconds(5))
        .Then<SendWelcome>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.WelcomeSent,s=>s.WelcomeSent);
}
// Retain v3 for in-flight runs; v4 adds access configuration after provisioning.
public sealed class OnboardingWorkflowV4 : IWorkflow<OnboardingData>
{
    public string Id=>"tenant-onboarding";
    public int Version=>4;
    public void Build(IWorkflowBuilder<OnboardingData> builder)=>builder
        .StartWith<ValidateTenant>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.Validated,s=>s.Validated)
        .Then<ProvisionTenant>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.WorkspaceId,s=>s.WorkspaceId)
        .OnError(WorkflowErrorHandling.Retry,TimeSpan.FromSeconds(5))
        .Then<ConfigureTenantAccess>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.AccessConfigured,s=>s.AccessConfigured)
        .Then<SendWelcome>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.WelcomeSent,s=>s.WelcomeSent);
}
public sealed class OnboardingTimerWorkflow : IWorkflow<OnboardingData>
{
    public string Id=>"tenant-onboarding-timer";
    public int Version=>3;
    public void Build(IWorkflowBuilder<OnboardingData> builder)=>builder
        .StartWith<ValidateTenant>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.Validated,s=>s.Validated)
        .Delay(d=>TimeSpan.FromSeconds(2))
        .Then<ProvisionTenant>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.WorkspaceId,s=>s.WorkspaceId)
        .OnError(WorkflowErrorHandling.Retry,TimeSpan.FromSeconds(5))
        .Then<SendWelcome>().Input(s=>s.TenantId,d=>d.TenantId).Output(d=>d.WelcomeSent,s=>s.WelcomeSent);
}
public sealed class ValidateTenant : StepBodyAsync
{
    public string TenantId { get; set; }="";
    public bool Validated { get; set; }
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        if(string.IsNullOrWhiteSpace(TenantId))throw new ArgumentException("Missing tenantId");
        await Task.Delay(15,context.CancellationToken);SampleSideEffectObservation.Record(context,"validate-tenant");Validated=true;return ExecutionResult.Next();
    }
}
public sealed class ProvisionTenant : StepBodyAsync
{
    public string TenantId { get; set; }="";
    public bool Provisioned { get; set; }
    public string? WorkspaceId { get; set; }
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    {
        var delay = int.TryParse(Environment.GetEnvironmentVariable("WFC_SAMPLE_PROVISION_DELAY_MS"), out var configured)
            ? Math.Clamp(configured, 0, 60000) : 400;
        await Task.Delay(delay,context.CancellationToken);SampleSideEffectObservation.Record(context,"provision-tenant");Provisioned=true;WorkspaceId="workspace-"+TenantId;
        return ExecutionResult.Next();
    }
}
public sealed class ConfigureTenantAccess : StepBodyAsync
{
    public string TenantId { get; set; }="";
    public bool AccessConfigured { get; set; }
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    { await Task.Delay(15,context.CancellationToken);AccessConfigured=true;return ExecutionResult.Next(); }
}
public sealed class SendWelcome : StepBodyAsync
{
    public string TenantId { get; set; }="";
    public bool WelcomeSent { get; set; }
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    { await Task.Delay(15,context.CancellationToken);SampleSideEffectObservation.Record(context,"send-welcome");WelcomeSent=true;return ExecutionResult.Next(); }
}
public sealed class RestoreDatabase : StepBodyAsync
{
    public string DatabaseId { get; set; }="";
    public bool Restored { get; set; }
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    { await Task.Delay(500,context.CancellationToken);Restored=true;return ExecutionResult.Next(); }
}
public sealed class CapturePayment : StepBodyAsync
{
    public string AccountId { get; set; }="";
    public bool Captured { get; set; }
    public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
    { await Task.Delay(300,context.CancellationToken);Captured=true;return ExecutionResult.Next(); }
}

