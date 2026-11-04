using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WorkflowCore.Hub.Telemetry;
using WorkflowCore.Provisioner.Host;
using WorkflowCore.RemoteExecution;

var builder=Host.CreateApplicationBuilder(args);
builder.Services.AddWorkflowHubOpenTelemetry("workflow-provisioner");
var registerOnly=args.Contains("--register-only",StringComparer.Ordinal);
builder.Services.AddHubWorkflowRuntime(options=>
{
    options.RegisterOnly=registerOnly;
    options.SequentialVersionOneCompatibility=true;
    options.WorkflowConcurrency=int.TryParse(Environment.GetEnvironmentVariable("WORKFLOW_WORKERS"),out var workflows)?workflows:8;
    options.ActivityConcurrency=int.TryParse(Environment.GetEnvironmentVariable("ACTIVITY_WORKERS") ?? Environment.GetEnvironmentVariable("WORKERS"),out var activities)?activities:16;
})
    .AddWorkflow<OnboardingWorkflow,OnboardingData>("onboarding")
    .AddWorkflow<OnboardingTimerWorkflow,OnboardingData>("onboarding")
    .AddActivity<ValidateTenant>("onboarding","validate-tenant",2,o=>o.Input(s=>s.TenantId).Output(s=>s.Validated)
        .SequentialOutput(s=>s.TenantId).SequentialOutput(s=>s.Validated))
    .AddActivity<ProvisionTenant>("onboarding","provision-tenant",2,o=>
    {
        o.Concurrency=20;o.ResourceName="tenant";o.ResourceField="tenantId";o.ResourceCapacity=1;o.StrictFifo=true;
        o.Input(s=>s.TenantId).Output(s=>s.Provisioned).Output(s=>s.WorkspaceId);
        o.SequentialOutput(s=>s.TenantId).SequentialOutput(s=>s.Provisioned);
    })
    .AddActivity<SendWelcome>("onboarding","send-welcome",2,o=>o.Input(s=>s.TenantId).Output(s=>s.WelcomeSent)
        .SequentialOutput(s=>s.TenantId).SequentialOutput(s=>s.WelcomeSent))
    .AddActivity<RestoreDatabase>("backup","restore-database",2,o=>
    {
        o.Partitions=16;o.HubConcurrency=16;o.Concurrency=16;o.ResourceName="database";o.ResourceField="databaseId";
        o.ResourceCapacity=1;o.StrictFifo=true;o.Input(s=>s.DatabaseId).Output(s=>s.Restored);
        o.SequentialOutput(s=>s.DatabaseId).SequentialOutput(s=>s.Restored);
    })
    .AddActivity<CapturePayment>("billing","capture-payment",2,o=>
    {
        o.HubConcurrency=64;o.Concurrency=32;o.ResourceName="account";o.ResourceField="accountId";
        o.ResourceCapacity=2;o.Input(s=>s.AccountId).Output(s=>s.Captured);
        o.SequentialOutput(s=>s.AccountId).SequentialOutput(s=>s.Captured);
    });
// Keep immutable v1 definitions and their sequential processor for existing runs.
builder.Services.AddSingleton(new RemoteWorkflowRegistry()
    .Register("onboarding","tenant-onboarding",1,32,b=>b.Activity("validate-tenant").Activity("provision-tenant").Activity("send-welcome"))
    .Register("onboarding","tenant-onboarding-timer",1,32,b=>b.Activity("validate-tenant").Delay("approval-timeout",TimeSpan.FromSeconds(2))
        .Activity("provision-tenant").Activity("send-welcome")));
await builder.Build().RunAsync();
