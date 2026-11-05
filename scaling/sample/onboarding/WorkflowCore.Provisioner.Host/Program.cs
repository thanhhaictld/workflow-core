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
    options.WorkflowConcurrency=int.TryParse(Environment.GetEnvironmentVariable("WORKFLOW_WORKERS"),out var workflows)?workflows:8;
    options.ActivityConcurrency=int.TryParse(Environment.GetEnvironmentVariable("ACTIVITY_WORKERS") ?? Environment.GetEnvironmentVariable("WORKERS"),out var activities)?activities:16;
})
    .AddWorkflow<OnboardingWorkflow,OnboardingData>("onboarding")
    .AddWorkflow<OnboardingWorkflowV4,OnboardingData>("onboarding")
    .AddWorkflow<OnboardingTimerWorkflow,OnboardingData>("onboarding")
    .AddWorkflow<ParallelOnboardingWorkflow,ParallelOnboardingData>("onboarding")
    .AddActivity<ValidateTenant>("onboarding","validate-tenant",3,o=>o.Input(s=>s.TenantId).Output(s=>s.Validated))
    .AddActivity<ProvisionTenant>("onboarding","provision-tenant",3,o=>
    {
        o.Concurrency=20;o.ResourceName="tenant";o.ResourceField="tenantId";o.ResourceCapacity=1;o.StrictFifo=true;
        o.Input(s=>s.TenantId).Output(s=>s.Provisioned).Output(s=>s.WorkspaceId);
    })
    .AddActivity<ConfigureTenantAccess>("onboarding","configure-tenant-access",4,o=>o.Input(s=>s.TenantId).Output(s=>s.AccessConfigured))
    .AddActivity<SendWelcome>("onboarding","send-welcome",3,o=>o.Input(s=>s.TenantId).Output(s=>s.WelcomeSent))
    .AddActivity<CreateResourceGroup>("onboarding","create-resource-group",1,o=>o.Input(s=>s.TenantId).Output(s=>s.Completed))
    .AddActivity<DeployDatabase>("onboarding","deploy-database",1,o=>o.Input(s=>s.TenantId).Output(s=>s.Completed))
    .AddActivity<RegisterApplication>("onboarding","register-application",1,o=>o.Input(s=>s.TenantId).Output(s=>s.Completed))
    .AddActivity<ConfigureRoles>("onboarding","configure-roles",1,o=>o.Input(s=>s.TenantId).Output(s=>s.Completed))
    .AddActivity<CreateBillingAccount>("onboarding","create-billing-account",1,o=>o.Input(s=>s.TenantId).Output(s=>s.Completed))
    .AddActivity<ConfigureQuota>("onboarding","configure-quota",1,o=>o.Input(s=>s.TenantId).Output(s=>s.Completed))
    .AddActivity<ActivateParallelTenant>("onboarding","activate-parallel-tenant",1,o=>o.Input(s=>s.TenantId).Input(s=>s.DatabaseReady).Input(s=>s.RolesReady).Input(s=>s.QuotaReady).Output(s=>s.Activated))
    .AddActivity<RestoreDatabase>("backup","restore-database",3,o=>
    {
        o.Partitions=16;o.HubConcurrency=16;o.Concurrency=16;o.ResourceName="database";o.ResourceField="databaseId";
        o.ResourceCapacity=1;o.StrictFifo=true;o.Input(s=>s.DatabaseId).Output(s=>s.Restored);
    })
    .AddActivity<CapturePayment>("billing","capture-payment",3,o=>
    {
        o.HubConcurrency=64;o.Concurrency=32;o.ResourceName="account";o.ResourceField="accountId";
        o.ResourceCapacity=2;o.Input(s=>s.AccountId).Output(s=>s.Captured);
    });
await builder.Build().RunAsync();
