using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Client;
using WorkflowCore.Hub.Telemetry;
using WorkflowCore.Scaling.Worker;

var builder=Host.CreateApplicationBuilder(args);
builder.Services.AddWorkflowHubOpenTelemetry("workflow-provisioner");
builder.Services.AddSingleton<HubRestClient>();
builder.Services.AddSingleton<IHubActivityHandler,ValidateTenantHandler>();
builder.Services.AddSingleton<IHubActivityHandler,ProvisionTenantHandler>();
builder.Services.AddSingleton<IHubActivityHandler,SendWelcomeHandler>();
builder.Services.AddSingleton<IHubActivityHandler,RestoreDatabaseHandler>();
builder.Services.AddSingleton<IHubActivityHandler,CapturePaymentHandler>();
builder.Services.AddHostedService<ProvisionerRegistration>();
if(!args.Contains("--register-only",StringComparer.Ordinal))
    builder.Services.AddHostedService<HubRemoteWorker>();
await builder.Build().RunAsync();

/// <summary>
/// Activity policies are registered through REST rather than linked into Hub.
/// This host has no WFC_PG and can scale independently from workflow processors.
/// </summary>
public sealed class ProvisionerRegistration(HubRestClient client,
    IHostApplicationLifetime lifetime, ILogger<ProvisionerRegistration> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        HubActivityRegistration[] policies=
        [
            new("onboarding","validate-tenant",1,32,100,100),
            new("onboarding","provision-tenant",1,32,100,20,
                "tenant","tenantId",1,true),
            new("onboarding","send-welcome",1,32,100,100),
            new("backup","restore-database",1,16,16,16,
                "database","databaseId",1,true),
            new("billing","capture-payment",1,32,64,32,
                "account","accountId",2,false)
        ];
        foreach(var policy in policies)
        {
            await client.RegisterAsync("api/hub/activity-registrations",policy,ct);
            logger.LogInformation("Published activity policy {Hub}/{Activity}",
                policy.HubId,policy.ActivityType);
        }
        if(Environment.GetCommandLineArgs().Contains("--register-only",StringComparer.Ordinal))
            lifetime.StopApplication();
    }
    public Task StopAsync(CancellationToken ct)=>Task.CompletedTask;
}
