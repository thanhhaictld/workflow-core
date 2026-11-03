using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WorkflowCore.Hub.Client;
using WorkflowCore.Hub.Telemetry;
using WorkflowCore.RemoteExecution;
using WorkflowCore.Processor.Host;

var builder=Host.CreateApplicationBuilder(args);
builder.Services.AddWorkflowHubOpenTelemetry("workflow-processor");
builder.Services.AddSingleton<HubRestClient>();
builder.Services.AddSingleton(new RemoteWorkflowRegistry()
    .Register("onboarding","tenant-onboarding",1,32,b=>b
        .Activity("validate-tenant")
        .Activity("provision-tenant")
        .Activity("send-welcome"))
    .Register("onboarding","tenant-onboarding-timer",1,32,b=>b
        .Activity("validate-tenant")
        .Delay("approval-timeout",TimeSpan.FromSeconds(2))
        .Activity("provision-tenant")
        .Activity("send-welcome")));
builder.Services.AddHostedService<DefinitionPublisher>();
if(!args.Contains("--register-only",StringComparer.Ordinal))
    builder.Services.AddHostedService<HubWorkflowProcessor>();
await builder.Build().RunAsync();
