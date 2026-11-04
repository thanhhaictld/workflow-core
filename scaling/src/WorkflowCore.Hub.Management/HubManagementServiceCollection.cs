using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using WorkflowCore.Scaling.Api;
using WorkflowCore.Scaling.PostgreSQL;

namespace WorkflowCore.Hub.Management;

public static class HubManagementServiceCollection
{
    public static IServiceCollection AddHubManagement(this IServiceCollection services, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        services.AddSingleton(new WorkflowMonitorReader(connectionString));
        services.AddSingleton(sp => new DefinitionCatalog(connectionString, sp.GetRequiredService<ILogger<DefinitionCatalog>>()));
        return services;
    }
}

