using System.Net;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkflowCore.Hub;

using WorkflowCore.RemoteExecution.Legacy;

namespace WorkflowCore.Scaling.Worker;

public sealed class ValidateTenantHandler : IHubActivityHandler
{
    public string HubId => "onboarding";
    public string TaskType => "validate-tenant";
    public int Version => 1;
    public async Task<JsonElement> ExecuteAsync(JsonElement payload, CancellationToken ct)
    {
        var tenantId = payload.GetProperty("tenantId").GetString();
        if (string.IsNullOrWhiteSpace(tenantId))
            throw new ArgumentException("Missing tenantId");
        await Task.Delay(15, ct);
        return JsonSerializer.SerializeToElement(new { tenantId, validated = true });
    }
}

public sealed class SendWelcomeHandler : IHubActivityHandler
{
    public string HubId => "onboarding";
    public string TaskType => "send-welcome";
    public int Version => 1;
    public async Task<JsonElement> ExecuteAsync(JsonElement payload, CancellationToken ct)
    {
        await Task.Delay(15, ct);
        return JsonSerializer.SerializeToElement(new
        {
            tenantId = payload.GetProperty("tenantId").GetString(),
            welcomeSent = true
        });
    }
}

public sealed class ProvisionTenantHandler : IHubActivityHandler
{
    public string HubId => "onboarding";
    public string TaskType => "provision-tenant";
    public int Version => 1;
    public async Task<JsonElement> ExecuteAsync(JsonElement payload, CancellationToken ct)
    {
        await Task.Delay(400, ct);
        return JsonSerializer.SerializeToElement(new
        {
            tenantId = payload.GetProperty("tenantId").GetString(),
            provisioned = true
        });
    }
}
public sealed class RestoreDatabaseHandler : IHubActivityHandler
{
    public string HubId => "backup";
    public string TaskType => "restore-database";
    public int Version => 1;
    public async Task<JsonElement> ExecuteAsync(JsonElement payload, CancellationToken ct)
    {
        await Task.Delay(500, ct);
        return JsonSerializer.SerializeToElement(new
        {
            databaseId = payload.GetProperty("databaseId").GetString(),
            restored = true
        });
    }
}
public sealed class CapturePaymentHandler : IHubActivityHandler
{
    public string HubId => "billing";
    public string TaskType => "capture-payment";
    public int Version => 1;
    public async Task<JsonElement> ExecuteAsync(JsonElement payload, CancellationToken ct)
    {
        await Task.Delay(300, ct);
        return JsonSerializer.SerializeToElement(new
        {
            accountId = payload.GetProperty("accountId").GetString(),
            captured = true
        });
    }
}

