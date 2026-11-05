using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace WorkflowCore.Hub.Client;

/// <summary>
/// Authenticated typed REST transport used by independently deployed application
/// hosts. No database dependencies or connection strings are required.
/// </summary>
public sealed class HubRestClient : IDisposable
{
    public HttpClient Http { get; }

    public HubRestClient() : this(
        Environment.GetEnvironmentVariable("WFC_HUB_URL") ?? throw new InvalidOperationException("WFC_HUB_URL is required"),
        Environment.GetEnvironmentVariable("WFC_HUB_AUTH_TOKEN") ?? throw new InvalidOperationException("WFC_HUB_AUTH_TOKEN is required"),
        Environment.GetEnvironmentVariable("WFC_HUB_ALLOW_INSECURE_HTTP")=="true") { }

    public HubRestClient(string url, string token, bool allowInsecureHttp = false)
    {
        if(token.Length<24)throw new InvalidOperationException("Hub token must be at least 24 characters");
        if(!Uri.TryCreate(url,UriKind.Absolute,out var endpoint) ||
            !(endpoint.Scheme==Uri.UriSchemeHttps || endpoint.Scheme==Uri.UriSchemeHttp &&
                (endpoint.IsLoopback || allowInsecureHttp)))
            throw new InvalidOperationException("Remote Hub requires HTTPS outside local development");
        Http=new HttpClient{BaseAddress=new Uri(url.TrimEnd('/')+"/"),
            Timeout=TimeSpan.FromSeconds(30)};
        Http.DefaultRequestHeaders.Authorization=new AuthenticationHeaderValue("Bearer",token);
    }

    public Task<JsonElement> StartWorkflowAsync(WorkflowCore.Hub.HubWorkflowStartRequest request, CancellationToken ct = default) =>
        PostAsync<JsonElement>("api/hub/workflows", request, ct);

    public Task PublishEventAsync(string hub, WorkflowCore.Hub.HubNativeEvent request, CancellationToken ct = default) =>
        RegisterAsync($"api/hub/hubs/{Uri.EscapeDataString(hub)}/events", request, ct);

    public Task ChangeStatusAsync(Guid id, WorkflowCore.Hub.HubNativeStatusChange request, CancellationToken ct = default) =>
        RegisterAsync($"api/hub/workflows/{id}/status", request, ct);

    public async Task<T> GetAsync<T>(string path,CancellationToken ct)
    {
        using var response=await Http.GetAsync(path,ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<T>(cancellationToken:ct)
            ?? throw new InvalidOperationException("Empty Hub response");
    }

    public async Task<T> PostAsync<T>(string path,object payload,CancellationToken ct)
    {
        using var response=await Http.PostAsJsonAsync(path,payload,ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<T>(cancellationToken:ct)
            ?? throw new InvalidOperationException("Empty Hub response");
    }

    public async Task RegisterAsync(string path,object payload,CancellationToken ct)
    {
        using var response=await Http.PostAsJsonAsync(path,payload,ct);
        if(!response.IsSuccessStatusCode)
        {
            var error=await response.Content.ReadAsStringAsync(ct);
            throw new HttpRequestException(
                $"Hub registration {path} failed with {(int)response.StatusCode}: {error[..Math.Min(error.Length,1500)]}",
                null,response.StatusCode);
        }
    }

    public void Dispose()=>Http.Dispose();
}
