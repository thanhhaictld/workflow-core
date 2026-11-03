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

    public HubRestClient()
    {
        var url=Environment.GetEnvironmentVariable("WFC_HUB_URL")
            ?? throw new InvalidOperationException("WFC_HUB_URL is required");
        var token=Environment.GetEnvironmentVariable("WFC_HUB_AUTH_TOKEN")
            ?? throw new InvalidOperationException("WFC_HUB_AUTH_TOKEN is required");
        if(token.Length<24)throw new InvalidOperationException("Hub token must be at least 24 characters");
        if(!(url.StartsWith("https://",StringComparison.OrdinalIgnoreCase) ||
            url.StartsWith("http://127.0.0.1",StringComparison.OrdinalIgnoreCase) ||
            url.StartsWith("http://localhost",StringComparison.OrdinalIgnoreCase) ||
            (Environment.GetEnvironmentVariable("WFC_HUB_ALLOW_INSECURE_HTTP")=="true" &&
            url.StartsWith("http://",StringComparison.OrdinalIgnoreCase))))
            throw new InvalidOperationException("Remote Hub requires HTTPS outside local development");
        Http=new HttpClient{BaseAddress=new Uri(url.TrimEnd('/')+"/"),
            Timeout=TimeSpan.FromSeconds(30)};
        Http.DefaultRequestHeaders.Authorization=new AuthenticationHeaderValue("Bearer",token);
    }

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
        response.EnsureSuccessStatusCode();
    }

    public void Dispose()=>Http.Dispose();
}
