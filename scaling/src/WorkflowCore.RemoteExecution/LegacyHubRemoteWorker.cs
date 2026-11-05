using System.Net;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkflowCore.Hub;

namespace WorkflowCore.RemoteExecution.Legacy;

/// <summary>
/// Remote code-first activity handler. No PostgreSQL or Hub persistence types
/// are injected into handlers.
/// </summary>
public interface IHubActivityHandler
{
    string HubId { get; }
    string TaskType { get; }
    int Version { get; }
    Task<JsonElement> ExecuteAsync(JsonElement payload, CancellationToken ct);
}

/// <summary>
/// One SSE stream per worker process, REST batch claims, fenced completions and
/// heartbeats. This mode never creates a PostgreSQL connection.
/// </summary>
public sealed class HubRemoteWorker : BackgroundService, IDisposable
{
    private readonly HttpClient _http;
    private readonly ILogger<HubRemoteWorker> _logger;
    private readonly Dictionary<(string Hub, string Type, int Version), IHubActivityHandler> _handlers;
    private readonly SemaphoreSlim _claimGate = new(1, 1);
    private readonly int _concurrency;
    private readonly string _workerId = $"{Environment.MachineName}:{Guid.NewGuid():N}";
    private int _running;

    public HubRemoteWorker(IEnumerable<IHubActivityHandler> handlers,
        ILogger<HubRemoteWorker> logger)
    {
        _logger = logger;
        _handlers = handlers.ToDictionary(x => (x.HubId, x.TaskType, x.Version));
        var url = Environment.GetEnvironmentVariable("WFC_HUB_URL")
            ?? throw new InvalidOperationException("WFC_HUB_URL is required");
        var token = Environment.GetEnvironmentVariable("WFC_HUB_AUTH_TOKEN")
            ?? throw new InvalidOperationException("WFC_HUB_AUTH_TOKEN is required");
        if (!(Environment.GetEnvironmentVariable("WFC_HUB_ALLOW_INSECURE_HTTP") == "true" &&
              url.StartsWith("http://", StringComparison.OrdinalIgnoreCase)) &&
            !url.StartsWith("https://", StringComparison.OrdinalIgnoreCase) &&
            !url.StartsWith("http://127.0.0.1", StringComparison.OrdinalIgnoreCase) &&
            !url.StartsWith("http://localhost", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Remote Hub requires HTTPS outside localhost.");
        _http = new HttpClient { BaseAddress = new Uri(url.TrimEnd('/') + "/") };
        _http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", token);
        _concurrency = int.TryParse(Environment.GetEnvironmentVariable("WORKERS"), out var count)
            ? Math.Clamp(count, 1, 32) : 8;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        if (_handlers.Count == 0)
            throw new InvalidOperationException("At least one code-first Hub handler is required");
        var hubs = string.Join(",", _handlers.Keys.Select(x => x.Hub).Distinct());
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get,
                    "api/hub/workers/events?hub=" + Uri.EscapeDataString(hubs));
                using var response = await _http.SendAsync(request,
                    HttpCompletionOption.ResponseHeadersRead, ct);
                response.EnsureSuccessStatusCode();
                _logger.LogInformation("Hub SSE session connected: worker={Worker}", _workerId);
                await ClaimAvailableAsync(ct); // Recover durable work on reconnect.
                await using var stream = await response.Content.ReadAsStreamAsync(ct);
                using var reader = new StreamReader(stream);
                var heartbeats = 0;
                while (!ct.IsCancellationRequested)
                {
                    var line = await reader.ReadLineAsync(ct);
                    if (line is null) break;
                    if (line.StartsWith("event: tasks.available", StringComparison.Ordinal))
                        await ClaimAvailableAsync(ct);
                    else if (line.StartsWith(": keepalive", StringComparison.Ordinal) &&
                        ++heartbeats % 3 == 0)
                        await ClaimAvailableAsync(ct); // Missed notification reconciliation.
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Hub stream unavailable; reconnecting");
            }
            try { await Task.Delay(TimeSpan.FromSeconds(2 + Random.Shared.Next(3)), ct); }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task ClaimAvailableAsync(CancellationToken ct)
    {
        if (!await _claimGate.WaitAsync(0, ct)) return;
        try
        {
            var capacity = _concurrency - Volatile.Read(ref _running);
            if (capacity <= 0) return;
            var query = new HubClaimRequest(_workerId,
                _handlers.Keys.Select(x => x.Hub).Distinct().ToArray(),
                _handlers.Keys.Select(x => new HubCapability(x.Hub, x.Type, x.Version)).Distinct().ToArray(),
                Math.Min(capacity, 16));
            using var response = await _http.PostAsJsonAsync("api/hub/workers/tasks:claim",
                query, ct);
            response.EnsureSuccessStatusCode();
            var tasks = await response.Content.ReadFromJsonAsync<HubLeasedTask[]>(cancellationToken: ct)
                ?? Array.Empty<HubLeasedTask>();
            foreach (var task in tasks)
            {
                Interlocked.Increment(ref _running);
                _ = RunTaskAsync(task, ct);
            }
        }
        finally { _claimGate.Release(); }
    }

    private async Task RunTaskAsync(HubLeasedTask task, CancellationToken shutdown)
    {
        using var span = HubTelemetry.Traces.StartActivity("hub.worker.activity.execute");
        span?.SetTag("hub", task.HubId);
        span?.SetTag("activity.type", task.TaskType);
        span?.SetTag("workflow.id", task.WorkflowId?.ToString());
        var startedAt = Stopwatch.GetTimestamp();
        HubTelemetry.WorkerInflight.Add(1, HubTelemetry.HubTag(task.HubId),
            HubTelemetry.ActivityTag(task.TaskType));
        var completed = false;
        using var execution = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
        using var heartbeatStop = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
        var heartbeat = HeartbeatAsync(task, execution, heartbeatStop.Token);
        try
        {
            if (!_handlers.TryGetValue((task.HubId, task.TaskType, task.Version), out var handler))
                throw new InvalidOperationException("Code-first activity handler is not registered");
            var output = await handler.ExecuteAsync(task.Payload, execution.Token);
            using var response = await _http.PostAsJsonAsync(
                $"api/hub/tasks/{task.Id}/complete",
                new HubTaskResultRequest(_workerId, task.Epoch, output), execution.Token);
            if (response.StatusCode == HttpStatusCode.Conflict)
                throw new OperationCanceledException("Hub rejected stale workflow task completion");
            response.EnsureSuccessStatusCode();
            completed = true;
        }
        catch (OperationCanceledException) when (execution.IsCancellationRequested ||
            shutdown.IsCancellationRequested)
        {
            _logger.LogWarning("Hub task {Task} stopped after lease loss/shutdown", task.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Hub task {Task} execution failed", task.Id);
            try
            {
                using var response = await _http.PostAsJsonAsync(
                    $"api/hub/tasks/{task.Id}/fail",
                    new HubTaskFailureRequest(_workerId, task.Epoch, ex.Message), shutdown);
                if (!response.IsSuccessStatusCode)
                    _logger.LogWarning("Hub rejected task failure for {Task}: {Status}",
                        task.Id, response.StatusCode);
            }
            catch (Exception reportError) when (!shutdown.IsCancellationRequested)
            {
                _logger.LogWarning(reportError, "Hub task failure report lost; lease expiry will recover");
            }
        }
        finally
        {
            HubTelemetry.WorkerInflight.Add(-1, HubTelemetry.HubTag(task.HubId),
                HubTelemetry.ActivityTag(task.TaskType));
            HubTelemetry.WorkerExecutions.Add(1, HubTelemetry.HubTag(task.HubId),
                HubTelemetry.ActivityTag(task.TaskType),
                new KeyValuePair<string, object?>("outcome", completed ? "success" : "failure"));
            HubTelemetry.WorkerExecutionDuration.Record(
                Stopwatch.GetElapsedTime(startedAt).TotalSeconds,
                HubTelemetry.HubTag(task.HubId), HubTelemetry.ActivityTag(task.TaskType));
            span?.SetTag("task.completed", completed);
            await heartbeatStop.CancelAsync();
            try { await heartbeat; }
            catch (OperationCanceledException) { }
            Interlocked.Decrement(ref _running);
            if (!shutdown.IsCancellationRequested)
            {
                try { await ClaimAvailableAsync(shutdown); }
                catch (Exception ex) { _logger.LogWarning(ex, "Hub task follow-up claim failed"); }
            }
        }
    }

    private async Task HeartbeatAsync(HubLeasedTask task,
        CancellationTokenSource execution, CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(12));
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                using var response = await _http.PostAsJsonAsync(
                    $"api/hub/tasks/{task.Id}/heartbeat",
                    new HubTaskHeartbeatRequest(_workerId, task.Epoch), ct);
                if (!response.IsSuccessStatusCode)
                {
                    execution.Cancel();
                    _logger.LogWarning("Lost Hub lease on task {Task}", task.Id);
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            execution.Cancel();
            _logger.LogWarning(ex, "Hub heartbeat failed for task {Task}", task.Id);
        }
    }

    public override void Dispose()
    {
        _claimGate.Dispose();
        _http.Dispose();
        base.Dispose();
    }
}
