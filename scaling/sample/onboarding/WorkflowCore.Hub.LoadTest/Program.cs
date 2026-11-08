using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;

var baseUrl = Environment.GetEnvironmentVariable("WFC_HUB_URL")
    ?? "http://127.0.0.1:5100";
var token = Environment.GetEnvironmentVariable("WFC_HUB_AUTH_TOKEN")
    ?? throw new InvalidOperationException("WFC_HUB_AUTH_TOKEN is required");
var runId = Environment.GetEnvironmentVariable("WFC_LOAD_RUN_ID")
    ?? $"hub-load-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid():N}"[..38];
var reportFile = Environment.GetEnvironmentVariable("WFC_LOAD_REPORT")
    ?? "/tmp/workflow-hub-load.json";
var phase = Environment.GetEnvironmentVariable("WFC_LOAD_PHASE") ?? "submit";
var definitionVersion = ReadInt("WFC_LOAD_VERSION", 3, 1, 10000);
var count = ReadInt("WFC_LOAD_COUNT", 1000, 1, 100000);
var submitters = ReadInt("WFC_LOAD_SUBMISSION_CONCURRENCY", 200, 1, 2000);
var timeoutSeconds = ReadInt("WFC_LOAD_TIMEOUT_SECONDS", 300, 15, 1800);
using var client = new HttpClient(new SocketsHttpHandler
{
    MaxConnectionsPerServer = Math.Max(submitters, 64),
    PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2)
})
{
    BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/"),
    Timeout = TimeSpan.FromMinutes(5)
};
client.DefaultRequestHeaders.Authorization =
    new AuthenticationHeaderValue("Bearer", token);
client.DefaultRequestHeaders.Accept.Add(
    new MediaTypeWithQualityHeaderValue("application/json"));

static int ReadInt(string name, int defaultValue, int min, int max)
{
    var value = Environment.GetEnvironmentVariable(name);
    return int.TryParse(value, out var parsed) && parsed >= min && parsed <= max
        ? parsed : defaultValue;
}
static double Percentile(IEnumerable<double> values, double p)
{
    var ordered = values.OrderBy(x => x).ToArray();
    if (ordered.Length == 0) return 0;
    var index = Math.Clamp((int)Math.Ceiling(ordered.Length * p) - 1, 0,
        ordered.Length - 1);
    return Math.Round(ordered[index], 2);
}
static long Field(JsonElement json, string name) => json.GetProperty(name).GetInt64();
static double? OptionalDouble(JsonElement json, string name) =>
    json.TryGetProperty(name, out var item) && item.ValueKind == JsonValueKind.Number
        ? item.GetDouble() : null;

async Task<JsonElement> GetJson(string path, CancellationToken ct)
{
    using var response = await client.GetAsync(path, ct);
    if (!response.IsSuccessStatusCode)
        throw new InvalidOperationException(
            $"Hub GET {path} returned {(int)response.StatusCode}: " +
            await response.Content.ReadAsStringAsync(ct));
    return (await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct)).Clone();
}
async Task SaveReport(object value)
{
    var json = JsonSerializer.Serialize(value, new JsonSerializerOptions
    {
        WriteIndented = true, PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    });
    var directory = Path.GetDirectoryName(Path.GetFullPath(reportFile));
    if (!string.IsNullOrWhiteSpace(directory)) Directory.CreateDirectory(directory);
    await File.WriteAllTextAsync(reportFile, json + Environment.NewLine);
    Console.WriteLine(json);
}
var summaryPath = $"api/hub/runs/{Uri.EscapeDataString(runId)}/summary";

if (phase.Equals("submit", StringComparison.OrdinalIgnoreCase))
{
    // The execution workers intentionally aren't started until ALL workflow
    // instances have been durably accepted. This guarantees 1,000 concurrently
    // pending workflow instances, instead of 1,000 unrelated activity tasks.
    var durations = new ConcurrentBag<double>();
    var errors = new ConcurrentBag<string>();
    var ids = new ConcurrentBag<Guid>();
    using var gate = new SemaphoreSlim(submitters);
    using var cancellation = new CancellationTokenSource(TimeSpan.FromMinutes(5));
    var stopwatch = Stopwatch.StartNew();

    await Task.WhenAll(Enumerable.Range(0, count).Select(async i =>
    {
        await gate.WaitAsync(cancellation.Token);
        try
        {
            var start = Stopwatch.GetTimestamp();
            var input = new
            {
                hubId = "onboarding",
                definitionId = "tenant-onboarding",
                version = definitionVersion,
                runId,
                idempotencyKey = $"{runId}:{i}",
                input = new
                {
                    tenantId = $"{runId}-tenant-{i:D5}",
                    organizationName = $"Load test tenant {i:D5}"
                }
            };
            using var response = await client.PostAsJsonAsync(
                "api/hub/workflows", input, cancellation.Token);
            durations.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            if (!response.IsSuccessStatusCode)
            {
                errors.Add($"workflow={i} HTTP={(int)response.StatusCode} " +
                    await response.Content.ReadAsStringAsync(cancellation.Token));
                return;
            }
            using var body = JsonDocument.Parse(
                await response.Content.ReadAsStringAsync(cancellation.Token));
            ids.Add(body.RootElement.GetProperty("workflowId").GetGuid());
        }
        catch (Exception ex) { errors.Add($"workflow={i}: {ex.GetType().Name}: {ex.Message}"); }
        finally { gate.Release(); }
    }));
    stopwatch.Stop();

    var summary = await GetJson(summaryPath, CancellationToken.None);
    var pending = Field(summary, "ready") + Field(summary, "running");
    var valid = ids.Count == count && Field(summary, "total") == count &&
                pending == count && Field(summary, "completed") == 0;
    var report = new
    {
        runId,
        phase = "submit",
        requestedWorkflows = count,
        successfulStarts = ids.Count,
        failedStarts = errors.Count,
        submissionConcurrency = submitters,
        submissionDurationSeconds = Math.Round(stopwatch.Elapsed.TotalSeconds, 3),
        submissionThroughputPerSecond = Math.Round(ids.Count /
            Math.Max(stopwatch.Elapsed.TotalSeconds, .001), 2),
        submitLatencyP50Ms = Percentile(durations, .5),
        submitLatencyP95Ms = Percentile(durations, .95),
        submitLatencyP99Ms = Percentile(durations, .99),
        readyBeforeWorkers = pending,
        peakSimultaneousPending = pending,
        valid,
        errors = errors.Take(10).ToArray(),
        sampleWorkflowIds = ids.Take(10).Select(x => x.ToString()).ToArray(),
        generatedAtUtc = DateTime.UtcNow
    };
    await SaveReport(report);
    if (!valid)
        throw new InvalidOperationException("Not all 1,000 workflows were pending before starting workers");
}
else if (phase.Equals("verify", StringComparison.OrdinalIgnoreCase))
{
    if (!File.Exists(reportFile))
        throw new FileNotFoundException("Submission report is missing", reportFile);
    using var previous = JsonDocument.Parse(await File.ReadAllTextAsync(reportFile));
    var submitted = previous.RootElement.GetProperty("successfulStarts").GetInt32();
    if (submitted != count || previous.RootElement.GetProperty("runId").GetString() != runId)
        throw new InvalidOperationException("Submission report does not match requested load run");

    long peakDb = 0, peakActiveDb = 0, peakReady = 0, peakLeased = 0;
    var errors = new List<string>();
    var samples = 0;
    var watch = Stopwatch.StartNew();
    using var cancellation = new CancellationTokenSource(
        TimeSpan.FromSeconds(timeoutSeconds));
    JsonElement summary = default;
    try
    {
        while (!cancellation.IsCancellationRequested)
        {
            summary = await GetJson(summaryPath, cancellation.Token);
            var metrics = await GetJson("api/hub/diagnostics", cancellation.Token);
            samples++;
            peakDb = Math.Max(peakDb, Field(metrics, "databaseConnections"));
            peakActiveDb = Math.Max(peakActiveDb, Field(metrics, "activeDbConnections"));
            peakReady = Math.Max(peakReady, Field(metrics, "readyTasks"));
            peakLeased = Math.Max(peakLeased, Field(metrics, "leasedTasks"));
            var finished = Field(summary, "completed") + Field(summary, "failed");
            if (finished >= count && Field(summary, "total") == count) break;
            await Task.Delay(500, cancellation.Token);
        }
    }
    catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
    {
        errors.Add($"Timed out after {timeoutSeconds} seconds");
    }
    watch.Stop();
    summary = await GetJson(summaryPath, CancellationToken.None);
    var completed = Field(summary, "completed");
    var failed = Field(summary, "failed");

    // Verify real three-step workflow results and ordered, durable history.
    var sampled = 0;
    foreach (var id in previous.RootElement.GetProperty("sampleWorkflowIds")
                 .EnumerateArray().Select(x => x.GetString()).Where(x => x is not null))
    {
        var status = await GetJson($"api/hub/workflows/{id}", CancellationToken.None);
        var history = await GetJson($"api/hub/workflows/{id}/history", CancellationToken.None);
        var steps = history.EnumerateArray()
            .Where(x => x.GetProperty("eventType").GetString() == "StepCompleted")
            .Select(x => x.GetProperty("stepIndex").GetInt32()).ToArray();
        var correct = status.GetProperty("state").GetString() == "Complete" && steps.SequenceEqual(new[] { 0, 1, 2 });
        if (status.GetProperty("version").GetInt32()>=2)
        {
            var runtime=await GetJson($"api/hub/workflows/{id}/native-runtime",CancellationToken.None);
            var data=runtime.GetProperty("snapshot").GetProperty("Data").GetProperty("value");
            correct=correct && data.GetProperty("Validated").GetBoolean() && data.GetProperty("WelcomeSent").GetBoolean() &&
                !string.IsNullOrEmpty(data.GetProperty("WorkspaceId").GetString());
        }
        else
        {
            var results=status.GetProperty("results");
            correct=correct && results.GetProperty("0").GetProperty("validated").GetBoolean() &&
                results.GetProperty("1").GetProperty("provisioned").GetBoolean() && results.GetProperty("2").GetProperty("welcomeSent").GetBoolean();
        }
        if (!correct)
            errors.Add("Unexpected workflow order/result for " + id);
        sampled++;
    }

    var valid = completed == count && failed == 0 && errors.Count == 0;
    await SaveReport(new
    {
        runId,
        phase = "complete",
        requestedWorkflows = count,
        successfulStarts = submitted,
        completedWorkflows = completed,
        failedWorkflows = failed,
        expectedActivitySteps = count * 3,
        verifiedWorkflowHistories = sampled,
        submissionConcurrency = previous.RootElement
            .GetProperty("submissionConcurrency").GetInt32(),
        submissionDurationSeconds = previous.RootElement
            .GetProperty("submissionDurationSeconds").GetDouble(),
        submitLatencyP50Ms = previous.RootElement
            .GetProperty("submitLatencyP50Ms").GetDouble(),
        submitLatencyP95Ms = previous.RootElement
            .GetProperty("submitLatencyP95Ms").GetDouble(),
        submitLatencyP99Ms = previous.RootElement
            .GetProperty("submitLatencyP99Ms").GetDouble(),
        peakSimultaneousPending = previous.RootElement
            .GetProperty("peakSimultaneousPending").GetInt64(),
        processingDurationSeconds = Math.Round(watch.Elapsed.TotalSeconds, 3),
        processingThroughputWorkflowsPerSecond = Math.Round(completed /
            Math.Max(watch.Elapsed.TotalSeconds, .001), 2),
        completionLatencyP50Ms = OptionalDouble(summary, "p50CompletionMs"),
        completionLatencyP95Ms = OptionalDouble(summary, "p95CompletionMs"),
        completionLatencyP99Ms = OptionalDouble(summary, "p99CompletionMs"),
        maxWorkflowCompletionMs = OptionalDouble(summary, "maxCompletionMs"),
        peakDatabaseConnections = peakDb,
        peakActiveDatabaseConnections = peakActiveDb,
        peakReadyTasks = peakReady,
        peakLeasedTasks = peakLeased,
        diagnosticSamples = samples,
        valid,
        errors,
        finishedAtUtc = DateTime.UtcNow
    });
    if (!valid)
        throw new InvalidOperationException("Hub 1,000-workflow end-to-end verification failed");
}
else
{
    throw new ArgumentException("WFC_LOAD_PHASE must be submit or verify");
}
