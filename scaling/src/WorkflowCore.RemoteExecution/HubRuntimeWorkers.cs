using System.Collections.Concurrent;
using System.Net;
using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Client;

namespace WorkflowCore.RemoteExecution;

internal static class HubPushOwnerRoute
{
    public static Uri Validate(string? owner, Uri baseAddress, HubRuntimeOptions options)
    {
        if (!Uri.TryCreate(owner, UriKind.Absolute, out var next) ||
            next.Scheme != Uri.UriSchemeHttps &&
            !(next.Scheme == Uri.UriSchemeHttp && (next.IsLoopback || options.AllowInsecureHttp)) ||
            !string.IsNullOrEmpty(next.UserInfo) || !string.IsNullOrEmpty(next.Query) ||
            !string.IsNullOrEmpty(next.Fragment) || next.AbsolutePath != "/" ||
            !options.PushTrustedOwners.Append(baseAddress.GetLeftPart(UriPartial.Authority))
                .Any(allowed => Uri.TryCreate(allowed, UriKind.Absolute, out var trusted) &&
                    trusted.GetLeftPart(UriPartial.Authority).Equals(
                        next.GetLeftPart(UriPartial.Authority), StringComparison.OrdinalIgnoreCase)))
            throw new InvalidOperationException("Unsafe matching owner URL");
        return new Uri(next.GetLeftPart(UriPartial.Authority) + "/");
    }
}

internal sealed class HubRuntimeStartup
{
    private readonly TaskCompletionSource _published=new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task Published=>_published.Task;
    public void Complete()=>_published.TrySetResult();
}

internal sealed class HubRuntimePublisher(HubRestClient client,HubNativeCatalog catalog,HubRuntimeOptions options,
    IHostApplicationLifetime lifetime,IServiceProvider services,HubRuntimeStartup startup) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        foreach(var activity in catalog.Activities)
            await client.RegisterAsync("api/hub/activity-registrations",activity.Policy,ct);
        foreach(var definition in catalog.Definitions)
            await client.RegisterAsync("api/hub/definitions",definition,ct);
        if(options.SequentialVersionOneCompatibility)
        {
            foreach(var activity in catalog.Activities)
                await client.RegisterAsync("api/hub/activity-registrations",activity.Policy with{Version=1,Contract=null},ct);
            var sequential=(RemoteWorkflowRegistry?)services.GetService(typeof(RemoteWorkflowRegistry));
            if(sequential is not null)
                foreach(var definition in sequential.All())
                    await client.RegisterAsync("api/hub/definitions",RemoteWorkflowRegistry.Metadata(definition),ct);
        }
        startup.Complete();
        if(options.RegisterOnly)lifetime.StopApplication();
    }
    public Task StopAsync(CancellationToken ct)=>Task.CompletedTask;
}

internal sealed class HubNativeWorkflowWorker(HubRestClient client,HubNativeCatalog catalog,HubRuntimeOptions options,
    HubNativeExecutor executor,ILogger<HubNativeWorkflowWorker> logger,IServiceProvider services,HubRuntimeStartup startup)
    : HubRuntimeWorker(client,options,logger,options.WorkflowConcurrency,
        catalog.Definitions.Where(d=>options.WorkerHub is null || d.HubId==options.WorkerHub).Select(d=>new HubCapability(d.HubId,"__native_workflow__",1))
            .Concat(options.SequentialVersionOneCompatibility
                ? (services.GetService<RemoteWorkflowRegistry>()?.All()??[]).Where(d=>options.WorkerHub is null || d.HubId==options.WorkerHub).Select(d=>new HubCapability(d.HubId,"__workflow__",1)) : [])
            .Distinct().ToArray(),startup,
        catalog.Definitions.Where(d=>options.WorkerHub is null || d.HubId==options.WorkerHub).Select(d=>new HubWorkflowCapability(d.HubId,d.DefinitionId,d.Version))
            .Concat(options.SequentialVersionOneCompatibility
                ? (services.GetService<RemoteWorkflowRegistry>()?.All()??[]).Where(d=>options.WorkerHub is null || d.HubId==options.WorkerHub).Select(d=>new HubWorkflowCapability(d.HubId,d.DefinitionId,d.Version)) : [])
            .Distinct().ToArray())
{
    protected override async Task<object> Execute(HubLeasedTask task,string owner,CancellationToken ct)
    {
        if(task.TaskType=="__workflow__")
        {
            var sequential=await Client.GetAsync<HubWorkflowRuntime>($"api/hub/workflows/{task.WorkflowId}/runtime",ct);
            var definition=services.GetRequiredService<RemoteWorkflowRegistry>().Find(sequential.HubId,sequential.DefinitionId,sequential.Version)
                ?? throw new InvalidOperationException("Unknown sequential workflow version");
            if(definition.Steps.Count!=sequential.StepCount)throw new InvalidOperationException("Sequential workflow version changed");
            if(sequential.NextStep>=definition.Steps.Count)
                return new HubTransitionRequest(owner,task.Epoch,sequential.Revision,task.Id,"CompleteWorkflow");
            var step=definition.Steps[sequential.NextStep];
            return step.Kind=="Delay"
                ? new HubTransitionRequest(owner,task.Epoch,sequential.Revision,task.Id,"ScheduleTimer",TimerKey:step.ActivityType,DelayMilliseconds:step.DelayMilliseconds)
                : new HubTransitionRequest(owner,task.Epoch,sequential.Revision,task.Id,"ScheduleActivity",ActivityType:step.ActivityType,
                    ActivityVersion:step.Version,ActivityInput:sequential.Input);
        }
        var runtime=await Client.GetAsync<HubNativeRuntime>($"api/hub/workflows/{task.WorkflowId}/native-runtime",ct);
        return await executor.ExecuteAsync(runtime,task,owner,ct);
    }
    protected override string ResultPath(HubLeasedTask task)=>$"api/hub/workflows/{task.WorkflowId}/"+
        (task.TaskType=="__workflow__"?"transitions":"native-commits");
}

internal sealed class HubNativeActivityWorker(HubRestClient client,HubNativeCatalog catalog,HubRuntimeOptions options,
    HubNativeExecutor executor,ILogger<HubNativeActivityWorker> logger,HubRuntimeStartup startup)
    : HubRuntimeWorker(client,options,logger,options.ActivityConcurrency,
        catalog.Activities.Where(a=>options.WorkerHub is null || a.Policy.HubId==options.WorkerHub).Select(a=>new HubCapability(a.Policy.HubId,a.Policy.ActivityType,a.Policy.Version))
            .Concat(options.SequentialVersionOneCompatibility ? catalog.Activities.Where(a=>options.WorkerHub is null || a.Policy.HubId==options.WorkerHub).Select(a=>new HubCapability(a.Policy.HubId,a.Policy.ActivityType,1)) : [])
            .Distinct().ToArray(),startup)
{
    protected override async Task<object> Execute(HubLeasedTask task,string owner,CancellationToken ct)=>
        new HubTaskResultRequest(owner,task.Epoch,await executor.ExecuteActivityAsync(task,ct));
    protected override string ResultPath(HubLeasedTask task)=>$"api/hub/tasks/{task.Id}/complete";
}

/// <summary>Bounded remote worker lane. SSE hints reduce latency; periodic claims
/// recover missed hints. Heartbeats fence execution and retries reuse the same result.</summary>
internal abstract class HubRuntimeWorker(HubRestClient client,HubRuntimeOptions options,ILogger logger,
    int concurrency,HubCapability[] capabilities,HubRuntimeStartup startup,HubWorkflowCapability[]? workflows=null) : BackgroundService
{
    protected HubRestClient Client => client;
    private readonly string _owner=Environment.MachineName+":"+Guid.NewGuid().ToString("N");
    private readonly Channel<bool> _hints=Channel.CreateBounded<bool>(new BoundedChannelOptions(1){FullMode=BoundedChannelFullMode.DropWrite});
    private readonly KeyValuePair<string, object?> _lane =
        new("lane", workflows is null ? "activity" : "workflow");
    protected abstract Task<object> Execute(HubLeasedTask task,string owner,CancellationToken ct);
    protected abstract string ResultPath(HubLeasedTask task);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        if(capabilities.Length==0)return;
        await startup.Published.WaitAsync(ct);
        // Each worker lane is bounded before leasing; no task waits locally
        // after lease acquisition merely to obtain an execution slot.
        var effectiveConcurrency = options.WorkerConcurrencyCap > 0
            ? Math.Min(concurrency, options.WorkerConcurrencyCap) : concurrency;
        if (options.PushEnabled && effectiveConcurrency > 0 &&
            capabilities.Select(c => c.HubId).Distinct().Count() == 1 &&
            await Push(effectiveConcurrency,ct)) return;
        var running=new List<Task>();
        // A matching poll requires one Hub namespace. Multi-hub workers retain
        // the proven PR #18 SSE/adaptive path until per-Hub fan-out is added.
        var matchingPoll=options.LongPollingEnabled &&
            capabilities.Select(c => c.HubId).Distinct().Count() == 1;
        var listener=matchingPoll ? Task.CompletedTask : Listen(ct);
        // True Hub-to-worker SSE distribution: server acquires a fenced
        // lease on readiness and pushes it over a persistent stream. A separate
        // REST credit frame replenishes capacity after execution completes.
        if (matchingPoll && options.MatchingPartitions > 1 &&
            effectiveConcurrency > 0 &&
            Environment.GetEnvironmentVariable("WFC_HUB_TASK_SSE_PUSH") is
                ("true" or "1" or "True"))
        {
            await RunSseTaskPushAsync(effectiveConcurrency, ct);
            return;
        }
        // Experimental: reserve one actual execution slot for each outstanding
        // shard poll. The current single rotating poll stays the default.
        // Do not enable for multi-Hub lanes, which still use SSE/REST fallback.
        if (matchingPoll && options.MatchingPartitions > 1 &&
            Environment.GetEnvironmentVariable("WFC_HUB_WORKER_CAPACITY_FANOUT") is
                ("true" or "1" or "True"))
        {
            await RunCapacityAwarePollsAsync(effectiveConcurrency, ct);
            return;
        }
        // Rotating one outstanding long poll per worker lane avoids racing
        // multiple matching partitions into leasing more tasks than slots.
        // A 4-second server hold bounds discovery when a shard has no pollers.
        var matchingPartition = Random.Shared.Next(options.MatchingPartitions);
        var emptyClaims=0;
        long nextClaimAt=0;
        var hintPreempted=false;
        try
        {
            while(!ct.IsCancellationRequested)
            {
                running.RemoveAll(t=>t.IsCompleted);
                if(running.Count<effectiveConcurrency &&
                    (matchingPoll || !options.AdaptiveClaimsEnabled ||
                     Stopwatch.GetTimestamp()>=nextClaimAt))
                {
                    try
                    {
                        var endpoint = matchingPoll
                            ? "api/hub/workers/tasks:poll"
                            : "api/hub/workers/tasks:claim";
                        var claimHttpStarted = Stopwatch.GetTimestamp();
                        var claimHttpOutcome = "error";
                        HubLeasedTask[] leases;
                        try
                        {
                            leases = await ClaimRequestAsync(endpoint,
                                new HubClaimRequest(_owner,capabilities.Select(c=>c.HubId).Distinct().ToArray(),capabilities,
                                    Math.Min(16,effectiveConcurrency-running.Count),workflows,
                                    matchingPoll && options.MatchingPartitions>1 ? matchingPartition : null,
                                    matchingPoll && options.MatchingPartitions>1 ? options.MatchingPartitions : null), ct);
                            claimHttpOutcome = leases.Length > 0 ? "claimed" : "empty";
                        }
                        finally
                        {
                            HubTelemetry.WorkerClaimHttpDuration.Record(Stopwatch.GetElapsedTime(claimHttpStarted).TotalSeconds,
                                _lane, new KeyValuePair<string, object?>("outcome", claimHttpOutcome));
                        }
                        if (matchingPoll && options.MatchingPartitions > 1)
                            matchingPartition = (matchingPartition + 1) % options.MatchingPartitions;
                        HubTelemetry.WorkerClaimRequests.Add(1,_lane,
                            new KeyValuePair<string,object?>("outcome",leases.Length>0?"claimed":"empty"));
                        foreach(var lease in leases)running.Add(Run(lease,ct));
                        if(leases.Length>0)
                        {
                            emptyClaims=0;
                            nextClaimAt=0;
                            hintPreempted=false;
                            continue;
                        }
                        if(options.AdaptiveClaimsEnabled && !matchingPoll)
                        {
                            emptyClaims=Math.Min(emptyClaims+1,8);
                            // Back off only after empty claims; successful work is claimed immediately.
                            // Jitter keeps different hosts from waking together on the periodic sweep.
                            var baseMilliseconds=Math.Max(1000,options.PollInterval.TotalMilliseconds);
                            var milliseconds=Math.Min(8000,baseMilliseconds*(1<<Math.Min(emptyClaims-1,3)));
                            var backoff=TimeSpan.FromMilliseconds(milliseconds*(0.8+Random.Shared.NextDouble()*0.4));
                            nextClaimAt=Stopwatch.GetTimestamp()+(long)(backoff.TotalSeconds*Stopwatch.Frequency);
                            hintPreempted=false;
                            HubTelemetry.WorkerEmptyClaimBackoff.Record(backoff.TotalSeconds,_lane);
                        }
                    }
                    catch(OperationCanceledException)when(ct.IsCancellationRequested){break;}
                    catch(Exception ex)
                    {
                        HubTelemetry.WorkerClaimRequests.Add(1,_lane,
                            new KeyValuePair<string,object?>("outcome","error"));
                        logger.LogWarning(ex,"Hub claim failed; retrying");
                        if(options.AdaptiveClaimsEnabled && !matchingPoll)
                            nextClaimAt=Stopwatch.GetTimestamp()+
                                (long)(Math.Max(1,options.PollInterval.TotalSeconds)*Stopwatch.Frequency);
                        if(matchingPoll)
                        {
                            if (options.MatchingPartitions > 1)
                                matchingPartition = (matchingPartition + 1) % options.MatchingPartitions;
                            await Task.Delay(TimeSpan.FromSeconds(1), ct);
                        }
                    }
                }
                if (matchingPoll && running.Count < concurrency)
                    continue; // next server-held poll; no local jitter/backoff
                using var wait=CancellationTokenSource.CreateLinkedTokenSource(ct);
                var pollDelay=options.PollInterval;
                if(options.AdaptiveClaimsEnabled && running.Count<effectiveConcurrency)
                {
                    var ticksRemaining=nextClaimAt-Stopwatch.GetTimestamp();
                    if(ticksRemaining>0)
                        pollDelay=TimeSpan.FromSeconds((double)ticksRemaining/Stopwatch.Frequency);
                }
                var poll=Task.Delay(pollDelay,wait.Token);
                var hint=_hints.Reader.WaitToReadAsync(wait.Token).AsTask();
                await Task.WhenAny(running.Append(poll).Append(hint));
                await wait.CancelAsync();
                var hinted=false;
                while(_hints.Reader.TryRead(out _)) hinted=true;
                if(hinted && options.AdaptiveClaimsEnabled && emptyClaims>0 &&
                    running.Count<effectiveConcurrency && !hintPreempted)
                {
                    // A fresh readiness hint may preempt one long empty-queue backoff,
                    // but never triggers an immediate, unbounded claim stampede.
                    var minHintMilliseconds=Math.Max(250,Math.Min(500,options.PollInterval.TotalMilliseconds/2));
                    var due=Stopwatch.GetTimestamp()+
                        (long)(minHintMilliseconds*Stopwatch.Frequency/1000);
                    nextClaimAt=Math.Min(nextClaimAt,due);
                    hintPreempted=true;
                }
            }
        }
        finally
        {
            await Task.WhenAll(running);
            try{await listener;}catch(OperationCanceledException)when(ct.IsCancellationRequested) { }
        }
    }

    /// <summary>
    /// The worker reserves a free execution slot per concurrent long-poll.
    /// Watching multiple shards eliminates serial four-second shard rotation,
    /// but cannot increase leased tasks beyond the worker's execution limit.
    /// Each request asks for MaxTasks=1. This is deliberately NOT Hub push,
    /// a durable credit registry, or a replacement for PostgreSQL eligibility.
    /// </summary>
    private async Task RunCapacityAwarePollsAsync(int capacity, CancellationToken ct)
    {
        if (capacity < 1) return;
        var upperBound = Math.Min(4, Math.Min(capacity, options.MatchingPartitions));
        var maxPolls = int.TryParse(
            Environment.GetEnvironmentVariable("WFC_HUB_WORKER_MAX_SHARD_POLLS"),
            out var requested) ? Math.Clamp(requested, 1, upperBound) : upperBound;
        var planner = new HubShardCapacityPlanner(capacity, options.MatchingPartitions,
            maxPolls, Random.Shared.Next(options.MatchingPartitions));
        logger.LogInformation("Capacity-aware shard polling enabled: {Credits} slots, {ShardPolls} concurrent polls, {Shards} matching shards",
            capacity, maxPolls, options.MatchingPartitions);
        var active = new List<Task>();
        var pending = new Dictionary<int, Task<HubLeasedTask[]>>();
        using var pollStop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                active.RemoveAll(task => task.IsCompleted);
                while (planner.TryReserve(active.Count, out var shard))
                {
                    HubTelemetry.WorkerShardPollCredits.Add(1, _lane);
                    pending.Add(shard, PollReservedShardAsync(shard, pollStop.Token));
                }
                var all = active.Concat(pending.Values.Cast<Task>()).ToArray();
                if (all.Length == 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), ct);
                    continue;
                }
                await Task.WhenAny(all);
                // Release only after each reply has arrived; a pending HTTP
                // request still consumes one reserved execution credit.
                foreach (var (shard, poll) in pending.Where(x => x.Value.IsCompleted).ToArray())
                {
                    pending.Remove(shard);
                    planner.Release(shard);
                    HubTelemetry.WorkerShardPollCredits.Add(-1, _lane);
                    try
                    {
                        foreach (var lease in await poll)
                            active.Add(Run(lease, ct));
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Capacity-aware shard poll failed; retrying");
                    }
                }
            }
        }
        finally
        {
            await pollStop.CancelAsync();
            try { await Task.WhenAll(pending.Values); }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            catch (Exception ex) { logger.LogDebug(ex, "Shard polls ended during shutdown"); }
            foreach (var shard in pending.Keys)
            {
                planner.Release(shard);
                HubTelemetry.WorkerShardPollCredits.Add(-1, _lane);
            }
            await Task.WhenAll(active);
        }
    }

    private async Task<HubLeasedTask[]> PollReservedShardAsync(int shard, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        var outcome = "error";
        try
        {
            var leases = await ClaimRequestAsync("api/hub/workers/tasks:poll",
                new HubClaimRequest(_owner,
                    capabilities.Select(c => c.HubId).Distinct().ToArray(), capabilities,
                    MaxTasks: 1, Workflows: workflows,
                    MatchingPartition: shard, MatchingPartitionCount: options.MatchingPartitions), ct);
            outcome = leases.Length > 0 ? "claimed" : "empty";
            HubTelemetry.WorkerClaimRequests.Add(1, _lane,
                new KeyValuePair<string, object?>("outcome", outcome));
            return leases;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (Exception ex)
        {
            HubTelemetry.WorkerClaimRequests.Add(1, _lane,
                new KeyValuePair<string, object?>("outcome", "error"));
            logger.LogWarning(ex, "Capacity-aware poll of matching shard {Shard} failed", shard);
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
            return [];
        }
        finally
        {
            HubTelemetry.WorkerClaimHttpDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds,
                _lane, new KeyValuePair<string, object?>("outcome", outcome));
        }
    }

    private Task<HubLeasedTask[]> ClaimRequestAsync(string endpoint, HubClaimRequest request, CancellationToken ct) =>
        InstrumentClaimAsync(endpoint, _lane, () => client.PostAsync<HubLeasedTask[]>(endpoint, request, ct));

    internal static async Task<HubLeasedTask[]> InstrumentClaimAsync(string endpoint,
        KeyValuePair<string, object?> lane, Func<Task<HubLeasedTask[]>> send)
    {
        var transport = new KeyValuePair<string, object?>("transport", endpoint.EndsWith(":poll", StringComparison.Ordinal) ? "poll" : "claim");
        var started = Stopwatch.GetTimestamp();
        var outcome = "error";
        HubTelemetry.ClaimAttempts.Add(1, lane, transport);
        HubTelemetry.ClaimInflight.Add(1, lane, transport);
        try
        {
            var leases = await send();
            outcome = leases.Length == 0 ? "empty" : "claimed";
            if (leases.Length == 0) HubTelemetry.ClaimEmpty.Add(1, lane, transport);
            HubTelemetry.ClaimBatchSize.Record(leases.Length, lane, transport);
            foreach (var group in leases.GroupBy(t => t.TaskType is "__workflow__" or "__native_workflow__" ? "workflow" : "activity"))
                HubTelemetry.ClaimSuccess.Add(group.Count(), new KeyValuePair<string, object?>("lane", group.Key), transport);
            return leases;
        }
        catch (OperationCanceledException)
        {
            outcome = "cancelled";
            throw;
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(started).TotalSeconds;
            HubTelemetry.ClaimHttpDuration.Record(elapsed, lane, transport, new KeyValuePair<string, object?>("outcome", outcome));
            HubTelemetry.ClaimInflight.Add(-1, lane, transport);
        }
    }

    /// <summary>
    /// Hub-side server-push SSE. A lane-wide coordinator owns ALL execution
    /// slots. It offers one-shot credits to whichever matching shard needs
    /// work; zero-credit streams still receive ownership-fencing heartbeats.
    /// No shard can independently overbook the same execution slot.
    /// </summary>
    private async Task RunSseTaskPushAsync(int capacity, CancellationToken ct)
    {
        capacity = Math.Min(capacity, 64); // the SSE registration protocol bound
        var credits = new HubSseWorkerCreditCoordinator(capacity, options.MatchingPartitions);
        var running = new ConcurrentDictionary<Guid, Task>();
        logger.LogInformation(
            "SSE task push enabled: {Capacity} globally shared execution slots across {Shards} matching shards",
            capacity, options.MatchingPartitions);
        try
        {
            var streams = Enumerable.Range(0, options.MatchingPartitions)
                .Select(shard => RunSseShardAsync(shard, credits, running, ct));
            await Task.WhenAll(streams.Append(RunSseGrantSchedulerAsync(credits, ct)));
        }
        finally
        {
            await Task.WhenAll(running.Values);
        }
    }

    private async Task RunSseGrantSchedulerAsync(
        HubSseWorkerCreditCoordinator credits, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            if (!credits.TryOffer(out var offer))
            {
                await credits.WaitForChangeAsync(ct);
                continue;
            }

            try
            {
                // A task is NOT claimed by this call. The server may lease
                // exactly one task and push it on the matching SSE stream.
                using var response = await client.Http.PostAsJsonAsync(
                    "api/hub/workers/tasks:credit",
                    new {
                        sessionId = offer.SessionId,
                        ownerEpoch = offer.OwnerEpoch,
                        workerId = _owner,
                        hubId = capabilities[0].HubId,
                        matchingPartition = offer.Shard,
                        creditId = offer.CreditId
                    }, ct);
                if (!response.IsSuccessStatusCode)
                {
                    credits.OfferFailed(offer);
                    // 409 / 503 / 404 means the session/owner may have
                    // transitioned. Reconnect rather than retrying a credit
                    // blindly into the wrong owner's in-memory registry.
                    credits.Invalidate(offer.Shard, offer.SessionId);
                    logger.LogWarning(
                        "SSE credit grant rejected for shard {Shard}: HTTP {Status}",
                        offer.Shard, (int)response.StatusCode);
                    await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                credits.OfferFailed(offer);
                credits.Invalidate(offer.Shard, offer.SessionId);
                logger.LogWarning(ex,
                    "SSE credit grant transport failure on shard {Shard}", offer.Shard);
                await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
            }
        }
    }

    private async Task RunSseShardAsync(int shard,
        HubSseWorkerCreditCoordinator credits,
        ConcurrentDictionary<Guid, Task> running, CancellationToken ct)
    {
        var attempted = false;
        while (!ct.IsCancellationRequested)
        {
            if (attempted) HubTelemetry.SseTaskReconnects.Add(1);
            attempted = true;
            Guid sessionId = Guid.Empty;
            using var sessionStop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post,
                    "api/hub/workers/tasks:stream");
                request.Content = JsonContent.Create(new {
                    claim = new HubClaimRequest(_owner,
                        capabilities.Select(c => c.HubId).Distinct().ToArray(),
                        capabilities, MaxTasks: 1, Workflows: workflows,
                        MatchingPartition: shard,
                        MatchingPartitionCount: options.MatchingPartitions),
                    capacity = credits.Capacity,
                    initialCredits = 0
                });
                using var response = await client.Http.SendAsync(request,
                    HttpCompletionOption.ResponseHeadersRead, sessionStop.Token);
                response.EnsureSuccessStatusCode();
                await using var stream = await response.Content.ReadAsStreamAsync(sessionStop.Token);
                using var reader = new StreamReader(stream);
                string? eventName = null;
                string? payload = null;
                while (await reader.ReadLineAsync(sessionStop.Token) is string line)
                {
                    if (line.StartsWith("event: ", StringComparison.Ordinal))
                        eventName = line["event: ".Length..];
                    else if (line.StartsWith("data: ", StringComparison.Ordinal))
                        payload = line["data: ".Length..];
                    else if (line.Length == 0)
                    {
                        if (eventName == "session" && payload is not null)
                        {
                            using var json = JsonDocument.Parse(payload);
                            var nextId = json.RootElement.GetProperty("sessionId").GetGuid();
                            var epoch = json.RootElement.GetProperty("ownerEpoch").GetInt64();
                            credits.Connected(shard, nextId, epoch, () => sessionStop.Cancel());
                            sessionId = nextId;
                        }
                        else if (eventName == "credit.unused" && payload is not null)
                        {
                            using var json = JsonDocument.Parse(payload);
                            credits.Unused(shard, sessionId,
                                json.RootElement.GetProperty("creditId").GetGuid());
                        }
                        else if (eventName == "task.leased" && payload is not null)
                        {
                            using var json = JsonDocument.Parse(payload);
                            var creditId = json.RootElement.GetProperty("creditId").GetGuid();
                            var task = json.RootElement.GetProperty("task").Deserialize<HubLeasedTask>(
                                new JsonSerializerOptions(JsonSerializerDefaults.Web))
                                ?? throw new InvalidOperationException("Empty SSE task lease");
                            if (!credits.Dispatched(shard, sessionId, creditId, task.Id))
                            {
                                // The session or credit was revoked while a
                                // response was in flight. Never execute an
                                // unreserved lease; PostgreSQL will recover it.
                                logger.LogWarning(
                                    "Ignoring stale/unreserved SSE task {Task} from shard {Shard}",
                                    task.Id, shard);
                            }
                            else
                            {
                                var done = new TaskCompletionSource(
                                    TaskCreationOptions.RunContinuationsAsynchronously);
                                if (!running.TryAdd(task.Id, done.Task))
                                {
                                    credits.Finished(task.Id);
                                    logger.LogWarning("Duplicate SSE task {Task} rejected", task.Id);
                                }
                                else
                                {
                                    _ = Task.Run(async () =>
                                    {
                                        try { await Run(task, ct); }
                                        catch (Exception ex)
                                        {
                                            logger.LogError(ex,
                                                "SSE task execution error for {Task}", task.Id);
                                        }
                                        finally
                                        {
                                            running.TryRemove(task.Id, out _);
                                            credits.Finished(task.Id);
                                            done.TrySetResult();
                                        }
                                    });
                                }
                            }
                        }
                        eventName = null;
                        payload = null;
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (OperationCanceledException)
            {
                // Session was explicitly invalidated because owner changed,
                // or a grant was rejected during matching failover.
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "SSE task stream for shard {Shard} disconnected; retrying", shard);
            }
            finally
            {
                credits.Disconnected(shard, sessionId);
            }
            // Existing tasks are allowed to finish independently of session
            // re-establishment; their PostgreSQL epochs remain authoritative.
            try { await Task.Delay(TimeSpan.FromMilliseconds(350), ct); }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
        }
    }

    // Local slots remain the final guard if a capacity snapshot races delivery.
    private async Task<bool> Push(int maxConcurrency, CancellationToken ct)
    {
        var hub = capabilities[0].HubId;
        var partition = Random.Shared.Next(options.MatchingPartitions);
        var seen = new HashSet<(Guid, long)>();
        var seenOrder = new Queue<(Guid, long)>();
        var gate = new object();
        var running = new Dictionary<Guid, Task>();
        using var slots = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        while (!ct.IsCancellationRequested)
        {
            Uri target = client.Http.BaseAddress!;
            HubPushSession? session = null;
            var connected = false;
            var revoked = false;
            try
            {
                var registration = new HubPushRegistration(_owner, hub, partition,
                    options.MatchingPartitions, capabilities, workflows, maxConcurrency);
                for (var hop = 0; hop < 2; hop++)
                {
                    using var response = await client.Http.PostAsJsonAsync(
                        new Uri(target, "api/hub/workers/push-sessions"), registration, ct);
                    // Earlier sessions have passed the revocation/expiry barrier
                    // before entering a new registration attempt.
                    if (response.StatusCode == HttpStatusCode.NotFound) return false;
                    if (response.StatusCode == HttpStatusCode.Conflict)
                    {
                        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
                        var owner = body.RootElement.GetProperty("ownerUrl").GetString();
                        target = HubPushOwnerRoute.Validate(owner, client.Http.BaseAddress!, options);
                        continue;
                    }
                    response.EnsureSuccessStatusCode();
                    session = await response.Content.ReadFromJsonAsync<HubPushSession>(cancellationToken: ct)
                        ?? throw new InvalidOperationException("Missing push session");
                    target = HubPushOwnerRoute.Validate(session.OwnerUrl, client.Http.BaseAddress!, options);
                    break;
                }
                if (session is null) throw new InvalidOperationException("Push owner routing failed");
                using var request = new HttpRequestMessage(HttpMethod.Get,
                    new Uri(target, $"api/hub/workers/push-sessions/{session.SessionId}/events"));
                using var streamResponse = await client.Http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
                streamResponse.EnsureSuccessStatusCode();
                connected = true;
                using var connection = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var capacityWake = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
                    { FullMode = BoundedChannelFullMode.DropWrite });
                var rotating = false;
                var released = new HashSet<Guid>();
                long sequence = 0;
                async Task RotateLater()
                {
                    if (options.MatchingPartitions <= 1) return;
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), connection.Token);
                        lock (gate) rotating = true;
                        capacityWake.Writer.TryWrite(true);
                    }
                    catch (OperationCanceledException) when (connection.IsCancellationRequested) { }
                }
                var rotation = RotateLater();
                async Task CapacityLoop()
                {
                    while (!connection.IsCancellationRequested)
                    {
                        int free; Guid[] active; Guid[] completed; bool revoke;
                        lock (gate)
                        {
                            free = rotating ? 0 : maxConcurrency - running.Count;
                            active = running.Keys.ToArray();
                            completed = released.ToArray();
                            revoke = rotating;
                        }
                        using var update = await client.Http.PutAsJsonAsync(
                            new Uri(target, $"api/hub/workers/push-sessions/{session.SessionId}/capacity"),
                            new HubPushCapacity(++sequence, free, SeenDeliveryIds: active,
                                ReleasedDeliveryIds: completed), connection.Token);
                        update.EnsureSuccessStatusCode();
                        lock (gate) foreach (var id in completed) released.Remove(id);
                        if (revoke)
                        {
                            using var response = await client.Http.PostAsync(new Uri(target,
                                $"api/hub/workers/push-sessions/{session.SessionId}/revoke"), null, connection.Token);
                            if (response.StatusCode is HttpStatusCode.NoContent or HttpStatusCode.NotFound)
                            {
                                revoked = true;
                                await connection.CancelAsync();
                                return;
                            }
                            if (response.StatusCode != HttpStatusCode.Conflict) response.EnsureSuccessStatusCode();
                        }
                        using var delay = CancellationTokenSource.CreateLinkedTokenSource(connection.Token);
                        await Task.WhenAny(Task.Delay(revoke ? TimeSpan.FromMilliseconds(250) : TimeSpan.FromSeconds(4), delay.Token),
                            capacityWake.Reader.WaitToReadAsync(delay.Token).AsTask());
                        await delay.CancelAsync();
                        while (capacityWake.Reader.TryRead(out _)) { }
                    }
                }
                var updates = CapacityLoop();
                try
                {
                    await using var stream = await streamResponse.Content.ReadAsStreamAsync(connection.Token);
                    using var reader = new StreamReader(stream);
                    while (!connection.IsCancellationRequested)
                    {
                        var read = reader.ReadLineAsync(connection.Token).AsTask();
                        var finished = await Task.WhenAny(read, updates);
                        if (finished == updates) { await updates; break; }
                        var line = await read;
                        if (line is null) break;
                        if (!line.StartsWith("data: ", StringComparison.Ordinal)) continue;
                        var assignment = JsonSerializer.Deserialize<HubPushAssignment>(line[6..]);
                        if (assignment is null || assignment.SessionId != session.SessionId ||
                            assignment.WorkerId != _owner || assignment.Task.HubId != hub ||
                            assignment.Task.Epoch < 1 ||
                            !capabilities.Any(c => c.TaskType == assignment.Task.TaskType &&
                                c.Version == assignment.Task.Version)) continue;
                        if (assignment.Task.LeaseUntil <= DateTime.UtcNow)
                        {
                            lock (gate) released.Add(assignment.DeliveryId);
                            capacityWake.Writer.TryWrite(true);
                            continue;
                        }
                        lock (gate)
                        {
                            var key = (assignment.Task.Id, assignment.Task.Epoch);
                            if (!seen.Add(key))
                            {
                                if (!running.ContainsKey(assignment.DeliveryId))
                                {
                                    released.Add(assignment.DeliveryId);
                                    capacityWake.Writer.TryWrite(true);
                                }
                                continue;
                            }
                            seenOrder.Enqueue(key);
                            if (seenOrder.Count > 4096) seen.Remove(seenOrder.Dequeue());
                            if (!slots.Wait(0))
                            {
                                // Never execute without a slot; the unstarted
                                // lease remains fenced until its natural expiry.
                                released.Add(assignment.DeliveryId);
                                capacityWake.Writer.TryWrite(true);
                                continue;
                            }
                            var task = Run(assignment.Task, ct);
                            running.Add(assignment.DeliveryId, task);
                            _ = task.ContinueWith(_ =>
                            {
                                lock (gate)
                                {
                                    running.Remove(assignment.DeliveryId);
                                    released.Add(assignment.DeliveryId);
                                    slots.Release();
                                }
                                capacityWake.Writer.TryWrite(true);
                            },
                                CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                        }
                    }
                }
                finally
                {
                    await connection.CancelAsync();
                    try { await updates; } catch (OperationCanceledException) when (connection.IsCancellationRequested) { }
                    await rotation;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { logger.LogWarning(ex, "Push session disconnected; reconnecting"); }
            Task[] draining;
            // Run continuations can remove entries; snapshot until all finish.
            while (true)
            {
                lock (gate) draining = running.Values.ToArray();
                if (draining.Length == 0) break;
                await Task.WhenAll(draining);
            }
            if (ct.IsCancellationRequested) break;
            // On an uncertain disconnect, wait out both session and task TTLs,
            // then require either an old-owner revocation barrier or a changed /
            // expired database owner epoch before reusing this lane's credit.
            if (connected && !revoked && session is not null)
            {
                await Task.Delay(TimeSpan.FromSeconds(75), ct);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        using var revoke = await client.Http.PostAsync(new Uri(target,
                            $"api/hub/workers/push-sessions/{session.SessionId}/revoke"), null, ct);
                        if (revoke.StatusCode is HttpStatusCode.NoContent or HttpStatusCode.NotFound) break;
                    }
                    catch (HttpRequestException) { }
                    catch (TaskCanceledException) when (!ct.IsCancellationRequested) { }
                    try
                    {
                        using var owner = await client.Http.GetAsync(
                            $"api/hub/workers/push-owners/{Uri.EscapeDataString(hub)}/{partition}", ct);
                        if (owner.StatusCode == HttpStatusCode.NotFound) break;
                        owner.EnsureSuccessStatusCode();
                        var current = await owner.Content.ReadFromJsonAsync<HubMatchingPartitionLease>(cancellationToken: ct);
                        if (current is not null && (current.Epoch != session.OwnerEpoch || current.LeaseUntil <= DateTime.UtcNow)) break;
                    }
                    catch (HttpRequestException) { }
                    catch (TaskCanceledException) when (!ct.IsCancellationRequested) { }
                    await Task.Delay(TimeSpan.FromSeconds(2), ct);
                }
            }
            else if (!connected) await Task.Delay(TimeSpan.FromSeconds(1), ct);
            partition = (partition + 1) % options.MatchingPartitions;
        }
        Task[] remaining;
        lock (gate) remaining = running.Values.ToArray();
        await Task.WhenAll(remaining);
        return true;
    }

    private async Task Listen(CancellationToken ct)
    {
        var hubs=string.Join(",",capabilities.Select(c=>c.HubId).Distinct());
        while(!ct.IsCancellationRequested)
        {
            try
            {
                using var request=new HttpRequestMessage(HttpMethod.Get,"api/hub/workers/events?hub="+Uri.EscapeDataString(hubs));
                using var response=await client.Http.SendAsync(request,HttpCompletionOption.ResponseHeadersRead,ct);
                response.EnsureSuccessStatusCode();
                await using var stream=await response.Content.ReadAsStreamAsync(ct);
                using var reader=new StreamReader(stream);
                while(await reader.ReadLineAsync(ct) is string line)
                    if(line.StartsWith("event: tasks.available",StringComparison.Ordinal))_hints.Writer.TryWrite(true);
            }
            catch(OperationCanceledException)when(ct.IsCancellationRequested){break;}
            catch(Exception ex){logger.LogDebug(ex,"Hub SSE reconnecting");}
            await Task.Delay(options.PollInterval,ct);
        }
    }

    private async Task Run(HubLeasedTask task,CancellationToken shutdown)
    {
        var isActivity=task.TaskType is not ("__workflow__" or "__native_workflow__");
        using var span=HubTelemetry.Traces.StartActivity(isActivity?"hub.worker.activity.execute":"hub.worker.workflow.execute");
        span?.SetTag("hub",task.HubId);span?.SetTag("activity.type",task.TaskType);
        span?.SetTag("workflow.id",task.WorkflowId?.ToString());
        span?.SetTag("task.id",task.Id.ToString());
        span?.SetTag("task.epoch",task.Epoch);
        var started=Stopwatch.GetTimestamp();var outcome="failure";
        if(isActivity)HubTelemetry.WorkerInflight.Add(1,HubTelemetry.HubTag(task.HubId),HubTelemetry.ActivityTag(task.TaskType));
        using var execution=CancellationTokenSource.CreateLinkedTokenSource(shutdown);
        execution.CancelAfter(TimeSpan.FromMilliseconds(Math.Max(1,(task.LeaseUntil-DateTime.UtcNow).TotalMilliseconds)));
        using var heartbeatStop=CancellationTokenSource.CreateLinkedTokenSource(shutdown);
        var heartbeat=Heartbeat(task,execution,heartbeatStop.Token);
        try
        {
            object result;
            HubExecutionObservation.Started(task);
            using (var bodySpan = HubTelemetry.Traces.StartActivity("hub.worker.execute.body"))
            {
                bodySpan?.SetTag("task.id", task.Id.ToString());
                bodySpan?.SetTag("workflow.id", task.WorkflowId?.ToString());
                var bodyStarted = Stopwatch.GetTimestamp();
                try { result = await Execute(task,_owner,execution.Token); }
                finally
                {
                    var bodySeconds = Stopwatch.GetElapsedTime(bodyStarted).TotalSeconds;
                    bodySpan?.SetTag("body.duration_seconds", bodySeconds);
                    HubTelemetry.WorkerBodyDuration.Record(bodySeconds,
                        HubTelemetry.ActivityTag(task.TaskType));
                }
            }
            // Transport failures retry the frozen result, never the body or decisions.
            for(var attempt=0;;attempt=Math.Min(attempt+1,5))
            {
                try
                {
                    using var completionSpan=HubTelemetry.Traces.StartActivity("hub.worker.completion.http");
                    completionSpan?.SetTag("task.id",task.Id.ToString());
                    completionSpan?.SetTag("workflow.id",task.WorkflowId?.ToString());
                    var completionStarted=Stopwatch.GetTimestamp();
                    using var response=await client.Http.PostAsJsonAsync(ResultPath(task),result,execution.Token);
                    var completionSeconds=Stopwatch.GetElapsedTime(completionStarted).TotalSeconds;
                    completionSpan?.SetTag("http.response.status_code",(int)response.StatusCode);
                    completionSpan?.SetTag("completion.duration_seconds",completionSeconds);
                    HubTelemetry.WorkerCompletionDuration.Record(completionSeconds,
                        HubTelemetry.ActivityTag(task.TaskType));
                    if(response.StatusCode==HttpStatusCode.Conflict){outcome="fenced";return;}
                    if((int)response.StatusCode>=500 || response.StatusCode is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests)
                        throw new HttpRequestException("Hub completion temporarily unavailable: "+response.StatusCode);
                    if(!response.IsSuccessStatusCode)
                        throw new InvalidOperationException("Hub rejected the completion contract: "+response.StatusCode);
                    outcome="success";return;
                }
                catch(Exception ex)when(ex is HttpRequestException or TaskCanceledException && !execution.IsCancellationRequested)
                { await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(2000,100*(1<<attempt))),execution.Token); }
            }
        }
        catch(OperationCanceledException)when(execution.IsCancellationRequested) { outcome="cancelled"; }
        catch(Exception ex)
        {
            logger.LogError(ex,"Hub execution failed for {Task}",task.Id);
            if(!execution.IsCancellationRequested)
            {
                try
                {
                    using var response=await client.Http.PostAsJsonAsync($"api/hub/tasks/{task.Id}/fail",
                        new HubTaskFailureRequest(_owner,task.Epoch,ex.Message),execution.Token);
                    response.EnsureSuccessStatusCode();
                }
                catch(Exception report){logger.LogWarning(report,"Hub failure report lost; lease recovery will retry");}
            }
        }
        finally
        {
            span?.SetTag("task.outcome",outcome);
            if(isActivity)
            {
                HubTelemetry.WorkerInflight.Add(-1,HubTelemetry.HubTag(task.HubId),HubTelemetry.ActivityTag(task.TaskType));
                HubTelemetry.WorkerExecutions.Add(1,HubTelemetry.HubTag(task.HubId),HubTelemetry.ActivityTag(task.TaskType),
                    new KeyValuePair<string,object?>("outcome",outcome));
                HubTelemetry.WorkerExecutionDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds,
                    HubTelemetry.HubTag(task.HubId),HubTelemetry.ActivityTag(task.TaskType));
            }
            await heartbeatStop.CancelAsync();
            await heartbeat;
            _hints.Writer.TryWrite(true);
        }
    }

    private async Task Heartbeat(HubLeasedTask task,CancellationTokenSource execution,CancellationToken ct)
    {
        using var timer=new PeriodicTimer(TimeSpan.FromSeconds(10));
        try
        {
            while(await timer.WaitForNextTickAsync(ct))
            {
                var renewalStarted=Stopwatch.GetTimestamp();
                using var response=await client.Http.PostAsJsonAsync($"api/hub/tasks/{task.Id}/heartbeat",
                    new HubTaskHeartbeatRequest(_owner,task.Epoch),ct);
                if(!response.IsSuccessStatusCode){await execution.CancelAsync();return;}
                // Include response latency in the deadline; a delayed ACK must
                // not extend local execution beyond the lease it renewed.
                execution.CancelAfter(TimeSpan.FromMilliseconds(Math.Max(1,29000-Stopwatch.GetElapsedTime(renewalStarted).TotalMilliseconds)));
            }
        }
        catch(OperationCanceledException)when(ct.IsCancellationRequested) { }
        catch(Exception ex){logger.LogWarning(ex,"Hub lease renewal failed");await execution.CancelAsync();}
    }
}
