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
            await client.RegisterAsync("api/hub/activity-registrations",activity.Registration,ct);
        foreach(var definition in catalog.Definitions)
            await client.RegisterAsync("api/hub/definitions",definition,ct);
        if(options.SequentialVersionOneCompatibility)
        {
            foreach(var activity in catalog.Activities)
                await client.RegisterAsync("api/hub/activity-registrations",activity.Registration with{Version=1,Contract=null},ct);
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
        catalog.Activities.Where(a=>options.WorkerHub is null || a.Registration.HubId==options.WorkerHub).Select(a=>new HubCapability(a.Registration.HubId,a.Registration.ActivityType,a.Registration.Version))
            .Concat(options.SequentialVersionOneCompatibility ? catalog.Activities.Where(a=>options.WorkerHub is null || a.Registration.HubId==options.WorkerHub).Select(a=>new HubCapability(a.Registration.HubId,a.Registration.ActivityType,1)) : [])
            .Distinct().ToArray(),startup)
{
    protected override async Task<object> Execute(HubLeasedTask task,string owner,CancellationToken ct)=>
        new HubTaskResultRequest(owner,task.Epoch,await executor.ExecuteActivityAsync(task,ct));
    protected override string ResultPath(HubLeasedTask task)=>$"api/hub/tasks/{task.Id}/complete";
}

/// <summary>Bounded remote worker lane that receives fenced assignments over SSE.</summary>
internal abstract class HubRuntimeWorker(HubRestClient client,HubRuntimeOptions options,ILogger logger,
    int concurrency,HubCapability[] capabilities,HubRuntimeStartup startup,HubWorkflowCapability[]? workflows=null) : BackgroundService
{
    protected HubRestClient Client => client;
    private readonly string _owner=Environment.MachineName+":"+Guid.NewGuid().ToString("N");
    protected abstract Task<object> Execute(HubLeasedTask task,string owner,CancellationToken ct);
    protected abstract string ResultPath(HubLeasedTask task);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        if (capabilities.Length == 0) return;
        if (!options.PushEnabled)
            throw new InvalidOperationException("Native Hub workers require WFC_HUB_PUSH_ENABLED=true on the worker and Hub");

        await startup.Published.WaitAsync(ct);
        var effectiveConcurrency = options.WorkerConcurrencyCap > 0
            ? Math.Min(concurrency, options.WorkerConcurrencyCap) : concurrency;
        await Push(effectiveConcurrency, ct);
    }

    private async Task Push(int maxConcurrency, CancellationToken ct)
    {
        var routes = capabilities.GroupBy(c => c.HubId)
            .Select(group => (Hub: group.Key, Capabilities: group.ToArray(),
                Workflows: workflows?.Where(w => w.HubId == group.Key).ToArray()))
            .ToArray();
        var routeIndex = Random.Shared.Next(routes.Length);
        var partition = Random.Shared.Next(options.MatchingPartitions);
        var rotationInterval = TimeSpan.FromSeconds(routes.Length > 1 ? 5 : 30);
        var seen = new HashSet<(Guid, long)>();
        var seenOrder = new Queue<(Guid, long)>();
        var gate = new object();
        var running = new Dictionary<Guid, Task>();
        using var slots = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        while (!ct.IsCancellationRequested)
        {
            var (hub, routeCapabilities, routeWorkflows) = routes[routeIndex];
            Uri target = client.Http.BaseAddress!;
            HubPushSession? session = null;
            var connected = false;
            var revoked = false;
            try
            {
                var registration = new HubPushRegistration(_owner, hub, partition,
                    options.MatchingPartitions, routeCapabilities, routeWorkflows, maxConcurrency);
                for (var hop = 0; hop < 2; hop++)
                {
                    using var response = await client.Http.PostAsJsonAsync(
                        new Uri(target, "api/hub/workers/push-sessions"), registration, ct);
                    // Earlier sessions have passed the revocation/expiry barrier
                    // before entering a new registration attempt.
                    if (response.StatusCode == HttpStatusCode.NotFound)
                        throw new InvalidOperationException("Hub push registration is unavailable. Enable WFC_HUB_PUSH_ENABLED on the Hub.");
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
                    if (routes.Length == 1 && options.MatchingPartitions == 1) return;
                    try
                    {
                        await Task.Delay(rotationInterval, connection.Token);
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
                            !routeCapabilities.Any(c => c.TaskType == assignment.Task.TaskType &&
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
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Push session disconnected; reconnecting");
            }
            Task[] draining;
            // Run continuations can remove entries; snapshot until all finish.
            while (true)
            {
                lock (gate) draining = running.Values.ToArray();
                if (draining.Length == 0) break;
                await Task.WhenAll(draining);
            }
            if (ct.IsCancellationRequested) break;
            // An uncertain disconnect does not require a fixed 75-second sleep:
            // first ask the original owner for a revocation barrier. A 204/404
            // proves its in-memory session is closed and no claim/send is active.
            // Otherwise only a changed/expired database ownership epoch can
            // safely free this lane's credit. Never reuse credits on a timeout.
            if (connected && !revoked && session is not null)
            {
                var reconciliationStarted = Stopwatch.GetTimestamp();
                var warned = false;
                while (!ct.IsCancellationRequested)
                {
                    // Independent deadlines matter: an unreachable old owner
                    // must not consume the timeout budget for the current owner
                    // lookup through the load-balanced Gateway.
                    using (var revokeProbe = CancellationTokenSource.CreateLinkedTokenSource(ct))
                    {
                        revokeProbe.CancelAfter(TimeSpan.FromSeconds(3));
                        try
                        {
                            using var revoke = await client.Http.PostAsync(new Uri(target,
                                $"api/hub/workers/push-sessions/{session.SessionId}/revoke"), null, revokeProbe.Token);
                            if (revoke.StatusCode is HttpStatusCode.NoContent or HttpStatusCode.NotFound)
                            {
                                revoked = true;
                                break;
                            }
                        }
                        catch (HttpRequestException) { }
                        catch (TaskCanceledException) when (!ct.IsCancellationRequested) { }
                    }
                    using (var ownershipProbe = CancellationTokenSource.CreateLinkedTokenSource(ct))
                    {
                        ownershipProbe.CancelAfter(TimeSpan.FromSeconds(3));
                        try
                        {
                            using var owner = await client.Http.GetAsync(
                                $"api/hub/workers/push-owners/{Uri.EscapeDataString(hub)}/{partition}", ownershipProbe.Token);
                            if (owner.StatusCode == HttpStatusCode.NotFound) break;
                            owner.EnsureSuccessStatusCode();
                            var current = await owner.Content.ReadFromJsonAsync<HubMatchingPartitionLease>(cancellationToken: ownershipProbe.Token);
                            if (current is not null && (current.Epoch != session.OwnerEpoch || current.LeaseUntil <= DateTime.UtcNow)) break;
                        }
                        catch (HttpRequestException) { }
                        catch (TaskCanceledException) when (!ct.IsCancellationRequested) { }
                    }
                    if (!warned && Stopwatch.GetElapsedTime(reconciliationStarted) > TimeSpan.FromSeconds(75))
                    {
                        warned = true;
                        logger.LogWarning("Push session reconciliation exceeded 75s for partition {Partition}; credits remain fenced",
                            partition);
                    }
                    await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
                }
            }
            else if (!connected) await Task.Delay(TimeSpan.FromSeconds(1), ct);
            if (!ct.IsCancellationRequested) HubTelemetry.SseTaskReconnects.Add(1);
            // Visit each namespace at the current shard index before advancing the shard.
            routeIndex = (routeIndex + 1) % routes.Length;
            if (routeIndex == 0) partition = (partition + 1) % options.MatchingPartitions;
        }
        Task[] remaining;
        lock (gate) remaining = running.Values.ToArray();
        await Task.WhenAll(remaining);
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
        HubTelemetry.WorkerInflight.Add(1,HubTelemetry.HubTag(task.HubId),
            HubTelemetry.TaskKindTag(isActivity ? "activity" : "workflow"),
            isActivity ? HubTelemetry.ActivityTag(task.TaskType) : new KeyValuePair<string, object?>("activity_type", "none"));
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
                    {
                        var detail = await response.Content.ReadAsStringAsync(execution.Token);
                        throw new InvalidOperationException("Hub rejected the completion contract: "+response.StatusCode+" "+detail);
                    }
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
            HubTelemetry.WorkerInflight.Add(-1,HubTelemetry.HubTag(task.HubId),
                HubTelemetry.TaskKindTag(isActivity ? "activity" : "workflow"),
                isActivity ? HubTelemetry.ActivityTag(task.TaskType) : new KeyValuePair<string, object?>("activity_type", "none"));
            if(isActivity)
            {
                HubTelemetry.WorkerExecutions.Add(1,HubTelemetry.HubTag(task.HubId),HubTelemetry.ActivityTag(task.TaskType),
                    new KeyValuePair<string,object?>("outcome",outcome));
                HubTelemetry.WorkerExecutionDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds,
                    HubTelemetry.HubTag(task.HubId),HubTelemetry.ActivityTag(task.TaskType));
            }
            await heartbeatStop.CancelAsync();
            await heartbeat;
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
