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
        catalog.Definitions.Select(d=>new HubCapability(d.HubId,"__native_workflow__",1))
            .Concat(options.SequentialVersionOneCompatibility
                ? (services.GetService<RemoteWorkflowRegistry>()?.All()??[]).Select(d=>new HubCapability(d.HubId,"__workflow__",1)) : [])
            .Distinct().ToArray(),startup,
        catalog.Definitions.Select(d=>new HubWorkflowCapability(d.HubId,d.DefinitionId,d.Version))
            .Concat(options.SequentialVersionOneCompatibility
                ? (services.GetService<RemoteWorkflowRegistry>()?.All()??[]).Select(d=>new HubWorkflowCapability(d.HubId,d.DefinitionId,d.Version)) : [])
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
        catalog.Activities.Select(a=>new HubCapability(a.Policy.HubId,a.Policy.ActivityType,a.Policy.Version))
            .Concat(options.SequentialVersionOneCompatibility ? catalog.Activities.Select(a=>new HubCapability(a.Policy.HubId,a.Policy.ActivityType,1)) : [])
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
        var running=new List<Task>();
        var listener=options.LongPollingEnabled ? Task.CompletedTask : Listen(ct);
        var emptyClaims=0;
        long nextClaimAt=0;
        var hintPreempted=false;
        try
        {
            while(!ct.IsCancellationRequested)
            {
                running.RemoveAll(t=>t.IsCompleted);
                if(running.Count<concurrency &&
                    (options.LongPollingEnabled || !options.AdaptiveClaimsEnabled ||
                     Stopwatch.GetTimestamp()>=nextClaimAt))
                {
                    try
                    {
                        var endpoint = options.LongPollingEnabled &&
                            capabilities.Select(c => c.HubId).Distinct().Count() == 1
                            ? "api/hub/workers/tasks:poll"
                            : "api/hub/workers/tasks:claim";
                        var leases=await client.PostAsync<HubLeasedTask[]>(endpoint,
                            new HubClaimRequest(_owner,capabilities.Select(c=>c.HubId).Distinct().ToArray(),capabilities,
                                Math.Min(16,concurrency-running.Count),workflows),ct);
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
                        if(options.AdaptiveClaimsEnabled && !options.LongPollingEnabled)
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
                        if(options.AdaptiveClaimsEnabled && !options.LongPollingEnabled)
                            nextClaimAt=Stopwatch.GetTimestamp()+
                                (long)(Math.Max(1,options.PollInterval.TotalSeconds)*Stopwatch.Frequency);
                        if(options.LongPollingEnabled)
                            await Task.Delay(TimeSpan.FromSeconds(1), ct);
                    }
                }
                if (options.LongPollingEnabled && running.Count < concurrency &&
                    capabilities.Select(c => c.HubId).Distinct().Count() == 1)
                    continue; // next server-held poll; no local jitter/backoff
                using var wait=CancellationTokenSource.CreateLinkedTokenSource(ct);
                var pollDelay=options.PollInterval;
                if(options.AdaptiveClaimsEnabled && running.Count<concurrency)
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
                    running.Count<concurrency && !hintPreempted)
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
