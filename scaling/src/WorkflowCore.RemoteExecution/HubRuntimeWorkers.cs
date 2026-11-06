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
    protected abstract Task<object> Execute(HubLeasedTask task,string owner,CancellationToken ct);
    protected abstract string ResultPath(HubLeasedTask task);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        if(capabilities.Length==0)return;
        await startup.Published.WaitAsync(ct);
        var running=new List<Task>();
        var listener=Listen(ct);
        try
        {
            while(!ct.IsCancellationRequested)
            {
                running.RemoveAll(t=>t.IsCompleted);
                if(running.Count<concurrency)
                {
                    try
                    {
                        var leases=await client.PostAsync<HubLeasedTask[]>("api/hub/workers/tasks:claim",
                            new HubClaimRequest(_owner,capabilities.Select(c=>c.HubId).Distinct().ToArray(),capabilities,
                                Math.Min(16,concurrency-running.Count),workflows),ct);
                        foreach(var lease in leases)running.Add(Run(lease,ct));
                        if(leases.Length>0)continue;
                    }
                    catch(OperationCanceledException)when(ct.IsCancellationRequested){break;}
                    catch(Exception ex){logger.LogWarning(ex,"Hub claim failed; retrying");}
                }
                using var wait=CancellationTokenSource.CreateLinkedTokenSource(ct);
                var poll=Task.Delay(options.PollInterval,wait.Token);
                var hint=_hints.Reader.WaitToReadAsync(wait.Token).AsTask();
                await Task.WhenAny(running.Append(poll).Append(hint));
                await wait.CancelAsync();
                while(_hints.Reader.TryRead(out _)) { }
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
        // A durable claim crosses an HTTP/task-queue boundary. The Hub returns
        // the W3C claim context so the worker and its completion HTTP call
        // belong to the same distributed trace (without retaining a live span).
        var hasParent=ActivityContext.TryParse(task.TraceParent,null,out var parent);
        using var span=hasParent
            ? HubTelemetry.Traces.StartActivity(
                isActivity?"hub.worker.activity.execute":"hub.worker.workflow.execute",
                ActivityKind.Consumer,parent)
            : HubTelemetry.Traces.StartActivity(
                isActivity?"hub.worker.activity.execute":"hub.worker.workflow.execute");
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
            var bodyStarted=Stopwatch.GetTimestamp();
            using(var bodySpan=HubTelemetry.Traces.StartActivity(
                isActivity?"hub.worker.activity.body":"hub.worker.workflow.body"))
            {
                bodySpan?.SetTag("activity.type",task.TaskType);
                bodySpan?.SetTag("workflow.id",task.WorkflowId?.ToString());
                bodySpan?.SetTag("task.id",task.Id.ToString());
                try { result=await Execute(task,_owner,execution.Token); }
                finally
                {
                    var seconds=Stopwatch.GetElapsedTime(bodyStarted).TotalSeconds;
                    bodySpan?.SetTag("duration.seconds",seconds);
                    if(isActivity) HubTelemetry.WorkerBodyDuration.Record(seconds,HubTelemetry.ActivityTag(task.TaskType));
                }
            }
            // Transport failures retry the frozen result, never the body or decisions.
            for(var attempt=0;;attempt=Math.Min(attempt+1,5))
            {
                try
                {
                    var httpStarted=Stopwatch.GetTimestamp();
                    using var completeSpan=HubTelemetry.Traces.StartActivity("hub.worker.completion_http");
                    completeSpan?.SetTag("retry.attempt",attempt);
                    completeSpan?.SetTag("activity.type",task.TaskType);
                    completeSpan?.SetTag("workflow.id",task.WorkflowId?.ToString());
                    completeSpan?.SetTag("task.id",task.Id.ToString());
                    HttpResponseMessage response;
                    try { response=await client.Http.PostAsJsonAsync(ResultPath(task),result,execution.Token); }
                    finally
                    {
                        var seconds=Stopwatch.GetElapsedTime(httpStarted).TotalSeconds;
                        completeSpan?.SetTag("duration.seconds",seconds);
                        if(isActivity) HubTelemetry.WorkerCompletionHttpDuration.Record(seconds,HubTelemetry.ActivityTag(task.TaskType));
                    }
                    using(response)
                    {
                    if(response.StatusCode==HttpStatusCode.Conflict){outcome="fenced";return;}
                    if((int)response.StatusCode>=500 || response.StatusCode is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests)
                        throw new HttpRequestException("Hub completion temporarily unavailable: "+response.StatusCode);
                    if(!response.IsSuccessStatusCode)
                        throw new InvalidOperationException("Hub rejected the completion contract: "+response.StatusCode);
                    outcome="success";return;
                    }
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
