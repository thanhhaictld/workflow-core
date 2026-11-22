using System.Net;
using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
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
    HubNativeExecutor executor,ILogger<HubNativeWorkflowWorker> logger,IServiceProvider services)
    : HubRuntimeWorker(client,logger,HubPushTaskKind.Workflow,
        catalog.Definitions.Where(d=>options.WorkerHub is null || d.HubId==options.WorkerHub).Select(d=>new HubCapability(d.HubId,"__native_workflow__",1))
            .Concat(options.SequentialVersionOneCompatibility
                ? (services.GetService<RemoteWorkflowRegistry>()?.All()??[]).Where(d=>options.WorkerHub is null || d.HubId==options.WorkerHub).Select(d=>new HubCapability(d.HubId,"__workflow__",1)) : [])
            .Distinct().ToArray(),
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
    HubNativeExecutor executor,ILogger<HubNativeActivityWorker> logger)
    : HubRuntimeWorker(client,logger,HubPushTaskKind.Activity,
        catalog.Activities.Where(a=>options.WorkerHub is null || a.Registration.HubId==options.WorkerHub).Select(a=>new HubCapability(a.Registration.HubId,a.Registration.ActivityType,a.Registration.Version))
            .Concat(options.SequentialVersionOneCompatibility ? catalog.Activities.Where(a=>options.WorkerHub is null || a.Registration.HubId==options.WorkerHub).Select(a=>new HubCapability(a.Registration.HubId,a.Registration.ActivityType,1)) : [])
            .Distinct().ToArray())
{
    protected override async Task<object> Execute(HubLeasedTask task,string owner,CancellationToken ct)=>
        new HubTaskResultRequest(owner,task.Epoch,await executor.ExecuteActivityAsync(task,ct));
    protected override string ResultPath(HubLeasedTask task)=>$"api/hub/tasks/{task.Id}/complete";
}

/// <summary>Executes a fenced assignment and commits or reports its outcome over REST.</summary>
internal abstract class HubRuntimeWorker(HubRestClient client, ILogger logger, HubPushTaskKind kind,
    HubCapability[] capabilities, HubWorkflowCapability[]? workflows = null)
{
    protected HubRestClient Client => client;

    public HubPushCapabilities[] Capabilities { get; } = capabilities.GroupBy(c => c.HubId)
        .Select(group => new HubPushCapabilities(group.Key, kind, group.ToArray(),
            workflows?.Where(w => w.HubId == group.Key).ToArray())).ToArray();

    protected abstract Task<object> Execute(HubLeasedTask task, string owner, CancellationToken ct);
    protected abstract string ResultPath(HubLeasedTask task);

    public async Task<bool> Run(HubLeasedTask task, string owner, CancellationToken shutdown)
    {
        if (task.LeaseUntil <= DateTime.UtcNow || shutdown.IsCancellationRequested) return false;
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
        var heartbeat=Heartbeat(task,owner,execution,heartbeatStop.Token);
        try
        {
            object result;
            HubExecutionObservation.Started(task);
            using (var bodySpan = HubTelemetry.Traces.StartActivity("hub.worker.execute.body"))
            {
                bodySpan?.SetTag("task.id", task.Id.ToString());
                bodySpan?.SetTag("workflow.id", task.WorkflowId?.ToString());
                var bodyStarted = Stopwatch.GetTimestamp();
                try { result = await Execute(task,owner,execution.Token); }
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
                    if(response.StatusCode==HttpStatusCode.Conflict){outcome="fenced";return false;}
                    if((int)response.StatusCode>=500 || response.StatusCode is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests)
                        throw new HttpRequestException("Hub completion temporarily unavailable: "+response.StatusCode);
                    if(!response.IsSuccessStatusCode)
                    {
                        var detail = await response.Content.ReadAsStringAsync(execution.Token);
                        throw new InvalidOperationException("Hub rejected the completion contract: "+response.StatusCode+" "+detail);
                    }
                    outcome="success";return true;
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
                        new HubTaskFailureRequest(owner,task.Epoch,ex.Message),execution.Token);
                    response.EnsureSuccessStatusCode();
                    return true;
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
        return false;
    }

    private async Task Heartbeat(HubLeasedTask task,string owner,CancellationTokenSource execution,CancellationToken ct)
    {
        using var timer=new PeriodicTimer(TimeSpan.FromSeconds(10));
        try
        {
            while(await timer.WaitForNextTickAsync(ct))
            {
                var renewalStarted=Stopwatch.GetTimestamp();
                using var response=await client.Http.PostAsJsonAsync($"api/hub/tasks/{task.Id}/heartbeat",
                    new HubTaskHeartbeatRequest(owner,task.Epoch),ct);
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
