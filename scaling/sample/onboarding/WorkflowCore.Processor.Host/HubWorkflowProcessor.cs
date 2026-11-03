using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Client;
using WorkflowCore.RemoteExecution;

namespace WorkflowCore.Processor.Host;

public sealed class DefinitionPublisher(HubRestClient client,
    RemoteWorkflowRegistry registry, IHostApplicationLifetime lifetime,
    ILogger<DefinitionPublisher> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        foreach(var definition in registry.All())
        {
            await client.RegisterAsync("api/hub/definitions",
                RemoteWorkflowRegistry.Metadata(definition),ct);
            logger.LogInformation("Registered workflow {Hub}/{Definition} v{Version}",
                definition.HubId,definition.DefinitionId,definition.Version);
        }
        if(Environment.GetCommandLineArgs().Contains("--register-only",StringComparer.Ordinal))
            lifetime.StopApplication();
    }
    public Task StopAsync(CancellationToken ct)=>Task.CompletedTask;
}

/// <summary>
/// REST/SSE-only workflow processor. Only this application executes the
/// code-first workflow decisions. The Hub stores only state and commands.
/// </summary>
public sealed class HubWorkflowProcessor(HubRestClient client,
    RemoteWorkflowRegistry registry, ILogger<HubWorkflowProcessor> logger)
    : BackgroundService
{
    private readonly string _owner=Environment.MachineName+":processor:"+Guid.NewGuid().ToString("N");
    private readonly SemaphoreSlim _gate=new(1,1);
    private readonly int _concurrency=int.TryParse(Environment.GetEnvironmentVariable("WORKERS"),
        out var count)?Math.Clamp(count,1,32):8;
    private int _running;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var hubs=string.Join(",",registry.All().Select(x=>x.HubId).Distinct());
        while(!ct.IsCancellationRequested)
        {
            try
            {
                using var req=new HttpRequestMessage(HttpMethod.Get,
                    "api/hub/workers/events?hub="+Uri.EscapeDataString(hubs));
                using var response=await client.Http.SendAsync(req,
                    HttpCompletionOption.ResponseHeadersRead,ct);
                response.EnsureSuccessStatusCode();
                await ClaimAsync(ct); // Recover from a lost SSE notification.
                await using var stream=await response.Content.ReadAsStreamAsync(ct);
                using var reader=new StreamReader(stream);
                var keepalives=0;
                while(!ct.IsCancellationRequested)
                {
                    var line=await reader.ReadLineAsync(ct);
                    if(line is null)break;
                    if(line.StartsWith("event: tasks.available",StringComparison.Ordinal) ||
                       line.StartsWith(": keepalive",StringComparison.Ordinal)&&++keepalives%2==0)
                        await ClaimAsync(ct);
                }
            }
            catch(OperationCanceledException)when(ct.IsCancellationRequested){break;}
            catch(Exception ex){logger.LogWarning(ex,"Processor stream lost; retrying");}
            try{await Task.Delay(TimeSpan.FromSeconds(2),ct);}
            catch(OperationCanceledException){break;}
        }
    }

    private async Task ClaimAsync(CancellationToken ct)
    {
        if(!await _gate.WaitAsync(0,ct))return;
        try
        {
            var capacity=_concurrency-Volatile.Read(ref _running);
            if(capacity<=0)return;
            var hubs=registry.All().Select(x=>x.HubId).Distinct().ToArray();
            var claim=new HubClaimRequest(_owner,hubs,
                hubs.Select(h=>new HubCapability(h,"__workflow__",1)).ToArray(),
                Math.Min(16,capacity));
            var leases=await client.PostAsync<HubLeasedTask[]>(
                "api/hub/workers/tasks:claim",claim,ct);
            foreach(var lease in leases)
            {
                Interlocked.Increment(ref _running);
                _=ExecuteOneAsync(lease,ct);
            }
        }
        finally{_gate.Release();}
    }

    private async Task ExecuteOneAsync(HubLeasedTask task,CancellationToken ct)
    {
        try
        {
            if(task.WorkflowId is not Guid id)
                throw new InvalidOperationException("Missing workflow ID");
            var runtime=await client.GetAsync<HubWorkflowRuntime>(
                $"api/hub/workflows/{id}/runtime",ct);
            if(runtime.State is "Failed" or "Complete")return;
            var definition=registry.Find(runtime.HubId,runtime.DefinitionId,runtime.Version)
                ?? throw new InvalidOperationException("Unrecognized code-first workflow version");
            if(definition.Steps.Count!=runtime.StepCount)
                throw new InvalidOperationException("Workflow definition changed while running");

            HubTransitionRequest decision;
            if(runtime.NextStep>=definition.Steps.Count)
                decision=new(_owner,task.Epoch,runtime.Revision,task.Id,"CompleteWorkflow");
            else
            {
                var step=definition.Steps[runtime.NextStep];
                decision=step.Kind=="Delay"
                    ? new(_owner,task.Epoch,runtime.Revision,task.Id,"ScheduleTimer",
                        TimerKey:step.ActivityType,
                        DelayMilliseconds:step.DelayMilliseconds)
                    : new(_owner,task.Epoch,runtime.Revision,task.Id,"ScheduleActivity",
                        ActivityType:step.ActivityType,ActivityVersion:step.Version,
                        ActivityInput:runtime.Input);
            }
            using var response=await client.Http.PostAsJsonAsync(
                $"api/hub/workflows/{id}/transitions",decision,ct);
            if(response.StatusCode==HttpStatusCode.Conflict)
            {
                logger.LogWarning("Processor fenced/stale workflow {Workflow}",id);
                return;
            }
            response.EnsureSuccessStatusCode();
        }
        catch(OperationCanceledException)when(ct.IsCancellationRequested){}
        catch(Exception ex)
        {
            logger.LogError(ex,"Failed remote workflow task {Task}",task.Id);
            try
            {
                using var response=await client.Http.PostAsJsonAsync(
                    $"api/hub/tasks/{task.Id}/fail",
                    new HubTaskFailureRequest(_owner,task.Epoch,ex.Message),ct);
                if(!response.IsSuccessStatusCode)
                    logger.LogWarning("Task failure fencing returned {Status}",response.StatusCode);
            }
            catch(Exception report)when(!ct.IsCancellationRequested)
            { logger.LogWarning(report,"Processor failure report not accepted"); }
        }
        finally
        {
            Interlocked.Decrement(ref _running);
            if(!ct.IsCancellationRequested)
            {
                try{await ClaimAsync(ct);}
                catch(Exception ex){logger.LogWarning(ex,"Follow-up workflow task claim failed");}
            }
        }
    }
}
