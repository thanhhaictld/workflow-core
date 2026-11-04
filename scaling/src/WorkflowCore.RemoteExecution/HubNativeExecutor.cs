using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using WorkflowCore.Hub;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Models.LifeCycleEvents;

namespace WorkflowCore.RemoteExecution;

internal sealed class HubExecutionSession(string hub)
{
    public string Hub { get; }=hub;
    public List<HubNativeDispatch> Dispatches { get; }=[];
    public List<HubNativeHistory> History { get; }=[];
    public WorkflowInstance Workflow { get; set; }=null!;
    public Dictionary<int,string> StepNames { get; set; }=[];
}
public sealed class HubExecutionSessionAccessor
{
    internal AsyncLocal<HubExecutionSession?> Current { get; }=new();
}

internal sealed class HubNativeLifeCyclePublisher(HubExecutionSessionAccessor sessions) : ILifeCycleEventPublisher
{
    public void Start() => throw new InvalidOperationException("Legacy background consumers cannot run in a Hub host");
    public void Stop() { }
    public void PublishNotification(LifeCycleEvent evt)
    {
        var session=sessions.Current.Value;
        if(session is null)return;
        var pointerId=evt switch { StepStarted s=>s.ExecutionPointerId,StepCompleted s=>s.ExecutionPointerId,
            WorkflowError e=>e.ExecutionPointerId,_=>"" };
        var pointer=session.Workflow.ExecutionPointers.FindById(pointerId);
        session.History.Add(new(evt.GetType().Name,pointerId,pointer?.StepId??0,pointer?.PredecessorId,
            pointer?.Scope.ToArray()??[],pointer?.RetryCount??0,
            JsonSerializer.SerializeToElement(evt,evt.GetType(),HubNativeExecutor.JsonOptions),
            pointer is null?null:session.StepNames.GetValueOrDefault(pointer.StepId)));
    }
}

public sealed record HubActivityReply(string DispatchKey, JsonElement? Outputs, JsonElement? Result, string? Error,
    JsonElement? ActivityData = null);

internal sealed class HubActivityMiddleware(HubNativeCatalog catalog,HubExecutionSessionAccessor sessions,
    HubSnapshotCodec codec) : IWorkflowStepMiddleware
{
    public Task<ExecutionResult> HandleAsync(IStepExecutionContext context,IStepBody body,WorkflowStepDelegate next)
    {
        var session=sessions.Current.Value;
        if(session is null)return next(); // Activity worker runs the actual body.
        var route=catalog.Route(session.Hub,context.Step.BodyType);
        if(route is null)return next();
        var pointer=context.ExecutionPointer;
        if(pointer.EventPublished && pointer.EventData is HubActivityReply reply)
        {
            // Consume the result in memory; outputs are persisted only by the fenced snapshot commit.
            pointer.EventPublished=false;pointer.EventData=null;
            if(reply.Error is not null)throw new InvalidOperationException(reply.Error);
            if(reply.Outputs is not JsonElement outputs || reply.Result is not JsonElement result)
                throw new InvalidOperationException("Incomplete typed activity result");
            HubTypedActivity.ApplyValues(body,codec.DecodeValue(outputs),route.Outputs);
            return Task.FromResult(codec.Decode<ExecutionResult>(result));
        }
        var dispatchKey=pointer.Id+":"+Guid.NewGuid().ToString("N");
        var properties=route.ReadInputs(body);
        // Policy fields stay at the top level; adapter context is explicitly separate.
        var payload=properties.EnumerateObject().ToDictionary(p=>p.Name,p=>(object?)p.Value.Clone());
        payload["__hubContext"]=new { dispatchKey,workflowId=context.Workflow.Id,
            definitionId=context.Workflow.WorkflowDefinitionId,version=context.Workflow.Version,
            pointer=codec.Encode(pointer) };
        payload["__hubValues"]=codec.EncodeValue(HubTypedActivity.ReadValues(body,route.Inputs));
        session.Dispatches.Add(new(dispatchKey,pointer.Id,pointer.StepId,route.Policy.ActivityType,
            route.Policy.Version,JsonSerializer.SerializeToElement(payload,HubNativeExecutor.JsonOptions)));
        pointer.ExtensionAttributes["hubDispatchKey"]=dispatchKey;
        return Task.FromResult(ExecutionResult.WaitForEvent("__hub_typed_activity",dispatchKey,DateTime.UtcNow));
    }
}

/// <summary>One native executor iteration per durable commit. Workflow code and
/// expressions stay in the application host; only the snapshot and effects travel to Hub.</summary>
public sealed class HubNativeExecutor(IServiceProvider services,HubNativeCatalog catalog,
    HubSnapshotCodec codec,HubExecutionSessionAccessor sessions,IDateTimeProvider clock)
{
    internal static JsonSerializerOptions JsonOptions { get; }=new(JsonSerializerDefaults.Web);

    public async Task<HubNativeCommit> ExecuteAsync(HubNativeRuntime runtime,HubLeasedTask lease,
        string owner,CancellationToken ct=default)
    {
        if(runtime.ProtocolVersion!=1)throw new NotSupportedException("Unknown Hub runtime protocol");
        var definition=catalog.Find(runtime.HubId,runtime.DefinitionId,runtime.Version);
        using var scope=services.CreateScope();
        var sp=scope.ServiceProvider;
        WorkflowInstance workflow;
        if(runtime.Snapshot is JsonElement snapshot)workflow=codec.Decode<WorkflowInstance>(snapshot);
        else
        {
            workflow=new(){Id=runtime.Id.ToString(),WorkflowDefinitionId=runtime.DefinitionId,Version=runtime.Version,
                CreateTime=runtime.CreatedAt,Status=WorkflowStatus.Runnable,NextExecution=0,Description=definition.Description,
                Data=HubSnapshotCodec.IsEncodedValue(runtime.Input) ? codec.DecodeValue(runtime.Input) :
                    definition.DataType==typeof(object)?codec.Decode<object>(runtime.Input):runtime.Input.Deserialize(definition.DataType,JsonOptions)};
            if(workflow.Data is null || !definition.DataType.IsInstanceOfType(workflow.Data))
                throw new InvalidOperationException("Input data type does not match workflow definition");
            workflow.ExecutionPointers.Add(sp.GetRequiredService<IExecutionPointerFactory>().BuildGenesisPointer(definition));
        }
        if(workflow.Id!=runtime.Id.ToString() || workflow.WorkflowDefinitionId!=runtime.DefinitionId || workflow.Version!=runtime.Version)
            throw new InvalidOperationException("Snapshot definition identity mismatch");
        workflow.Status=Enum.Parse<WorkflowStatus>(runtime.Status);
        var session=new HubExecutionSession(runtime.HubId){Workflow=workflow};
        session.StepNames=definition.Steps.ToDictionary(s=>s.Id,s=>catalog.Route(runtime.HubId,s.BodyType)?.Policy.ActivityType??s.Name??s.BodyType.Name);
        var before=workflow.ExecutionPointers.ToDictionary(p=>p.Id,p=>p.Status);
        sessions.Current.Value=session;
        try
        {
            if(runtime.Snapshot is null)
            {
                await sp.GetRequiredService<IWorkflowMiddlewareRunner>().RunPreMiddleware(workflow,definition);
                var genesis=workflow.ExecutionPointers.FirstOrDefault();
                session.History.Add(new("WorkflowInitialized",genesis?.Id??"",genesis?.StepId??0,null,[],0));
            }
            var consumed=new List<long>();
            foreach(var message in runtime.Inbox)
            {
                var pointer=workflow.ExecutionPointers.FindById(message.PointerId);
                if(pointer is not null && pointer.Status==PointerStatus.WaitingForEvent && !pointer.EventPublished)
                {
                    if(message.Kind=="Activity")
                    {
                        var reply=message.Payload.Deserialize<HubActivityReply>(JsonOptions)
                            ?? throw new InvalidOperationException("Empty activity result");
                        if(pointer.EventName==Event.EventTypeActivity)
                        {
                            pointer.EventData=new ActivityResult{Status=reply.Error is null?ActivityResult.StatusType.Success:ActivityResult.StatusType.Fail,
                                Data=reply.Error is not null?reply.Error:reply.ActivityData is JsonElement data?codec.DecodeValue(data):
                                    reply.Outputs is JsonElement result?codec.DecodeValue(result):null};
                            pointer.EventPublished=true;pointer.Active=true;
                        }
                        else if(pointer.ExtensionAttributes.TryGetValue("hubDispatchKey",out var expected) && expected.ToString()==reply.DispatchKey)
                        { pointer.EventData=reply;pointer.EventPublished=true;pointer.Active=true; }
                    }
                    else if(message.Kind=="Event")
                    {
                        pointer.EventData=HubSnapshotCodec.IsEncodedValue(message.Payload)?codec.DecodeValue(message.Payload):
                            message.Payload.ValueKind==JsonValueKind.Null?null:codec.Decode<object>(message.Payload);
                        pointer.EventPublished=true;pointer.Active=true;
                    }
                }
                consumed.Add(message.Id);
                session.History.Add(new("InboxConsumed",message.PointerId,pointer?.StepId??0,pointer?.PredecessorId,
                    pointer?.Scope.ToArray()??[],pointer?.RetryCount??0));
            }
            WorkflowExecutorResult? execution=null;
            // Persist genesis before running any user step, matching native
            // creation semantics and keeping its id stable after a lost commit.
            if(workflow.Status==WorkflowStatus.Runnable && runtime.Snapshot is not null)
                execution=await sp.GetRequiredService<IWorkflowExecutor>().Execute(workflow,ct);
            ct.ThrowIfCancellationRequested();
            foreach(var pointer in workflow.ExecutionPointers)
                if(!before.TryGetValue(pointer.Id,out var previous) || previous!=pointer.Status)
                    session.History.Add(new("PointerTransition",pointer.Id,pointer.StepId,pointer.PredecessorId,
                        pointer.Scope.ToArray(),pointer.RetryCount,JsonSerializer.SerializeToElement(new{
                            status=pointer.Status.ToString(),pointer.Active,pointer.SleepUntil},JsonOptions),session.StepNames[pointer.StepId]));
            // Native Activity primitives produce an activity subscription rather than a typed dispatch.
            // Capture subscription data from this iteration before it leaves the native executor.
            var subscriptions=new List<HubNativeSubscription>();
            foreach(var subscription in execution?.Subscriptions ?? [])
            {
                var pointer=workflow.ExecutionPointers.FindById(subscription.ExecutionPointerId);
                if(pointer.Status!=PointerStatus.WaitingForEvent || pointer.EndTime is not null)continue;
                pointer.ExtensionAttributes["hubEventAsOf"]=subscription.SubscribeAsOf.ToUniversalTime().Ticks.ToString();
                if(subscription.EventName!=Event.EventTypeActivity)continue;
                var route=catalog.Activities.SingleOrDefault(a=>a.Policy.HubId==runtime.HubId && a.Policy.ActivityType==subscription.EventKey)
                    ?? throw new InvalidOperationException("Native Activity name must have an activity registration");
                var key=pointer.Id+":"+Guid.NewGuid().ToString("N");
                pointer.ExtensionAttributes["hubDispatchKey"]=key;
                var parameters=route.ParametersProperty is null?subscription.SubscriptionData:
                    new Dictionary<string,object?>{[HubTypedActivity.PropertyName(route.ParametersProperty)]=subscription.SubscriptionData};
                var input=parameters is null?JsonSerializer.SerializeToElement(new{},JsonOptions):
                    JsonSerializer.SerializeToElement(parameters,JsonOptions);
                if(input.ValueKind!=JsonValueKind.Object)throw new InvalidOperationException("Native Activity parameters must be an object with declared input properties");
                var payload=input.EnumerateObject().ToDictionary(p=>p.Name,p=>(object?)p.Value.Clone());
                payload["__hubContext"]=new{dispatchKey=key,workflowId=workflow.Id,definitionId=workflow.WorkflowDefinitionId,
                    version=workflow.Version,pointer=codec.Encode(pointer)};
                var values=route.Inputs.ToDictionary(HubTypedActivity.PropertyName,p=>
                {
                    if(p==route.ParametersProperty)return subscription.SubscriptionData;
                    if(subscription.SubscriptionData is System.Collections.IDictionary dictionary)
                    {
                        if(dictionary.Contains(HubTypedActivity.PropertyName(p)))return dictionary[HubTypedActivity.PropertyName(p)];
                        if(dictionary.Contains(p.Name))return dictionary[p.Name];
                    }
                    var source=subscription.SubscriptionData?.GetType().GetProperty(p.Name,
                        System.Reflection.BindingFlags.Public|System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.IgnoreCase);
                    return source is not null?source.GetValue(subscription.SubscriptionData):
                        input.GetProperty(HubTypedActivity.PropertyName(p)).Deserialize(p.PropertyType,JsonOptions);
                });
                payload["__hubValues"]=codec.EncodeValue(values);
                session.Dispatches.Add(new(key,pointer.Id,pointer.StepId,route.Policy.ActivityType,route.Policy.Version,
                    JsonSerializer.SerializeToElement(payload,JsonOptions),subscription.SubscribeAsOf>clock.UtcNow?subscription.SubscribeAsOf:null));
            }
            foreach(var pointer in workflow.ExecutionPointers.Where(p=>p.Status==PointerStatus.WaitingForEvent && !p.EventPublished))
            {
                if(pointer.EventName=="__hub_typed_activity" || pointer.EventName==Event.EventTypeActivity)continue;
                var asOf=pointer.ExtensionAttributes.TryGetValue("hubEventAsOf",out var ticks)
                    ?new DateTime(long.Parse(ticks.ToString()!),DateTimeKind.Utc):DateTime.SpecifyKind(DateTime.MinValue,DateTimeKind.Utc);
                subscriptions.Add(new(pointer.Id,pointer.EventName,pointer.EventKey,asOf));
            }
            var activeDispatches=workflow.ExecutionPointers.Where(p=>p.Status==PointerStatus.WaitingForEvent && p.EndTime is null)
                .Where(p=>p.ExtensionAttributes.ContainsKey("hubDispatchKey"))
                .Select(p=>p.ExtensionAttributes["hubDispatchKey"].ToString()!).ToArray();
            var timerPointer=workflow.ExecutionPointers.FirstOrDefault(p=>p.Active && p.SleepUntil?.Ticks==workflow.NextExecution);
            return new(1,owner,lease.Epoch,runtime.Revision,lease.Id,codec.Encode(workflow),workflow.Status.ToString(),
                workflow.Status==WorkflowStatus.Runnable && workflow.NextExecution is long next && next<=clock.UtcNow.Ticks,
                workflow.NextExecution is >0 ? new DateTime(workflow.NextExecution.Value,DateTimeKind.Utc):null,
                consumed.ToArray(),subscriptions.ToArray(),session.Dispatches.Where(d=>activeDispatches.Contains(d.DispatchKey)).ToArray(),
                session.History.ToArray(),activeDispatches,timerPointer is null?null:new(timerPointer.Id,timerPointer.StepId));
        }
        finally { sessions.Current.Value=null; }
    }

    public async Task<JsonElement> ExecuteActivityAsync(HubLeasedTask task,CancellationToken ct=default)
    {
        var route=catalog.Activities.Single(a=>a.Policy.HubId==task.HubId && a.Policy.ActivityType==task.TaskType &&
            (a.Policy.Version==task.Version || task.Version==1 && services.GetRequiredService<HubRuntimeOptions>().SequentialVersionOneCompatibility));
        if(!task.Payload.TryGetProperty("__hubContext",out _))
        {
            using var legacyScope=services.CreateScope();
            var legacyBody=(IStepBody)legacyScope.ServiceProvider.GetRequiredService(route.StepType);
            HubTypedActivity.Apply(legacyBody,task.Payload,route.Inputs);
            await legacyBody.RunAsync(new StepExecutionContext{CancellationToken=ct,Workflow=new(){Id=task.WorkflowId?.ToString()},
                ExecutionPointer=new(){Id=task.Id.ToString()}});
            return route.ReadSequentialOutputs(legacyBody);
        }
        var metadata=task.Payload.GetProperty("__hubContext");
        var key=metadata.GetProperty("dispatchKey").GetString()!;
        try
        {
            var pointer=codec.Decode<ExecutionPointer>(metadata.GetProperty("pointer"));
            var context=new StepExecutionContext{Workflow=new WorkflowInstance{Id=metadata.GetProperty("workflowId").GetString(),
                WorkflowDefinitionId=metadata.GetProperty("definitionId").GetString()??throw new InvalidOperationException("Missing activity definition identity"),
                Version=metadata.GetProperty("version").GetInt32()},
                ExecutionPointer=pointer,PersistenceData=pointer.PersistenceData,Item=pointer.ContextItem,CancellationToken=ct};
            context.Step=catalog.Step(task.HubId,context.Workflow.WorkflowDefinitionId,context.Workflow.Version,pointer.StepId)
                ?? new ActivityStep(route.StepType){Id=pointer.StepId,Name=pointer.StepName};
            using var scope=services.GetRequiredService<IScopeProvider>().CreateScope(context);
            var body=(IStepBody)scope.ServiceProvider.GetRequiredService(route.StepType);
            HubTypedActivity.ApplyValues(body,codec.DecodeValue(task.Payload.GetProperty("__hubValues")),route.Inputs);
            var result=await scope.ServiceProvider.GetRequiredService<IStepExecutor>().ExecuteStep(context,body);
            ct.ThrowIfCancellationRequested();
            var activityData=route.ResultProperty is null?null:(JsonElement?)codec.EncodeValue(route.ResultProperty.GetValue(body));
            return JsonSerializer.SerializeToElement(new HubActivityReply(key,codec.EncodeValue(HubTypedActivity.ReadValues(body,route.Outputs)),codec.Encode(result),null,activityData),JsonOptions);
        }
        catch(OperationCanceledException) when(ct.IsCancellationRequested){throw;}
        catch(Exception ex){return JsonSerializer.SerializeToElement(new HubActivityReply(key,null,null,ex.Message),JsonOptions);}
    }

    private sealed class ActivityStep(Type bodyType) : WorkflowStep
    { public override Type BodyType=>bodyType; }
}
