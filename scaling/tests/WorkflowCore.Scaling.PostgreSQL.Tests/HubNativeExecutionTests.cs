using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using WorkflowCore.Hub;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeExecutionTests
{
    public sealed class PointerProbe { public List<string> Attempts { get; }=[]; }
    public sealed class OrdinaryStep(PointerProbe probe) : StepBody
    {
        public override ExecutionResult Run(IStepExecutionContext context)
        {probe.Attempts.Add(context.ExecutionPointer.Id);((Data)context.Workflow.Data).Value++;return ExecutionResult.Next();}
    }
    public sealed class OrdinaryWorkflow : IWorkflow<Data>
    {
        public string Id=>"ordinary";public int Version=>1;
        public void Build(IWorkflowBuilder<Data> b)=>b.StartWith<OrdinaryStep>();
    }
    [Fact]
    public async Task InitialPointerIsPersistableBeforeAnyStepAndSurvivesAnUncommittedAttempt()
    {
        var probe=new PointerProbe();var services=new ServiceCollection().AddLogging();
        services.AddSingleton(probe);services.AddTransient<OrdinaryStep>();
        services.AddHubWorkflowRuntime().AddWorkflow<OrdinaryWorkflow,Data>("tests");
        using var provider=services.BuildServiceProvider();
        var executor=provider.GetRequiredService<HubNativeExecutor>();
        var id=Guid.NewGuid();var empty=JsonSerializer.SerializeToElement(new{});
        var runtime=new HubNativeRuntime(1,id,"tests","ordinary",1,0,"Runnable",empty,null,[],DateTime.UtcNow);
        var lease=new HubLeasedTask(Guid.NewGuid(),"tests",0,"__native_workflow__",1,id,empty,1,1,DateTime.UtcNow.AddSeconds(30));
        var initialized=await executor.ExecuteAsync(runtime,lease,"worker");
        Assert.Empty(probe.Attempts);
        runtime=runtime with{Snapshot=initialized.Snapshot,Revision=1};
        // Lose the first execution's commit, then retry from the last durable snapshot.
        await executor.ExecuteAsync(runtime,lease,"worker");
        var completed=await executor.ExecuteAsync(runtime,lease with{Epoch=2},"worker");
        Assert.Equal(2,probe.Attempts.Count);Assert.Single(probe.Attempts.Distinct());
        var result=provider.GetRequiredService<HubSnapshotCodec>().Decode<WorkflowInstance>(completed.Snapshot);
        Assert.Equal(1,((Data)result.Data).Value);Assert.Equal(WorkflowStatus.Complete,result.Status);
    }

    public sealed class Unsupported : IWorkflow<object>
    {
        public string Id=>"unsupported";
        public int Version=>1;
        public void Build(IWorkflowBuilder<object> b)=>b.StartWith<WorkflowCore.Primitives.SubWorkflowStepBody>();
    }
    public sealed class ObjectWorkflow : IWorkflow<object>
    {
        public string Id=>"object";
        public int Version=>1;
        public void Build(IWorkflowBuilder<object> b)=>b.StartWith(c=>ExecutionResult.Next());
    }

    [Fact]
    public void UnimplementedSubWorkflowIsRejectedAtRegistration()
    {
        var services=new ServiceCollection().AddLogging();
        services.AddHubWorkflowRuntime().AddWorkflow<Unsupported,object>("tests");
        using var provider=services.BuildServiceProvider();
        Assert.Throws<NotSupportedException>(()=>provider.GetRequiredService<HubNativeCatalog>());
    }

    [Fact]
    public void SnapshotCodecPreservesBoxedValuesAndRejectsUnregisteredRuntimeTypes()
    {
        var services=new ServiceCollection().AddLogging();
        services.AddHubWorkflowRuntime().AddWorkflow<ObjectWorkflow,object>("tests");
        using var provider=services.BuildServiceProvider();
        var codec=provider.GetRequiredService<HubSnapshotCodec>();
        Assert.IsType<int>(codec.DecodeValue(codec.EncodeValue(42)));
        Assert.IsType<int[]>(codec.DecodeValue(codec.EncodeValue(new[]{1,2,3})));
        Assert.IsType<JsonElement>(codec.DecodeValue(codec.EncodeValue(JsonSerializer.SerializeToElement(new{number=1}))));
        Assert.Throws<Newtonsoft.Json.JsonSerializationException>(()=>codec.EncodeValue(typeof(string)));
        var malicious=JsonSerializer.SerializeToElement(new Dictionary<string,object>{["$hubValueFormat"]="v1",
            ["NativeValue"]=new{clrAssembly="System.Diagnostics.Process",clrType="System.Diagnostics.Process",value=new{}}});
        Assert.Throws<Newtonsoft.Json.JsonSerializationException>(()=>codec.DecodeValue(malicious));
    }
    public sealed class Clock : IDateTimeProvider
    {
        public DateTime UtcNow { get; set; } = new(2026,11,4,0,0,0,DateTimeKind.Utc);
        public DateTime Now => UtcNow;
    }
    public sealed class Data
    {
        public string NativeValue { get; set; }="a domain property, not a transport envelope";
        public int Value { get; set; }
        public int Other { get; set; }
        public bool Compensated { get; set; }
        public string? Event { get; set; }
        public Signal? Signal { get; set; }
    }
    public sealed class Signal { public string Text { get; set; }="typed"; }
    public sealed class Increment : StepBody
    {
        public int Value { get; set; }
        public override ExecutionResult Run(IStepExecutionContext context) { Value++;return ExecutionResult.Next(); }
    }
    public sealed class FailOnce : StepBody
    {
        public override ExecutionResult Run(IStepExecutionContext context)
        {
            if(context.ExecutionPointer.RetryCount==0)throw new InvalidOperationException("retry me");
            ((Data)context.Workflow.Data).Value++;
            return ExecutionResult.Next();
        }
    }
    public sealed class Scenarios(string scenario) : IWorkflow<Data>
    {
        public string Id => "semantics-"+scenario;
        public int Version => 2;
        public void Build(IWorkflowBuilder<Data> b)
        {
            switch(scenario)
            {
                case "mapping":
                    b.StartWith<Increment>().Input(s=>s.Value,d=>d.Value).Output(d=>d.Value,s=>s.Value)
                        .Then(c=>{((Data)c.Workflow.Data).Other=7;return ExecutionResult.Next();});break;
                case "branch-loop":
                    b.StartWith(c=>ExecutionResult.Next()).If(d=>d.Value==0).Do(t=>t
                        .StartWith<Increment>().Input(s=>s.Value,d=>d.Value).Output(d=>d.Value,s=>s.Value))
                        .While(d=>d.Value<3).Do(t=>t.StartWith<Increment>()
                            .Input(s=>s.Value,d=>d.Value).Output(d=>d.Value,s=>s.Value));break;
                case "parallel":
                    b.StartWith(c=>ExecutionResult.Next()).Parallel()
                        .Do(t=>t.StartWith(c=>{((Data)c.Workflow.Data).Value=3;return ExecutionResult.Next();}))
                        .Do(t=>t.StartWith(c=>{((Data)c.Workflow.Data).Other=9;return ExecutionResult.Next();})).Join();break;
                case "foreach":
                    b.StartWith(c=>ExecutionResult.Next()).ForEach(d=>new[]{1,2,3}).Do(t=>t.StartWith(c=>
                        {((Data)c.Workflow.Data).Value+=(int)c.Item;return ExecutionResult.Next();}));break;
                case "delay":
                    b.StartWith(c=>ExecutionResult.Next()).Delay(d=>TimeSpan.FromMilliseconds(3))
                        .Then(c=>{((Data)c.Workflow.Data).Value=1;return ExecutionResult.Next();});break;
                case "schedule":
                    b.StartWith(c=>ExecutionResult.Next()).Schedule(d=>TimeSpan.FromMilliseconds(3)).Do(t=>t
                        .StartWith(c=>{((Data)c.Workflow.Data).Value=1;return ExecutionResult.Next();}));break;
                case "recurrence":
                    b.StartWith(c=>ExecutionResult.Next()).Recur(d=>TimeSpan.FromMilliseconds(3),d=>d.Value>=3).Do(t=>t
                        .StartWith(c=>{((Data)c.Workflow.Data).Value++;return ExecutionResult.Next();}));break;
                case "retry":
                    b.StartWith<FailOnce>().OnError(WorkflowErrorHandling.Retry,TimeSpan.FromMilliseconds(3));break;
                case "compensation":
                    b.StartWith(c=>ExecutionResult.Next()).Saga(t=>t
                        .StartWith(c=>{((Data)c.Workflow.Data).Value++;return ExecutionResult.Next();})
                            .CompensateWith(c=>{((Data)c.Workflow.Data).Compensated=true;})
                        .Then(c=>throw new InvalidOperationException("compensate")));break;
                case "event":
                case "null-event":
                    b.StartWith(c=>ExecutionResult.Next()).WaitFor("approved",d=>"key",d=>DateTime.UnixEpoch)
                        .Output(d=>d.Event,s=>s.EventData);break;
                case "typed-event":
                    b.StartWith(c=>ExecutionResult.Next()).WaitFor("approved",d=>"key",d=>DateTime.UnixEpoch)
                        .Output(d=>d.Signal,s=>s.EventData);break;
                case "cancel":
                    b.StartWith(c=>ExecutionResult.Next()).Parallel()
                        .Do(t=>t.StartWith(c=>ExecutionResult.Next()).WaitFor("unused",d=>"key",null,d=>d.Value>0))
                        .Do(t=>t.StartWith(c=>{((Data)c.Workflow.Data).Value=1;return ExecutionResult.Next();})).Join();break;
                default:throw new ArgumentException(scenario);
            }
        }
    }

    [Theory]
    [InlineData("mapping")]
    [InlineData("branch-loop")]
    [InlineData("parallel")]
    [InlineData("foreach")]
    [InlineData("delay")]
    [InlineData("schedule")]
    [InlineData("recurrence")]
    [InlineData("retry")]
    [InlineData("compensation")]
    [InlineData("event")]
    [InlineData("typed-event")]
    [InlineData("null-event")]
    [InlineData("cancel")]
    public async Task NativeSnapshotsMatchUpstreamDataAndPointerStructure(string scenario)
    {
        var workflow=new Scenarios(scenario);
        var services=new ServiceCollection().AddLogging();
        services.AddSingleton(workflow);
        services.AddHubWorkflowRuntime().AddWorkflow<Scenarios,Data>("tests");
        var clock=new Clock();services.AddSingleton<IDateTimeProvider>(clock);
        using var native=services.BuildServiceProvider();
        var catalog=native.GetRequiredService<HubNativeCatalog>();
        var codec=native.GetRequiredService<HubSnapshotCodec>();
        var id=Guid.NewGuid();
        var runtime=new HubNativeRuntime(1,id,"tests",workflow.Id,2,0,"Runnable",
            JsonSerializer.SerializeToElement(new Data()),null,[],DateTime.UtcNow);
        WorkflowInstance? actual=null;
        for(var iteration=0;iteration<200;iteration++)
        {
            var task=new HubLeasedTask(Guid.NewGuid(),"tests",0,"__native_workflow__",1,id,JsonSerializer.SerializeToElement(new{}),1,1,DateTime.UtcNow.AddMinutes(1));
            var decision=await native.GetRequiredService<HubNativeExecutor>().ExecuteAsync(runtime,task,"tests");
            actual=codec.Decode<WorkflowInstance>(decision.Snapshot);
            if(actual.Status==WorkflowStatus.Complete)break;
            var inbox=decision.Subscriptions.Select((s,i)=>new HubNativeMessage(iteration*100+i,s.PointerId,"Event",
                scenario=="typed-event"?codec.EncodeValue(new Signal()):JsonSerializer.SerializeToElement(scenario=="null-event"?null:"accepted"))).ToArray();
            runtime=runtime with{Revision=iteration+1,Snapshot=decision.Snapshot,Inbox=inbox};
            if(!decision.Runnable && decision.WakeAt is DateTime deadline)
            {
                Assert.True(deadline-clock.UtcNow<TimeSpan.FromSeconds(1),JsonSerializer.Serialize(decision.History));
                clock.UtcNow=deadline.AddTicks(1);
            }
        }
        Assert.Equal(WorkflowStatus.Complete,actual!.Status);
        // Execute the identical definition with the upstream executor, without the adapter or serialization.
        var upstreamServices=new ServiceCollection().AddLogging();upstreamServices.AddWorkflow();
        var upstreamClock=new Clock();upstreamServices.AddSingleton<IDateTimeProvider>(upstreamClock);
        using var upstream=upstreamServices.BuildServiceProvider();
        var registry=upstream.GetRequiredService<IWorkflowRegistry>();registry.RegisterWorkflow(workflow);
        var definition=registry.GetDefinition(workflow.Id,2);
        var expected=new WorkflowInstance{Id=Guid.NewGuid().ToString(),WorkflowDefinitionId=workflow.Id,Version=2,
            Data=new Data(),CreateTime=DateTime.UtcNow,Status=WorkflowStatus.Runnable};
        expected.ExecutionPointers.Add(upstream.GetRequiredService<IExecutionPointerFactory>().BuildGenesisPointer(definition));
        for(var i=0;i<200 && expected.Status!=WorkflowStatus.Complete;i++)
        {
            var outcome=await upstream.GetRequiredService<IWorkflowExecutor>().Execute(expected);
            foreach(var subscription in outcome.Subscriptions)
            {
                var pointer=expected.ExecutionPointers.FindById(subscription.ExecutionPointerId);
                pointer.EventPublished=true;pointer.EventData=scenario=="typed-event"?new Signal():scenario=="null-event"?null:"accepted";pointer.Active=true;
            }
            if(expected.NextExecution is >0 && expected.NextExecution>=upstreamClock.UtcNow.Ticks)
            {
                Assert.True(expected.NextExecution-upstreamClock.UtcNow.Ticks<TimeSpan.FromSeconds(1).Ticks,JsonSerializer.Serialize(outcome.Errors));
                upstreamClock.UtcNow=new DateTime(expected.NextExecution.Value+1,DateTimeKind.Utc);
            }
        }
        Assert.Equal(WorkflowStatus.Complete,expected.Status);
        Assert.Equal(JsonSerializer.Serialize((Data)expected.Data),JsonSerializer.Serialize((Data)actual.Data));
        string[] Structure(WorkflowInstance instance)=>instance.ExecutionPointers
            .Select(p=>$"{p.StepId}:{p.Status}:{p.Active}:{p.RetryCount}:{p.Scope.Count}:{p.Children.Count}").Order().ToArray();
        Assert.Equal(Structure(expected),Structure(actual));
    }

    [Fact]
    public void NativeHostRegistersNoLegacyBackgroundConsumers()
    {
        var services=new ServiceCollection().AddLogging();services.AddHubWorkflowRuntime();
        using var provider=services.BuildServiceProvider();
        Assert.Empty(provider.GetServices<IBackgroundTask>());
    }
}
