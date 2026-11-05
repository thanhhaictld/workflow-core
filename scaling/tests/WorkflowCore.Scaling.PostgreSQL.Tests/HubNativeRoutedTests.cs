using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeRoutedTests
{
    public sealed class Data
    {
        public int Value { get; set; }=10;
        public int Other { get; set; }
        public int Tail { get; set; }
        public string Resource { get; set; }="shared";
        public bool Compensated { get; set; }
        public ResultData? Result { get; set; }
    }
    public sealed class ResultData { public int Value { get; set; } public object? Boxed { get; set; } }
    public sealed class Add : StepBody
    {
        public int Value { get; set; }
        public object? Boxed { get; set; }
        public string Resource { get; set; }="";
        public ResultData Result { get; set; }=new();
        public override ExecutionResult Run(IStepExecutionContext context)
        { Value++;if(Boxed is int value)Boxed=value+1;Result=new(){Value=Value,Boxed=Boxed};return ExecutionResult.Next(); }
    }
    public sealed class FailOnce : StepBody
    {
        public int Value { get; set; }
        public override ExecutionResult Run(IStepExecutionContext context)
        {
            if(context.ExecutionPointer.RetryCount==0)throw new InvalidOperationException("remote failure");
            Value++;return ExecutionResult.Next();
        }
    }
    public sealed class FailAlways : StepBody
    { public override ExecutionResult Run(IStepExecutionContext context)=>throw new InvalidOperationException("rollback saga"); }
    public sealed class Scalar : StepBody
    {
        public int Parameter { get; set; }
        public int Result { get; set; }
        public override ExecutionResult Run(IStepExecutionContext context){Result=Parameter*2;return ExecutionResult.Next();}
    }

    public sealed class Workflow(string mode) : IWorkflow<Data>
    {
        public string Id=>"routed-"+mode;
        public int Version=>2;
        public void Build(IWorkflowBuilder<Data> b)
        {
            if(mode=="scalar")
            { b.StartWith(c=>ExecutionResult.Next()).Activity("scalar",d=>d.Value).Output(d=>d.Value,s=>s.Result);return; }
            if(mode=="mixed")
            {
                b.StartWith(c=>ExecutionResult.Next()).Parallel()
                    .Do(t=>t.StartWith<Add>().Input(s=>s.Value,d=>d.Value).Input(s=>s.Resource,d=>d.Resource).Output(d=>d.Other,s=>s.Value))
                    .Do(t=>t.StartWith<Scalar>().Input(s=>s.Parameter,d=>d.Value).Output(d=>d.Tail,s=>s.Result)).Join();return;
            }
            if(mode=="dictionary")
            {
                b.StartWith(c=>ExecutionResult.Next()).Activity("add",d=>new Dictionary<string,object?>{
                    {"value",d.Value},{"resource",d.Resource},{"boxed",d.Value}}).Output(d=>d.Result,s=>s.Result);return;
            }
            if(mode=="boxed")
            {
                b.StartWith<Add>().Input(s=>s.Value,d=>d.Value).Input(s=>s.Resource,d=>d.Resource)
                    .Input(s=>s.Boxed,d=>(object)d.Value).Output(d=>d.Value,s=>(int)s.Boxed!);return;
            }
            if(mode=="primitive")
            {
                b.StartWith(c=>ExecutionResult.Next()).Activity("add",d=>new{value=d.Value,resource=d.Resource,boxed=(object?)null})
                    .Output(d=>d.Result,s=>s.Result);return;
            }
            if(mode=="compensate")
            {
                b.StartWith(c=>ExecutionResult.Next()).Saga(t=>t
                    .StartWith<Add>().Input(s=>s.Value,d=>d.Value).Input(s=>s.Resource,d=>d.Resource)
                        .Output(d=>d.Value,s=>s.Value).CompensateWith(c=>{((Data)c.Workflow.Data).Compensated=true;})
                    .Then<FailAlways>());return;
            }
            b.StartWith<FailOnce>().Input(s=>s.Value,d=>d.Value).Output(d=>d.Value,s=>s.Value)
                .OnError(WorkflowErrorHandling.Retry,TimeSpan.FromMilliseconds(3))
                .While(d=>d.Value<13).Do(t=>t.StartWith<Add>()
                    .Input(s=>s.Value,d=>d.Value).Input(s=>s.Resource,d=>d.Resource).Output(d=>d.Value,s=>s.Value))
                .Parallel()
                    .Do(t=>t.StartWith<Add>().Input(s=>s.Value,d=>d.Value).Input(s=>s.Resource,d=>d.Resource).Output(d=>d.Other,s=>s.Value))
                    .Do(t=>t.StartWith<Add>().Input(s=>s.Value,d=>d.Value).Input(s=>s.Resource,d=>d.Resource).Output(d=>d.Tail,s=>s.Value))
                .Join();
        }
    }

    private static ServiceProvider Provider(string hub,string mode)
    {
        var services=new ServiceCollection().AddLogging();
        services.AddSingleton(new Workflow(mode));
        services.AddHubWorkflowRuntime().AddWorkflow<Workflow,Data>(hub,4)
            .AddActivity<Add>(hub,"add",2,o=>
            {
                o.Partitions=4;o.ResourceName="key";o.ResourceField="resource";o.StrictFifo=true;
                o.Input(s=>s.Value).Input(s=>s.Resource).Input(s=>s.Boxed).Output(s=>s.Value).Output(s=>s.Boxed).ActivityResult(s=>s.Result);
            })
            .AddActivity<FailOnce>(hub,"fail-once",2,o=>{o.Partitions=4;o.Input(s=>s.Value).Output(s=>s.Value);})
            .AddActivity<FailAlways>(hub,"fail-always",2,o=>o.Partitions=4)
            .AddActivity<Scalar>(hub,"scalar",2,o=>{o.Partitions=4;o.ActivityParameters(s=>s.Parameter).Output(s=>s.Result).ActivityResult(s=>s.Result);});
        return services.BuildServiceProvider();
    }

    [Theory]
    [InlineData("mapped")]
    [InlineData("primitive")]
    [InlineData("compensate")]
    [InlineData("boxed")]
    [InlineData("scalar")]
    [InlineData("dictionary")]
    [InlineData("mixed")]
    public async Task RoutedStepsRecoverFromSnapshotsAndApplyOutputsOnce(string mode)
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");if(database is null)return;
        var hub="routed-"+Guid.NewGuid().ToString("N");
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);
        var native=new HubNativeWorkflowStore(tasks,definitions);
        var timers=new HubTimerStore(tasks);
        using(var provider=Provider(hub,mode))
        {
            var catalog=provider.GetRequiredService<HubNativeCatalog>();
            foreach(var activity in catalog.Activities)await definitions.RegisterActivityAsync(activity.Policy,default);
            foreach(var workflow in catalog.Definitions)await definitions.RegisterAsync(workflow,default);
        }
        var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(new(hub,"routed-"+mode,2,
            JsonSerializer.SerializeToElement(new Data(),new JsonSerializerOptions(JsonSerializerDefaults.Web))),default);
        var sawParallel=false;var repeatedSteps=new List<int>();
        var fifo=new Queue<string>();
        for(var iteration=0;iteration<100;iteration++)
        {
            // Rebuild all application services to prove no in-memory result/cursor is required.
            using var provider=Provider(hub,mode);
            var executor=provider.GetRequiredService<HubNativeExecutor>();
            var runtime=(await native.GetRuntimeAsync(id,default))!;
            if(runtime.Status=="Complete")break;
            var processor=await tasks.ClaimAsync("processor",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default);
            if(processor.Count>0)
            {
                var commit=await executor.ExecuteAsync(runtime,processor[0],"processor");
                if(commit.Activities.Length>1)sawParallel=true;
                repeatedSteps.AddRange(commit.Activities.Select(a=>a.StepId));
                var result=await native.CommitAsync(id,commit,default);Assert.NotNull(result);
                foreach(var dispatch in commit.Activities.Where(a=>a.ActivityType=="add"))fifo.Enqueue(dispatch.DispatchKey);
                Assert.True((await native.CommitAsync(id,commit,default))!.Replayed);
            }
            var activityLeases=await tasks.ClaimAsync("activity",[hub],
                [new(hub,"add",2),new(hub,"fail-once",2),new(hub,"fail-always",2),new(hub,"scalar",2)],8,TimeSpan.FromSeconds(30),default);
            // All Add tasks share a FIFO resource of capacity one.
            Assert.True(activityLeases.Count(a=>a.TaskType=="add")<=1);
            foreach(var activity in activityLeases)
            {
                if(activity.TaskType=="add")Assert.Equal(fifo.Dequeue(),activity.Payload.GetProperty("__hubContext").GetProperty("dispatchKey").GetString());
                var result=await executor.ExecuteActivityAsync(activity);
                Assert.True(await native.CompleteActivityAsync(activity.Id,"activity",activity.Epoch,result,default));
                Assert.True(await native.CompleteActivityAsync(activity.Id,"activity",activity.Epoch,result,default));
                Assert.False(await native.CompleteActivityAsync(activity.Id,"activity",activity.Epoch+1,result,default));
            }
            await timers.FireDueAsync(100,default);
            if(processor.Count==0 && activityLeases.Count==0)await Task.Delay(5);
        }
        using var finalProvider=Provider(hub,mode);
        var final=(await native.GetRuntimeAsync(id,default))!;
        Assert.Equal("Complete",final.Status);
        var snapshot=finalProvider.GetRequiredService<HubSnapshotCodec>().Decode<WorkflowInstance>(final.Snapshot!.Value);
        var data=(Data)snapshot.Data;
        if(mode=="primitive")Assert.Equal(11,data.Result!.Value);
        else if(mode=="compensate")Assert.True(data.Compensated);
        else if(mode=="boxed")Assert.Equal(11,data.Value);
        else if(mode=="scalar")Assert.Equal(20,data.Value);
        else if(mode=="dictionary")Assert.Equal(11,Assert.IsType<int>(data.Result!.Boxed));
        else if(mode=="mixed"){Assert.Equal(11,data.Other);Assert.Equal(20,data.Tail);Assert.True(sawParallel);}
        else
        {
            Assert.Equal(13,data.Value);Assert.Equal(14,data.Other);Assert.Equal(14,data.Tail);
            Assert.True(sawParallel);
            Assert.Contains(repeatedSteps.GroupBy(i=>i),g=>g.Count()>1);
            Assert.Contains(snapshot.ExecutionPointers,p=>p.RetryCount==1);
        }
        Assert.Empty(final.Inbox);
    }

    [Fact]
    public async Task LeaseTakeoverAndStatusChangesFenceOldSnapshots()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");if(database is null)return;
        var hub="fence-"+Guid.NewGuid().ToString("N");
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);
        using var provider=Provider(hub,"primitive");
        var catalog=provider.GetRequiredService<HubNativeCatalog>();
        foreach(var definition in catalog.Definitions)await definitions.RegisterAsync(definition,default);
        var native=new HubNativeWorkflowStore(tasks,definitions);
        var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(new(hub,"routed-primitive",2,
            JsonSerializer.SerializeToElement(new Data())),default);
        var lease=Assert.Single(await tasks.ClaimAsync("old",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));
        var oldCommit=await provider.GetRequiredService<HubNativeExecutor>().ExecuteAsync((await native.GetRuntimeAsync(id,default))!,lease,"old");
        await using(var db=new NpgsqlConnection(database))
        {
            await db.OpenAsync();
            await using var expire=new NpgsqlCommand("UPDATE wfc_scale.hub_task SET lease_until=now()-interval '1 second' WHERE id=@id",db);
            expire.Parameters.AddWithValue("id",lease.Id);await expire.ExecuteNonQueryAsync();
        }
        Assert.Null(await native.CommitAsync(id,oldCommit,default));
        var takeover=Assert.Single(await tasks.ClaimAsync("new",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));
        Assert.True(takeover.Epoch>lease.Epoch);
        Assert.Null(await native.CommitAsync(id,oldCommit,default));
        Assert.True(await native.ChangeStatusAsync(id,new("Suspended","suspend"),default));
        var monitor=new HubWorkflowStore(tasks,new HubPolicyRegistry(),new HubWorkflowRegistry());
        Assert.Equal("Suspended",(await monitor.GetAsync(id,default))!.State);
        Assert.Equal(id,Assert.Single((await monitor.ListAsync(hub,null,"Suspended",1,10,default)).Items).Id);
        var overview=await monitor.OverviewAsync(hub,default);
        Assert.Equal(1,overview.Suspended);Assert.Equal(0,overview.Running);
        Assert.False(await tasks.HeartbeatAsync(takeover.Id,"new",takeover.Epoch,TimeSpan.FromSeconds(30),default));
        Assert.Empty(await tasks.ClaimAsync("new",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));
        Assert.True(await native.ChangeStatusAsync(id,new("Runnable","resume"),default));
        Assert.True(await native.ChangeStatusAsync(id,new("Terminated","terminate"),default));
        Assert.True(await native.ChangeStatusAsync(id,new("Terminated","terminate"),default));
        Assert.Equal("Terminated",(await native.GetRuntimeAsync(id,default))!.Status);
        overview=await monitor.OverviewAsync(hub,default);
        Assert.Equal(1,overview.Terminated);Assert.Equal(0,overview.Failed);
    }
}
