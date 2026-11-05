using System.Text.Json;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Real PostgreSQL contract test: external definition/policy registration,
/// revision-fenced processor transitions, durable timers and worker completions.
/// Requires WFC_TEST_PG with migrations 002 and 007-010 applied.
/// </summary>
public sealed class HubRemoteSchedulerTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");

    [Fact]
    public async Task RemoteWorkflow_ActivityTimerActivity_ResumesWithDurableWaterfall()
    {
        if(Database is null)return;
        await using var tasks=new HubTaskStore(Database,8);
        var defs=new HubDefinitionStore(tasks);
        var remote=new HubRemoteWorkflowStore(tasks,defs);
        var timers=new HubTimerStore(tasks);
        var hub="remote-test-"+Guid.NewGuid().ToString("N")[..12];
        var metadata=JsonSerializer.SerializeToElement(new {
            steps=new[]{
                new{activityType="validate",kind="Activity"},
                new{activityType="hold",kind="Delay"},
                new{activityType="provision",kind="Activity"}
            }
        });
        Assert.True(await defs.RegisterAsync(new(hub,"onboard",1,8,3,metadata),default));
        Assert.False(await defs.RegisterAsync(new(hub,"onboard",1,8,3,metadata),default));
        Assert.False(await defs.RegisterAsync(new(hub,"onboard",1,8,2,metadata),default));
        Assert.Equal(2,(await defs.GetAsync(hub,"onboard",1,default))!.StepCount);
        Assert.False(await defs.RegisterAsync(new(hub,"onboard",1,8,3,metadata),default));
        Assert.True(await defs.RegisterActivityAsync(new(hub,"validate",1,8,10,10),default));
        Assert.True(await defs.RegisterActivityAsync(new(hub,"provision",1,8,10,10,
            "tenant","tenantId",1,true),default));

        var id=await remote.StartAsync(new(hub,"onboard",1,
            JsonSerializer.SerializeToElement(new{tenantId="t-123"}),
            "test-idempotency","test-run"),default);
        Assert.Equal(id,await remote.StartAsync(new(hub,"onboard",1,
            JsonSerializer.SerializeToElement(new{tenantId="t-123"}),
            "test-idempotency","test-run"),default));

        async Task<HubLeasedTask> Claim(string type,string worker)
        {
            var result=await tasks.ClaimAsync(worker,[hub],
                [new HubCapability(hub,type,1)],8,TimeSpan.FromSeconds(30),default);
            return Assert.Single(result);
        }
        HubTransitionRequest Decision(HubLeasedTask t,long expected,string command,
            string? activity=null,string? timer=null,long? milliseconds=null)=>
            new("processor",t.Epoch,expected,t.Id,command,activity,1,timer,milliseconds);

        var p1=await Claim("__workflow__","processor");
        var first=await remote.CommitAsync(id,Decision(p1,0,"ScheduleActivity","validate"),default);
        Assert.NotNull(first);
        Assert.Equal(1,first.Revision);
        var replay=await remote.CommitAsync(id,Decision(p1,0,"ScheduleActivity","validate"),default);
        Assert.NotNull(replay);
        Assert.True(replay.Replayed);
        Assert.Null(await remote.CommitAsync(id,Decision(p1,1,"ScheduleActivity","validate"),default));

        var a1=await Claim("validate","provisioner");
        Assert.True(await remote.CompleteActivityAsync(a1.Id,"provisioner",a1.Epoch,
            """{"validated":true}""",default));

        var p2=await Claim("__workflow__","processor");
        Assert.NotNull(await remote.CommitAsync(id,
            Decision(p2,2,"ScheduleTimer",timer:"hold",milliseconds:100),default));
        var waiting=await remote.GetRuntimeAsync(id,default);
        Assert.NotNull(waiting);
        Assert.StartsWith("timer:",waiting.WaitingFor!);
        Assert.Equal(1,waiting.NextStep);
        await Task.Delay(130);

        // Two scheduler replicas may race; exactly one logical timer fires.
        var claims=await Task.WhenAll(timers.FireDueAsync(10,default),
            timers.FireDueAsync(10,default));
        Assert.Equal(1,claims.Sum());
        Assert.Equal(0,await timers.FireDueAsync(10,default));
        var awakened=await remote.GetRuntimeAsync(id,default);
        Assert.NotNull(awakened);
        Assert.Null(awakened.WaitingFor);
        Assert.Equal(2,awakened.NextStep);
        Assert.True(awakened.Results.GetProperty("1").GetProperty("timerFired").GetBoolean());

        var p3=await Claim("__workflow__","processor");
        Assert.NotNull(await remote.CommitAsync(id,
            Decision(p3,4,"ScheduleActivity","provision"),default));
        var a3=await Claim("provision","provisioner");
        Assert.True(await remote.CompleteActivityAsync(a3.Id,"provisioner",a3.Epoch,
            """{"provisioned":true}""",default));

        var p4=await Claim("__workflow__","processor");
        var completed=await remote.CommitAsync(id,
            Decision(p4,6,"CompleteWorkflow"),default);
        Assert.NotNull(completed);
        Assert.Equal("Complete",completed.State);
        var final=await remote.GetRuntimeAsync(id,default);
        Assert.NotNull(final);
        Assert.Equal(3,final.NextStep);
        Assert.Equal("Complete",final.State);

        var legacy=new HubWorkflowStore(tasks,new HubPolicyRegistry(),new HubWorkflowRegistry());
        var history=await legacy.HistoryAsync(id,default);
        Assert.Contains(history,x=>x.EventType=="TimerScheduled");
        Assert.Contains(history,x=>x.EventType=="TimerFired");
        Assert.Contains(history,x=>x.EventType=="WorkflowTaskClaimed");
        Assert.Contains(history,x=>x.EventType=="WorkflowCompleted");
        Assert.Equal(new[]{0,2},history
            .Where(x=>x.EventType=="StepCompleted").Select(x=>x.StepIndex).ToArray());
        Assert.True((await timers.MetricsAsync(default)).Fired>=1);
    }
}
