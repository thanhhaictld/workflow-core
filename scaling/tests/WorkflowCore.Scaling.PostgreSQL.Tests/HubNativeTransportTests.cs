using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Client;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeTransportTests
{
    [Fact]
    public void DevelopmentTransportChecksTheHostInsteadOfALocalhostPrefix()
    {
        Assert.Throws<InvalidOperationException>(()=>new HubRestClient("http://localhost.example.invalid","a-token-with-at-least-24-characters"));
        using var client=new HubRestClient("http://127.0.0.1:5100","a-token-with-at-least-24-characters");
        Assert.True(client.Http.BaseAddress!.IsLoopback);
    }
    public sealed class Effects { public int Calls; }
    public sealed class CountEffect(Effects effects) : StepBody
    {
        // This DTO is in a different assembly and is only an ActivityResult.
        public HubCapability Result { get; set; }=new("tests","count",2);
        public override ExecutionResult Run(IStepExecutionContext context)
        { Interlocked.Increment(ref effects.Calls);return ExecutionResult.Next(); }
    }

    [Fact]
    public async Task CompletionOutageResendsFrozenResultWithoutFailingOrExecutingBodyAgain()
    {
        var builder=WebApplication.CreateBuilder();builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        await using var hub=builder.Build();
        var ready=1;var epoch=0;var completions=0;var failures=0;var published=0;var earlyClaims=0;
        HubLeasedTask? activity=null;
        var accepted=new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        hub.MapPost("/api/hub/activity-registrations",async ()=>
        { await Task.Delay(100);Volatile.Write(ref published,1);return Results.Ok(new{registered=true}); });
        hub.MapPost("/api/hub/workers/tasks:claim",(HubClaimRequest claim)=>
        {
            if(Volatile.Read(ref published)==0)Interlocked.Increment(ref earlyClaims);
            return Results.Json(Interlocked.Exchange(ref ready,0)==1 ? new[]{activity! with{Epoch=Interlocked.Increment(ref epoch)}}:[]);
        });
        hub.MapGet("/api/hub/workers/events",()=>Results.Text(": keepalive\n\n","text/event-stream"));
        hub.MapPost("/api/hub/tasks/{id}/heartbeat",()=>Results.NoContent());
        hub.MapPost("/api/hub/tasks/{id}/fail",()=>
        { Interlocked.Increment(ref failures);Interlocked.Exchange(ref ready,1);return Results.NoContent(); });
        hub.MapPost("/api/hub/tasks/{id}/complete",(HubTaskResultRequest result)=>
        {
            if(Interlocked.Increment(ref completions)<=4)return Results.StatusCode((int)HttpStatusCode.ServiceUnavailable);
            accepted.TrySetResult(result.Result.Clone());return Results.NoContent();
        });
        await hub.StartAsync();
        var effects=new Effects();
        using var host=new HostBuilder().ConfigureServices(services=>
        {
            services.AddLogging();services.AddSingleton(effects);
            services.Configure<HostOptions>(o=>o.ServicesStartConcurrently=true);
            services.AddHubWorkflowRuntime(o=>
            {o.HubUrl=hub.Urls.Single();o.Token="local-transport-test-with-24-characters";o.PollInterval=TimeSpan.FromMilliseconds(20);})
                .AddActivity<CountEffect>("tests","count",2,o=>o.ActivityResult(s=>s.Result));
        }).Build();
        var codec=host.Services.GetRequiredService<HubSnapshotCodec>();
        var pointer=new ExecutionPointer{Id=Guid.NewGuid().ToString(),StepId=0};
        var payload=JsonSerializer.SerializeToElement(new{
            __hubContext=new{dispatchKey="stable-dispatch",workflowId=Guid.NewGuid().ToString(),definitionId="test",version=1,pointer=codec.Encode(pointer)},
            __hubValues=codec.EncodeValue(new Dictionary<string,object?>())});
        activity=new(Guid.NewGuid(),"tests",0,"count",2,Guid.NewGuid(),payload,1,1,DateTime.UtcNow.AddSeconds(30));
        try
        {
            await host.StartAsync();
            var reply=await accepted.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(JsonValueKind.Null,reply.GetProperty("error").ValueKind);
            var data=codec.DecodeValue(reply.GetProperty("activityData"));
            Assert.Equal(new HubCapability("tests","count",2),Assert.IsType<HubCapability>(data));
            Assert.Equal(1,Volatile.Read(ref effects.Calls));
            Assert.Equal(0,Volatile.Read(ref failures));
            Assert.Equal(5,Volatile.Read(ref completions));
            Assert.Equal(0,Volatile.Read(ref earlyClaims));
        }
        finally { await host.StopAsync();await hub.StopAsync(); }
    }
}
