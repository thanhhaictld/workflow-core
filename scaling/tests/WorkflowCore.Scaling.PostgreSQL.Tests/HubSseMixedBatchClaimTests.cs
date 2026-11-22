using System.Collections.Concurrent;
using System.Text.Json;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Gateway;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Deterministic protocol tests for all SSE worker-lane task kinds. Credits
/// are queued before the session starts consuming them, so each test proves
/// real multi-task PostgreSQL claim requests (not just a configured MaxTasks).
/// </summary>
public sealed class HubSseMixedBatchClaimTests
{
    private static HubSseTaskDistributor.Subscribe Subscribe(string type,int count) =>
        new(new HubClaimRequest("mixed-worker",["mixed-hub"],
            [new HubCapability("mixed-hub",type,1)],
            MaxTasks:1,MatchingPartition:0,MatchingPartitionCount:4),
            count,InitialCredits:0);

    private static HubLeasedTask Lease(string type) =>
        new(Guid.NewGuid(),"mixed-hub",0,type,1,Guid.NewGuid(),
            JsonSerializer.SerializeToElement(new { }),1,1,DateTime.UtcNow.AddMinutes(1));

    [Theory]
    [InlineData("__workflow__",4)]
    [InlineData("activity",4)]
    [InlineData("activity",8)]
    [InlineData("activity",16)]
    [InlineData("activity",32)]
    [InlineData("standalone-task",8)]
    public async Task QueuedCreditsClaimAndDispatchBatchesForAllKinds(string type,int count)
    {
        var ready = new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var claims = new List<int>();
        var assigned = new ConcurrentDictionary<Guid,Guid>();
        var requested = new List<Guid>();
        var deliveryCount = 0;
        var distributor = new HubSseTaskDistributor((request,_) =>
        {
            claims.Add(request.MaxTasks);
            return Task.FromResult<IReadOnlyList<HubLeasedTask>>(
                Enumerable.Range(0,request.MaxTasks).Select(_=>Lease(type)).ToArray());
        },mixedClaimBatchSize:count,creditCoalesceMilliseconds:0);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var stream = distributor.StreamAsync(Subscribe(type,count),5,
            _=>Task.FromResult(true),(frame,_) =>
            {
                if(frame.StartsWith("event: session",StringComparison.Ordinal))
                {
                    using var doc=Parse(frame);
                    ready.TrySetResult(doc.RootElement.GetProperty("sessionId").GetGuid());
                    return resume.Task;
                }
                if(frame.StartsWith("event: task.leased",StringComparison.Ordinal))
                {
                    using var doc=Parse(frame);
                    var credit=doc.RootElement.GetProperty("creditId").GetGuid();
                    var task=doc.RootElement.GetProperty("task").GetProperty("id").GetGuid();
                    Assert.True(assigned.TryAdd(credit,task));
                    if(Interlocked.Increment(ref deliveryCount)==count)
                        completed.TrySetResult();
                }
                return Task.CompletedTask;
            },stop.Token);
        var session=await ready.Task.WaitAsync(stop.Token);
        for(var i=0;i<count;i++)
        {
            var id=Guid.NewGuid();
            requested.Add(id);
            Assert.True(distributor.Offer(new(session,5,"mixed-worker","mixed-hub",0,id)));
        }
        Assert.False(distributor.Offer(new(session,5,"mixed-worker","mixed-hub",0,Guid.NewGuid())));
        resume.TrySetResult();
        await completed.Task.WaitAsync(stop.Token);
        Assert.Single(claims);
        Assert.Equal(count,claims[0]);
        Assert.Equal(requested.Order(),assigned.Keys.Order());
        Assert.Equal(count,assigned.Values.Distinct().Count());
        Assert.Equal(count,distributor.ClaimedTasks);
        Assert.Equal(1,distributor.MultiTaskClaims);
        Assert.Equal(count,distributor.MaxTasksClaimed);
        await stop.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>stream);
    }

    [Fact]
    public async Task DefaultRemainsOneClaimForActivityEvenWithQueuedCredits()
    {
        var ready=new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var done=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sizes=new List<int>();
        var unused=0;
        var distributor=new HubSseTaskDistributor((request,_) =>
        {
            sizes.Add(request.MaxTasks);
            return Task.FromResult<IReadOnlyList<HubLeasedTask>>([]);
        },mixedClaimBatchSize:1);
        using var stop=new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var stream=distributor.StreamAsync(Subscribe("activity",4),9,
            _=>Task.FromResult(true),(frame,_) =>
            {
                if(frame.StartsWith("event: session",StringComparison.Ordinal))
                {
                    using var doc=Parse(frame);
                    ready.TrySetResult(doc.RootElement.GetProperty("sessionId").GetGuid());
                    return resume.Task;
                }
                if(frame.StartsWith("event: credit.unused",StringComparison.Ordinal) &&
                    Interlocked.Increment(ref unused)==4)
                    done.TrySetResult();
                return Task.CompletedTask;
            },stop.Token);
        var session=await ready.Task.WaitAsync(stop.Token);
        for(var i=0;i<4;i++)
            Assert.True(distributor.Offer(new(session,9,"mixed-worker","mixed-hub",0,Guid.NewGuid())));
        resume.TrySetResult();
        await done.Task.WaitAsync(stop.Token);
        Assert.Equal(new[]{1,1,1,1},sizes);
        Assert.Equal(0,distributor.MultiTaskClaims);
        await stop.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>stream);
    }

    [Fact]
    public async Task PartialClaimReturnsUnmatchedCreditsWithoutLeasingThem()
    {
        var ready=new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var done=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var assigned=0;
        var unused=0;
        var distributor=new HubSseTaskDistributor((request,_) =>
        {
            Assert.Equal(4,request.MaxTasks);
            return Task.FromResult<IReadOnlyList<HubLeasedTask>>(
                [Lease("activity"),Lease("activity")]);
        },mixedClaimBatchSize:4);
        using var stop=new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var stream=distributor.StreamAsync(Subscribe("activity",4),9,
            _=>Task.FromResult(true),(frame,_) =>
            {
                if(frame.StartsWith("event: session",StringComparison.Ordinal))
                {
                    using var doc=Parse(frame);
                    ready.TrySetResult(doc.RootElement.GetProperty("sessionId").GetGuid());
                    return resume.Task;
                }
                if(frame.StartsWith("event: task.leased",StringComparison.Ordinal))
                    Interlocked.Increment(ref assigned);
                if(frame.StartsWith("event: credit.unused",StringComparison.Ordinal))
                    Interlocked.Increment(ref unused);
                if(Volatile.Read(ref assigned)+Volatile.Read(ref unused)==4)
                    done.TrySetResult();
                return Task.CompletedTask;
            },stop.Token);
        var session=await ready.Task.WaitAsync(stop.Token);
        for(var i=0;i<4;i++)
            Assert.True(distributor.Offer(new(session,9,"mixed-worker","mixed-hub",0,Guid.NewGuid())));
        resume.TrySetResult();
        await done.Task.WaitAsync(stop.Token);
        Assert.Equal(2,assigned);
        Assert.Equal(2,unused);
        Assert.Equal(2,distributor.ClaimedTasks);
        await stop.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>stream);
    }

    [Theory]
    [InlineData(0,0)]
    [InlineData(33,0)]
    [InlineData(4,6)]
    public void InvalidSseBatchParametersRejectAtStartup(int batchSize,int coalesceMs)
    {
        Assert.Throws<ArgumentException>(()=>new HubSseTaskDistributor(
            (_,_)=>Task.FromResult<IReadOnlyList<HubLeasedTask>>([]),
            mixedClaimBatchSize:batchSize,creditCoalesceMilliseconds:coalesceMs));
    }

    private static JsonDocument Parse(string frame) =>
        JsonDocument.Parse(frame.Split("data: ",StringSplitOptions.None)[1].Trim());
}
