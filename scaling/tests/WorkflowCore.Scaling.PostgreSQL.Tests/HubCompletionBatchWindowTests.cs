using System.Diagnostics;
using System.Threading.Channels;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// No PostgreSQL dependency: exercises the actual worker-side collection
/// deadline rather than measuring the database lock_timeout (also 100ms).
/// </summary>
public sealed class HubCompletionBatchWindowTests
{
    [Fact]
    public async Task HundredMillisecondWindowFlushesOneItemWithoutReachingBatchCapacity()
    {
        var channel=Channel.CreateUnbounded<int>();
        Assert.True(channel.Writer.TryWrite(42));
        Assert.True(channel.Reader.TryRead(out var first));
        var clock=Stopwatch.StartNew();

        var batch=await HubCompletionBatchWindow.FillAsync(
            channel.Reader,first,64,TimeSpan.FromMilliseconds(100),CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        clock.Stop();
        Assert.Equal(new[] {42},batch);
        // A partial batch must wait for the configured collection deadline,
        // but should not wait forever for 64 activity completions.
        Assert.True(clock.Elapsed >= TimeSpan.FromMilliseconds(70),
            "100ms collection deadline returned too early: "+clock.Elapsed);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(4),
            "100ms collection deadline took unexpectedly long: "+clock.Elapsed);
    }

    [Fact]
    public async Task FullBatchFlushesImmediatelyEvenWithHundredMillisecondWindow()
    {
        var channel=Channel.CreateUnbounded<int>();
        for(var i=0;i<65;i++)
            Assert.True(channel.Writer.TryWrite(i));
        Assert.True(channel.Reader.TryRead(out var first));
        // Use a 5s deadline to prove the size threshold wins without
        // depending on CI runner timing within the 100ms window.
        var batch=await HubCompletionBatchWindow.FillAsync(
            channel.Reader,first,64,TimeSpan.FromSeconds(5),CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(64,batch.Count);
        Assert.Equal(Enumerable.Range(0,64),batch);
        Assert.True(channel.Reader.TryRead(out var remaining));
        Assert.Equal(64,remaining);
    }

    [Fact]
    public async Task PartialBatchFlushesWhenProducerCompletesBeforeWindow()
    {
        var channel=Channel.CreateUnbounded<int>();
        Assert.True(channel.Writer.TryWrite(1));
        Assert.True(channel.Reader.TryRead(out var first));
        var pending=HubCompletionBatchWindow.FillAsync(
            channel.Reader,first,32,TimeSpan.FromMilliseconds(100),CancellationToken.None);
        Assert.True(channel.Writer.TryWrite(2));
        channel.Writer.TryComplete();

        var batch=await pending.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(new[] {1,2},batch);
    }

    [Fact]
    public async Task CancellationInterruptsHundredMillisecondCollection()
    {
        var channel=Channel.CreateUnbounded<int>();
        Assert.True(channel.Writer.TryWrite(1));
        Assert.True(channel.Reader.TryRead(out var first));
        using var cancel=new CancellationTokenSource();
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async ()=>
            await HubCompletionBatchWindow.FillAsync(
                channel.Reader,first,32,TimeSpan.FromMilliseconds(100),cancel.Token));
    }
}
