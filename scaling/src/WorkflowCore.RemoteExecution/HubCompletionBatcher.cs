using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Client;

namespace WorkflowCore.RemoteExecution;

/// <summary>
/// Optional bounded, processor-local native activity completion buffer.
/// Flushes after 2-5ms or 16-64 items, whichever occurs first. Never ACKs
/// until the Hub confirms the corresponding fenced durable PostgreSQL commit.
/// Each batch executes independently; four in-flight batches prevent one busy
/// workflow from blocking all other completion traffic.
/// </summary>
internal sealed class HubCompletionBatcher(HubRestClient client, HubRuntimeOptions options,
    ILogger<HubCompletionBatcher> logger) : BackgroundService
{
    private sealed record Pending(HubCompletionBatchItem Item,
        TaskCompletionSource<HttpStatusCode> Result, CancellationToken Cancel);

    private readonly Channel<Pending> _queue=Channel.CreateBounded<Pending>(
        new BoundedChannelOptions(4096)
        {
            FullMode=BoundedChannelFullMode.Wait,
            SingleReader=true,
            SingleWriter=false
        });

    public async Task<HttpStatusCode> SubmitAsync(
        HubLeasedTask task, HubTaskResultRequest result, CancellationToken ct)
    {
        var promise=new TaskCompletionSource<HttpStatusCode>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var pending=new Pending(new HubCompletionBatchItem(task.Id,result.WorkerId,
            result.Epoch,result.Result),promise,ct);
        await _queue.Writer.WriteAsync(pending,ct);
        return await promise.Task.WaitAsync(ct);
    }

    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        using var slots=new SemaphoreSlim(4,4);
        var running=new List<Task>();
        try
        {
            while(await _queue.Reader.WaitToReadAsync(stop))
            {
                if(!_queue.Reader.TryRead(out var first))continue;
                var batch=new List<Pending>(options.CompletionBatchSize){first};
                var started=Stopwatch.GetTimestamp();
                var window=TimeSpan.FromMilliseconds(options.CompletionBatchDelayMs);
                while(batch.Count<options.CompletionBatchSize)
                {
                    while(batch.Count<options.CompletionBatchSize &&
                          _queue.Reader.TryRead(out var next))
                        batch.Add(next);
                    if(batch.Count>=options.CompletionBatchSize)break;
                    var remaining=window-Stopwatch.GetElapsedTime(started);
                    if(remaining<=TimeSpan.Zero)break;
                    using var waiting=CancellationTokenSource.CreateLinkedTokenSource(stop);
                    waiting.CancelAfter(remaining);
                    try { await _queue.Reader.WaitToReadAsync(waiting.Token); }
                    catch(OperationCanceledException) when(!stop.IsCancellationRequested) { break; }
                }
                await slots.WaitAsync(stop);
                var sending=SendBatchAsync(batch,slots,stop);
                running.Add(sending);
                running.RemoveAll(x=>x.IsCompleted);
            }
        }
        catch(OperationCanceledException) when(stop.IsCancellationRequested) { }
        finally
        {
            _queue.Writer.TryComplete();
            while(_queue.Reader.TryRead(out var pending))
                pending.Result.TrySetCanceled();
            // SendBatchAsync handles all exceptions by completing its promises.
            await Task.WhenAll(running);
        }
    }

    private async Task SendBatchAsync(List<Pending> batch,SemaphoreSlim slots,CancellationToken stop)
    {
        try
        {
            var active=batch.Where(x=>!x.Cancel.IsCancellationRequested).ToArray();
            foreach(var cancelled in batch.Except(active))
                cancelled.Result.TrySetCanceled(cancelled.Cancel);
            if(active.Length==0)return;
            using var response=await client.Http.PostAsJsonAsync(
                "api/hub/workers/tasks:complete-batch",
                new HubCompletionBatchRequest(active.Select(x=>x.Item).ToArray()),stop);
            if(!response.IsSuccessStatusCode)
            {
                var details=await response.Content.ReadAsStringAsync(stop);
                throw response.StatusCode is HttpStatusCode.TooManyRequests or
                    HttpStatusCode.RequestTimeout or HttpStatusCode.ServiceUnavailable ||
                    (int)response.StatusCode>=500
                    ? new HttpRequestException("Batch completion temporarily unavailable: "+
                        response.StatusCode+" "+details)
                    : new InvalidOperationException("Invalid native completion microbatch: "+
                        response.StatusCode+" "+details);
            }
            var items=await response.Content.ReadFromJsonAsync<HubCompletionBatchItemResult[]>(
                cancellationToken:stop)
                ?? throw new HttpRequestException("Hub completion batch returned no acknowledgements");
            if(items.Length!=active.Length ||
               items.Select(x=>x.TaskId).Distinct().Count()!=active.Length)
                throw new HttpRequestException("Incomplete/duplicate Hub completion acknowledgements");
            var byId=items.ToDictionary(x=>x.TaskId);
            foreach(var pending in active)
            {
                if(!byId.TryGetValue(pending.Item.TaskId,out var result))
                {
                    pending.Result.TrySetException(new HttpRequestException(
                        "Missing Hub completion acknowledgement"));
                    continue;
                }
                switch(result.Status)
                {
                    case "completed":
                        pending.Result.TrySetResult(HttpStatusCode.NoContent);
                        break;
                    case "fenced":
                        pending.Result.TrySetResult(HttpStatusCode.Conflict);
                        break;
                    case "retry":
                        pending.Result.TrySetException(new HttpRequestException(
                            "Hub completion row busy; retry immutable result"));
                        break;
                    default:
                        pending.Result.TrySetException(new InvalidOperationException(
                            "Unknown Hub completion status: "+result.Status));
                        break;
                }
            }
        }
        catch(Exception ex) when(ex is not OperationCanceledException || !stop.IsCancellationRequested)
        {
            logger.LogWarning(ex,"Native completion microbatch failed; individual workers retry frozen results");
            foreach(var pending in batch)pending.Result.TrySetException(ex);
        }
        catch(OperationCanceledException) when(stop.IsCancellationRequested)
        {
            foreach(var pending in batch)pending.Result.TrySetCanceled();
        }
        finally { slots.Release(); }
    }
}
