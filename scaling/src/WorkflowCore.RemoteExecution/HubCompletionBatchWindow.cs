using System.Diagnostics;
using System.Threading.Channels;

namespace WorkflowCore.RemoteExecution;

/// <summary>
/// Gathers up to <paramref name="capacity"/> items; waits no longer than the
/// configured window after the FIRST item is dequeued. A full batch flushes
/// immediately, while a partial batch flushes at the deadline even if no more
/// completions arrive. Does not depend on PostgreSQL, HTTP or wall-clock time.
/// </summary>
internal static class HubCompletionBatchWindow
{
    internal static async Task<List<T>> FillAsync<T>(
        ChannelReader<T> reader, T first, int capacity,
        TimeSpan window, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        if (window <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(window));

        var batch = new List<T>(capacity) { first };
        var started = Stopwatch.GetTimestamp();
        while (batch.Count < capacity)
        {
            while (batch.Count < capacity && reader.TryRead(out var next))
                batch.Add(next);
            if (batch.Count >= capacity) break;

            var remaining = window - Stopwatch.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero) break;
            using var waiting = CancellationTokenSource.CreateLinkedTokenSource(ct);
            waiting.CancelAfter(remaining);
            try
            {
                if (!await reader.WaitToReadAsync(waiting.Token))
                    break; // producer shut down; flush remaining items now
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                break; // partial-batch deadline
            }
        }
        return batch;
    }
}
