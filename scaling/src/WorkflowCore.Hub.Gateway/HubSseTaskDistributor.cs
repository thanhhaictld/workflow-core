using System.Collections.Concurrent;
using System.Text.Json;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Hub-side task distributor. A worker registers a persistent SSE stream and
/// advertises bounded execution credits. The Hub waits for work using its
/// existing event-gated matcher, obtains a PostgreSQL-fenced lease and pushes
/// the leased task as an SSE event. SSE itself never transfers ownership.
/// </summary>
public sealed class HubSseTaskDistributor
{
    private readonly Func<HubClaimRequest, CancellationToken, Task<IReadOnlyList<HubLeasedTask>>> _poll;

    public HubSseTaskDistributor(HubTaskMatcher matcher) : this(matcher.LongPollAsync) { }

    // Injectable matching delegate enables deterministic tests without a DB.
    public HubSseTaskDistributor(Func<HubClaimRequest, CancellationToken,
        Task<IReadOnlyList<HubLeasedTask>>> poll) => _poll = poll;

    public sealed record Subscribe(HubClaimRequest Claim, int Capacity, int InitialCredits);
    public sealed record Credit(Guid SessionId, string WorkerId, string HubId,
        int MatchingPartition, Guid TaskId, long Epoch);
    private readonly object _registryGate = new();
    private readonly ConcurrentDictionary<Guid, Session> _sessions = new();
    private readonly ConcurrentDictionary<(string Worker, string Hub, int Shard), Guid> _active = new();
    private const int MaxSessions = 2048;

    private sealed class Session(Subscribe subscribe)
    {
        private readonly object _sync = new();
        private readonly SemaphoreSlim _availableSignal = new(0, 1);
        private readonly HashSet<(Guid TaskId, long Epoch)> _outstanding = [];
        private readonly HashSet<(Guid TaskId, long Epoch)> _returned = [];
        private readonly Queue<(Guid TaskId, long Epoch)> _recent = new();
        private int _available = subscribe.InitialCredits;
        private bool _claiming;
        public Guid Id { get; } = Guid.NewGuid();
        public Subscribe Request { get; } = subscribe;
        public CancellationTokenSource Closed { get; } = new();

        public async Task TakeAsync(CancellationToken ct)
        {
            while (true)
            {
                lock (_sync)
                {
                    if (_available > 0)
                    {
                        _available--;
                        _claiming = true;
                        return;
                    }
                }
                await _availableSignal.WaitAsync(ct);
            }
        }

        public void RestoreEmptyClaim()
        {
            lock (_sync)
            {
                _claiming = false;
                _available++;
                Wake();
            }
        }

        public void MarkLeased(HubLeasedTask task)
        {
            lock (_sync)
            {
                if (!_claiming) throw new InvalidOperationException("SSE claim had no capacity reservation");
                _claiming = false;
                _outstanding.Add((task.Id, task.Epoch));
            }
        }

        public bool ReturnCredit(Credit credit)
        {
            lock (_sync)
            {
                var key = (credit.TaskId, credit.Epoch);
                if (_returned.Contains(key)) return true; // idempotent retry
                if (_outstanding.Remove(key))
                {
                    _available++;
                }
                else if (_available + _outstanding.Count + (_claiming ? 1 : 0) < Request.Capacity)
                {
                    // A worker may reconnect while an old task continues to
                    // execute. Its new session advertises only FREE capacity;
                    // finishing an old task replenishes the missing credit.
                    _available++;
                }
                else return false;
                _returned.Add(key);
                _recent.Enqueue(key);
                while (_recent.Count > 512)
                    _returned.Remove(_recent.Dequeue());
                Wake();
                return true;
            }
        }

        public void Cancel() { try { Closed.Cancel(); } catch (ObjectDisposedException) { } }

        private void Wake()
        {
            if (_availableSignal.CurrentCount == 0)
                _availableSignal.Release();
        }
    }

    public int ActiveSessions => _sessions.Count;

    public bool Replenish(Credit credit)
    {
        if (!_sessions.TryGetValue(credit.SessionId, out var session) ||
            session.Request.Claim.WorkerId != credit.WorkerId ||
            session.Request.Claim.Hubs[0] != credit.HubId ||
            session.Request.Claim.MatchingPartition != credit.MatchingPartition)
            return false;
        return session.ReturnCredit(credit);
    }

    public async Task StreamAsync(Subscribe subscribe,
        Func<string, CancellationToken, Task> send, CancellationToken ct)
    {
        if (subscribe.Claim.Hubs is not { Length: 1 } ||
            subscribe.Claim.Capabilities is not { Length: > 0 and <= 128 } ||
            subscribe.Claim.MaxTasks != 1 ||
            subscribe.Claim.MatchingPartition is null ||
            subscribe.Claim.WorkerId is not { Length: > 0 and <= 128 } ||
            subscribe.Capacity is < 1 or > 64 ||
            subscribe.InitialCredits < 0 || subscribe.InitialCredits > subscribe.Capacity)
            throw new ArgumentException("Invalid SSE worker capacity/capability registration");

        var session = new Session(subscribe);
        var key = (subscribe.Claim.WorkerId, subscribe.Claim.Hubs[0],
            subscribe.Claim.MatchingPartition.Value);
        lock (_registryGate)
        {
            if (_sessions.Count >= MaxSessions)
                throw new InvalidOperationException("SSE session capacity exceeded");
            if (_active.TryGetValue(key, out var previous) &&
                _sessions.TryGetValue(previous, out var prior))
                prior.Cancel();
            _sessions[session.Id] = session;
            _active[key] = session.Id;
        }
        HubTelemetry.SseTaskSessions.Add(1);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, session.Closed.Token);
        var token = linked.Token;
        try
        {
            await send("event: session\ndata: " +
                JsonSerializer.Serialize(new { sessionId = session.Id }) + "\n\n", token);
            while (!token.IsCancellationRequested)
            {
                // Keep the worker, reverse proxy and matching owner connection
                // alive even when all worker execution credits are in use.
                var take = session.TakeAsync(token);
                while (!take.IsCompleted)
                {
                    if (await Task.WhenAny(take, Task.Delay(TimeSpan.FromSeconds(10), token)) != take)
                        await send(": keepalive\n\n", token);
                }
                await take;
                IReadOnlyList<HubLeasedTask> leases;
                try { leases = await _poll(subscribe.Claim, token); }
                catch
                {
                    // If ownership is lost or claim fails, close the stream.
                    // No speculative reassignment of any delivered task.
                    session.RestoreEmptyClaim();
                    throw;
                }
                if (leases.Count == 0)
                {
                    session.RestoreEmptyClaim();
                    await send(": keepalive\n\n", token);
                    continue;
                }
                if (leases.Count != 1)
                    throw new InvalidOperationException("SSE distributor requires MaxTasks=1");
                var task = leases[0];
                session.MarkLeased(task);
                HubTelemetry.SseTasksDispatched.Add(1);
                await send("event: task.leased\ndata: " +
                    JsonSerializer.Serialize(task, new JsonSerializerOptions(JsonSerializerDefaults.Web)) +
                    "\n\n", token);
            }
        }
        finally
        {
            session.Cancel();
            lock (_registryGate)
            {
                _sessions.TryRemove(session.Id, out _);
                // Do not erase a replacement generation's ownership.
                if (_active.TryGetValue(key, out var current) && current == session.Id)
                    _active.TryRemove(key, out _);
            }
            HubTelemetry.SseTaskSessions.Add(-1);
            session.Closed.Dispose();
        }
    }
}
