namespace WorkflowCore.RemoteExecution;

/// <summary>
/// Worker-wide accounting for ONE-SHOT SSE task grants across matching shards.
/// A free execution slot may be offered to any connected shard. Outstanding
/// offers and executing tasks both consume slots; reconnect only releases
/// undelivered offers from the old session. Running work is never forgotten.
/// This does not alter PostgreSQL task leases, FIFO permits or completion.
/// </summary>
public sealed class HubSseWorkerCreditCoordinator
{
    public sealed record Grant(int Shard, Guid SessionId, long OwnerEpoch, Guid CreditId);
    private sealed record Session(Guid Id, long Epoch, Action Invalidate);
    private sealed record Offer(int Shard, Guid Session);
    private readonly object _sync = new();
    private readonly int _capacity;
    private readonly int _shardCount;
    private readonly Dictionary<int, Session> _sessions = [];
    private readonly Dictionary<Guid, Offer> _offers = [];
    private readonly HashSet<Guid> _executing = [];
    private readonly Dictionary<int, DateTime> _quietUntil = [];
    private readonly SemaphoreSlim _wake = new(0, 1);
    private int _nextShard;
    private int _hotShard = -1;
    private int _hotSelections;

    public HubSseWorkerCreditCoordinator(int capacity, int shards)
    {
        if (capacity is < 1 or > 64)
            throw new ArgumentOutOfRangeException(nameof(capacity));
        if (shards is < 1 or > 64)
            throw new ArgumentOutOfRangeException(nameof(shards));
        _capacity = capacity;
        _shardCount = shards;
    }

    public int Capacity => _capacity;
    public int ReservedOrRunning
    {
        get { lock (_sync) return _offers.Count + _executing.Count; }
    }

    public void Connected(int shard, Guid sessionId, long epoch, Action invalidate)
    {
        ValidateShard(shard);
        if (sessionId == Guid.Empty || epoch < 1)
            throw new ArgumentException("Invalid SSE matching session");
        Action? old = null;
        lock (_sync)
        {
            if (_sessions.TryGetValue(shard, out var current))
                old = current.Invalidate;
            DropUndelivered(shard);
            _sessions[shard] = new Session(sessionId, epoch, invalidate);
            Wake();
        }
        // Cancel the old generation *outside* the lock.
        old?.Invoke();
    }

    public void Disconnected(int shard, Guid sessionId)
    {
        ValidateShard(shard);
        lock (_sync)
        {
            if (!_sessions.TryGetValue(shard, out var session) ||
                session.Id != sessionId) return;
            _sessions.Remove(shard);
            DropUndelivered(shard);
            Wake();
        }
    }

    public void Invalidate(int shard, Guid sessionId)
    {
        Action? stop = null;
        lock (_sync)
        {
            if (_sessions.TryGetValue(shard, out var session) && session.Id == sessionId)
                stop = session.Invalidate;
        }
        stop?.Invoke();
    }

    public bool TryOffer(out Grant grant)
    {
        lock (_sync)
        {
            grant = null!;
            if (_executing.Count + _offers.Count >= _capacity || _sessions.Count == 0)
                return false;
            var now = DateTime.UtcNow;
            int? chosen = null;
            // Favor a shard that actually delivered work, but regularly
            // inspect all others to prevent starvation across shards.
            if (_hotShard >= 0 && _hotSelections < 3 &&
                _sessions.ContainsKey(_hotShard) &&
                (!_quietUntil.TryGetValue(_hotShard, out var hotQuiet) || hotQuiet <= now))
            {
                chosen = _hotShard;
                _hotSelections++;
            }
            else
            {
                for (var i = 0; i < _shardCount; i++)
                {
                    var candidate = (_nextShard + i) % _shardCount;
                    if (!_sessions.ContainsKey(candidate)) continue;
                    if (_quietUntil.TryGetValue(candidate, out var until) && until > now) continue;
                    chosen = candidate;
                    _nextShard = (candidate + 1) % _shardCount;
                    _hotSelections = 0;
                    break;
                }
            }
            if (chosen is null) return false;
            var session = _sessions[chosen.Value];
            var creditId = Guid.NewGuid();
            _offers.Add(creditId, new Offer(chosen.Value, session.Id));
            grant = new Grant(chosen.Value, session.Id, session.Epoch, creditId);
            return true;
        }
    }

    public bool Dispatched(int shard, Guid sessionId, Guid creditId, Guid taskId)
    {
        ValidateShard(shard);
        lock (_sync)
        {
            if (!_sessions.TryGetValue(shard, out var current) ||
                current.Id != sessionId ||
                !_offers.TryGetValue(creditId, out var offer) ||
                offer.Session != sessionId || offer.Shard != shard)
                return false;
            _offers.Remove(creditId);
            if (!_executing.Add(taskId))
            {
                Wake();
                return false; // Task ID already executing; do not run twice.
            }
            _hotShard = shard;
            _hotSelections = 0;
            _quietUntil.Remove(shard);
            Wake();
            return true;
        }
    }

    public bool Unused(int shard, Guid sessionId, Guid creditId)
    {
        ValidateShard(shard);
        lock (_sync)
        {
            if (!_sessions.TryGetValue(shard, out var current) ||
                current.Id != sessionId ||
                !_offers.TryGetValue(creditId, out var offer) ||
                offer.Session != sessionId || offer.Shard != shard)
                return false;
            _offers.Remove(creditId);
            _quietUntil[shard] = DateTime.UtcNow.AddMilliseconds(250);
            Wake();
            return true;
        }
    }

    public void Finished(Guid taskId)
    {
        lock (_sync)
        {
            if (_executing.Remove(taskId)) Wake();
        }
    }

    public void OfferFailed(Grant grant)
    {
        lock (_sync)
        {
            if (_offers.TryGetValue(grant.CreditId, out var offer) &&
                offer.Shard == grant.Shard && offer.Session == grant.SessionId)
            {
                _offers.Remove(grant.CreditId);
                Wake();
            }
        }
    }

    public async Task WaitForChangeAsync(CancellationToken ct)
    {
        // A short bounded tick also refreshes cooled-down shards, so a lost
        // notification cannot permanently stall the credit allocator.
        await _wake.WaitAsync(TimeSpan.FromMilliseconds(100), ct);
    }

    private void DropUndelivered(int shard)
    {
        foreach (var (id, offer) in _offers.Where(x => x.Value.Shard == shard).ToArray())
            _offers.Remove(id);
        _quietUntil.Remove(shard);
    }

    private void Wake()
    {
        if (_wake.CurrentCount == 0) _wake.Release();
    }

    private void ValidateShard(int shard)
    {
        if (shard < 0 || shard >= _shardCount)
            throw new ArgumentOutOfRangeException(nameof(shard));
    }
}
