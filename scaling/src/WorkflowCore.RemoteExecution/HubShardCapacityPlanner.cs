namespace WorkflowCore.RemoteExecution;

/// <summary>
/// In-memory execution-slot reservations for parallel matching long polls.
/// Every outstanding poll reserves exactly one available execution slot and
/// one distinct matching shard; no durable task or ownership lives here.
/// A completed poll relinquishes its reservation before the worker schedules
/// its returned (at most one) PostgreSQL-fenced lease.
/// This is process-local and is recreated on every worker restart.
/// </summary>
public sealed class HubShardCapacityPlanner
{
    private readonly int _capacity;
    private readonly int _maxPolls;
    private readonly int _shards;
    private readonly HashSet<int> _reserved = [];
    private int _nextShard;

    public HubShardCapacityPlanner(int capacity, int shards, int maxPolls, int initialShard = 0)
    {
        if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity));
        if (shards is < 1 or > 64) throw new ArgumentOutOfRangeException(nameof(shards));
        if (maxPolls < 1 || maxPolls > shards || maxPolls > capacity)
            throw new ArgumentOutOfRangeException(nameof(maxPolls));
        _capacity = capacity;
        _shards = shards;
        _maxPolls = maxPolls;
        _nextShard = ((initialShard % shards) + shards) % shards;
    }

    public int ReservedPolls => _reserved.Count;

    /// <summary>
    /// The caller must process all completed poll replies before reserving
    /// further slots. A poll uses MaxTasks=1, so at any instant
    /// executing + reserved is bounded by the configured worker capacity.
    /// </summary>
    public bool TryReserve(int executing, out int shard)
    {
        if (executing < 0 || executing > _capacity)
            throw new ArgumentOutOfRangeException(nameof(executing));
        shard = -1;
        if (_reserved.Count >= _maxPolls || executing + _reserved.Count >= _capacity)
            return false;
        for (var i = 0; i < _shards; i++)
        {
            var candidate = _nextShard;
            _nextShard = (_nextShard + 1) % _shards;
            if (_reserved.Add(candidate))
            {
                shard = candidate;
                return true;
            }
        }
        return false;
    }

    public void Release(int shard)
    {
        if (!_reserved.Remove(shard))
            throw new InvalidOperationException("Matching poll shard was not reserved");
    }
}
