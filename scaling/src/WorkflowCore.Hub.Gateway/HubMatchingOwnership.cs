using Microsoft.AspNetCore.Http;
using System.Collections.Concurrent;
using System.Net.Http.Json;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// PostgreSQL-fenced matching ownership. One-owner-per-Hub remains supported
/// for legacy mode; sharded mode uses independent (hub, matchingPartition)
/// owner leases. Matching ownership never substitutes for task lease fencing.
/// </summary>
public sealed class HubMatchingOwnership : BackgroundService
{
    private readonly HubMatchingLeaseStore _legacy;
    private readonly HubMatchingPartitionLeaseStore _partitions;
    private readonly ILogger<HubMatchingOwnership> _logger;
    private readonly ConcurrentDictionary<string, HubMatchingLease> _owned = new();
    private readonly ConcurrentDictionary<string, byte> _configured = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<(string Hub, int Partition), HubMatchingPartitionLease>
        _partitionOwned = new();
    private readonly HttpClient _forwarder = new(new SocketsHttpHandler {
        AllowAutoRedirect = false,
        ConnectTimeout = TimeSpan.FromSeconds(2)
    }) { Timeout = TimeSpan.FromSeconds(27) };
    private readonly HttpClient _streamForwarder = new(new SocketsHttpHandler {
        AllowAutoRedirect = false,
        ConnectTimeout = TimeSpan.FromSeconds(2)
    }) { Timeout = Timeout.InfiniteTimeSpan };
    private readonly HashSet<string> _trustedPeers;
    private readonly string[] _orderedPeers;
    private readonly TimeSpan _leaseDuration = TimeSpan.FromSeconds(8);
    private readonly TimeSpan _renewInterval = TimeSpan.FromSeconds(2);

    public bool Enabled { get; }
    public bool Sharded => Enabled && PartitionCount > 1;
    public int PartitionCount { get; }
    public string NodeId { get; }
    public string AdvertiseUrl { get; }

    public HubMatchingOwnership(HubMatchingLeaseStore legacy,
        HubMatchingPartitionLeaseStore partitions,
        ILogger<HubMatchingOwnership> logger)
    {
        _legacy = legacy;
        _partitions = partitions;
        _logger = logger;
        Enabled = Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_ENABLED") is "true" or "1" or "True";
        var rawCount = Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_PARTITIONS");
        if (rawCount is not null && (!int.TryParse(rawCount, out var parsed) ||
            parsed is < 1 or > 64))
            throw new InvalidOperationException("WFC_HUB_MATCHING_PARTITIONS must be 1..64");
        PartitionCount = rawCount is null ? 1 : int.Parse(rawCount);
        NodeId = Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_NODE_ID") ??
            Environment.MachineName + "-" + Guid.NewGuid().ToString("N");
        AdvertiseUrl = (Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_ADVERTISE_URL") ?? "").TrimEnd('/');
        _trustedPeers = (Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_PEERS") ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(p => p.TrimEnd('/'))
            .ToHashSet(StringComparer.Ordinal);
        _orderedPeers = _trustedPeers.OrderBy(x => x, StringComparer.Ordinal).ToArray();
        if (Enabled && (!Uri.TryCreate(AdvertiseUrl, UriKind.Absolute, out var uri) ||
                        uri.Scheme is not ("http" or "https") ||
                        NodeId.Length > 128 || _trustedPeers.Count == 0 ||
                        !_trustedPeers.Contains(AdvertiseUrl) ||
                        _trustedPeers.Any(p => !Uri.TryCreate(p, UriKind.Absolute, out var peer) ||
                                               peer.Scheme is not ("http" or "https"))))
            throw new InvalidOperationException(
                "Matching HA requires trusted WFC_HUB_MATCHING_ADVERTISE_URL and WFC_HUB_MATCHING_PEERS");
    }

    public async Task<HubMatchingLease> ResolveAsync(string hub, CancellationToken ct)
    {
        var lease = await _legacy.AcquireOrReadAsync(hub, NodeId, AdvertiseUrl, _leaseDuration, ct);
        if (lease.OwnerId == NodeId) _owned[hub] = lease;
        else _owned.TryRemove(hub, out _);
        return lease;
    }

    public bool IsLocal(HubMatchingLease lease) =>
        lease.OwnerId == NodeId && lease.LeaseUntil > DateTime.UtcNow;

    public Task<HubMatchingLease?> ReadAsync(string hub, CancellationToken ct) =>
        _legacy.ReadAsync(hub, ct);

    public Task<bool> StillOwnsAsync(string hub, CancellationToken ct) =>
        _owned.TryGetValue(hub, out var lease)
            ? _legacy.OwnsAsync(hub, NodeId, lease.Epoch, ct)
            : Task.FromResult(false);

    // In a healthy two-Hub system different matching partitions prefer
    // different owners. This is an affinity hint, NOT authority; SQL is.
    public string PreferredUrl(int partition) =>
        _orderedPeers[partition % _orderedPeers.Length];

    public async Task<HubMatchingPartitionLease?> ResolvePartitionAsync(
        string hub, int partition, CancellationToken ct)
    {
        ValidatePartition(partition);
        if (!_configured.ContainsKey(hub))
        {
            await _partitions.EnsureConfigAsync(hub, PartitionCount, ct);
            _configured.TryAdd(hub, 0);
        }
        var current = await _partitions.ReadAsync(hub, partition, ct);
        if (current is not null && current.LeaseUntil > DateTime.UtcNow)
        {
            TrackPartition(hub, partition, current);
            return current;
        }
        // Let the preferred owner acquire lazily on its first forwarded poll.
        // The gateway falls back to electing locally if that owner is down.
        if (PreferredUrl(partition) != AdvertiseUrl)
            return null;
        return await AcquirePartitionAsync(hub, partition, ct);
    }

    public async Task<HubMatchingPartitionLease> AcquirePartitionAsync(
        string hub, int partition, CancellationToken ct)
    {
        ValidatePartition(partition);
        var lease = await _partitions.AcquireOrReadAsync(
            hub, partition, PartitionCount, NodeId, AdvertiseUrl, _leaseDuration, ct);
        TrackPartition(hub, partition, lease);
        return lease;
    }

    public Task<HubMatchingPartitionLease?> ReadPartitionAsync(
        string hub, int partition, CancellationToken ct) =>
        _partitions.ReadAsync(hub, partition, ct);

    public Task<IReadOnlyList<HubMatchingPartitionLease>> ListPartitionsAsync(
        string hub, CancellationToken ct) => _partitions.ListAsync(hub, ct);

    public bool IsLocal(HubMatchingPartitionLease lease) =>
        lease.OwnerId == NodeId && lease.LeaseUntil > DateTime.UtcNow;

    public Task<bool> StillOwnsPartitionAsync(
        string hub, int partition, CancellationToken ct) =>
        _partitionOwned.TryGetValue((hub, partition), out var lease)
            ? _partitions.OwnsAsync(hub, partition, NodeId, lease.Epoch, ct)
            : Task.FromResult(false);

    public Task<HubLeasedTask[]?> ForwardAsync(
        HubMatchingLease owner, HubClaimRequest claim, string authorization, CancellationToken ct) =>
        ForwardToAsync(owner.OwnerUrl, claim, authorization, ct);

    public async Task<HubLeasedTask[]?> ForwardToAsync(
        string url, HubClaimRequest claim, string authorization, CancellationToken ct)
    {
        url = url.TrimEnd('/');
        if (!_trustedPeers.Contains(url))
            throw new InvalidOperationException("Matching owner URL is not in trusted peer set");
        using var message = new HttpRequestMessage(HttpMethod.Post,
            url + "/api/hub/workers/tasks:poll");
        message.Headers.TryAddWithoutValidation("Authorization", authorization);
        message.Headers.Add("X-Wfc-Matching-Forwarded", "1");
        message.Content = JsonContent.Create(claim);
        using var response = await _forwarder.SendAsync(message, ct);
        if (response.IsSuccessStatusCode)
            return await response.Content.ReadFromJsonAsync<HubLeasedTask[]>(cancellationToken: ct);
        _logger.LogWarning("Matching peer {Url} rejected poll with HTTP {Status}",
            url, (int)response.StatusCode);
        return null;
    }

    /// <summary>
    /// Forward SSE without buffering it as an ordinary claim response.
    /// Clients use the same load-balanced gateway URL; routing to a per-shard
    /// owner remains a trusted internal forwarding operation.
    /// </summary>
    public async Task ForwardSseToAsync(string url,
        HubSseTaskDistributor.Subscribe request, string authorization,
        HttpContext context, CancellationToken ct)
    {
        url = url.TrimEnd('/');
        if (!_trustedPeers.Contains(url))
            throw new InvalidOperationException("SSE owner URL is not a trusted peer");
        using var message = new HttpRequestMessage(HttpMethod.Post,
            url + "/api/hub/workers/tasks:stream");
        message.Headers.TryAddWithoutValidation("Authorization", authorization);
        message.Headers.Add("X-Wfc-Matching-Forwarded", "1");
        message.Content = JsonContent.Create(request);
        using var response = await _streamForwarder.SendAsync(message,
            HttpCompletionOption.ResponseHeadersRead, ct);
        context.Response.StatusCode = (int)response.StatusCode;
        if (!response.IsSuccessStatusCode) return;
        context.Response.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-cache, no-transform";
        context.Response.Headers["X-Accel-Buffering"] = "no";
        await context.Response.StartAsync(ct);
        await response.Content.CopyToAsync(context.Response.Body, ct);
    }

    public async Task<bool> ForwardSseCreditToAsync(string url,
        HubSseTaskDistributor.Credit credit, string authorization, CancellationToken ct)
    {
        url = url.TrimEnd('/');
        if (!_trustedPeers.Contains(url))
            throw new InvalidOperationException("Credit owner URL is not a trusted peer");
        using var message = new HttpRequestMessage(HttpMethod.Post,
            url + "/api/hub/workers/tasks:credit");
        message.Headers.TryAddWithoutValidation("Authorization", authorization);
        message.Headers.Add("X-Wfc-Matching-Forwarded", "1");
        message.Content = JsonContent.Create(credit);
        using var response = await _forwarder.SendAsync(message, ct);
        return response.IsSuccessStatusCode;
    }

    private void ValidatePartition(int partition)
    {
        if (partition < 0 || partition >= PartitionCount)
            throw new ArgumentException("Matching partition outside configured range");
    }

    private void TrackPartition(string hub, int partition, HubMatchingPartitionLease lease)
    {
        if (lease.OwnerId == NodeId && lease.LeaseUntil > DateTime.UtcNow)
            _partitionOwned[(hub, partition)] = lease;
        else _partitionOwned.TryRemove((hub, partition), out _);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!Enabled) return;
        try
        {
            using var timer = new PeriodicTimer(_renewInterval);
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                foreach (var (hub, lease) in _owned.ToArray())
                {
                    try
                    {
                        if (await _legacy.RenewAsync(hub, NodeId, lease.Epoch,
                                _leaseDuration, stoppingToken))
                            _owned[hub] = lease with { LeaseUntil = DateTime.UtcNow.Add(_leaseDuration) };
                        else _owned.TryRemove(hub, out _);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                    catch (Exception ex)
                    {
                        _owned.TryRemove(hub, out _);
                        _logger.LogWarning(ex, "Matching owner lease renewal failed for {Hub}", hub);
                    }
                }
                foreach (var (key, lease) in _partitionOwned.ToArray())
                {
                    try
                    {
                        if (await _partitions.RenewAsync(key.Hub, key.Partition, NodeId,
                                lease.Epoch, _leaseDuration, stoppingToken))
                            _partitionOwned[key] = lease with { LeaseUntil = DateTime.UtcNow.Add(_leaseDuration) };
                        else _partitionOwned.TryRemove(key, out _);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                    catch (Exception ex)
                    {
                        _partitionOwned.TryRemove(key, out _);
                        _logger.LogWarning(ex, "Matching partition lease renewal failed for {Hub}/{Partition}",
                            key.Hub, key.Partition);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            // Graceful shutdown makes other replicas eligible immediately.
            // Hard crashes rely on expiry; old owner epochs cannot renew.
            foreach (var (key, lease) in _partitionOwned.ToArray())
            {
                try { await _partitions.ReleaseAsync(key.Hub, key.Partition, NodeId,
                    lease.Epoch, CancellationToken.None); }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Could not release matching partition {Hub}/{Partition}",
                        key.Hub, key.Partition);
                }
            }
        }
    }

    public override void Dispose()
    {
        _forwarder.Dispose();
        base.Dispose();
    }
}
