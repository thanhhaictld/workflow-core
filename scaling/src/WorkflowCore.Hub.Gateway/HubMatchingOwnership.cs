using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Net.Http.Headers;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Matching owner election and renewal shared across Hub gateway replicas.
/// One logical matching partition per hub namespace for the initial HA rollout.
/// SQL, not process-local state, decides ownership and lease generations.
/// </summary>
public sealed class HubMatchingOwnership : BackgroundService
{
    private readonly HubMatchingLeaseStore _store;
    private readonly ILogger<HubMatchingOwnership> _logger;
    private readonly ConcurrentDictionary<string, HubMatchingLease> _owned = new();
    private readonly HttpClient _forwarder = new() { Timeout = TimeSpan.FromSeconds(27) };
    private readonly HashSet<string> _trustedPeers;
    private readonly TimeSpan _leaseDuration = TimeSpan.FromSeconds(8);
    private readonly TimeSpan _renewInterval = TimeSpan.FromSeconds(2);

    public bool Enabled { get; }
    public string NodeId { get; }
    public string AdvertiseUrl { get; }

    public HubMatchingOwnership(HubMatchingLeaseStore store,
        ILogger<HubMatchingOwnership> logger)
    {
        _store = store;
        _logger = logger;
        Enabled = Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_ENABLED") is "true" or "1" or "True";
        NodeId = Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_NODE_ID") ??
            Environment.MachineName + "-" + Guid.NewGuid().ToString("N");
        AdvertiseUrl = (Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_ADVERTISE_URL") ?? "").TrimEnd('/');
        _trustedPeers = (Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_PEERS") ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(p => p.TrimEnd('/'))
            .ToHashSet(StringComparer.Ordinal);
        if (Enabled && (!Uri.TryCreate(AdvertiseUrl, UriKind.Absolute, out var uri) ||
                        uri.Scheme is not ("http" or "https") ||
                        NodeId.Length > 128 || _trustedPeers.Count == 0 ||
                        !_trustedPeers.Contains(AdvertiseUrl) ||
                        _trustedPeers.Any(p => !Uri.TryCreate(p, UriKind.Absolute, out var peer) ||
                                               peer.Scheme is not ("http" or "https"))))
            throw new InvalidOperationException(
                "Matching HA requires a trusted WFC_HUB_MATCHING_ADVERTISE_URL and WFC_HUB_MATCHING_PEERS");
    }

    public async Task<HubMatchingLease> ResolveAsync(string hub, CancellationToken ct)
    {
        var lease = await _store.AcquireOrReadAsync(hub, NodeId, AdvertiseUrl, _leaseDuration, ct);
        if (lease.OwnerId == NodeId) _owned[hub] = lease;
        else _owned.TryRemove(hub, out _);
        return lease;
    }

    public bool IsLocal(HubMatchingLease lease) =>
        lease.OwnerId == NodeId && lease.LeaseUntil > DateTime.UtcNow;

    public Task<HubMatchingLease?> ReadAsync(string hub, CancellationToken ct) =>
        _store.ReadAsync(hub, ct);

    public Task<bool> StillOwnsAsync(string hub, CancellationToken ct) =>
        _owned.TryGetValue(hub, out var lease)
            ? _store.OwnsAsync(hub, NodeId, lease.Epoch, ct)
            : Task.FromResult(false);

    public async Task<HubLeasedTask[]?> ForwardAsync(
        HubMatchingLease owner, HubClaimRequest claim, string authorization, CancellationToken ct)
    {
        // The DB owner URL must match deployment-configured peers. It cannot
        // redirect worker tokens to an arbitrary URL or follow HTTP redirects.
        var url = owner.OwnerUrl.TrimEnd('/');
        if (!_trustedPeers.Contains(url))
            throw new InvalidOperationException("Matching owner URL is not in the trusted peer set");
        using var message = new HttpRequestMessage(HttpMethod.Post,
            url + "/api/hub/workers/tasks:poll");
        message.Headers.TryAddWithoutValidation("Authorization", authorization);
        message.Headers.Add("X-Wfc-Matching-Forwarded", "1");
        message.Content = JsonContent.Create(claim);
        using var response = await _forwarder.SendAsync(message, ct);
        if (response.IsSuccessStatusCode)
            return await response.Content.ReadFromJsonAsync<HubLeasedTask[]>(cancellationToken: ct);
        // A crashed or lease-fenced owner is retried through the worker's
        // load-balanced Hub endpoint; never forward recursively.
        _logger.LogWarning("Matching peer {Owner} rejected poll with {Status}",
            owner.OwnerId, (int)response.StatusCode);
        return null;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!Enabled) return;
        using var timer = new PeriodicTimer(_renewInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            foreach (var (hub, lease) in _owned.ToArray())
            {
                try
                {
                    if (await _store.RenewAsync(hub, NodeId, lease.Epoch,
                            _leaseDuration, stoppingToken))
                        _owned[hub] = lease with { LeaseUntil = DateTime.UtcNow.Add(_leaseDuration) };
                    else
                        _owned.TryRemove(hub, out _);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // Never assume ownership following a failed renewal.
                    _owned.TryRemove(hub, out _);
                    _logger.LogWarning(ex, "Matching lease renewal failed for {Hub}", hub);
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
