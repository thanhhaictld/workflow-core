using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Client;

namespace WorkflowCore.RemoteExecution;

/// <summary>Shares a processor's execution budget across namespaces, shards and owner streams.</summary>
internal sealed class HubPushProcessor(
    HubRestClient client, HubRuntimeOptions options, HubRuntimeStartup startup,
    HubNativeActivityWorker activities, HubNativeWorkflowWorker workflows,
    ILogger<HubPushProcessor> logger) : BackgroundService
{
    private const string Prefix = "api/hub/workers/multiplexed-push/";

    private sealed class Route(HubPushCapabilities capabilities, int partition)
    {
        public HubPushCapabilities Capabilities { get; } = capabilities;
        public int Partition { get; } = partition;
        public HubMatchingPartitionLease? Owner { get; set; }
        public Uri? Target { get; set; }
        public int Pending { get; set; }
        public long LastOffered { get; set; }
        public DateTime NextProbe { get; set; }
        public DateTime HotUntil { get; set; }
    }

    private sealed class Connection(Uri target, string owner, CancellationToken shutdown) : IDisposable
    {
        public Uri Target { get; } = target;
        public string Owner { get; } = owner;
        public CancellationTokenSource Stop { get; } = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
        public Task Run { get; set; } = Task.CompletedTask;
        public Task? Submission { get; set; }
        public HubPushOwnerSession? Session { get; set; }
        public long Sequence { get; set; }
        public DateTime LastSend { get; set; }
        public void Dispose() => Stop.Dispose();
    }

    private sealed class GrantState(HubPushCreditJournal.Offer offer, Route? route, Connection? connection)
    {
        public HubPushCreditJournal.Offer Offer { get; } = offer;
        public Route? Route { get; } = route;
        public Connection? Connection { get; } = connection;
        public int Reserved { get; set; } = offer.Recovery.Grant.Credits;
        public int Running { get; set; }
        public bool Received { get; set; }
        public bool NeedsRecovery { get; set; } = connection is null;
    }

    private readonly object _gate = new();
    private readonly Dictionary<Uri, Connection> _connections = new();
    private readonly Dictionary<(Guid Session, long Sequence), GrantState> _grants = new();
    private readonly Dictionary<(Guid Task, long Epoch), Task> _executions = new();
    private long _offerOrder;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var capabilities = activities.Capabilities.Concat(workflows.Capabilities).ToArray();
        if (capabilities.Length == 0) return;
        if (!options.PushEnabled)
            throw new InvalidOperationException("Native Hub workers require WFC_HUB_PUSH_ENABLED=true on the worker and Hub");
        await startup.Published.WaitAsync(stoppingToken);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        using var journal = new HubPushCreditJournal(options);
        var registration = new HubPushProcessorRegistration(journal.Initial.ProcessorId, options.MatchingPartitions,
            options.ActivityConcurrency, options.WorkflowConcurrency, capabilities);
        var routes = capabilities.SelectMany(c => Enumerable.Range(0, options.MatchingPartitions)
            .Select(partition => new Route(c, partition))).ToArray();
        foreach (var offer in journal.Initial.Offers)
            _grants.Add(Key(offer.Recovery), new GrantState(offer, null, null));
        var discover = RefreshTopologyAsync(routes.GroupBy(r => r.Capabilities.HubId).ToArray(), stop.Token);
        var recover = ReconcileAsync(journal.Initial.Offers, stop.Token);
        try
        {
            using var tick = new PeriodicTimer(TimeSpan.FromMilliseconds(20));
            while (await tick.WaitForNextTickAsync(stop.Token))
            {
                lock (_gate)
                {
                    RefreshConnections(routes, registration, stop.Token);
                    OfferCredits(routes, journal);
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        finally
        {
            await stop.CancelAsync();
            await Task.WhenAll(discover, recover);
            await Task.WhenAll(_connections.Values.Select(c => c.Run));
            Task[] executions;
            lock (_gate) executions = _executions.Values.ToArray();
            await Task.WhenAll(executions);
            journal.Save(OutstandingOffers());
            foreach (var connection in _connections.Values) connection.Dispose();
        }
    }

    private async Task RefreshTopologyAsync(IGrouping<string, Route>[] namespaces, CancellationToken ct)
    {
        try
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
            do
            {
                await Parallel.ForEachAsync(namespaces, new ParallelOptions { CancellationToken = ct, MaxDegreeOfParallelism = 4 },
                    async (group, token) => await DiscoverNamespaceAsync(group, token));
            } while (await timer.WaitForNextTickAsync(ct));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    private async Task DiscoverNamespaceAsync(IGrouping<string, Route> routes, CancellationToken ct)
    {
        var query = $"?hub={Uri.EscapeDataString(routes.Key)}&partitionCount={options.MatchingPartitions}";
        try
        {
            using var timeout = Deadline(ct);
            var topology = await client.GetAsync<HubPushShardLocation[]>($"{Prefix}topology{query}", timeout.Token);
            await Parallel.ForEachAsync(topology, new ParallelOptions { CancellationToken = ct, MaxDegreeOfParallelism = 8 },
                async (shard, token) =>
            {
                try
                {
                    var owner = shard.Owner;
                    if (owner is null)
                    {
                        var preferred = OwnerUrl(shard.PreferredUrl);
                        var path = $"{Prefix}topology/{shard.Partition}{query}";
                        try { owner = await ElectAsync(new Uri(preferred, path), token); }
                        catch (Exception) when (!token.IsCancellationRequested && preferred != client.Http.BaseAddress)
                        { owner = await ElectAsync(new Uri(client.Http.BaseAddress!, path), token); }
                    }
                    if (owner.HubId != routes.Key || owner.Partition != shard.Partition || owner.Epoch < 1)
                        throw new InvalidDataException("Invalid matching shard owner");
                    var target = OwnerUrl(owner.OwnerUrl);
                    lock (_gate)
                        foreach (var route in routes.Where(r => r.Partition == shard.Partition))
                        {
                            if (route.Owner?.Epoch != owner.Epoch || route.Owner?.OwnerId != owner.OwnerId)
                            {
                                route.HotUntil = default;
                                route.NextProbe = default;
                            }
                            route.Owner = owner;
                            route.Target = target;
                        }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "Could not resolve push owner for {Hub}/{Shard}", routes.Key, shard.Partition);
                    lock (_gate)
                        foreach (var route in routes.Where(r => r.Partition == shard.Partition))
                        {
                            route.Owner = null;
                            route.Target = null;
                        }
                }
            });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Could not refresh push topology for {Hub}", routes.Key);
        }
    }

    private async Task<HubMatchingPartitionLease> ElectAsync(Uri endpoint, CancellationToken ct)
    {
        using var timeout = Deadline(ct);
        using var response = await client.Http.PostAsync(endpoint, null, timeout.Token);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<HubMatchingPartitionLease>(timeout.Token)
            ?? throw new InvalidDataException("Empty push ownership response");
    }

    private void RefreshConnections(Route[] routes, HubPushProcessorRegistration registration, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var owners = routes.Where(r => r.Target is not null && r.Owner!.LeaseUntil > now)
            .Select(r => (Target: r.Target!, Owner: r.Owner!.OwnerId)).Distinct().ToLookup(r => r.Target);
        foreach (var existing in _connections.Values.ToArray())
        {
            if (existing.Run.IsCompleted)
            {
                _connections.Remove(existing.Target);
                existing.Dispose();
            }
            else if (!owners[existing.Target].Any(r => r.Owner == existing.Owner))
                _ = existing.Stop.CancelAsync();
        }
        foreach (var owner in owners)
        {
            if (_connections.ContainsKey(owner.Key)) continue;
            var connection = new Connection(owner.Key, owner.First().Owner, ct);
            _connections.Add(owner.Key, connection);
            connection.Run = RunConnectionAsync(connection, registration, ct);
        }
    }

    private async Task RunConnectionAsync(Connection connection, HubPushProcessorRegistration registration, CancellationToken shutdown)
    {
        var ct = connection.Stop.Token;
        HubPushOwnerSession? session = null;
        try
        {
            using var timeout = Deadline(ct);
            using var response = await client.Http.PostAsJsonAsync(
                new Uri(connection.Target, Prefix + "sessions"), registration, timeout.Token);
            response.EnsureSuccessStatusCode();
            session = await response.Content.ReadFromJsonAsync<HubPushOwnerSession>(timeout.Token)
                ?? throw new InvalidDataException("Missing processor push session");
            if (session.OwnerId != connection.Owner || session.SessionId == Guid.Empty || session.InstanceId == Guid.Empty)
                throw new InvalidDataException("Push session is not on the matching owner");
            using var streamResponse = await client.Http.GetAsync(SessionUrl(connection.Target, session, "/events"),
                HttpCompletionOption.ResponseHeadersRead, timeout.Token);
            streamResponse.EnsureSuccessStatusCode();
            await using var stream = await streamResponse.Content.ReadAsStreamAsync(ct);
            using var reader = new StreamReader(stream);
            lock (_gate) connection.Session = session;
            while (!ct.IsCancellationRequested)
            {
                using var idle = CancellationTokenSource.CreateLinkedTokenSource(ct);
                idle.CancelAfter(TimeSpan.FromSeconds(12));
                var line = await reader.ReadLineAsync(idle.Token);
                if (line is null) break;
                if (!line.StartsWith("data: ", StringComparison.Ordinal)) continue;
                var frame = JsonSerializer.Deserialize<HubPushGrantResult>(line.AsSpan(6));
                if (frame is not null) Receive(session, frame, shutdown);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Push owner connection to {Owner} ended", connection.Target);
        }
        finally
        {
            Task? submission;
            lock (_gate)
            {
                connection.Session = null;
                submission = connection.Submission;
                foreach (var grant in _grants.Values.Where(g => g.Connection == connection && !g.Received))
                    MarkReceived(grant, 0, uncertain: true);
            }
            await connection.Stop.CancelAsync();
            if (submission is not null) await submission;
            if (session is not null) await CloseAsync(connection.Target, session);
            if (!shutdown.IsCancellationRequested)
            {
                HubTelemetry.SseTaskReconnects.Add(1);
                await Task.Delay(500);
            }
        }
    }

    private void OfferCredits(Route[] routes, HubPushCreditJournal journal)
    {
        var now = DateTime.UtcNow;
        var batches = new Dictionary<Connection, List<HubPushGrant>>();
        Fill(HubPushTaskKind.Activity, options.ActivityConcurrency);
        Fill(HubPushTaskKind.Workflow, options.WorkflowConcurrency);
        if (batches.Count != 0) journal.Save(OutstandingOffers());
        foreach (var connection in _connections.Values)
        {
            if (connection.Session is not { } session || connection.Stop.IsCancellationRequested ||
                connection.Submission is { IsCompleted: false }) continue;
            if (!batches.TryGetValue(connection, out var batch) && now - connection.LastSend < TimeSpan.FromSeconds(4))
                continue;
            connection.LastSend = now;
            connection.Submission = SubmitAsync(connection, session, batch?.ToArray() ?? []);
        }

        void Fill(HubPushTaskKind kind, int capacity)
        {
            var free = capacity - _grants.Values.Where(g => g.Offer.Recovery.Grant.Kind == kind).Sum(g => g.Reserved);
            if (free <= 0) return;
            var eligible = routes.Where(r => r.Capabilities.Kind == kind && r.Target is not null &&
                    r.Owner!.LeaseUntil > now && r.NextProbe <= now)
                .Select(r => (Route: r, Connection: _connections.GetValueOrDefault(r.Target!)))
                .Where(x => x.Connection is { Session: not null } conn && conn.Owner == x.Route.Owner!.OwnerId &&
                    !conn.Stop.IsCancellationRequested && conn.Submission is not { IsCompleted: false }).ToArray();
            while (free > 0)
            {
                var (route, connection) = eligible.Where(x => x.Route.HotUntil > now || x.Route.Pending == 0)
                    .OrderBy(x => x.Route.LastOffered).FirstOrDefault();
                if (route is null || connection is null) break;
                var credits = route.HotUntil > now ? Math.Min(4, free) : 1;
                var grant = new HubPushGrant(++connection.Sequence, route.Capabilities.HubId,
                    route.Partition, route.Owner!.Epoch, kind, credits);
                var recovery = new HubPushGrantRecovery(connection.Session!, grant);
                var state = new GrantState(new HubPushCreditJournal.Offer(connection.Target.AbsoluteUri, recovery),
                    route, connection);
                _grants.Add(Key(recovery), state);
                if (!batches.TryGetValue(connection, out var batch)) batches.Add(connection, batch = []);
                batch.Add(grant);
                route.Pending++;
                route.LastOffered = ++_offerOrder;
                free -= credits;
            }
        }
    }

    private async Task SubmitAsync(Connection connection, HubPushOwnerSession session, HubPushGrant[] grants)
    {
        try
        {
            using var timeout = Deadline(connection.Stop.Token);
            using var response = await client.Http.PutAsJsonAsync(SessionUrl(connection.Target, session, "/grants"),
                grants, timeout.Token);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Push grant submission to {Owner} ended", connection.Target);
            await connection.Stop.CancelAsync();
        }
    }

    private void Receive(HubPushOwnerSession session, HubPushGrantResult frame, CancellationToken shutdown)
    {
        if (frame.SessionId != session.SessionId || frame.Grant is null) return;
        lock (_gate)
        {
            var key = (session.SessionId, frame.Grant.Sequence);
            if (!_grants.TryGetValue(key, out var state) || state.Received || state.Route is not { } route ||
                frame.Grant != state.Offer.Recovery.Grant) return;
            var now = DateTime.UtcNow;
            var valid = !frame.Uncertain && frame.Tasks is not null && frame.Tasks.Length <= state.Reserved &&
                frame.Tasks.All(t => t is not null && t.Id != Guid.Empty && t.Epoch > 0 && t.LeaseUntil > now &&
                    t.HubId == frame.Grant.HubId && t.PartitionId % options.MatchingPartitions == frame.Grant.Partition &&
                    route.Capabilities.Tasks.Any(c => c.TaskType == t.TaskType && c.Version == t.Version) &&
                    !_executions.ContainsKey((t.Id, t.Epoch))) &&
                frame.Tasks.Select(t => (t.Id, t.Epoch)).Distinct().Count() == frame.Tasks.Length;
            MarkReceived(state, valid ? frame.Tasks!.Length : 0, uncertain: !valid);
            if (!valid) return;
            if (state.Reserved == 0)
            {
                _grants.Remove(key);
                return;
            }
            var worker = frame.Grant.Kind == HubPushTaskKind.Activity ? (HubRuntimeWorker)activities : workflows;
            state.Running = frame.Tasks!.Length;
            foreach (var task in frame.Tasks)
                _executions.Add((task.Id, task.Epoch), Task.Run(() => ExecuteAssignmentAsync(worker, task, state, shutdown)));
        }
    }

    private static void MarkReceived(GrantState state, int assignments, bool uncertain)
    {
        state.Received = true;
        state.NeedsRecovery = uncertain;
        if (!uncertain) state.Reserved = assignments;
        if (state.Route is not { } route) return;
        route.Pending--;
        if (route.Owner?.Epoch != state.Offer.Recovery.Grant.OwnerEpoch ||
            route.Owner.OwnerId != state.Offer.Recovery.Session.OwnerId) return;
        var now = DateTime.UtcNow;
        if (assignments > 0) route.HotUntil = now.AddSeconds(2);
        else
        {
            route.HotUntil = default;
            route.NextProbe = now.AddMilliseconds(200);
        }
    }

    private async Task ExecuteAssignmentAsync(HubRuntimeWorker worker, HubLeasedTask task,
        GrantState state, CancellationToken ct)
    {
        var retired = false;
        try { retired = await worker.Run(task, state.Offer.Recovery.LeaseOwner, ct); }
        catch (Exception ex) { logger.LogError(ex, "Hub execution ended for {Task}", task.Id); }
        finally
        {
            lock (_gate)
            {
                _executions.Remove((task.Id, task.Epoch));
                state.Running--;
                if (retired) state.Reserved--;
                else state.NeedsRecovery = true;
                if (state.Reserved == 0) _grants.Remove(Key(state.Offer.Recovery));
            }
        }
    }

    private async Task ReconcileAsync(HubPushCreditJournal.Offer[] restored, CancellationToken ct)
    {
        try
        {
            await Task.WhenAll(restored.DistinctBy(o => o.Recovery.Session.SessionId)
                .Select(o => CloseRestoredAsync(o)));
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
            do
            {
                GrantState[] pending;
                lock (_gate) pending = _grants.Values.Where(g => g.NeedsRecovery && g.Running == 0).ToArray();
                await Parallel.ForEachAsync(pending, new ParallelOptions { CancellationToken = ct, MaxDegreeOfParallelism = 4 },
                    async (grant, token) =>
                    {
                        var offer = grant.Offer;
                        var retired = false;
                        try { retired = await RetiredAsync(new Uri(OwnerUrl(offer.OwnerUrl), Prefix + "retired"), offer, token); }
                        catch (Exception) when (!token.IsCancellationRequested)
                        {
                            try { retired = await RetiredAsync(new Uri(client.Http.BaseAddress!, Prefix + "retired"), offer, token); }
                            catch (Exception ex) when (!token.IsCancellationRequested)
                            { logger.LogDebug(ex, "Push grant retirement check is unavailable"); }
                        }
                        if (retired)
                            lock (_gate) _grants.Remove(Key(offer.Recovery));
                    });
            } while (await timer.WaitForNextTickAsync(ct));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }

        async Task CloseRestoredAsync(HubPushCreditJournal.Offer offer)
        {
            try { await CloseAsync(OwnerUrl(offer.OwnerUrl), offer.Recovery.Session); }
            catch (InvalidOperationException ex) { logger.LogDebug(ex, "Restored push owner URL is unavailable"); }
        }
    }

    private async Task<bool> RetiredAsync(Uri endpoint, HubPushCreditJournal.Offer offer, CancellationToken ct)
    {
        using var timeout = Deadline(ct);
        using var response = await client.Http.PostAsJsonAsync(endpoint, offer.Recovery, timeout.Token);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<bool>(timeout.Token);
    }

    private async Task CloseAsync(Uri target, HubPushOwnerSession session)
    {
        try
        {
            using var timeout = Deadline(CancellationToken.None);
            using var response = await client.Http.DeleteAsync(SessionUrl(target, session), timeout.Token);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex) { logger.LogDebug(ex, "Could not close push session on {Owner}", target); }
    }

    private static CancellationTokenSource Deadline(CancellationToken ct)
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(3));
        return timeout;
    }

    private Uri OwnerUrl(string url) => HubPushOwnerRoute.Validate(url, client.Http.BaseAddress!, options);
    private static Uri SessionUrl(Uri target, HubPushOwnerSession session, string suffix = "") =>
        new(target, $"{Prefix}sessions/{session.SessionId}{suffix}");
    private static (Guid, long) Key(HubPushGrantRecovery recovery) => (recovery.Session.SessionId, recovery.Grant.Sequence);
    private HubPushCreditJournal.Offer[] OutstandingOffers() => _grants.Values.Select(g => g.Offer).ToArray();
}
