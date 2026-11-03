using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace WorkflowCore.Hub;

/// <summary>A code-first policy definition. Only the Hub resolves resource keys.
/// Workers may not submit or alter concurrency rules.</summary>
public sealed record HubResourceRule(string Key, int Capacity, bool StrictFifo);
public sealed record HubPolicyView(string HubId, string TaskType, int Version,
    int PartitionCount, IReadOnlyList<string> Rules);
public sealed record HubTaskRoute(string HubId, string TaskType, int Version,
    int PartitionCount, IReadOnlyList<HubResourceRule> Resources);

internal sealed record PolicyRule(
    string Name, int Capacity, bool StrictFifo, Func<JsonElement, string?> Selector);

public sealed class HubTaskPolicyBuilder
{
    private readonly List<PolicyRule> _rules = new();

    public HubTaskPolicyBuilder MaxHubConcurrency(int limit)
    {
        Add("hub", limit, false, _ => "all");
        return this;
    }

    public HubTaskPolicyBuilder MaxActivityConcurrency(int limit)
    {
        Add("activity", limit, false, _ => "all");
        return this;
    }

    public HubTaskPolicyBuilder ExclusiveBy(string resource, Func<JsonElement, string?> key)
    {
        Add("exclusive:" + resource, 1, false, key);
        return this;
    }

    public HubTaskPolicyBuilder SequentialBy(string resource, Func<JsonElement, string?> key)
    {
        Add("sequential:" + resource, 1, true, key);
        return this;
    }

    public HubTaskPolicyBuilder LimitBy(string resource, int limit,
        Func<JsonElement, string?> key)
    {
        Add("limited:" + resource, limit, false, key);
        return this;
    }

    private void Add(string name, int capacity, bool fifo, Func<JsonElement, string?> selector)
    {
        if (capacity <= 0 || string.IsNullOrWhiteSpace(name))
            throw new ArgumentOutOfRangeException(nameof(capacity));
        _rules.Add(new(name, capacity, fifo, selector));
    }

    internal IReadOnlyList<PolicyRule> Build() => _rules.ToArray();
}

public sealed class HubPolicyRegistry
{
    private sealed record Definition(string Hub, string TaskType, int Version,
        int PartitionCount, IReadOnlyList<PolicyRule> Rules);

    private readonly Dictionary<(string Hub, string TaskType, int Version), Definition> _defs = new();

    public HubPolicyRegistry Register(string hub, string taskType, int version,
        int partitions, Action<HubTaskPolicyBuilder> configure)
    {
        if (string.IsNullOrWhiteSpace(hub) || string.IsNullOrWhiteSpace(taskType) ||
            hub.Length > 80 || taskType.Length > 80 || version < 1 ||
            partitions < 1 || partitions > 4096)
            throw new ArgumentException("Invalid Hub task registration");
        var builder = new HubTaskPolicyBuilder();
        configure(builder);
        var key = (hub, taskType, version);
        _defs.Add(key, new(hub, taskType, version, partitions, builder.Build()));
        return this;
    }

    public HubTaskRoute Resolve(string hub, string taskType, int version, JsonElement payload)
    {
        if (!_defs.TryGetValue((hub, taskType, version), out var def))
            throw new KeyNotFoundException("Hub task type/version is not registered in C#");
        var resources = new Dictionary<string, HubResourceRule>(StringComparer.Ordinal);
        foreach (var rule in def.Rules)
        {
            var value = rule.Selector(payload);
            if (string.IsNullOrWhiteSpace(value))
                throw new ArgumentException("Missing policy resource key: " + rule.Name);
            // Avoid putting raw tenant IDs or other sensitive routing keys in SQL.
            var digest = Convert.ToHexString(SHA256.HashData(
                Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
            var scope = rule.Name == "hub" ? $"hub:{def.Hub}" :
                rule.Name == "activity" ? $"activity:{def.Hub}:{def.TaskType}" :
                $"resource:{def.Hub}:{rule.Name}";
            var key = $"{scope}:{digest}";
            if (resources.ContainsKey(key))
                throw new InvalidOperationException("Duplicate resource constraint: " + rule.Name);
            resources.Add(key, new(key, rule.Capacity, rule.StrictFifo));
        }
        return new HubTaskRoute(def.Hub, def.TaskType, def.Version,
            def.PartitionCount, resources.Values.OrderBy(x => x.Key, StringComparer.Ordinal).ToArray());
    }

    public IReadOnlyList<HubPolicyView> Describe() => _defs.Values
        .OrderBy(x => x.Hub).ThenBy(x => x.TaskType)
        .Select(x => new HubPolicyView(x.Hub, x.TaskType, x.Version, x.PartitionCount,
            x.Rules.Select(r => $"{r.Name}: capacity={r.Capacity}, fifo={r.StrictFifo}").ToArray()))
        .ToArray();
}

public static class HubPartitionRouter
{
    // The persisted partition is immutable. Increasing partition count for an
    // existing hub requires an explicit, versioned migration, not rehashing.
    public static int ForWorkflow(Guid workflowId, int partitionCount)
    {
        if (partitionCount < 1) throw new ArgumentOutOfRangeException(nameof(partitionCount));
        var digest = SHA256.HashData(workflowId.ToByteArray());
        return (int)(BitConverter.ToUInt32(digest, 0) % (uint)partitionCount);
    }
}
