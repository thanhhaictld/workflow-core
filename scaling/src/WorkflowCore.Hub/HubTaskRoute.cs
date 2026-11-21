using System.Security.Cryptography;

namespace WorkflowCore.Hub;

public sealed record HubTaskRoute(string HubId, string TaskType, int Version, int PartitionCount);

public sealed class HubTaskRouteRegistry
{
    private readonly Dictionary<(string Hub, string TaskType, int Version), HubTaskRoute> _routes = new();

    public HubTaskRouteRegistry Register(string hub, string taskType, int version, int partitions)
    {
        if (string.IsNullOrWhiteSpace(hub) || string.IsNullOrWhiteSpace(taskType) ||
            hub.Length > 80 || taskType.Length > 80 ||
            hub.Contains('|') || taskType.Contains('|') || version < 1 ||
            partitions is < 1 or > 4096)
            throw new ArgumentException("Invalid Hub task registration");
        _routes.Add((hub, taskType, version), new(hub, taskType, version, partitions));
        return this;
    }

    public HubTaskRoute Resolve(string hub, string taskType, int version) =>
        _routes.TryGetValue((hub, taskType, version), out var route)
            ? route : throw new KeyNotFoundException("Hub task type/version is not registered in C#");
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
