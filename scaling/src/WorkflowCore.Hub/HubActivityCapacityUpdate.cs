namespace WorkflowCore.Hub;

/// <summary>Compare-and-swap operational capacity update performed by a policy operator.</summary>
public sealed record HubActivityCapacityUpdate(string HubId,string ActivityType,int Version,
    long ExpectedRevision,int HubConcurrency,int ActivityConcurrency,int ResourceCapacity,
    bool CoordinateSharedResources = false);
public sealed record HubActivityPolicySnapshot(string HubId,string ActivityType,int Version,
    int HubConcurrency,int ActivityConcurrency,int ResourceCapacity,long Revision);
