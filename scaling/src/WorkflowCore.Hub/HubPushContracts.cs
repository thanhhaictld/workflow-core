namespace WorkflowCore.Hub;

// The session and delivery identifiers are ephemeral transport identities;
// neither is a substitute for the durable task epoch.
public sealed record HubPushRegistration(string WorkerId, string HubId, int Partition,
    int PartitionCount, HubCapability[] Capabilities, HubWorkflowCapability[]? Workflows, int MaxCredits = 1);
public sealed record HubPushSession(Guid SessionId, string OwnerUrl, long OwnerEpoch, int TtlSeconds);
public sealed record HubPushCapacity(long Sequence, int FreeCredits, Guid? ReleasedDeliveryId = null,
    Guid[]? SeenDeliveryIds = null, Guid[]? ReleasedDeliveryIds = null);
public sealed record HubPushAssignment(Guid SessionId, Guid DeliveryId, string WorkerId, HubLeasedTask Task);
