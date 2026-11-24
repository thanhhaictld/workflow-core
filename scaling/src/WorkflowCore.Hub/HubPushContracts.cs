using System.Text.Json.Serialization;

namespace WorkflowCore.Hub;

// The session and delivery identifiers are ephemeral transport identities;
// neither is a substitute for the durable task epoch.
public sealed record HubPushRegistration(string WorkerId, string HubId, int Partition,
    int PartitionCount, HubCapability[] Capabilities, HubWorkflowCapability[]? Workflows, int MaxCredits = 1);
public sealed record HubPushSession(Guid SessionId, string OwnerUrl, long OwnerEpoch, int TtlSeconds);
public sealed record HubPushCapacity(long Sequence, int FreeCredits, Guid? ReleasedDeliveryId = null,
    Guid[]? SeenDeliveryIds = null, Guid[]? ReleasedDeliveryIds = null);
public sealed record HubPushAssignment(Guid SessionId, Guid DeliveryId, string WorkerId, HubLeasedTask Task);

public enum HubPushTaskKind { Activity, Workflow }
public sealed record HubPushShardLocation(int Partition, string PreferredUrl, HubMatchingPartitionLease? Owner);
public sealed record HubPushCapabilities(string HubId, HubPushTaskKind Kind,
    HubCapability[] Tasks, HubWorkflowCapability[]? Workflows = null);
public sealed record HubPushProcessorRegistration(Guid ProcessorId, int PartitionCount,
    int ActivitySlots, int WorkflowSlots, HubPushCapabilities[] Capabilities);
public sealed record HubPushOwnerSession(Guid SessionId, Guid InstanceId, string OwnerId);
public sealed record HubPushGrant(long Sequence, string HubId, int Partition, long OwnerEpoch,
    HubPushTaskKind Kind, int Credits);
public sealed record HubPushGrantResult(Guid SessionId, HubPushGrant Grant, HubLeasedTask[] Tasks,
    bool Uncertain = false);
public sealed record HubPushGrantRecovery(HubPushOwnerSession Session, HubPushGrant Grant)
{
    // A one-shot grant owns at most Credits durable leases across all outcomes.
    // Its identity also scopes completion and heartbeat fencing.
    [JsonIgnore]
    public string LeaseOwner => FormattableString.Invariant($"push:{Session.SessionId:N}:{Grant.Sequence}");
}
