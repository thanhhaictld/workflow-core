using System.Text.Json;

namespace WorkflowCore.Hub;

// Hub treats snapshots and activity payloads as opaque application data.
public sealed record HubNativeRuntime(int ProtocolVersion, Guid Id, string HubId,
    string DefinitionId, int Version, long Revision, string Status, JsonElement Input,
    JsonElement? Snapshot, HubNativeMessage[] Inbox, DateTime CreatedAt);
public sealed record HubNativeMessage(long Id, string PointerId, string Kind, JsonElement Payload);
public sealed record HubNativeSubscription(string PointerId, string EventName, string EventKey, DateTime AsOf);
public sealed record HubNativeDispatch(string DispatchKey, string PointerId, int StepId,
    string ActivityType, int Version, JsonElement Input, DateTime? NotBefore = null);
public sealed record HubNativeHistory(string EventType, string PointerId, int StepId,
    string? ParentId, string[] Scope, int Attempt, JsonElement? Payload = null, string? ActivityType = null);
public sealed record HubNativeTimer(string PointerId, int StepId);
public sealed record HubNativeCommit(int ProtocolVersion, string WorkerId, long Epoch,
    long ExpectedRevision, Guid TaskId, JsonElement Snapshot, string Status,
    bool Runnable, DateTime? WakeAt, long[] ConsumedInbox,
    HubNativeSubscription[] Subscriptions, HubNativeDispatch[] Activities, HubNativeHistory[] History,
    string[]? ActiveDispatches = null, HubNativeTimer? Timer = null);
public sealed record HubNativeEvent(string EventName, string EventKey, JsonElement Data,
    string IdempotencyKey, DateTime? EffectiveAt = null);
public sealed record HubNativeStatusChange(string Status, string IdempotencyKey);
