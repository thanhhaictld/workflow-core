using System.Text.Json;

namespace WorkflowCore.Hub;

/// <summary>Current metadata published by independently deployed processor hosts.</summary>
public sealed record HubDefinitionRegistration(string HubId, string DefinitionId,
    int Version, int Partitions, int StepCount, JsonElement Metadata);
/// <summary>Serializable Hub-enforced activity concurrency policy. No arbitrary C# delegates.</summary>
public sealed record HubActivityRegistration(string HubId, string ActivityType,
    int Version, int Partitions, int HubConcurrency, int ActivityConcurrency,
    string? ResourceName = null, string? ResourceField = null,
    int ResourceCapacity = 1, bool StrictFifo = false, JsonElement? Contract = null);
public sealed record HubWorkflowRuntime(Guid Id, string HubId, string DefinitionId,
    int Version, int PartitionId, long Revision, string State,
    int NextStep, int StepCount, string? WaitingFor, JsonElement Input,
    JsonElement Results, JsonElement ProcessorState);
public sealed record HubTransitionRequest(string WorkerId, long Epoch,
    long ExpectedRevision, Guid TaskId, string Command,
    string? ActivityType = null, int ActivityVersion = 1,
    string? TimerKey = null, long? DelayMilliseconds = null,
    JsonElement? ProcessorState = null, JsonElement? ActivityInput = null);
public sealed record HubTransitionResult(Guid WorkflowId, long Revision,
    string State, bool Replayed);
public sealed record HubRemoteTimerMetrics(long Pending, long Due, long Fired);
