namespace WorkflowCore.Hub;

/// <summary>
/// Code-first, declarative *remote activity chain* definition. This is not
/// a replacement for IWorkflow&lt;T&gt;, nor can it encode arbitrary branches.
/// Each completed step atomically makes the next durable step claimable.
/// </summary>
public sealed record HubWorkflowStep(string ActivityType, int Version);
public sealed record HubWorkflowDefinition(string HubId, string DefinitionId,
    int Version, int Partitions, IReadOnlyList<HubWorkflowStep> Steps);
public sealed class HubWorkflowBuilder
{
    private readonly List<HubWorkflowStep> _steps = new();
    public HubWorkflowBuilder Activity(string activityType, int version = 1)
    {
        if (string.IsNullOrWhiteSpace(activityType) || version < 1)
            throw new ArgumentException("Invalid activity type/version");
        _steps.Add(new(activityType, version));
        return this;
    }
    public HubWorkflowBuilder Then(string activityType, int version = 1) =>
        Activity(activityType, version);
    internal IReadOnlyList<HubWorkflowStep> Build()
    {
        if (_steps.Count is < 1 or > 32)
            throw new ArgumentException("A Hub workflow requires 1..32 activities");
        return _steps.ToArray();
    }
}

public sealed class HubWorkflowRegistry
{
    private readonly Dictionary<(string Hub, string Definition, int Version),
        HubWorkflowDefinition> _definitions = new();

    public HubWorkflowRegistry Register(string hub, string definition, int version,
        int partitions, Action<HubWorkflowBuilder> configure)
    {
        if (string.IsNullOrWhiteSpace(hub) || string.IsNullOrWhiteSpace(definition) ||
            hub.Length > 80 || definition.Length > 80 ||
            version < 1 || partitions is < 1 or > 4096)
            throw new ArgumentException("Invalid Hub workflow definition");
        var builder = new HubWorkflowBuilder();
        configure(builder);
        _definitions.Add((hub, definition, version),
            new(hub, definition, version, partitions, builder.Build()));
        return this;
    }

    public HubWorkflowDefinition? Get(string hub, string definition, int version) =>
        _definitions.GetValueOrDefault((hub, definition, version));

    public IReadOnlyList<HubWorkflowDefinition> Describe() =>
        _definitions.Values.OrderBy(x => x.HubId).ThenBy(x => x.DefinitionId)
            .ThenBy(x => x.Version).ToArray();
}
