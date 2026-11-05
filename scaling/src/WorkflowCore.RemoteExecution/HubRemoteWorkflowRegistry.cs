using System.Text.Json;
using WorkflowCore.Hub;

namespace WorkflowCore.RemoteExecution;

/// <summary>
/// Sequential v1 compatibility definitions. New fluent workflows use
/// AddHubWorkflowRuntime and the upstream IWorkflow interfaces.
/// </summary>
public sealed record RemoteWorkflowStep(
    string Kind,string ActivityType,int Version,long DelayMilliseconds);
public sealed record RemoteWorkflowDefinition(
    string HubId,string DefinitionId,int Version,int Partitions,
    IReadOnlyList<RemoteWorkflowStep> Steps);

public sealed class RemoteWorkflowBuilder
{
    private readonly List<RemoteWorkflowStep> _steps=[];
    public RemoteWorkflowBuilder Activity(string type,int version=1)
    {
        if(string.IsNullOrWhiteSpace(type)||type.Length>80||version<1)
            throw new ArgumentException("Invalid workflow activity");
        _steps.Add(new("Activity",type,version,0));
        return this;
    }
    public RemoteWorkflowBuilder Delay(string name,TimeSpan duration)
    {
        if(string.IsNullOrWhiteSpace(name)||name.Length>80||
           duration<TimeSpan.Zero||duration>TimeSpan.FromDays(365))
            throw new ArgumentException("Invalid workflow timer");
        _steps.Add(new("Delay",name,1,(long)duration.TotalMilliseconds));
        return this;
    }
    internal IReadOnlyList<RemoteWorkflowStep> Build()
    {
        if(_steps.Count is < 1 or > 128)throw new ArgumentException("Workflow must have 1..128 steps");
        return _steps.ToArray();
    }
}

public sealed class RemoteWorkflowRegistry
{
    private readonly Dictionary<(string Hub,string Definition,int Version),
        RemoteWorkflowDefinition> _definitions = [];
    public RemoteWorkflowRegistry Register(string hub,string definition,int version,
        int partitions,Action<RemoteWorkflowBuilder> configure)
    {
        if(string.IsNullOrWhiteSpace(hub)||hub.Length>80||
           string.IsNullOrWhiteSpace(definition)||definition.Length>80||
           version<1||partitions is < 1 or > 4096)
            throw new ArgumentException("Invalid workflow registration");
        var builder=new RemoteWorkflowBuilder();
        configure(builder);
        var key=(hub,definition,version);
        _definitions.Add(key,new(hub,definition,version,partitions,builder.Build()));
        return this;
    }
    public RemoteWorkflowDefinition? Find(string hub,string definition,int version)=>
        _definitions.GetValueOrDefault((hub,definition,version));
    public IReadOnlyList<RemoteWorkflowDefinition> All() => _definitions.Values.ToArray();

    public static HubDefinitionRegistration Metadata(RemoteWorkflowDefinition d)=>
        new(d.HubId,d.DefinitionId,d.Version,d.Partitions,d.Steps.Count,
            JsonSerializer.SerializeToElement(new {
                steps=d.Steps.Select(x=>new {
                    activityType=x.ActivityType,
                    version=x.Version,
                    kind=x.Kind,
                    delayMilliseconds=x.DelayMilliseconds
                }).ToArray()
            }));
}
