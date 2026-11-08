using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using WorkflowCore.Hub;
using WorkflowCore.Hub.Client;
using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace WorkflowCore.RemoteExecution;

public sealed class HubRuntimeOptions
{
    public string HubUrl { get; set; } = Environment.GetEnvironmentVariable("WFC_HUB_URL") ?? "http://localhost:5100";
    public string Token { get; set; } = Environment.GetEnvironmentVariable("WFC_HUB_AUTH_TOKEN") ?? "";
    public bool AllowInsecureHttp { get; set; } = Environment.GetEnvironmentVariable("WFC_HUB_ALLOW_INSECURE_HTTP")=="true";
    public int WorkflowConcurrency { get; set; } = 8;
    public int ActivityConcurrency { get; set; } = 16;
    // Opt-in processor-side cap. Applies independently to workflow and
    // activity lanes, including long-poll demand; 0 keeps legacy settings.
    public int WorkerConcurrencyCap { get; set; } =
        int.TryParse(Environment.GetEnvironmentVariable("WFC_HUB_WORKER_MAX_INFLIGHT_PER_LANE"),
            out var laneCap) ? laneCap : 0;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);
    // Only enable against matching-capable, load-balanced Hub replicas.
    // Polls wait server-side; failed long-poll requests retry with backoff.
    public bool LongPollingEnabled { get; set; } =
        Environment.GetEnvironmentVariable("WFC_HUB_LONG_POLL_ENABLED") is "true" or "1" or "True";
    // Every processor and Hub replica must use the same count. The Hub also
    // enforces an immutable per-namespace count stored in PostgreSQL.
    public int MatchingPartitions { get; set; } =
        int.TryParse(Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_PARTITIONS"),
            out var count) ? count : 1;
    // Feature flag for controlled baseline/optimized benchmark runs.
    public bool AdaptiveClaimsEnabled { get; set; } =
        Environment.GetEnvironmentVariable("WFC_HUB_ADAPTIVE_CLAIMS") is not ("0" or "false" or "False");
    public TimeSpan ErrorRetryInterval { get; set; } = TimeSpan.FromSeconds(60);
    public bool RegisterOnly { get; set; }
    public bool SequentialVersionOneCompatibility { get; set; }
}

public sealed class HubActivityOptions<TStep> where TStep : class, IStepBody
{
    internal List<PropertyInfo> Inputs { get; } = [];
    internal List<PropertyInfo> Outputs { get; } = [];
    internal List<PropertyInfo> SequentialOutputs { get; } = [];
    internal PropertyInfo? ResultProperty { get; private set; }
    internal PropertyInfo? ParametersProperty { get; private set; }
    public int Partitions { get; set; } = 32;
    public int HubConcurrency { get; set; } = 100;
    public int Concurrency { get; set; } = 100;
    public string? ResourceName { get; set; }
    public string? ResourceField { get; set; }
    public int ResourceCapacity { get; set; } = 1;
    public bool StrictFifo { get; set; }
    // New definitions should opt in explicitly. Legacy preserves older
    // registrations until they can be migrated without policy drift.
    public HubActivityOrdering Ordering { get; set; } = HubActivityOrdering.Legacy;
    public HubActivityOptions<TStep> Input<T>(Expression<Func<TStep,T>> property) { Inputs.Add(Property(property));return this; }
    public HubActivityOptions<TStep> Output<T>(Expression<Func<TStep,T>> property) { Outputs.Add(Property(property));return this; }
    public HubActivityOptions<TStep> SequentialOutput<T>(Expression<Func<TStep,T>> property) { SequentialOutputs.Add(Property(property));return this; }
    public HubActivityOptions<TStep> ActivityResult<T>(Expression<Func<TStep,T>> property)
    { ResultProperty=Property(property);return this; }
    public HubActivityOptions<TStep> ActivityParameters<T>(Expression<Func<TStep,T>> property)
    { ParametersProperty=Property(property);if(!Inputs.Contains(ParametersProperty))Inputs.Add(ParametersProperty);return this; }
    private static PropertyInfo Property(LambdaExpression expression)
    {
        if(expression.Body is not MemberExpression { Member: PropertyInfo p, Expression: ParameterExpression } ||
           p.GetMethod?.IsPublic!=true || p.SetMethod?.IsPublic!=true || p.GetIndexParameters().Length!=0)
            throw new ArgumentException("Select a public readable/writable step property");
        return p;
    }
}

internal sealed record HubWorkflowRegistration(string HubId, int Partitions, Type WorkflowType, Type DataType,
    Func<IWorkflowRegistry,IServiceProvider,WorkflowDefinition> Register);

public sealed record HubTypedActivity(Type StepType, HubActivityRegistration Policy,
    IReadOnlyList<PropertyInfo> Inputs, IReadOnlyList<PropertyInfo> Outputs, PropertyInfo? ResultProperty = null,
    PropertyInfo? ParametersProperty = null, IReadOnlyList<PropertyInfo>? SequentialOutputs = null)
{
    internal JsonElement ReadInputs(IStepBody body) => Read(body,Inputs);
    internal JsonElement ReadOutputs(IStepBody body) => Read(body,Outputs);
    internal JsonElement ReadSequentialOutputs(IStepBody body) => Read(body,SequentialOutputs is { Count: >0 }?SequentialOutputs:Outputs);
    internal static string PropertyName(PropertyInfo p) => JsonNamingPolicy.CamelCase.ConvertName(p.Name);
    internal static Dictionary<string,object?> ReadValues(IStepBody body,IReadOnlyList<PropertyInfo> properties) =>
        properties.ToDictionary(PropertyName,p=>p.GetValue(body));
    private static JsonElement Read(IStepBody body,IReadOnlyList<PropertyInfo> properties) =>
        JsonSerializer.SerializeToElement(ReadValues(body,properties),HubNativeExecutor.JsonOptions);
    internal static void ApplyValues(IStepBody body,object? values,IReadOnlyList<PropertyInfo> properties)
    {
        var dictionary=values as Dictionary<string,object?> ?? throw new InvalidOperationException("Invalid typed activity values");
        foreach(var property in properties)
        {
            if(!dictionary.TryGetValue(PropertyName(property),out var value))
                throw new InvalidOperationException("Missing typed activity property: "+property.Name);
            property.SetValue(body,value);
        }
    }
    internal static void Apply(IStepBody body,JsonElement payload,IReadOnlyList<PropertyInfo> properties)
    {
        foreach(var property in properties)
        {
            if(!payload.TryGetProperty(PropertyName(property),out var value))
                throw new InvalidOperationException("Missing activity property: "+property.Name);
            property.SetValue(body,value.Deserialize(property.PropertyType,HubNativeExecutor.JsonOptions));
        }
    }
}

public sealed class HubRuntimeRegistrationBuilder(IServiceCollection services)
{
    public HubRuntimeRegistrationBuilder AddWorkflow<TWorkflow,TData>(string hubId,int partitions=32)
        where TWorkflow : class,IWorkflow<TData> where TData : new()
    {
        services.TryAddTransient<TWorkflow>();
        services.AddSingleton(new HubWorkflowRegistration(hubId,partitions,typeof(TWorkflow),typeof(TData),
            (registry,sp)=>{var workflow=sp.GetRequiredService<TWorkflow>();registry.RegisterWorkflow(workflow);
                return registry.GetDefinition(workflow.Id,workflow.Version);}));
        return this;
    }

    public HubRuntimeRegistrationBuilder AddActivity<TStep>(string hubId,string name,int version,
        Action<HubActivityOptions<TStep>> configure) where TStep : class,IStepBody
    {
        var options=new HubActivityOptions<TStep>();configure(options);
        if(string.IsNullOrWhiteSpace(hubId)||hubId.Length>80||string.IsNullOrWhiteSpace(name)||name.Length>80||
           name.StartsWith("__",StringComparison.Ordinal)||version<1||options.Partitions is <1 or >4096||
           options.Concurrency<1||options.HubConcurrency<1||options.ResourceCapacity<1||
           !Enum.IsDefined(options.Ordering))
            throw new ArgumentException("Invalid typed activity registration");
        if(options.Inputs.Distinct().Count()!=options.Inputs.Count || options.Outputs.Distinct().Count()!=options.Outputs.Count ||
           options.SequentialOutputs.Distinct().Count()!=options.SequentialOutputs.Count)
            throw new ArgumentException("Duplicate activity properties");
        if(options.ParametersProperty is not null && options.Inputs.Count!=1)
            throw new ArgumentException("ActivityParameters maps the entire native Activity input to one declared property");
        if(options.ResourceField is not null && !options.Inputs.Any(p=>HubTypedActivity.PropertyName(p)==options.ResourceField))
            throw new ArgumentException("Resource field must be a declared activity input");
        var keyed = options.Ordering is HubActivityOrdering.KeyedExclusive or
            HubActivityOrdering.KeyedFifo or HubActivityOrdering.KeyedBounded;
        if(keyed && (string.IsNullOrWhiteSpace(options.ResourceName) ||
                     string.IsNullOrWhiteSpace(options.ResourceField)))
            throw new ArgumentException("Keyed activity ordering requires ResourceName and ResourceField");
        if((options.Ordering is HubActivityOrdering.Unordered or HubActivityOrdering.PartitionSerial) &&
           (options.ResourceName is not null || options.ResourceField is not null))
            throw new ArgumentException("Resource fields are invalid for unordered/partition policy");
        var contract=JsonSerializer.SerializeToElement(new{
            inputs=options.Inputs.Select(p=>new{name=HubTypedActivity.PropertyName(p),type=p.PropertyType.FullName}).ToArray(),
            outputs=options.Outputs.Select(p=>new{name=HubTypedActivity.PropertyName(p),type=p.PropertyType.FullName}).ToArray(),
            activityResult=options.ResultProperty?.Name,
            activityParameters=options.ParametersProperty?.Name,
            implementation=typeof(TStep).Assembly.ManifestModule.ModuleVersionId
        });
        services.TryAddTransient<TStep>();
        services.AddSingleton(new HubTypedActivity(typeof(TStep),new(hubId,name,version,options.Partitions,
            options.HubConcurrency,options.Concurrency,options.ResourceName,options.ResourceField,
            options.ResourceCapacity,options.StrictFifo,contract,options.Ordering),options.Inputs.ToArray(),options.Outputs.ToArray(),options.ResultProperty,options.ParametersProperty,
            options.SequentialOutputs.ToArray()));
        return this;
    }
}

public static class HubWorkflowRuntimeExtensions
{
    public static HubRuntimeRegistrationBuilder AddHubWorkflowRuntime(this IServiceCollection services,Action<HubRuntimeOptions>? configure=null)
    {
        var options=new HubRuntimeOptions();configure?.Invoke(options);
        if(options.WorkflowConcurrency is <1 or >256 || options.ActivityConcurrency is <1 or >256 ||
           options.WorkerConcurrencyCap is < 0 or > 256 ||
           options.PollInterval<TimeSpan.FromMilliseconds(10) ||
           options.MatchingPartitions is < 1 or > 64)throw new ArgumentException("Invalid Hub worker options");
        services.AddSingleton(options);
        services.AddWorkflow(o=>{
            o.EnableWorkflows=false;o.EnableEvents=false;o.EnableIndexes=false;o.EnablePolling=false;
            o.EnableLifeCycleEventsPublisher=false;o.UseErrorRetryInterval(options.ErrorRetryInterval);
        });
        // Register the native engine services, but no legacy host/background consumers.
        services.RemoveAll<IBackgroundTask>();
        services.RemoveAll<ILifeCycleEventPublisher>();
        services.AddSingleton<HubExecutionSessionAccessor>();
        services.AddSingleton<HubRuntimeStartup>();
        services.AddSingleton<ILifeCycleEventPublisher,HubNativeLifeCyclePublisher>();
        services.AddSingleton(sp=>new HubNativeCatalog(sp.GetServices<HubWorkflowRegistration>(),
            sp.GetServices<HubTypedActivity>(),sp.GetRequiredService<IWorkflowRegistry>(),sp,options));
        services.AddSingleton<HubSnapshotCodec>();
        services.AddTransient<HubNativeExecutor>();
        services.AddTransient<HubWorkflowClient>();
        services.AddWorkflowStepMiddleware<HubActivityMiddleware>();
        services.TryAddSingleton(_=>new HubRestClient(options.HubUrl,options.Token,options.AllowInsecureHttp));
        services.AddHostedService<HubRuntimePublisher>();
        if(!options.RegisterOnly)
        {
            services.AddHostedService<HubNativeWorkflowWorker>();
            services.AddHostedService<HubNativeActivityWorker>();
        }
        return new(services);
    }
}

public sealed class HubNativeCatalog
{
    private readonly Dictionary<(string Hub,string Id,int Version),WorkflowDefinition> _definitions=[];
    public IReadOnlyList<HubTypedActivity> Activities { get; }
    public IReadOnlyList<HubDefinitionRegistration> Definitions { get; }
    internal HashSet<Assembly> SnapshotAssemblies { get; } = [typeof(WorkflowInstance).Assembly,typeof(HubNativeExecutor).Assembly];

    internal HubNativeCatalog(IEnumerable<HubWorkflowRegistration> registrations,IEnumerable<HubTypedActivity> activities,
        IWorkflowRegistry registry,IServiceProvider sp,HubRuntimeOptions options)
    {
        Activities=activities.ToArray();
        if(options.SequentialVersionOneCompatibility && Activities.GroupBy(a=>(a.Policy.HubId,a.Policy.ActivityType)).Any(g=>g.Count()>1))
            throw new InvalidOperationException("Sequential v1 aliases require one target version per activity name");
        if(Activities.GroupBy(a=>(a.Policy.HubId,a.StepType)).Any(g=>g.Count()>1) ||
           Activities.GroupBy(a=>(a.Policy.HubId,a.Policy.ActivityType,a.Policy.Version)).Any(g=>g.Count()>1))
            throw new InvalidOperationException("Duplicate typed activity identity or routed step type");
        var metadata=new List<HubDefinitionRegistration>();
        foreach(var registration in registrations)
        {
            var definition=registration.Register(registry,sp);
            if(definition.Steps.Any(s=>typeof(WorkflowCore.Primitives.SubWorkflowStepBody).IsAssignableFrom(s.BodyType)))
                throw new NotSupportedException("Upstream SubWorkflowStepBody is unimplemented and cannot be registered");
            _definitions.Add((registration.HubId,definition.Id,definition.Version),definition);
            AddAssembly(registration.WorkflowType);
            AddAssembly(registration.DataType);
            foreach(var step in definition.Steps)AddAssembly(step.BodyType);
            metadata.Add(new(registration.HubId,definition.Id,definition.Version,registration.Partitions,definition.Steps.Count,
                JsonSerializer.SerializeToElement(new{executionMode="native",protocolVersion=1,
                    implementation=registration.WorkflowType.Assembly.ManifestModule.ModuleVersionId,
                    steps=definition.Steps.Select(s=>new{stepId=s.Id,name=s.Name,bodyType=s.BodyType.FullName,
                        children=s.Children,compensationStepId=s.CompensationStepId,
                        activityType=Activities.SingleOrDefault(a=>a.Policy.HubId==registration.HubId && a.StepType==s.BodyType)?.Policy.ActivityType,
                        kind=Activities.Any(a=>a.Policy.HubId==registration.HubId && a.StepType==s.BodyType)?"Activity":"Native"}).ToArray()})));
        }
        foreach(var activity in Activities)
        {
            AddAssembly(activity.StepType);
            foreach(var property in activity.Inputs.Concat(activity.Outputs))AddAssembly(property.PropertyType);
            if(activity.ResultProperty is not null)AddAssembly(activity.ResultProperty.PropertyType);
        }
        Definitions=metadata;
    }
    private void AddAssembly(Type type)
    {
        if(type.Assembly!=typeof(object).Assembly && !(type.Assembly.GetName().Name??"").StartsWith("System.",StringComparison.Ordinal))
            SnapshotAssemblies.Add(type.Assembly);
        if(type.IsGenericType)foreach(var argument in type.GetGenericArguments())AddAssembly(argument);
    }
    public WorkflowDefinition Find(string hub,string id,int version) => _definitions.GetValueOrDefault((hub,id,version))
        ?? throw new InvalidOperationException("Native workflow version is not registered in this host");
    internal HubTypedActivity? Route(string hub,Type stepType) => Activities.SingleOrDefault(a=>a.Policy.HubId==hub && a.StepType==stepType);
    internal WorkflowStep? Step(string hub,string definition,int version,int stepId) =>
        _definitions.GetValueOrDefault((hub,definition,version))?.Steps.FindById(stepId);
}
