using System.Text.Json;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json.Linq;
using WorkflowCore.Models;

namespace WorkflowCore.RemoteExecution;

/// <summary>Retains native pointer persistence types across process restarts.
/// Type metadata is restricted to registered application assemblies and safe value/container types.</summary>
public sealed class HubSnapshotCodec(HubNativeCatalog catalog)
{
    public JsonElement EncodeValue(object? value) => Encode(new HubSnapshotValue{NativeValue=value});
    internal static bool IsEncodedValue(JsonElement value) => value.ValueKind==JsonValueKind.Object &&
        value.TryGetProperty("$hubValueFormat",out var format) && format.ValueKind==JsonValueKind.String && format.GetString()=="v1";
    public object? DecodeValue(JsonElement value) => IsEncodedValue(value)?Decode<HubSnapshotValue>(value).NativeValue:
        throw new InvalidOperationException("Unknown native value format");
    private JsonSerializerSettings Settings => new()
    {
        TypeNameHandling=TypeNameHandling.Auto,
        DateTimeZoneHandling=DateTimeZoneHandling.Utc,
        SerializationBinder=new SnapshotBinder(catalog),
        ContractResolver=new SnapshotResolver(new SnapshotBinder(catalog)),
        Converters=[new JsonElementConverter()]
    };
    public JsonElement Encode(object value) =>
        JsonDocument.Parse(JsonConvert.SerializeObject(value,Settings)).RootElement.Clone();
    public T Decode<T>(JsonElement value) => JsonConvert.DeserializeObject<T>(value.GetRawText(),Settings)
        ?? throw new InvalidOperationException("Empty native snapshot");
    public object? Decode(JsonElement value,Type type) => JsonConvert.DeserializeObject(value.GetRawText(),type,Settings);

    private sealed class JsonElementConverter : JsonConverter<JsonElement>
    {
        public override void WriteJson(JsonWriter writer,JsonElement value,Newtonsoft.Json.JsonSerializer serializer) =>
            writer.WriteRawValue(value.GetRawText());
        public override JsonElement ReadJson(JsonReader reader,Type type,JsonElement existing,bool hasExisting,Newtonsoft.Json.JsonSerializer serializer) =>
            JsonDocument.Parse(JToken.Load(reader).ToString(Formatting.None)).RootElement.Clone();
    }

    // Json.NET does not emit primitive type names in object slots. Preserve the
    // actual CLR type of context items, outcomes, persistence and branch values.
    private sealed class SnapshotResolver(SnapshotBinder binder) : DefaultContractResolver
    {
        private readonly JsonConverter _boxed=new BoxedValueConverter(binder);
        protected override Newtonsoft.Json.Serialization.JsonProperty CreateProperty(System.Reflection.MemberInfo member,MemberSerialization serialization)
        {
            var property=base.CreateProperty(member,serialization);
            if(property.PropertyType==typeof(object))property.Converter=_boxed;
            return property;
        }
        protected override JsonArrayContract CreateArrayContract(Type type)
        {
            var contract=base.CreateArrayContract(type);
            if(contract.CollectionItemType==typeof(object))contract.ItemConverter=_boxed;
            return contract;
        }
        protected override JsonDictionaryContract CreateDictionaryContract(Type type)
        {
            var contract=base.CreateDictionaryContract(type);
            if(contract.DictionaryValueType==typeof(object))contract.ItemConverter=_boxed;
            return contract;
        }
    }
    private sealed class BoxedValueConverter(SnapshotBinder binder) : JsonConverter
    {
        public override bool CanConvert(Type type)=>type==typeof(object);
        public override void WriteJson(JsonWriter writer,object? value,Newtonsoft.Json.JsonSerializer serializer)
        {
            if(value is null){writer.WriteNull();return;}
            binder.BindToName(value.GetType(),out var assembly,out var name);
            writer.WriteStartObject();writer.WritePropertyName("clrType");writer.WriteValue(name);
            writer.WritePropertyName("clrAssembly");writer.WriteValue(assembly);
            writer.WritePropertyName("value");
            if(value is JsonElement element)writer.WriteRawValue(element.GetRawText());
            else serializer.Serialize(writer,value,value.GetType());
            writer.WriteEndObject();
        }
        public override object? ReadJson(JsonReader reader,Type type,object? existing,Newtonsoft.Json.JsonSerializer serializer)
        {
            if(reader.TokenType==JsonToken.Null)return null;
            var token=JObject.Load(reader);
            var actual=binder.BindToType((string?)token["clrAssembly"],(string?)token["clrType"]
                ?? throw new JsonSerializationException("Missing boxed snapshot type"));
            return token["value"]!.ToObject(actual,serializer);
        }
    }

    private sealed class SnapshotBinder(HubNativeCatalog catalog) : ISerializationBinder
    {
        private readonly DefaultSerializationBinder _default=new();
        public Type BindToType(string? assemblyName,string typeName)
        {
            var type=_default.BindToType(assemblyName,typeName);
            if(!Allowed(type))throw new JsonSerializationException("Unregistered snapshot type: "+typeName);
            return type;
        }
        public void BindToName(Type serializedType,out string? assemblyName,out string? typeName)
        {
            if(!Allowed(serializedType))throw new JsonSerializationException("Unregistered snapshot type: "+serializedType);
            _default.BindToName(serializedType,out assemblyName,out typeName);
        }
        private bool Allowed(Type type)
        {
            if(typeof(Delegate).IsAssignableFrom(type)||typeof(Type).IsAssignableFrom(type))return false;
            if(typeof(JToken).IsAssignableFrom(type))return true;
            if(type.IsArray)return Allowed(type.GetElementType()!);
            if(type.IsPrimitive || type.IsEnum || type==typeof(object) || type==typeof(JsonElement) || type==typeof(string) || type==typeof(decimal) ||
               type==typeof(DateTime) || type==typeof(DateTimeOffset) || type==typeof(TimeSpan) || type==typeof(Guid))return true;
            if(type.IsGenericType)
            {
                var generic=type.GetGenericTypeDefinition();
                if(generic==typeof(List<>)||generic==typeof(Dictionary<,>)||generic==typeof(Nullable<>)||generic==typeof(HashSet<>))
                    return type.GetGenericArguments().All(Allowed);
                return catalog.SnapshotAssemblies.Contains(generic.Assembly) && type.GetGenericArguments().All(Allowed);
            }
            return catalog.SnapshotAssemblies.Contains(type.Assembly);
        }
    }
}

public sealed class HubSnapshotValue
{
    [JsonProperty("$hubValueFormat")]
    public string Format=>"v1";
    public object? NativeValue { get; set; }
}
