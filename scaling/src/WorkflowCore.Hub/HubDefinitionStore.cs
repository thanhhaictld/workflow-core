using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

/// <summary>
/// Immutable definition and declarative concurrency registration for independent
/// processor / provisioner processes. Persisted metadata is common to all Hub replicas.
/// </summary>
public sealed class HubDefinitionStore(HubTaskStore tasks)
{
    private NpgsqlDataSource Source => tasks.DataSource;

    public async Task<bool> RegisterAsync(HubDefinitionRegistration definition, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(definition.HubId) || definition.HubId.Length > 80 ||
            string.IsNullOrWhiteSpace(definition.DefinitionId) || definition.DefinitionId.Length > 80 ||
            definition.Version < 1 || definition.Partitions is < 1 or > 4096 ||
            definition.StepCount is < 1 or > 128 ||
            definition.Metadata.ValueKind != JsonValueKind.Object ||
            definition.Metadata.GetRawText().Length > 65536)
            throw new ArgumentException("Invalid workflow definition registration");

        await using var cmd = Source.CreateCommand("""
            INSERT INTO wfc_scale.hub_definition
                (hub_id,definition_id,version,partitions,step_count,metadata)
            VALUES (@hub,@definition,@version,@partitions,@steps,@metadata)
            ON CONFLICT DO NOTHING
            """);
        BindDefinition(cmd, definition);
        var inserted = await cmd.ExecuteNonQueryAsync(ct);
        if (inserted == 1) return true;

        await using var check = Source.CreateCommand("""
            SELECT 1 FROM wfc_scale.hub_definition
            WHERE hub_id=@hub AND definition_id=@definition AND version=@version
              AND partitions=@partitions AND step_count=@steps AND metadata=@metadata
            """);
        BindDefinition(check, definition);
        if (await check.ExecuteScalarAsync(ct) is null)
            throw new InvalidOperationException("Immutable workflow definition/version mismatch");
        return false;
    }

    private static void BindDefinition(NpgsqlCommand cmd, HubDefinitionRegistration definition)
    {
        cmd.Parameters.AddWithValue("hub", definition.HubId);
        cmd.Parameters.AddWithValue("definition", definition.DefinitionId);
        cmd.Parameters.AddWithValue("version", definition.Version);
        cmd.Parameters.AddWithValue("partitions", definition.Partitions);
        cmd.Parameters.AddWithValue("steps", definition.StepCount);
        cmd.Parameters.Add("metadata", NpgsqlDbType.Jsonb).Value = definition.Metadata.GetRawText();
    }

    public async Task<HubDefinitionRegistration?> GetAsync(
        string hub, string definition, int version, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            SELECT partitions,step_count,metadata::text
            FROM wfc_scale.hub_definition
            WHERE hub_id=@hub AND definition_id=@definition AND version=@version
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("definition", definition);
        cmd.Parameters.AddWithValue("version", version);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new(hub, definition, version, reader.GetInt32(0), reader.GetInt32(1),
            JsonDocument.Parse(reader.GetString(2)).RootElement.Clone());
    }

    public async Task<IReadOnlyList<HubDefinitionRegistration>> DefinitionsAsync(CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            SELECT hub_id,definition_id,version,partitions,step_count,metadata::text
            FROM wfc_scale.hub_definition ORDER BY hub_id,definition_id,version
            """);
        var items = new List<HubDefinitionRegistration>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            items.Add(new(reader.GetString(0),reader.GetString(1),reader.GetInt32(2),
                reader.GetInt32(3),reader.GetInt32(4),
                JsonDocument.Parse(reader.GetString(5)).RootElement.Clone()));
        return items;
    }

    public async Task<bool> RegisterActivityAsync(HubActivityRegistration policy, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(policy.HubId) || policy.HubId.Length > 80 ||
            string.IsNullOrWhiteSpace(policy.ActivityType) || policy.ActivityType.Length > 80 ||
            policy.Version < 1 || policy.Partitions is < 1 or > 4096 ||
            policy.HubConcurrency is < 1 or > 100000 ||
            policy.ActivityConcurrency is < 1 or > 100000 ||
            policy.ResourceCapacity is < 1 or > 100000 ||
            (policy.ResourceName is null) != (policy.ResourceField is null) ||
            policy.ResourceName?.Length > 80 || policy.ResourceField?.Length > 80)
            throw new ArgumentException("Invalid activity registration");

        await using var cmd = Source.CreateCommand("""
            INSERT INTO wfc_scale.hub_activity_registration
            (hub_id,activity_type,version,partitions,hub_concurrency,
             activity_concurrency,resource_name,resource_field,resource_capacity,resource_fifo)
            VALUES (@hub,@type,@version,@partitions,@hubcap,@cap,@resource,@field,@resourcecap,@fifo)
            ON CONFLICT DO NOTHING
            """);
        BindActivity(cmd, policy);
        var inserted = await cmd.ExecuteNonQueryAsync(ct);
        if (inserted == 1) return true;
        await using var check = Source.CreateCommand("""
            SELECT 1 FROM wfc_scale.hub_activity_registration
            WHERE hub_id=@hub AND activity_type=@type AND version=@version
              AND partitions=@partitions AND hub_concurrency=@hubcap
              AND activity_concurrency=@cap
              AND resource_name IS NOT DISTINCT FROM @resource
              AND resource_field IS NOT DISTINCT FROM @field
              AND resource_capacity=@resourcecap AND resource_fifo=@fifo
            """);
        BindActivity(check, policy);
        if (await check.ExecuteScalarAsync(ct) is null)
            throw new InvalidOperationException("Immutable activity policy/version mismatch");
        return false;
    }

    private static void BindActivity(NpgsqlCommand cmd, HubActivityRegistration p)
    {
        cmd.Parameters.AddWithValue("hub", p.HubId);
        cmd.Parameters.AddWithValue("type", p.ActivityType);
        cmd.Parameters.AddWithValue("version", p.Version);
        cmd.Parameters.AddWithValue("partitions", p.Partitions);
        cmd.Parameters.AddWithValue("hubcap", p.HubConcurrency);
        cmd.Parameters.AddWithValue("cap", p.ActivityConcurrency);
        cmd.Parameters.Add("resource", NpgsqlDbType.Text).Value = (object?)p.ResourceName ?? DBNull.Value;
        cmd.Parameters.Add("field", NpgsqlDbType.Text).Value = (object?)p.ResourceField ?? DBNull.Value;
        cmd.Parameters.AddWithValue("resourcecap", p.ResourceCapacity);
        cmd.Parameters.AddWithValue("fifo", p.StrictFifo);
    }

    public async Task<IReadOnlyList<HubActivityRegistration>> ActivitiesAsync(CancellationToken ct)
    {
        await using var cmd=Source.CreateCommand("""
            SELECT hub_id,activity_type,version,partitions,hub_concurrency,
                   activity_concurrency,resource_name,resource_field,
                   resource_capacity,resource_fifo
            FROM wfc_scale.hub_activity_registration ORDER BY hub_id,activity_type,version
            """);
        var values=new List<HubActivityRegistration>();
        await using var r=await cmd.ExecuteReaderAsync(ct);
        while(await r.ReadAsync(ct))
            values.Add(new(r.GetString(0),r.GetString(1),r.GetInt32(2),r.GetInt32(3),
                r.GetInt32(4),r.GetInt32(5),
                r.IsDBNull(6)?null:r.GetString(6),r.IsDBNull(7)?null:r.GetString(7),
                r.GetInt32(8),r.GetBoolean(9)));
        return values;
    }

    public async Task<HubTaskRoute?> ResolveAsync(
        string hub, string activity, int version, JsonElement input, CancellationToken ct,
        NpgsqlConnection? connection = null, NpgsqlTransaction? transaction = null)
    {
        const string sql = """
            SELECT partitions,hub_concurrency,activity_concurrency,resource_name,
                   resource_field,resource_capacity,resource_fifo
            FROM wfc_scale.hub_activity_registration
            WHERE hub_id=@hub AND activity_type=@type AND version=@version
            """;
        await using var cmd = connection is null ? Source.CreateCommand(sql) :
            new NpgsqlCommand(sql, connection, transaction);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("type", activity);
        cmd.Parameters.AddWithValue("version", version);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        int partitions=reader.GetInt32(0), hubLimit=reader.GetInt32(1), activityLimit=reader.GetInt32(2);
        string? resource=reader.IsDBNull(3)?null:reader.GetString(3);
        string? field=reader.IsDBNull(4)?null:reader.GetString(4);
        int resourceLimit=reader.GetInt32(5); bool fifo=reader.GetBoolean(6);
        var rules = new List<HubResourceRule>
        {
            new("hub:" + hub + ":" + Digest(hub), hubLimit, false),
            new("activity:" + hub + ":" + activity + ":" + Digest(activity), activityLimit, false)
        };
        if (resource is not null && field is not null)
        {
            if (input.ValueKind != JsonValueKind.Object ||
                !input.TryGetProperty(field, out var key) ||
                key.ValueKind != JsonValueKind.String ||
                string.IsNullOrWhiteSpace(key.GetString()))
                throw new ArgumentException("Missing declarative policy resource field: " + field);
            rules.Add(new("resource:" + hub + ":" + resource + ":" + Digest(key.GetString()!),
                resourceLimit, fifo));
        }
        return new(hub, activity, version, partitions,
            rules.OrderBy(x => x.Key,StringComparer.Ordinal).ToArray());
    }

    private static string Digest(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
