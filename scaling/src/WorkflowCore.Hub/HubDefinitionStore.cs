using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

/// <summary>
/// Workflow metadata and activity routing registration for independent
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
            ON CONFLICT (hub_id,definition_id,version) DO UPDATE
            SET partitions=EXCLUDED.partitions,step_count=EXCLUDED.step_count,metadata=EXCLUDED.metadata
            RETURNING xmax=0
            """);
        BindDefinition(cmd, definition);
        return (bool)(await cmd.ExecuteScalarAsync(ct))!;
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

    public async Task<bool> RegisterActivityAsync(HubActivityRegistration activity, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(activity.HubId) || activity.HubId.Length > 80 ||
            string.IsNullOrWhiteSpace(activity.ActivityType) || activity.ActivityType.Length > 80 ||
            activity.Version < 1 || activity.Partitions is < 1 or > 4096 ||
            activity.Contract?.GetRawText().Length > 65536)
            throw new ArgumentException("Invalid activity registration");

        // The two required historical columns are unused by Hub admission.
        await using var cmd = Source.CreateCommand("""
            INSERT INTO wfc_scale.hub_activity_registration
                (hub_id,activity_type,version,partitions,contract,hub_concurrency,activity_concurrency)
            VALUES (@hub,@type,@version,@partitions,@contract,1,1)
            ON CONFLICT (hub_id,activity_type,version) DO UPDATE
            SET partitions=EXCLUDED.partitions,contract=EXCLUDED.contract
            RETURNING xmax=0
            """);
        cmd.Parameters.AddWithValue("hub", activity.HubId);
        cmd.Parameters.AddWithValue("type", activity.ActivityType);
        cmd.Parameters.AddWithValue("version", activity.Version);
        cmd.Parameters.AddWithValue("partitions", activity.Partitions);
        cmd.Parameters.Add("contract", NpgsqlDbType.Jsonb).Value =
            (object?)activity.Contract?.GetRawText() ?? DBNull.Value;
        return (bool)(await cmd.ExecuteScalarAsync(ct))!;
    }

    public async Task<HubTaskRoute?> ResolveAsync(
        string hub, string activity, int version, CancellationToken ct,
        NpgsqlConnection? connection = null, NpgsqlTransaction? transaction = null)
    {
        const string sql = """
            SELECT partitions FROM wfc_scale.hub_activity_registration
            WHERE hub_id=@hub AND activity_type=@type AND version=@version
            """;
        await using var cmd = connection is null ? Source.CreateCommand(sql) :
            new NpgsqlCommand(sql + " FOR SHARE", connection, transaction);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("type", activity);
        cmd.Parameters.AddWithValue("version", version);
        return await cmd.ExecuteScalarAsync(ct) is int partitions
            ? new HubTaskRoute(hub, activity, version, partitions) : null;
    }
}
