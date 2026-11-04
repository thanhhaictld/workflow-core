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
            policy.ResourceName?.Length > 80 || policy.ResourceField?.Length > 80 ||
            policy.Contract?.GetRawText().Length > 65536)
            throw new ArgumentException("Invalid activity registration");

        await using var cmd = Source.CreateCommand("""
            INSERT INTO wfc_scale.hub_activity_registration
            (hub_id,activity_type,version,partitions,hub_concurrency,
             activity_concurrency,resource_name,resource_field,resource_capacity,resource_fifo,contract)
            VALUES (@hub,@type,@version,@partitions,@hubcap,@cap,@resource,@field,@resourcecap,@fifo,@contract)
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
              AND contract IS NOT DISTINCT FROM @contract
            """);
        BindActivity(check, policy);
        if (await check.ExecuteScalarAsync(ct) is null)
            throw new InvalidOperationException("Immutable activity policy/version mismatch");
        return false;
    }

    /// <summary>CAS update for operational capacities. Activity contract, key selection,
    /// partition count, and FIFO semantics remain immutable within an activity version.</summary>
    public async Task<long> UpdateActivityCapacityAsync(HubActivityCapacityUpdate request, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(request.HubId) || request.HubId.Length > 80 ||
            string.IsNullOrWhiteSpace(request.ActivityType) || request.ActivityType.Length > 80 ||
            request.Version < 1 || request.ExpectedRevision < 1 ||
            request.HubConcurrency is < 1 or > 100000 ||
            request.ActivityConcurrency is < 1 or > 100000 ||
            request.ResourceCapacity is < 1 or > 100000)
            throw new ArgumentException("Invalid activity capacity update");

        await using var db = await Source.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);

        // Enqueues take a shared registration lock; updates take an exclusive lock.
        // All resource upserts and updates therefore observe one consistent policy.
        await using var query = new NpgsqlCommand("""
            SELECT hub_concurrency,activity_concurrency,resource_capacity,resource_name,revision
            FROM wfc_scale.hub_activity_registration
            WHERE hub_id=@hub AND activity_type=@type AND version=@version
            FOR UPDATE
            """, db, tx);
        query.Parameters.AddWithValue("hub",request.HubId);
        query.Parameters.AddWithValue("type",request.ActivityType);
        query.Parameters.AddWithValue("version",request.Version);
        int oldHub,oldActivity,oldResource;
        string? resourceName;
        long revision;
        await using (var reader=await query.ExecuteReaderAsync(ct))
        {
            if(!await reader.ReadAsync(ct))
                throw new KeyNotFoundException("Activity registration not found");
            oldHub=reader.GetInt32(0);
            oldActivity=reader.GetInt32(1);
            oldResource=reader.GetInt32(2);
            resourceName=reader.IsDBNull(3)?null:reader.GetString(3);
            revision=reader.GetInt64(4);
        }
        if(revision!=request.ExpectedRevision)
            throw new InvalidOperationException(
                $"Stale activity policy revision for {request.HubId}/{request.ActivityType}/v{request.Version}: expected {request.ExpectedRevision}, current {revision}");
        if(oldHub==request.HubConcurrency && oldActivity==request.ActivityConcurrency &&
            oldResource==request.ResourceCapacity)
        {
            await tx.CommitAsync(ct);
            return revision;
        }

        // A global hub capacity is shared by all activities in this hub, as is
        // a named resource capacity shared by different activity registrations.
        // Reject divergent configuration instead of silently changing other owners.
        await using (var conflict=new NpgsqlCommand("""
            SELECT 1 FROM wfc_scale.hub_activity_registration
            WHERE hub_id=@hub
              AND NOT (activity_type=@type AND version=@version)
              AND hub_concurrency<>@hubcap
            LIMIT 1
            """,db,tx))
        {
            conflict.Parameters.AddWithValue("hub",request.HubId);
            conflict.Parameters.AddWithValue("type",request.ActivityType);
            conflict.Parameters.AddWithValue("version",request.Version);
            conflict.Parameters.AddWithValue("hubcap",request.HubConcurrency);
            if(await conflict.ExecuteScalarAsync(ct) is not null)
                throw new InvalidOperationException("Shared hub capacity belongs to multiple activity registrations; update requires coordinated hub policy ownership");
        }
        var hubKey="hub:"+request.HubId+":"+Digest(request.HubId);
        var activityKey="activity:"+request.HubId+":"+request.ActivityType+":"+Digest(request.ActivityType);
        var resourcePrefix="resource:"+request.HubId+":"+resourceName+":";
        await using (var keys=new NpgsqlCommand("""
            SELECT resource_key FROM wfc_scale.hub_resource
            WHERE resource_key=@hubkey OR resource_key=@activitykey
              OR (@resource IS NOT NULL AND starts_with(resource_key,@prefix))
            ORDER BY resource_key
            """,db,tx))
        {
            keys.Parameters.AddWithValue("hubkey",hubKey);
            keys.Parameters.AddWithValue("activitykey",activityKey);
            keys.Parameters.Add("resource",NpgsqlDbType.Text).Value=(object?)resourceName??DBNull.Value;
            keys.Parameters.AddWithValue("prefix",resourcePrefix);
            var resourceKeys=new List<string>();
            await using(var reader=await keys.ExecuteReaderAsync(ct))
                while(await reader.ReadAsync(ct))resourceKeys.Add(reader.GetString(0));
            foreach(var key in resourceKeys)
            {
                await using var update=new NpgsqlCommand("""
                    UPDATE wfc_scale.hub_resource SET capacity=@capacity WHERE resource_key=@key
                    """,db,tx);
                update.Parameters.AddWithValue("key",key);
                update.Parameters.AddWithValue("capacity",
                    key==hubKey?request.HubConcurrency:
                    key==activityKey?request.ActivityConcurrency:request.ResourceCapacity);
                await update.ExecuteNonQueryAsync(ct);
            }
        }
        await using(var update=new NpgsqlCommand("""
            UPDATE wfc_scale.hub_activity_registration
            SET hub_concurrency=@hubcap,activity_concurrency=@activitycap,
                resource_capacity=@resourcecap,revision=revision+1
            WHERE hub_id=@hub AND activity_type=@type AND version=@version
            """,db,tx))
        {
            update.Parameters.AddWithValue("hub",request.HubId);
            update.Parameters.AddWithValue("type",request.ActivityType);
            update.Parameters.AddWithValue("version",request.Version);
            update.Parameters.AddWithValue("hubcap",request.HubConcurrency);
            update.Parameters.AddWithValue("activitycap",request.ActivityConcurrency);
            update.Parameters.AddWithValue("resourcecap",request.ResourceCapacity);
            await update.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        return revision+1;
    }

    public async Task<HubActivityPolicySnapshot?> GetActivityPolicyAsync(
        string hub,string activity,int version,CancellationToken ct)
    {
        await using var cmd=Source.CreateCommand("""
            SELECT hub_concurrency,activity_concurrency,resource_capacity,revision
            FROM wfc_scale.hub_activity_registration
            WHERE hub_id=@hub AND activity_type=@type AND version=@version
            """);
        cmd.Parameters.AddWithValue("hub",hub);
        cmd.Parameters.AddWithValue("type",activity);
        cmd.Parameters.AddWithValue("version",version);
        await using var reader=await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct)
            ? new HubActivityPolicySnapshot(hub,activity,version,reader.GetInt32(0),
                reader.GetInt32(1),reader.GetInt32(2),reader.GetInt64(3))
            : null;
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
        cmd.Parameters.Add("contract", NpgsqlDbType.Jsonb).Value =
            (object?)p.Contract?.GetRawText() ?? DBNull.Value;
    }

    public async Task<IReadOnlyList<HubActivityRegistration>> ActivitiesAsync(CancellationToken ct)
    {
        await using var cmd=Source.CreateCommand("""
            SELECT hub_id,activity_type,version,partitions,hub_concurrency,
                   activity_concurrency,resource_name,resource_field,
                   resource_capacity,resource_fifo,contract::text
            FROM wfc_scale.hub_activity_registration ORDER BY hub_id,activity_type,version
            """);
        var values=new List<HubActivityRegistration>();
        await using var r=await cmd.ExecuteReaderAsync(ct);
        while(await r.ReadAsync(ct))
            values.Add(new(r.GetString(0),r.GetString(1),r.GetInt32(2),r.GetInt32(3),
                r.GetInt32(4),r.GetInt32(5),
                r.IsDBNull(6)?null:r.GetString(6),r.IsDBNull(7)?null:r.GetString(7),
                r.GetInt32(8),r.GetBoolean(9),r.IsDBNull(10)?null:
                    JsonDocument.Parse(r.GetString(10)).RootElement.Clone()));
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
