using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

/// <summary>
/// Workflow metadata and declarative concurrency registration for independent
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
            policy.Contract?.GetRawText().Length > 65536 || !Enum.IsDefined(policy.Ordering) ||
            ((policy.Ordering is HubActivityOrdering.KeyedExclusive or
                HubActivityOrdering.KeyedFifo or HubActivityOrdering.KeyedBounded) &&
             (string.IsNullOrWhiteSpace(policy.ResourceName) ||
              string.IsNullOrWhiteSpace(policy.ResourceField))) ||
            ((policy.Ordering is HubActivityOrdering.Unordered or HubActivityOrdering.PartitionSerial) &&
             policy.ResourceName is not null))
            throw new ArgumentException("Invalid activity registration");

        await using var db = await Source.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);
        await LockPolicyHub(db,tx,policy.HubId,ct);
        // Existing shared keys are authoritative even for a newly deployed version.
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_activity_registration
            (hub_id,activity_type,version,partitions,hub_concurrency,
             activity_concurrency,resource_name,resource_field,resource_capacity,resource_fifo,contract,execution_policy)
            VALUES (@hub,@type,@version,@partitions,
              COALESCE((SELECT hub_concurrency FROM wfc_scale.hub_activity_registration WHERE hub_id=@hub LIMIT 1),@hubcap),
              COALESCE((SELECT activity_concurrency FROM wfc_scale.hub_activity_registration WHERE hub_id=@hub AND activity_type=@type LIMIT 1),@cap),
              @resource,@field,
              COALESCE((SELECT resource_capacity FROM wfc_scale.hub_activity_registration WHERE hub_id=@hub AND resource_name=@resource LIMIT 1),@resourcecap),
              @fifo,@contract,@policy)
            ON CONFLICT (hub_id,activity_type,version) DO UPDATE
            SET partitions=EXCLUDED.partitions,resource_name=EXCLUDED.resource_name,
                resource_field=EXCLUDED.resource_field,resource_fifo=EXCLUDED.resource_fifo,
                contract=EXCLUDED.contract,execution_policy=EXCLUDED.execution_policy
            RETURNING xmax=0
            """,db,tx);
        BindActivity(cmd, policy);
        var inserted = (bool)(await cmd.ExecuteScalarAsync(ct))!;
        await tx.CommitAsync(ct);
        return inserted;
    }

    internal static async Task LockPolicyHub(NpgsqlConnection db,NpgsqlTransaction tx,string hub,CancellationToken ct,bool shared=false)
    {
        await using var gate=new NpgsqlCommand(
            shared ? "SELECT pg_advisory_xact_lock_shared(hashtextextended('hub-policy:' || @hub,0))" :
                "SELECT pg_advisory_xact_lock(hashtextextended('hub-policy:' || @hub,0))",db,tx);
        gate.Parameters.AddWithValue("hub",hub);
        await gate.ExecuteNonQueryAsync(ct);
    }

    /// <summary>CAS update for operational capacities. Registration updates metadata
    /// separately and preserves these operator-controlled capacities.</summary>
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

        await LockPolicyHub(db,tx,request.HubId,ct);
        // Lock all shared owners before resources, matching enqueue lock order.
        await using(var owners=new NpgsqlCommand("""
            SELECT 1 FROM wfc_scale.hub_activity_registration
            WHERE hub_id=@hub ORDER BY activity_type,version FOR UPDATE
            """,db,tx))
        {
            owners.Parameters.AddWithValue("hub",request.HubId);
            await owners.ExecuteNonQueryAsync(ct);
        }

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

        // Hub and resource capacities are shared keys; never let one activity
        // silently modify another registration's published policy.
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
            if(await conflict.ExecuteScalarAsync(ct) is not null && !request.CoordinateSharedResources)
                throw new InvalidOperationException("Shared hub capacity belongs to multiple activity registrations; update requires coordinated hub policy ownership");
        }
        if(resourceName is not null && oldResource!=request.ResourceCapacity)
        {
            await using var shared=new NpgsqlCommand("""
                SELECT 1 FROM wfc_scale.hub_activity_registration
                WHERE hub_id=@hub AND resource_name=@resource
                  AND NOT (activity_type=@type AND version=@version)
                LIMIT 1
                """,db,tx);
            shared.Parameters.AddWithValue("hub",request.HubId);
            shared.Parameters.AddWithValue("resource",resourceName);
            shared.Parameters.AddWithValue("type",request.ActivityType);
            shared.Parameters.AddWithValue("version",request.Version);
            if(await shared.ExecuteScalarAsync(ct) is not null && !request.CoordinateSharedResources)
                throw new InvalidOperationException("Resource capacity is shared by multiple activities; coordinate the update");
        }
        if(oldActivity!=request.ActivityConcurrency)
        {
            await using var shared=new NpgsqlCommand("""
                SELECT 1 FROM wfc_scale.hub_activity_registration
                WHERE hub_id=@hub AND activity_type=@type AND version<>@version
                LIMIT 1
                """,db,tx);
            shared.Parameters.AddWithValue("hub",request.HubId);
            shared.Parameters.AddWithValue("type",request.ActivityType);
            shared.Parameters.AddWithValue("version",request.Version);
            if(await shared.ExecuteScalarAsync(ct) is not null && !request.CoordinateSharedResources)
                throw new InvalidOperationException("Activity capacity is shared across versions; coordinate the update");
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
            SET hub_concurrency=CASE WHEN @hubchanged THEN @hubcap ELSE hub_concurrency END,
                activity_concurrency=CASE WHEN activity_type=@type AND @activitychanged THEN @activitycap ELSE activity_concurrency END,
                resource_capacity=CASE WHEN (resource_name=@resource OR (activity_type=@type AND version=@version))
                    AND @resourcechanged THEN @resourcecap ELSE resource_capacity END,
                revision=revision+1
            WHERE hub_id=@hub AND (
                (activity_type=@type AND version=@version) OR @hubchanged
                OR (activity_type=@type AND @activitychanged)
                OR (resource_name=@resource AND @resourcechanged))
            """,db,tx))
        {
            update.Parameters.AddWithValue("hub",request.HubId);
            update.Parameters.AddWithValue("type",request.ActivityType);
            update.Parameters.AddWithValue("version",request.Version);
            update.Parameters.AddWithValue("hubchanged",oldHub!=request.HubConcurrency);
            update.Parameters.AddWithValue("activitychanged",oldActivity!=request.ActivityConcurrency);
            update.Parameters.AddWithValue("resourcechanged",oldResource!=request.ResourceCapacity);
            update.Parameters.Add("resource",NpgsqlDbType.Text).Value=(object?)resourceName??DBNull.Value;
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
        cmd.Parameters.AddWithValue("policy", p.Ordering.ToString());
        cmd.Parameters.Add("contract", NpgsqlDbType.Jsonb).Value =
            (object?)p.Contract?.GetRawText() ?? DBNull.Value;
    }

    public async Task<IReadOnlyList<HubActivityRegistration>> ActivitiesAsync(CancellationToken ct)
    {
        await using var cmd=Source.CreateCommand("""
            SELECT hub_id,activity_type,version,partitions,hub_concurrency,
                   activity_concurrency,resource_name,resource_field,
                   resource_capacity,resource_fifo,contract::text,execution_policy
            FROM wfc_scale.hub_activity_registration ORDER BY hub_id,activity_type,version
            """);
        var values=new List<HubActivityRegistration>();
        await using var r=await cmd.ExecuteReaderAsync(ct);
        while(await r.ReadAsync(ct))
            values.Add(new(r.GetString(0),r.GetString(1),r.GetInt32(2),r.GetInt32(3),
                r.GetInt32(4),r.GetInt32(5),
                r.IsDBNull(6)?null:r.GetString(6),r.IsDBNull(7)?null:r.GetString(7),
                r.GetInt32(8),r.GetBoolean(9),r.IsDBNull(10)?null:
                    JsonDocument.Parse(r.GetString(10)).RootElement.Clone(),
                Enum.Parse<HubActivityOrdering>(r.GetString(11))));
        return values;
    }

    public async Task<HubTaskRoute?> ResolveAsync(
        string hub, string activity, int version, JsonElement input, CancellationToken ct,
        NpgsqlConnection? connection = null, NpgsqlTransaction? transaction = null)
    {
        var registration=await ReadActivityRouteRegistrationAsync(hub,activity,version,ct,connection,transaction);
        return registration?.Resolve(hub,activity,version,input);
    }

    internal async Task<ActivityRouteRegistration?> ReadActivityRouteRegistrationAsync(
        string hub, string activity, int version, CancellationToken ct,
        NpgsqlConnection? connection = null, NpgsqlTransaction? transaction = null)
    {
        if(connection is not null && transaction is not null)
            await LockPolicyHub(connection,transaction,hub,ct,shared:true);
        const string sql = """
            SELECT partitions,hub_concurrency,activity_concurrency,resource_name,
                   resource_field,resource_capacity,resource_fifo,execution_policy
            FROM wfc_scale.hub_activity_registration
            WHERE hub_id=@hub AND activity_type=@type AND version=@version
            """;
        // Remote/native workflow commits resolve inside their existing transaction.
        // Hold a shared policy row lock until commit, so operator updates cannot
        // invalidate a resolved route before resource rows are materialized.
        await using var cmd = connection is null ? Source.CreateCommand(sql) :
            new NpgsqlCommand(sql + " FOR SHARE", connection, transaction);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("type", activity);
        cmd.Parameters.AddWithValue("version", version);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        int partitions=reader.GetInt32(0), hubLimit=reader.GetInt32(1), activityLimit=reader.GetInt32(2);
        string? resource=reader.IsDBNull(3)?null:reader.GetString(3);
        string? field=reader.IsDBNull(4)?null:reader.GetString(4);
        int resourceLimit=reader.GetInt32(5); bool fifo=reader.GetBoolean(6);
        var ordering = Enum.Parse<HubActivityOrdering>(reader.GetString(7));
        return new(partitions,hubLimit,activityLimit,resource,field,resourceLimit,fifo,ordering);
    }

    internal sealed record ActivityRouteRegistration(int Partitions,int HubLimit,int ActivityLimit,
        string? Resource,string? Field,int ResourceLimit,bool Fifo,HubActivityOrdering Ordering)
    {
        internal HubTaskRoute Resolve(string hub,string activity,int version,JsonElement input)
        {
            var ordering=Ordering;
            var resource=Resource;
            var field=Field;
            // Hub identifies the policy owner and authorizes requests; it does not
            // implicitly serialize business work for explicit scheduling modes.
            var rules = new List<HubResourceRule>();
            if(ordering == HubActivityOrdering.Legacy)
            {
                rules.Add(new("hub:" + hub + ":" + Digest(hub), HubLimit, false));
                rules.Add(new("activity:" + hub + ":" + activity + ":" + Digest(activity), ActivityLimit, false));
            }
            if ((ordering is HubActivityOrdering.KeyedExclusive or HubActivityOrdering.KeyedFifo or
                HubActivityOrdering.KeyedBounded) && (resource is null || field is null))
                throw new InvalidOperationException("Keyed policy has no resource selector");
            if ((ordering == HubActivityOrdering.Legacy && resource is not null && field is not null) ||
                 (ordering is HubActivityOrdering.KeyedExclusive or HubActivityOrdering.KeyedFifo or HubActivityOrdering.KeyedBounded))
            {
                if (field is null || input.ValueKind != JsonValueKind.Object ||
                    !input.TryGetProperty(field, out var key) ||
                    key.ValueKind != JsonValueKind.String ||
                    string.IsNullOrWhiteSpace(key.GetString()))
                    throw new ArgumentException("Missing declarative policy resource field: " + field);
                var capacity = ordering is HubActivityOrdering.KeyedExclusive or HubActivityOrdering.KeyedFifo
                    ? 1 : ResourceLimit;
                var strictFifo = ordering == HubActivityOrdering.KeyedFifo ||
                    (ordering == HubActivityOrdering.Legacy && Fifo);
                rules.Add(new("resource:" + hub + ":" + resource + ":" + Digest(key.GetString()!),
                    capacity, strictFifo));
            }
            return new(hub, activity, version, Partitions,
                rules.OrderBy(x => x.Key,StringComparer.Ordinal).ToArray(), ordering);
        }
    }

    private static string Digest(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
