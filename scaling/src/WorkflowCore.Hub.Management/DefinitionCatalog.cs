using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Primitives;

namespace WorkflowCore.Scaling.Api;

/// <summary>
/// Immutable, normalized definition metadata. It captures structural contract,
/// not C# source, captured expression values, executable delegates or secrets.
/// </summary>
public sealed record DefinitionSchema(
    int SchemaFormat, string Id, int Version, string? Description, string? DataType,
    string DefaultErrorBehavior, string? DefaultRetryInterval,
    IReadOnlyList<DefinitionStep> Steps);
public sealed record DefinitionStep(
    int Id, string Name, string? ExternalId, string BodyType, string Kind,
    IReadOnlyList<int> Children, IReadOnlyList<DefinitionOutcome> Outcomes,
    IReadOnlyList<DefinitionBinding> Inputs, IReadOnlyList<DefinitionBinding> Outputs,
    string? ErrorBehavior, string? RetryInterval, int? CompensationStepId,
    bool HasCancelCondition, bool ProceedOnCancel);
public sealed record DefinitionOutcome(int TargetStepId, string? Label, string OutcomeType);
public sealed record DefinitionBinding(int Index, string Kind, string? TargetMember, string? SourceMember);
public sealed record CatalogVersion(
    string DefinitionId, int Version, string SchemaSha256, DateTime RegisteredAt,
    DefinitionSchema Schema, bool DriftDetected);
public sealed record DefinitionChange(string Change, string Path, string? Before, string? After);
public sealed record DefinitionComparison(
    string DefinitionId, int FromVersion, int ToVersion,
    string FromSha256, string ToSha256,
    IReadOnlyList<DefinitionChange> Changes);

public static class DefinitionMetadata
{
    public static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web) { WriteIndented = false };

    public static DefinitionSchema Snapshot(WorkflowDefinition definition) =>
        new(1, definition.Id, definition.Version, definition.Description,
            definition.DataType?.FullName, definition.DefaultErrorBehavior.ToString(),
            definition.DefaultErrorRetryInterval?.ToString("c"),
            definition.Steps.OrderBy(step => step.Id).Select(step => new DefinitionStep(
                step.Id, step.Name ?? step.BodyType.Name, step.ExternalId,
                step.BodyType.FullName ?? step.BodyType.Name,
                step.BodyType == typeof(Sequence) && step.Children.Count > 1 ? "parallel" : "step",
                step.Children.OrderBy(child => child).ToArray(),
                step.Outcomes.Select(outcome => new DefinitionOutcome(
                    outcome.NextStep, outcome.Label, outcome.GetType().Name)).ToArray(),
                Bindings(step.Inputs), Bindings(step.Outputs),
                step.ErrorBehavior?.ToString(), step.RetryInterval?.ToString("c"),
                step.CompensationStepId, step.CancelCondition is not null, step.ProceedOnCancel
            )).ToArray());

    private static IReadOnlyList<DefinitionBinding> Bindings(IReadOnlyList<IStepParameter> bindings) =>
        bindings.Select((binding, index) => binding is MemberMapParameter map
            ? new DefinitionBinding(index, "member", map.TargetMemberName, map.SourceMemberName)
            : new DefinitionBinding(index, binding.GetType().Name, null, null)).ToArray();

    public static string Serialize(DefinitionSchema schema) =>
        JsonSerializer.Serialize(schema, JsonOptions);

    public static string Hash(string json) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(json))).ToLowerInvariant();

    /// <summary>
    /// Compare normalized paths so structural edits are distinct from metadata
    /// formatting. A step is matched by stable ExternalId, otherwise by numeric
    /// step ID. Without explicit stable IDs, insertion can appear as many edits.
    /// </summary>
    public static DefinitionComparison Diff(CatalogVersion from, CatalogVersion to)
    {
        if (from.DefinitionId != to.DefinitionId)
            throw new ArgumentException("Versions must belong to the same definition.");

        var before = Flatten(from.Schema);
        var after = Flatten(to.Schema);
        var keys = before.Keys.Union(after.Keys).OrderBy(key => key, StringComparer.Ordinal);
        var changes = new List<DefinitionChange>();
        foreach (var key in keys)
        {
            var oldPresent = before.TryGetValue(key, out var oldValue);
            var newPresent = after.TryGetValue(key, out var newValue);
            if (oldPresent && newPresent && oldValue == newValue) continue;
            changes.Add(new DefinitionChange(oldPresent ? newPresent ? "Modified" : "Removed" : "Added",
                key, oldPresent ? oldValue : null, newPresent ? newValue : null));
        }
        return new DefinitionComparison(from.DefinitionId, from.Version, to.Version,
            from.SchemaSha256, to.SchemaSha256, changes);
    }

    private static SortedDictionary<string, string> Flatten(DefinitionSchema schema)
    {
        var values = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["description"] = JsonSerializer.Serialize(schema.Description),
            ["dataType"] = JsonSerializer.Serialize(schema.DataType),
            ["defaultErrorBehavior"] = JsonSerializer.Serialize(schema.DefaultErrorBehavior),
            ["defaultRetryInterval"] = JsonSerializer.Serialize(schema.DefaultRetryInterval)
        };
        foreach (var step in schema.Steps)
        {
            // JSON pointer-safe key; no human-controlled values in field names.
            var identity = string.IsNullOrWhiteSpace(step.ExternalId)
                ? $"id:{step.Id}"
                : $"external:{Uri.EscapeDataString(step.ExternalId)}";
            var p = "steps/" + identity + "/";
            values[p + "name"] = JsonSerializer.Serialize(step.Name);
            values[p + "id"] = JsonSerializer.Serialize(step.Id);
            values[p + "externalId"] = JsonSerializer.Serialize(step.ExternalId);
            values[p + "bodyType"] = JsonSerializer.Serialize(step.BodyType);
            values[p + "kind"] = JsonSerializer.Serialize(step.Kind);
            values[p + "children"] = JsonSerializer.Serialize(step.Children);
            values[p + "outcomes"] = JsonSerializer.Serialize(step.Outcomes);
            values[p + "inputs"] = JsonSerializer.Serialize(step.Inputs);
            values[p + "outputs"] = JsonSerializer.Serialize(step.Outputs);
            values[p + "errorBehavior"] = JsonSerializer.Serialize(step.ErrorBehavior);
            values[p + "retryInterval"] = JsonSerializer.Serialize(step.RetryInterval);
            values[p + "compensationStepId"] = JsonSerializer.Serialize(step.CompensationStepId);
            values[p + "hasCancelCondition"] = JsonSerializer.Serialize(step.HasCancelCondition);
            values[p + "proceedOnCancel"] = JsonSerializer.Serialize(step.ProceedOnCancel);
        }
        return values;
    }
}

/// <summary>
/// Passive snapshot catalog for code-first workflow definitions. The runtime
/// never consults this catalog to select versions or authorize starts.
/// Snapshots are immutable; same-version drift is reported, not enforced.
/// </summary>
public sealed class DefinitionCatalog(string connectionString, ILogger<DefinitionCatalog> logger)
{
    private readonly ConcurrentDictionary<(string Id, int Version), byte> _drift = new();

    private NpgsqlConnection Connect() => new(connectionString);

    public async Task ReconcileAsync(IEnumerable<WorkflowDefinition> definitions, CancellationToken ct)
    {
        foreach (var definition in definitions.OrderBy(def => def.Id, StringComparer.Ordinal)
                                              .ThenBy(def => def.Version))
        {
            var schema = DefinitionMetadata.Snapshot(definition);
            var json = DefinitionMetadata.Serialize(schema);
            var hash = DefinitionMetadata.Hash(json);
            await using var db = Connect();
            await db.OpenAsync(ct);
            await using (var insert = new NpgsqlCommand("""
                INSERT INTO wfc_scale.definition_version
                    (definition_id, version, schema_format, schema_json, schema_sha256)
                VALUES (@id, @version, 1, @schema, @sha)
                ON CONFLICT (definition_id, version) DO NOTHING
                """, db))
            {
                insert.Parameters.AddWithValue("id", definition.Id);
                insert.Parameters.AddWithValue("version", definition.Version);
                insert.Parameters.Add("schema", NpgsqlDbType.Jsonb).Value = json;
                insert.Parameters.AddWithValue("sha", hash);
                await insert.ExecuteNonQueryAsync(ct);
            }
            await using var verify = new NpgsqlCommand("""
                SELECT schema_sha256 FROM wfc_scale.definition_version
                WHERE definition_id = @id AND version = @version
                """, db);
            verify.Parameters.AddWithValue("id", definition.Id);
            verify.Parameters.AddWithValue("version", definition.Version);
            var storedHash = (string?)await verify.ExecuteScalarAsync(ct);
            if (storedHash != hash)
            {
                _drift[(definition.Id, definition.Version)] = 1;
                logger.LogWarning(
                    "Code-first workflow {DefinitionId} v{Version} differs from its archived metadata snapshot. " +
                    "Bump the workflow version to preserve reliable comparisons. Snapshot remains unchanged.",
                    definition.Id, definition.Version);
            }
            else
            {
                _drift.TryRemove((definition.Id, definition.Version), out _);
                logger.LogInformation("Captured definition metadata {DefinitionId} v{Version} SHA {Hash}",
                    definition.Id, definition.Version, hash);
            }
        }
    }

    public async Task<IReadOnlyList<CatalogVersion>> ListAsync(string? definitionId, CancellationToken ct)
    {
        var records = new List<CatalogVersion>();
        await using var db = Connect();
        await db.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT definition_id, version, schema_sha256,
                   registered_at, schema_json::text
            FROM wfc_scale.definition_version
            WHERE (@id IS NULL OR definition_id = @id)
            ORDER BY definition_id, version DESC
            """, db);
        cmd.Parameters.Add("id", NpgsqlDbType.Text).Value = (object?)definitionId ?? DBNull.Value;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var id = reader.GetString(0);
            var version = reader.GetInt32(1);
            var schema = JsonSerializer.Deserialize<DefinitionSchema>(reader.GetString(4),
                DefinitionMetadata.JsonOptions) ??
                throw new InvalidOperationException("Invalid definition metadata snapshot");
            records.Add(new CatalogVersion(id, version, reader.GetString(2),
                reader.GetDateTime(3), schema, _drift.ContainsKey((id, version))));
        }
        return records;
    }

    public async Task<CatalogVersion?> GetAsync(string id, int version, CancellationToken ct) =>
        (await ListAsync(id, ct)).FirstOrDefault(item => item.Version == version);

    public async Task<DefinitionComparison?> CompareAsync(
        string id, int fromVersion, int toVersion, CancellationToken ct)
    {
        var list = await ListAsync(id, ct);
        var from = list.FirstOrDefault(v => v.Version == fromVersion);
        var to = list.FirstOrDefault(v => v.Version == toVersion);
        return from is null || to is null ? null : DefinitionMetadata.Diff(from, to);
    }
}
