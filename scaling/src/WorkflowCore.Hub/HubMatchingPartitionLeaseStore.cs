using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

/// <summary>
/// Matching ownership is ephemeral and may be reacquired after a crash.
/// A monotonic ownership epoch fences stale renewals; task leases and activity
/// ordering are separately enforced in HubTaskStore.
/// </summary>
public sealed record HubMatchingPartitionLease(string HubId, int Partition,
    string OwnerId, string OwnerUrl, long Epoch, DateTime LeaseUntil);

public sealed class HubMatchingPartitionLeaseStore(HubTaskStore tasks)
{
    private NpgsqlDataSource Source => tasks.DataSource;

    public async Task EnsureConfigAsync(string hub, int count, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(hub) || hub.Length > 80 ||
            count is < 1 or > 64)
            throw new ArgumentException("Invalid matching partition configuration");
        await using var cmd = Source.CreateCommand("""
            INSERT INTO wfc_scale.hub_matching_partition_config AS c (hub_id, partition_count)
            VALUES (@hub,@count)
            ON CONFLICT (hub_id) DO UPDATE
                SET partition_count=EXCLUDED.partition_count
            WHERE c.partition_count=EXCLUDED.partition_count
            RETURNING partition_count
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("count", count);
        if (await cmd.ExecuteScalarAsync(ct) is null)
            throw new InvalidOperationException(
                $"Matching partition count mismatch for hub '{hub}'. " +
                "Coordinate topology migration; never silently remap live task queues.");
    }

    public async Task<HubMatchingPartitionLease> AcquireOrReadAsync(
        string hub, int partition, int count, string nodeId, string nodeUrl,
        TimeSpan duration, CancellationToken ct)
    {
        Validate(partition, count, nodeId, nodeUrl, duration);
        await EnsureConfigAsync(hub, count, ct);
        await using var cmd = Source.CreateCommand("""
            INSERT INTO wfc_scale.hub_matching_partition_owner AS current_owner
                (hub_id,matching_partition,owner_id,owner_url,epoch,lease_until)
            VALUES (@hub,@partition,@owner,@url,1,clock_timestamp()+@duration)
            ON CONFLICT (hub_id,matching_partition) DO UPDATE SET
                owner_id=EXCLUDED.owner_id,
                owner_url=EXCLUDED.owner_url,
                epoch=CASE WHEN current_owner.owner_id=EXCLUDED.owner_id
                                 AND current_owner.lease_until>clock_timestamp()
                           THEN current_owner.epoch
                           ELSE current_owner.epoch+1 END,
                lease_until=clock_timestamp()+@duration,
                updated_at=clock_timestamp()
            WHERE current_owner.lease_until<=clock_timestamp()
               OR current_owner.owner_id=EXCLUDED.owner_id
            RETURNING hub_id,matching_partition,owner_id,owner_url,epoch,lease_until
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("partition", partition);
        cmd.Parameters.AddWithValue("owner", nodeId);
        cmd.Parameters.AddWithValue("url", nodeUrl);
        cmd.Parameters.Add("duration", NpgsqlDbType.Interval).Value = duration;
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct)) return Read(reader);
        }
        return await ReadAsync(hub, partition, ct) ??
            throw new InvalidOperationException("Matching partition owner disappeared during election");
    }

    public async Task<HubMatchingPartitionLease?> ReadAsync(
        string hub, int partition, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            SELECT hub_id,matching_partition,owner_id,owner_url,epoch,lease_until
            FROM wfc_scale.hub_matching_partition_owner
            WHERE hub_id=@hub AND matching_partition=@partition
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("partition", partition);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Read(reader) : null;
    }

    public async Task<IReadOnlyList<HubMatchingPartitionLease>> ListAsync(
        string hub, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            SELECT hub_id,matching_partition,owner_id,owner_url,epoch,lease_until
            FROM wfc_scale.hub_matching_partition_owner
            WHERE hub_id=@hub ORDER BY matching_partition
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        var leases = new List<HubMatchingPartitionLease>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) leases.Add(Read(reader));
        return leases;
    }

    public async Task<bool> RenewAsync(
        string hub, int partition, string nodeId, long epoch,
        TimeSpan duration, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            UPDATE wfc_scale.hub_matching_partition_owner
            SET lease_until=clock_timestamp()+@duration,updated_at=clock_timestamp()
            WHERE hub_id=@hub AND matching_partition=@partition
              AND owner_id=@owner AND epoch=@epoch AND lease_until>clock_timestamp()
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("partition", partition);
        cmd.Parameters.AddWithValue("owner", nodeId);
        cmd.Parameters.AddWithValue("epoch", epoch);
        cmd.Parameters.Add("duration", NpgsqlDbType.Interval).Value = duration;
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<bool> OwnsAsync(
        string hub, int partition, string nodeId, long epoch, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            SELECT 1 FROM wfc_scale.hub_matching_partition_owner
            WHERE hub_id=@hub AND matching_partition=@partition
              AND owner_id=@owner AND epoch=@epoch AND lease_until>clock_timestamp()
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("partition", partition);
        cmd.Parameters.AddWithValue("owner", nodeId);
        cmd.Parameters.AddWithValue("epoch", epoch);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }

    public async Task<bool> ReleaseAsync(
        string hub, int partition, string nodeId, long epoch, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            UPDATE wfc_scale.hub_matching_partition_owner
            SET lease_until=clock_timestamp(),updated_at=clock_timestamp()
            WHERE hub_id=@hub AND matching_partition=@partition
              AND owner_id=@owner AND epoch=@epoch
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("partition", partition);
        cmd.Parameters.AddWithValue("owner", nodeId);
        cmd.Parameters.AddWithValue("epoch", epoch);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    private static void Validate(int partition, int count,
        string nodeId, string nodeUrl, TimeSpan duration)
    {
        if (count is < 1 or > 64 || partition < 0 || partition >= count ||
            string.IsNullOrWhiteSpace(nodeId) || nodeId.Length > 128 ||
            !Uri.TryCreate(nodeUrl, UriKind.Absolute, out var endpoint) ||
            endpoint.Scheme is not ("http" or "https") ||
            duration <= TimeSpan.Zero || duration > TimeSpan.FromMinutes(1))
            throw new ArgumentException("Invalid matching partition lease parameters");
    }

    private static HubMatchingPartitionLease Read(NpgsqlDataReader r) =>
        new(r.GetString(0),r.GetInt32(1),r.GetString(2),r.GetString(3),
            r.GetInt64(4),r.GetDateTime(5));
}
