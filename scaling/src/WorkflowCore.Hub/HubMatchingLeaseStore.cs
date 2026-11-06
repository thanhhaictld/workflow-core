using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

/// <summary>
/// PostgreSQL authority for matching ownership per logical Hub namespace.
/// Ownership is a performance/routing lease, not permission to bypass
/// HubTaskStore's independent durable task leases or resource/FIFO guards.
/// </summary>
public sealed record HubMatchingLease(string HubId, string OwnerId, string OwnerUrl,
    long Epoch, DateTime LeaseUntil);

public sealed class HubMatchingLeaseStore(HubTaskStore tasks)
{
    private NpgsqlDataSource Source => tasks.DataSource;

    public async Task<HubMatchingLease> AcquireOrReadAsync(
        string hub, string nodeId, string nodeUrl, TimeSpan duration, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(hub) || hub.Length > 80 ||
            string.IsNullOrWhiteSpace(nodeId) || nodeId.Length > 128 ||
            string.IsNullOrWhiteSpace(nodeUrl) || duration <= TimeSpan.Zero)
            throw new ArgumentException("Invalid matching owner registration");

        // One atomic statement. An unexpired owner cannot be displaced.
        // Recovering an expired lease increments its fencing generation.
        await using var cmd = Source.CreateCommand("""
            INSERT INTO wfc_scale.hub_matching_owner AS current_owner
                (hub_id,owner_id,owner_url,epoch,lease_until)
            VALUES (@hub,@owner,@url,1,clock_timestamp()+@duration)
            ON CONFLICT (hub_id) DO UPDATE SET
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
            RETURNING hub_id,owner_id,owner_url,epoch,lease_until
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("owner", nodeId);
        cmd.Parameters.AddWithValue("url", nodeUrl);
        cmd.Parameters.Add("duration", NpgsqlDbType.Interval).Value = duration;
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct)) return ReadLease(reader);
        }
        // A different, still-live owner won. Do not claim on this replica.
        return await ReadAsync(hub, ct) ??
            throw new InvalidOperationException("Matching owner disappeared during election");
    }

    public async Task<HubMatchingLease?> ReadAsync(string hub, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            SELECT hub_id,owner_id,owner_url,epoch,lease_until
            FROM wfc_scale.hub_matching_owner WHERE hub_id=@hub
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadLease(reader) : null;
    }

    public async Task<bool> RenewAsync(
        string hub, string nodeId, long epoch, TimeSpan duration, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            UPDATE wfc_scale.hub_matching_owner
            SET lease_until=clock_timestamp()+@duration, updated_at=clock_timestamp()
            WHERE hub_id=@hub AND owner_id=@owner AND epoch=@epoch
              AND lease_until>clock_timestamp()
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("owner", nodeId);
        cmd.Parameters.AddWithValue("epoch", epoch);
        cmd.Parameters.Add("duration", NpgsqlDbType.Interval).Value = duration;
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<bool> OwnsAsync(
        string hub, string nodeId, long epoch, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            SELECT 1 FROM wfc_scale.hub_matching_owner
            WHERE hub_id=@hub AND owner_id=@owner AND epoch=@epoch
              AND lease_until>clock_timestamp()
            """);
        cmd.Parameters.AddWithValue("hub", hub);
        cmd.Parameters.AddWithValue("owner", nodeId);
        cmd.Parameters.AddWithValue("epoch", epoch);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }

    private static HubMatchingLease ReadLease(NpgsqlDataReader reader) =>
        new(reader.GetString(0), reader.GetString(1), reader.GetString(2),
            reader.GetInt64(3), reader.GetDateTime(4));
}
