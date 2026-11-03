using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

/// <summary>
/// Read-only operational projections from the Hub-owned PostgreSQL schema.
/// No worker credentials or payloads leave the gateway. The first-attempt
/// latency series intentionally excludes remote executions without attempt rows.
/// </summary>
public sealed class HubOperationsStore(HubTaskStore tasks)
{
    private NpgsqlDataSource Source => tasks.DataSource;

    private static async Task<JsonElement> ReadAsync(NpgsqlConnection db,
        string sql, string? hub, int windowMinutes, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, db);
        cmd.Parameters.Add("hub", NpgsqlDbType.Text).Value = (object?)hub ?? DBNull.Value;
        if (sql.Contains("@window", StringComparison.Ordinal))
            cmd.Parameters.AddWithValue("window", windowMinutes);
        var result = (string?)await cmd.ExecuteScalarAsync(ct);
        using var doc = JsonDocument.Parse(result ?? "null");
        return doc.RootElement.Clone();
    }

    public async Task<object> SnapshotAsync(string? hub, int windowMinutes, CancellationToken ct)
    {
        if (hub is { Length: > 80 } || windowMinutes is not (60 or 360 or 1440))
            throw new ArgumentException("Invalid hub or windowMinutes (60, 360, 1440)");
        await using var db = await Source.OpenConnectionAsync(ct);
        var workflows = await ReadAsync(db, """
            SELECT jsonb_build_object(
                'active',count(*) FILTER (WHERE state IN (0,1)),
                'ready',count(*) FILTER (WHERE state=0),
                'running',count(*) FILTER (WHERE state=1),
                'waiting',count(*) FILTER (WHERE state IN (0,1) AND waiting_for IS NOT NULL),
                'scheduled',count(*) FILTER (WHERE state IN (0,1) AND EXISTS(
                    SELECT 1 FROM wfc_scale.hub_timer timer
                    WHERE timer.workflow_id=w.id AND timer.state=0 AND timer.due_at>now())),
                'completed24h',count(*) FILTER (WHERE state=2 AND completed_at>=now()-interval '24 hours'),
                'failed24h',count(*) FILTER (WHERE state=3 AND completed_at>=now()-interval '24 hours'),
                'completedPerMinute',(count(*) FILTER (WHERE state=2 AND
                    completed_at>=now()-interval '5 minutes')::double precision / 5.0)
            )::text FROM wfc_scale.hub_workflow w
            WHERE (@hub IS NULL OR hub_id=@hub)
            """, hub, windowMinutes, ct);
        var tasksSummary = await ReadAsync(db, """
            SELECT jsonb_build_object(
                'ready',count(*) FILTER (WHERE state=0 AND due_at<=now() AND attempts<max_attempts),
                'leased',count(*) FILTER (WHERE state=1 AND lease_until>now()),
                'delayed',count(*) FILTER (WHERE state=0 AND due_at>now()),
                'retryDue',count(*) FILTER (WHERE state=0 AND due_at<=now() AND attempts>0 AND attempts<max_attempts),
                'dead',count(*) FILTER (WHERE state=3),
                'reclaimable',count(*) FILTER (WHERE state=1 AND lease_until<=now() AND attempts<max_attempts),
                'runnableWorkflows',count(DISTINCT workflow_id) FILTER (
                    WHERE workflow_id IS NOT NULL AND attempts<max_attempts AND
                    ((state=0 AND due_at<=now()) OR (state=1 AND lease_until<=now()))),
                'oldestReadyAgeSeconds',max(extract(epoch FROM now()-due_at))
                    FILTER (WHERE state=0 AND due_at<=now() AND attempts<max_attempts)
            )::text FROM wfc_scale.hub_task
            WHERE (@hub IS NULL OR hub_id=@hub)
            """, hub, windowMinutes, ct);
        // Capacity/FIFO blocked count is a point-in-time estimate. A worker
        // without a matching capability can produce further queue delays.
        var contention = await ReadAsync(db, """
            WITH occupied AS (
              SELECT c.resource_key,count(*) AS active
              FROM wfc_scale.hub_task_constraint c
              JOIN wfc_scale.hub_task t ON t.id=c.task_id
              WHERE t.state=1 AND t.lease_until>now() GROUP BY c.resource_key
            ), blocked AS (
              SELECT DISTINCT t.id FROM wfc_scale.hub_task t
              JOIN wfc_scale.hub_task_constraint c ON c.task_id=t.id
              JOIN wfc_scale.hub_resource r ON r.resource_key=c.resource_key
              LEFT JOIN occupied o ON o.resource_key=c.resource_key
              WHERE (@hub IS NULL OR t.hub_id=@hub)
                AND t.state=0 AND t.due_at<=now() AND t.attempts<t.max_attempts
                AND (coalesce(o.active,0)>=r.capacity OR
                  (r.strict_fifo AND EXISTS (
                    SELECT 1 FROM wfc_scale.hub_task_constraint prior
                    JOIN wfc_scale.hub_task previous ON previous.id=prior.task_id
                    WHERE prior.resource_key=c.resource_key
                    AND previous.sequence<t.sequence AND previous.state IN (0,1))))
            )
            SELECT jsonb_build_object('blockedReady',count(*))::text FROM blocked
            """, hub, windowMinutes, ct);
        var latency = await ReadAsync(db, """
            SELECT jsonb_build_object(
                'queueP95Seconds',percentile_cont(0.95) WITHIN GROUP
                  (ORDER BY extract(epoch FROM a.claimed_at-t.created_at))
                  FILTER (WHERE a.attempt_number=1),
                'leaseP95Seconds',percentile_cont(0.95) WITHIN GROUP
                  (ORDER BY extract(epoch FROM a.finished_at-a.claimed_at))
                  FILTER (WHERE a.finished_at IS NOT NULL),
                'queueSamples',count(*) FILTER (WHERE a.attempt_number=1)
            )::text
            FROM wfc_scale.hub_task_attempt a
            JOIN wfc_scale.hub_task t ON t.id=a.task_id
            WHERE a.claimed_at>=now()-interval '1 hour'
              AND (@hub IS NULL OR t.hub_id=@hub)
            """, hub, windowMinutes, ct);
        var partitions = await ReadAsync(db, """
            WITH w AS (
              SELECT hub_id,partition_id,count(*) AS active
              FROM wfc_scale.hub_workflow
              WHERE state IN (0,1) AND (@hub IS NULL OR hub_id=@hub)
              GROUP BY 1,2
            ), t AS (
              SELECT hub_id,partition_id,
                count(*) FILTER (WHERE state=0 AND due_at<=now() AND attempts<max_attempts) AS ready,
                count(*) FILTER (WHERE state=1 AND lease_until>now()) AS leased,
                count(*) FILTER (WHERE state=0 AND due_at>now()) AS delayed,
                count(*) FILTER (WHERE state=3) AS dead,
                max(extract(epoch FROM now()-due_at)) FILTER
                  (WHERE state=0 AND due_at<=now() AND attempts<max_attempts) AS oldest
              FROM wfc_scale.hub_task WHERE (@hub IS NULL OR hub_id=@hub)
              GROUP BY 1,2
            ), rows AS (
              SELECT coalesce(w.hub_id,t.hub_id) AS hub,
                coalesce(w.partition_id,t.partition_id) AS partition,
                coalesce(w.active,0) AS active,coalesce(t.ready,0) AS ready,
                coalesce(t.leased,0) AS leased,coalesce(t.delayed,0) AS delayed,
                coalesce(t.dead,0) AS dead,t.oldest
              FROM w FULL JOIN t USING (hub_id,partition_id)
              ORDER BY ready DESC LIMIT 200
            )
            SELECT coalesce(jsonb_agg(jsonb_build_object(
              'hubId',hub,'partitionId',partition,'activeWorkflows',active,
              'ready',ready,'leased',leased,'delayed',delayed,'dead',dead,
              'oldestReadyAgeSeconds',oldest)), '[]'::jsonb)::text FROM rows
            """, hub, windowMinutes, ct);
        var activities = await ReadAsync(db, """
            WITH rows AS (
              SELECT hub_id,task_type,
                count(*) FILTER (WHERE state=0 AND due_at<=now() AND attempts<max_attempts) AS ready,
                count(*) FILTER (WHERE state=1 AND lease_until>now()) AS leased,
                count(*) FILTER (WHERE state=0 AND due_at>now()) AS delayed,
                count(*) FILTER (WHERE state=3) AS dead,
                count(*) FILTER (WHERE state=0 AND due_at<=now() AND attempts>0 AND attempts<max_attempts) AS retry,
                max(extract(epoch FROM now()-due_at)) FILTER
                    (WHERE state=0 AND due_at<=now() AND attempts<max_attempts) AS oldest
              FROM wfc_scale.hub_task WHERE (@hub IS NULL OR hub_id=@hub)
              GROUP BY hub_id,task_type ORDER BY ready DESC,leased DESC LIMIT 100
            )
            SELECT coalesce(jsonb_agg(jsonb_build_object(
              'hubId',hub_id,'activityType',task_type,'ready',ready,'leased',leased,
              'delayed',delayed,'dead',dead,'retryDue',retry,
              'oldestReadyAgeSeconds',oldest)), '[]'::jsonb)::text FROM rows
            """, hub, windowMinutes, ct);
        // Do not reveal hashed constraint keys. Aggregate by resource class.
        // Concurrency is a lease-permit calculation, not worker CPU usage.
        var policies = await ReadAsync(db, """
            WITH relevant AS (
              SELECT DISTINCT c.resource_key
              FROM wfc_scale.hub_task_constraint c
              JOIN wfc_scale.hub_task t ON t.id=c.task_id
              WHERE (@hub IS NULL OR t.hub_id=@hub)
            ), usage AS (
              SELECT c.resource_key,
                count(*) FILTER (WHERE t.state=1 AND t.lease_until>now()) AS active,
                count(*) FILTER (WHERE t.state=0 AND t.due_at<=now() AND t.attempts<t.max_attempts) AS queued
              FROM wfc_scale.hub_task_constraint c
              JOIN wfc_scale.hub_task t ON t.id=c.task_id
              WHERE c.resource_key IN (SELECT resource_key FROM relevant)
              GROUP BY c.resource_key
            ), rows AS (
              SELECT regexp_replace(r.resource_key, ':[0-9a-f]{64}$','') AS name,
                count(*) AS resources,sum(r.capacity) AS capacity,
                sum(coalesce(u.active,0)) AS active,sum(coalesce(u.queued,0)) AS queued,
                count(*) FILTER (WHERE coalesce(u.active,0)>=r.capacity) AS saturated
              FROM wfc_scale.hub_resource r
              JOIN relevant keys ON keys.resource_key=r.resource_key
              LEFT JOIN usage u ON u.resource_key=r.resource_key
              GROUP BY 1 ORDER BY queued DESC LIMIT 80
            )
            SELECT coalesce(jsonb_agg(jsonb_build_object(
              'name',name,'resources',resources,'capacity',capacity,
              'active',active,'queued',queued,'saturated',saturated)), '[]'::jsonb)::text
            FROM rows
            """, hub, windowMinutes, ct);
        // Only leased owners are visible, not idle/registered worker fleet.
        var workers = await ReadAsync(db, """
            WITH rows AS (
              SELECT owner,count(*) AS leased,count(DISTINCT (hub_id,partition_id)) AS partitions,
                     max(updated_at) AS last_claim
              FROM wfc_scale.hub_task
              WHERE state=1 AND lease_until>now() AND owner IS NOT NULL
                AND (@hub IS NULL OR hub_id=@hub)
              GROUP BY owner ORDER BY leased DESC LIMIT 100
            )
            SELECT coalesce(jsonb_agg(jsonb_build_object(
              'workerId',owner,'leasedTasks',leased,
              'partitions',partitions,'lastClaimAt',last_claim)), '[]'::jsonb)::text
            FROM rows
            """, hub, windowMinutes, ct);
        var timers = await ReadAsync(db, """
            SELECT jsonb_build_object(
              'pending',count(*) FILTER (WHERE state=0),
              'due',count(*) FILTER (WHERE state=0 AND due_at<=now()),
              'fired24h',count(*) FILTER (WHERE state=1 AND fired_at>=now()-interval '24 hours')
            )::text FROM wfc_scale.hub_timer
            WHERE (@hub IS NULL OR hub_id=@hub)
            """, hub, windowMinutes, ct);
        var trends = await ReadAsync(db, """
            WITH bins AS (
              SELECT generate_series(
                date_bin(make_interval(mins=>CASE WHEN @window=60 THEN 5 WHEN @window=360 THEN 15 ELSE 60 END),
                  now()-make_interval(mins=>@window),'2000-01-01'::timestamptz),
                date_bin(make_interval(mins=>CASE WHEN @window=60 THEN 5 WHEN @window=360 THEN 15 ELSE 60 END),
                  now(),'2000-01-01'::timestamptz),
                make_interval(mins=>CASE WHEN @window=60 THEN 5 WHEN @window=360 THEN 15 ELSE 60 END)
              ) AS bucket
            ), s AS (
              SELECT date_bin(make_interval(mins=>CASE WHEN @window=60 THEN 5 WHEN @window=360 THEN 15 ELSE 60 END),
                  bucket,'2000-01-01'::timestamptz) AS slot,
                (array_agg(ready_activities ORDER BY bucket DESC))[1] AS ready,
                (array_agg(leased_activities ORDER BY bucket DESC))[1] AS leased,
                (array_agg(runnable_workflows ORDER BY bucket DESC))[1] AS runnable
              FROM wfc_scale.hub_monitor_sample
              WHERE hub_id=coalesce(@hub,'')
                AND bucket>=now()-make_interval(mins=>@window) GROUP BY 1
            ), started AS (
              SELECT date_bin(make_interval(mins=>CASE WHEN @window=60 THEN 5 WHEN @window=360 THEN 15 ELSE 60 END),
                  created_at,'2000-01-01'::timestamptz) AS slot,count(*) AS value
              FROM wfc_scale.hub_workflow
              WHERE created_at>=now()-make_interval(mins=>@window)
                AND (@hub IS NULL OR hub_id=@hub) GROUP BY 1
            ), done AS (
              SELECT date_bin(make_interval(mins=>CASE WHEN @window=60 THEN 5 WHEN @window=360 THEN 15 ELSE 60 END),
                  completed_at,'2000-01-01'::timestamptz) AS slot,
                count(*) FILTER (WHERE state=2) AS completed,
                count(*) FILTER (WHERE state=3) AS failed
              FROM wfc_scale.hub_workflow
              WHERE completed_at>=now()-make_interval(mins=>@window)
                AND (@hub IS NULL OR hub_id=@hub) GROUP BY 1
            ), task_done AS (
              SELECT date_bin(make_interval(mins=>CASE WHEN @window=60 THEN 5 WHEN @window=360 THEN 15 ELSE 60 END),
                  updated_at,'2000-01-01'::timestamptz) AS slot,
                count(*) FILTER (WHERE state=2) AS completed,
                count(*) FILTER (WHERE state=3) AS dead
              FROM wfc_scale.hub_task
              WHERE updated_at>=now()-make_interval(mins=>@window)
                AND state IN (2,3) AND (@hub IS NULL OR hub_id=@hub) GROUP BY 1
            )
            SELECT coalesce(jsonb_agg(jsonb_build_object(
              'bucket',b.bucket,'started',coalesce(started.value,0),
              'completed',coalesce(done.completed,0),'failed',coalesce(done.failed,0),
              'activityCompleted',coalesce(task_done.completed,0),
              'activityDead',coalesce(task_done.dead,0),
              'ready',s.ready,'leased',s.leased,'runnable',s.runnable
            ) ORDER BY b.bucket), '[]'::jsonb)::text
            FROM bins b LEFT JOIN s ON b.bucket=s.slot
            LEFT JOIN started ON b.bucket=started.slot
            LEFT JOIN done ON b.bucket=done.slot
            LEFT JOIN task_done ON b.bucket=task_done.slot
            """, hub, windowMinutes, ct);
        return new { observedAt=DateTime.UtcNow, windowMinutes,
            workflows, tasks=tasksSummary, contention, latency,
            partitions, activities, policies, workers, timers, trends };
    }

    // Per-minute durable snapshots, idempotent across multiple Hub replicas.
    public async Task RecordSampleAsync(CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            WITH grouped AS (
              SELECT hub_id,
                count(*) FILTER (WHERE state=0 AND due_at<=now() AND attempts<max_attempts) AS ready,
                count(*) FILTER (WHERE state=1 AND lease_until>now()) AS leased,
                count(DISTINCT workflow_id) FILTER (WHERE workflow_id IS NOT NULL
                   AND attempts<max_attempts AND
                   ((state=0 AND due_at<=now()) OR (state=1 AND lease_until<=now()))) AS runnable
              FROM wfc_scale.hub_task GROUP BY hub_id
            ), entries AS (
              SELECT hub_id,ready,leased,runnable FROM grouped
              UNION ALL
              SELECT ''::text,sum(ready)::bigint,sum(leased)::bigint,
                (SELECT count(DISTINCT workflow_id) FROM wfc_scale.hub_task
                 WHERE workflow_id IS NOT NULL AND attempts<max_attempts
                   AND ((state=0 AND due_at<=now()) OR (state=1 AND lease_until<=now())))::bigint
              FROM grouped
            )
            INSERT INTO wfc_scale.hub_monitor_sample
              (bucket,hub_id,ready_activities,leased_activities,runnable_workflows)
            SELECT date_trunc('minute',now()),hub_id,coalesce(ready,0),
                   coalesce(leased,0),coalesce(runnable,0) FROM entries
            ON CONFLICT (bucket,hub_id) DO UPDATE SET
                ready_activities=excluded.ready_activities,
                leased_activities=excluded.leased_activities,
                runnable_workflows=excluded.runnable_workflows
            """);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}