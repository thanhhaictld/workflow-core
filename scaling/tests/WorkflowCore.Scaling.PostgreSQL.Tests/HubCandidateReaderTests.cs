using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateReaderTests
{
    [Fact]
    public async Task OversizedResourcesUseUnknownConstraintFallbackWithoutLeasing()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate reader tests");
        var hub = "candidate-large-" + Guid.NewGuid().ToString("N");
        var id = Guid.NewGuid(); var key = new string('x', 512) + id;
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@id,@hub,0,'activity',1,'{}');
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,false);
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@id,@key);
            """, db))
        {
            command.Parameters.AddWithValue("id", id); command.Parameters.AddWithValue("hub", hub);
            command.Parameters.AddWithValue("key", key); await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var small = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Ready, horizon, null, 1, 256, default);
        var fallback = Assert.Single(small.Items);
        Assert.Equal(id, fallback.Id);
        Assert.False(fallback.ResourceKeysComplete);
        Assert.Empty(fallback.ResourceKeys);
        Assert.NotNull(small.Next);
        var page = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Ready, horizon, null, 1, 2048, default);
        var item = Assert.Single(page.Items);
        Assert.Equal(id, item.Id);
        Assert.Equal(key, Assert.Single(item.ResourceKeys));
        Assert.True(item.ResourceKeysComplete);
        Assert.Equal(0, item.Epoch);
    }

    [Fact]
    public async Task ExpiredPagesUseDeadlineAndSequenceCursor()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate reader tests");
        var hub = "candidate-expired-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        var expected = new List<Guid>();
        for (var i = 0; i < 3; i++)
        {
            var id = Guid.NewGuid(); expected.Add(id);
            await using var command = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,lease_until)
                VALUES (@id,@hub,0,'activity',1,'{}',1,'worker',TIMESTAMPTZ '2020-01-01 00:00:00+00')
                """, db);
            command.Parameters.AddWithValue("id", id); command.Parameters.AddWithValue("hub", hub);
            await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var actual = new List<Guid>(); HubCandidateCursor? cursor = null;
        while (true)
        {
            var page = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Expired, horizon, cursor, 1, 1024, default);
            actual.AddRange(page.Items.Select(item => item.Id)); cursor = page.Next;
            if (page.EndOfStream) break;
        }
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task PagesRespectHorizonCursorAndByteLimit()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate reader tests");
        var hub = "candidate-pages-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        async Task<Guid> Insert()
        {
            var id = Guid.NewGuid();
            await using var command = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')", db);
            command.Parameters.AddWithValue("id", id); command.Parameters.AddWithValue("hub", hub);
            await command.ExecuteNonQueryAsync(); return id;
        }
        var expected = new List<Guid>();
        for (var i = 0; i < 5; i++) expected.Add(await Insert());
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var late = await Insert();
        var all = new List<Guid>(); HubCandidateCursor? cursor = null;
        do
        {
            var page = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Ready, horizon, cursor, 3, 256, default);
            all.AddRange(page.Items.Select(x => x.Id)); cursor = page.Next;
            if (page.EndOfStream) break;
        } while (all.Count <= 5);
        Assert.Equal(expected, all);
        var nextHorizon = await reader.CaptureHorizonAsync(hub, default);
        var fresh = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Ready, nextHorizon, cursor, 3, 1024, default);
        Assert.Equal(late, Assert.Single(fresh.Items).Id);
        var empty = await reader.ReadPageAsync(hub, 1, HubCandidateStream.Ready, nextHorizon, null, 3, 1024, default);
        Assert.True(empty.EndOfStream);
    }
    [Fact]
    public async Task NewSweepRecoversLateCommitAndTasksBecomingDueOrExpiredBehindCursor()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-wrap-" + Guid.NewGuid().ToString("N");
        await using var early = new NpgsqlConnection(database);
        await using var db = new NpgsqlConnection(database);
        await early.OpenAsync(); await db.OpenAsync();
        await using var tx = await early.BeginTransactionAsync();
        var late = Guid.NewGuid(); var due = Guid.NewGuid(); var expired = Guid.NewGuid(); var head = Guid.NewGuid();
        await using (var cmd = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')", early, tx))
        { cmd.Parameters.AddWithValue("id", late); cmd.Parameters.AddWithValue("hub", hub); await cmd.ExecuteNonQueryAsync(); }
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,due_at)
            VALUES (@due,@hub,0,'activity',1,'{}',now()+interval '1 hour'), (@head,@hub,0,'activity',1,'{}',now());
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,lease_until)
            VALUES (@expired,@hub,0,'activity',1,'{}',1,'worker',now()+interval '1 hour');
            """, db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("due",due); cmd.Parameters.AddWithValue("head",head); cmd.Parameters.AddWithValue("expired",expired); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var first = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,null,10,4096,default);
        Assert.Equal(head, Assert.Single(first.Items).Id);
        await tx.CommitAsync();
        await using (var cmd = new NpgsqlCommand("UPDATE wfc_scale.hub_task SET due_at=now()-interval '1 hour' WHERE id=@due; UPDATE wfc_scale.hub_task SET lease_until=now()-interval '1 hour' WHERE id=@expired",db))
        { cmd.Parameters.AddWithValue("due",due); cmd.Parameters.AddWithValue("expired",expired); await cmd.ExecuteNonQueryAsync(); }
        var remainder = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,first.Next,10,4096,default);
        Assert.Empty(remainder.Items);
        var fresh = await reader.CaptureHorizonAsync(hub,default);
        var wrapped = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,fresh,null,10,4096,default);
        Assert.Contains(wrapped.Items, x => x.Id == late);
        Assert.Contains(wrapped.Items, x => x.Id == due);
        var expiredPage = await reader.ReadPageAsync(hub,0,HubCandidateStream.Expired,fresh,null,10,4096,default);
        Assert.Equal(expired, Assert.Single(expiredPage.Items).Id);
    }

    [Fact]
    public async Task DueStreamUsesDeadlineAndSequenceCursor()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-due-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        var earlier = Guid.NewGuid(); var later = Guid.NewGuid();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,due_at)
            VALUES (@later,@hub,0,'activity',1,'{}',now()-interval '1 hour'),
                   (@earlier,@hub,0,'activity',1,'{}',now()-interval '2 hours')
            """,db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("later",later); cmd.Parameters.AddWithValue("earlier",earlier); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var first = await reader.ReadPageAsync(hub,0,HubCandidateStream.ReadyDue,horizon,null,1,1024,default);
        Assert.Equal(earlier, Assert.Single(first.Items).Id);
        var second = await reader.ReadPageAsync(hub,0,HubCandidateStream.ReadyDue,horizon,first.Next,1,1024,default);
        Assert.Equal(later, Assert.Single(second.Items).Id);
    }

    [Fact]
    public async Task BucketStreamsCoverSkewWithoutLeasing()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-buckets-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            SELECT gen_random_uuid(),@hub,g,'activity',1,'{}' FROM generate_series(0,63) g;
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            SELECT gen_random_uuid(),@hub,64,'activity',1,'{}' FROM generate_series(1,10);
            """,db)) { cmd.Parameters.AddWithValue("hub",hub); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub,default); var total = 0;
        for (var bucket = 0; bucket < 64; bucket++)
        {
            HubCandidateCursor? cursor = null; var count = 0;
            do { var page = await reader.ReadPageAsync(hub,bucket,HubCandidateStream.Ready,horizon,cursor,3,4096,default);
                 count += page.Items.Count; cursor = page.Next; if (page.EndOfStream) break; } while (true);
            Assert.Equal(bucket == 0 ? 11 : 1,count); total += count;
        }
        Assert.Equal(74,total);
        await using var check = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_task WHERE hub_id=@hub AND (attempts<>0 OR epoch<>0 OR state<>0)",db);
        check.Parameters.AddWithValue("hub",hub); Assert.Equal(0L,(long)(await check.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task ExpiredTaskBehindDeadlineCursorAppearsAfterWrap()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-expiry-wrap-" + Guid.NewGuid().ToString("N"); var a = Guid.NewGuid(); var b = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,lease_until)
            VALUES (@a,@hub,0,'activity',1,'{}',1,'worker',now()-interval '1 hour'),
                   (@b,@hub,0,'activity',1,'{}',1,'worker',now()+interval '1 hour');
            """,db)) { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("a",a); cmd.Parameters.AddWithValue("b",b); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub,default);
        var first = await reader.ReadPageAsync(hub,0,HubCandidateStream.Expired,horizon,null,1,1024,default);
        Assert.Equal(a,Assert.Single(first.Items).Id);
        await using (var cmd = new NpgsqlCommand("UPDATE wfc_scale.hub_task SET lease_until=now()-interval '2 hours' WHERE id=@id",db))
        { cmd.Parameters.AddWithValue("id",b); await cmd.ExecuteNonQueryAsync(); }
        var rest = await reader.ReadPageAsync(hub,0,HubCandidateStream.Expired,horizon,first.Next,2,1024,default);
        Assert.Empty(rest.Items);
        var next = await reader.CaptureHorizonAsync(hub,default);
        var wrapped = await reader.ReadPageAsync(hub,0,HubCandidateStream.Expired,next,null,2,1024,default);
        Assert.Equal(b,wrapped.Items[0].Id);
    }

    [Fact]
    public async Task OversizedHeadIsReportedAndDoesNotBlockFollowingCandidate()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "oversized-head-" + Guid.NewGuid().ToString("N"); var oversized = Guid.NewGuid(); var ordinary = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@oversized,@hub,0,@large,1,'{}'),(@ordinary,@hub,0,'activity',1,'{}')
            """,db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("oversized",oversized); cmd.Parameters.AddWithValue("ordinary",ordinary); cmd.Parameters.AddWithValue("large",new string('z',4000)); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub,default);
        var page = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,null,10,256,default);
        Assert.Equal(oversized,Assert.Single(page.Oversized!).Id);
        Assert.Equal(ordinary,Assert.Single(page.Items).Id);
        Assert.Equal(page.Items[0].Sequence,page.Next!.Sequence);
    }

    [Fact]
    public async Task GlobalHorizonCoversOlderHubWhileLaterInsertsRemainOutsideSweep()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "old-hub-" + Guid.NewGuid().ToString("N"); var other = hub+"-other"; var old = Guid.NewGuid(); var later = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        async Task Insert(Guid id,string target)
        { await using var cmd = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')",db); cmd.Parameters.AddWithValue("id",id); cmd.Parameters.AddWithValue("hub",target); await cmd.ExecuteNonQueryAsync(); }
        await Insert(old,hub); await Insert(Guid.NewGuid(),other);
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub,default);
        await Insert(Guid.NewGuid(),other); await Insert(later,hub);
        var page = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,null,10,2048,default);
        Assert.Equal(old,Assert.Single(page.Items).Id);
        var fresh = await reader.CaptureHorizonAsync(hub,default);
        var next = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,fresh,page.Next,10,2048,default);
        Assert.Equal(later,Assert.Single(next.Items).Id);
    }

}
