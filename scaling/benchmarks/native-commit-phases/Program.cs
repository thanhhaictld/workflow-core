using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;

// The benchmark exercises the real HubNativeWorkflowStore.CommitAsync method.
// It excludes fixture creation and workflow claiming to isolate native commits.
var connection = Environment.GetEnvironmentVariable("WFC_TEST_PG")
    ?? throw new InvalidOperationException("WFC_TEST_PG required");
var unpooled = new NpgsqlConnectionStringBuilder(connection){Pooling=false}.ConnectionString;
var phase = Environment.GetEnvironmentVariable("BENCH_PHASE") ?? "A";
var mode = Environment.GetEnvironmentVariable("BENCH_MODE") ?? "off";
var count = int.TryParse(Environment.GetEnvironmentVariable("BENCH_COUNT"),out var n)?n:128;
var concurrency = int.TryParse(Environment.GetEnvironmentVariable("BENCH_CONCURRENCY"),out var c)?c:8;
var target = Environment.GetEnvironmentVariable("BENCH_REPORT") ?? "results/native-commit-result.json";
var container = Environment.GetEnvironmentVariable("BENCH_PG_CONTAINER");
if(phase is not ("A" or "B" or "C" or "D") || mode is not ("on" or "off") ||
   count is <16 or >2000 || concurrency is <1 or >64)
    throw new ArgumentException("Invalid benchmark inputs");
var prefix="native-opt-"+phase.ToLowerInvariant()+"-"+Guid.NewGuid().ToString("N")+"-";
const int SubscriptionCount=12, InboxCount=8, ActivityCount=16, HistoryCount=64;
var historyPayload=JsonSerializer.SerializeToElement(new { sample=new string('h',1024) });
var bigSnapshot=JsonSerializer.SerializeToElement(new { snapshot=new string('s',96*1024) });
var smallSnapshot=JsonSerializer.SerializeToElement(new { tick=1 });
var empty=JsonSerializer.SerializeToElement(new {});
var fixtures=await Seed();
Console.WriteLine("Prepared "+fixtures.Count+" independent native workflow commits for "+phase+"/"+mode);
await Task.Delay(1400);
var before=await PgStats();
var cpuBefore=await ContainerStats();
var durations=new ConcurrentBag<double>();
var errors=new ConcurrentQueue<string>();
var success=0;
var watch=Stopwatch.StartNew();
await using(var tasks=new HubTaskStore(connection,Math.Max(20,concurrency+4)))
{
    var native=new HubNativeWorkflowStore(tasks,new HubDefinitionStore(tasks));
    await Parallel.ForEachAsync(fixtures,new ParallelOptions{MaxDegreeOfParallelism=concurrency},
        async (item,ct)=>
        {
            try {
                HubNativeSubscription[] subscriptions=phase=="B"
                    ?Enumerable.Range(0,SubscriptionCount).Select(i=>
                        new HubNativeSubscription("new-"+i,"unused-event",item.Hub,DateTime.UnixEpoch)).ToArray()
                    :[];
                HubNativeDispatch[] activities=phase=="C"
                    ?Enumerable.Range(0,ActivityCount).Select(i=>
                        new HubNativeDispatch("dispatch-"+i,"pointer-"+i,i,"bench-activity",1,empty)).ToArray()
                    :[];
                HubNativeHistory[] history=phase=="D"
                    ?Enumerable.Range(0,HistoryCount).Select(i=>
                        new HubNativeHistory("StepCompleted","history-"+i,0,null,[],1,historyPayload)).ToArray()
                    :[];
                var request=new HubNativeCommit(1,"bench-worker",1,0,item.Task,
                    phase=="D"?bigSnapshot:smallSnapshot,"Runnable",false,null,item.Inbox,
                    subscriptions,activities,history);
                var started=Stopwatch.GetTimestamp();
                var result=await native.CommitAsync(item.Workflow,request,ct);
                durations.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                if(result is null || result.Replayed || result.Revision!=1)
                    throw new InvalidOperationException("Commit rejected valid native workflow");
                Interlocked.Increment(ref success);
            }catch(Exception ex){errors.Enqueue(ex.ToString());}
        });
}
watch.Stop();
var cpuAfter=await ContainerStats();
await Task.Delay(1800);
var after=await PgStats();
var verified=await Verify();
if(errors.TryPeek(out var first))throw new InvalidOperationException("Native commit error: "+first);
if(success!=count||verified.Completed!=count||verified.Scheduled!=(phase=="C"?count*ActivityCount:0)
    ||verified.ScheduledEvents!=(phase=="C"?count*ActivityCount:0)
    ||verified.History!=(phase=="D"?count*HistoryCount:0)
    ||verified.Consumed!=(phase=="B"?count*InboxCount:0)
    ||verified.Subscriptions!=(phase=="B"?count*SubscriptionCount:0))
    throw new InvalidOperationException("Durability mismatch: "+JsonSerializer.Serialize(verified));
var ordered=durations.Order().ToArray();
var cpuUsec=cpuBefore.HasValue && cpuAfter.HasValue?cpuAfter.Value.CpuUsec-cpuBefore.Value.CpuUsec:(long?)null;
var walBytes=after.WalBytes-before.WalBytes;
var report=new {
    phase,mode,count,concurrency,
    completed=success,
    throughputPerSecond=Math.Round(success/watch.Elapsed.TotalSeconds,3),
    commitP50Ms=Percentile(ordered,.50),
    commitP95Ms=Percentile(ordered,.95),
    commitP99Ms=Percentile(ordered,.99),
    pgCpuMsPerWorkflow=cpuUsec.HasValue?Math.Round(cpuUsec.Value/1000d/count,3):(double?)null,
    walBytesPerWorkflow=Math.Round(walBytes/count,3),
    pgObservedCommits=after.Commits-before.Commits,
    pgBlockHits=after.Hits-before.Hits,
    pgBlockReads=after.Reads-before.Reads,
    pgIoReads=after.IoReads-before.IoReads,
    pgIoWrites=after.IoWrites-before.IoWrites,
    pgIoFsync=after.IoFsync-before.IoFsync,
    pgDiskReadBytes=cpuBefore.HasValue&&cpuAfter.HasValue?cpuAfter.Value.ReadBytes-cpuBefore.Value.ReadBytes:(long?)null,
    pgDiskWriteBytes=cpuBefore.HasValue&&cpuAfter.HasValue?cpuAfter.Value.WriteBytes-cpuBefore.Value.WriteBytes:(long?)null,
    verified=new {
        completed=verified.Completed,
        scheduledTasks=verified.Scheduled,
        scheduledHistoryEvents=verified.ScheduledEvents,
        history=verified.History,
        consumedInbox=verified.Consumed,
        subscriptions=verified.Subscriptions
    }
};
Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(target))!);
await File.WriteAllTextAsync(target,JsonSerializer.Serialize(report,new JsonSerializerOptions{WriteIndented=true}));
Console.WriteLine("RESULT "+JsonSerializer.Serialize(report));

static double Percentile(double[] sorted,double p) =>
    Math.Round(sorted[(int)Math.Ceiling(sorted.Length*p)-1],3);

async Task<List<(Guid Workflow,Guid Task,string Hub,long[] Inbox)>> Seed()
{
    await using var db=new NpgsqlConnection(unpooled);
    await db.OpenAsync();
    await using(var cmd=new NpgsqlCommand("""
        WITH workflows AS (
          INSERT INTO wfc_scale.hub_workflow
             (id,hub_id,definition_id,definition_version,partition_id,input_json,
              step_count,state,execution_mode,native_status)
          SELECT gen_random_uuid(),@prefix||i::text,'benchmark',1,0,'{}'::jsonb,
             1,1,'native','Runnable'
          FROM generate_series(1,@count) i RETURNING id,hub_id
        ), leased AS (
          INSERT INTO wfc_scale.hub_task
             (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,
              state,owner,last_owner,epoch,lease_until,attempts,admission)
          SELECT gen_random_uuid(),w.hub_id,0,'__native_workflow__',1,w.id,'{}'::jsonb,
             1,'bench-worker','bench-worker',1,now()+interval '30 minutes',1,1
          FROM workflows w RETURNING id,workflow_id
        )
        INSERT INTO wfc_scale.hub_task_attempt
          (task_id,epoch,workflow_id,step_index,attempt_number,worker_id)
        SELECT id,1,workflow_id,0,1,'bench-worker' FROM leased
        """,db))
    {
        cmd.Parameters.AddWithValue("prefix",prefix);
        cmd.Parameters.AddWithValue("count",count);
        await cmd.ExecuteNonQueryAsync();
    }
    if(phase=="B")
    {
        await using var subs=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_native_subscription
              (workflow_id,pointer_id,event_name,event_key,as_of)
            SELECT w.id,'old-'||i::text,'obsolete-event',w.hub_id,now()-interval '1 hour'
            FROM wfc_scale.hub_workflow w CROSS JOIN generate_series(1,@subs) i
            WHERE w.hub_id LIKE @pattern
            """,db);
        subs.Parameters.AddWithValue("pattern",prefix+"%");
        subs.Parameters.AddWithValue("subs",SubscriptionCount);
        await subs.ExecuteNonQueryAsync();
        await using var inbox=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_native_inbox
              (workflow_id,pointer_id,kind,source_key,payload)
            SELECT w.id,'inbox-'||i::text,'Event','event-'||i::text,'{}'::jsonb
            FROM wfc_scale.hub_workflow w CROSS JOIN generate_series(1,@inbox) i
            WHERE w.hub_id LIKE @pattern
            """,db);
        inbox.Parameters.AddWithValue("pattern",prefix+"%");
        inbox.Parameters.AddWithValue("inbox",InboxCount);
        await inbox.ExecuteNonQueryAsync();
    }
    var fixtures=new List<(Guid Workflow,Guid Task,string Hub,long[] Inbox)>();
    await using(var cmd=new NpgsqlCommand("""
        SELECT w.id,t.id,w.hub_id
        FROM wfc_scale.hub_workflow w
        JOIN wfc_scale.hub_task t ON t.workflow_id=w.id AND t.task_type='__native_workflow__'
        WHERE w.hub_id LIKE @pattern ORDER BY w.id
        """,db))
    {
        cmd.Parameters.AddWithValue("pattern",prefix+"%");
        await using var reader=await cmd.ExecuteReaderAsync();
        while(await reader.ReadAsync())fixtures.Add((reader.GetGuid(0),reader.GetGuid(1),reader.GetString(2),[]));
    }
    if(phase=="B")
    {
        var inboxIds=new Dictionary<Guid,List<long>>();
        await using var cmd=new NpgsqlCommand("""
            SELECT workflow_id,id FROM wfc_scale.hub_native_inbox
            WHERE workflow_id=ANY(@workflows) ORDER BY id
            """,db);
        cmd.Parameters.AddWithValue("workflows",fixtures.Select(x=>x.Workflow).ToArray());
        await using var reader=await cmd.ExecuteReaderAsync();
        while(await reader.ReadAsync())
        {
            var id=reader.GetGuid(0);
            if(!inboxIds.TryGetValue(id,out var list))inboxIds[id]=list=new();
            list.Add(reader.GetInt64(1));
        }
        fixtures=fixtures.Select(x=>(x.Workflow,x.Task,x.Hub,inboxIds[x.Workflow].ToArray())).ToList();
    }
    if(phase=="C")
    {
        await using var registration=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_activity_registration
              (hub_id,activity_type,version,partitions,hub_concurrency,activity_concurrency)
            SELECT hub_id,'bench-activity',1,1,1,1 FROM wfc_scale.hub_workflow
            WHERE hub_id LIKE @pattern
            """,db);
        registration.Parameters.AddWithValue("pattern",prefix+"%");
        await registration.ExecuteNonQueryAsync();
    }
    if(fixtures.Count!=count)throw new InvalidOperationException("Fixture count mismatch");
    return fixtures;
}
async Task<(long Completed,long Scheduled,long ScheduledEvents,long History,long Consumed,long Subscriptions)> Verify()
{
    await using var db=new NpgsqlConnection(unpooled);
    await db.OpenAsync();
    await using var cmd=new NpgsqlCommand("""
       SELECT
        (SELECT count(*) FROM wfc_scale.hub_task
           WHERE hub_id LIKE @pattern AND task_type='__native_workflow__' AND state=2),
        (SELECT count(*) FROM wfc_scale.hub_task
           WHERE hub_id LIKE @pattern AND task_type='bench-activity' AND state=0),
        (SELECT count(*) FROM wfc_scale.hub_workflow_event e
           JOIN wfc_scale.hub_workflow w ON w.id=e.workflow_id
           WHERE w.hub_id LIKE @pattern AND e.event_type='ActivityScheduled'),
        (SELECT count(*) FROM wfc_scale.hub_workflow_event e
           JOIN wfc_scale.hub_workflow w ON w.id=e.workflow_id
           WHERE w.hub_id LIKE @pattern AND e.event_type='StepCompleted'),
        (SELECT count(*) FROM wfc_scale.hub_native_inbox i
           JOIN wfc_scale.hub_workflow w ON w.id=i.workflow_id
           WHERE w.hub_id LIKE @pattern AND i.consumed),
        (SELECT count(*) FROM wfc_scale.hub_native_subscription s
           JOIN wfc_scale.hub_workflow w ON w.id=s.workflow_id
           WHERE w.hub_id LIKE @pattern)
       """,db);
    cmd.Parameters.AddWithValue("pattern",prefix+"%");
    await using var r=await cmd.ExecuteReaderAsync();
    if(!await r.ReadAsync())throw new InvalidOperationException("Missing counts");
    return(r.GetInt64(0),r.GetInt64(1),r.GetInt64(2),r.GetInt64(3),
        r.GetInt64(4),r.GetInt64(5));
}
async Task<(double WalBytes,double Commits,double Hits,double Reads,double IoReads,double IoWrites,double IoFsync)> PgStats()
{
    await using var db=new NpgsqlConnection(unpooled);
    await db.OpenAsync();
    await using(var clear=new NpgsqlCommand("SELECT pg_stat_clear_snapshot()",db))
        await clear.ExecuteNonQueryAsync();
    await using var cmd=new NpgsqlCommand("""
        SELECT
         (SELECT wal_bytes::numeric FROM pg_stat_wal),
         d.xact_commit,d.blks_hit,d.blks_read,
         (SELECT COALESCE(sum(reads),0) FROM pg_stat_io),
         (SELECT COALESCE(sum(writes),0) FROM pg_stat_io),
         (SELECT COALESCE(sum(fsyncs),0) FROM pg_stat_io)
        FROM pg_stat_database d WHERE d.datname=current_database()
        """,db);
    await using var r=await cmd.ExecuteReaderAsync();
    if(!await r.ReadAsync())throw new InvalidOperationException("No PostgreSQL stats");
    double Val(int i)=>Convert.ToDouble(r.GetValue(i),CultureInfo.InvariantCulture);
    return(Val(0),Val(1),Val(2),Val(3),Val(4),Val(5),Val(6));
}
async Task<(long CpuUsec,long ReadBytes,long WriteBytes)?> ContainerStats()
{
    if(string.IsNullOrEmpty(container))return null;
    async Task<string> DockerCat(string file)
    {
        using var proc=new Process();
        proc.StartInfo=new ProcessStartInfo("docker") {
           RedirectStandardOutput=true,RedirectStandardError=true,UseShellExecute=false
        };
        foreach(var arg in new[]{"exec",container,"cat",file})proc.StartInfo.ArgumentList.Add(arg);
        if(!proc.Start())throw new InvalidOperationException("docker exec failed");
        var stdout=proc.StandardOutput.ReadToEndAsync();
        var stderr=proc.StandardError.ReadToEndAsync();
        await proc.WaitForExitAsync();
        if(proc.ExitCode!=0)throw new InvalidOperationException(await stderr);
        return await stdout;
    }
    var cpu=await DockerCat("/sys/fs/cgroup/cpu.stat");
    var io=await DockerCat("/sys/fs/cgroup/io.stat");
    var usage=long.Parse(cpu.Split('\n',StringSplitOptions.RemoveEmptyEntries)
        .Single(x=>x.StartsWith("usage_usec ")).Split(' ')[1],CultureInfo.InvariantCulture);
    long read=0,write=0;
    foreach(var row in io.Split('\n',StringSplitOptions.RemoveEmptyEntries))
        foreach(var field in row.Split(' ',StringSplitOptions.RemoveEmptyEntries))
        {
            if(field.StartsWith("rbytes="))read+=long.Parse(field["rbytes=".Length..],CultureInfo.InvariantCulture);
            if(field.StartsWith("wbytes="))write+=long.Parse(field["wbytes=".Length..],CultureInfo.InvariantCulture);
        }
    return(usage,read,write);
}
