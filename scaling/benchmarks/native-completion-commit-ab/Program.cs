using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;

// This is a PostgreSQL-native task-completion benchmark, not an end-to-end
// workflow throughput benchmark. Each fixture has one independent native
// workflow, one fenced activity lease, and its attempt row.
var connection = Environment.GetEnvironmentVariable("WFC_TEST_PG")
    ?? throw new InvalidOperationException("Set WFC_TEST_PG");
var count = int.TryParse(Environment.GetEnvironmentVariable("BENCH_COUNT"),out var c) ? c : 600;
var concurrency = int.TryParse(Environment.GetEnvironmentVariable("BENCH_CONCURRENCY"),out var p) ? p : 8;
var batchSize = int.TryParse(Environment.GetEnvironmentVariable("BENCH_BATCH_SIZE"),out var b) ? b : 16;
var reportPath = Environment.GetEnvironmentVariable("BENCH_REPORT") ?? "native-completion-commit-ab.json";
if(count is < 32 or > 10000 || concurrency is < 1 or > 64 || batchSize is < 2 or > 32)
    throw new ArgumentException("Invalid count, concurrency or batch size");
var pgContainer = Environment.GetEnvironmentVariable("BENCH_POSTGRES_CONTAINER");
var completion = JsonSerializer.SerializeToElement(new { result = "completed", version = 1 });
var rounds = new[] { "single", "batch", "batch", "single" };
var results = new List<object>();
await using var tasks = new HubTaskStore(connection,Math.Max(20,concurrency+4));
var store = new HubNativeWorkflowStore(tasks,new HubDefinitionStore(tasks));

// Warm both paths and Npgsql pooling before collecting PostgreSQL statistics.
await Run("single",16,0, false);
await Run("batch",16,0, false);
foreach(var mode in rounds)
    await Run(mode,count,batchSize,true);
var output = new {
    benchmark = "native-task-completion-commit-ab",
    version = 1,
    countPerArm = count,
    concurrency,
    batchSize,
    workload = "independent native workflows; one leased activity and attempt each",
    sequence = rounds,
    results,
    caveat = "Microbenchmark of task commit persistence; no network, worker execution, workflow scheduling or discovery delay."
};
Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(reportPath))!);
await File.WriteAllTextAsync(reportPath,JsonSerializer.Serialize(output,new JsonSerializerOptions { WriteIndented = true }));
Console.WriteLine(JsonSerializer.Serialize(output,new JsonSerializerOptions { WriteIndented = true }));

async Task Run(string mode,int itemCount,int size,bool measured)
{
    var hub = "pg-commit-ab-" + Guid.NewGuid().ToString("N");
    await Seed(hub,itemCount);
    var taskIds = await GetTasks(hub);
    if(taskIds.Length!=itemCount)throw new InvalidOperationException($"Expected {itemCount} seeded tasks, got {taskIds.Length}");
    await Task.Delay(1100); // stats collector flush before the measured window
    var before=await PgSnapshot();
    var cpuBefore=await ContainerSnapshot(pgContainer);
    var errors = new ConcurrentQueue<string>();
    var durations = new ConcurrentBag<double>();
    var successful = 0;
    var stopwatch = Stopwatch.StartNew();
    var groups=taskIds.Chunk(mode=="single"?1:Math.Max(2,size)).ToArray();
    await Parallel.ForEachAsync(groups,new ParallelOptions {MaxDegreeOfParallelism=concurrency},
        async (chunk,ct) =>
        {
            var started=Stopwatch.GetTimestamp();
            try
            {
                if(mode=="single")
                {
                    if(!await store.CompleteActivityAsync(chunk[0],"bench-worker",1,completion,ct))
                        throw new InvalidOperationException("Single completion rejected a valid lease");
                }
                else
                {
                    var entries=chunk.Select(id=>new HubNativeCompletion(id,"bench-worker",1,completion)).ToArray();
                    var accepted=await store.CompleteActivitiesBatchAsync(entries,ct);
                    if(accepted.Count!=chunk.Length||accepted.Any(ok=>!ok))
                        throw new InvalidOperationException("Batch completion rejected a valid lease");
                }
                Interlocked.Add(ref successful,chunk.Length);
                var ms=Stopwatch.GetElapsedTime(started).TotalMilliseconds;
                for(var i=0;i<chunk.Length;i++)durations.Add(ms);
            }
            catch(Exception ex){errors.Enqueue(ex.ToString());}
        });
    stopwatch.Stop();
    var cpuAfter=await ContainerSnapshot(pgContainer);
    await Task.Delay(1150);
    var after=await PgSnapshot();
    var verified=await Verify(hub);
    if(errors.TryPeek(out var first))throw new InvalidOperationException(mode+" failed: "+first);
    if(successful!=itemCount || verified.Completed!=itemCount ||
       verified.Attempts!=itemCount || verified.Inbox!=itemCount ||
       verified.History!=itemCount || verified.Wakes!=itemCount)
        throw new InvalidOperationException($"Durability mismatch for {mode}: {JsonSerializer.Serialize(verified)} expected {itemCount}");
    if(!measured)return;

    var ordered=durations.Order().ToArray();
    var dbDelta=MetricNames.Select((key,i)=>(key,value:Math.Round(after[i]-before[i],3)))
        .ToDictionary(x=>x.key,x=>x.value);
    var pgCpuSeconds=cpuBefore.HasValue && cpuAfter.HasValue
        ? Math.Round((cpuAfter.Value.CpuUsec-cpuBefore.Value.CpuUsec)/1_000_000d,4) : (double?)null;
    var pgDiskReadBytes=cpuBefore.HasValue && cpuAfter.HasValue
        ? cpuAfter.Value.ReadBytes-cpuBefore.Value.ReadBytes : (long?)null;
    var pgDiskWriteBytes=cpuBefore.HasValue && cpuAfter.HasValue
        ? cpuAfter.Value.WriteBytes-cpuBefore.Value.WriteBytes : (long?)null;
    if(Environment.GetEnvironmentVariable("BENCH_REQUIRE_CPU")=="1" && pgCpuSeconds is null)
        throw new InvalidOperationException("PostgreSQL cgroup CPU stats unavailable");
    var sample=new {
        mode,
        count=itemCount,
        completed=successful,
        durationSeconds=Math.Round(stopwatch.Elapsed.TotalSeconds,4),
        tasksPerSecond=Math.Round(itemCount/stopwatch.Elapsed.TotalSeconds,3),
        requestLatencyP50Ms=Percentile(ordered,.50),
        requestLatencyP95Ms=Percentile(ordered,.95),
        requestLatencyP99Ms=Percentile(ordered,.99),
        transactionsPerTask=Math.Round(dbDelta["xact_commit"]/itemCount,4),
        walBytesPerTask=Math.Round(dbDelta["wal_bytes"]/itemCount,2),
        pgCpuSeconds,
        pgCpuMsPerTask=pgCpuSeconds.HasValue?Math.Round(pgCpuSeconds.Value*1000/itemCount,3):(double?)null,
        pgDiskReadBytes,
        pgDiskWriteBytes,
        pgStatsDelta=dbDelta,
        verified
    };
    results.Add(sample);
    Console.WriteLine($"SAMPLE mode={mode} throughput={sample.tasksPerSecond:F3}/s p95={sample.requestLatencyP95Ms:F2}ms commits/task={sample.transactionsPerTask:F2} wal/task={sample.walBytesPerTask:F0}B pgCpu/task={sample.pgCpuMsPerTask}ms");
}
static double Percentile(double[] values,double p) => values.Length==0?0:Math.Round(values[(int)Math.Ceiling(values.Length*p)-1],3);

async Task Seed(string hub,int n)
{
    await using var db=new NpgsqlConnection(connection);
    await db.OpenAsync();
    await using var cmd=new NpgsqlCommand("""
        WITH created_workflows AS (
          INSERT INTO wfc_scale.hub_workflow
            (id,hub_id,definition_id,definition_version,partition_id,input_json,step_count,
             state,execution_mode,native_status)
          SELECT gen_random_uuid(),@hub,'bench-native',1,g.i%4,'{}'::jsonb,1,1,'native','Runnable'
          FROM generate_series(1,@count) AS g(i)
          RETURNING id,partition_id
        ), created_tasks AS (
          INSERT INTO wfc_scale.hub_task
            (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,
             state,owner,last_owner,epoch,lease_until,attempts,native_step_id,
             dispatch_key,execution_pointer_id,admission)
          SELECT gen_random_uuid(),@hub,w.partition_id,'bench-activity',1,w.id,'{}'::jsonb,
                 1,'bench-worker','bench-worker',1,now()+interval '30 minutes',1,0,
                 w.id::text,'pointer-'||w.id::text,1
          FROM created_workflows w
          RETURNING id,workflow_id
        )
        INSERT INTO wfc_scale.hub_task_attempt
          (task_id,epoch,workflow_id,step_index,attempt_number,worker_id)
        SELECT id,1,workflow_id,0,1,'bench-worker' FROM created_tasks
        """,db);
    cmd.Parameters.AddWithValue("hub",hub);
    cmd.Parameters.AddWithValue("count",n);
    await cmd.ExecuteNonQueryAsync();
}
async Task<Guid[]> GetTasks(string hub)
{
    await using var db=new NpgsqlConnection(connection);
    await db.OpenAsync();
    await using var cmd=new NpgsqlCommand("""
        SELECT id FROM wfc_scale.hub_task
        WHERE hub_id=@hub AND task_type='bench-activity' AND state=1 ORDER BY id
        """,db);
    cmd.Parameters.AddWithValue("hub",hub);
    var ids=new List<Guid>();
    await using var r=await cmd.ExecuteReaderAsync();
    while(await r.ReadAsync())ids.Add(r.GetGuid(0));
    return ids.ToArray();
}
async Task<(long Completed,long Attempts,long Inbox,long History,long Wakes)> Verify(string hub)
{
    await using var db=new NpgsqlConnection(connection);
    await db.OpenAsync();
    await using var cmd=new NpgsqlCommand("""
        SELECT
          (SELECT count(*) FROM wfc_scale.hub_task WHERE hub_id=@hub AND task_type='bench-activity' AND state=2),
          (SELECT count(*) FROM wfc_scale.hub_task_attempt a JOIN wfc_scale.hub_task t ON t.id=a.task_id
           WHERE t.hub_id=@hub AND t.task_type='bench-activity' AND a.outcome='Completed' AND a.finished_at IS NOT NULL),
          (SELECT count(*) FROM wfc_scale.hub_native_inbox i JOIN wfc_scale.hub_workflow w ON w.id=i.workflow_id
           WHERE w.hub_id=@hub AND i.kind='Activity'),
          (SELECT count(*) FROM wfc_scale.hub_workflow_event e JOIN wfc_scale.hub_workflow w ON w.id=e.workflow_id
           WHERE w.hub_id=@hub AND e.event_type='ActivityResultReceived'),
          (SELECT count(*) FROM wfc_scale.hub_task WHERE hub_id=@hub AND task_type='__native_workflow__' AND state=0)
        """,db);
    cmd.Parameters.AddWithValue("hub",hub);
    await using var r=await cmd.ExecuteReaderAsync();
    if(!await r.ReadAsync())throw new InvalidOperationException("Missing verification row");
    return(r.GetInt64(0),r.GetInt64(1),r.GetInt64(2),r.GetInt64(3),r.GetInt64(4));
}
var __unused=0;
