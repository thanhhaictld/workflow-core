using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

var container = Environment.GetEnvironmentVariable("WFC_BATCH_PG") ?? "wfc-pg17-batch-luna-20261115";
var port = Environment.GetEnvironmentVariable("WFC_BATCH_PORT") ?? "5432";
var database = Environment.GetEnvironmentVariable("WFC_BATCH_DB") ?? "hub_batch";
var connectionString = $"Host=127.0.0.1;Port={port};Database={database};Username=postgres;Timeout=10;Command Timeout=60;Max Auto Prepare=0";
var sqlPath = args.Length > 0 ? args[0] : "baseline.sql";
var statsMode = args.Length > 1 ? args[1] : "stale";
var outputDir = args.Length > 2 ? args[2] : "raw";
Directory.CreateDirectory(outputDir);
var sql = File.ReadAllText(sqlPath).Trim();
var pairSqlPath = args.Length > 3 ? args[3] : null;
var variants = pairSqlPath is null
    ? new[] { (Arm: "baseline", Sql: sql) }
    : new[] { (Arm: "baseline", Sql: sql), (Arm: "prototype", Sql: File.ReadAllText(pairSqlPath).Trim()) };
var explainVariants = variants.Select(v => (v.Arm, Sql: "EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT JSON,TIMING OFF) " + v.Sql)).ToArray();
var capJson = JsonSerializer.Serialize(Enumerable.Range(0, 8).SelectMany(t => Enumerable.Range(1, 3)
    .Select(v => new { hub = "hub-0", task_type = $"activity-{t}", version = v })));
var cellRows = new List<Row>();
var settings = new NpgsqlConnectionStringBuilder(connectionString);
Console.WriteLine($"container={container} database={database} npgsql={typeof(NpgsqlConnection).Assembly.GetName().Version} max_auto_prepare={settings.MaxAutoPrepare} prepare_min_usages={settings.AutoPrepareMinUsages}");

await using var dataSource = NpgsqlDataSource.Create(connectionString);
if (statsMode == "correctness")
{
    if (pairSqlPath is null) throw new InvalidOperationException("Correctness usage: BatchExperiment <baseline.sql> correctness <output-dir> <prototype.sql>");
    var result = await RunEquivalence(dataSource, sqlPath, pairSqlPath);
    Console.WriteLine(JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));
    await File.WriteAllTextAsync(Path.Combine(outputDir, "correctness-equivalence.json"), JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));
    return;
}
if (statsMode == "concurrency")
{
    var result = await RunConcurrency(dataSource, sqlPath);
    Console.WriteLine(JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));
    await File.WriteAllTextAsync(Path.Combine(outputDir, "correctness-concurrency.json"), JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));
    return;
}
await using (var admin = await dataSource.OpenConnectionAsync())
{
    await using var check = new NpgsqlCommand("SELECT version(), current_setting('plan_cache_mode'), (SELECT count(*) FROM wfc_scale.hub_task), (SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname='wfc_scale' AND relname='hub_task')", admin);
    await using var reader = await check.ExecuteReaderAsync();
    await reader.ReadAsync();
    Console.WriteLine($"postgres={reader.GetString(0)} plan_cache_mode={reader.GetString(1)} rows={reader.GetInt64(2)} estimated_rows={reader.GetInt64(3)} stats_mode={statsMode}");
}

var modes = new[] { "custom", "generic", "npgsql-unprepared" };
foreach (var batchSize in new[] { 1, 8, 16, 32 })
foreach (var position in new[] { "early", "late" })
foreach (var planMode in modes)
{
    await using var connection = await dataSource.OpenConnectionAsync();
    await using (var setPlan = new NpgsqlCommand($"SET plan_cache_mode = '{(planMode == "generic" ? "force_generic_plan" : "force_custom_plan")}'", connection))
        await setPlan.ExecuteNonQueryAsync();
    var ids = await ReadIds(connection, batchSize, position);
    if (ids.Count != batchSize) throw new InvalidOperationException($"Expected {batchSize} eligible ids, got {ids.Count}");
    var selected = JsonSerializer.Serialize(ids.Select((id, i) => new {
        id,
        epoch = 0,
        worker = $"worker-{i % 4}",
        capabilities = JsonDocument.Parse(capJson).RootElement,
        pins = (object?)null
    }));

    var commands = explainVariants.Select(v => (v.Arm, Command: NewClaimCommand(v.Sql, connection, selected))).ToArray();
    var explicitPrepare = planMode is "custom" or "generic";
    if (explicitPrepare) foreach (var query in commands) await query.Command.PrepareAsync();

    for (var run = 0; run < 7; run++)
    {
        var pairedReturnRows = new Dictionary<string, List<(string Id, string Owner, string Canonical)>>();
        var orderedCommands = run % 2 == 0 ? commands : commands.Reverse().ToArray();
        foreach (var query in orderedCommands)
        {
            await using var transaction = await connection.BeginTransactionAsync();
            query.Command.Transaction = transaction;
            var sw = Stopwatch.StartNew();
            var raw = (string?)await query.Command.ExecuteScalarAsync() ?? throw new InvalidOperationException("EXPLAIN returned NULL");
            sw.Stop();
            await transaction.RollbackAsync();
            await using var returnedTransaction = await connection.BeginTransactionAsync();
            var returnedRows = await ExecuteClaim(connection, returnedTransaction,
                variants.Single(v => v.Arm == query.Arm).Sql, selected, "hub-0");
            await returnedTransaction.RollbackAsync();
            if (returnedRows.Count != batchSize)
                throw new InvalidOperationException($"{query.Arm} returned {returnedRows.Count} rows for batch size {batchSize}");
            pairedReturnRows[query.Arm] = returnedRows;
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement[0];
            var plan = root.GetProperty("Plan");
            var update = FindUpdateNode(plan);
            var taskScans = FindTaskScans(plan).Where(n => Get(n, "Node Type")?.Contains("Scan", StringComparison.Ordinal) == true).ToArray();
            var measured = run >= 2;
            var armFolder = pairSqlPath is null ? outputDir : Path.Combine(outputDir, query.Arm);
            Directory.CreateDirectory(armFolder);
            var name = $"{statsMode}_{planMode}_{batchSize}_{position}_{run + 1:00}.json";
            File.WriteAllText(Path.Combine(armFolder, name), JsonSerializer.Serialize(new {
                arm = query.Arm,
                cell = new { statsMode, planMode, batchSize, position },
                sample = run + 1,
                warmup = !measured,
                client_elapsed_ms = sw.Elapsed.TotalMilliseconds,
                returned_rows = returnedRows.Count,
                returned_ids = returnedRows.Select(r => r.Id).Order().ToArray(),
                returned_owners = returnedRows.ToDictionary(r => r.Id, r => r.Owner),
                returned_hash = ReturnHash(returnedRows),
                plan = root
            }, new JsonSerializerOptions { WriteIndented = true }));
            if (measured)
            {
                cellRows.Add(new Row(query.Arm, statsMode, planMode, batchSize, position, run - 1,
                    root.GetProperty("Planning Time").GetDouble(), root.GetProperty("Execution Time").GetDouble(),
                    sw.Elapsed.TotalMilliseconds, update?.TryGetProperty("Actual Rows", out var rows) == true ? rows.GetDouble() : 0,
                    string.Join("|", taskScans.Select(s => $"{Get(s,"Node Type")}:plan={Get(s,"Plan Rows")}:actual={Get(s,"Actual Rows")}:removed={Get(s,"Rows Removed by Filter")}:hit={Get(s,"Shared Hit Blocks")}:read={Get(s,"Shared Read Blocks")}:loops={Get(s,"Actual Loops")}"))));
            }
        }
        if (commands.Length == 2 && !ReturnHash(pairedReturnRows["baseline"]).Equals(ReturnHash(pairedReturnRows["prototype"]), StringComparison.Ordinal))
            throw new InvalidOperationException($"Baseline/prototype exact RETURNING rows differ in {statsMode}/{planMode}/{batchSize}/{position}/sample-{run + 1}");
    }
    foreach (var query in commands) await query.Command.DisposeAsync();
    await using var prepStats = new NpgsqlCommand("SELECT name, statement FROM pg_prepared_statements", connection);
    await using var prepReader = await prepStats.ExecuteReaderAsync();
    var preparedNames = new List<string>();
    while (await prepReader.ReadAsync()) preparedNames.Add(prepReader.GetString(0));
    Console.WriteLine($"cell stats={statsMode} batch={batchSize} ids={position} plan={planMode} prepared={explicitPrepare} prepared_entries={preparedNames.Count}");
}

var csv = Path.Combine(outputDir, $"summary_{statsMode}.csv");
await File.WriteAllTextAsync(csv, "arm,stats,plan,batch,position,sample,planning_ms,execution_ms,client_elapsed_ms,leased_rows,task_scan_nodes\n" +
    string.Join("\n", cellRows.Select(r => string.Join(',', r.Arm, r.Stats, r.Mode, r.Batch, r.Position, r.Sample,
        F(r.PlanningMs), F(r.ExecutionMs), F(r.ClientMs), F(r.LeasedRows), Quote(r.ScanNodes)))) + "\n");
Console.WriteLine($"Wrote {cellRows.Count} measured samples to {csv}");

static async Task<List<Guid>> ReadIds(NpgsqlConnection connection, int count, string position)
{
    var direction = position == "early" ? "ASC" : "DESC";
    var sql = $"SELECT t.id FROM wfc_scale.hub_task t WHERE t.hub_id='hub-0' AND t.partition_id % 4=0 AND t.state=0 AND t.due_at<=now() AND t.attempts<t.max_attempts AND NOT EXISTS (SELECT 1 FROM wfc_scale.hub_task_constraint c WHERE c.task_id=t.id) AND t.payload->>'class' IN ('active-backlog','active-backlog-extra') ORDER BY t.sequence {direction} LIMIT @limit";
    await using var command = new NpgsqlCommand(sql, connection);
    command.Parameters.AddWithValue("limit", count);
    var ids = new List<Guid>();
    await using var reader = await command.ExecuteReaderAsync();
    while (await reader.ReadAsync()) ids.Add(reader.GetGuid(0));
    return ids;
}

static NpgsqlCommand NewClaimCommand(string sql, NpgsqlConnection connection, string selected)
{
    var command = new NpgsqlCommand(sql, connection);
    command.Parameters.Add("selected", NpgsqlDbType.Jsonb).Value = selected;
    command.Parameters.Add("hub", NpgsqlDbType.Text).Value = "hub-0";
    command.Parameters.Add("count", NpgsqlDbType.Integer).Value = 4;
    command.Parameters.Add("partition", NpgsqlDbType.Integer).Value = 0;
    command.Parameters.Add("lease", NpgsqlDbType.Interval).Value = TimeSpan.FromSeconds(30);
    return command;
}

static async Task<object> RunEquivalence(NpgsqlDataSource source, string baselinePath, string prototypePath)
{
    var baselineFile = Path.GetFullPath(baselinePath);
    var prototypeFile = Path.GetFullPath(prototypePath);
    var baseline = File.ReadAllText(baselineFile).Trim();
    var prototype = File.ReadAllText(prototypeFile).Trim();
    var baselineHash = Sha256(baseline);
    var prototypeHash = Sha256(prototype);
    var baselineNormalized = NormalizeSql(baseline);
    var prototypeNormalized = NormalizeSql(prototype);
    if (baselineHash == prototypeHash || baselineNormalized == prototypeNormalized)
        throw new InvalidOperationException("Baseline and prototype SQL are identical after whitespace normalization; refusing a self-comparison");
    await using var db = await source.OpenConnectionAsync();
    await using var tx = await db.BeginTransactionAsync();
    var hub = "batch-equality-" + Guid.NewGuid().ToString("N");
    var activity = Enumerable.Range(0, 10).Select(_ => Guid.NewGuid()).ToArray();
    var workflows = Enumerable.Range(0, 3).Select(_ => Guid.NewGuid()).ToArray();
    await using (var workflow = new NpgsqlCommand("""
        INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count,execution_mode,native_status)
        VALUES (@id,@hub,@definition,@version,0,'{}',1,'native',@status)
        """, db, tx))
    {
        for (var i = 0; i < workflows.Length; i++)
        {
            workflow.Parameters.Clear();
            workflow.Parameters.AddWithValue("id", workflows[i]); workflow.Parameters.AddWithValue("hub", hub);
            workflow.Parameters.AddWithValue("definition", "definition"); workflow.Parameters.AddWithValue("version", 1);
            workflow.Parameters.AddWithValue("status", i == 1 ? "Completed" : "Runnable");
            await workflow.ExecuteNonQueryAsync();
        }
    }
    await using (var insert = new NpgsqlCommand("""
        INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload,state,due_at,owner,epoch,lease_until,attempts,max_attempts)
        VALUES
        (@a0,@hub,0,'activity-a',1,NULL,'{}',0,now()-interval '1 second',NULL,0,NULL,0,5),
        (@a1,@hub,0,'activity-a',1,NULL,'{}',1,now()-interval '1 second','old-worker',1,now()-interval '1 second',1,5),
        (@a2,@hub,0,'activity-a',1,NULL,'{}',1,now()-interval '1 second','live-worker',1,now()+interval '1 minute',1,5),
        (@a3,@hub,0,'activity-a',1,NULL,'{}',0,now()+interval '1 hour',NULL,0,NULL,0,5),
        (@a4,@hub,0,'activity-a',1,NULL,'{}',0,now()-interval '1 second',NULL,0,NULL,5,5),
        (@a5,@hub,0,'activity-a',1,NULL,'{}',0,now()-interval '1 second',NULL,0,NULL,0,5),
        (@a6,@hub,0,'activity-b',1,NULL,'{}',0,now()-interval '1 second',NULL,0,NULL,0,5),
        (@a7,@hub,0,'activity-a',1,NULL,'{}',0,now()-interval '1 second',NULL,0,NULL,0,5),
        (@a8,@hub,0,'activity-b',1,NULL,'{}',0,now()-interval '1 second',NULL,0,NULL,0,5),
        (@a9,@hub,0,'activity-b',1,NULL,'{}',0,now()-interval '1 second',NULL,0,NULL,0,5),
        (@n0,@hub,0,'__native_workflow__',1,@w0,'{}',0,now()-interval '1 second',NULL,0,NULL,0,5),
        (@n1,@hub,0,'__native_workflow__',1,@w1,'{}',0,now()-interval '1 second',NULL,0,NULL,0,5),
        (@n2,@hub,0,'__native_workflow__',1,@w2,'{}',0,now()-interval '1 second',NULL,0,NULL,0,5)
        """, db, tx))
    {
        for (var i = 0; i < activity.Length; i++) insert.Parameters.AddWithValue($"a{i}", activity[i]);
        insert.Parameters.AddWithValue("n0", Guid.NewGuid()); insert.Parameters.AddWithValue("n1", Guid.NewGuid()); insert.Parameters.AddWithValue("n2", Guid.NewGuid());
        insert.Parameters.AddWithValue("hub", hub); insert.Parameters.AddWithValue("w0", workflows[0]);
        insert.Parameters.AddWithValue("w1", workflows[1]); insert.Parameters.AddWithValue("w2", workflows[2]);
        await insert.ExecuteNonQueryAsync();
    }
    await using (var resource = new NpgsqlCommand("INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,false)", db, tx))
    {
        resource.Parameters.AddWithValue("key", hub + "-resource"); await resource.ExecuteNonQueryAsync();
    }
    await using (var constraint = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@id,@key)", db, tx))
    { constraint.Parameters.AddWithValue("key", hub + "-resource"); constraint.Parameters.AddWithValue("id", activity[5]); await constraint.ExecuteNonQueryAsync(); }
    var items = new List<object>();
    void Add(Guid id, string taskType, string worker, long epoch = 0, object? pins = null, string? capabilityType = null) => items.Add(new {
        id, epoch, worker,
        capabilities = new[] { new { hub, task_type = capabilityType ?? taskType, version = 1 } }, pins
    });
    Add(activity[0], "activity-a", "worker-ready");
    Add(activity[1], "activity-a", "worker-expired", 1);
    Add(activity[2], "activity-a", "worker-live", 1);
    Add(activity[3], "activity-a", "worker-future");
    Add(activity[4], "activity-a", "worker-exhausted");
    Add(activity[5], "activity-a", "worker-constrained");
    Add(activity[6], "activity-a", "worker-wrong-capability");
    Add(activity[7], "activity-a", "worker-wrong-epoch", 1);
    Add(activity[8], "activity-b", "worker-capability-b");
    Add(activity[9], "activity-b", "worker-capability-mismatch", capabilityType: "activity-a");
    var nativeTasks = new Guid[3];
    for (var i = 0; i < workflows.Length; i++) nativeTasks[i] = await ScalarGuid(db, tx, "SELECT id FROM wfc_scale.hub_task WHERE workflow_id=@w", workflows[i]);
    Add(nativeTasks[0], "__native_workflow__", "worker-native-runnable", pins: new[] { new { hub, definition = "definition", version = 1 } });
    Add(nativeTasks[1], "__native_workflow__", "worker-native-completed", pins: new[] { new { hub, definition = "definition", version = 1 } });
    Add(nativeTasks[2], "__native_workflow__", "worker-native-wrong-pin", pins: new[] { new { hub, definition = "wrong-definition", version = 1 } });
    var selected = JsonSerializer.Serialize(items);
    var itemIds = items.Select(i => JsonDocument.Parse(JsonSerializer.Serialize(i)).RootElement.GetProperty("id").GetGuid()).ToArray();
    var beforeRows = await ReadTaskStates(db, tx, itemIds);
    await using (var save = new NpgsqlCommand("SAVEPOINT baseline_result", db, tx)) await save.ExecuteNonQueryAsync();
    var baselineRows = await ExecuteClaim(db, tx, baseline, selected, hub);
    await using (var rollback = new NpgsqlCommand("ROLLBACK TO SAVEPOINT baseline_result", db, tx)) await rollback.ExecuteNonQueryAsync();
    var prototypeRows = await ExecuteClaim(db, tx, prototype, selected, hub);
    var expected = new[] { activity[0], activity[1], activity[8], nativeTasks[0] }.ToHashSet();
    var actual = prototypeRows.Select(r => Guid.Parse(r.Id)).ToHashSet();
    if (ReturnHash(baselineRows) != ReturnHash(prototypeRows))
        throw new InvalidOperationException("Baseline/prototype RETURNING mismatch");
    if (!actual.SetEquals(expected) || prototypeRows.Count != 4)
        throw new InvalidOperationException($"Eligibility mismatch; expected {string.Join(',', expected)}, baseline={string.Join(',', baselineRows.Select(r => r.Id))}, prototype={string.Join(',', prototypeRows.Select(r => r.Id))}");
    var expectedOwners = new Dictionary<string, string> {
        [activity[0].ToString()] = "worker-ready", [activity[1].ToString()] = "worker-expired",
        [activity[8].ToString()] = "worker-capability-b",
        [nativeTasks[0].ToString()] = "worker-native-runnable"
    };
    if (prototypeRows.Any(r => !expectedOwners.TryGetValue(r.Id, out var worker) || worker != r.Owner))
        throw new InvalidOperationException("Per-item worker ownership did not match selected JSON");
    var afterRows = await ReadTaskStates(db, tx, itemIds);
    var rejectedIds = itemIds.Where(id => !expected.Contains(id)).ToArray();
    foreach (var id in expected)
    {
        var before = beforeRows[id]; var after = afterRows[id];
        if (after.State != 1 || after.Epoch != before.Epoch + 1 || after.Attempts != before.Attempts + 1 || after.Owner != expectedOwners[id.ToString()])
            throw new InvalidOperationException($"Admitted task state mismatch for {id}");
    }
    foreach (var id in rejectedIds)
    {
        var before = beforeRows[id]; var after = afterRows[id];
        if (after != before) throw new InvalidOperationException($"Rejected task changed state/owner/epoch/attempts: {id}");
    }
    var result = new { baseline_path = baselineFile, baseline_sha256 = baselineHash, baseline_normalized_sha256 = Sha256(baselineNormalized),
        prototype_path = prototypeFile, prototype_sha256 = prototypeHash, prototype_normalized_sha256 = Sha256(prototypeNormalized),
        sql_distinct = true, hub, selected_items = items.Count, expected_leases = 4, baseline_leases = baselineRows.Count,
        prototype_leases = prototypeRows.Count, equality = true, lease_ids = prototypeRows.Select(r => r.Id).Order().ToArray(),
        owner_by_task = prototypeRows.ToDictionary(r => r.Id, r => r.Owner), rejected_rows_unchanged = rejectedIds.Length,
        covered = new[] { "per-item worker", "per-item capability", "workflow pins", "observed epoch", "due-ready", "expired lease", "live lease exclusion", "future-ready exclusion", "attempt exhaustion", "native Runnable status", "constrained exclusion" } };
    await tx.RollbackAsync();
    return result;

    static async Task<Guid> ScalarGuid(NpgsqlConnection db, NpgsqlTransaction tx, string sql, Guid workflow)
    { await using var c = new NpgsqlCommand(sql, db, tx); c.Parameters.AddWithValue("w", workflow); return (Guid)(await c.ExecuteScalarAsync() ?? throw new InvalidOperationException("Expected task id")); }
}

static async Task<Dictionary<Guid, TaskState>> ReadTaskStates(NpgsqlConnection db, NpgsqlTransaction tx, Guid[] ids)
{
    await using var command = new NpgsqlCommand("SELECT id,state,epoch,attempts,owner FROM wfc_scale.hub_task WHERE id=ANY(@ids)", db, tx);
    command.Parameters.AddWithValue("ids", ids);
    var result = new Dictionary<Guid, TaskState>();
    await using var reader = await command.ExecuteReaderAsync();
    while (await reader.ReadAsync()) result.Add(reader.GetGuid(0), new(reader.GetInt16(1), reader.GetInt64(2), reader.GetInt32(3), reader.IsDBNull(4) ? null : reader.GetString(4)));
    if (result.Count != ids.Length) throw new InvalidOperationException("Fixture row missing from state check");
    return result;
}

static async Task<List<(string Id, string Owner, string Canonical)>> ExecuteClaim(NpgsqlConnection db,
    NpgsqlTransaction tx, string sql, string selected, string hub)
{
    await using var command = new NpgsqlCommand(sql, db, tx);
    command.Parameters.Add("selected", NpgsqlDbType.Jsonb).Value = selected;
    command.Parameters.AddWithValue("hub", hub); command.Parameters.AddWithValue("count", 4); command.Parameters.AddWithValue("partition", 0);
    command.Parameters.Add("lease", NpgsqlDbType.Interval).Value = TimeSpan.FromSeconds(30);
    var result = new List<(string Id, string Owner, string Canonical)>();
    await using (var reader = await command.ExecuteReaderAsync())
        while (await reader.ReadAsync())
        {
            var values = Enumerable.Range(0, reader.FieldCount).Where(i => i != 9).Select(i => reader.GetValue(i)).ToArray();
            result.Add((reader.GetGuid(0).ToString(), "", JsonSerializer.Serialize(values)));
        }
    var ids = result.Select(r => Guid.Parse(r.Id)).ToArray();
    if (ids.Length == 0) return result;
    await using var owners = new NpgsqlCommand("SELECT id,owner FROM wfc_scale.hub_task WHERE id=ANY(@ids)", db, tx);
    owners.Parameters.AddWithValue("ids", ids);
    await using var ownerReader = await owners.ExecuteReaderAsync();
    var map = new Dictionary<string, string>();
    while (await ownerReader.ReadAsync()) map[ownerReader.GetGuid(0).ToString()] = ownerReader.GetString(1);
    ownerReader.Close();
    return result.Select(r => (r.Id, map[r.Id], r.Canonical)).ToList();
}

static async Task<object> RunConcurrency(NpgsqlDataSource source, string prototypePath)
{
    await using var first = await source.OpenConnectionAsync();
    await using var second = await source.OpenConnectionAsync();
    var hub = "hub-0";
    var candidates = await ReadIds(first, 24, "early");
    var firstIds = candidates.Take(16).ToArray();
    var secondIds = candidates.Skip(8).Reverse().ToArray();
    var caps = JsonSerializer.Serialize(Enumerable.Range(0, 8).SelectMany(t => Enumerable.Range(1, 3)
        .Select(v => new { hub, task_type = $"activity-{t}", version = v })));
    string Payload(IEnumerable<Guid> order) => JsonSerializer.Serialize(order.Select((id, i) => new {
        id, epoch = 0, worker = $"competing-{i}", capabilities = JsonDocument.Parse(caps).RootElement, pins = (object?)null
    }));
    var query = File.ReadAllText(prototypePath).Trim();
    await using var tx1 = await first.BeginTransactionAsync();
    var firstCount = await ExecuteCount(first, tx1, query, Payload(firstIds), hub);
    await using var tx2 = await second.BeginTransactionAsync();
    var secondCount = await ExecuteCount(second, tx2, query, Payload(secondIds), hub);
    await tx2.CommitAsync();
    await tx1.CommitAsync();
    var ids = firstIds.Concat(secondIds).Distinct().ToArray();
    await using var check = new NpgsqlCommand("SELECT count(*) FILTER (WHERE state=1 AND epoch=1), count(DISTINCT id) FILTER (WHERE state=1 AND epoch=1), count(*) FILTER (WHERE state=1 AND owner LIKE 'competing-%') FROM wfc_scale.hub_task WHERE id=ANY(@ids)", first);
    check.Parameters.AddWithValue("ids", ids);
    await using var reader = await check.ExecuteReaderAsync(); await reader.ReadAsync();
    var leased = reader.GetInt64(0); var distinct = reader.GetInt64(1); var owned = reader.GetInt64(2);
    if (firstCount != 16 || secondCount != 8 || leased != 24 || distinct != 24 || owned != 24)
        throw new InvalidOperationException($"Opposite-order overlapping claims failed: first={firstCount}, second={secondCount}, leased={leased}, distinct={distinct}, owned={owned}");
    return new { ids = ids.Length, overlap = 8, first_input = "first 16 in sequence order", second_input = "16 IDs in reverse sequence order with 8 overlapping", first_claimed = firstCount,
        second_claimed = secondCount, final_leased = leased, unique_leased = distinct, worker_owners = owned, outcome = "passed; stable sequence lock order and SKIP LOCKED prevented duplicate admission" };
}

static async Task<int> ExecuteCount(NpgsqlConnection db, NpgsqlTransaction tx, string sql, string selected, string hub)
{
    await using var command = new NpgsqlCommand(sql, db, tx);
    command.Parameters.Add("selected", NpgsqlDbType.Jsonb).Value = selected;
    command.Parameters.AddWithValue("hub", hub); command.Parameters.AddWithValue("count", 4); command.Parameters.AddWithValue("partition", 0);
    command.Parameters.Add("lease", NpgsqlDbType.Interval).Value = TimeSpan.FromSeconds(30);
    var count = 0;
    await using var reader = await command.ExecuteReaderAsync();
    while (await reader.ReadAsync()) count++;
    return count;
}

static JsonElement? FindUpdateNode(JsonElement node)
{
    if (Get(node, "Operation") == "Update") return node;
    if (node.TryGetProperty("Plans", out var children)) foreach (var child in children.EnumerateArray())
    { var found = FindUpdateNode(child); if (found is not null) return found; }
    return null;
}
static IEnumerable<JsonElement> FindTaskScans(JsonElement node)
{
    if (Get(node, "Relation Name") == "hub_task") yield return node;
    if (node.TryGetProperty("Plans", out var children)) foreach (var child in children.EnumerateArray())
        foreach (var result in FindTaskScans(child)) yield return result;
}
static string? Get(JsonElement e, string property) => e.TryGetProperty(property, out var v) ? v.ToString() : null;
static string F(double d) => d.ToString("0.###", CultureInfo.InvariantCulture);
static string Quote(string s) => "\"" + s.Replace("\"", "\"\"") + "\"";
static string Sha256(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
static string NormalizeSql(string value) => Regex.Replace(value.Trim(), @"\s+", " ");
static string ReturnHash(List<(string Id, string Owner, string Canonical)> rows) => Sha256(string.Join("\n",
    rows.OrderBy(r => r.Id, StringComparer.Ordinal).Select(r => $"{r.Id}|{r.Owner}|{r.Canonical}")));
record Row(string Arm, string Stats, string Mode, int Batch, string Position, int Sample, double PlanningMs, double ExecutionMs, double ClientMs, double LeasedRows, string ScanNodes);
record TaskState(short State, long Epoch, int Attempts, string? Owner);
