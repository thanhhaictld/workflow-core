using Npgsql;
using NpgsqlTypes;
using WorkflowCore.Hub;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

// A real pre-033 schema for upgrade/continuation tests. Never targets an
// existing database or changes the shared test connection string.
internal sealed class LegacyDatabaseFixture : IAsyncDisposable
{
    private readonly string _adminConnection;
    private readonly string _name;
    private readonly string _sqlDirectory;
    private bool _owned;
    private bool _passed;
    internal string ConnectionString { get; }

    private LegacyDatabaseFixture(string adminConnection, string name, string sqlDirectory)
    {
        _adminConnection=adminConnection; _name=name; _sqlDirectory=sqlDirectory;
        ConnectionString=new NpgsqlConnectionStringBuilder(adminConnection) { Database=name }.ConnectionString;
    }

    internal static async Task<LegacyDatabaseFixture> CreateAsync(string database)
    {
        var source=Environment.GetEnvironmentVariable("WFC_TEST_SOURCE_ROOT");
        var directory=new DirectoryInfo(source ?? AppContext.BaseDirectory);
        while(directory is not null && !File.Exists(Path.Combine(directory.FullName,"scaling","sql","033_hub_business_compatibility.sql")))
            directory=directory.Parent;
        if(directory is null) throw new InvalidOperationException("Test checkout not found; set WFC_TEST_SOURCE_ROOT");
        var name="wfc_upgrade_"+Guid.NewGuid().ToString("N");
        var fixture=new LegacyDatabaseFixture(database,name,Path.Combine(directory.FullName,"scaling","sql"));
        await using(var admin=new NpgsqlConnection(database))
        {
            await admin.OpenAsync();
            await using var create=new NpgsqlCommand($"CREATE DATABASE \"{name}\"",admin);
            await create.ExecuteNonQueryAsync();
            fixture._owned=true;
        }
        try
        {
            var numbers=Enumerable.Range(2,21).Concat([24,29,30,31,32]);
            foreach(var number in numbers) await fixture.ApplyAsync(number);
            return fixture;
        }
        catch(Exception error)
        {
            // Preserve incomplete setup for diagnosis; never guess at ownership.
            throw new InvalidOperationException($"Compatibility fixture setup failed; test-created database {name} retained",error);
        }
    }

    internal async Task ApplyAsync(int number)
    {
        var path=Directory.GetFiles(_sqlDirectory,$"{number:000}_*.sql").Single();
        var sql=await File.ReadAllTextAsync(path);
        await using var db=new NpgsqlConnection(ConnectionString); await db.OpenAsync();
        if(number==31)
        {
            // DROP INDEX CONCURRENTLY must execute in autocommit, one at a time.
            foreach(var line in sql.Split('\n').Where(x=>x.TrimStart().StartsWith("DROP INDEX CONCURRENTLY",StringComparison.Ordinal)))
            { await using var cmd=new NpgsqlCommand(line,db); await cmd.ExecuteNonQueryAsync(); }
        }
        else { await using var cmd=new NpgsqlCommand(sql,db); await cmd.ExecuteNonQueryAsync(); }
    }

    internal async Task SeedRegistrationAsync(HubActivityRegistration policy)
    {
        await using var db=new NpgsqlConnection(ConnectionString); await db.OpenAsync();
        await using var cmd=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_activity_registration
              (hub_id,activity_type,version,partitions,hub_concurrency,activity_concurrency,
               resource_name,resource_field,resource_capacity,resource_fifo,contract,execution_policy)
            VALUES (@hub,@type,@version,@partitions,@hubcap,@activitycap,@name,@field,@capacity,@fifo,@contract,@policy)
            """,db);
        cmd.Parameters.AddWithValue("hub",policy.HubId); cmd.Parameters.AddWithValue("type",policy.ActivityType);
        cmd.Parameters.AddWithValue("version",policy.Version); cmd.Parameters.AddWithValue("partitions",policy.Partitions);
        cmd.Parameters.AddWithValue("hubcap",policy.HubConcurrency); cmd.Parameters.AddWithValue("activitycap",policy.ActivityConcurrency);
        cmd.Parameters.Add("name",NpgsqlDbType.Text).Value=(object?)policy.ResourceName ?? DBNull.Value;
        cmd.Parameters.Add("field",NpgsqlDbType.Text).Value=(object?)policy.ResourceField ?? DBNull.Value;
        cmd.Parameters.AddWithValue("capacity",policy.ResourceCapacity); cmd.Parameters.AddWithValue("fifo",policy.StrictFifo);
        cmd.Parameters.Add("contract",NpgsqlDbType.Jsonb).Value=policy.Contract is { } contract ? contract.GetRawText() : DBNull.Value;
        cmd.Parameters.AddWithValue("policy",policy.Ordering.ToString());
        await cmd.ExecuteNonQueryAsync();
    }

    internal async Task<Guid> SeedTaskAsync(HubTaskRoute route, string payload, string? requestKey=null)
    {
        var id=Guid.NewGuid(); var partition=HubPartitionRouter.ForWorkflow(id,route.PartitionCount);
        await using var db=new NpgsqlConnection(ConnectionString); await db.OpenAsync();
        await using var tx=await db.BeginTransactionAsync();
        foreach(var resource in route.ResourcesForPartition(partition))
        {
            await using var add=new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
                VALUES (@key,@capacity,@fifo) ON CONFLICT DO NOTHING
                """,db,tx);
            add.Parameters.AddWithValue("key",resource.Key); add.Parameters.AddWithValue("capacity",resource.Capacity);
            add.Parameters.AddWithValue("fifo",resource.StrictFifo); await add.ExecuteNonQueryAsync();
        }
        await using(var add=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,request_key)
            VALUES (@id,@hub,@partition,@type,@version,@payload,@request)
            """,db,tx))
        {
            add.Parameters.AddWithValue("id",id); add.Parameters.AddWithValue("hub",route.HubId);
            add.Parameters.AddWithValue("partition",partition); add.Parameters.AddWithValue("type",route.TaskType);
            add.Parameters.AddWithValue("version",route.Version); add.Parameters.Add("payload",NpgsqlDbType.Jsonb).Value=payload;
            add.Parameters.Add("request",NpgsqlDbType.Text).Value=(object?)requestKey ?? DBNull.Value;
            await add.ExecuteNonQueryAsync();
        }
        foreach(var resource in route.ResourcesForPartition(partition))
        {
            await using var add=new NpgsqlCommand("INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@id,@key)",db,tx);
            add.Parameters.AddWithValue("id",id); add.Parameters.AddWithValue("key",resource.Key); await add.ExecuteNonQueryAsync();
        }
        await tx.CommitAsync(); return id;
    }

    internal async Task SeedWorkflowConstraintsAsync(Guid workflowId, HubResourceRule resource)
    {
        await using var db=new NpgsqlConnection(ConnectionString); await db.OpenAsync();
        await using var cmd=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
              VALUES (@key,@capacity,@fifo) ON CONFLICT DO NOTHING;
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
              SELECT id,@key FROM wfc_scale.hub_task WHERE workflow_id=@workflow
            """,db);
        cmd.Parameters.AddWithValue("key",resource.Key); cmd.Parameters.AddWithValue("capacity",resource.Capacity);
        cmd.Parameters.AddWithValue("fifo",resource.StrictFifo); cmd.Parameters.AddWithValue("workflow",workflowId);
        await cmd.ExecuteNonQueryAsync();
    }

    internal void MarkPassed() => _passed=true;

    public async ValueTask DisposeAsync()
    {
        if(!_owned)return;
        if(!_passed)
        {
            Console.Error.WriteLine($"Test-created compatibility database {_name} retained for diagnosis");
            return;
        }
        await using(var owned=new NpgsqlConnection(ConnectionString)) NpgsqlConnection.ClearPool(owned);
        try
        {
            await using var admin=new NpgsqlConnection(_adminConnection); await admin.OpenAsync();
            await using var drop=new NpgsqlCommand($"DROP DATABASE \"{_name}\"",admin);
            await drop.ExecuteNonQueryAsync(); _owned=false;
        }
        catch(Exception error)
        { throw new InvalidOperationException($"Owned test database {_name} retained because cleanup failed",error); }
    }
}
