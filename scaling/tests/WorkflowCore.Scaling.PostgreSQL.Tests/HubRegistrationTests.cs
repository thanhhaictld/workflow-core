using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using WorkflowCore.Hub;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubRegistrationTests
{
    [Fact]
    public async Task NewBusinessPoliciesAndRetiredOptionsAreRejected()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub="retired-registration-"+Guid.NewGuid().ToString("N");
        await using var tasks=new HubTaskStore(database); var defs=new HubDefinitionStore(tasks);
        foreach(var policy in new[]{HubActivityOrdering.KeyedExclusive,HubActivityOrdering.KeyedFifo,HubActivityOrdering.KeyedBounded})
            await Assert.ThrowsAsync<InvalidOperationException>(()=>defs.RegisterActivityAsync(
                new(hub,"a",(int)policy,4,10,10,"tenant","tenantId",Ordering:policy),default));
        foreach(var fifo in new[]{false,true})
            await Assert.ThrowsAsync<InvalidOperationException>(()=>defs.RegisterActivityAsync(
                new(hub,"legacy",fifo?2:1,4,10,10,"tenant","tenantId",StrictFifo:fifo),default));
        await Assert.ThrowsAsync<InvalidOperationException>(()=>defs.RegisterActivityAsync(
            new(hub,"unused-option",1,4,10,10,ResourceCapacity:2),default));
        var safe=new HubActivityRegistration(hub,"safe",1,4,10,10);
        Assert.True(await defs.RegisterActivityAsync(safe,default));
        await Assert.ThrowsAsync<InvalidOperationException>(()=>defs.RegisterActivityAsync(
            safe with {ResourceName="tenant",ResourceField="tenantId"},default));
    }
    [Fact]
    public async Task RolloutBoundaryOrdersConcurrentStartsAndNeverReclassifiesLaterExecutions()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var root=Environment.GetEnvironmentVariable("WFC_TEST_SOURCE_ROOT");
        var directory=new DirectoryInfo(root ?? AppContext.BaseDirectory);
        while(directory is not null && !File.Exists(Path.Combine(directory.FullName,"scaling","sql","033_hub_business_compatibility.sql"))) directory=directory.Parent;
        Assert.NotNull(directory);
        var schema="boundary_fixture_"+Guid.NewGuid().ToString("N");
        var migration=(await File.ReadAllTextAsync(Path.Combine(directory!.FullName,"scaling","sql","033_hub_business_compatibility.sql")))
            .Replace("wfc_scale.",schema+".",StringComparison.Ordinal);
        await using var before=new NpgsqlConnection(database); await before.OpenAsync();
        await using(var setup=new NpgsqlCommand($"CREATE SCHEMA {schema}; CREATE TABLE {schema}.hub_workflow(id uuid PRIMARY KEY)",before)) await setup.ExecuteNonQueryAsync();
        await using var tx=await before.BeginTransactionAsync();
        var oldId=Guid.NewGuid(); var newId=Guid.NewGuid();
        await using(var insert=new NpgsqlCommand($"INSERT INTO {schema}.hub_workflow(id) VALUES (@id)",before,tx))
        { insert.Parameters.AddWithValue("id",oldId); await insert.ExecuteNonQueryAsync(); }
        await using var cutover=new NpgsqlConnection(database); await cutover.OpenAsync();
        await using var migrate=new NpgsqlCommand(migration,cutover);
        var pending=migrate.ExecuteNonQueryAsync();
        await Task.Delay(100);
        Assert.False(pending.IsCompleted); // the start's table lock fences cutover
        await tx.CommitAsync(); await pending;
        await using(var insert=new NpgsqlCommand($"INSERT INTO {schema}.hub_workflow(id) VALUES (@id)",before))
        { insert.Parameters.AddWithValue("id",newId); await insert.ExecuteNonQueryAsync(); }
        await using(var replay=new NpgsqlCommand(migration,cutover)) await replay.ExecuteNonQueryAsync();
        await using var read=new NpgsqlCommand($"SELECT id,business_policy_compatibility FROM {schema}.hub_workflow ORDER BY business_policy_compatibility",before);
        await using var rows=await read.ExecuteReaderAsync();
        Assert.True(await rows.ReadAsync()); Assert.Equal(newId,rows.GetGuid(0)); Assert.False(rows.GetBoolean(1));
        Assert.True(await rows.ReadAsync()); Assert.Equal(oldId,rows.GetGuid(0)); Assert.True(rows.GetBoolean(1));
        Assert.False(await rows.ReadAsync());
    }
    public sealed class Data { public int Result { get; set; } }

    public class WaitingWorkflow(string label) : IWorkflow<Data>
    {
        public string Id => "waiting";
        public virtual int Version => 1;
        public void Build(IWorkflowBuilder<Data> builder) => builder
            .StartWith(context => ExecutionResult.Next()).Name(label)
            .WaitFor("continue", data => "key", data => DateTime.UnixEpoch)
            .Then(context => { ((Data)context.Workflow.Data).Result = Version; return ExecutionResult.Next(); });
    }

    public sealed class NextWorkflow() : WaitingWorkflow("version two")
    {
        public override int Version => 2;
    }

    private static ServiceProvider Provider(string hub, string label)
    {
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton(new WaitingWorkflow(label));
        services.AddHubWorkflowRuntime().AddWorkflow<WaitingWorkflow, Data>(hub)
            .AddWorkflow<NextWorkflow, Data>(hub);
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task CompatiblePublicationResumesWaitingSnapshotAndCodeDefinedVersionsComplete()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var hub = "registration-" + Guid.NewGuid().ToString("N");
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var remote = new HubRemoteWorkflowStore(tasks, definitions);
        async Task Publish(ServiceProvider provider)
        {
            foreach (var definition in provider.GetRequiredService<HubNativeCatalog>().Definitions)
                await definitions.RegisterAsync(definition, default);
        }
        async Task Drive(ServiceProvider provider)
        {
            for (var iteration = 0; iteration < 20; iteration++)
            {
                var leases = await tasks.ClaimAsync("worker", [hub], [new(hub, "__native_workflow__", 1)],
                    1, TimeSpan.FromSeconds(30), default);
                if (leases.Count == 0) return;
                var lease = Assert.Single(leases);
                var runtime = (await native.GetRuntimeAsync(lease.WorkflowId!.Value, default))!;
                var commit = await provider.GetRequiredService<HubNativeExecutor>().ExecuteAsync(runtime, lease, "worker");
                Assert.NotNull(await native.CommitAsync(runtime.Id, commit, default));
            }
            Assert.Fail("Workflow decisions did not become idle");
        }

        Guid existing;
        HubNativeRuntime waiting;
        using (var original = Provider(hub, "before deployment"))
        {
            await Publish(original);
            existing = await remote.StartAsync(new(hub, "waiting", 1, JsonSerializer.SerializeToElement(new Data())), default);
            await Drive(original);
            waiting = (await native.GetRuntimeAsync(existing, default))!;
            Assert.Equal("Runnable", waiting.Status);
            var snapshot = original.GetRequiredService<HubSnapshotCodec>().Decode<WorkflowInstance>(waiting.Snapshot!.Value);
            Assert.Contains(snapshot.ExecutionPointers, pointer => pointer.Status == PointerStatus.WaitingForEvent);
        }

        using var deployed = Provider(hub, "after deployment");
        await Publish(deployed);
        Assert.Equal(JsonSerializer.Serialize(waiting), JsonSerializer.Serialize(await native.GetRuntimeAsync(existing, default)));
        var fresh = await remote.StartAsync(new(hub, "waiting", 1, JsonSerializer.SerializeToElement(new Data())), default);
        var nextVersion = await remote.StartAsync(new(hub, "waiting", 2, JsonSerializer.SerializeToElement(new Data())), default);
        await Drive(deployed);
        await native.PublishEventAsync(hub, new("continue", "key", JsonSerializer.SerializeToElement(true), "continue"), default);
        await Drive(deployed);
        foreach (var id in new[] { existing, fresh, nextVersion })
        {
            var runtime = (await native.GetRuntimeAsync(id, default))!;
            Assert.Equal("Complete", runtime.Status);
            var snapshot = deployed.GetRequiredService<HubSnapshotCodec>().Decode<WorkflowInstance>(runtime.Snapshot!.Value);
            Assert.Equal(id == nextVersion ? 2 : 1, snapshot.Version);
            Assert.Equal(snapshot.Version, ((Data)snapshot.Data).Result);
            if (id == existing)
            {
                var old = deployed.GetRequiredService<HubSnapshotCodec>().Decode<WorkflowInstance>(waiting.Snapshot!.Value);
                Assert.All(old.ExecutionPointers, pointer => Assert.NotNull(snapshot.ExecutionPointers.FindById(pointer.Id)));
            }
        }
    }

    [Fact]
    public async Task WorkflowPublicationUpdatesMetadataWithoutChangingExistingRun()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var hub = "registration-" + Guid.NewGuid().ToString("N");
        var initial = new HubDefinitionRegistration(hub, "workflow", 1, 4, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native", implementation = "old-build" }));
        Assert.True(await definitions.RegisterAsync(initial, default));
        var id = await new HubRemoteWorkflowStore(tasks, definitions).StartAsync(
            new(hub, "workflow", 1, JsonSerializer.SerializeToElement(new { })), default);
        var lease = Assert.Single(await tasks.ClaimAsync("worker", [hub],
            [new(hub, "__native_workflow__", 1)], 1, TimeSpan.FromSeconds(30), default));
        Assert.NotNull(await native.CommitAsync(id, new(1, "worker", lease.Epoch, 0, lease.Id,
            JsonSerializer.SerializeToElement(new { pointer = "waiting-pointer" }), "Runnable", false,
            DateTime.UtcNow.AddHours(1), [], [], [], [], [], new("waiting-pointer", 0)), default));
        var before = (await native.GetRuntimeAsync(id, default))!;
        var monitor = new HubWorkflowStore(tasks, new HubPolicyRegistry(), new HubWorkflowRegistry());
        var history = await monitor.HistoryAsync(id, default);

        var updated = initial with { Partitions = 8, StepCount = 2,
            Metadata = JsonSerializer.SerializeToElement(new { executionMode = "native", implementation = "new-build", name = "updated" }) };
        Assert.False(await definitions.RegisterAsync(updated, default));
        Assert.False(await definitions.RegisterAsync(updated, default));
        var published = (await definitions.GetAsync(hub, "workflow", 1, default))!;
        Assert.Equal(8, published.Partitions);
        Assert.Equal(2, published.StepCount);
        Assert.Equal("new-build", published.Metadata.GetProperty("implementation").GetString());
        Assert.Equal("updated", published.Metadata.GetProperty("name").GetString());
        var after = (await native.GetRuntimeAsync(id, default))!;
        Assert.Equal(JsonSerializer.Serialize(before), JsonSerializer.Serialize(after));
        Assert.Equal(JsonSerializer.Serialize(history), JsonSerializer.Serialize(await monitor.HistoryAsync(id, default)));
        Assert.Equal("Complete", (await tasks.GetAsync(lease.Id, default))!.State);

        await definitions.RegisterAsync(initial with { Version = 2 }, default);
        var fresh = await new HubRemoteWorkflowStore(tasks, definitions).StartAsync(
            new(hub, "workflow", 2, JsonSerializer.SerializeToElement(new { })), default);
        Assert.Equal(1, (await native.GetRuntimeAsync(id, default))!.Version);
        Assert.Equal(2, (await native.GetRuntimeAsync(fresh, default))!.Version);
    }

    [Fact]
    public async Task BusinessActivityPublicationIsFrozenAndRetainsOperatorCapacities()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var hub = "registration-" + Guid.NewGuid().ToString("N");
        var initial = new HubActivityRegistration(hub, "activity", 1, 4, 10, 10,
            "tenant", "tenantId", 4, false, JsonSerializer.SerializeToElement(new { implementation = "old-build" }));
        await Assert.ThrowsAsync<InvalidOperationException>(() => definitions.RegisterActivityAsync(initial, default));
        // Seed a pre-retirement registration to exercise compatibility publication.
        await using (var db = new NpgsqlConnection(database))
        {
            await db.OpenAsync();
            await using var seed = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_activity_registration
                  (hub_id,activity_type,version,partitions,hub_concurrency,activity_concurrency,
                   resource_name,resource_field,resource_capacity,resource_fifo,contract,execution_policy)
                VALUES (@hub,'activity',1,4,10,10,'tenant','tenantId',4,false,
                        '{"implementation":"old-build"}'::jsonb,'Legacy')
                """, db);
            seed.Parameters.AddWithValue("hub",hub);
            await seed.ExecuteNonQueryAsync();
        }
        Assert.False(await definitions.RegisterActivityAsync(initial, default));
        var revision = await definitions.UpdateActivityCapacityAsync(new(hub, "activity", 1, 1, 3, 2, 1), default);
        var updated = initial with { Partitions = 8, ResourceField = "accountId", StrictFifo = true,
            Contract = JsonSerializer.SerializeToElement(new { implementation = "new-build", inputs = new[] { "accountId" } }) };
        await Assert.ThrowsAsync<InvalidOperationException>(() => definitions.RegisterActivityAsync(updated, default));
        Assert.All(await Task.WhenAll(Enumerable.Range(0, 8)
            .Select(_ => definitions.RegisterActivityAsync(initial, default))), inserted => Assert.False(inserted));
        var published = Assert.Single(await definitions.ActivitiesAsync(default), a => a.HubId == hub);
        Assert.Equal(4, published.Partitions);
        Assert.Equal("tenantId", published.ResourceField);
        Assert.False(published.StrictFifo);
        Assert.Equal("old-build", published.Contract!.Value.GetProperty("implementation").GetString());
        var policy = (await definitions.GetActivityPolicyAsync(hub, "activity", 1, default))!;
        Assert.Equal(revision, policy.Revision);
        Assert.Equal(3, policy.HubConcurrency);
        Assert.Equal(2, policy.ActivityConcurrency);
        Assert.Equal(1, policy.ResourceCapacity);
    }

    [Fact]
    public async Task PolicyCapacityChangePublishesCrossReplicaCandidateInvalidation()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for policy hint tests");
        var hub = "policy-hint-" + Guid.NewGuid().ToString("N");
        await using var tasks = new HubTaskStore(database, enableTaskIdHints: true);
        var definitions = new HubDefinitionStore(tasks);
        var registration = new HubActivityRegistration(hub, "activity", 1, 4, 10, 10);
        Assert.True(await definitions.RegisterActivityAsync(registration, default));
        await using var listener = new NpgsqlConnection(database); await listener.OpenAsync();
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        listener.Notification += (_, e) =>
        {
            if (e.Channel == "wfc_hub_matching_ready" && e.Payload.Contains(hub, StringComparison.Ordinal))
                received.TrySetResult(e.Payload);
        };
        await using (var listen = new NpgsqlCommand("LISTEN wfc_hub_matching_ready", listener)) await listen.ExecuteNonQueryAsync();
        await definitions.UpdateActivityCapacityAsync(new(hub, "activity", 1, 1, 11, 10, 10), default);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!received.Task.IsCompleted) await listener.WaitAsync(timeout.Token);
        using var payload = System.Text.Json.JsonDocument.Parse(await received.Task);
        Assert.Equal(hub, payload.RootElement.GetProperty("hub").GetString());
        Assert.Equal("policy-change", payload.RootElement.GetProperty("kind").GetString());
    }

    [Fact]
    public async Task PolicyChangeUsesExistingRecoveryWhenTaskIdHintsAreDisabled()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for policy hint tests");
        var hub = "policy-hint-off-" + Guid.NewGuid().ToString("N");
        await using var tasks = new HubTaskStore(database, enableTaskIdHints: false);
        var definitions = new HubDefinitionStore(tasks);
        var registration = new HubActivityRegistration(hub, "activity", 1, 4, 10, 10);
        Assert.True(await definitions.RegisterActivityAsync(registration, default));
        await using var listener = new NpgsqlConnection(database); await listener.OpenAsync();
        var received = 0;
        listener.Notification += (_, e) =>
        {
            if (e.Channel == "wfc_hub_matching_ready" && e.Payload.Contains(hub, StringComparison.Ordinal))
                Interlocked.Increment(ref received);
        };
        await using (var listen = new NpgsqlCommand("LISTEN wfc_hub_matching_ready", listener)) await listen.ExecuteNonQueryAsync();
        await definitions.UpdateActivityCapacityAsync(new(hub, "activity", 1, 1, 11, 10, 10), default);
        using (var quiet = new CancellationTokenSource(TimeSpan.FromMilliseconds(250)))
        {
            try { await listener.WaitAsync(quiet.Token); }
            catch (OperationCanceledException) when (quiet.IsCancellationRequested) { }
        }
        Assert.Equal(0, Volatile.Read(ref received));
    }
}
