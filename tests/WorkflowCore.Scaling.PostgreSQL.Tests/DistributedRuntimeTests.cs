using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Persistence.PostgreSQL;
using WorkflowCore.Scaling.PostgreSQL;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class DistributedRuntimeTests
{
    private static string Connection => Environment.GetEnvironmentVariable("WFC_TEST_PG")
        ?? throw new InvalidOperationException("WFC_TEST_PG must point to a PostgreSQL database with scaling SQL applied");

    private static async Task<Guid> EnqueueAsync(string queueName, string workKey, Guid itemId)
    {
        var workId = Guid.NewGuid();
        await using var connection = new NpgsqlConnection(Connection);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.work_item(id,queue_name,item_key,item_id,due_at)
            VALUES (@id,@name,@key,@item,now())
            """, connection);
        command.Parameters.AddWithValue("id", workId);
        command.Parameters.AddWithValue("name", queueName);
        command.Parameters.AddWithValue("key", workKey);
        command.Parameters.AddWithValue("item", itemId);
        await command.ExecuteNonQueryAsync();
        return workId;
    }

    [Fact]
    public async Task ConcurrentWorkersCannotClaimSameWork()
    {
        await using var queue = new FencedWorkQueue(Connection);
        var name = "concurrent:" + Guid.NewGuid().ToString("N");
        var id = await EnqueueAsync(name, name, Guid.NewGuid());
        var claims = await Task.WhenAll(Enumerable.Range(0, 20).Select(i =>
            queue.ClaimAsync(name, $"worker:{i}", 1, TimeSpan.FromSeconds(10), 5, CancellationToken.None)));
        var acquired = claims.SelectMany(x => x).ToArray();
        Assert.Single(acquired);
        Assert.Equal(id, acquired[0].Id);
    }

    [Fact]
    public async Task ExpiredLeaseIsFencedFromNewGeneration()
    {
        await using var queue = new FencedWorkQueue(Connection);
        var name = "expired:" + Guid.NewGuid().ToString("N");
        await EnqueueAsync(name, name, Guid.NewGuid());
        var a = Assert.Single(await queue.ClaimAsync(name, "A", 1,
            TimeSpan.FromMilliseconds(250), 5, CancellationToken.None));
        await Task.Delay(600);
        var b = Assert.Single(await queue.ClaimAsync(name, "B", 1,
            TimeSpan.FromSeconds(5), 5, CancellationToken.None));
        Assert.True(b.Epoch > a.Epoch);
        Assert.False(await queue.RetryAsync(a, "A", TimeSpan.Zero, 5, "stale", CancellationToken.None));
        Assert.True(await queue.RetryAsync(b, "B", TimeSpan.Zero, 5, "retry", CancellationToken.None));
    }

    [Fact]
    public async Task WorkflowGateRejectsCompetingAndStaleOwners()
    {
        await using var queue = new FencedWorkQueue(Connection);
        var id = Guid.NewGuid();
        var a = await queue.AcquireWorkflowAsync(id, "A", TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(a);
        Assert.Null(await queue.AcquireWorkflowAsync(id, "B", TimeSpan.FromSeconds(30), CancellationToken.None));
        await queue.ReleaseGateAsync(a!, CancellationToken.None);
        var b = await queue.AcquireWorkflowAsync(id, "B", TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(b);
        Assert.True(b!.Epoch > a!.Epoch);
        await queue.ReleaseGateAsync(a, CancellationToken.None);
        Assert.Null(await queue.AcquireWorkflowAsync(id, "C", TimeSpan.FromSeconds(30), CancellationToken.None));
        await queue.ReleaseGateAsync(b, CancellationToken.None);
    }

    [Fact]
    public async Task EngineCommitsWorkflowAndAcknowledgementTogether()
    {
        await using (var db = new PostgresContext(Connection, "wfc"))
            await db.Database.MigrateAsync();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDistributedWorkflowCore(Connection);
        services.AddTransient<NoopStep>();
        using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IWorkflowHost>().RegisterWorkflow<ExampleWorkflow>();
        var engine = provider.GetRequiredService<DistributedWorkflowEngine>();
        var queue = provider.GetRequiredService<FencedWorkQueue>();
        var id = await engine.StartAsync("distributed-smoke");
        var work = Assert.Single(await queue.ClaimAsync("workflow", "test-worker", 1,
            TimeSpan.FromSeconds(30), 5, CancellationToken.None));
        Assert.Equal(id, work.ItemId);
        var fence = await queue.AcquireWorkflowAsync(id, "test-worker",
            TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(fence);
        await engine.ExecuteWorkflowAsync(work, fence!, "test-worker", CancellationToken.None);
        await queue.ReleaseGateAsync(fence!, CancellationToken.None);
        await using var verify = new PostgresContext(Connection, "wfc");
        var stored = await verify.Set<WorkflowCore.Persistence.EntityFramework.Models.PersistedWorkflow>()
            .AsNoTracking().SingleAsync(x => x.InstanceId == id);
        Assert.Equal(WorkflowStatus.Complete, stored.Status);
        await using var command = new NpgsqlConnection(Connection);
        await command.OpenAsync();
        await using var count = new NpgsqlCommand(
            "SELECT state FROM wfc_scale.work_item WHERE id = @id", command);
        count.Parameters.AddWithValue("id", work.Id);
        Assert.Equal((short)2, Convert.ToInt16(await count.ExecuteScalarAsync()));
    }

    [Fact]
    public async Task EventPublishedBeforeWaitForIsDurablyReplayed()
    {
        await using (var db = new PostgresContext(Connection, "wfc"))
            await db.Database.MigrateAsync();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDistributedWorkflowCore(Connection);
        using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IWorkflowHost>().RegisterWorkflow<WaitWorkflow, WaitData>();

        var queue = provider.GetRequiredService<FencedWorkQueue>();
        var engine = provider.GetRequiredService<DistributedWorkflowEngine>();
        var key = Guid.NewGuid().ToString("N");
        var eventId = await engine.PublishEventAsync("DistributedTestSignal", key, "hello");

        // There is no subscription yet: the initial event message is ACKed
        // but the durable event stays unprocessed for future WaitFor creation.
        var eventWork = Assert.Single(await queue.ClaimAsync("event", "signal-test", 1,
            TimeSpan.FromSeconds(30), 5, CancellationToken.None));
        Assert.Equal(eventId, eventWork.ItemId);
        await engine.ExecuteEventAsync(eventWork, "signal-test", CancellationToken.None);

        var workflowId = await engine.StartAsync("distributed-wait", new WaitData { Key = key });
        await RunWorkflowAsync(queue, engine, "start-wait");
        await using (var db = new PostgresContext(Connection, "wfc"))
        {
            var count = await db.Set<WorkflowCore.Persistence.EntityFramework.Models.PersistedSubscription>()
                .CountAsync(x => x.WorkflowId == workflowId.ToString());
            Assert.True(count > 0, "WaitFor should persist a subscription");
        }

        // Persisting the subscription must enqueue a NEW event work item
        // for the previously received event, without a process restart.
        var replay = Assert.Single(await queue.ClaimAsync("event", "signal-replay", 1,
            TimeSpan.FromSeconds(30), 5, CancellationToken.None));
        Assert.Equal(eventId, replay.ItemId);
        await engine.ExecuteEventAsync(replay, "signal-replay", CancellationToken.None);
        await RunWorkflowAsync(queue, engine, "resume-wait");

        await using var verify = new PostgresContext(Connection, "wfc");
        var stored = await verify.Set<WorkflowCore.Persistence.EntityFramework.Models.PersistedWorkflow>()
            .AsNoTracking().SingleAsync(x => x.InstanceId == workflowId);
        Assert.Equal(WorkflowStatus.Complete, stored.Status);
    }

    private static async Task RunWorkflowAsync(
        FencedWorkQueue queue, DistributedWorkflowEngine engine, string owner)
    {
        var item = Assert.Single(await queue.ClaimAsync("workflow", owner, 1,
            TimeSpan.FromSeconds(30), 5, CancellationToken.None));
        var gate = await queue.AcquireWorkflowAsync(item.ItemId, owner,
            TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(gate);
        try { await engine.ExecuteWorkflowAsync(item, gate!, owner, CancellationToken.None); }
        finally { await queue.ReleaseGateAsync(gate!, CancellationToken.None); }
    }

    public sealed class WaitData
    {
        public string Key { get; set; } = string.Empty;
    }

    public sealed class WaitWorkflow : IWorkflow<WaitData>
    {
        public string Id => "distributed-wait";
        public int Version => 1;
        public void Build(IWorkflowBuilder<WaitData> builder)
        {
            builder.StartWith(context => ExecutionResult.Next())
                .WaitFor("DistributedTestSignal", (data, context) => data.Key,
                    data => DateTime.UtcNow.AddDays(-1))
                .Then(context => ExecutionResult.Next());
        }
    }

    public sealed class ExampleWorkflow : IWorkflow
    {
        public string Id => "distributed-smoke";
        public int Version => 1;
        public void Build(IWorkflowBuilder<object> builder) =>
            builder.StartWith<NoopStep>();
    }

    public sealed class NoopStep : StepBody
    {
        public override ExecutionResult Run(IStepExecutionContext context) =>
            ExecutionResult.Next();
    }
}
