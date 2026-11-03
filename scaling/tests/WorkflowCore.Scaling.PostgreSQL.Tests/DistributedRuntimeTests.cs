using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
    public async Task ExecutionHistoryTracksDurableStepAttemptsAndCommittedTransitions()
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
        var history = provider.GetRequiredService<ExecutionHistoryStore>();
        var id = await engine.StartAsync("distributed-smoke");
        Assert.Equal(new[] { "WorkflowStarted" },
            (await history.ReadAsync(id)).Select(x => x.Event.EventType));

        var work = Assert.Single(await queue.ClaimAsync("workflow", "journal-test", 1,
            TimeSpan.FromSeconds(30), 5, CancellationToken.None));
        var fence = await queue.AcquireWorkflowAsync(id, "journal-test",
            TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(fence);
        try
        {
            await engine.ExecuteWorkflowAsync(work, fence!, "journal-test", CancellationToken.None);
        }
        finally { await queue.ReleaseGateAsync(fence!, CancellationToken.None); }

        var entries = await history.ReadAsync(id);
        Assert.Contains(entries, e => e.Event.EventType == "WorkAttemptStarted");
        Assert.Contains(entries, e => e.Event.EventType == "StepAttemptStarted");
        Assert.Contains(entries, e => e.Event.EventType == "StepBodyReturned");
        Assert.Contains(entries, e => e.Event.EventType == "StepCompleted");
        Assert.Contains(entries, e => e.Event.EventType == "WorkflowCompleted");
        Assert.Contains(entries, e => e.Event.EventType == "WorkAttemptCommitted");
        Assert.All(entries.Where(e => e.Event.EventType == "StepAttemptStarted"),
            e => Assert.NotNull(e.Event.StepAttemptId));
        Assert.True(entries.Zip(entries.Skip(1)).All(pair =>
            pair.First.HistoryId < pair.Second.HistoryId));

        // Keyset pagination reads the same entries without modification.
        var first = await history.ReadAsync(id, take: 2);
        var second = await history.ReadAsync(id, afterHistoryId: first[^1].HistoryId);
        Assert.Equal(entries.Select(e => e.HistoryId),
            first.Concat(second).Select(e => e.HistoryId));
    }

    [Fact]
    public async Task ExecutionHistoryRollsBackWithItsContainingTransaction()
    {
        var id = Guid.NewGuid();
        await using (var db = new PostgresContext(Connection, "wfc"))
        {
            await using var tx = await db.Database.BeginTransactionAsync();
            await ExecutionHistoryStore.AppendInTransactionAsync(db,
                new ExecutionHistoryEvent(id, "RolledBack"), CancellationToken.None);
            await tx.RollbackAsync();
        }

        var history = new ExecutionHistoryStore(Connection);
        Assert.Empty(await history.ReadAsync(id));
    }

    [Fact]
    public async Task RetriesCreateDifferentStepAttemptIdsAndPreserveFailureHistory()
    {
        await using (var db = new PostgresContext(Connection, "wfc"))
            await db.Database.MigrateAsync();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDistributedWorkflowCore(Connection);
        services.AddTransient<FailOnceStep>();
        using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IWorkflowHost>().RegisterWorkflow<RetryWorkflow, RetryData>();

        var engine = provider.GetRequiredService<DistributedWorkflowEngine>();
        var queue = provider.GetRequiredService<FencedWorkQueue>();
        var history = provider.GetRequiredService<ExecutionHistoryStore>();
        var id = await engine.StartAsync("history-retry", new RetryData());

        await RunWorkflowAsync(queue, engine, "retry-history-test");
        await RunWorkflowAsync(queue, engine, "retry-history-test");

        var timeline = await history.ReadAsync(id);
        var started = timeline.Where(x => x.Event.EventType == "StepAttemptStarted").ToArray();
        Assert.Equal(2, started.Length);
        Assert.NotEqual(started[0].Event.StepAttemptId, started[1].Event.StepAttemptId);
        Assert.Contains(timeline, x => x.Event.EventType == "StepBodyThrew"
            && x.Event.StepAttemptId == started[0].Event.StepAttemptId);
        Assert.Contains(timeline, x => x.Event.EventType == "StepCompleted"
            && x.Event.StepAttemptId == started[1].Event.StepAttemptId);
        Assert.Contains(timeline, x => x.Event.EventType == "WorkflowCompleted");
    }

    [Fact]
    public async Task HostedWorkerPoolProcessesNewWorkflowWithoutManualDispatch()
    {
        await using (var db = new PostgresContext(Connection, "wfc"))
            await db.Database.MigrateAsync();
        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDistributedWorkflowCore(Connection, o =>
                {
                    o.Concurrency = 4;
                    o.IdleDelay = TimeSpan.FromMilliseconds(25);
                });
                services.AddTransient<NoopStep>();
            }).Build();

        host.Services.GetRequiredService<IWorkflowHost>()
            .RegisterWorkflow<ExampleWorkflow>();
        await host.StartAsync();
        try
        {
            var engine = host.Services.GetRequiredService<DistributedWorkflowEngine>();
            var id = await engine.StartAsync("distributed-smoke");
            var completed = false;
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (!timeout.IsCancellationRequested)
            {
                await using var db = new PostgresContext(Connection, "wfc");
                var status = await db.Set<WorkflowCore.Persistence.EntityFramework.Models.PersistedWorkflow>()
                    .AsNoTracking().Where(w => w.InstanceId == id)
                    .Select(w => w.Status).SingleAsync();
                if (status == WorkflowStatus.Complete)
                {
                    completed = true;
                    break;
                }
                await Task.Delay(50);
            }
            Assert.True(completed, "Hosted workers did not complete the workflow");
        }
        finally { await host.StopAsync(); }
    }

    [Fact]
    public async Task DeadLetterIsVisibleAndRequiresExplicitOperatorReplay()
    {
        await using var queue = new FencedWorkQueue(Connection);
        var name = "deadletter:" + Guid.NewGuid().ToString("N");
        var id = await EnqueueAsync(name, name, Guid.NewGuid());
        var claimed = Assert.Single(await queue.ClaimAsync(name, "failure", 1,
            TimeSpan.FromSeconds(10), 1, CancellationToken.None));
        Assert.True(await queue.RetryAsync(claimed, "failure", TimeSpan.Zero,
            1, "permanent failure", CancellationToken.None));
        var stats = await queue.GetStatsAsync(name);
        Assert.Equal(1, stats.Dead);
        Assert.Empty(await queue.ClaimAsync(name, "another-worker", 1,
            TimeSpan.FromSeconds(10), 1, CancellationToken.None));
        Assert.True(await queue.RequeueDeadAsync(id));
        var replay = Assert.Single(await queue.ClaimAsync(name, "operator", 1,
            TimeSpan.FromSeconds(10), 2, CancellationToken.None));
        Assert.Equal(id, replay.Id);
        Assert.True(replay.Epoch > claimed.Epoch);
    }

    [Fact]
    public async Task IdempotencyKeysPreventDuplicateWorkflowStartsAndSignals()
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
        var requestKey = Guid.NewGuid().ToString("N");
        var a = await engine.StartAsync("distributed-smoke", idempotencyKey: requestKey);
        var b = await engine.StartAsync("distributed-smoke", idempotencyKey: requestKey);
        Assert.Equal(a, b);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            engine.StartAsync("distributed-smoke", reference: "different", idempotencyKey: requestKey));

        var eventKey = Guid.NewGuid().ToString("N");
        var signalA = await engine.PublishEventAsync("idempotent-signal", eventKey, "ok",
            idempotencyKey: eventKey);
        var signalB = await engine.PublishEventAsync("idempotent-signal", eventKey, "ok",
            idempotencyKey: eventKey);
        Assert.Equal(signalA, signalB);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            engine.PublishEventAsync("idempotent-signal", eventKey, "different",
                idempotencyKey: eventKey));

        // Drain both queues so later tests do not observe work from this test.
        var queue = provider.GetRequiredService<FencedWorkQueue>();
        await RunWorkflowAsync(queue, engine, "idempotency-worker");
        var eventWork = Assert.Single(await queue.ClaimAsync("event", "idempotency-worker", 1,
            TimeSpan.FromSeconds(30), 5, CancellationToken.None));
        Assert.Equal(signalA, eventWork.ItemId);
        await engine.ExecuteEventAsync(eventWork, "idempotency-worker", CancellationToken.None);
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
        var subscriptionCount = 0;
        for (var iteration = 0; iteration < 12 && subscriptionCount == 0; iteration++)
        {
            // Workflow Core advances its execution pointer one step at a time.
            await RunWorkflowAsync(queue, engine, "start-wait");
            await using var db = new PostgresContext(Connection, "wfc");
            subscriptionCount = await db.Set<WorkflowCore.Persistence.EntityFramework.Models.PersistedSubscription>()
                .CountAsync(x => x.WorkflowId == workflowId.ToString());
        }
        Assert.True(subscriptionCount > 0, "WaitFor should persist a subscription");

        // Persisting the subscription must enqueue a NEW event work item
        // for the previously received event, without a process restart.
        var replay = Assert.Single(await queue.ClaimAsync("event", "signal-replay", 1,
            TimeSpan.FromSeconds(30), 5, CancellationToken.None));
        Assert.Equal(eventId, replay.ItemId);
        await engine.ExecuteEventAsync(replay, "signal-replay", CancellationToken.None);
        WorkflowStatus? finalStatus = null;
        for (var iteration = 0; iteration < 12 && finalStatus != WorkflowStatus.Complete; iteration++)
        {
            await RunWorkflowAsync(queue, engine, "resume-wait");
            await using var verify = new PostgresContext(Connection, "wfc");
            finalStatus = await verify.Set<WorkflowCore.Persistence.EntityFramework.Models.PersistedWorkflow>()
                .Where(x => x.InstanceId == workflowId).Select(x => x.Status).SingleAsync();
        }
        Assert.Equal(WorkflowStatus.Complete, finalStatus);
        var timeline = await provider.GetRequiredService<ExecutionHistoryStore>().ReadAsync(workflowId);
        Assert.Contains(timeline, x => x.Event.EventType == "SignalDelivered");
        Assert.Contains(timeline, x => x.Event.EventType == "StepWaitingForEvent");
        Assert.Contains(timeline, x => x.Event.EventType == "WorkflowCompleted");
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

    public sealed class RetryData
    {
        public int Attempts { get; set; }
    }

    public sealed class RetryWorkflow : IWorkflow<RetryData>
    {
        public string Id => "history-retry";
        public int Version => 1;
        public void Build(IWorkflowBuilder<RetryData> builder) =>
            builder.StartWith<FailOnceStep>()
                .OnError(WorkflowErrorHandling.Retry, TimeSpan.Zero);
    }

    public sealed class FailOnceStep : StepBody
    {
        public override ExecutionResult Run(IStepExecutionContext context)
        {
            var state = (RetryData)context.Workflow.Data;
            if (state.Attempts++ == 0)
                throw new InvalidOperationException("Intentional test-only failure");
            return ExecutionResult.Next();
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
