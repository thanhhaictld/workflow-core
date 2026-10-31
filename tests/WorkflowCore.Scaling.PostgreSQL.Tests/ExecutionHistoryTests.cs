using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Persistence.PostgreSQL;
using WorkflowCore.Scaling.PostgreSQL;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class ExecutionHistoryTests
{
    private static string Connection => Environment.GetEnvironmentVariable("WFC_TEST_PG")
        ?? throw new InvalidOperationException("WFC_TEST_PG must be configured");

    private static ServiceProvider BuildHost()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDistributedWorkflowCore(Connection);
        services.AddTransient<SampleStep>();
        var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IWorkflowHost>().RegisterWorkflow<HistoryWorkflow>();
        return provider;
    }

    private static async Task ExecuteOneAsync(DistributedWorkflowEngine engine,
        FencedWorkQueue queue, string owner, int maxAttempts = 5)
    {
        var work = Assert.Single(await queue.ClaimAsync("workflow", owner, 1,
            TimeSpan.FromSeconds(30), maxAttempts, CancellationToken.None));
        var fence = await queue.AcquireWorkflowAsync(work.ItemId, owner,
            TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(fence);
        try { await engine.ExecuteWorkflowAsync(work, fence!, owner, CancellationToken.None); }
        finally { await queue.ReleaseGateAsync(fence!, CancellationToken.None); }
    }

    [Fact]
    public async Task LifecycleHistoryIsDurableAndPagedWithoutPayloads()
    {
        await using (var db = new PostgresContext(Connection, "wfc"))
            await db.Database.MigrateAsync();
        using var provider = BuildHost();
        var engine = provider.GetRequiredService<DistributedWorkflowEngine>();
        var queue = provider.GetRequiredService<FencedWorkQueue>();
        var history = provider.GetRequiredService<ExecutionHistoryReader>();
        var id = await engine.StartAsync("audit-smoke", new { Password = "history-redaction-test" });

        var before = await history.ReadAsync(id, limit: 1);
        Assert.Single(before);
        Assert.Equal("WorkflowStarted", before[0].EventType);

        await ExecuteOneAsync(engine, queue, "history-test");
        var entries = await history.ReadAsync(id);
        Assert.Contains(entries, e => e.EventType == "WorkflowStarted");
        Assert.Contains(entries, e => e.EventType == "StepScheduled");
        Assert.Contains(entries, e => e.EventType == "StepAttemptDispatched");
        Assert.Contains(entries, e => e.EventType == "StepAttemptCommitted");
        Assert.Contains(entries, e => e.EventType == "StepCompleted");
        Assert.Contains(entries, e => e.EventType == "WorkflowStatusChanged");
        Assert.DoesNotContain("history-redaction-test", string.Join("", entries.Select(e => e.Details)));
        Assert.Equal(entries.Select(e => e.Id).OrderBy(x => x), entries.Select(e => e.Id));

        var page = await history.ReadAsync(id, afterId: before[0].Id, limit: 2);
        Assert.Equal(entries.Skip(1).Take(2).Select(e => e.Id), page.Select(e => e.Id));
    }

    [Fact]
    public async Task RetryJournalIsAtomicWithQueueStateAndReclaimIsVisible()
    {
        await using (var db = new PostgresContext(Connection, "wfc"))
            await db.Database.MigrateAsync();
        using var provider = BuildHost();
        var engine = provider.GetRequiredService<DistributedWorkflowEngine>();
        var queue = provider.GetRequiredService<FencedWorkQueue>();
        var history = provider.GetRequiredService<ExecutionHistoryReader>();
        var id = await engine.StartAsync("audit-smoke");
        var claimed = Assert.Single(await queue.ClaimAsync("workflow", "history-retry", 1,
            TimeSpan.FromSeconds(30), 5, CancellationToken.None));
        Assert.Equal(id, claimed.ItemId);
        Assert.True(await queue.RetryAsync(claimed, "history-retry",
            TimeSpan.Zero, 5, "sensitive-api-key-must-not-appear", CancellationToken.None));
        var pending = await history.ReadAsync(id);
        Assert.Single(pending.Where(x => x.EventType == "WorkRetryScheduled"));
        Assert.DoesNotContain("sensitive-api-key-must-not-appear",
            string.Join("", pending.Select(e => e.Details)));

        await ExecuteOneAsync(engine, queue, "history-reclaim");
        var entries = await history.ReadAsync(id);
        Assert.Contains(entries, x => x.EventType == "StepAttemptDispatched" && x.Attempt == 2);
    }

    [Fact]
    public async Task ReleasedFenceCannotRecordAttemptOrExecute()
    {
        await using (var db = new PostgresContext(Connection, "wfc"))
            await db.Database.MigrateAsync();
        using var provider = BuildHost();
        var engine = provider.GetRequiredService<DistributedWorkflowEngine>();
        var queue = provider.GetRequiredService<FencedWorkQueue>();
        var history = provider.GetRequiredService<ExecutionHistoryReader>();
        var id = await engine.StartAsync("audit-smoke");
        var work = Assert.Single(await queue.ClaimAsync("workflow", "stale-test", 1,
            TimeSpan.FromSeconds(30), 5, CancellationToken.None));
        var gate = await queue.AcquireWorkflowAsync(id, "stale-test",
            TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(gate);
        await queue.ReleaseGateAsync(gate!, CancellationToken.None);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            engine.ExecuteWorkflowAsync(work, gate!, "stale-test", CancellationToken.None));
        Assert.DoesNotContain(await history.ReadAsync(id), x => x.EventType == "StepAttemptDispatched");

        var replacement = await queue.AcquireWorkflowAsync(id, "stale-test",
            TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(replacement);
        try { await engine.ExecuteWorkflowAsync(work, replacement!, "stale-test", CancellationToken.None); }
        finally { await queue.ReleaseGateAsync(replacement!, CancellationToken.None); }
    }

    public sealed class HistoryWorkflow : IWorkflow
    {
        public string Id => "audit-smoke";
        public int Version => 1;
        public void Build(IWorkflowBuilder<object> builder) => builder.StartWith<SampleStep>();
    }

    public sealed class SampleStep : StepBody
    {
        public override ExecutionResult Run(IStepExecutionContext context) => ExecutionResult.Next();
    }
}
