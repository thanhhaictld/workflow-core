using System.Text.Json;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Real DB checks of multi-step Hub workflow execution, rather than merely
/// asserting that unrelated Hub activities can complete.
/// </summary>
public sealed class HubWorkflowE2ETests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");
    private static string NewHub() => "workflow-test-" + Guid.NewGuid().ToString("N")[..10];

    private static (HubTaskStore Tasks, HubWorkflowStore Workflows) Create(string hub)
    {
        var tasks = new HubTaskStore(Database!, 5);
        var policies = new HubPolicyRegistry()
            .Register(hub, "validate", 1, 8, p => p
                .SequentialBy("tenant", v => v.GetProperty("tenantId").GetString()))
            .Register(hub, "provision", 1, 8, p => p
                .SequentialBy("tenant", v => v.GetProperty("tenantId").GetString()));
        var definitions = new HubWorkflowRegistry()
            .Register(hub, "onboard", 1, 8, b => b
                .Activity("validate").Then("provision"));
        return (tasks, new HubWorkflowStore(tasks, policies, definitions));
    }

    private static HubWorkflowStartRequest Request(string hub, string tenant,
        string? key = null, string? runId = null) =>
        new(hub, "onboard", 1,
            JsonSerializer.SerializeToElement(new { tenantId = tenant }), key, runId);

    private static async Task<HubLeasedTask> ClaimOne(HubTaskStore tasks, string hub, string worker)
    {
        var candidates = await tasks.ClaimAsync(worker, new[] { hub },
            new[] { new HubCapability(hub, "validate", 1),
                    new HubCapability(hub, "provision", 1) },
            8, TimeSpan.FromSeconds(30), CancellationToken.None);
        return Assert.Single(candidates);
    }

    [Fact]
    public async Task Start_CompleteAndTransitionTwoSteps_Atomically()
    {
        if (Database is null) return;
        var hub = NewHub();
        var runId = "run-" + Guid.NewGuid().ToString("N");
        var (tasks, workflows) = Create(hub);
        await using (tasks)
        {
            var started = await workflows.StartAsync(Request(hub, "tenant-a", "request-1", runId),
                CancellationToken.None);
            var repeated = await workflows.StartAsync(Request(hub, "tenant-a", "request-1", runId),
                CancellationToken.None);
            Assert.Equal(started, repeated);
            var initial = await workflows.GetAsync(started, CancellationToken.None);
            Assert.NotNull(initial);
            Assert.Equal("Ready", initial.State);
            Assert.Equal(2, initial.StepCount);

            var first = await ClaimOne(tasks, hub, "worker-1");
            Assert.Equal(0, (await workflows.GetAsync(started, CancellationToken.None))!.NextStep);
            Assert.Equal("validate", first.TaskType);
            Assert.True(await workflows.CompleteAsync(first.Id, "worker-1", first.Epoch,
                """{"validated":true}""", CancellationToken.None));
            Assert.True(await workflows.CompleteAsync(first.Id, "worker-1", first.Epoch,
                """{"validated":true}""", CancellationToken.None)); // lost HTTP response
            var running = await workflows.GetAsync(started, CancellationToken.None);
            Assert.Equal("Running", running!.State);
            Assert.Equal(1, running.NextStep);

            var second = await ClaimOne(tasks, hub, "worker-2");
            Assert.Equal("provision", second.TaskType);
            Assert.True(await workflows.CompleteAsync(second.Id, "worker-2", second.Epoch,
                """{"provisioned":true}""", CancellationToken.None));
            var done = await workflows.GetAsync(started, CancellationToken.None);
            Assert.Equal("Complete", done!.State);
            Assert.Equal(2, done.NextStep);
            Assert.True(done.Results.GetProperty("0").GetProperty("validated").GetBoolean());
            Assert.True(done.Results.GetProperty("1").GetProperty("provisioned").GetBoolean());
            var history = await workflows.HistoryAsync(started, CancellationToken.None);
            Assert.Equal(new[] { 0, 1 }, history.Select(e => e.StepIndex).ToArray());
            Assert.All(history, e => Assert.Equal("StepCompleted", e.EventType));
            var summary = await workflows.SummaryAsync(runId, CancellationToken.None);
            Assert.Equal(1, summary.Total);
            Assert.Equal(1, summary.Completed);
            Assert.True(summary.P95CompletionMs.HasValue);
        }
    }

    [Fact]
    public async Task SequentialResource_BlocksOtherWorkflowUntilItsStepsFinish()
    {
        if (Database is null) return;
        var hub = NewHub();
        var (tasks, workflows) = Create(hub);
        await using (tasks)
        {
            var a = await workflows.StartAsync(Request(hub, "same-tenant"), CancellationToken.None);
            var b = await workflows.StartAsync(Request(hub, "same-tenant"), CancellationToken.None);
            var first = await ClaimOne(tasks, hub, "worker-1");
            Assert.Equal(a, first.WorkflowId);
            Assert.True(await workflows.CompleteAsync(first.Id, "worker-1", first.Epoch,
                """{"validated":true}""", CancellationToken.None));
            var next = await ClaimOne(tasks, hub, "worker-1");
            Assert.Equal(a, next.WorkflowId);
            Assert.True(await workflows.CompleteAsync(next.Id, "worker-1", next.Epoch,
                """{"provisioned":true}""", CancellationToken.None));
            var other = await ClaimOne(tasks, hub, "worker-2");
            Assert.Equal(b, other.WorkflowId);
        }
    }

    [Fact]
    public async Task Retry_HigherLeaseEpochRejectsStaleCompletion()
    {
        if (Database is null) return;
        var hub = NewHub();
        var (tasks, workflows) = Create(hub);
        await using (tasks)
        {
            var id = await workflows.StartAsync(Request(hub, "tenant-retry"), CancellationToken.None);
            var first = await ClaimOne(tasks, hub, "worker-a");
            Assert.True(await workflows.FailAsync(first.Id, "worker-a", first.Epoch,
                "temporary", TimeSpan.Zero, CancellationToken.None));
            Assert.False(await workflows.CompleteAsync(first.Id, "worker-a", first.Epoch,
                """{"stale":true}""", CancellationToken.None));
            var retry = await ClaimOne(tasks, hub, "worker-b");
            Assert.Equal(first.Id, retry.Id);
            Assert.True(retry.Epoch > first.Epoch);
            Assert.True(await workflows.CompleteAsync(retry.Id, "worker-b", retry.Epoch,
                """{"ok":true}""", CancellationToken.None));
            var second = await ClaimOne(tasks, hub, "worker-b");
            Assert.True(await workflows.CompleteAsync(second.Id, "worker-b", second.Epoch,
                """{"ok":true}""", CancellationToken.None));
            Assert.Equal("Complete", (await workflows.GetAsync(id, CancellationToken.None))!.State);
        }
    }
}
