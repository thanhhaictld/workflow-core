using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using WorkflowCore.Hub;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubRegistrationTests
{
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
    public async Task ConcurrentActivityPublicationUpdatesContractAndRetainsOperatorCapacities()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var hub = "registration-" + Guid.NewGuid().ToString("N");
        var initial = new HubActivityRegistration(hub, "activity", 1, 4, 10, 10,
            "tenant", "tenantId", 4, false, JsonSerializer.SerializeToElement(new { implementation = "old-build" }));
        Assert.True(await definitions.RegisterActivityAsync(initial, default));
        var revision = await definitions.UpdateActivityCapacityAsync(new(hub, "activity", 1, 1, 3, 2, 1), default);
        var updated = initial with { Partitions = 8, ResourceField = "accountId", StrictFifo = true,
            Contract = JsonSerializer.SerializeToElement(new { implementation = "new-build", inputs = new[] { "accountId" } }) };
        Assert.All(await Task.WhenAll(Enumerable.Range(0, 8)
            .Select(_ => definitions.RegisterActivityAsync(updated, default))), inserted => Assert.False(inserted));
        var published = Assert.Single(await definitions.ActivitiesAsync(default), a => a.HubId == hub);
        Assert.Equal(8, published.Partitions);
        Assert.Equal("accountId", published.ResourceField);
        Assert.True(published.StrictFifo);
        Assert.Equal("new-build", published.Contract!.Value.GetProperty("implementation").GetString());
        Assert.Equal("accountId", Assert.Single(published.Contract.Value.GetProperty("inputs").EnumerateArray()).GetString());
        var policy = (await definitions.GetActivityPolicyAsync(hub, "activity", 1, default))!;
        Assert.Equal(revision, policy.Revision);
        Assert.Equal(3, policy.HubConcurrency);
        Assert.Equal(2, policy.ActivityConcurrency);
        Assert.Equal(1, policy.ResourceCapacity);
    }
}
