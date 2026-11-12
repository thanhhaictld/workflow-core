using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using WorkflowCore.Models;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubSnapshotContractReuseTests
{
    [Fact]
    public async Task ReusedContracts_PreserveWireFormatAndValuesAcrossConcurrentOperations()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddHubWorkflowRuntime()
            .AddWorkflow<HubNativeExecutionTests.ObjectWorkflow, object>("codec");
        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<HubNativeCatalog>();
        var original = new HubSnapshotCodec(catalog, reuseContracts: false);
        var reused = new HubSnapshotCodec(catalog, reuseContracts: true);
        var workflow = new WorkflowInstance
        {
            Id = Guid.NewGuid().ToString(), WorkflowDefinitionId = "object", Version = 1,
            Data = new HubNativeExecutionTests.Data { Value = 42 },
            Status = WorkflowStatus.Runnable, CreateTime = DateTime.UtcNow
        };
        workflow.ExecutionPointers.Add(new ExecutionPointer
        {
            Id = "pointer", StepId = 0, Active = true, PersistenceData = new[] { 1, 2, 3 },
            ExtensionAttributes = new Dictionary<string, object> { ["number"] = 42, ["flag"] = true }
        });
        var wire = original.Encode(workflow);
        await Task.WhenAll(Enumerable.Range(0, 64).Select(i => Task.Run(() =>
        {
            var decoded = reused.Decode<WorkflowInstance>(wire);
            Assert.Equal(42, Assert.IsType<HubNativeExecutionTests.Data>(decoded.Data).Value);
            Assert.Equal(new[] { 1, 2, 3 }, Assert.IsType<int[]>(decoded.ExecutionPointers.Single().PersistenceData));
            Assert.Equal(wire.GetRawText(), reused.Encode(decoded).GetRawText());
            Assert.Equal(i, Assert.IsType<int>(original.DecodeValue(reused.EncodeValue(i))));
            Assert.Equal(i, Assert.IsType<int>(reused.DecodeValue(original.EncodeValue(i))));
        })));
    }

    [Fact]
    public void ReusedContracts_KeepCatalogAllowListsSeparateAfterWarmup()
    {
        var registered = new ServiceCollection().AddLogging();
        registered.AddHubWorkflowRuntime()
            .AddWorkflow<HubNativeExecutionTests.ObjectWorkflow, object>("codec");
        using var allowed = registered.BuildServiceProvider();
        var empty = new ServiceCollection().AddLogging();
        empty.AddHubWorkflowRuntime();
        using var restricted = empty.BuildServiceProvider();
        var first = new HubSnapshotCodec(allowed.GetRequiredService<HubNativeCatalog>(), true);
        var second = new HubSnapshotCodec(restricted.GetRequiredService<HubNativeCatalog>(), true);
        var encoded = first.EncodeValue(new HubNativeExecutionTests.Data { Value = 1 });
        Assert.IsType<HubNativeExecutionTests.Data>(first.DecodeValue(encoded));
        Assert.Equal(42, Assert.IsType<int>(second.DecodeValue(second.EncodeValue(42))));
        Assert.Throws<JsonSerializationException>(() => second.DecodeValue(encoded));
        Assert.Throws<JsonSerializationException>(() => second.EncodeValue(new HubNativeExecutionTests.Data()));
        var malicious = System.Text.Json.JsonSerializer.SerializeToElement(new Dictionary<string, object>
        {
            ["$hubValueFormat"] = "v1",
            ["NativeValue"] = new { clrAssembly = "System.Diagnostics.Process", clrType = "System.Diagnostics.Process", value = new { } }
        });
        Assert.Throws<JsonSerializationException>(() => first.DecodeValue(malicious));
        Assert.Throws<JsonSerializationException>(() => first.EncodeValue(typeof(string)));
    }
}
