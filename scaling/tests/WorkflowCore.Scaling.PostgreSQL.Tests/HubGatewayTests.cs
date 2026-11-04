using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>Real PostgreSQL acceptance tests for Hub-side admission and fencing.
/// CI provisions WFC_TEST_PG and applies scaling/sql/007_hub_gateway.sql.
/// </summary>
public sealed class HubGatewayTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    private static HubPolicyRegistry Registry(string hub, int capacity, bool fifo) =>
        new HubPolicyRegistry().Register(hub, "demo-activity", 1, 8, p =>
        {
            if (fifo)
                p.SequentialBy("resource", v => v.GetProperty("resourceId").GetString());
            else
                p.LimitBy("resource", capacity,
                    v => v.GetProperty("resourceId").GetString());
        });

    private static string Key() => "hub-test-" + Guid.NewGuid().ToString("N")[..12];
    private static JsonElement Payload(string id) =>
        JsonSerializer.SerializeToElement(new { resourceId = id });

    private static async Task<Guid> Create(HubTaskStore store, HubPolicyRegistry registry,
        string hub, string resource, string? requestKey = null)
    {
        var payload = Payload(resource);
        return await store.EnqueueAsync(registry.Resolve(hub, "demo-activity", 1, payload),
            payload.GetRawText(), null, requestKey, CancellationToken.None);
    }

    private static Task<IReadOnlyList<HubLeasedTask>> Claim(HubTaskStore store,
        string hub, string owner, int count = 16) =>
        store.ClaimAsync(owner, new[] { hub },
            new[] { new HubCapability(hub, "demo-activity", 1) },
            count, Lease, CancellationToken.None);

    [Fact]
    public async Task SequentialResource_AdmitsOnlyHead_AndBlocksDuringRetry()
    {
        if (Database is null) return;
        var hub = Key();
        var policy = Registry(hub, 1, fifo: true);
        await using var store = new HubTaskStore(Database, 3);
        var first = await Create(store, policy, hub, "same");
        var second = await Create(store, policy, hub, "same");
        var third = await Create(store, policy, hub, "same");

        var running = Assert.Single(await Claim(store, hub, "worker-A"));
        Assert.Equal(first, running.Id);
        Assert.Empty(await Claim(store, hub, "worker-B"));

        Assert.True(await store.CompleteAsync(first, "worker-A", running.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        var next = Assert.Single(await Claim(store, hub, "worker-B"));
        Assert.Equal(second, next.Id);
        Assert.True(await store.FailAsync(second, "worker-B", next.Epoch,
            "transient", TimeSpan.FromMinutes(1), CancellationToken.None));
        // Strict FIFO: a delayed retry cannot be bypassed by a newer task.
        Assert.Empty(await Claim(store, hub, "worker-C"));
        Assert.Equal("Ready", (await store.GetAsync(third, CancellationToken.None))!.State);
    }

    [Fact]
    public async Task LimitedConcurrency_IsClusterWide_WhileOtherResourcesProceed()
    {
        if (Database is null) return;
        var hub = Key();
        var policy = Registry(hub, 2, fifo: false);
        await using var store = new HubTaskStore(Database, 3);
        await Create(store, policy, hub, "same");
        await Create(store, policy, hub, "same");
        await Create(store, policy, hub, "same");
        await Create(store, policy, hub, "different");

        var claimed = await Claim(store, hub, "worker-A");
        Assert.Equal(3, claimed.Count);
        Assert.Equal(2, claimed.Count(x => x.Payload.GetProperty("resourceId").GetString() == "same"));
        Assert.Single(claimed.Where(x => x.Payload.GetProperty("resourceId").GetString() == "different"));
        Assert.Empty(await Claim(store, hub, "worker-B"));
        var one = claimed.First(x => x.Payload.GetProperty("resourceId").GetString() == "same");
        Assert.True(await store.CompleteAsync(one.Id, "worker-A", one.Epoch,
            "{\"completed\":true}", CancellationToken.None));
        Assert.Single(await Claim(store, hub, "worker-B"));
    }

    [Fact]
    public async Task CompletionIsIdempotent_AndRejectsStaleEpoch()
    {
        if (Database is null) return;
        var hub = Key();
        var policy = Registry(hub, 1, fifo: false);
        await using var store = new HubTaskStore(Database, 3);
        var id = await Create(store, policy, hub, "resource", "same-request");
        Assert.Equal(id, await Create(store, policy, hub, "resource", "same-request"));
        var first = Assert.Single(await Claim(store, hub, "worker-A"));

        // Force the previous lease to expire as if worker A disconnected.
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_task SET lease_until = now() - interval '1 second'
                WHERE id = @id
                """, db);
            cmd.Parameters.AddWithValue("id", id);
            await cmd.ExecuteNonQueryAsync();
        }
        var second = Assert.Single(await Claim(store, hub, "worker-B"));
        Assert.True(second.Epoch > first.Epoch);
        Assert.False(await store.CompleteAsync(id, "worker-A", first.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        Assert.True(await store.CompleteAsync(id, "worker-B", second.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        Assert.True(await store.CompleteAsync(id, "worker-B", second.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        Assert.Equal("Complete", (await store.GetAsync(id, CancellationToken.None))!.State);
    }

    [Fact]
    public async Task ConcurrentWorkerClaims_NeverExceedExclusivePermit()
    {
        if (Database is null) return;
        var hub = Key();
        var policy = Registry(hub, 1, fifo: false);
        await using var store = new HubTaskStore(Database, 8);
        for (var i = 0; i < 8; i++)
            await Create(store, policy, hub, "same");
        var claims = await Task.WhenAll(Enumerable.Range(0, 8)
            .Select(i => Claim(store, hub, "worker-" + i, 1)));
        Assert.Equal(1, claims.Sum(list => list.Count));
    }

    [Fact]
    public async Task GlobalExclusiveResource_SerializesAcrossBusinessHubs()
    {
        if (Database is null) return;
        var a = Key();
        var b = Key();
        var policy = new HubPolicyRegistry()
            .Register(a, "demo-activity", 1, 8, p => p
                .ExclusiveGloballyBy("shared-database", x => x.GetProperty("resourceId").GetString()))
            .Register(b, "demo-activity", 1, 8, p => p
                .ExclusiveGloballyBy("shared-database", x => x.GetProperty("resourceId").GetString()));
        await using var store = new HubTaskStore(Database, 4);
        await Create(store, policy, a, "database-one");
        await Create(store, policy, b, "database-one");
        Assert.Single(await Claim(store, a, "worker-A"));
        Assert.Empty(await Claim(store, b, "worker-B"));
    }

    [Fact]
    public void CodeFirstPolicy_RequiringResource_IsEnforcedBeforeEnqueue()
    {
        var registry = Registry(Key(), 1, fifo: true);
        var info = Assert.Single(registry.Describe());
        Assert.Contains("fifo=True", info.Rules[0]);
        Assert.Throws<ArgumentException>(() =>
            registry.Resolve(info.HubId, "demo-activity", 1,
                JsonSerializer.SerializeToElement(new { other = 1 })));
    }
    [Fact]
    public async Task ActivityCapacityUpdate_ChangesExistingResourcesAndPreservesLease()
    {
        if(Database is null)return;
        var hub=Key();
        await using var tasks=new HubTaskStore(Database,4);
        var definitions=new HubDefinitionStore(tasks);
        var contract=JsonSerializer.SerializeToElement(new {schema="v1"});
        var registration=new HubActivityRegistration(hub,"demo-activity",2,8,4,3,
            "tenant","resourceId",3,false,contract);
        Assert.True(await definitions.RegisterActivityAsync(registration,CancellationToken.None));
        var first=await EnqueuePublished(tasks,definitions,hub,"same");
        var second=await EnqueuePublished(tasks,definitions,hub,"same");
        var third=await EnqueuePublished(tasks,definitions,hub,"same");
        var active=Assert.Single(await tasks.ClaimAsync("worker-A",[hub],
            [new(hub,"demo-activity",2)],1,Lease,CancellationToken.None));
        Assert.Equal(first,active.Id);
        var before=await definitions.GetActivityPolicyAsync(hub,"demo-activity",2,CancellationToken.None);
        Assert.NotNull(before);
        var revised=await definitions.UpdateActivityCapacityAsync(
            new(hub,"demo-activity",2,before.Revision,1,1,1),CancellationToken.None);
        Assert.Equal(before.Revision+1,revised);
        Assert.Empty(await tasks.ClaimAsync("worker-B",[hub],
            [new(hub,"demo-activity",2)],3,Lease,CancellationToken.None));
        Assert.Equal("Leased",(await tasks.GetAsync(first,CancellationToken.None))!.State);
        Assert.True(await tasks.CompleteAsync(first,"worker-A",active.Epoch,
            "{\"ok\":true}",CancellationToken.None));
        var next=Assert.Single(await tasks.ClaimAsync("worker-B",[hub],
            [new(hub,"demo-activity",2)],3,Lease,CancellationToken.None));
        Assert.Equal(second,next.Id);
        var current=await definitions.GetActivityPolicyAsync(hub,"demo-activity",2,CancellationToken.None);
        Assert.NotNull(current);
        Assert.Equal(1,current.ActivityConcurrency);
        Assert.Equal(1,current.ResourceCapacity);
        Assert.Equal(revised,current.Revision);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>definitions.UpdateActivityCapacityAsync(
            new(hub,"demo-activity",2,before.Revision,3,3,3),CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            definitions.RegisterActivityAsync(registration,CancellationToken.None));
    }

    [Fact]
    public async Task ActivityCapacityUpdate_ConcurrentExpectedRevisionsOnlyOneWins()
    {
        if(Database is null)return;
        var hub=Key();
        await using var tasks=new HubTaskStore(Database,4);
        var definitions=new HubDefinitionStore(tasks);
        var registration=new HubActivityRegistration(hub,"demo-activity",2,8,8,8);
        Assert.True(await definitions.RegisterActivityAsync(registration,CancellationToken.None));
        var requests=new[]
        {
            new HubActivityCapacityUpdate(hub,"demo-activity",2,1,7,7,1),
            new HubActivityCapacityUpdate(hub,"demo-activity",2,1,6,6,1)
        };
        var outcomes=await Task.WhenAll(requests.Select(async request=>
        {
            try {await definitions.UpdateActivityCapacityAsync(request,CancellationToken.None);return true;}
            catch(InvalidOperationException){return false;}
        }));
        Assert.Single(outcomes.Where(x=>x));
        Assert.Equal(2,(await definitions.GetActivityPolicyAsync(
            hub,"demo-activity",2,CancellationToken.None))!.Revision);
    }

    private static async Task<Guid> EnqueuePublished(HubTaskStore tasks,HubDefinitionStore definitions,
        string hub,string resource)
    {
        var payload=Payload(resource);
        var route=await definitions.ResolveAsync(hub,"demo-activity",2,payload,CancellationToken.None);
        Assert.NotNull(route);
        return await tasks.EnqueueAsync(route,payload.GetRawText(),null,null,CancellationToken.None);
    }

}
