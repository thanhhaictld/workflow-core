using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateReaderTests
{
    [Fact]
    public void MetadataEnrichmentGateIsIndependentAndOffByDefault()
    {
        Assert.False(HubCandidateReader.ParseMetadataEnrichmentFlag(null));
        Assert.False(HubCandidateReader.ParseMetadataEnrichmentFlag("false"));
        Assert.False(HubCandidateReader.ParseMetadataEnrichmentFlag("0"));
        Assert.True(HubCandidateReader.ParseMetadataEnrichmentFlag("true"));
        Assert.True(HubCandidateReader.ParseMetadataEnrichmentFlag("1"));
    }

    private sealed class CountingDiscovery(IHubCandidateDiscovery inner) : IHubCandidateDiscovery
    {
        public int PageCalls;
        public int HydrationCalls;
        public Task<HubCandidateHorizon> CaptureHorizonAsync(string hub, CancellationToken ct) => inner.CaptureHorizonAsync(hub, ct);
        public async Task<HubCandidatePage> ReadPageAsync(string hub, int bucket, HubCandidateStream stream,
            HubCandidateHorizon horizon, HubCandidateCursor? cursor, int pageSize, int byteLimit, CancellationToken ct)
        { Interlocked.Increment(ref PageCalls); return await inner.ReadPageAsync(hub, bucket, stream, horizon, cursor, pageSize, byteLimit, ct); }
        public async Task<HubCandidateHydrationPage> HydrateAsync(string hub, int matchingPartition, int partitionCount,
            IReadOnlyList<Guid> ids, int byteLimit, CancellationToken ct)
        { Interlocked.Increment(ref HydrationCalls); return await inner.HydrateAsync(hub, matchingPartition, partitionCount, ids, byteLimit, ct); }
    }

    [Fact]
    public async Task OversizedResourcesUseUnknownConstraintFallbackWithoutLeasing()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate reader tests");
        var hub = "candidate-large-" + Guid.NewGuid().ToString("N");
        var id = Guid.NewGuid(); var key = new string('x', 512) + id;
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@id,@hub,0,'activity',1,'{}');
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,false);
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@id,@key);
            """, db))
        {
            command.Parameters.AddWithValue("id", id); command.Parameters.AddWithValue("hub", hub);
            command.Parameters.AddWithValue("key", key); await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks, metadataEnrichmentEnabled: true);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var small = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Ready, horizon, null, 1, 256, default);
        var fallback = Assert.Single(small.Items);
        Assert.Equal(id, fallback.Id);
        Assert.False(fallback.ResourceKeysComplete);
        Assert.Empty(fallback.ResourceKeys);
        Assert.NotNull(small.Next);
        var page = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Ready, horizon, null, 1, 2048, default);
        var item = Assert.Single(page.Items);
        Assert.Equal(id, item.Id);
        Assert.Equal(key, Assert.Single(item.ResourceKeys));
        Assert.True(item.ResourceKeysComplete);
        Assert.True(item.EnrichmentLoaded);
        Assert.Equal(0, item.Epoch);
        var enriched = Assert.Single((await reader.HydrateAsync(hub, 0, 64, [id], 2048, default)).Items);
        Assert.Equal(key, Assert.Single(enriched.ResourceKeys));
        Assert.True(enriched.ResourceKeysComplete);
    }

    [Fact]
    public async Task CandidatePageRemainsEnrichedForEitherLegacyFlagValue()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate reader tests");
        var hub = "metadata-gate-" + Guid.NewGuid().ToString("N"); var id = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@id,@hub,0,'activity',1,'{}')
            """, db))
        { command.Parameters.AddWithValue("id", id); command.Parameters.AddWithValue("hub", hub); await command.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database);
        var horizon = await new HubCandidateReader(tasks, metadataEnrichmentEnabled: false).CaptureHorizonAsync(hub, default);
        var baselineReader = new HubCandidateReader(tasks, metadataEnrichmentEnabled: false);
        var enabledReader = new HubCandidateReader(tasks, metadataEnrichmentEnabled: true);
        Assert.True(baselineReader.MetadataEnrichmentEnabled);
        Assert.True(enabledReader.MetadataEnrichmentEnabled);
        var baseline = Assert.Single((await baselineReader.ReadPageAsync(hub, 0, HubCandidateStream.Ready,
            horizon, null, 8, 4096, default)).Items);
        var enabled = Assert.Single((await enabledReader.ReadPageAsync(hub, 0, HubCandidateStream.Ready,
            horizon, null, 8, 4096, default)).Items);
        Assert.Equal(id, baseline.Id); Assert.True(baseline.EnrichmentLoaded);
        Assert.Equal(id, enabled.Id); Assert.True(enabled.EnrichmentLoaded);
    }

    [Fact]
    public async Task HydrationLoadsOnlyRequestedCurrentEligibleIdsAndKeepsOversizedConstraintsUnknown()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate hydration tests");
        var hub = "candidate-hydrate-" + Guid.NewGuid().ToString("N");
        var eligible = Guid.NewGuid(); var terminal = Guid.NewGuid(); var otherPartition = Guid.NewGuid();
        var key = new string('k', 512) + eligible;
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@eligible,@hub,0,'activity',1,'{}'),(@terminal,@hub,0,'activity',1,'{}'),
                   (@other,@hub,1,'activity',1,'{}');
            UPDATE wfc_scale.hub_task SET state=2 WHERE id=@terminal;
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,false);
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@eligible,@key);
            """, db))
        {
            command.Parameters.AddWithValue("eligible", eligible); command.Parameters.AddWithValue("terminal", terminal);
            command.Parameters.AddWithValue("other", otherPartition); command.Parameters.AddWithValue("hub", hub);
            command.Parameters.AddWithValue("key", key); await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks);
        var page = await reader.HydrateAsync(hub, 0, 64, [eligible, terminal, otherPartition], 256, default);
        var item = Assert.Single(page.Items);
        Assert.Equal(eligible, item.Id);
        Assert.False(item.ResourceKeysComplete);
        Assert.Empty(item.ResourceKeys);
        Assert.Equal(0, page.Oversized);
    }

    [Fact]
    public async Task HydrationEnforcesCombinedUtf8BudgetAndSuppressesOversizedBaseText()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate hydration tests");
        var hub = "candidate-hydrate-budget-" + Guid.NewGuid().ToString("N");
        var combined = Guid.NewGuid(); var oversized = Guid.NewGuid(); var unicode = Guid.NewGuid();
        var combinedKey = new string('k', 220) + "-" + combined;
        var unicodeKey = string.Concat(Enumerable.Repeat("🧪", 70)) + unicode;
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@combined,@hub,0,@base,1,'{}'),(@oversized,@hub,0,@large,1,'{}'),(@unicode,@hub,0,'activity',1,'{}');
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
            VALUES (@combinedKey,1,false),(@unicodeKey,1,false);
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
            VALUES (@combined,@combinedKey),(@unicode,@unicodeKey);
            """, db))
        {
            command.Parameters.AddWithValue("combined", combined); command.Parameters.AddWithValue("oversized", oversized);
            command.Parameters.AddWithValue("unicode", unicode); command.Parameters.AddWithValue("hub", hub);
            command.Parameters.AddWithValue("base", new string('b', 180));
            command.Parameters.AddWithValue("large", new string('z', 4096));
            command.Parameters.AddWithValue("combinedKey", combinedKey); command.Parameters.AddWithValue("unicodeKey", unicodeKey);
            await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks, metadataEnrichmentEnabled: true);
        var page = await reader.HydrateAsync(hub, 0, 64, [combined, combined], 512, default);
        var combinedItem = Assert.Single(page.Items);
        Assert.Equal(combined, combinedItem.Id);
        Assert.False(combinedItem.ResourceKeysComplete);
        Assert.Empty(combinedItem.ResourceKeys);
        Assert.InRange(page.MetadataBytes, 0, 512);
        var unicodePage = await reader.HydrateAsync(hub, 0, 64, [unicode], 512, default);
        Assert.False(Assert.Single(unicodePage.Items).ResourceKeysComplete);
        Assert.Empty(unicodePage.Items[0].ResourceKeys);
        Assert.InRange(unicodePage.MetadataBytes, 0, 512);
        var oversizedPage = await reader.HydrateAsync(hub, 0, 64, [oversized], 512, default);
        Assert.Empty(oversizedPage.Items);
        Assert.Equal(1, oversizedPage.Oversized);

        // A minimum legal byte budget with duplicate requested IDs is still
        // bounded and does not duplicate result rows.
        var minimum = await reader.HydrateAsync(hub, 0, 64, [unicode, unicode], 256, default);
        Assert.Equal(unicode, Assert.Single(minimum.Items).Id);
        Assert.InRange(minimum.MetadataBytes, 0, 256);
    }

    [Fact]
    public async Task HydrationBatchesSharedWorkflowMetadataAndKeepsMissingOrOversizedPinsUnknown()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate workflow metadata tests");
        var hub = "candidate-workflow-meta-" + Guid.NewGuid().ToString("N");
        var workflow = Guid.NewGuid(); var normalA = Guid.NewGuid(); var normalB = Guid.NewGuid();
        var largeWorkflow = Guid.NewGuid(); var largeTask = Guid.NewGuid(); var missingTask = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count)
            VALUES (@workflow,@hub,'shared-flow',3,0,'{}',1),
                   (@largeWorkflow,@hub,@largeDefinition,7,0,'{}',1);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,workflow_id)
            VALUES (@a,@hub,0,'__workflow__',1,'{}',@workflow),
                   (@b,@hub,0,'__workflow__',1,'{}',@workflow),
                   (@largeTask,@hub,0,'__workflow__',1,'{}',@largeWorkflow),
                   (@missingTask,@hub,0,'__workflow__',1,'{}',@missingWorkflow);
            """, db))
        {
            command.Parameters.AddWithValue("workflow", workflow); command.Parameters.AddWithValue("hub", hub);
            command.Parameters.AddWithValue("largeWorkflow", largeWorkflow); command.Parameters.AddWithValue("largeDefinition", new string('d', 5000));
            command.Parameters.AddWithValue("a", normalA); command.Parameters.AddWithValue("b", normalB);
            command.Parameters.AddWithValue("largeTask", largeTask); command.Parameters.AddWithValue("missingTask", missingTask);
            command.Parameters.AddWithValue("missingWorkflow", Guid.NewGuid());
            await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks, workflowCacheEntriesPerPartition: 1,
            workflowCacheEntriesPerProcess: 2, workflowCacheBytesPerPartition: 1024,
            workflowCacheBytesPerProcess: 2048);
        var shared = await reader.HydrateAsync(hub, 0, 64, [normalA, normalB], 8192, default);
        Assert.Equal(2, shared.Items.Count);
        Assert.All(shared.Items, item =>
        {
            Assert.True(item.WorkflowMetadataComplete);
            Assert.Equal("shared-flow", item.DefinitionId);
            Assert.Equal(3, item.DefinitionVersion);
        });
        var afterBatch = reader.WorkflowCacheSnapshot;
        Assert.Equal(1, afterBatch.Entries);
        Assert.Equal(1, afterBatch.Misses); // one workflow row for two tasks
        Assert.Equal(0, afterBatch.Hits);

        var warm = Assert.Single((await reader.HydrateAsync(hub, 0, 64, [normalA], 4096, default)).Items);
        Assert.True(warm.WorkflowMetadataComplete);
        Assert.Equal("shared-flow", warm.DefinitionId);
        Assert.Equal(1, reader.WorkflowCacheSnapshot.Hits);

        var oversized = Assert.Single((await reader.HydrateAsync(hub, 0, 64, [largeTask], 4096, default)).Items);
        Assert.False(oversized.WorkflowMetadataComplete);
        Assert.Null(oversized.DefinitionId);
        var missing = Assert.Single((await reader.HydrateAsync(hub, 0, 64, [missingTask], 4096, default)).Items);
        Assert.False(missing.WorkflowMetadataComplete);
        Assert.Null(missing.DefinitionId);
        var bounded = reader.WorkflowCacheSnapshot;
        Assert.InRange(bounded.Entries, 0, 1);
        Assert.InRange(bounded.Bytes, 0, 1024);
        Assert.True(bounded.Evictions >= 2);
    }

    [Fact]
    public async Task RepeatedIndexedDiscoveryReusesEnrichedPageWithoutExtraHydration()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate enrichment reuse tests");
        var hub = "candidate-enrichment-reuse-" + Guid.NewGuid().ToString("N");
        var id = Guid.NewGuid(); var key = hub + ":scope";
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@id,@hub,0,'activity',1,'{}');
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,false);
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@id,@key);
            """, db))
        {
            command.Parameters.AddWithValue("id", id); command.Parameters.AddWithValue("hub", hub);
            command.Parameters.AddWithValue("key", key); await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database);
        var discovery = new CountingDiscovery(new HubCandidateReader(tasks, metadataEnrichmentEnabled: true));
        var options = new HubCandidateCoordinatorOptions(PageSize: 1, PageWireBytes: 4096, PartitionEntries: 8,
            ProcessEntries: 16, PartitionBytes: 65536, ProcessBytes: 131072, PartitionCredits: 4,
            ProcessCredits: 8, ProcessPartitions: 2, RecoveryCadence: TimeSpan.FromSeconds(10));
        using var registry = new HubPartitionCandidateRegistry(discovery, options);
        var owner = new HubMatchingPartitionLease(hub, 0, "owner", "http://localhost:5100", 1,
            DateTime.UtcNow.AddMinutes(5));
        registry.StartGeneration(owner, 64);
        for (var turn = 0; turn < 3; turn++) await registry.RunOneTurnAsync(DateTimeOffset.UtcNow, default);
        Assert.Equal(0, discovery.HydrationCalls);
        Assert.Equal(1, registry.Snapshot().Entries);
        registry.Notify(hub, 0, owner.Epoch);
        for (var turn = 0; turn < 3; turn++) await registry.RunOneTurnAsync(DateTimeOffset.UtcNow, default);
        Assert.True(discovery.PageCalls >= 4);
        Assert.Equal(0, discovery.HydrationCalls);
        Assert.Equal(1, registry.Snapshot().Entries);
    }

    [Fact]
    public async Task LongUtf8BaseMetadataProgressesAcrossReadyDueAndExpiredStreams()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate page byte fairness tests");
        var hub = "candidate-long-" + new string('h', 80) + Guid.NewGuid().ToString("N");
        var longType = new string('t', 80); var unicodeType = string.Concat(Enumerable.Repeat("🧪", 40));
        var readyIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var future = Guid.NewGuid(); var expired = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@a,@hub,0,@longType,1,'{}'),(@b,@hub,0,@unicodeType,1,'{}'),(@c,@hub,0,'activity',1,'{}');
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,due_at)
            VALUES (@future,@hub,0,@longType,1,'{}',clock_timestamp()+interval '1 hour');
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,lease_until)
            VALUES (@expired,@hub,0,@unicodeType,1,'{}',1,'prior-worker',clock_timestamp()-interval '1 hour');
            """, db))
        {
            command.Parameters.AddWithValue("hub", hub); command.Parameters.AddWithValue("longType", longType);
            command.Parameters.AddWithValue("unicodeType", unicodeType); command.Parameters.AddWithValue("a", readyIds[0]);
            command.Parameters.AddWithValue("b", readyIds[1]); command.Parameters.AddWithValue("c", readyIds[2]);
            command.Parameters.AddWithValue("future", future); command.Parameters.AddWithValue("expired", expired);
            await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks, metadataEnrichmentEnabled: true);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        async Task<HubCandidatePage> Drain(HubCandidateStream stream)
        {
            var rows = new List<HubTaskCandidate>(); var oversized = new List<HubCandidateOversized>();
            HubCandidateCursor? cursor = null; var metadataBytes = 0;
            do
            {
                var page = await reader.ReadPageAsync(hub, 0, stream, horizon, cursor, 256, 65536, default);
                rows.AddRange(page.Items); oversized.AddRange(page.Oversized ?? []); cursor = page.Next;
                metadataBytes += page.MetadataBytes;
                if (page.EndOfStream) return new(rows, cursor, true, metadataBytes, oversized);
            } while (rows.Count < 16);
            throw new InvalidOperationException("Bounded page traversal did not reach the end of the stream");
        }
        var ready = await Drain(HubCandidateStream.Ready);
        Assert.Equal(readyIds.Order().ToArray(), ready.Items.Select(x => x.Id).Order().ToArray());
        Assert.Empty(ready.Oversized!);
        Assert.InRange(ready.MetadataBytes, 0, 65536);
        var due = await Drain(HubCandidateStream.ReadyDue);
        Assert.Equal(readyIds.Order().ToArray(), due.Items.Select(x => x.Id).Order().ToArray());
        Assert.Empty(due.Oversized!);
        Assert.InRange(due.MetadataBytes, 0, 65536);
        var hydrated = await reader.HydrateAsync(hub, 0, 64, due.Items.Select(x => x.Id).ToArray(), 65536, default);
        Assert.Equal(3, hydrated.Items.Count);
        Assert.All(hydrated.Items, x => Assert.True(x.ResourceKeysComplete));
        Assert.InRange(hydrated.MetadataBytes, 0, 65536);
        var expiredPage = await Drain(HubCandidateStream.Expired);
        Assert.Equal(expired, Assert.Single(expiredPage.Items).Id);
        Assert.Empty(expiredPage.Oversized!);
        Assert.InRange(expiredPage.MetadataBytes, 0, 65536);
        var expiredHydration = Assert.Single((await reader.HydrateAsync(hub, 0, 64, [expired], 65536, default)).Items);
        Assert.Equal(expired, expiredHydration.Id);

        // A 256-row page divides 512 bytes into tiny nominal shares in older
        // per-row budgeting schemes, although each 288-byte UTF-8 base row is
        // representable on its own once IDs and rows are paged fairly.
        var smallBudgetRows = new List<HubTaskCandidate>(); HubCandidateCursor? smallCursor = null;
        for (var turns = 0; turns < 8; turns++)
        {
            var page = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Ready, horizon, smallCursor, 256, 512, default);
            smallBudgetRows.AddRange(page.Items); smallCursor = page.Next;
            Assert.InRange(page.MetadataBytes, 0, 512);
            Assert.Empty(page.Oversized!);
            if (page.EndOfStream) break;
        }
        Assert.Equal(readyIds.Order().ToArray(), smallBudgetRows.Select(x => x.Id).Order().ToArray());
        foreach (var candidate in smallBudgetRows)
        {
            var loaded = Assert.Single((await reader.HydrateAsync(hub, 0, 64, [candidate.Id], 512, default)).Items);
            Assert.True(loaded.ResourceKeysComplete);
        }
    }

    [Fact]
    public async Task ColdWorkflowHydrationReleasesSinglePoolConnectionBeforeMetadataLookup()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for pool lifetime tests");
        var hub = "candidate-pool-one-" + Guid.NewGuid().ToString("N");
        var workflow = Guid.NewGuid(); var task = Guid.NewGuid(); var canceledTask = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count)
            VALUES (@workflow,@hub,'pool-flow',1,0,'{}',1);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,workflow_id)
            VALUES (@task,@hub,0,'__workflow__',1,'{}',@workflow),(@canceled,@hub,0,'activity',1,'{}',NULL);
            """, db))
        {
            command.Parameters.AddWithValue("workflow", workflow); command.Parameters.AddWithValue("hub", hub);
            command.Parameters.AddWithValue("task", task); command.Parameters.AddWithValue("canceled", canceledTask);
            await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database, maxPoolSize: 1);
        var reader = new HubCandidateReader(tasks);
        var hydrated = Assert.Single((await reader.HydrateAsync(hub,0,64,[task],4096,default)
            .WaitAsync(TimeSpan.FromSeconds(3))).Items);
        Assert.True(hydrated.WorkflowMetadataComplete);
        Assert.Equal("pool-flow", hydrated.DefinitionId);

        var coldPageReader = new HubCandidateReader(tasks);
        var horizon = await coldPageReader.CaptureHorizonAsync(hub,default);
        var page = await coldPageReader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,null,8,4096,default)
            .WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal("pool-flow", Assert.Single(page.Items, x => x.Id == task).DefinitionId);

        await using (var held = await tasks.DataSource.OpenConnectionAsync())
        using (var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100)))
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.HydrateAsync(hub,0,64,[canceledTask],4096,cancellation.Token));
        await using var returned = await tasks.DataSource.OpenConnectionAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task WorkflowCacheEvictsWithinScopeAndRefreshesExpiredPositiveAndNegativeEntries()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for workflow cache scope tests");
        var hub = "candidate-cache-scope-" + Guid.NewGuid().ToString("N");
        var workflowA = Guid.NewGuid(); var workflowB = Guid.NewGuid(); var workflowC = Guid.NewGuid();
        var taskA = Guid.NewGuid(); var taskB = Guid.NewGuid(); var taskC = Guid.NewGuid();
        var expireWorkflow = Guid.NewGuid(); var missingWorkflow = Guid.NewGuid();
        var expireTask = Guid.NewGuid(); var missingTask = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count)
            VALUES (@a,@hub,'a',1,0,'{}',1),(@b,@hub,'b',2,1,'{}',1),(@c,@hub,'c',3,0,'{}',1),
                   (@expire,@hub,'before-expiry',4,0,'{}',1);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,workflow_id)
            VALUES (@ta,@hub,0,'__workflow__',1,'{}',@a),(@tb,@hub,1,'__workflow__',1,'{}',@b),
                   (@tc,@hub,0,'__workflow__',1,'{}',@c),(@te,@hub,0,'__workflow__',1,'{}',@expire),
                   (@tm,@hub,0,'__workflow__',1,'{}',@missing);
            """, db))
        {
            command.Parameters.AddWithValue("hub",hub); command.Parameters.AddWithValue("a",workflowA);
            command.Parameters.AddWithValue("b",workflowB); command.Parameters.AddWithValue("c",workflowC);
            command.Parameters.AddWithValue("expire",expireWorkflow); command.Parameters.AddWithValue("missing",missingWorkflow);
            command.Parameters.AddWithValue("ta",taskA); command.Parameters.AddWithValue("tb",taskB);
            command.Parameters.AddWithValue("tc",taskC); command.Parameters.AddWithValue("te",expireTask);
            command.Parameters.AddWithValue("tm",missingTask); await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database);
        var scoped = new HubCandidateReader(tasks, workflowCacheEntriesPerPartition:1,
            workflowCacheEntriesPerProcess:4, workflowCacheBytesPerPartition:2048,
            workflowCacheBytesPerProcess:8192);
        Assert.True(Assert.Single((await scoped.HydrateAsync(hub,0,64,[taskA],4096,default)).Items).WorkflowMetadataComplete);
        Assert.True(Assert.Single((await scoped.HydrateAsync(hub,1,64,[taskB],4096,default)).Items).WorkflowMetadataComplete);
        Assert.True(Assert.Single((await scoped.HydrateAsync(hub,0,64,[taskC],4096,default)).Items).WorkflowMetadataComplete);
        var scopes = scoped.WorkflowCacheSnapshot;
        Assert.Equal(2,scopes.Entries);
        Assert.InRange(scopes.Bytes,0,4096);
        Assert.True(scopes.Evictions >= 1);
        Assert.True(Assert.Single((await scoped.HydrateAsync(hub,1,64,[taskB],4096,default)).Items).WorkflowMetadataComplete);
        Assert.Equal(1,scoped.WorkflowCacheSnapshot.Hits);

        var expiring = new HubCandidateReader(tasks, workflowCacheTtl:TimeSpan.FromMilliseconds(10));
        var firstPositive = Assert.Single((await expiring.HydrateAsync(hub,0,64,[expireTask],4096,default)).Items);
        var firstNegative = Assert.Single((await expiring.HydrateAsync(hub,0,64,[missingTask],4096,default)).Items);
        Assert.Equal("before-expiry",firstPositive.DefinitionId);
        Assert.False(firstNegative.WorkflowMetadataComplete);
        await Task.Delay(30);
        await using (var update = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_workflow SET definition_id='after-expiry',definition_version=5 WHERE id=@expire;
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count)
            VALUES (@missing,@hub,'appeared-after-miss',6,0,'{}',1);
            """,db))
        { update.Parameters.AddWithValue("expire",expireWorkflow); update.Parameters.AddWithValue("missing",missingWorkflow);
          update.Parameters.AddWithValue("hub",hub); await update.ExecuteNonQueryAsync(); }
        var refreshedPositive = Assert.Single((await expiring.HydrateAsync(hub,0,64,[expireTask],4096,default)).Items);
        var refreshedNegative = Assert.Single((await expiring.HydrateAsync(hub,0,64,[missingTask],4096,default)).Items);
        Assert.Equal("after-expiry",refreshedPositive.DefinitionId);
        Assert.Equal(5,refreshedPositive.DefinitionVersion);
        Assert.True(refreshedNegative.WorkflowMetadataComplete);
        Assert.Equal("appeared-after-miss",refreshedNegative.DefinitionId);
    }

    [Fact]
    public async Task ConcurrentColdHydrationDeduplicatesSharedWorkflowLookup()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for workflow lookup deduplication tests");
        var hub = "candidate-shared-workflow-" + Guid.NewGuid().ToString("N");
        var workflow = Guid.NewGuid(); var taskA = Guid.NewGuid(); var taskB = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var command = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count)
            VALUES (@workflow,@hub,'shared-cold-flow',7,0,'{}',1);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,workflow_id)
            VALUES (@a,@hub,0,'__workflow__',1,'{}',@workflow),(@b,@hub,0,'__workflow__',1,'{}',@workflow);
            """, db))
        {
            command.Parameters.AddWithValue("workflow", workflow); command.Parameters.AddWithValue("hub", hub);
            command.Parameters.AddWithValue("a", taskA); command.Parameters.AddWithValue("b", taskB);
            await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks);
        var both = await Task.WhenAll(
            reader.HydrateAsync(hub, 0, 64, [taskA], 4096, default),
            reader.HydrateAsync(hub, 0, 64, [taskB], 4096, default));
        Assert.All(both.SelectMany(x => x.Items), item =>
        {
            Assert.True(item.WorkflowMetadataComplete);
            Assert.Equal("shared-cold-flow", item.DefinitionId);
        });
        Assert.Equal(1, reader.WorkflowCacheSnapshot.LookupCalls);
        Assert.Equal(1, reader.WorkflowCacheSnapshot.Entries);
        Assert.Equal(1, reader.WorkflowCacheSnapshot.Hits);
    }

    [Fact]
    public async Task ExpiredPagesUseDeadlineAndSequenceCursor()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate reader tests");
        var hub = "candidate-expired-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        var expected = new List<Guid>();
        for (var i = 0; i < 3; i++)
        {
            var id = Guid.NewGuid(); expected.Add(id);
            await using var command = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,lease_until)
                VALUES (@id,@hub,0,'activity',1,'{}',1,'worker',TIMESTAMPTZ '2020-01-01 00:00:00+00')
                """, db);
            command.Parameters.AddWithValue("id", id); command.Parameters.AddWithValue("hub", hub);
            await command.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var actual = new List<Guid>(); HubCandidateCursor? cursor = null;
        while (true)
        {
            var page = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Expired, horizon, cursor, 1, 1024, default);
            actual.AddRange(page.Items.Select(item => item.Id)); cursor = page.Next;
            if (page.EndOfStream) break;
        }
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task PagesRespectHorizonCursorAndByteLimit()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate reader tests");
        var hub = "candidate-pages-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        async Task<Guid> Insert()
        {
            var id = Guid.NewGuid();
            await using var command = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')", db);
            command.Parameters.AddWithValue("id", id); command.Parameters.AddWithValue("hub", hub);
            await command.ExecuteNonQueryAsync(); return id;
        }
        var expected = new List<Guid>();
        for (var i = 0; i < 5; i++) expected.Add(await Insert());
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var late = await Insert();
        var all = new List<Guid>(); HubCandidateCursor? cursor = null;
        do
        {
            var page = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Ready, horizon, cursor, 3, 256, default);
            all.AddRange(page.Items.Select(x => x.Id)); cursor = page.Next;
            if (page.EndOfStream) break;
        } while (all.Count <= 5);
        Assert.Equal(expected, all);
        var nextHorizon = await reader.CaptureHorizonAsync(hub, default);
        var fresh = await reader.ReadPageAsync(hub, 0, HubCandidateStream.Ready, nextHorizon, cursor, 3, 1024, default);
        Assert.Equal(late, Assert.Single(fresh.Items).Id);
        var empty = await reader.ReadPageAsync(hub, 1, HubCandidateStream.Ready, nextHorizon, null, 3, 1024, default);
        Assert.True(empty.EndOfStream);
    }
    [Fact]
    public async Task NewSweepRecoversLateCommitAndTasksBecomingDueOrExpiredBehindCursor()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-wrap-" + Guid.NewGuid().ToString("N");
        await using var early = new NpgsqlConnection(database);
        await using var db = new NpgsqlConnection(database);
        await early.OpenAsync(); await db.OpenAsync();
        await using var tx = await early.BeginTransactionAsync();
        var late = Guid.NewGuid(); var due = Guid.NewGuid(); var expired = Guid.NewGuid(); var head = Guid.NewGuid();
        await using (var cmd = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')", early, tx))
        { cmd.Parameters.AddWithValue("id", late); cmd.Parameters.AddWithValue("hub", hub); await cmd.ExecuteNonQueryAsync(); }
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,due_at)
            VALUES (@due,@hub,0,'activity',1,'{}',now()+interval '1 hour'), (@head,@hub,0,'activity',1,'{}',now());
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,lease_until)
            VALUES (@expired,@hub,0,'activity',1,'{}',1,'worker',now()+interval '1 hour');
            """, db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("due",due); cmd.Parameters.AddWithValue("head",head); cmd.Parameters.AddWithValue("expired",expired); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database);
        var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var first = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,null,10,4096,default);
        Assert.Equal(head, Assert.Single(first.Items).Id);
        await tx.CommitAsync();
        await using (var cmd = new NpgsqlCommand("UPDATE wfc_scale.hub_task SET due_at=now()-interval '1 hour' WHERE id=@due; UPDATE wfc_scale.hub_task SET lease_until=now()-interval '1 hour' WHERE id=@expired",db))
        { cmd.Parameters.AddWithValue("due",due); cmd.Parameters.AddWithValue("expired",expired); await cmd.ExecuteNonQueryAsync(); }
        var remainder = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,first.Next,10,4096,default);
        Assert.Empty(remainder.Items);
        var fresh = await reader.CaptureHorizonAsync(hub,default);
        var wrapped = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,fresh,null,10,4096,default);
        Assert.Contains(wrapped.Items, x => x.Id == late);
        Assert.Contains(wrapped.Items, x => x.Id == due);
        var expiredPage = await reader.ReadPageAsync(hub,0,HubCandidateStream.Expired,fresh,null,10,4096,default);
        Assert.Equal(expired, Assert.Single(expiredPage.Items).Id);
    }

    [Fact]
    public async Task DueStreamUsesDeadlineAndSequenceCursor()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-due-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        var earlier = Guid.NewGuid(); var later = Guid.NewGuid();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,due_at)
            VALUES (@later,@hub,0,'activity',1,'{}',now()-interval '1 hour'),
                   (@earlier,@hub,0,'activity',1,'{}',now()-interval '2 hours')
            """,db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("later",later); cmd.Parameters.AddWithValue("earlier",earlier); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks, metadataEnrichmentEnabled: true);
        var horizon = await reader.CaptureHorizonAsync(hub, default);
        var first = await reader.ReadPageAsync(hub,0,HubCandidateStream.ReadyDue,horizon,null,1,1024,default);
        Assert.Equal(earlier, Assert.Single(first.Items).Id);
        var second = await reader.ReadPageAsync(hub,0,HubCandidateStream.ReadyDue,horizon,first.Next,1,1024,default);
        Assert.Equal(later, Assert.Single(second.Items).Id);
    }

    [Fact]
    public async Task BucketStreamsCoverSkewWithoutLeasing()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-buckets-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            SELECT gen_random_uuid(),@hub,g,'activity',1,'{}' FROM generate_series(0,63) g;
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            SELECT gen_random_uuid(),@hub,64,'activity',1,'{}' FROM generate_series(1,10);
            """,db)) { cmd.Parameters.AddWithValue("hub",hub); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub,default); var total = 0;
        for (var bucket = 0; bucket < 64; bucket++)
        {
            HubCandidateCursor? cursor = null; var count = 0;
            do { var page = await reader.ReadPageAsync(hub,bucket,HubCandidateStream.Ready,horizon,cursor,3,4096,default);
                 count += page.Items.Count; cursor = page.Next; if (page.EndOfStream) break; } while (true);
            Assert.Equal(bucket == 0 ? 11 : 1,count); total += count;
        }
        Assert.Equal(74,total);
        await using var check = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_task WHERE hub_id=@hub AND (attempts<>0 OR epoch<>0 OR state<>0)",db);
        check.Parameters.AddWithValue("hub",hub); Assert.Equal(0L,(long)(await check.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task ExpiredTaskBehindDeadlineCursorAppearsAfterWrap()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-expiry-wrap-" + Guid.NewGuid().ToString("N"); var a = Guid.NewGuid(); var b = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,lease_until)
            VALUES (@a,@hub,0,'activity',1,'{}',1,'worker',now()-interval '1 hour'),
                   (@b,@hub,0,'activity',1,'{}',1,'worker',now()+interval '1 hour');
            """,db)) { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("a",a); cmd.Parameters.AddWithValue("b",b); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub,default);
        var first = await reader.ReadPageAsync(hub,0,HubCandidateStream.Expired,horizon,null,1,1024,default);
        Assert.Equal(a,Assert.Single(first.Items).Id);
        await using (var cmd = new NpgsqlCommand("UPDATE wfc_scale.hub_task SET lease_until=now()-interval '2 hours' WHERE id=@id",db))
        { cmd.Parameters.AddWithValue("id",b); await cmd.ExecuteNonQueryAsync(); }
        var rest = await reader.ReadPageAsync(hub,0,HubCandidateStream.Expired,horizon,first.Next,2,1024,default);
        Assert.Empty(rest.Items);
        var next = await reader.CaptureHorizonAsync(hub,default);
        var wrapped = await reader.ReadPageAsync(hub,0,HubCandidateStream.Expired,next,null,2,1024,default);
        Assert.Equal(b,wrapped.Items[0].Id);
    }

    [Fact]
    public async Task OversizedHeadIsReportedAndDoesNotBlockFollowingCandidate()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "oversized-head-" + Guid.NewGuid().ToString("N"); var oversized = Guid.NewGuid(); var ordinary = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@oversized,@hub,0,@large,1,'{}'),(@ordinary,@hub,0,'activity',1,'{}')
            """,db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("oversized",oversized); cmd.Parameters.AddWithValue("ordinary",ordinary); cmd.Parameters.AddWithValue("large",new string('z',4000)); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks, metadataEnrichmentEnabled: true);
        var horizon = await reader.CaptureHorizonAsync(hub,default);
        var first = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,null,10,256,default);
        Assert.Equal(oversized,Assert.Single(first.Oversized!).Id);
        var second = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,first.Next,10,256,default);
        var all = first.Items.Concat(second.Items).ToArray();
        Assert.Equal(ordinary,Assert.Single(all).Id);
        Assert.True(second.Next!.Sequence >= all[0].Sequence);
    }

    [Fact]
    public async Task GlobalHorizonCoversOlderHubWhileLaterInsertsRemainOutsideSweep()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "old-hub-" + Guid.NewGuid().ToString("N"); var other = hub+"-other"; var old = Guid.NewGuid(); var later = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        async Task Insert(Guid id,string target)
        { await using var cmd = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')",db); cmd.Parameters.AddWithValue("id",id); cmd.Parameters.AddWithValue("hub",target); await cmd.ExecuteNonQueryAsync(); }
        await Insert(old,hub); await Insert(Guid.NewGuid(),other);
        await using var tasks = new HubTaskStore(database); var reader = new HubCandidateReader(tasks);
        var horizon = await reader.CaptureHorizonAsync(hub,default);
        await Insert(Guid.NewGuid(),other); await Insert(later,hub);
        var page = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,horizon,null,10,2048,default);
        Assert.Equal(old,Assert.Single(page.Items).Id);
        var fresh = await reader.CaptureHorizonAsync(hub,default);
        var next = await reader.ReadPageAsync(hub,0,HubCandidateStream.Ready,fresh,page.Next,10,2048,default);
        Assert.Equal(later,Assert.Single(next.Items).Id);
    }

}
