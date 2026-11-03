using Microsoft.EntityFrameworkCore;
using Npgsql;
using WorkflowCore.Models;
using WorkflowCore.Persistence.EntityFramework.Models;
using WorkflowCore.Persistence.PostgreSQL;

namespace WorkflowCore.Scaling.Api;

/// <summary>
/// Read-only monitoring projections backed by the existing workflow snapshot,
/// queue ledger and append-only execution history. No workflow payloads are returned.
/// </summary>
public sealed class WorkflowMonitorReader(string connectionString)
{
    private PostgresContext Context() => new(connectionString, "wfc");

    public async Task<WorkflowMonitorOverview> OverviewAsync(CancellationToken ct)
    {
        await using var db = Context();
        var groups = await db.Set<PersistedWorkflow>().AsNoTracking()
            .GroupBy(w => w.Status)
            .Select(g => new { Status = g.Key, Count = g.Count() })
            .ToListAsync(ct);
        var counts = groups.ToDictionary(g => g.Status.ToString(), g => g.Count);
        int Get(string status) => counts.GetValueOrDefault(status);
        var since = DateTime.UtcNow.AddHours(-24);
        var created = await db.Set<PersistedWorkflow>().CountAsync(w => w.CreateTime >= since, ct);
        var completed = await db.Set<PersistedWorkflow>().CountAsync(
            w => w.CompleteTime >= since && w.Status == WorkflowStatus.Complete, ct);
        var withErrors = await db.Set<PersistedExecutionError>()
            .Select(e => e.WorkflowId).Distinct().CountAsync(ct);
        return new WorkflowMonitorOverview(groups.Sum(g => g.Count),
            Get(nameof(WorkflowStatus.Runnable)), Get(nameof(WorkflowStatus.Complete)),
            Get(nameof(WorkflowStatus.Suspended)), Get(nameof(WorkflowStatus.Terminated)),
            withErrors, created, completed, DateTime.UtcNow);
    }

    public async Task<WorkflowMonitorPage> ListAsync(
        string? search, string? status, int page, int take, CancellationToken ct)
    {
        if (page < 1 || take is < 1 or > 100)
            throw new ArgumentOutOfRangeException(nameof(page), "Use page >= 1 and take 1..100");
        await using var db = Context();
        var workflows = db.Set<PersistedWorkflow>().AsNoTracking().AsQueryable();
        if (!string.IsNullOrWhiteSpace(status) && status != "All")
        {
            if (!Enum.TryParse<WorkflowStatus>(status, true, out var statusValue)
                || !Enum.IsDefined(statusValue))
                throw new ArgumentException("Invalid workflow status", nameof(status));
            workflows = workflows.Where(w => w.Status == statusValue);
        }

        if (!string.IsNullOrWhiteSpace(search))
        {
            var term = search.Trim();
            if (term.Length > 200)
                throw new ArgumentException("Search is limited to 200 characters", nameof(search));
            if (Guid.TryParse(term, out var id))
                workflows = workflows.Where(w => w.InstanceId == id);
            else
                workflows = workflows.Where(w =>
                    EF.Functions.ILike(w.WorkflowDefinitionId!, $"%{EscapeLike(term)}%", "\\")
                    || (w.Reference != null &&
                        EF.Functions.ILike(w.Reference, $"%{EscapeLike(term)}%", "\\")));
        }
        var total = await workflows.CountAsync(ct);
        var rows = await workflows.OrderByDescending(w => w.CreateTime)
            .ThenByDescending(w => w.InstanceId)
            .Skip((page - 1) * take).Take(take)
            .Select(w => new
            {
                w.InstanceId, w.WorkflowDefinitionId, w.Version, w.Reference,
                w.Description, w.Status, w.CreateTime, w.CompleteTime
            }).ToListAsync(ct);

        var idStrings = rows.Select(w => w.InstanceId.ToString()).ToArray();
        var errors = await db.Set<PersistedExecutionError>().AsNoTracking()
            .Where(e => idStrings.Contains(e.WorkflowId))
            .GroupBy(e => e.WorkflowId)
            .Select(g => new { Id = g.Key, Count = g.Count() })
            .ToListAsync(ct);
        var byWorkflow = errors.ToDictionary(e => e.Id, e => e.Count);
        var items = rows.Select(w => new WorkflowMonitorItem(
            w.InstanceId, w.WorkflowDefinitionId ?? string.Empty,
            w.Version, w.Reference, w.Description, w.Status.ToString(),
            w.CreateTime, w.CompleteTime,
            byWorkflow.GetValueOrDefault(w.InstanceId.ToString()))).ToList();
        return new WorkflowMonitorPage(items, page, take, total);
    }

    public async Task<WorkflowMonitorDetail?> GetAsync(Guid id, bool showErrorMessages,
        CancellationToken ct)
    {
        await using var db = Context();
        var workflow = await db.Set<PersistedWorkflow>().AsNoTracking()
            .Where(w => w.InstanceId == id)
            .Select(w => new
            {
                w.PersistenceId, w.InstanceId, w.WorkflowDefinitionId, w.Version,
                w.Reference, w.Description, w.Status, w.CreateTime, w.CompleteTime
            }).SingleOrDefaultAsync(ct);
        if (workflow is null) return null;

        var pointers = await db.Set<PersistedExecutionPointer>().AsNoTracking()
            .Where(p => p.WorkflowId == workflow.PersistenceId)
            .OrderBy(p => p.PersistenceId)
            .Select(p => new WorkflowMonitorPointer(p.Id, p.StepId, p.StepName,
                p.PredecessorId, p.Status.ToString(), p.Active, p.RetryCount,
                p.StartTime, p.EndTime, p.SleepUntil, p.EventName, p.EventPublished))
            .ToListAsync(ct);

        var errors = await db.Set<PersistedExecutionError>().AsNoTracking()
            .Where(e => e.WorkflowId == id.ToString())
            .OrderByDescending(e => e.ErrorTime).Take(100)
            .Select(e => new WorkflowMonitorError(e.ExecutionPointerId, e.ErrorTime,
                showErrorMessages ? e.Message : null))
            .ToListAsync(ct);

        var workItems = new List<WorkflowMonitorWorkItem>();
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand("""
                SELECT id, state, attempts, created_at, due_at, lease_until, owner
                FROM wfc_scale.work_item
                WHERE queue_name = 'workflow' AND item_id = @id
                ORDER BY created_at DESC, id DESC LIMIT 100
                """, connection);
            command.Parameters.AddWithValue("id", id);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                workItems.Add(new WorkflowMonitorWorkItem(reader.GetGuid(0),
                    reader.GetInt16(1) switch
                    {
                        0 => "Ready", 1 => "Leased", 2 => "Completed",
                        3 => "DeadLetter", _ => "Unknown"
                    }, reader.GetInt32(2), reader.GetDateTime(3),
                    reader.GetDateTime(4),
                    reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6)));
        }

        return new WorkflowMonitorDetail(
            new WorkflowMonitorItem(workflow.InstanceId,
                workflow.WorkflowDefinitionId ?? string.Empty, workflow.Version,
                workflow.Reference, workflow.Description, workflow.Status.ToString(),
                workflow.CreateTime, workflow.CompleteTime, errors.Count),
            pointers, errors, workItems);
    }

    private static string EscapeLike(string value) =>
        value.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");
}

public sealed record WorkflowMonitorOverview(
    int Total, int Running, int Completed, int Suspended, int Terminated,
    int WithErrors, int Created24h, int Completed24h, DateTime AsOf);
public sealed record WorkflowMonitorItem(Guid Id, string DefinitionId, int Version,
    string? Reference, string? Description, string Status,
    DateTime CreatedAt, DateTime? CompletedAt, int ErrorCount);
public sealed record WorkflowMonitorPage(
    IReadOnlyList<WorkflowMonitorItem> Items, int Page, int Take, int Total);
public sealed record WorkflowMonitorPointer(
    string Id, int StepId, string? StepName, string? PredecessorId,
    string Status, bool Active, int RetryCount, DateTime? StartTime,
    DateTime? EndTime, DateTime? SleepUntil, string? EventName, bool EventPublished);
public sealed record WorkflowMonitorError(
    string? ExecutionPointerId, DateTime ErrorTime, string? Message);
public sealed record WorkflowMonitorWorkItem(
    Guid Id, string State, int Attempts, DateTime CreatedAt,
    DateTime DueAt, DateTime? LeaseUntil, string? Owner);
public sealed record WorkflowMonitorDetail(
    WorkflowMonitorItem Workflow, IReadOnlyList<WorkflowMonitorPointer> Pointers,
    IReadOnlyList<WorkflowMonitorError> Errors,
    IReadOnlyList<WorkflowMonitorWorkItem> WorkItems);
