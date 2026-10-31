using System.Threading;
using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace WorkflowCore.Scaling.PostgreSQL;

/// <summary>
/// One journal capture per asynchronous workflow execution. Each worker slot
/// has an independent AsyncLocal scope; committed outcomes are buffered until
/// the fenced state transition commits.
/// </summary>
internal sealed class ExecutionHistoryScope : IDisposable
{
    private static readonly AsyncLocal<ExecutionHistoryScope?> Ambient = new();
    private readonly ExecutionHistoryScope? _previous;

    public static ExecutionHistoryScope? Current => Ambient.Value;
    public Guid WorkItemId { get; }
    public long Revision { get; }
    public List<ExecutionHistoryEvent> Pending { get; } = new();

    private ExecutionHistoryScope(Guid workItemId, long revision)
    {
        WorkItemId = workItemId;
        Revision = revision;
        _previous = Ambient.Value;
        Ambient.Value = this;
    }

    public static ExecutionHistoryScope Begin(Guid workItemId, long revision) =>
        new(workItemId, revision);

    public void Dispose() => Ambient.Value = _previous;
}

/// <summary>
/// Write-ahead record of the step attempt before running a step body. Its
/// returned/failed outcome is journaled only with the fenced workflow commit.
/// A Started record without a committed outcome means the result is uncertain.
/// </summary>
public sealed class ExecutionHistoryMiddleware : IWorkflowStepMiddleware
{
    private readonly ExecutionHistoryStore _history;

    public ExecutionHistoryMiddleware(ExecutionHistoryStore history) => _history = history;

    public async Task<ExecutionResult> HandleAsync(
        IStepExecutionContext context, IStepBody body, WorkflowStepDelegate next)
    {
        var capture = ExecutionHistoryScope.Current;
        if (capture is null)
            return await next();

        var workflowId = Guid.Parse(context.Workflow.Id);
        var pointer = context.ExecutionPointer;
        var attemptId = Guid.NewGuid();
        var metadata = ExecutionHistoryEvent.Json(new
        {
            stepName = pointer.StepName,
            retryCount = pointer.RetryCount
        });

        // Fail closed: do not execute the body if its start cannot be persisted.
        await _history.AppendAsync(new ExecutionHistoryEvent(
            workflowId, "StepAttemptStarted", capture.WorkItemId,
            pointer.Id, pointer.StepId, attemptId, capture.Revision, metadata),
            context.CancellationToken);

        try
        {
            var result = await next();
            capture.Pending.Add(new ExecutionHistoryEvent(
                workflowId, result.Proceed ? "StepBodyReturned" : "StepBodyYielded",
                capture.WorkItemId, pointer.Id, pointer.StepId, attemptId,
                capture.Revision,
                ExecutionHistoryEvent.Json(new
                {
                    proceeds = result.Proceed,
                    waitingForEvent = !string.IsNullOrEmpty(result.EventName),
                    sleeping = result.SleepFor.HasValue
                })));
            return result;
        }
        catch (Exception error)
        {
            // The existing executor handles step exceptions and retry policy.
            // Do not persist raw error messages or user input in audit details.
            capture.Pending.Add(new ExecutionHistoryEvent(
                workflowId, "StepBodyThrew", capture.WorkItemId,
                pointer.Id, pointer.StepId, attemptId, capture.Revision,
                ExecutionHistoryEvent.Json(new { exceptionType = error.GetType().FullName })));
            throw;
        }
    }
}
