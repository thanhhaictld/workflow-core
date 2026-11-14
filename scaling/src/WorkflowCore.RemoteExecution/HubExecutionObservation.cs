using System.Text.Json;
using WorkflowCore.Hub;

namespace WorkflowCore.RemoteExecution;

// Optional benchmark-only process-local evidence. It never accesses PostgreSQL
// and records observed starts, not inferred durable attempts.
internal static class HubExecutionObservation
{
    private static readonly object Gate = new();
    private static readonly string? Path = Environment.GetEnvironmentVariable("WFC_HUB_EXECUTION_OBSERVATION_LOG");
    public static void Started(HubLeasedTask task)
    {
        if (string.IsNullOrWhiteSpace(Path)) return;
        var dispatch = task.Payload.ValueKind == JsonValueKind.Object && task.Payload.TryGetProperty("__hubContext", out var context) &&
            context.TryGetProperty("dispatchKey", out var key) ? key.GetString() : null;
        var line = JsonSerializer.Serialize(new { kind = "execution_start", taskId = task.Id,
            workflowId = task.WorkflowId, taskType = task.TaskType, epoch = task.Epoch,
            dispatchKey = dispatch, observedAt = DateTime.UtcNow });
        lock (Gate) File.AppendAllText(Path, line + Environment.NewLine);
    }
}
