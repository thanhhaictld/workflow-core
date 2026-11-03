namespace WorkflowCore.Scaling.PostgreSQL;

/// <summary>
/// Requeue because of an in-progress dependency, without counting it as a
/// workflow failure or charging a dead-letter retry attempt.
/// </summary>
public sealed class DeferredWorkException : Exception
{
    public DeferredWorkException(string message) : base(message) { }
}
