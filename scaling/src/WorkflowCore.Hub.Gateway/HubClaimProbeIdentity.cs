using System.Text;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>Canonical, bounded-content identity for the five-second durable catch-up probe.</summary>
internal static class HubClaimProbeIdentity
{
    public static string Create(HubClaimRequest request)
    {
        var builder = new StringBuilder();
        Append(builder, request.Hubs[0]);
        builder.Append('|').Append(request.MatchingPartition ?? 0).Append('|');

        var capabilities = request.Capabilities
            .Select(x => (x.HubId, x.TaskType, x.Version))
            .Distinct()
            .OrderBy(x => x.HubId, StringComparer.Ordinal)
            .ThenBy(x => x.TaskType, StringComparer.Ordinal)
            .ThenBy(x => x.Version);
        foreach (var capability in capabilities)
        {
            Append(builder, capability.HubId);
            Append(builder, capability.TaskType);
            builder.Append(capability.Version).Append(';');
        }

        builder.Append(request.Workflows is null ? "|U|" : "|R|");
        if (request.Workflows is not null)
        {
            var workflows = request.Workflows
                .Select(x => (x.HubId, x.DefinitionId, x.Version))
                .Distinct()
                .OrderBy(x => x.HubId, StringComparer.Ordinal)
                .ThenBy(x => x.DefinitionId, StringComparer.Ordinal)
                .ThenBy(x => x.Version);
            foreach (var workflow in workflows)
            {
                Append(builder, workflow.HubId);
                Append(builder, workflow.DefinitionId);
                builder.Append(workflow.Version).Append(';');
            }
        }

        return builder.ToString();
    }

    private static void Append(StringBuilder builder, string value) =>
        builder.Append(value.Length).Append(':').Append(value).Append(';');
}
