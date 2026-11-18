using System.Security.Cryptography;
using System.Text;

namespace WorkflowCore.Hub;

[Flags]
internal enum HubTaskAdmission : short
{
    Unknown = 0,
    Unconstrained = 1,
    PartitionSerial = 2,
    RegisteredHubQuota = 4,
    RegisteredActivityQuota = 8,
    HubQuota = 16,
    ActivityQuota = 32,
    WorkflowTaskQuota = 64
}

// Classification happens when the Hub writes a task and its constraints in
// the same transaction. The flags describe complete, fixed permit identities.
// Capacities still come from the locked PostgreSQL permit rows at claim time.
internal static class HubTaskAdmissionPolicy
{
    private const HubTaskAdmission Quotas = HubTaskAdmission.RegisteredHubQuota |
        HubTaskAdmission.RegisteredActivityQuota | HubTaskAdmission.HubQuota |
        HubTaskAdmission.ActivityQuota | HubTaskAdmission.WorkflowTaskQuota;
    private static readonly string AllDigest = Digest("all");

    internal static HubTaskAdmission Classify(HubTaskRoute route)
    {
        if (route.Ordering is not (HubActivityOrdering.Unordered or HubActivityOrdering.PartitionSerial or HubActivityOrdering.Legacy))
            return HubTaskAdmission.Unknown;
        var admission = route.Ordering == HubActivityOrdering.PartitionSerial
            ? HubTaskAdmission.PartitionSerial : HubTaskAdmission.Unknown;
        if (route.Resources.Count == 0)
            return admission == HubTaskAdmission.Unknown ? HubTaskAdmission.Unconstrained : admission;

        var canonicalQuotas = Permits(Quotas, route.HubId, route.TaskType, 0).ToArray();
        foreach (var rule in route.Resources)
        {
            if (rule.StrictFifo || rule.Capacity < 1) return HubTaskAdmission.Unknown;
            var quota = canonicalQuotas.FirstOrDefault(p => p.Key == rule.Key).Kind;
            if (quota == HubTaskAdmission.Unknown) return HubTaskAdmission.Unknown;
            admission |= quota;
        }
        return admission;
    }

    internal static IEnumerable<(HubTaskAdmission Kind, string Key)> Permits(
        HubTaskAdmission admission, string hub, string taskType, int partition)
    {
        if ((admission & HubTaskAdmission.PartitionSerial) != 0)
            yield return (HubTaskAdmission.PartitionSerial, PartitionPermit(hub, partition));
        if ((admission & HubTaskAdmission.RegisteredHubQuota) != 0)
            yield return (HubTaskAdmission.RegisteredHubQuota, RegisteredHubPermit(hub));
        if ((admission & HubTaskAdmission.RegisteredActivityQuota) != 0)
            yield return (HubTaskAdmission.RegisteredActivityQuota, RegisteredActivityPermit(hub, taskType));
        if ((admission & HubTaskAdmission.HubQuota) != 0)
            yield return (HubTaskAdmission.HubQuota, $"hub:{hub}:{AllDigest}");
        if ((admission & HubTaskAdmission.ActivityQuota) != 0)
            yield return (HubTaskAdmission.ActivityQuota, $"activity:{hub}:{taskType}:{AllDigest}");
        if ((admission & HubTaskAdmission.WorkflowTaskQuota) != 0)
            yield return (HubTaskAdmission.WorkflowTaskQuota, $"workflow-task:{hub}:{taskType}:{AllDigest}");
    }

    internal static string PartitionPermit(string hub, int partition) =>
        FormattableString.Invariant($"partition:{hub}:{partition}");

    internal static string RegisteredHubPermit(string hub) => $"hub:{hub}:{Digest(hub)}";
    internal static string RegisteredActivityPermit(string hub, string taskType) =>
        $"activity:{hub}:{taskType}:{Digest(taskType)}";

    private static string Digest(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
