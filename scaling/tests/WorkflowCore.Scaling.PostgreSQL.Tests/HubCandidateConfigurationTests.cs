using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateConfigurationTests
{
    [Fact]
    public void StandardPresetsKeepExistingCandidateMatcherOnAndTaskIdHintsOff()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null && !File.Exists(Path.Combine(root.FullName, "Makefile")))
            root = root.Parent;
        Assert.NotNull(root);

        var makefile = File.ReadAllText(Path.Combine(root!.FullName, "Makefile"));
        Assert.Contains("WFC_HUB_MATCHING_CANDIDATES_ENABLED := true", makefile);
        Assert.Contains("WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED := false", makefile);

        var presets = new[]
        {
            "compose.yaml", "compose.matching-ha.yaml", "compose.matching-shards.yaml",
            "compose.owner-recovery.yaml"
        };
        foreach (var preset in presets)
        {
            var source = File.ReadAllText(Path.Combine(root.FullName, "scaling", "observability", preset));
            Assert.Contains("WFC_HUB_MATCHING_CANDIDATES_ENABLED: \"${WFC_HUB_MATCHING_CANDIDATES_ENABLED:-true}\"", source);
            Assert.Contains("WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED: \"${WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED:-false}\"", source);
        }
    }
}
