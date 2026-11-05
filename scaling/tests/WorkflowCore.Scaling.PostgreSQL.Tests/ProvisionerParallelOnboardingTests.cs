using Microsoft.Extensions.DependencyInjection;
using WorkflowCore.Primitives;
using WorkflowCore.Provisioner.Host;
using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public class ProvisionerParallelOnboardingTests
{
    [Fact]
    public void ParallelDefinitionContainsThreeBranchesAndPostJoinActivation()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddHubWorkflowRuntime().AddWorkflow<ParallelOnboardingWorkflow, ParallelOnboardingData>("onboarding");
        using var provider = services.BuildServiceProvider();
        var definition = provider.GetRequiredService<HubNativeCatalog>().Find("onboarding", "tenant-onboarding-parallel", 1);
        var fork = Assert.Single(definition.Steps, step => step.BodyType == typeof(Sequence) && step.Children.Count == 3);
        var branchRoots = fork.Children.Select(id => definition.Steps.FindById(id).BodyType).ToArray();
        Assert.Contains(typeof(CreateResourceGroup), branchRoots);
        Assert.Contains(typeof(RegisterApplication), branchRoots);
        Assert.Contains(typeof(CreateBillingAccount), branchRoots);
        Assert.Contains(fork.Outcomes, outcome => definition.Steps.FindById(outcome.NextStep).BodyType == typeof(ActivateParallelTenant));
        Assert.Contains(definition.Steps, step => step.BodyType == typeof(DeployDatabase));
        Assert.Contains(definition.Steps, step => step.BodyType == typeof(ConfigureRoles));
        Assert.Contains(definition.Steps, step => step.BodyType == typeof(ConfigureQuota));
        Assert.Contains(definition.Steps, step => step.BodyType == typeof(SendWelcome));
    }
}
