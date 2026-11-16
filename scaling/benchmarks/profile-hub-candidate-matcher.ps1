param(
    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Release'
)

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$testProject = Join-Path $repoRoot 'scaling\tests\WorkflowCore.Scaling.PostgreSQL.Tests\WorkflowCore.Scaling.PostgreSQL.Tests.csproj'
$previous = $env:WFC_RUN_HUB_MATCHER_PROFILE
try {
    $env:WFC_RUN_HUB_MATCHER_PROFILE = 'true'
    dotnet test $testProject -c $Configuration --filter 'FullyQualifiedName~HubCandidateMatcherProfileTests' --logger 'console;verbosity=detailed'
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
    $env:WFC_RUN_HUB_MATCHER_PROFILE = $previous
}
