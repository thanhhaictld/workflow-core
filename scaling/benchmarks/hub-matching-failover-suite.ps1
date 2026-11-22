#Requires -Version 7.0
[CmdletBinding()]
param([string]$Output = 'scaling/benchmarks/results/push-local/failover-suite')
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Assumes Release binaries and the local development PostgreSQL container have
# been prepared. Each scenario creates its own isolated database and proxy.
# Hub faults kill the owner without draining. Worker faults hard-kill the
# processor, then restart it with the same durable credit journal.
$runner = Join-Path $PSScriptRoot 'hub-push-failover.ps1'
$pwsh = (Get-Command pwsh).Source
$results = @()
foreach ($scenario in @('before-claim','after-claim','during-execution','worker-before-claim','worker-during-execution')) {
    Write-Host "Running candidate matching + SSE failover: $scenario"
    & $pwsh -NoProfile -File $runner -Scenario $scenario -Output $Output
    $results += [ordered]@{ scenario = $scenario; exitCode = $LASTEXITCODE }
    if ($LASTEXITCODE -ne 0) {
        $results | ConvertTo-Json -Depth 4 | Write-Host
        throw "Candidate-mode failover regression in scenario: $scenario"
    }
}
Write-Host 'All five candidate matching + SSE failover windows passed.'
$results | ConvertTo-Json -Depth 4 | Write-Host
