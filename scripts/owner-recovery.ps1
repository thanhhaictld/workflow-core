#Requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateSet('help','config','build','up','ps','logs','down','db-test','smoke-test','test','fault-test')]
    [string]$Task = 'help',
    [string]$Project = '',
    [ValidateRange(1024,65200)][int]$PortBase = 26200,
    [string]$Results = 'artifacts/owner-recovery',
    [string]$Python = 'python',
    [string]$Filter = '',
    [ValidateSet('both','hub','hub-2')][string]$Victim = 'both',
    [ValidateSet('both','idle','busy')][string]$Mode = 'both',
    [ValidateRange(1,10)][int]$Repeat = 1
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ($Task -eq 'help') {
    Write-Host @'
Owner recovery Compose suite (Docker + PowerShell 7; Python for smoke/fault tests):
  make obs-owner-recovery-up          Build and start 2 Hubs / 4 shards / 4 workers
  make obs-owner-recovery-test        Full configured PostgreSQL tests + workflow smoke
  make obs-owner-recovery-db-test     Database tests only, separate retained test database
  make obs-owner-recovery-smoke-test  40 unordered + 40 keyed workflows; durable audit
  make obs-owner-recovery-fault-test  Either owner, idle/busy; 85s per case, victim restored afterward
  make obs-owner-recovery-ps          Show this suite only
  make obs-owner-recovery-logs        Recent suite logs
  make obs-owner-recovery-down        Stop/remove suite containers; retain both DB volumes
  make obs-owner-recovery-config      Validate/print resolved Compose config (lab credentials)
Overrides: OWNER_PORT_BASE=26200 OWNER_PROJECT=<unique-name> OWNER_RESULTS=artifacts/owner-recovery
Fault selection: OWNER_FAULT_VICTIM=hub|hub-2|both OWNER_FAULT_MODE=idle|busy|both OWNER_FAULT_REPEAT=2
Without Make: pwsh -NoProfile -File scripts/owner-recovery.ps1 test
Default project is derived from this worktree. Other suites and historical evidence are untouched.
'@
    exit 0
}
Set-Location -LiteralPath $root
if (!$Project) {
    $identityPath = if ($IsWindows) { $root.ToLowerInvariant() } else { $root }
    $bytes = [Text.Encoding]::UTF8.GetBytes($identityPath)
    $suffix = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).Substring(0,16).ToLowerInvariant()
    $Project = "workflow-owner-recovery-$suffix"
}
if ($Project -notmatch '^[a-z0-9][a-z0-9-]{1,60}$') { throw 'Use a lowercase Docker-safe project name (2-61 characters)' }
$resultsPath = [IO.Path]::GetFullPath($Results, $root)
New-Item -ItemType Directory -Path (Join-Path $resultsPath 'tests') -Force | Out-Null
$env:OWNER_RECOVERY_PROJECT = $Project
$env:OWNER_RECOVERY_RESULTS = $resultsPath.Replace('\','/')
$env:OWNER_RECOVERY_HUB_PORT = "$PortBase"
$env:OWNER_RECOVERY_HUB2_PORT = "$($PortBase + 1)"
$env:OWNER_RECOVERY_LB_PORT = "$($PortBase + 10)"
$env:OWNER_RECOVERY_METRICS_PORT = "$($PortBase + 89)"
$env:OWNER_RECOVERY_PG_PORT = "$($PortBase + 240)"
$composeArgs = @('compose','--project-name',$Project,'-f','scaling/observability/compose.owner-recovery.yaml','--profile','tests')
function Invoke-Compose {
    & docker @composeArgs @args
    if ($LASTEXITCODE) { throw "Docker Compose failed ($LASTEXITCODE): $args" }
}
function Invoke-Lab {
    & $Python scaling/observability/benchmark/owner_recovery_lab.py @args --project $Project --compose-project $Project --port-base $PortBase --evidence-dir $runPath
    if ($LASTEXITCODE) { throw "Ownership lab failed ($LASTEXITCODE): $args" }
}
function Audit-Lab {
    & $Python scaling/observability/benchmark/analyze_owner_recovery.py --evidence-dir $runPath
    if ($LASTEXITCODE) { throw 'Durable workflow/fault audit failed' }
}
function Start-Lab {
    Invoke-Compose build hub processor
    Invoke-Compose up -d --wait pg collector
    # Reapply ordered forward migrations without resetting existing schemas.
    Invoke-Compose run --rm --no-deps migrate
    # Both Hubs must be ready before processors subscribe and choose owners.
    Invoke-Compose up -d --no-deps --wait hub hub-2 lb
    Invoke-Compose up -d --no-deps --wait --scale processor=4 processor
    Write-Host "Owner recovery lab: http://127.0.0.1:$($PortBase + 10); metrics: http://127.0.0.1:$($PortBase + 89)/metrics"
}
function Test-Database {
    Invoke-Compose build tests
    Invoke-Compose up -d --wait test-pg
    Invoke-Compose run --rm --no-deps test-migrate
    # Unique output directory preserves every invocation's TRX, including failures.
    $outputId = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)
    $output = '/results/' + $outputId
    $testArgs = @('run','--rm','--no-deps','tests','--results-directory',$output)
    if ($Filter) { $testArgs += @('--filter',$Filter) }
    Invoke-Compose @testArgs
    $trxPath = Join-Path $resultsPath "tests/$outputId/owner-recovery.trx"
    if (!(Test-Path -LiteralPath $trxPath)) { throw 'Test runner produced no TRX result' }
    [xml]$trx = Get-Content -Raw -LiteralPath $trxPath
    $counts = $trx.TestRun.ResultSummary.Counters
    if ([int]$counts.executed -lt 1 -or [int]$counts.failed -gt 0) {
        throw 'Database test run failed or executed zero tests; check OWNER_TEST_FILTER and TRX'
    }
    Write-Host "Configured database tests: $($counts.passed) passed / $($counts.executed) executed. TRX: $trxPath"
}
function Test-Smoke {
    foreach ($workload in @('unordered','keyed')) {
        Invoke-Lab benchmark --label "smoke-$workload-$runId" --count 40 --mode $workload
    }
    Audit-Lab
}

# A supplied project cannot adopt or stop containers from another Compose suite.
$existing = @(& docker ps -aq --filter "label=com.docker.compose.project=$Project")
if ($LASTEXITCODE) { throw 'Cannot inspect Docker project' }
foreach ($container in $existing) {
    $label = & docker inspect --format '{{index .Config.Labels "cadence.owner-suite"}}' $container
    if ($LASTEXITCODE -or $label -ne $Project) { throw "Project $Project contains a container outside the ownership suite" }
}
if ($Task -in @('smoke-test','test','fault-test')) {
    & $Python --version
    if ($LASTEXITCODE) { throw 'Python is required for durable evidence audits' }
    $runId = ([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)).ToLowerInvariant()
    $runPath = Join-Path $resultsPath $runId
    New-Item -ItemType Directory -Path $runPath | Out-Null
    [ordered]@{ utc=[DateTime]::UtcNow.ToString('o'); project=$Project; portBase=$PortBase; task=$Task; commit=(& git rev-parse HEAD); status=@(& git status --short) } |
        ConvertTo-Json -Depth 4 | Set-Content -Encoding utf8 (Join-Path $runPath 'source.json')
    Write-Host "Evidence: $runPath"
}
switch ($Task) {
    config { Invoke-Compose config }
    build { Invoke-Compose build hub processor tests }
    up { Start-Lab }
    ps { Invoke-Compose ps -a }
    logs { Invoke-Compose logs --tail=100 hub hub-2 processor lb pg collector test-pg }
    down { Invoke-Compose down }
    db-test { Test-Database }
    smoke-test { Start-Lab; Test-Smoke }
    test { Test-Database; Start-Lab; Test-Smoke }
    fault-test {
        Start-Lab
        $victims = if ($Victim -eq 'both') { @('hub','hub-2') } else { @($Victim) }
        $modes = if ($Mode -eq 'both') { @('idle','busy') } else { @($Mode) }
        for ($trial = 1; $trial -le $Repeat; $trial++) {
            foreach ($target in $victims) {
                foreach ($laneMode in $modes) {
                    # Establish healthy preferred affinity before each independent case.
                    Invoke-Compose stop processor hub hub-2
                    Invoke-Compose up -d --no-deps --wait hub hub-2 lb
                    Invoke-Compose up -d --no-deps --wait --scale processor=4 processor
                    $faultArgs = @('experiment','--label',"modified-compose-$target-$laneMode-$trial-$runId",'--victim',$target,'--seconds','85')
                    if ($laneMode -eq 'busy') { $faultArgs += '--busy' }
                    Invoke-Lab @faultArgs
                    Audit-Lab
                }
            }
        }
    }
}
