#Requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateSet('help','setup','config','build','up','ps','logs','down','db-test','smoke-test','stress-test','test','test-4k','fault-test')]
    [string]$Task = 'help',
    [string]$Project = '',
    [string]$ComposeFile = 'scaling/observability/compose.yaml',
    [string]$EnvFile = 'scaling/observability/.env',
    [ValidateRange(0,65200)][int]$PortBase = 0,
    [string]$Results = 'artifacts/owner-recovery',
    [string]$Python = 'python',
    [string]$Filter = '',
    [ValidateSet('both','hub','hub-2')][string]$Victim = 'both',
    [ValidateSet('both','idle','busy')][string]$Mode = 'both',
    [ValidateRange(1,10)][int]$Repeat = 1,
    [ValidateSet('unordered','partition','keyed')][string]$StressMode = 'unordered',
    [ValidateRange(1,1000000)][int]$Count = 300,
    [ValidateRange(1,10000)][int]$Vus = 60,
    [ValidateRange(15,7200)][int]$TimeoutSeconds = 600
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ($Task -eq 'help') {
    Write-Host @'
Owner recovery on the existing Grafana/Prometheus/Tempo/Loki observability stack:
  make obs-owner-recovery-setup        Create OBS_ENV_FILE if missing
  make obs-owner-recovery-up           Build/start base stack + HA/shards/recovery overlays
  make obs-owner-recovery-up-off-metric-sampler  Same stack, without Hub SQL metric sampling
  make obs-owner-recovery-test         Run k6 smoke on the running stack (unordered/keyed)
  make obs-owner-recovery-test-4k      Run 4,000 workflows with 400 VUs on the running stack
  make obs-owner-recovery-stress-test  Existing k6-policy workload + durable audit
  make obs-owner-recovery-db-test      Isolated test project, base postgres/migrate definitions
  make obs-owner-recovery-fault-test   Either owner, idle/busy, continuous tracing (85s each)
  make obs-owner-recovery-ps           Inspect the existing observability project
  make obs-owner-recovery-logs         Recent logs
  make obs-owner-recovery-down         Remove containers/networks; retain all data volumes
Config uses OBS_COMPOSE_FILE and OBS_ENV_FILE; the project matches compose.ps1.
K6_POLICY_MODE=unordered|partition|keyed K6_POLICY_COUNT=300 K6_POLICY_VUS=60
OWNER_PORT_BASE=0 keeps .env ports; a nonzero base assigns distinct loopback ports.
OWNER_FAULT_VICTIM=hub|hub-2|both OWNER_FAULT_MODE=idle|busy|both OWNER_FAULT_REPEAT=2
Without Make: pwsh -NoProfile -File scripts/owner-recovery.ps1 test
'@
    exit 0
}
Set-Location -LiteralPath $root
if ($Task -eq 'setup') {
    if (Test-Path -LiteralPath $EnvFile) { Write-Host "Keeping existing $EnvFile" }
    else {
        Copy-Item -LiteralPath scaling/observability/.env.example -Destination $EnvFile
        Write-Host "Created $EnvFile with local development defaults."
    }
    exit 0
}
if (!(Test-Path -LiteralPath $EnvFile)) { throw 'Run make obs-setup or make obs-owner-recovery-setup first' }
$baseFile = (Resolve-Path -LiteralPath $ComposeFile).Path
if (!$Project) {
    # Same worktree identity as scaling/observability/compose.ps1: one runtime stack.
    $identityPath = if ($IsWindows) { $root.ToLowerInvariant() } else { $root }
    $bytes = [Text.Encoding]::UTF8.GetBytes($identityPath)
    $suffix = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).Substring(0,16).ToLowerInvariant()
    $Project = "workflow-hub-observability-$suffix"
}
if ($Project -notmatch '^[a-z0-9][a-z0-9-]{1,47}$') { throw 'Use a lowercase Docker-safe project name (2-48 characters)' }
$testProject = "$Project-owner-tests"
$resultsPath = [IO.Path]::GetFullPath($Results, $root)
New-Item -ItemType Directory -Path (Join-Path $resultsPath 'tests') -Force | Out-Null
$env:WFC_OBSERVABILITY_PROJECT = $Project
$env:OWNER_RECOVERY_RESULTS = $resultsPath.Replace('\','/')
if ($PortBase) {
    if ($PortBase -lt 1024) { throw 'A nonzero port base must be >=1024' }
    $env:HUB_PORT = "$PortBase"
    $env:HUB2_PORT = "$($PortBase + 1)"
    $env:HUB_LB_PORT = "$($PortBase + 10)"
    $env:GRAFANA_PORT = "$($PortBase + 20)"
    $env:MONITOR_PORT = "$($PortBase + 21)"
    $env:PROMETHEUS_PORT = "$($PortBase + 30)"
    $env:TEMPO_PORT = "$($PortBase + 40)"
    $env:OTLP_HTTP_PORT = "$($PortBase + 50)"
    $env:OWNER_METRICS_PORT = "$($PortBase + 89)"
}
$files = @('-f',$baseFile,'-f','scaling/observability/compose.matching-ha.yaml',
    '-f','scaling/observability/compose.matching-shards.yaml','-f','scaling/observability/compose.owner-recovery.yaml',
    '--env-file',$EnvFile,'--profile','offboarding-load','--profile','policy-k6','--profile','owner-tests')
$composeArgs = @('compose','--project-name',$Project) + $files
$testComposeArgs = @('compose','--project-name',$testProject) + $files
function Invoke-Compose {
    & docker @composeArgs @args
    if ($LASTEXITCODE) { throw "Docker Compose failed ($LASTEXITCODE): $args" }
}
function Invoke-TestCompose {
    & docker @testComposeArgs @args
    if ($LASTEXITCODE) { throw "Test Compose failed ($LASTEXITCODE): $args" }
}
function Assert-Project([string]$Name) {
    $ids = @(& docker ps -aq --filter "label=com.docker.compose.project=$Name")
    if ($LASTEXITCODE) { throw 'Cannot inspect Docker project' }
    foreach ($id in $ids) {
        $configFiles = & docker inspect --format '{{index .Config.Labels "com.docker.compose.project.config_files"}}' $id
        $first = $configFiles.Split(',')[0].Replace('\','/')
        $expected = $baseFile.Replace('\','/')
        if ($IsWindows) { $first = $first.ToLowerInvariant(); $expected = $expected.ToLowerInvariant() }
        if ($LASTEXITCODE -or $first -ne $expected) { throw "Project $Name contains a container from another Compose base" }
    }
}
Assert-Project $Project
Assert-Project $testProject
$modelRaw = & docker @composeArgs config --format json
if ($LASTEXITCODE) { throw 'Invalid observability Compose configuration' }
$model = $modelRaw | ConvertFrom-Json
function Port([string]$Service, [int]$Target) {
    return [int](@($model.services.$Service.ports | Where-Object target -EQ $Target)[0].published)
}
$hubPort = Port hub 5100
$hub2Port = Port hub-2 5100
$lbPort = Port hub-lb 5100
$metricsPort = Port otel-collector 9464
$grafanaPort = Port grafana 3000
$prometheusPort = Port prometheus 9090
$pgUser = [string]$model.services.postgres.environment.POSTGRES_USER
$pgDatabase = [string]$model.services.postgres.environment.POSTGRES_DB
$env:OWNER_RECOVERY_TOKEN = [string]$model.services.hub.environment.WFC_HUB_AUTH_TOKEN
function Invoke-Lab {
    & $Python scaling/observability/benchmark/owner_recovery_lab.py @args --project $Project --compose-project $Project --observability --compose-base-file $baseFile `
        --hub-port $hubPort --hub2-port $hub2Port --lb-port $lbPort --metrics-port $metricsPort `
        --db-user $pgUser --database $pgDatabase --evidence-dir $runPath
    if ($LASTEXITCODE) { throw "Ownership lab failed ($LASTEXITCODE): $args" }
}
function Audit-Lab {
    & $Python scaling/observability/benchmark/analyze_owner_recovery.py --evidence-dir $runPath
    if ($LASTEXITCODE) { throw 'Durable workflow/fault audit failed' }
}
function Wait-LabReady {
    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        try {
            $defs = Invoke-RestMethod "http://127.0.0.1:$lbPort/api/hub/definitions" -Headers @{Authorization="Bearer $env:OWNER_RECOVERY_TOKEN"} -TimeoutSec 3
            $ready = @($defs | Where-Object { $_.hubId -eq 'policy-benchmark' -and $_.definitionId -in @('policy-unordered','policy-partition-fifo','policy-keyed-fifo') }).Count -eq 3
            if ($ready) { break }
        } catch { }
        Start-Sleep -Seconds 1
    }
    if (!$ready) { throw 'Policy benchmark definitions did not become ready; run make obs-owner-recovery-up and inspect processor logs' }
}
function Start-Lab {
    Invoke-Compose build hub hub-2 processor monitor
    Invoke-Compose up -d --wait postgres otel-collector prometheus tempo loki grafana postgres-process-exporter
    Invoke-Compose run --rm --no-deps migrate
    Invoke-Compose up -d --no-deps --wait hub hub-2 hub-lb monitor
    Invoke-Compose up -d --no-deps --wait --scale processor=8 processor
    Wait-LabReady
    Write-Host "Hub: http://127.0.0.1:$lbPort; Grafana: http://127.0.0.1:$grafanaPort; Prometheus: http://127.0.0.1:$prometheusPort"
}
function Test-Database {
    Invoke-TestCompose build tests
    Invoke-TestCompose up -d --wait postgres
    Invoke-TestCompose run --rm --no-deps migrate
    $outputId = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)
    $testArgs = @('run','--rm','--no-deps','tests','--results-directory',"/results/$outputId")
    if ($Filter) { $testArgs += @('--filter',$Filter) }
    Invoke-TestCompose @testArgs
    $trxPath = Join-Path $resultsPath "tests/$outputId/owner-recovery.trx"
    if (!(Test-Path -LiteralPath $trxPath)) { throw 'Test runner produced no TRX result' }
    [xml]$trx = Get-Content -Raw -LiteralPath $trxPath
    $counts = $trx.TestRun.ResultSummary.Counters
    if ([int]$counts.executed -lt 1 -or [int]$counts.failed -gt 0) { throw 'Database run failed or executed zero tests; check filter and TRX' }
    Write-Host "Configured database tests: $($counts.passed) passed / $($counts.executed) executed. TRX: $trxPath"
}
function Test-K6([string]$Workload, [int]$Total, [int]$VirtualUsers = $Vus) {
    $label = "k6-$Workload-$runId"
    $cohortPath = Join-Path $runPath $label
    New-Item -ItemType Directory -Path $cohortPath | Out-Null
    Invoke-Compose run --rm --no-deps -v "$($cohortPath.Replace('\','/')):/results" `
        -e "K6_POLICY_MODE=$Workload" -e "K6_POLICY_COUNT=$Total" -e "K6_POLICY_VUS=$([Math]::Min($Total,$VirtualUsers))" `
        -e "K6_POLICY_RUN_ID=$label" -e "K6_POLICY_TIMEOUT_SECONDS=$TimeoutSeconds" k6-policy
    Invoke-Lab export --label $label --count $Total
    Audit-Lab
}
if ($Task -in @('smoke-test','stress-test','test','test-4k','fault-test')) {
    & $Python --version
    if ($LASTEXITCODE) { throw 'Python is required for durable evidence audits' }
    $runId = ([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)).ToLowerInvariant()
    $runPath = Join-Path $resultsPath $runId
    New-Item -ItemType Directory -Path $runPath | Out-Null
    [ordered]@{ utc=[DateTime]::UtcNow.ToString('o'); project=$Project; task=$Task; commit=(& git rev-parse HEAD); status=@(& git status --short) } |
        ConvertTo-Json -Depth 4 | Set-Content -Encoding utf8 (Join-Path $runPath 'source.json')
    Write-Host "Evidence: $runPath"
}
switch ($Task) {
    config { Invoke-Compose config }
    build { Invoke-Compose build hub hub-2 processor monitor; Invoke-TestCompose build tests }
    up { Start-Lab }
    ps { Invoke-Compose ps -a; Invoke-TestCompose ps -a }
    logs { Invoke-Compose logs --tail=100 hub hub-2 processor hub-lb postgres otel-collector grafana prometheus }
    down { Invoke-Compose down; Invoke-TestCompose down }
    db-test { Test-Database }
    smoke-test { Wait-LabReady; Test-K6 unordered 40; Test-K6 keyed 40 }
    stress-test { Wait-LabReady; Test-K6 $StressMode $Count }
    test { Wait-LabReady; Test-K6 unordered 40; Test-K6 keyed 40 }
    test-4k { Wait-LabReady; Test-K6 $StressMode 4000 100 }
    fault-test {
        Start-Lab
        $victims = if ($Victim -eq 'both') { @('hub','hub-2') } else { @($Victim) }
        $modes = if ($Mode -eq 'both') { @('idle','busy') } else { @($Mode) }
        for ($trial = 1; $trial -le $Repeat; $trial++) {
            foreach ($target in $victims) {
                foreach ($laneMode in $modes) {
                    Invoke-Compose stop processor hub hub-2
                    Invoke-Compose up -d --no-deps --wait hub hub-2 hub-lb
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
