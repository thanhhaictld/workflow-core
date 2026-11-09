#Requires -Version 7.0
<#
.SYNOPSIS
  Cross-platform local developer tasks for Workflow Core monitoring.
.EXAMPLE
  pwsh -NoProfile -File ./scripts/dev.ps1 bootstrap
  pwsh -NoProfile -File ./scripts/dev.ps1 up -PgPort 55433 -ApiPort 5001
  pwsh -NoProfile -File ./scripts/dev.ps1 db-reset -ConfirmReset
.NOTES
  Windows, macOS and Linux with PowerShell 7+. Requires Docker Compose v2 for
  database tasks, .NET SDK 10, Node.js 22+, and Corepack. GNU Make is optional.
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('help', 'check-tools', 'bootstrap', 'setup', 'up', 'dev',
        'api', 'worker', 'hub', 'remote-worker', 'provisioner', 'ui', 'install', 'db-up', 'db-ef', 'db-schema', 'db-down',
        'db-logs', 'db-reset', 'seed', 'approve', 'test', 'build', 'check', 'clean')]
    [string] $Task = 'help',
    [ValidateRange(0, 65535)][int] $PgPort = 0,
    [ValidateRange(0, 65535)][int] $ApiPort = 0,
    [ValidateRange(0, 65535)][int] $UiPort = 0,
    [ValidateRange(0, 65535)][int] $HubPort = 0,
    [string] $PgUser,
    [string] $PgPassword,
    [string] $PgDatabase,
    [switch] $ConfirmReset,
    [Guid] $WorkflowId = [Guid]::Empty,
    # CI runs on macOS/Windows where Docker is not always available.
    [switch] $SkipDocker
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$MonitorDir = Join-Path $RepoRoot 'scaling/monitor'
$ComposeFile = Join-Path $RepoRoot 'scaling/compose.monitor.yaml'
$ApiProject = Join-Path $RepoRoot 'scaling/monitor/api/WorkflowCore.Scaling.Api.csproj'
$WorkerProject = Join-Path $RepoRoot 'scaling/sample/onboarding/WorkflowCore.Scaling.Worker/WorkflowCore.Scaling.Worker.csproj'
$HubProject = Join-Path $RepoRoot 'scaling/src/WorkflowCore.Hub.Server/WorkflowCore.Hub.Server.csproj'
$ProvisionerProject = Join-Path $RepoRoot 'scaling/sample/onboarding/WorkflowCore.Provisioner.Host/WorkflowCore.Provisioner.Host.csproj'
$TestProject = Join-Path $RepoRoot 'scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj'
$CorepackCommand = if ($IsWindows) { 'corepack.cmd' } else { 'corepack' }

if ($PgPort -eq 0) { $PgPort = if ($env:PG_PORT) { [int]$env:PG_PORT } else { 55432 } }
if ($ApiPort -eq 0) { $ApiPort = if ($env:API_PORT) { [int]$env:API_PORT } else { 5000 } }
if ($UiPort -eq 0) { $UiPort = if ($env:UI_PORT) { [int]$env:UI_PORT } else { 3002 } }
if ($HubPort -eq 0) { $HubPort = if ($env:HUB_PORT) { [int]$env:HUB_PORT } else { 5100 } }
if (-not $PgUser) { $PgUser = if ($env:PG_USER) { $env:PG_USER } else { 'workflow' } }
if (-not $PgPassword) { $PgPassword = if ($env:PG_PASSWORD) { $env:PG_PASSWORD } else { 'workflow_dev_only' } }
if (-not $PgDatabase) { $PgDatabase = if ($env:PG_DATABASE) { $env:PG_DATABASE } else { 'workflow' } }
if ($env:CONFIRM -eq 'reset') { $ConfirmReset = $true }

if (@($PgPort, $ApiPort, $UiPort, $HubPort) | Where-Object { $_ -lt 1 }) {
    throw 'Port settings must be in range 1..65535.'
}
foreach ($setting in @(@('PG_PORT', "$PgPort"), @('PG_USER', $PgUser),
    @('PG_PASSWORD', $PgPassword), @('PG_DATABASE', $PgDatabase))) {
    [Environment]::SetEnvironmentVariable($setting[0], $setting[1], 'Process')
}
$LocalConnection = "Host=127.0.0.1;Port=$PgPort;Database=$PgDatabase;Username=$PgUser;Password=$PgPassword"

function Assert-Executable([string] $Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Missing required tool '$Name'. See scaling/monitor/README.md."
    }
}

function Invoke-External {
    param(
        [Parameter(Mandatory)][string] $Executable,
        [string[]] $Arguments = @(),
        [string] $Directory = $RepoRoot
    )
    Push-Location -LiteralPath $Directory
    try {
        & $Executable @Arguments
        if ($LASTEXITCODE -ne 0) { throw "$Executable exited with code $LASTEXITCODE." }
    } finally {
        Pop-Location
    }
}

function Invoke-Compose([string[]] $ComposeArgs) {
    Invoke-External -Executable 'docker' -Arguments (@('compose', '-f', $ComposeFile) + $ComposeArgs)
}

function Test-Prerequisites([bool] $WithDocker = $true) {
    foreach ($name in @('dotnet', 'node', $CorepackCommand)) { Assert-Executable $name }
    $sdks = @(& dotnet --list-sdks)
    if ($LASTEXITCODE -ne 0 -or -not ($sdks | Where-Object { $_ -match '^10\.' })) {
        throw '.NET SDK 10 is required.'
    }
    $nodeVersion = ((& node --version) -replace '^v', '')
    if ($LASTEXITCODE -ne 0 -or [version]$nodeVersion -lt [version]'22.0.0') {
        throw 'Node.js 22+ is required.'
    }
    if ($WithDocker) {
        Assert-Executable 'docker'
        Invoke-External -Executable 'docker' -Arguments @('compose', 'version')
    }
    Write-Host "Prerequisites OK: .NET 10, Node $nodeVersion, Corepack$(if ($WithDocker) { ', Docker Compose' })."
}

function Start-Database {
    Invoke-Compose -ComposeArgs @('up', '-d', '--wait', 'postgres')
}

function Apply-EfMigrations {
    Start-Database
    Write-Host 'Applying Workflow Core EF migrations...'
    $env:WFC_PG = $LocalConnection
    $env:WFC_MIGRATE_ONLY = '1'
    try {
        Invoke-External -Executable 'dotnet' -Arguments @('run', '--no-launch-profile', '--project', $ApiProject)
    } finally {
        Remove-Item Env:WFC_MIGRATE_ONLY -ErrorAction SilentlyContinue
    }
}

function Apply-DatabaseSchema {
    Apply-EfMigrations
    foreach ($migration in @('002_distributed_runtime.sql', '003_signal_delivery.sql',
        '004_request_dedupe.sql', '005_execution_history.sql', '006_definition_versions.sql',
        '007_hub_gateway.sql', '008_hub_workflows.sql', '009_hub_task_attempts.sql',
        '010_hub_remote_scheduler.sql', '011_hub_monitor_sample.sql', '012_hub_native_runtime.sql',
        '013_activity_policy_revision.sql', '014_hub_work_ready_notifications.sql',
        '015_explicit_activity_ordering.sql', '016_hub_matching_ownership.sql',
        '017_hub_matching_capability_hints.sql',
        '018_hub_matching_partition_ownership.sql', '019_hub_matching_partition_hints.sql',
        '020_hub_reconciliation_indexes.sql', '021_hub_active_task_indexes.sql',
        '022_hub_claim_index_selectivity.sql', '023_hub_claim_hub_sequence.sql',
        '024_hub_native_reconcile_tasks.sql')) {
        Write-Host "Applying $migration..."
        Invoke-Compose -ComposeArgs @('exec', '-T', 'postgres', 'psql', '-U', $PgUser,
            '-d', $PgDatabase, '-v', 'ON_ERROR_STOP=1', '-f', "/migrations/$migration")
    }
}

function Install-Monitor {
    Invoke-External -Executable $CorepackCommand -Arguments @('pnpm', 'install', '--frozen-lockfile') -Directory $MonitorDir
    $localEnv = Join-Path $MonitorDir '.env.local'
    if (-not (Test-Path $localEnv)) {
        Copy-Item -LiteralPath (Join-Path $MonitorDir '.env.example') -Destination $localEnv
        Write-Host 'Created scaling/monitor/.env.local'
    }
}

function Invoke-Bootstrap {
    Test-Prerequisites
    Apply-DatabaseSchema
    Install-Monitor
    Write-Host 'Bootstrap complete.'
}

function Start-Api {
    Apply-DatabaseSchema
    $env:WFC_PG = $LocalConnection
    $env:WFC_EMBEDDED_WORKER = 'false'
    Invoke-External -Executable 'dotnet' -Arguments @('run', '--no-launch-profile', '--project',
        $ApiProject, '--urls', "http://127.0.0.1:$ApiPort")
}

function Start-Worker {
    Apply-DatabaseSchema
    $env:WFC_PG = $LocalConnection
    Invoke-External -Executable 'dotnet' -Arguments @('run', '--no-launch-profile', '--project', $WorkerProject)
}

function Start-Hub {
    if (-not $env:WFC_HUB_AUTH_TOKEN) { throw "Set WFC_HUB_AUTH_TOKEN (24+ characters) before starting the Hub" }
    Apply-DatabaseSchema
    $env:WFC_PG = $LocalConnection
    Invoke-External -Executable 'dotnet' -Arguments @('run', '--no-launch-profile', '--project',
        $HubProject, '--urls', "http://127.0.0.1:$HubPort")
}

function Start-RemoteWorker {
    if (-not $env:WFC_HUB_AUTH_TOKEN) { throw 'Set WFC_HUB_AUTH_TOKEN' }
    if (-not $env:WFC_HUB_URL) { $env:WFC_HUB_URL = "http://127.0.0.1:$HubPort" }
    Remove-Item Env:WFC_PG -ErrorAction SilentlyContinue
    Invoke-External -Executable 'dotnet' -Arguments @('run', '--no-launch-profile', '--project', $WorkerProject)
}

function Start-Ui {
    Install-Monitor
    $env:WORKFLOW_API_URL = "http://127.0.0.1:$ApiPort"
    Invoke-External -Executable $CorepackCommand -Arguments @('pnpm', 'run', 'dev',
        '--hostname', '127.0.0.1', '--port', "$UiPort") -Directory $MonitorDir
}

# Start both programs without Unix shell operators or package-manager subprocesses.
function Start-ChildProcess([string] $Executable, [string[]] $Arguments,
    [string] $Directory) {
    $command = Get-Command $Executable -ErrorAction Stop
    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = $command.Source
    $psi.WorkingDirectory = $Directory
    $psi.UseShellExecute = $false
    foreach ($argument in $Arguments) { [void] $psi.ArgumentList.Add($argument) }
    $process = [System.Diagnostics.Process]::Start($psi)
    if (-not $process) { throw "Unable to launch $Executable" }
    return $process
}

function Stop-ChildProcess([System.Diagnostics.Process] $Process) {
    if ($null -eq $Process) { return }
    try {
        if (-not $Process.HasExited) {
            $Process.Kill($true)
            [void] $Process.WaitForExit(5000)
        }
    } catch [System.InvalidOperationException] {
        # The child exited between HasExited and Kill.
    } finally {
        $Process.Dispose()
    }
}

function Start-Development {
    Invoke-Bootstrap
    $env:WFC_PG = $LocalConnection
    $env:WFC_EMBEDDED_WORKER = 'false'
    $env:WORKFLOW_API_URL = "http://127.0.0.1:$ApiPort"
    Write-Host "API: http://127.0.0.1:$ApiPort | Dashboard: http://127.0.0.1:$UiPort/workflows"
    $api = $null
    $worker = $null
    $ui = $null
    try {
        $api = Start-ChildProcess -Executable 'dotnet' -Arguments @('run', '--no-launch-profile', '--project',
            $ApiProject, '--urls', "http://127.0.0.1:$ApiPort") -Directory $RepoRoot
        $worker = Start-ChildProcess -Executable 'dotnet' -Arguments @('run', '--no-launch-profile', '--project',
            $WorkerProject) -Directory $RepoRoot
        $nextCli = Join-Path $MonitorDir 'node_modules/next/dist/bin/next'
        if (-not (Test-Path $nextCli)) { throw "Missing Next.js CLI: $nextCli" }
        $ui = Start-ChildProcess -Executable 'node' -Arguments @($nextCli, 'dev', '--hostname', '127.0.0.1',
            '--port', "$UiPort") -Directory $MonitorDir
        while (-not $api.HasExited -and -not $worker.HasExited -and -not $ui.HasExited) {
            Start-Sleep -Milliseconds 500
        }
        throw 'A development process exited. Check its console output.'
    } finally {
        Stop-ChildProcess $ui
        Stop-ChildProcess $worker
        Stop-ChildProcess $api
    }
}

function Show-Help {
    Write-Host @'
Workflow Core Monitor — PowerShell 7+ (Windows, macOS, Linux)
  pwsh ./scripts/dev.ps1 bootstrap   PostgreSQL + EF/SQL migrations + pnpm
  pwsh ./scripts/dev.ps1 up          Bootstrap and start API + worker + UI
  pwsh ./scripts/dev.ps1 dev         Alias for up
  pwsh ./scripts/dev.ps1 api         Run .NET API only (no embedded workers)
  pwsh ./scripts/dev.ps1 worker      Run legacy PostgreSQL-backed worker
  pwsh ./scripts/dev.ps1 hub         Run standalone REST+SSE Hub gateway (set WFC_HUB_AUTH_TOKEN)
  pwsh ./scripts/dev.ps1 remote-worker  Run legacy Hub activity worker without PostgreSQL
  pwsh ./scripts/dev.ps1 provisioner  Run native workflow/activity Provisioner (no DB)
  pwsh ./scripts/dev.ps1 ui          Run Next.js dashboard only
  pwsh ./scripts/dev.ps1 seed        Create happy, retry, approval and parallel runs
  pwsh ./scripts/dev.ps1 approve -WorkflowId UUID   Signal onboarding approval
  pwsh ./scripts/dev.ps1 test        PostgreSQL integration tests
  pwsh ./scripts/dev.ps1 build       Build API, worker and Next.js
  pwsh ./scripts/dev.ps1 check       Type-check Next.js
  pwsh ./scripts/dev.ps1 db-up       Start PostgreSQL 17
  pwsh ./scripts/dev.ps1 db-ef       Apply EF migrations
  pwsh ./scripts/dev.ps1 db-schema   Apply EF + 002–024 migrations
  pwsh ./scripts/dev.ps1 db-down     Stop PostgreSQL (keep data)
  pwsh ./scripts/dev.ps1 db-logs     Tail PostgreSQL logs
  pwsh ./scripts/dev.ps1 db-reset -ConfirmReset    DELETE local DB volume
  pwsh ./scripts/dev.ps1 clean       Delete Next.js build cache

Port overrides: -PgPort 55433 -ApiPort 5001 -UiPort 3001 -HubPort 5101
Environment overrides: PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE, API_PORT, UI_PORT.
GNU Make shortcuts are available where Make is installed: make up / make test.
'@
}

try {
    switch ($Task) {
        'help'        { Show-Help }
        'check-tools' { Test-Prerequisites -WithDocker:(-not $SkipDocker) }
        'db-up'       { Start-Database }
        'db-ef'       { Apply-EfMigrations }
        'db-schema'   { Apply-DatabaseSchema }
        'install'     { Install-Monitor }
        'bootstrap'   { Invoke-Bootstrap }
        'setup'       { Invoke-Bootstrap }
        'up'          { Start-Development }
        'dev'         { Start-Development }
        'api'         { Start-Api }
        'worker'      { Start-Worker }
        'hub'         { Start-Hub }
        'remote-worker' { Start-RemoteWorker }
        'provisioner' {
            if (-not $env:WFC_HUB_AUTH_TOKEN) { throw 'Set WFC_HUB_AUTH_TOKEN' }
            if (-not $env:WFC_HUB_URL) { $env:WFC_HUB_URL = "http://127.0.0.1:$HubPort" }
            Remove-Item Env:WFC_PG -ErrorAction SilentlyContinue
            Invoke-External -Executable 'dotnet' -Arguments @('run', '--no-launch-profile', '--project', $ProvisionerProject)
        }
        'ui'          { Start-Ui }
        'seed' {
            foreach ($scenario in @(
                @{ Name = 'complete'; Approval = $false; Retry = $false; Parallel = $false },
                @{ Name = 'retry'; Approval = $false; Retry = $true; Parallel = $false },
                @{ Name = 'await-approval'; Approval = $true; Retry = $false; Parallel = $false },
                @{ Name = 'parallel'; Approval = $false; Retry = $false; Parallel = $true }
            )) {
                $tenant = "demo-$($scenario.Name)-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
                $payload = @{
                    tenantId = $tenant
                    organizationName = "Sample $($scenario.Name) organization"
                    requiresApproval = $scenario.Approval
                    simulateTransientFailure = $scenario.Retry
                    parallel = $scenario.Parallel
                } | ConvertTo-Json -Compress
                $created = Invoke-RestMethod -Uri "http://127.0.0.1:$ApiPort/api/demo/onboardings" -Method Post -ContentType 'application/json' -Body $payload
                Write-Host "$($scenario.Name) | $tenant | Workflow: $($created.workflowId)"
                if ($scenario.Approval) {
                    Write-Host "Approve with: pwsh ./scripts/dev.ps1 approve -WorkflowId $($created.workflowId)"
                }
            }
        }
        'approve' {
            if ($WorkflowId -eq [Guid]::Empty) {
                throw 'Provide -WorkflowId with the onboarding workflow UUID.'
            }
            $result = Invoke-RestMethod -Uri "http://127.0.0.1:$ApiPort/api/demo/onboardings/$WorkflowId/approve" -Method Post -ContentType 'application/json'
            Write-Host "Approval submitted for $($result.workflowId) (event $($result.eventId))"
        }
        'test' {
            Apply-DatabaseSchema
            $env:WFC_TEST_PG = $LocalConnection
            Invoke-External -Executable 'dotnet' -Arguments @('test', $TestProject, '-c', 'Release')
        }
        'check' {
            Install-Monitor
            Invoke-External -Executable $CorepackCommand -Arguments @('pnpm', 'run', 'check-types') -Directory $MonitorDir
        }
        'build' {
            Install-Monitor
            Invoke-External -Executable 'dotnet' -Arguments @('build', $ApiProject, '-c', 'Release')
            Invoke-External -Executable 'dotnet' -Arguments @('build', $WorkerProject, '-c', 'Release')
            Invoke-External -Executable 'dotnet' -Arguments @('build', $HubProject, '-c', 'Release')
            Invoke-External -Executable $CorepackCommand -Arguments @('pnpm', 'run', 'build') -Directory $MonitorDir
        }
        'db-logs' { Invoke-Compose -ComposeArgs @('logs', '-f', 'postgres') }
        'db-down' { Invoke-Compose -ComposeArgs @('down') }
        'db-reset' {
            if (-not $ConfirmReset) {
                throw 'Refusing to delete local PostgreSQL. Pass -ConfirmReset or use make db-reset CONFIRM=reset.'
            }
            Invoke-Compose -ComposeArgs @('down', '-v')
            Write-Host 'Local PostgreSQL data deleted.'
        }
        'clean' {
            foreach ($generated in @('.next', 'tsconfig.tsbuildinfo')) {
                Remove-Item -LiteralPath (Join-Path $MonitorDir $generated) -Recurse -Force -ErrorAction SilentlyContinue
            }
        }
    }
} catch {
    [Console]::Error.WriteLine("Workflow Core setup failed: $($_.Exception.Message)")
    exit 1
}
