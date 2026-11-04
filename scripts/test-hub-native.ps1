#Requires -Version 7.0
[CmdletBinding()]
param(
    [string]$ApiUrl = 'http://127.0.0.1:5100',
    [string]$HubToken = $env:WFC_HUB_AUTH_TOKEN,
    [int]$TimeoutSeconds = 60,
    [switch]$ManageHosts,
    [string]$ConnectionString = $env:WFC_TEST_PG
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$headers = @{ Authorization = "Bearer $HubToken" }
$runId = 'native-smoke-' + [guid]::NewGuid().ToString('N')
$managed = [Collections.Generic.List[object]]::new()

function Start-ManagedHost([string]$Project, [switch]$RegisterOnly) {
    $binary = Join-Path $root "scaling/sample/onboarding/$Project/bin/Release/net10.0/$Project.dll"
    if (-not (Test-Path -LiteralPath $binary)) { throw "Build the Release scaling solution first: $binary" }
    $start = [Diagnostics.ProcessStartInfo]::new('dotnet')
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.WorkingDirectory = $root
    $start.ArgumentList.Add($binary)
    $start.Environment['WFC_HUB_AUTH_TOKEN'] = $HubToken
    $start.Environment['WFC_HUB_URL'] = $ApiUrl
    $start.Environment['Logging__LogLevel__Default'] = 'Warning'
    $start.Environment['OTEL_SDK_DISABLED'] = 'true'
    $start.Environment.Remove('WFC_PG') | Out-Null
    $start.Environment.Remove('WFC_TEST_PG') | Out-Null
    if ($Project -eq 'WorkflowCore.Hub.Host') {
        $start.Environment['WFC_PG'] = $ConnectionString
        $start.ArgumentList.Add('--urls')
        $start.ArgumentList.Add($ApiUrl)
    }
    if ($RegisterOnly) { $start.ArgumentList.Add('--register-only') }
    $process = [Diagnostics.Process]::Start($start)
    $hostState = [pscustomobject]@{
        Process = $process
        Output = $process.StandardOutput.ReadToEndAsync()
        ErrorOutput = $process.StandardError.ReadToEndAsync()
    }
    $managed.Add($hostState)
    return $hostState
}
function Stop-ManagedHost($HostState) {
    if (-not $HostState.Process.HasExited) {
        # Force process death to exercise durable recovery rather than graceful drain.
        $HostState.Process.Kill($true)
        $HostState.Process.WaitForExit(5000) | Out-Null
    }
}
function Wait-For([scriptblock]$Condition, [string]$Description) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        if (& $Condition) { return }
        Start-Sleep -Milliseconds 100
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out: $Description"
}
function Call-Hub([string]$Method, [string]$Path, $Body = $null) {
    $parameters = @{ Uri = "$ApiUrl/api/hub/$Path"; Method = $Method; Headers = $headers; TimeoutSec = 10 }
    if ($null -ne $Body) { $parameters.ContentType = 'application/json'; $parameters.Body = ConvertTo-Json -InputObject $Body -Depth 30 }
    $response = Invoke-RestMethod @parameters
    return $response
}
function Start-Workflow([int]$Version, [string]$Definition = 'tenant-onboarding') {
    $body = @{ hubId = 'onboarding'; definitionId = $Definition; version = $Version; runId = $runId
        idempotencyKey = "$runId-$Version-$Definition"; input = @{ tenantId = "$runId-v$Version" } }
    $id = (Call-Hub POST workflows $body).workflowId
    if ((Call-Hub POST workflows $body).workflowId -ne $id) { throw 'Start idempotency failed' }
    return $id
}
function Wait-Completed([string]$Id) {
    Wait-For { (Call-Hub GET "workflows/$Id").state -in @('Complete', 'Failed', 'Terminated') } "workflow $Id completion"
    if ((Call-Hub GET "workflows/$Id").state -ne 'Complete') { throw "Workflow $Id failed" }
}
function Assert-NativeResult([string]$Id, [int]$Steps) {
    $runtime = Call-Hub GET "workflows/$Id/native-runtime"
    $data = $runtime.snapshot.Data.value
    if (-not $data.Validated -or -not $data.WelcomeSent -or $data.WorkspaceId -ne "workspace-$($data.TenantId)") {
        throw 'Native input/output mappings were not preserved'
    }
    $history = @(Call-Hub GET "workflows/$Id/history")
    $completed = @($history | Where-Object eventType -eq StepCompleted)
    if ($completed.Count -ne $Steps -or @($completed.executionPointerId | Sort-Object -Unique).Count -ne $Steps) {
        throw "Native progression mismatch: expected $Steps completions, got $($completed.Count), distinct pointers $(@($completed.executionPointerId | Sort-Object -Unique).Count)"
    }
    $trace = @(Call-Hub GET "workflows/$Id/trace")
    if ($trace.Count -ne 3 -or @($trace | Where-Object { -not $_.executionPointerId -or -not $_.dispatchKey }).Count) {
        throw 'Activity trace is missing native pointer/dispatch identities'
    }
}

try {
    if ([string]::IsNullOrWhiteSpace($HubToken)) { throw 'Set WFC_HUB_AUTH_TOKEN or pass -HubToken' }
    if ($ManageHosts) {
        if ([string]::IsNullOrWhiteSpace($ConnectionString)) { throw '-ManageHosts requires WFC_TEST_PG or -ConnectionString, with migrations through 013 applied' }
        $hub = Start-ManagedHost WorkflowCore.Hub.Host
        Wait-For { try { (Invoke-RestMethod "$ApiUrl/health/ready" -TimeoutSec 2).status -eq 'ready' } catch { $false } } 'Hub readiness'
        $publisher = Start-ManagedHost WorkflowCore.Provisioner.Host -RegisterOnly
        if (-not $publisher.Process.WaitForExit(30000) -or $publisher.Process.ExitCode -ne 0) { throw 'Register-only publication failed' }
        $provisioner = Start-ManagedHost WorkflowCore.Provisioner.Host
    }
    $native = Start-Workflow 3
    Wait-Completed $native
    Assert-NativeResult $native 3
    $timer = Start-Workflow 3 tenant-onboarding-timer
    Wait-For { @((Call-Hub GET "workflows/$timer/history") | Where-Object eventType -eq TimerScheduled).Count -gt 0 } 'durable timer wait'
    if ($ManageHosts) {
        Stop-ManagedHost $provisioner
        Stop-ManagedHost $hub
        $hub = Start-ManagedHost WorkflowCore.Hub.Host
        Wait-For { try { (Invoke-RestMethod "$ApiUrl/health/ready" -TimeoutSec 2).status -eq 'ready' } catch { $false } } 'restarted Hub readiness'
        $provisioner = Start-ManagedHost WorkflowCore.Provisioner.Host
    }
    Wait-Completed $timer
    Assert-NativeResult $timer 4
    $history = @(Call-Hub GET "workflows/$timer/history")
    if (@($history | Where-Object eventType -eq TimerFired).Count -lt 1) { throw 'Durable timer did not fire' }
    Write-Output "PASS: native v3 mappings, idempotent starts, durable wait, pointer/attempt history; process restart=$ManageHosts"
    Write-Output "Native workflow: $native; timer workflow: $timer"
} catch {
    foreach ($hostState in $managed) {
        Stop-ManagedHost $hostState
        Write-Warning ($hostState.Output.GetAwaiter().GetResult() + $hostState.ErrorOutput.GetAwaiter().GetResult())
    }
    throw
} finally {
    foreach ($hostState in $managed) { Stop-ManagedHost $hostState; $hostState.Process.Dispose() }
}
