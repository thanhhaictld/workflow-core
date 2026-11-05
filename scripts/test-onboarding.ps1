#Requires -Version 7.0
<#
  Real API/worker acceptance check for demo onboarding and workflow journal.
  Requires a migrated PostgreSQL database, live API, and a separate worker.
  No cloud resources or real tenant provisioning are performed.
#>
[CmdletBinding()]
param(
    [string] $ApiUrl = 'http://127.0.0.1:5000',
    [ValidateRange(10, 180)][int] $TimeoutSeconds = 90
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$RootUrl = $ApiUrl.TrimEnd('/')
$Suffix = [Guid]::NewGuid().ToString('N').Substring(0, 8)

function New-Onboarding([string] $Name, [bool] $Approval, [bool] $Retry, [bool] $Parallel = $false) {
    $tenantId = "accept-$Name-$Suffix"
    $json = @{
        tenantId = $tenantId
        organizationName = "Acceptance $Name"
        requiresApproval = $Approval
        simulateTransientFailure = $Retry
        parallel = $Parallel
    } | ConvertTo-Json -Compress
    $created = Invoke-RestMethod -Uri "$RootUrl/api/demo/onboardings" -Method Post -ContentType 'application/json' -Body $json
    if (-not $created.workflowId) { throw "Missing workflowId for $Name" }
    Write-Host "Created $Name onboarding $($created.workflowId) ($tenantId)"
    return $created.workflowId
}

function Get-History([string] $Id) {
    return Invoke-RestMethod -Uri "$RootUrl/api/workflows/$Id/history?take=1000"
}

function Assert-Event([string] $Id, [string] $Event) {
    $history = Get-History $Id
    if (-not @($history.items | Where-Object { $_.event.eventType -eq $Event }).Count) {
        throw "Workflow $Id did not record $Event"
    }
}

function Wait-For([string] $Id, [scriptblock] $Condition, [string] $Description) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        if (& $Condition $Id) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "Timed out waiting for $Description on workflow $Id"
}

function Is-Complete([string] $Id) {
    $detail = Invoke-RestMethod -Uri "$RootUrl/api/monitor/workflows/$Id"
    return $detail.workflow.status -eq 'Complete'
}

$happy = New-Onboarding 'happy' $false $false
$retry = New-Onboarding 'retry' $false $true
$approval = New-Onboarding 'approval' $true $false
$parallel = New-Onboarding 'parallel' $false $false $true

Wait-For $happy { param($id) Is-Complete $id } 'happy-path completion'
Assert-Event $happy 'WorkflowCompleted'
Assert-Event $happy 'StepAttemptStarted'

Wait-For $retry { param($id) Is-Complete $id } 'retry-path completion'
Assert-Event $retry 'StepBodyThrew'
Assert-Event $retry 'WorkflowCompleted'
$retryHistory = Get-History $retry
$workspaceAttempts = @($retryHistory.items | Where-Object {
    $_.event.eventType -eq 'StepAttemptStarted' -and
    $_.event.details -match 'ProvisionWorkspaceStep'
})
if ($workspaceAttempts.Count -lt 2) {
    throw "Expected >=2 workspace attempts in retry history, got $($workspaceAttempts.Count)"
}

Wait-For $approval {
    param($id)
    $h = Get-History $id
    return @($h.items | Where-Object { $_.event.eventType -eq 'StepWaitingForEvent' }).Count -gt 0
} 'approval waiting state'

# Publishing only after the event subscription is persisted tests wakeup.
$null = Invoke-RestMethod -Uri "$RootUrl/api/demo/onboardings/$approval/approve" -Method Post -ContentType 'application/json'
Wait-For $approval { param($id) Is-Complete $id } 'approval resumption'
Assert-Event $approval 'SignalDelivered'
Assert-Event $approval 'WorkflowCompleted'

# Verify the exact registered definition exposes three distinct branch roots.
$topology = Invoke-RestMethod -Uri "$RootUrl/api/monitor/workflows/$parallel/topology"
$forks = @($topology.steps | Where-Object { $_.kind -eq 'parallel' })
if ($forks.Count -ne 1 -or $forks[0].children.Count -ne 3) {
    throw "Parallel onboarding topology must contain one fork with three branch roots."
}
Wait-For $parallel { param($id) Is-Complete $id } 'parallel join completion'
Assert-Event $parallel 'WorkflowCompleted'
$parallelHistory = Get-History $parallel
$observedStepIds = @($parallelHistory.items | Where-Object { $_.event.eventType -eq 'StepAttemptStarted' } |
    ForEach-Object { $_.event.stepId } | Sort-Object -Unique)
foreach ($branchStart in $forks[0].children) {
    if ($branchStart -notin $observedStepIds) {
        throw "Parallel branch root $branchStart never executed."
    }
}
$completedJoin = @($parallelHistory.items | Where-Object {
    $_.event.eventType -eq 'StepCompleted' -and $_.event.stepId -eq $forks[0].id
})
if ($completedJoin.Count -lt 1) {
    throw "Parallel Sequence did not complete its branch join."
}
Write-Host "All onboarding workflows succeeded: happy=$happy retry=$retry approval=$approval parallel=$parallel"
