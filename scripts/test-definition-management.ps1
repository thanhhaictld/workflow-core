#Requires -Version 7.0
<#
  End-to-end code-first workflow metadata review and instance management.
  Version schema snapshots are descriptive: no lifecycle or default selection
  should change how code-first workflow execution works.
#>
[CmdletBinding()]
param(
    [string] $ApiUrl = 'http://127.0.0.1:5000',
    [ValidateRange(10,180)][int] $TimeoutSeconds = 90
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$RootUrl = $ApiUrl.TrimEnd('/')
$Name = 'tenant-onboarding-versioned'
$Suffix = [Guid]::NewGuid().ToString('N').Substring(0, 9)

function Request([string] $Path, [string] $Method = 'GET', [object] $Body = $null) {
    $args = @{ Uri = "$RootUrl/api/$Path"; Method = $Method }
    if ($null -ne $Body) {
        $args.ContentType = 'application/json'
        $args.Body = ($Body | ConvertTo-Json -Compress)
    }
    return Invoke-RestMethod @args
}
function Expect-Http([int] $Expected, [string] $Path, [string] $Method, [object] $Body) {
    $json = $Body | ConvertTo-Json -Compress
    $response = Invoke-WebRequest -Uri "$RootUrl/api/$Path" -Method $Method -ContentType 'application/json' -Body $json -SkipHttpErrorCheck
    if ([int]$response.StatusCode -ne $Expected) {
        throw "Expected HTTP $Expected for $Method $Path; got $($response.StatusCode): $($response.Content)"
    }
}
function Wait-Status([string] $Id, [string] $Expected) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        $item = Request "monitor/workflows/$Id"
        if ($item.workflow.status -eq $Expected) { return }
        Start-Sleep -Milliseconds 400
    }
    throw "Workflow $Id did not reach status $Expected"
}
function Start-Versioned([int] $Version) {
    return Request 'demo/versioned-onboardings' 'POST' @{
        tenantId = "version-$Version-$Suffix"
        organizationName = "Metadata review $Version"
        version = $Version
    }
}
$cap = Request 'manage/capabilities'
if (-not $cap.writesEnabled) { throw 'CI must explicitly enable WFC_ENABLE_MANAGEMENT_WRITES=true for instance status tests' }
$index = Request 'definitions'
$versions = @($index.versions | Where-Object { $_.definitionId -eq $Name })
if ($versions.Count -ne 2) { throw "Expected archived metadata for v1/v2, got $($versions.Count)" }
$originalHash = ($versions | Where-Object version -eq 1).schemaSha256
$diff = Request "definitions/$Name/diff?from=1&to=2"
if ($diff.changes.Count -lt 1) { throw 'Version metadata differs but structural diff is empty' }
if (-not @($diff.changes | Where-Object {
    $_.path -match '^steps/' -and ($_.after -match 'ConfigureAccessStep')
}).Count) {
    throw 'Diff must show ConfigureAccessStep added by the C# v2 workflow'
}
$schema = Request "definitions/$Name/versions/1"
if ($schema.schemaSha256 -ne $originalHash) { throw 'Metadata fingerprint mismatch' }
if ($null -ne $schema.PSObject.Properties['lifecycle'] -or
    $null -ne $schema.PSObject.Properties['isDefault']) {
    throw 'Metadata API must not expose version governance state'
}
$first = Start-Versioned 1
$second = Start-Versioned 2
if ($first.version -ne 1 -or $second.version -ne 2) {
    throw 'Runtime failed to pin a version from the code-first registry'
}
Wait-Status $first.workflowId 'Complete'
Wait-Status $second.workflowId 'Complete'
$immutable = Request "definitions/$Name/versions/1"
if ($immutable.schemaSha256 -ne $originalHash) { throw 'Archived metadata was mutated' }
# The old management routes must not be able to publish or change code-first versions.
Expect-Http 404 "definitions/$Name/versions/1/default" 'POST' @{}
Expect-Http 404 "definitions/$Name/versions/1/lifecycle" 'PUT' @{ lifecycle = 'Deprecated' }
Write-Host 'Code-first version starts, read-only snapshots and structural metadata diff passed.'

# The approval definition is long-lived enough to validate instance lifecycle.
$approvalId = "control-$Suffix"
$started = Request 'demo/onboardings' 'POST' @{
    tenantId = $approvalId
    organizationName = "Control flow acceptance"
    requiresApproval = $true
}
$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
$seenWaiting = $false
while ([DateTime]::UtcNow -lt $deadline) {
    $history = Request "workflows/$($started.workflowId)/history?take=300"
    if (@($history.items | Where-Object { $_.event.eventType -eq 'StepWaitingForEvent' }).Count -gt 0) {
        $seenWaiting = $true
        break
    }
    Start-Sleep -Milliseconds 500
}
if (-not $seenWaiting) { throw 'Approval workflow never reached waiting step' }
$null = Request "manage/workflows/$($started.workflowId)/status" 'POST' @{ action = 'Suspend' }
Wait-Status $started.workflowId 'Suspended'
Expect-Http 409 "manage/workflows/$($started.workflowId)/status" 'POST' @{ action = 'Suspend' }
$null = Request "manage/workflows/$($started.workflowId)/status" 'POST' @{ action = 'Resume' }
Wait-Status $started.workflowId 'Runnable'
$null = Request "demo/onboardings/$($started.workflowId)/approve" 'POST'
Wait-Status $started.workflowId 'Complete'
Expect-Http 409 "manage/workflows/$($started.workflowId)/status" 'POST' @{ action = 'Resume' }
Write-Host 'Fenced instance suspend/resume and invalid state transitions passed.'
