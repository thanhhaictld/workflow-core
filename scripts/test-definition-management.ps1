#Requires -Version 7.0
<#
  End-to-end immutable definition versioning, metadata diff and policy checks.
  Requires API + dedicated worker + 006_definition_versions.sql.
  Uses development-only mutation controls; never point at production.
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
    return Request "definitions/$Name/start" 'POST' @{
        tenantId = "version-$Version-$Suffix"
        organizationName = "Catalog acceptance $Version"
        version = $Version
    }
}
function Put-State([int] $Version, [string] $Lifecycle) {
    return Request "definitions/$Name/versions/$Version/lifecycle" 'PUT' @{
        lifecycle = $Lifecycle
    }
}
$cap = Request 'manage/capabilities'
if (-not $cap.writesEnabled) { throw 'CI must explicitly enable WFC_ENABLE_MANAGEMENT_WRITES=true' }
$index = Request 'definitions'
$versions = @($index.versions | Where-Object { $_.definitionId -eq $Name })
if ($versions.Count -ne 2) { throw "Expected v1 and v2, got $($versions.Count)" }
if (@($versions | Where-Object { $_.isDefault -and $_.version -eq 2 }).Count -ne 1) {
    throw "Expected v2 initial default"
}
$originalHash = ($versions | Where-Object version -eq 1).schemaSha256
$diff = Request "definitions/$Name/diff?from=1&to=2"
if ($diff.changes.Count -lt 1) { throw 'v1 to v2 has no metadata changes' }
if (-not @($diff.changes | Where-Object {
    $_.path -match '^steps/' -and ($_.after -match 'ConfigureAccessStep')
}).Count) {
    throw 'v2 diff should reveal ConfigureAccessStep'
}
$schema = Request "definitions/$Name/versions/1"
if ($schema.schemaSha256 -ne $originalHash) { throw 'Snapshot hash mismatch' }
$first = Start-Versioned 1
$second = Start-Versioned 2
if ($first.version -ne 1 -or $second.version -ne 2) { throw 'Incorrect instance version pinning' }
try {
    $null = Request "definitions/$Name/versions/1/default" 'POST'
    $selected = Request "definitions/$Name/versions"
    if (@($selected | Where-Object { $_.version -eq 1 -and $_.isDefault }).Count -ne 1) {
        throw 'Promotion did not update default'
    }
    Expect-Http 409 "definitions/$Name/versions/1/lifecycle" 'PUT' @{ lifecycle = 'Deprecated' }
    $null = Request "definitions/$Name/versions/2/default" 'POST'
    $null = Put-State 1 'Deprecated'
    Expect-Http 409 "definitions/$Name/start" 'POST' @{
        tenantId = "retired-$Suffix"
        organizationName = "Blocked version"
        version = 1
    }
    $null = Put-State 1 'Published'
    $third = Start-Versioned 1
    if ($third.version -ne 1) { throw 'Republishing version should allow explicit start' }
    Wait-Status $third.workflowId 'Complete'
    Wait-Status $first.workflowId 'Complete'
    Wait-Status $second.workflowId 'Complete'
    $immutable = Request "definitions/$Name/versions/1"
    if ($immutable.schemaSha256 -ne $originalHash) {
        throw 'Lifecycle changes must not mutate definition metadata'
    }
} finally {
    # Restore shared local test DB policy even when an assertion fails.
    $null = Put-State 1 'Published'
    $null = Request "definitions/$Name/versions/2/default" 'POST'
}
Write-Host "Version diff, immutable schema, promotion, deprecation and pinned starts passed."

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
