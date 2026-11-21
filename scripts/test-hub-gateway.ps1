#Requires -Version 7.0
<#
 End-to-end REST + SSE remote worker test. API runs with WFC_HUB_ENABLED=true
 and a remote Worker with WFC_HUB_URL and no WFC_PG.
#>
[CmdletBinding()]
param(
    [string]$ApiUrl = 'http://127.0.0.1:5000',
    [ValidateRange(10,180)][int]$TimeoutSeconds = 90
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Token = $env:WFC_HUB_AUTH_TOKEN
if ([string]::IsNullOrWhiteSpace($Token)) {
    throw 'Set WFC_HUB_AUTH_TOKEN to the same token as the Hub gateway'
}
$Root = $ApiUrl.TrimEnd('/')
$Suffix = [Guid]::NewGuid().ToString('N').Substring(0, 10)
$Headers = @{ Authorization = "Bearer $Token" }

function Request([string]$Path, [string]$Method = 'GET', [object]$Body = $null) {
    $args = @{ Uri = "$Root/api/hub/$Path"; Method = $Method; Headers = $Headers }
    if ($null -ne $Body) {
        $args.ContentType = 'application/json'
        $args.Body = $Body | ConvertTo-Json -Depth 10 -Compress
    }
    return Invoke-RestMethod @args
}
function Submit([string]$Hub, [string]$Type, [hashtable]$Payload, [string]$Key) {
    $answer = Request 'tasks' 'POST' @{
        hubId = $Hub
        taskType = $Type
        # The MVP provisioner publishes provision and restore activities at v4.
        version = 4
        payload = $Payload
        idempotencyKey = "$Suffix-$Key"
    }
    return "$($answer.taskId)"
}

# Probe a current protected route, not the retired policy management API.
$unauthorized = Invoke-WebRequest "$Root/api/hub/definitions" -SkipHttpErrorCheck
if ([int]$unauthorized.StatusCode -ne 401) {
    throw 'Hub gateway unexpectedly allows unauthenticated requests'
}
$definitions = @(Request 'definitions')
if (@($definitions | Where-Object {
    $_.hubId -eq 'onboarding' -and $_.definitionId -eq 'tenant-onboarding' -and $_.version -eq 5
}).Count -lt 1) {
    throw 'MVP onboarding v5 workflow not registered'
}

# Enqueue four independent tasks for one tenant and two other tasks.
$ids = @()
$tenant = "tenant-$Suffix"
$first = Submit 'onboarding' 'provision-tenant' @{tenantId=$tenant} 'serial-0'
$ids += $first
for ($i = 1; $i -le 3; $i++) {
    $ids += Submit 'onboarding' 'provision-tenant' @{tenantId=$tenant} "serial-$i"
}
$ids += Submit 'onboarding' 'provision-tenant' @{tenantId="other-$Suffix"} 'other'
$ids += Submit 'backup' 'restore-database' @{databaseId="db-$Suffix"} 'restore'

$replay = Submit 'onboarding' 'provision-tenant' @{tenantId=$tenant} 'serial-0'
if ($replay -ne $first) { throw 'Idempotency replay generated a second Hub task' }

$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
$complete = 0
while ([DateTime]::UtcNow -lt $deadline) {
    $all = @($ids | ForEach-Object { Request "tasks/$_" })
    $complete = @($all | Where-Object { $_.state -eq 'Complete' }).Count
    if (@($all | Where-Object { $_.state -eq 'Dead' }).Count -gt 0) {
        throw 'Hub task reached dead-letter instead of succeeding'
    }
    if ($complete -eq $ids.Count) { break }
    Start-Sleep -Milliseconds 100
}
if ($complete -ne $ids.Count) {
    throw "Only $complete/$($ids.Count) remote tasks completed. Check remote worker SSE and REST logs."
}
Write-Host 'REST/SSE remote activity execution, authorization, idempotency and unordered MVP dispatch passed.'
