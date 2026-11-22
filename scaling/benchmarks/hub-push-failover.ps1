#Requires -Version 7.0
[CmdletBinding()]
param([ValidateSet('before-claim','after-claim','during-execution','worker-before-claim','worker-during-execution')][string]$Scenario = 'before-claim',
      [string]$Output = 'scaling/benchmarks/results/push-local/failover')
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = (Get-Location).Path
$outDir = [IO.Path]::GetFullPath($Output)
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$suffix = [guid]::NewGuid().ToString('N').Substring(0,8)
$db = 'wfc_push_ha_' + $suffix
$proxy = 'wfc-push-ha-' + $suffix
$run = 'ha-' + $Scenario + '-' + $suffix
$token = 'push-failover-local-token-20261114'
$headers = @{ Authorization = "Bearer $token" }
$hubDll = Join-Path $root 'scaling/src/WorkflowCore.Hub.Server/bin/Release/net10.0/WorkflowCore.Hub.Server.dll'
$workerDll = Join-Path $root 'scaling/sample/onboarding/WorkflowCore.Provisioner.Host/bin/Release/net10.0/WorkflowCore.Provisioner.Host.dll'
$dotnet = (Get-Command dotnet).Source
$backgroundWindow = if ($IsWindows) { @{ WindowStyle = 'Hidden' } } else { @{} }
$pgContainer = if ($env:WFC_PUSH_FAILOVER_POSTGRES_CONTAINER) { $env:WFC_PUSH_FAILOVER_POSTGRES_CONTAINER } else { 'workflow-core-monitor-postgres-1' }
$pgPort = if ($env:WFC_PUSH_FAILOVER_PGPORT) { [int]$env:WFC_PUSH_FAILOVER_PGPORT } else { 55432 }
$config = Join-Path $root 'scaling/benchmarks/hub-push-failover-nginx.conf'
$conn = "Host=127.0.0.1;Port=$pgPort;Database=$db;Username=workflow;Password=workflow_dev_only"
$execLog = Join-Path $outDir "$run-execution.jsonl"
$sideLog = Join-Path $outDir "$run-effects.jsonl"
$hubA = $null; $hubB = $null; $worker = $null
$workerFault = $Scenario.StartsWith('worker-')
$beforeClaim = $Scenario -in @('before-claim','worker-before-claim')
$result = [ordered]@{ scenario=$Scenario; database=$db; runId=$run; started=[DateTime]::UtcNow.ToString('O'); events=@(); errors=@() }
function Mark([string]$Name) { $result.events += @{ name=$Name; at=[DateTime]::UtcNow.ToString('O') } }
function Query([string]$Sql) {
    $value = & docker exec $pgContainer psql -U workflow -d $db -A -t -v ON_ERROR_STOP=1 -c $Sql
    if ($LASTEXITCODE -ne 0) { throw "psql failed: $Sql" }
    return ($value -join "`n").Trim()
}
function Wait-Until([scriptblock]$Condition, [int]$Seconds, [string]$Label) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        try { if (& $Condition) { return } } catch { }
        Start-Sleep -Milliseconds 300
    }
    throw "Timed out waiting for $Label after $Seconds seconds"
}
function Start-Hub([int]$Port, [string]$Node, [int]$Delay) {
    $vars = @{
        ASPNETCORE_URLS="http://0.0.0.0:$Port"; WFC_PG=$conn; WFC_HUB_AUTH_TOKEN=$token
        WFC_HUB_MATCHING_ENABLED='true'; WFC_HUB_MATCHING_NODE_ID=$Node
        WFC_HUB_MATCHING_ADVERTISE_URL="http://127.0.0.1:$Port"
        WFC_HUB_MATCHING_PEERS='http://127.0.0.1:54110,http://127.0.0.1:54111'
        WFC_HUB_MATCHING_PARTITIONS='1'; WFC_HUB_PUSH_ENABLED='true'; WFC_HUB_DB_POOL='12'
        WFC_HUB_MATCHING_CANDIDATES_ENABLED='true'; WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY='2'
        WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED='false'
        WFC_HUB_CLAIM_ADMISSION_ENABLED='true'; WFC_HUB_CLAIM_ADMISSION_LIMIT='3'
        WFC_HUB_PUSH_TEST_BEFORE_SEND_MS="$Delay"; Logging__LogLevel__Default='Warning'
    }
    Start-Process @backgroundWindow -FilePath $dotnet -ArgumentList @($hubDll) -Environment $vars -PassThru `
        -RedirectStandardOutput (Join-Path $outDir "$run-$Node.stdout.log") `
        -RedirectStandardError (Join-Path $outDir "$run-$Node.stderr.log")
}
try {
    & docker exec $pgContainer createdb -U workflow $db
    if ($LASTEXITCODE -ne 0) { throw 'Could not create isolated test database' }
    & docker exec $pgContainer sh -c "pg_dump -U workflow --schema-only --schema=wfc_scale workflow | psql -q -v ON_ERROR_STOP=1 -U workflow -d $db"
    if ($LASTEXITCODE -ne 0) { throw 'Could not copy runtime schema' }
    # Docker Engine on Linux needs an explicit host-gateway alias. Docker
    # Desktop already supplies host.docker.internal.
    $hostAlias = if ($IsLinux) { @('--add-host', 'host.docker.internal:host-gateway') } else { @() }
    & docker run -d --name $proxy @hostAlias -p '127.0.0.1:54112:5100' -v "${config}:/etc/nginx/conf.d/default.conf:ro" nginx:1.29-alpine | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not start isolated nginx proxy' }
    $delay = if ($Scenario -eq 'after-claim') { 20000 } else { 0 }
    $hubA = Start-Hub 54110 'hub-a' $delay
    Wait-Until { (Invoke-RestMethod 'http://127.0.0.1:54110/health/ready' -TimeoutSec 2).status -eq 'ready' } 30 'Hub A ready'
    $workerVars = @{
        WFC_HUB_URL='http://127.0.0.1:54112'; WFC_HUB_AUTH_TOKEN=$token; WFC_HUB_PUSH_ENABLED='true'
        WFC_HUB_PUSH_TRUSTED_OWNERS='http://127.0.0.1:54110,http://127.0.0.1:54111'
        WFC_HUB_MATCHING_PARTITIONS='1'; WFC_HUB_WORKER_HUB='onboarding'
        WFC_HUB_PUSH_CREDIT_JOURNAL=(Join-Path $outDir "$run-worker-credits.json")
        WORKFLOW_WORKERS='4'; ACTIVITY_WORKERS='4'; WFC_SAMPLE_PROVISION_DELAY_MS='15000'
        WFC_HUB_EXECUTION_OBSERVATION_LOG=$execLog; WFC_HUB_SIDE_EFFECT_OBSERVATION_LOG=$sideLog
        Logging__LogLevel__Default='Warning'
    }
    $worker = Start-Process @backgroundWindow -FilePath $dotnet -ArgumentList @($workerDll) -Environment $workerVars -PassThru `
        -RedirectStandardOutput (Join-Path $outDir "$run-worker.stdout.log") `
        -RedirectStandardError (Join-Path $outDir "$run-worker.stderr.log")
    Wait-Until { (Invoke-RestMethod 'http://127.0.0.1:54110/api/hub/workers/transport-diagnostics' -Headers $headers -TimeoutSec 2).pushConnected -eq 1 } 45 'one active push session on A'
    $diagA = Invoke-RestMethod 'http://127.0.0.1:54110/api/hub/workers/transport-diagnostics' -Headers $headers -TimeoutSec 2
    if (-not $diagA.candidateEnabled) { throw 'Candidate-mode matching was not active on Hub A' }
    $result.initialMatching = $diagA
    $hubB = Start-Hub 54111 'hub-b' 0
    Wait-Until { (Invoke-RestMethod 'http://127.0.0.1:54111/health/ready' -TimeoutSec 2).status -eq 'ready' } 30 'Hub B ready'
    Mark 'both-hubs-and-sessions-ready'
    $oldLease = $null
    if (-not $beforeClaim) {
        for ($i=0; $i -lt 2; $i++) {
            $body = @{ hubId='onboarding'; definitionId='tenant-onboarding'; version=5; runId=$run
                idempotencyKey="${run}:$i"; input=@{ tenantId="same-$suffix"; organizationName="Failover $i" } } | ConvertTo-Json -Depth 5
            Invoke-RestMethod 'http://127.0.0.1:54112/api/hub/workflows' -Headers $headers -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 10 | Out-Null
        }
        if ($Scenario -eq 'after-claim') {
            Wait-Until { [int](Query "SELECT count(*) FROM wfc_scale.hub_task t JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id WHERE w.run_id='$run' AND t.state=1") -gt 0 } 30 'committed claim before delayed SSE send'
        } else {
            Wait-Until { (Test-Path $execLog) -and ((Get-Content $execLog -Raw) -match '"taskType":"provision-tenant"') } 45 'active provision execution'
        }
        $activeType = if ($Scenario -eq 'worker-during-execution') { "AND t.task_type='provision-tenant'" } else { '' }
        $oldLease = Query "SELECT json_build_object('id',t.id,'owner',t.owner,'epoch',t.epoch) FROM wfc_scale.hub_task t JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id WHERE w.run_id='$run' AND t.state=1 $activeType ORDER BY t.sequence LIMIT 1" | ConvertFrom-Json
        if ($null -eq $oldLease) { throw 'Expected a live task lease before fault injection' }
        $result.oldTaskLease = $oldLease
    }
    $owner = Query "SELECT split_part(owner_id, ':', 1) FROM wfc_scale.hub_matching_partition_owner WHERE hub_id='onboarding' AND matching_partition=0"
    if ($owner -ne 'hub-a') { throw "Unexpected initial matching owner: $owner" }
    $killTime = [DateTime]::UtcNow
    if ($workerFault) {
        Mark 'worker-hard-killed'
        # Process.Kill() deliberately skips graceful shutdown, credit release and drain.
        $worker.Kill()
        if (-not $worker.WaitForExit(10000)) { throw 'Worker process did not exit after hard kill' }
        # Reuse the *same* durable credit journal, as on a Kubernetes pod restart
        # with a persistent identity/volume; use distinct stdout/stderr files.
        $worker = Start-Process @backgroundWindow -FilePath $dotnet -ArgumentList @($workerDll) -Environment $workerVars -PassThru `
            -RedirectStandardOutput (Join-Path $outDir "$run-worker-restarted.stdout.log") `
            -RedirectStandardError (Join-Path $outDir "$run-worker-restarted.stderr.log")
        Wait-Until {
            -not $worker.HasExited -and
            (Invoke-RestMethod 'http://127.0.0.1:54110/api/hub/workers/transport-diagnostics' -Headers $headers -TimeoutSec 2).pushConnected -ge 1
        } 90 'replacement worker SSE session on the surviving Hub'
        Mark 'worker-restarted'
        $result.workerToReconnectSeconds = [Math]::Round(([DateTime]::UtcNow - $killTime).TotalSeconds, 2)
    } else {
        Mark 'owner-hard-killed'
        # Hard stop intentionally skips Hub ownership release and SSE drain.
        $hubA.Kill()
        if (-not $hubA.WaitForExit(10000)) { throw 'Hub process did not exit after hard kill' }
        Wait-Until { (Query "SELECT split_part(owner_id, ':', 1) FROM wfc_scale.hub_matching_partition_owner WHERE hub_id='onboarding' AND matching_partition=0") -eq 'hub-b' } 120 'Hub B ownership takeover'
        Mark 'owner-takeover'
        $result.ownerTakeoverSeconds = [Math]::Round(([DateTime]::UtcNow - $killTime).TotalSeconds, 2)
        Wait-Until { (Invoke-RestMethod 'http://127.0.0.1:54111/api/hub/workers/transport-diagnostics' -Headers $headers -TimeoutSec 2).pushConnected -eq 1 } 60 'worker session reconnected to Hub B'
        Mark 'sessions-reconnected'
        $result.ownerToReconnectSeconds = [Math]::Round(([DateTime]::UtcNow - $killTime).TotalSeconds, 2)
        if ($result.ownerToReconnectSeconds -ge 65) { throw 'SSE reconnection exceeded the failover latency budget' }
    }
    $survivorPort = if ($workerFault) { 54110 } else { 54111 }
    $diagUrl = "http://127.0.0.1:$survivorPort/api/hub/workers/transport-diagnostics"
    $diagRecovered = Invoke-RestMethod $diagUrl -Headers $headers -TimeoutSec 2
    if (-not $diagRecovered.candidateEnabled) { throw 'Candidate matching was not active after fault recovery' }
    $result.recoveredMatching = $diagRecovered
    if ($beforeClaim) {
        for ($i=0; $i -lt 2; $i++) {
            $body = @{ hubId='onboarding'; definitionId='tenant-onboarding'; version=5; runId=$run
                idempotencyKey="${run}:$i"; input=@{ tenantId="same-$suffix"; organizationName="Failover $i" } } | ConvertTo-Json -Depth 5
            Invoke-RestMethod 'http://127.0.0.1:54112/api/hub/workflows' -Headers $headers -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 15 | Out-Null
        }
    }
    Wait-Until { [int](Query "SELECT count(*) FROM wfc_scale.hub_workflow WHERE run_id='$run' AND completed_at IS NOT NULL") -eq 2 } 180 'both workflows complete'
    Mark 'workflows-completed'
    # A hard-killed worker may leave ambiguous, durably granted leases.
    # Keep their credits fenced through the 45s lease TTL and one recovery sweep
    # rather than assuming every old slot can be retired within 20 seconds.
    $creditSettleBudget = if ($workerFault) { 90 } else { 20 }
    $creditSettleStarted = [DateTime]::UtcNow
    Wait-Until {
        $d = Invoke-RestMethod $diagUrl -Headers $headers -TimeoutSec 2
        # Preserve the last observation even when the cleanup assertion times out,
        # so leaked credit, pending admission, and stale SSE session are distinguishable.
        $result.postCompletionDiagnostics = $d
        # One stream carries the two independently configured four-slot budgets.
        $d.pushConnected -eq 1 -and
            $d.candidateCredits -ge 0 -and
            $d.candidateCredits -le 8 -and
            $d.candidateReservations -eq 0 -and
            $d.candidateUncertain -eq 0 -and
            $d.candidateAdmissionsActive -eq 0
    } $creditSettleBudget 'bounded waiting credits and no leaked reservations, uncertain leases or admissions after completion'
    $result.creditSettleSeconds = [Math]::Round(([DateTime]::UtcNow - $creditSettleStarted).TotalSeconds, 2)
    $result.finalMatching = Invoke-RestMethod $diagUrl -Headers $headers -TimeoutSec 2
    $result.taskSummary = Query "SELECT json_build_object('tasks',count(*),'complete',count(*) FILTER (WHERE t.state=2),'retries',sum(greatest(t.attempts-1,0))) FROM wfc_scale.hub_task t JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id WHERE w.run_id='$run'" | ConvertFrom-Json
    $result.fifoOverlaps = [int](Query "SELECT count(*) FROM wfc_scale.hub_task_attempt a JOIN wfc_scale.hub_task ta ON ta.id=a.task_id JOIN wfc_scale.hub_workflow wa ON wa.id=ta.workflow_id JOIN wfc_scale.hub_task_attempt b ON b.task_id<>a.task_id AND a.claimed_at < coalesce(b.finished_at,now()) AND b.claimed_at < coalesce(a.finished_at,now()) JOIN wfc_scale.hub_task tb ON tb.id=b.task_id JOIN wfc_scale.hub_workflow wb ON wb.id=tb.workflow_id WHERE wa.run_id='$run' AND wb.run_id='$run' AND ta.task_type='provision-tenant' AND tb.task_type='provision-tenant'")
    # New onboarding v5 intentionally has no business-key FIFO policy.
    # Cross-workflow provisioning overlap is allowed and recorded for diagnosis.
    if ($result.taskSummary.tasks -ne $result.taskSummary.complete) { throw 'Incomplete tasks after failover' }
    $starts = @(Get-Content $execLog | ForEach-Object { $_ | ConvertFrom-Json })
    $effects = @(Get-Content $sideLog | ForEach-Object { $_ | ConvertFrom-Json })
    $result.executionStarts = $starts.Count
    $result.duplicateSameEpochStarts = [int]($starts | Group-Object { "$($_.taskId):$($_.epoch)" } | Where-Object Count -gt 1 | ForEach-Object { $_.Count - 1 } | Measure-Object -Sum).Sum
    $result.sideEffects = $effects.Count
    $result.missingDispatchKeys = @($effects | Where-Object { -not $_.dispatchKey }).Count
    $result.duplicateSideEffects = [int]($effects | Group-Object { "$($_.workflowId):$($_.dispatchKey)" } | Where-Object Count -gt 1 | ForEach-Object { $_.Count - 1 } | Measure-Object -Sum).Sum
    if ($result.duplicateSameEpochStarts -or $result.missingDispatchKeys -or $result.duplicateSideEffects -or $result.sideEffects -ne 6) { throw 'Unexpected execution or side-effect observation' }
    if ($null -ne $oldLease) {
        try {
            $hb = @{ workerId=$oldLease.owner; epoch=$oldLease.epoch } | ConvertTo-Json
            Invoke-WebRequest "http://127.0.0.1:$survivorPort/api/hub/tasks/$($oldLease.id)/heartbeat" -Headers $headers -Method Post -ContentType 'application/json' -Body $hb -TimeoutSec 10 | Out-Null
            throw 'Stale heartbeat unexpectedly accepted'
        } catch [Microsoft.PowerShell.Commands.HttpResponseException] {
            $result.staleHeartbeatStatus = [int]$_.Exception.Response.StatusCode
            if ($result.staleHeartbeatStatus -ne 409) { throw }
        }
    }
    $result.outcome='passed'
} catch {
    $result.outcome='failed'; $result.errors += $_.ToString()
} finally {
    $result.finished=[DateTime]::UtcNow.ToString('O')
    $result | ConvertTo-Json -Depth 10 | Set-Content -Encoding utf8 (Join-Path $outDir "$run-result.json")
    foreach ($proc in @($worker,$hubA,$hubB)) { if ($null -ne $proc -and -not $proc.HasExited) { Stop-Process -Id $proc.Id -Force } }
    & docker rm -f $proxy 2>$null | Out-Null
    Write-Host ($result | ConvertTo-Json -Compress -Depth 10)
}
if ($result.outcome -ne 'passed') { exit 1 }
