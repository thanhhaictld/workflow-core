param([string]$ExperimentName = "wfc-batch-luna-20261115", [switch]$Include028)
$ErrorActionPreference = 'Stop'
$containerName = $ExperimentName
$label = "wfc.experiment=$ExperimentName"
$existing = docker ps -a --filter "name=^/$containerName$" --format '{{.Names}}'
if ($existing) { throw "Refusing to reuse or remove existing Docker resource '$containerName'. Choose a new ExperimentName." }
if (-not (docker image inspect postgres:17 2>$null)) { docker pull postgres:17 }
$containerId = docker run -d --name $containerName --label $label --cpus=2 --memory=2g --memory-swap=2g -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=hub_batch -p 127.0.0.1::5432 postgres:17
if ($LASTEXITCODE -ne 0) { throw "docker run failed with exit $LASTEXITCODE" }
$portLine = docker port $containerName 5432/tcp
$port = [int](($portLine -split ':')[-1])
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
    docker exec $containerName pg_isready -U postgres -d hub_batch *> $null
    if ($LASTEXITCODE -eq 0) { $ready = $true; break }
    Start-Sleep -Seconds 1
}
if (-not $ready) { throw "PostgreSQL in '$containerName' did not become ready; container left running for diagnosis." }

$log = Join-Path $PSScriptRoot 'raw/setup-migrations.log'
New-Item -ItemType Directory -Force (Split-Path $log) | Out-Null
$appliedMigrations = @()
foreach ($i in 2..30) {
    if ($i -eq 28 -and -not $Include028) { continue }
    $migration = Get-ChildItem (Join-Path $PSScriptRoot ("../../sql/{0:D3}_*.sql" -f $i))
    if (-not $migration) { throw "Migration $i missing" }
    $appliedMigrations += $migration.Name
    "APPLY $($migration.Name)" | Add-Content $log
    Get-Content -Raw $migration.FullName | docker exec -i $containerName psql -X -v ON_ERROR_STOP=1 -U postgres -d hub_batch 2>&1 | Tee-Object -FilePath $log -Append
    if ($LASTEXITCODE -ne 0) { throw "Migration failed: $($migration.Name), exit $LASTEXITCODE" }
}
Get-Content -Raw (Join-Path $PSScriptRoot 'seed.sql') | docker exec -i $containerName psql -X -v ON_ERROR_STOP=1 -U postgres -d hub_batch
if ($LASTEXITCODE -ne 0) { throw "Seed failed with exit $LASTEXITCODE" }
Get-Content -Raw (Join-Path $PSScriptRoot 'append-history.sql') | docker exec -i $containerName psql -X -v ON_ERROR_STOP=1 -v prefix=extra-completed- -U postgres -d hub_batch
if ($LASTEXITCODE -ne 0) { throw "Historical append failed with exit $LASTEXITCODE" }
docker exec $containerName psql -X -v ON_ERROR_STOP=1 -U postgres -d hub_batch -c 'ALTER TABLE wfc_scale.hub_task SET (autovacuum_enabled=false); ANALYZE wfc_scale.hub_task'
docker exec $containerName psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres -c 'CREATE DATABASE hub_batch_stale_active TEMPLATE hub_batch'
docker exec $containerName psql -X -v ON_ERROR_STOP=1 -U postgres -d hub_batch_stale_active -c 'ALTER TABLE wfc_scale.hub_task SET (autovacuum_enabled=false); ANALYZE wfc_scale.hub_task'
Get-Content -Raw (Join-Path $PSScriptRoot 'append-active.sql') | docker exec -i $containerName psql -X -v ON_ERROR_STOP=1 -v prefix=stale-active- -U postgres -d hub_batch_stale_active
if ($LASTEXITCODE -ne 0) { throw "Stale active-backlog append failed with exit $LASTEXITCODE" }
docker exec $containerName psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres -c 'CREATE DATABASE hub_batch_tests TEMPLATE hub_batch'
$metadata = [ordered]@{ experiment=$ExperimentName; container=$containerName; container_id=$containerId; label=$label; image='postgres:17'; host_port=$port; database='hub_batch'; databases=@('hub_batch','hub_batch_stale_active','hub_batch_tests'); cpu_limit='2'; memory_limit='2g'; memory_swap_limit='2g'; include028=[bool]$Include028; migration_files=$appliedMigrations }
$migrationListFile = if ($Include028) { 'applied-migrations-028-nondeployment.json' } else { 'applied-migrations-no028.json' }
$appliedMigrations | ConvertTo-Json | Set-Content -Encoding utf8 (Join-Path (Join-Path $PSScriptRoot 'raw') $migrationListFile)
$metadata | ConvertTo-Json -Depth 5 | Set-Content -Encoding utf8 (Join-Path $PSScriptRoot 'raw/resource.json')
Write-Output "Created isolated PostgreSQL resource $containerName on 127.0.0.1:$port; databases prepared; no credential configured."
