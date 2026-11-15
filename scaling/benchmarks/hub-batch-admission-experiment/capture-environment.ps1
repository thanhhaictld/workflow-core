param([string]$ContainerName = 'wfc-pg17-batch-luna-20261115')
$ErrorActionPreference = 'Stop'
$meta = Join-Path $PSScriptRoot 'raw/resource.json'
$inspect = docker inspect $ContainerName | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw "Cannot inspect '$ContainerName'" }
$obj = $inspect[0]
$bindings = $obj.NetworkSettings.Ports.'5432/tcp'
$port = if ($bindings -and $bindings.Count) { [int]$bindings[0].HostPort } else { $null }
$out = [ordered]@{
    container_name = $obj.Name.TrimStart('/')
    container_id = $obj.Id
    label = $obj.Config.Labels.'wfc.experiment'
    image_tag = 'postgres:17'
    image_id = (docker image inspect postgres:17 --format '{{.Id}}')
    status = $obj.State.Status
    host_binding = if ($port) { "127.0.0.1:$port" } else { $null }
    cpu_quota = $obj.HostConfig.NanoCpus
    memory_bytes = $obj.HostConfig.Memory
    memory_swap_bytes = $obj.HostConfig.MemorySwap
    postgres_version = if ($obj.State.Running) { docker exec $ContainerName psql -X -At -U postgres -d hub_batch -c 'select version()' } else { 'PostgreSQL 17 version previously captured while running' }
    databases = @('hub_batch','hub_batch_stale_active','hub_batch_tests')
    commit = (git rev-parse HEAD)
    sdk = (dotnet --version)
    npgsql = '9.0.4'
    migrations = (Get-Content (Join-Path $PSScriptRoot 'raw/applied-migrations-no028.json') -Raw | ConvertFrom-Json)
}
$out | ConvertTo-Json -Depth 5 | Set-Content -Encoding utf8 $meta
$query = @'
SELECT current_database() AS database, count(*) AS actual_rows, c.reltuples AS planner_rows,
       s.last_analyze, s.last_autoanalyze, c.reloptions
FROM wfc_scale.hub_task CROSS JOIN pg_class c JOIN pg_stat_user_tables s
  ON s.schemaname='wfc_scale' AND s.relname='hub_task'
WHERE c.oid='wfc_scale.hub_task'::regclass
GROUP BY c.reltuples,s.last_analyze,s.last_autoanalyze,c.reloptions;
'@
$stats = Join-Path $PSScriptRoot 'raw/database-stats.txt'
if ($obj.State.Running) {
  foreach ($db in @('hub_batch','hub_batch_stale_active')) {
      "DATABASE $db" | Add-Content $stats
      docker exec $ContainerName psql -X -v ON_ERROR_STOP=1 -U postgres -d $db -c $query 2>&1 | Tee-Object -FilePath $stats -Append
  }
}
"CONTAINER $($obj.Name.TrimStart('/')) $($obj.Id) status=$($obj.State.Status) label=$($obj.Config.Labels.'wfc.experiment') binding=$(if ($port) { "127.0.0.1:$port" } else { 'inactive while stopped' }) cpu_nano=$($obj.HostConfig.NanoCpus) memory_bytes=$($obj.HostConfig.Memory) memory_swap_bytes=$($obj.HostConfig.MemorySwap)" | Set-Content -Encoding utf8 (Join-Path $PSScriptRoot 'raw/resource-state.txt')
Write-Output "Captured isolated resource and database metadata at $meta"
