param([string]$ExperimentName = 'wfc-pg17-batch-luna-20261115', [string]$ExperimentLabel = $ExperimentName)
$container = docker inspect $ExperimentName --format '{{.Name}} {{index .Config.Labels "wfc.experiment"}} {{.State.Running}}'
if ($LASTEXITCODE -ne 0) { throw "Cannot inspect '$ExperimentName'; refusing to stop an unverified resource." }
if (-not $container.StartsWith("/$ExperimentName $ExperimentLabel ")) { throw "Label/name verification failed: $container" }
if ($container.EndsWith(' true')) { docker stop $ExperimentName; if ($LASTEXITCODE -ne 0) { throw "docker stop failed: $LASTEXITCODE" } }
docker inspect $ExperimentName --format '{{.Name}} {{index .Config.Labels "wfc.experiment"}} {{.State.Status}}'
