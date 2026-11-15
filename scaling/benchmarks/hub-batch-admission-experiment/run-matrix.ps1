param([int]$Port = 52322, [string]$AnalyzedDatabase = 'hub_batch', [string]$StaleDatabase = 'hub_batch_stale_active')
$ErrorActionPreference = 'Stop'
if (-not $env:WFC_BATCH_RAW_DIR) { throw 'Set WFC_BATCH_RAW_DIR to an experiment scratch directory before running the raw plan matrix.' }
$rawRoot = $env:WFC_BATCH_RAW_DIR
New-Item -ItemType Directory -Force $rawRoot | Out-Null
Push-Location $PSScriptRoot
try {
$env:WFC_BATCH_PORT = "$Port"
$env:WFC_BATCH_DB = $AnalyzedDatabase
dotnet run --project BatchExperiment.csproj -- baseline.sql analyzed (Join-Path $rawRoot 'paired-analyzed') prototype.sql
if ($LASTEXITCODE -ne 0) { throw "Analyzed paired matrix failed: $LASTEXITCODE" }
$env:WFC_BATCH_DB = $StaleDatabase
$env:WFC_BATCH_RAW_DIR = $rawRoot
dotnet run --project BatchExperiment.csproj -- baseline.sql stale-active (Join-Path $rawRoot 'paired-stale-active') prototype.sql
if ($LASTEXITCODE -ne 0) { throw "Stale-active paired matrix failed: $LASTEXITCODE" }
python summarize.py --raw-root $rawRoot
if ($LASTEXITCODE -ne 0) { throw "Summary failed: $LASTEXITCODE" }
} finally { Pop-Location }
