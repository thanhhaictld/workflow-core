# Give each checkout a stable Compose project and therefore its own named volumes.
# Forward all arguments to Docker Compose, including additional compose files.
$ErrorActionPreference = 'Stop'
$worktree = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../..')).Path
if ($IsWindows) { $worktree = $worktree.ToLowerInvariant() }
$bytes = [System.Text.Encoding]::UTF8.GetBytes($worktree)
$hash = [System.Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($bytes)).Substring(0, 16).ToLowerInvariant()
$env:WFC_OBSERVABILITY_PROJECT = "workflow-hub-observability-$hash"
& docker compose --project-name $env:WFC_OBSERVABILITY_PROJECT @args
exit $LASTEXITCODE
