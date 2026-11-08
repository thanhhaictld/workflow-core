# Give each checkout a stable Compose project and therefore its own named volumes.
# Forward all arguments to Docker Compose, including additional compose files.
$ErrorActionPreference = 'Stop'
$worktree = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../..')).Path
if ($IsWindows) { $worktree = $worktree.ToLowerInvariant() }
$bytes = [System.Text.Encoding]::UTF8.GetBytes($worktree)
$hash = [System.Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($bytes)).Substring(0, 16).ToLowerInvariant()
# Preserve a CI-supplied project name so the verifier inspects the same
# PostgreSQL instance as Docker Compose. Local worktrees still get the stable hash.
if (-not $env:WFC_OBSERVABILITY_PROJECT) {
    if ($env:GITHUB_RUN_ID) {
        # Match compose.yaml's CI fallback: direct Docker Compose and this
        # PowerShell wrapper must always target the SAME PostgreSQL volume.
        $env:WFC_OBSERVABILITY_PROJECT = "wfc-ci-$($env:GITHUB_RUN_ID)"
    } else {
        $env:WFC_OBSERVABILITY_PROJECT = "workflow-hub-observability-$hash"
    }
}
& docker compose --project-name $env:WFC_OBSERVABILITY_PROJECT @args
exit $LASTEXITCODE
