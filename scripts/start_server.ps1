# Start the Engram API server on Windows/PowerShell.
#
# PowerShell has no `source .env`, so every new terminal starts without the
# secrets and without ENGRAM_CONFIG_PATH — which surfaces as a 500 on
# /api/v1/admin/* ("api.api_key is required" or "config not found: config.yaml").
# This script loads the env file, pins the config path to an absolute location,
# and starts uvicorn from the repo root so relative data paths resolve.
#
# Usage (from anywhere):
#   powershell -ExecutionPolicy Bypass -File scripts\start_server.ps1
#   # or, if .env lives elsewhere:
#   .\scripts\start_server.ps1 -EnvFile "D:\path\to\.env"

param(
    [string]$EnvFile = "",
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"

# Repo root = parent of this script's directory.
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

# Default: .env one level above the repo (Code\.env), falling back to repo-local.
if (-not $EnvFile) {
    $candidates = @(
        (Join-Path (Split-Path -Parent $RepoRoot) ".env"),
        (Join-Path $RepoRoot ".env")
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) { $EnvFile = $c; break }
    }
}

if (-not $EnvFile -or -not (Test-Path $EnvFile)) {
    Write-Error "No .env found. Pass one explicitly: -EnvFile <path>"
}

Write-Host "loading env from: $EnvFile"
Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith('#') -and $line.Contains('=')) {
        $i = $line.IndexOf('=')
        $name = $line.Substring(0, $i).Trim()
        $value = $line.Substring($i + 1).Trim().Trim('"').Trim("'")
        if ($name) { Set-Item -Path "Env:$name" -Value $value }
    }
}

# Absolute config path so the server no longer depends on its working directory.
$ConfigPath = Join-Path $RepoRoot "config.yaml"
if (-not (Test-Path $ConfigPath)) {
    Write-Error "config.yaml not found at $ConfigPath"
}
$env:ENGRAM_CONFIG_PATH = $ConfigPath

# Fail fast with a clear message instead of a 500 later.
$missing = @("ENGRAM_API_KEY", "ENGRAM_ADMIN_KEY", "OPENCODE_API_KEY") |
    Where-Object { -not (Get-Item -Path "Env:$_" -ErrorAction SilentlyContinue) }
if ($missing) {
    Write-Error ("Missing required env vars: {0} (add them to {1})" -f ($missing -join ", "), $EnvFile)
}

Write-Host "config:  $env:ENGRAM_CONFIG_PATH"
Write-Host "cwd:     $RepoRoot"
Write-Host "starting uvicorn on ${BindHost}:${Port} ..."
uvicorn engram.api.app:app --host $BindHost --port $Port
