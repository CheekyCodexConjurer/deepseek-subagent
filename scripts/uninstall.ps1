[CmdletBinding()]
param(
  [switch]$RemoveCodex,
  [switch]$PurgeData,
  [switch]$ConfirmPurge
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $repo
try {
  schtasks.exe /Delete /TN "DeepSeek Sub-Agent Daemon" /F 2>$null
  $configPath = Join-Path $env:LOCALAPPDATA "DeepSeek Sub-Agent\config.json"
  if (Test-Path -LiteralPath $configPath) {
    try { node .\dist\cli.js stop --config $configPath | Out-Null } catch { }
  }

  if ($RemoveCodex) {
    $codexConfig = Join-Path $env:USERPROFILE ".codex\config.toml"
    if (Test-Path -LiteralPath $codexConfig) {
      Copy-Item -LiteralPath $codexConfig -Destination ($codexConfig + ".deepseek-backup-" + (Get-Date -Format "yyyyMMdd-HHmmss")) -Force
    }
    & codex mcp remove deepseek-subagent
  }

  $dataDir = Split-Path -Parent $configPath
  if ($PurgeData) {
    if (-not $ConfirmPurge) {
      throw "Purge requires -ConfirmPurge. Data is preserved by default."
    }
    $resolved = [System.IO.Path]::GetFullPath($dataDir)
    if ($resolved -eq [System.IO.Path]::GetPathRoot($resolved) -or $resolved.Length -lt 12 -or $resolved -notlike "*DeepSeek Sub-Agent") {
      throw "Refusing to purge an unverified data path: $resolved"
    }
    Remove-Item -LiteralPath $resolved -Recurse -Force
  }

  Write-Output "DeepSeek Sub-Agent uninstalled. Local data was preserved unless -PurgeData -ConfirmPurge was supplied."
} finally {
  Pop-Location
}
