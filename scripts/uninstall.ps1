[CmdletBinding()]
param(
  [switch]$RemoveCodex,
  [switch]$RemoveLegacyCodex,
  [switch]$PurgeData,
  [switch]$ConfirmPurge
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$canonicalServerName = "subagents"
$legacyServerNames = @("subagents-mcp", "deepseek-subagent", "deepseek_subagent")
$canonicalTaskName = "SubAgents MCP Daemon"
$legacyTaskName = "DeepSeek Sub-Agent Daemon"
$legacyDataDirectoryName = "DeepSeek Sub-Agent"

function Get-CodexConfigPath {
  $codexHome = if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $env:USERPROFILE ".codex" }
  return Join-Path $codexHome "config.toml"
}

function Remove-CodexRegistration([string]$Name) {
  & codex mcp remove $Name
  if ($LASTEXITCODE -ne 0) {
    Write-Warning "Codex MCP registration '$Name' was not removed (exit code $LASTEXITCODE)."
  }
}

Push-Location $repo
try {
  foreach ($taskName in @($canonicalTaskName, $legacyTaskName)) {
    & schtasks.exe /Query /TN $taskName 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
      & schtasks.exe /Delete /TN $taskName /F 2>$null | Out-Null
      if ($LASTEXITCODE -ne 0) { throw "scheduled-task removal failed with exit code $LASTEXITCODE" }
    }
  }

  if ($RemoveCodex) {
    $codexConfig = Get-CodexConfigPath
    if (Test-Path -LiteralPath $codexConfig) {
      Copy-Item -LiteralPath $codexConfig -Destination ($codexConfig + ".subagents-backup-" + (Get-Date -Format "yyyyMMdd-HHmmss")) -Force
    }
    Remove-CodexRegistration $canonicalServerName
    if ($RemoveLegacyCodex) {
      foreach ($legacyName in $legacyServerNames) {
        Remove-CodexRegistration $legacyName
      }
    }
  }

  $dataDir = Join-Path $env:LOCALAPPDATA $legacyDataDirectoryName
  if ($PurgeData) {
    if (-not $ConfirmPurge) {
      throw "Purge requires -ConfirmPurge. Data is preserved by default."
    }
    $resolved = [System.IO.Path]::GetFullPath($dataDir)
    if ($resolved -eq [System.IO.Path]::GetPathRoot($resolved) -or $resolved.Length -lt 12 -or $resolved -notlike "*\$legacyDataDirectoryName") {
      throw "Refusing to purge an unverified data path: $resolved"
    }
    Remove-Item -LiteralPath $resolved -Recurse -Force
  }

  $legacyMessage = if ($RemoveLegacyCodex) { "Legacy Codex aliases were removed." } else { "Legacy Codex aliases were preserved for compatibility." }
  Write-Output "SubAgents MCP uninstalled. The historical data directory was preserved unless -PurgeData -ConfirmPurge was supplied. $legacyMessage"
} finally {
  Pop-Location
}
