[CmdletBinding()]
param(
  [ValidateSet("safe", "full")]
  [string]$Profile = "safe",
  [switch]$RegisterCodex,
  [switch]$StartDaemon,
  [switch]$InstallScheduledTask
)

# Idempotent installer for SubAgents MCP (Antigravity + Gemini).
# Does not start or stop background processes in this patch.
# Preserves legacy historical data and compatibility aliases.
$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$canonicalServerName = "subagents"
$canonicalTaskName = "SubAgents MCP Daemon"
$legacyTaskName = "DeepSeek Sub-Agent Daemon"
$legacyDataDirectoryName = "DeepSeek Sub-Agent"

function Get-CodexConfigPath {
  $codexHome = if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $env:USERPROFILE ".codex" }
  return Join-Path $codexHome "config.toml"
}

function Backup-CodexConfig([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path)) { return $null }
  $backup = $Path + ".subagents-backup-" + (Get-Date -Format "yyyyMMdd-HHmmssfff")
  Copy-Item -LiteralPath $Path -Destination $backup -Force
  return $backup
}

function Set-CodexMcpToolTimeout([string]$Path, [int]$TimeoutSec) {
  if (-not (Test-Path -LiteralPath $Path)) { return }
  $text = Get-Content -LiteralPath $Path -Raw
  $headerPattern = '(?m)^\[mcp_servers\.(?:subagents|"subagents")\][ \t]*(?:\r?\n|$)'
  $header = [regex]::Match($text, $headerPattern)
  if (-not $header.Success) {
    $suffix = if ($text.Length -gt 0 -and -not $text.EndsWith("`n")) { "`r`n" } else { "" }
    $text = $text + $suffix + "[mcp_servers.$canonicalServerName]`r`ntool_timeout_sec = $TimeoutSec`r`n"
  } else {
    $remainder = $text.Substring($header.Index + $header.Length)
    $nextHeader = [regex]::Match($remainder, '(?m)^\[')
    $sectionLength = if ($nextHeader.Success) { $nextHeader.Index } else { $remainder.Length }
    $section = $remainder.Substring(0, $sectionLength)
    $timeoutPattern = '(?m)^[ \t]*tool_timeout_sec[ \t]*=.*$'
    if ([regex]::IsMatch($section, $timeoutPattern)) {
      $section = [regex]::Replace($section, $timeoutPattern, "tool_timeout_sec = $TimeoutSec", 1)
    } else {
      $section = "tool_timeout_sec = $TimeoutSec`r`n" + $section
    }
    $text = $text.Substring(0, $header.Index + $header.Length) + $section + $remainder.Substring($sectionLength)
  }
  [System.IO.File]::WriteAllText($Path, $text, [System.Text.UTF8Encoding]::new($false))
}

Push-Location $repo
try {
  npm install --ignore-scripts --package-lock=false
  if ($LASTEXITCODE -ne 0) { throw "npm install failed with exit code $LASTEXITCODE" }
  npm run build
  if ($LASTEXITCODE -ne 0) { throw "npm run build failed with exit code $LASTEXITCODE" }
  node .\dist\cli.js install
  if ($LASTEXITCODE -ne 0) { throw "bridge install failed with exit code $LASTEXITCODE" }

  if ($RegisterCodex) {
    $codexConfig = Get-CodexConfigPath
    $backup = Backup-CodexConfig $codexConfig
    $entry = (Resolve-Path ".\dist\cli.js").Path
    # Remove existing registration if present so re-registration is cleanly idempotent
    & codex mcp remove $canonicalServerName 2>$null | Out-Null
    & codex mcp add $canonicalServerName -- node $entry mcp
    if ($LASTEXITCODE -ne 0) { throw "Codex MCP registration failed with exit code $LASTEXITCODE" }
    Set-CodexMcpToolTimeout -Path $codexConfig -TimeoutSec 4500
    if ($backup) { Write-Host "Codex config backup: $backup" }
    Write-Host "Registered $canonicalServerName; legacy server names remain compatibility aliases."
  }

  if ($StartDaemon) {
    Write-Host "Notice: Installation does not start or stop processes in this patch. Start the daemon explicitly via 'npm start' or 'node dist/cli.js daemon'."
  }

  if ($InstallScheduledTask) {
    $node = (Get-Command node).Source
    $entry = (Resolve-Path ".\dist\cli.js").Path
    $config = Join-Path $env:LOCALAPPDATA (Join-Path $legacyDataDirectoryName "config.json")
    $taskRun = '"' + $node + '" "' + $entry + '" daemon --config "' + $config + '"'
    & schtasks.exe /Query /TN $legacyTaskName 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
      & schtasks.exe /Delete /TN $legacyTaskName /F | Out-Null
      if ($LASTEXITCODE -ne 0) { throw "legacy scheduled-task migration failed with exit code $LASTEXITCODE" }
    }
    & schtasks.exe /Create /TN $canonicalTaskName /TR $taskRun /SC ONLOGON /F | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "scheduled-task registration failed with exit code $LASTEXITCODE" }
    Write-Host "Registered $canonicalTaskName; historical data remains in $config."
  }

  # -Profile controls doctor check depth: safe (fast) or full (deep quick_check)
  if ($Profile -eq "full") {
    node .\dist\cli.js doctor --full
  } else {
    node .\dist\cli.js doctor
  }
  if ($LASTEXITCODE -ne 0) { throw "bridge doctor failed with exit code $LASTEXITCODE" }
} finally {
  Pop-Location
}
