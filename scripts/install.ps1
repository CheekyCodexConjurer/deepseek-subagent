[CmdletBinding()]
param(
  [ValidateSet("safe", "full")]
  [string]$Profile = "safe",
  [switch]$RegisterCodex,
  [switch]$StartDaemon,
  [switch]$InstallScheduledTask
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Get-CodexConfigPath {
  $codexHome = if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $env:USERPROFILE ".codex" }
  return Join-Path $codexHome "config.toml"
}

function Backup-CodexConfig([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path)) { return $null }
  $backup = $Path + ".deepseek-backup-" + (Get-Date -Format "yyyyMMdd-HHmmssfff")
  Copy-Item -LiteralPath $Path -Destination $backup -Force
  return $backup
}

function Set-CodexMcpToolTimeout([string]$Path, [int]$TimeoutSec) {
  if (-not (Test-Path -LiteralPath $Path)) { return }
  $text = Get-Content -LiteralPath $Path -Raw
  $headerPattern = '(?m)^\[mcp_servers\.(?:deepseek-subagent|deepseek_subagent|"deepseek-subagent"|"deepseek_subagent")\][ \t]*(?:\r?\n|$)'
  $header = [regex]::Match($text, $headerPattern)
  if (-not $header.Success) {
    $suffix = if ($text.Length -gt 0 -and -not $text.EndsWith("`n")) { "`r`n" } else { "" }
    $text = $text + $suffix + "[mcp_servers.deepseek-subagent]`r`ntool_timeout_sec = $TimeoutSec`r`n"
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
  npm install --ignore-scripts
  npm run build
  node .\dist\cli.js install

  if ($RegisterCodex) {
    $codexConfig = Get-CodexConfigPath
    $backup = Backup-CodexConfig $codexConfig
    $entry = (Resolve-Path ".\dist\cli.js").Path
    & codex mcp add deepseek-subagent -- node $entry mcp
    Set-CodexMcpToolTimeout -Path $codexConfig -TimeoutSec 4500
    if ($backup) { Write-Host "Codex config backup: $backup" }
  }

  if ($StartDaemon) {
    node .\dist\cli.js start
  }

  if ($InstallScheduledTask) {
    $node = (Get-Command node).Source
    $entry = (Resolve-Path ".\dist\cli.js").Path
    $config = Join-Path $env:LOCALAPPDATA "DeepSeek Sub-Agent\config.json"
    $taskRun = '"' + $node + '" "' + $entry + '" daemon --config "' + $config + '"'
    schtasks.exe /Create /TN "DeepSeek Sub-Agent Daemon" /TR $taskRun /SC ONLOGON /F | Out-Null
  }

  node .\dist\cli.js doctor
} finally {
  Pop-Location
}
