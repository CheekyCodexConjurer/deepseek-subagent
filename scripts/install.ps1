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
Push-Location $repo
try {
  npm install --ignore-scripts
  npm run build
  node .\dist\cli.js install

  if ($RegisterCodex) {
    $codexConfig = Join-Path $env:USERPROFILE ".codex\config.toml"
    if (Test-Path -LiteralPath $codexConfig) {
      Copy-Item -LiteralPath $codexConfig -Destination ($codexConfig + ".deepseek-backup-" + (Get-Date -Format "yyyyMMdd-HHmmss")) -Force
    }
    $entry = (Resolve-Path ".\dist\cli.js").Path
    & codex mcp add deepseek-subagent -- node $entry mcp
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
