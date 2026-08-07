$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $repo
try {
  npm run build
  node .\dist\cli.js doctor --json
} finally {
  Pop-Location
}
