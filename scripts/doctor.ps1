$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $repo
try {
  # Doctor is strictly a read-only diagnostic check; it does not build or generate
  # dist artifacts, start Antigravity, stop or kill processes, delete history, or alter Codex registration.
  if (Test-Path -LiteralPath ".\dist\cli.js") {
    node .\dist\cli.js doctor --json
  } else {
    node --import tsx .\src\cli.ts doctor --json
  }
  if ($LASTEXITCODE -ne 0) { throw "bridge doctor failed with exit code $LASTEXITCODE" }
} finally {
  Pop-Location
}
