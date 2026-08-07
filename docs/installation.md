# Installation

## Safe local setup

From PowerShell:

    npm install
    npm run build
    .\scripts\install.ps1 -Profile safe

The default configuration is stored below the current user’s local application data directory. It contains a random daemon bearer token and model settings. Secrets are redacted from doctor, config and error output.

## Register with Codex

Registration is opt-in because it changes the user’s Codex MCP configuration:

    .\scripts\install.ps1 -Profile safe -RegisterCodex

The script makes a timestamped backup before invoking codex mcp add. The MCP process is stdio-only; logs go to stderr so stdout remains protocol-clean.

## Start the daemon

Foreground:

    node .\dist\cli.js daemon

Detached:

    node .\dist\cli.js start

The registered MCP now starts the detached local daemon automatically when it connects and the daemon is offline, then waits for the bridge health endpoint before exposing tools. `-StartDaemon` is still available to prewarm it after installation. Managed mode starts OpenCode on loopback and injects only a bridge-owned local server credential into that child process. Attach mode can be selected in config for an already running loopback OpenCode server.

## Uninstall

    .\scripts\uninstall.ps1

Uninstall preserves the SQLite database, result files and inbox by default. Data removal requires both -PurgeData and -ConfirmPurge and is limited to the exact DeepSeek Sub-Agent data directory.

To remove the opt-in Codex registration, use `-RemoveCodex`; the script creates a timestamped Codex config backup before changing it:

    .\scripts\uninstall.ps1 -RemoveCodex
