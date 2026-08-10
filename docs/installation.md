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

Registration exposes the DeepSeek tools but does not by itself make them the default delegation route. To route unqualified delegation requests to DeepSeek Sub-Agent, merge the canonical routing block from docs/orchestrator-instructions.md into `%USERPROFILE%\.codex\AGENTS.md`.

## Start the daemon

Foreground:

    node .\dist\cli.js daemon

Detached:

    node .\dist\cli.js start

The registered MCP now starts the detached local daemon automatically when it connects and the daemon is offline, then waits for the bridge health endpoint before exposing tools. `-StartDaemon` is still available to prewarm it after installation. Managed mode starts OpenCode on loopback and injects only a bridge-owned local server credential into that child process. The managed `serve` also sets `OPENCODE_PERMISSION={"*":"allow"}`, the headless configuration equivalent of `--auto`, so the dedicated sub-agent session never asks for approval. Attach mode can be selected in config for an already running loopback OpenCode server.

The installer configures `tool_timeout_sec = 4500` (75 minutes) in the `deepseek-subagent` Codex MCP section, preserving the other options and creating a timestamped backup before registration. This is longer than the maximum `deepseek_follow` window of 60 minutes plus 10 minutes of graceful finalization.

## Optional Codex App Server endpoint

The bridge can connect to an explicitly configured local Codex WebSocket endpoint:

    "codexAppServerSocket": "ws://127.0.0.1:PORT"

This requires a separately launched App Server (`codex app-server --listen ws://127.0.0.1:PORT`). It does not attach to the current Windows Desktop App Server automatically; leave this field `null` to keep the inbox fallback.

Same-chat push is experimental and disabled by default. Enable `experimentalSameChatDelivery` only for development with an explicitly configured and live-tested App Server correlation. Normal spawn, consult, follow, persistence and inbox recovery do not depend on it.

## Uninstall

    .\scripts\uninstall.ps1

Uninstall preserves the SQLite database, result files and inbox by default. Data removal requires both -PurgeData and -ConfirmPurge and is limited to the exact DeepSeek Sub-Agent data directory.

To remove the opt-in Codex registration, use `-RemoveCodex`; the script creates a timestamped Codex config backup before changing it:

    .\scripts\uninstall.ps1 -RemoveCodex
