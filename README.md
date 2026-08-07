# DeepSeek Sub-Agent

codex-opencode-bridge is a local, event-driven bridge that lets Codex delegate an independent task to an authenticated OpenCode session. The human-facing MCP identity is **DeepSeek Sub-Agent**.

The bridge keeps the Codex provider unchanged, never reads OpenCode auth.json, and binds OpenCode to loopback. deepseek_spawn returns an accepted job immediately; the daemon listens for OpenCode SSE events and persists a complete result. The Codex App Server adapter is separate and fail-closed: when a supported App Server control connection is not configured and proven, results go to the durable inbox and can be recovered with deepseek_recover_result or the CLI.

## Development

    npm install
    npm run build
    npm test
    node dist/cli.js doctor --json

The live OpenCode smoke is opt-in because it consumes the authenticated provider:

    $env:BRIDGE_LIVE_E2E = '1'
    npm run test:e2e:live

## Install for Codex

Run the idempotent installer from PowerShell after building:

    .\scripts\install.ps1 -Profile safe -RegisterCodex -StartDaemon

It backs up Codex configuration before registration, creates only the bridge-owned data directory, and does not delete result history during uninstall. See docs/installation.md and docs/discovery.md for the detected local capabilities.

## Tools

- deepseek_spawn: new topic, new agent and new OpenCode session.
- deepseek_continue: direct follow-up on the same agent/session; returns busy instead of looping.
- deepseek_abort: abort active work.
- deepseek_close: close the logical agent without deleting its session or report.
- deepseek_recover_result: explicit recovery only; it is not a status/polling tool.

The normal CLI hides UUIDs. Use --json for machine-readable details, --verbose for technical IDs in list views, or `agent show <id>` for a full agent diagnostic.
