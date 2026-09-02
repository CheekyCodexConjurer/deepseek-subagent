# SubAgents MCP

codex-opencode-bridge is a local, event-driven bridge that lets Codex delegate an independent task to an authenticated OpenCode session or configured model routes. The human-facing MCP identity is **SubAgents MCP** (formerly **DeepSeek Sub-Agent**), with canonical server name `subagents` (aliases `subagents-mcp`, `deepseek-subagent`).

The bridge keeps the Codex provider unchanged, never reads OpenCode auth.json, and binds OpenCode to loopback. The main flow is event-driven: `subagents_spawn` returns immediately, Codex continues useful independent work, `subagents_status` provides one observable snapshot when needed, and `subagents_follow` waits on the daemon's completion event. Results are persisted before delivery.

Automatic same-chat push is experimental and disabled by default because Codex Desktop currently does not expose a reliable supported external attachment path. This does not prevent normal operation: the durable result file and private inbox remain available.

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

## Route delegation to SubAgents

MCP registration exposes the SubAgents tools but does not by itself make them the default delegation route. Codex App users merge the canonical routing block from docs/orchestrator-instructions.md into `%USERPROFILE%\.codex\AGENTS.md`.

## Tools

Canonical tools (with `deepseek_*` migration aliases preserved):

- `subagents_spawn` (alias `deepseek_spawn`): new topic, new agent and new session on the pinned model route.
- `subagents_continue` (alias `deepseek_continue`): direct follow-up on the same agent/session; returns busy instead of looping.
- `subagents_status` (alias `deepseek_consult`): one immediate snapshot of observable activity; it never exposes private reasoning and is not a wait loop.
- `subagents_follow` (alias `deepseek_follow`): event-driven wait for an existing agent after independent work is exhausted; it handles deadline and graceful finalization.
- `subagents_abort` (alias `deepseek_abort`): abort active work.
- `subagents_close` (alias `deepseek_close`): close the logical agent without deleting its session or report.
- `subagents_recover_result` (alias `deepseek_recover_result`): explicit recovery only; it is not a status/polling tool.

The normal CLI hides UUIDs. Use --json for machine-readable details, --verbose for technical IDs in list views, or `agent show <id>` for a full agent diagnostic. The installer configures a 75-minute Codex MCP tool timeout, which is longer than the 60-minute follow deadline plus its 10-minute grace maximum.
