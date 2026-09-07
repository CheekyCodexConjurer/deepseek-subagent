# SubAgents MCP

SubAgents MCP is a local, event-driven bridge for delegating work to one
execution path: authenticated Antigravity (`agy`) running Gemini 3.8 Flash
High via MCP. OpenCode and DeepSeek are not active providers. New work never
selects or starts a second provider, and an unavailable or failed Antigravity
run fails closed instead of being rerouted.

The package name `codex-opencode-bridge`, legacy bin names (`deepseek-subagent`,
`codex-opencode-bridge`), and `deepseek_*` tool names are retained strictly as
protocol compatibility aliases if required by the host. They do not identify
an active provider. Historical data and records from previous providers are
preserved in a read-only state. The canonical server name is `subagents`; the
canonical CLI bin is `subagents-mcp`.

## Execution contract

- The active route is `antigravity-flash-high` (`antigravity` /
  `gemini-3.8-flash-high`) and is the only enabled/default route in the
  installable capability contract. OpenCode/DeepSeek is not an active provider.
- `subagents_spawn` returns an accepted pending job; `subagents_follow` waits
  for the persisted completion event. Results are persisted before delivery.
- A timeout, invalid route, unavailable executable, or ambiguous result is a
  terminal failure. There is no provider, model, or route fallback.
- `fallback=forbidden` remains an external orchestration invariant; inbox
  delivery is a durable delivery mechanism, not a provider substitute.
- `deepseek_*` tool and server names remain protocol compatibility aliases
  if required by the host. They resolve exclusively to canonical
  Antigravity+Gemini execution and never select a historical provider. New
  instructions and integrations must use `subagents_*`.

## Development

```powershell
npm install
npm run build
npm test
node dist/cli.js doctor --json
```

- Development commands and shortcuts do not point to live tests or probes of
  OpenCode/DeepSeek. `npm test` runs local unit and integration suites.
- `scripts/doctor.ps1` is strictly read-only and does not compile or generate
  build artifacts in `dist/`.
- The live execution check is opt-in and must target the Antigravity/Gemini
  route in the current source checkout. Do not use historical provider smoke
  tests as evidence for the current execution contract.

## Install for Codex

Run the idempotent installer from PowerShell after building:

```powershell
.\scripts\install.ps1 -RegisterCodex
```

- **Idempotency & Process Safety**: Installation is idempotent and does not
  start, stop, or terminate any background processes in this patch. To run the
  daemon after installation, start it explicitly via `npm start` or
  `node dist/cli.js daemon`.
- **Diagnostic Profile**: The optional `-Profile` parameter controls
  diagnostic verification depth: `safe` (default, standard fast checks) or
  `full` (comprehensive SQLite integrity quick_check).
- **Scheduled Task**: `-InstallScheduledTask` registers the bridge daemon for
  logon using the same historical data directory so existing results remain
  visible. It updates task registration without terminating running processes.
- **Codex Registration**: The installer registers the canonical Codex MCP
  section `[mcp_servers.subagents]` and backs up the Codex configuration first.
  Existing legacy MCP sections are not deleted. See
  [docs/installation.md](docs/installation.md) for upgrade and uninstall rules.

## Historical data and migration

The existing `%LOCALAPPDATA%\DeepSeek Sub-Agent` directory is a compatibility
data location, not an active provider selection. SQLite database, result files,
inbox, spool, backups, and historical route/session identifiers are preserved
as read-only by default. Installation does not move or purge them, and uninstall
requires the explicit `-PurgeData -ConfirmPurge` pair before any data deletion
is attempted.

Historical records are read as legacy records. Their old provider labels are
not remapped into a new execution request, and they must never reactivate a
provider. A future data migration must be explicit, manifest-backed, and
performed only after quiescence has been proven; an unreadable or ambiguous
record fails closed.

## Route delegation to SubAgents

Codex App users should merge the canonical routing block from
[docs/orchestrator-instructions.md](docs/orchestrator-instructions.md) into
their user-level governance file. This repository does not modify the user's
global `AGENTS.md`, `GEMINI.md`, or Codex registration automatically beyond
the explicit `-RegisterCodex` option.

## Tools

Canonical tools:

- `subagents_spawn`: start work on the active Antigravity/Gemini route.
- `subagents_continue`: continue the same pinned agent when allowed.
- `subagents_status`: one observable progress snapshot; never a polling loop.
- `subagents_follow`: wait for the persisted completion/error event.
- `subagents_abort`: explicitly abort active work.
- `subagents_close`: close the logical agent after its result is consumed.
- `subagents_recover_result`: recover a persisted delivery result explicitly.

The legacy `deepseek_*` tool names (`deepseek_spawn`, `deepseek_follow`, etc.)
remain protocol compatibility aliases if required by the host. They route
exclusively to canonical Antigravity+Gemini execution; OpenCode/DeepSeek is not
an active provider.

The normal CLI hides UUIDs. Use `--json` for machine-readable details,
`--verbose` for technical IDs in list views, or `agent show <id>` for a full
diagnostic. The installer configures a 75-minute Codex MCP tool timeout,
which is longer than the 60-minute follow deadline plus its 10-minute grace
maximum.

## Ownership boundary

This documentation/package slice defines the Antigravity + Gemini contract,
installation behavior, compatibility rules, and packaging aliases. The
runtime cutover in `src/**` and user-level governance/configuration are
separate integration work and must be completed by their owners before a live
daemon can be claimed to satisfy this contract.
