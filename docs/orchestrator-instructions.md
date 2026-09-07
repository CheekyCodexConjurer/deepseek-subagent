# Canonical SubAgents MCP routing

This is the active routing block for Codex governance. It selects the one
supported execution path: Antigravity with Gemini 3.8 Flash High via MCP.
OpenCode and DeepSeek are not active providers. All failure handling remains
fail-closed.

## Active rules

- For unqualified delegation terms such as sub-agent, agent, worker, reader,
  writer, explorer, reviewer, or delegation, use canonical `subagents_*`
  MCP tools.
- New spawns use only route `antigravity-flash-high`:
  `antigravity` / `gemini-3.8-flash-high`.
- OpenCode and DeepSeek are not active providers.
- Do not send a remembered/default route override. An unknown, disabled, or
  conflicting route must be rejected before any workspace, process, session,
  or job side effect.
- If the canonical SubAgents MCP tools are unavailable, fail closed and
  report the blocker. Never switch to another provider, model, bridge, or
  local fallback silently.
- The orchestration lifecycle is always `fallback=forbidden`.
- A source change, health-looking configuration, or successful build does not
  prove that an already running daemon has been migrated. Runtime readiness,
  artifact identity, and a harmless Antigravity/Gemini canary must be proven
  separately.

## Canonical lifecycle

1. Call `subagents_spawn` or `subagents_spawn_batch` with a complete topic,
   task, mode, and explicit workspace when the workspace matters.
2. Continue useful independent work while the job is pending. Do not poll.
3. Use `subagents_status` only for a user-requested progress update, an
   unusually long task, or a snapshot that changes the next decision.
4. Once useful independent work is exhausted, use `subagents_follow` to
   consume the terminal result. A pending job is not evidence for synthesis.
5. Consume the result before a dependent gate or final response, then use
   `subagents_close` after review. Closing an agent and consuming its result
   are separate operations.
6. Use `subagents_continue` only for a correction, clarification, review, or
   continuation of the same pinned agent. A rejected, expired, or ambiguous
   request remains fail-closed.
7. Use `subagents_abort` only when active work must explicitly stop, and use
   `subagents_recover_result` only when a known persisted job result needs
   explicit delivery recovery.

## Failure and compatibility rules

- Never retry a timeout, transport error, invalid result, or unavailable
  executable through another provider or route.
- Never infer provider identity from a persisted display label, old session
  ID, historical route, or package alias.
- The legacy `deepseek_*` tool names, `deepseek-subagent` server name, and old
  package/bin names remain strictly protocol compatibility aliases if required
  by the host. They resolve exclusively to canonical Antigravity/Gemini
  execution; they do not select OpenCode or DeepSeek.
- Historical results, inbox entries, SQLite rows, and audit events are
  preserved in a read-only state. Keep their labels intact unless an explicit,
  manifest-backed migration is authorized. A malformed or ambiguous record
  fails closed.

## Workspace and history safety

- Preserve unrelated working-tree changes and never create, switch, or delete
  branches or worktrees as an implicit fallback.
- Context files must be contained, existing, regular, readable, and within
  the size limit before any side effect. Reject oversized input; never
  truncate or substitute another provider.
- Keep the historical `%LOCALAPPDATA%\DeepSeek Sub-Agent` data location until
  an explicit migration has proved quiescence, backup integrity, and restore.
- Do not purge history as part of install, uninstall, doctor, route selection,
  or delivery.
- Doctor (`scripts/doctor.ps1`) is strictly read-only and does not build or
  generate dist artifacts. Installation (`scripts/install.ps1`) is idempotent
  and does not start or terminate processes in this patch.

The repository copy of this block does not modify user-level `AGENTS.md` or
`GEMINI.md`. Applying it to those files is a separately authorized integration
step owned outside this documentation slice.
