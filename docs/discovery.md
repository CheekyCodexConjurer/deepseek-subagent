# Discovery record

This document separates the current installable contract from the historical
environment inventory. The historical section is retained for audit and
migration planning only; it is not an execution instruction.

## Current contract

- The canonical MCP server is `subagents` and the human-facing identity is
  **SubAgents MCP**.
- New execution uses only Antigravity `agy` with Gemini 3.8 Flash High through
  route `antigravity-flash-high`.
- OpenCode and DeepSeek are not active providers.
- The `antigravity-flash-high` route is enabled and default. Unknown, disabled,
  stale, or conflicting route values fail closed before a workspace, process,
  or job side effect.
- No provider, model, or route fallback is permitted. A failed timeout or
  ambiguous result is terminal and must remain visible in the audit trail.
- The `subagents_*` tools are canonical. The legacy `deepseek_*` names and
  `deepseek-subagent` registration are protocol compatibility aliases only if
  required by the host; they route exclusively to Antigravity+Gemini.

## Historical baseline (read-only evidence)

Earlier discovery runs observed historical OpenCode/DeepSeek traces, loopback
configurations, and legacy package and data-directory names. That evidence
explains why old SQLite rows, result envelopes, inbox files, route pointers,
and Codex registrations can still contain those strings. OpenCode and DeepSeek
are not active providers, and historical records remain strictly read-only.
They do not authorize starting that provider or selecting its route today.

The historical data location remains `%LOCALAPPDATA%\DeepSeek Sub-Agent` so
the cutover does not silently split the existing SQLite database, WAL/SHM
files, results, spool, or inbox. Historical records are preserved read-only
and are never remapped into new execution requests.

## Installation observations

The installer (`scripts/install.ps1`) is idempotent, registers
`[mcp_servers.subagents]`, keeps the existing Codex configuration backed up,
and does not write `package-lock.json` while installing dependencies.
Installation does not start, stop, or kill background processes in this patch.
The optional `-Profile` parameter controls diagnostic verification depth
(`safe` for fast checks, `full` for SQLite quick_check).

Existing legacy MCP sections are not silently removed; the operator can
migrate or remove them explicitly after the runtime cutover has been verified.

The scheduled-task migration changes only the registration name. It does not
stop an already running process, delete the data directory, or restart
Antigravity. The doctor script (`scripts/doctor.ps1`) is strictly read-only:
it performs diagnostic checks without generating build artifacts in `dist/`,
and reports state without deleting history, killing processes, or changing
Codex registration.

## Migration verification

Before any live activation, verify all of the following on the exact target:

1. `node dist/cli.js doctor --json` (or `scripts/doctor.ps1`) reports the
   canonical server, sole route, and Antigravity/Gemini identity.
2. `GET /health` and route status are ready and identify the same built
   artifact that was reviewed.
3. A harmless disposable Antigravity/Gemini canary completes and persists its
   full result without a second-provider attempt.
4. Existing historical rows and result files remain readable in read-only mode
   without changing their provider labels or creating a fallback route.
5. The daemon and configuration are handled through authorized channels only
   after active jobs, durable spool, and history have been audited.

This repository slice cannot prove those runtime facts: the daemon and the
runtime implementation live outside the documentation/package ownership.
