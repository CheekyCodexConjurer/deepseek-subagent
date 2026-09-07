# Security and safety boundaries

- The daemon is local-only: its HTTP endpoint is bound to loopback and
  requests require the bridge-owned bearer token.
- New execution is pinned to Antigravity `agy` with Gemini 3.8 Flash High via MCP.
  OpenCode and DeepSeek are not active providers.
  The executable is one trusted file plus argument arrays, never an arbitrary
  shell command line.
- Antigravity credentials remain in the local Antigravity installation. The
  bridge never reads, writes, prints, or forwards them.
- Context files are checked for containment, existence, regular-file status,
  readability, and bounded size before creating a workspace, process, or job.
  Invalid or oversized input is rejected and never silently truncated.
- Unknown, disabled, stale, or conflicting route values fail closed with a
  typed error. There is no provider, model, route, or timeout fallback.
- `fallback=forbidden` remains the external orchestration invariant. The
  private inbox is durable delivery, not a provider substitute.
- An abort targets the owned Antigravity execution tree. Installation,
  uninstallation, and doctor scripts do not start or kill processes or restart
  Antigravity.
- Doctor (`scripts/doctor.ps1`) is strictly read-only and does not compile or
  generate build artifacts in `dist/`.
- Installation (`scripts/install.ps1`) is idempotent and does not start, stop,
  or terminate processes in this patch. Normal uninstall preserves data.

## Historical compatibility

Existing SQLite records, result envelopes, inbox files, audit events, and
configuration may contain historical OpenCode/DeepSeek labels, session IDs, or
flat provider columns. Those values are historical data, not executable authority.
They are preserved strictly in read-only mode for audit and recovery, are not
rewritten into a new provider selection, and must not reactivate a historical
execution path. OpenCode and DeepSeek are not active providers.

The legacy `deepseek_*` tool names, `deepseek-subagent` server name, package
name, and CLI bins remain protocol compatibility aliases if required by the
host. They all resolve to the same canonical Antigravity/Gemini contract and
cannot select a provider.

## Data and migration

The historical `%LOCALAPPDATA%\DeepSeek Sub-Agent` directory remains the
compatibility location for SQLite, WAL/SHM, results, inbox, spool, backups,
and logs. Install and uninstall do not delete or move it by default; all
historical records are read-only. A future explicit migration must:

1. prove that the daemon and all jobs are quiescent;
2. preserve the source and create a size/SHA-256 manifest for the destination;
3. validate the copy and a restore path before changing configuration; and
4. fail closed on partial, private, malformed, or ambiguous records.

Purging requires an explicit confirmation switch and is never an implicit
cleanup side effect.

## Operational proof

Static configuration and a passing unit suite do not prove a live cutover.
Before activation, verify the exact artifact, `/health` readiness, the active
route, a harmless Antigravity/Gemini canary, and historical result readability.
Do not stop or restart an unrelated daemon to manufacture that evidence.
