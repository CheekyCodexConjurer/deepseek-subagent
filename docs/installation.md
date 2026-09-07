# Installation

SubAgents MCP configures one local execution path: Antigravity (`agy`)
running Gemini 3.8 Flash High via MCP. OpenCode and DeepSeek are not active
providers. Any legacy names, bins, or `deepseek_*` aliases exist strictly for
protocol compatibility if required by the host. Historical data is preserved
as read-only.

## Prerequisites

- Windows PowerShell and Node.js 24 or newer.
- An authenticated Antigravity installation with `agy` available on `PATH`,
  or an explicit trusted executable path in the bridge configuration.
- A Codex CLI installation when `-RegisterCodex` is used.

The bridge does not import, copy, or expose provider credentials. Authentication
stays with the local Antigravity installation.

## Install

From the repository root, run:

```powershell
.\scripts\install.ps1 -RegisterCodex
```

The installer:

1. installs dependencies without running arbitrary scripts and without rewriting
   `package-lock.json`;
2. builds the bridge TypeScript files into `dist/`;
3. runs the local bridge configuration and doctor checks;
4. when `-RegisterCodex` is supplied, backs up the Codex configuration, removes
   any preexisting canonical registration, and registers `[mcp_servers.subagents]`;
5. sets `tool_timeout_sec = 4500` on the canonical section (and preserves
   existing compatibility sections);
6. optionally registers the bridge daemon at logon with `-InstallScheduledTask`.

### Diagnostic Profile parameter

The optional `-Profile` parameter controls diagnostic verification depth:
- `safe` (default): runs standard fast doctor diagnostics without running
  blocking SQLite PRAGMA quick_check.
- `full`: runs comprehensive doctor diagnostics with `--full` (executes SQLite
  integrity quick_check).

Example:

```powershell
.\scripts\install.ps1 -Profile safe -RegisterCodex
```

### Process lifecycle safety

Installation is idempotent and does not start, stop, or kill background
processes in this patch. To run the daemon after installation, start it
explicitly via:

```powershell
npm start
# or: node .\dist\cli.js daemon
```

Installation never starts Antigravity, kills a process, rewrites active routes,
or selects a fallback provider.

The installer keeps the historical `%LOCALAPPDATA%\DeepSeek Sub-Agent`
configuration and data location so existing SQLite, results, inbox, spool, and
backups remain addressable in read-only mode.

## Codex registration and compatibility

New registrations use the neutral server name `subagents`. Existing
`subagents-mcp`, `deepseek-subagent`, or `deepseek_subagent` sections are
legacy compatibility entries and are not removed automatically. They must be
reviewed after the runtime cutover; a stale or conflicting entry must fail
closed rather than silently redirecting work.

The package exposes the canonical `subagents-mcp` and `subagents` bins while
retaining `deepseek-subagent` and `codex-opencode-bridge` as CLI aliases. The
historical npm package name is retained because the lockfile and existing
installations are outside this ownership slice.

## Scheduled task

`-InstallScheduledTask` registers `SubAgents MCP Daemon`. If the old scheduled
task name is present, the installer removes that scheduler registration before
creating the canonical registration; it does not stop an already running
process and does not touch data. This prevents two logon registrations from
starting the same bridge after migration.

## Uninstall

```powershell
.\scripts\uninstall.ps1
```

Uninstall removes the canonical scheduled-task registration and, when
`-RemoveCodex` is supplied, the canonical Codex MCP registration. It never
stops the daemon, kills a process, restarts Antigravity, or deletes history.
The legacy Codex aliases remain by default for compatibility. To remove those
registrations as a separate explicit choice, use:

```powershell
.\scripts\uninstall.ps1 -RemoveCodex -RemoveLegacyCodex
```

Data removal is a separate destructive operation and requires both switches:

```powershell
.\scripts\uninstall.ps1 -PurgeData -ConfirmPurge
```

The purge is restricted to the verified historical data directory and is not
part of normal uninstall. Do not purge while a daemon, job, or database writer
is active; prove quiescence and retain a verified backup first.

## Doctor

```powershell
.\scripts\doctor.ps1
```

Doctor (`scripts/doctor.ps1`) is strictly read-only: it runs `doctor --json`
without compiling or generating build artifacts in `dist/`. It is a diagnostic
check: it does not alter Codex registration, purge data, kill a process, or
restart Antigravity. Runtime readiness and a real Antigravity / Gemini canary
must still be proven separately on the exact reviewed artifact.

## External integration boundary

This repository does not edit the user's global `%USERPROFILE%\.codex\AGENTS.md`,
`GEMINI.md`, or existing external daemon/configuration in this slice. After
reviewing the runtime owner's changes, apply the canonical routing block from
`docs/orchestrator-instructions.md` through the separately authorized
configuration owner. Do not treat a source change or a successful local build
as proof that an already running daemon has switched providers.
