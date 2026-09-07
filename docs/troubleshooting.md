# Troubleshooting

Use canonical `subagents_*` names and the Antigravity/Gemini route while
diagnosing the bridge. OpenCode and DeepSeek are not active providers.
Historical provider labels in a result or log are read-only and do not
authorize a new execution path.

## First checks

Run the read-only diagnostic from the repository root:

```powershell
.\scripts\doctor.ps1
```

`scripts/doctor.ps1` is strictly read-only: it does not compile or generate
build artifacts in `dist/`, start Antigravity, stop or kill processes, or
alter Codex configuration.

For a machine-readable report, run `node dist/cli.js doctor --json`. Check the
canonical server identity, `antigravity-flash-high` as the sole enabled/default
route, the configured `agy` executable, data-directory health, and any open
obligations. Doctor never auto-closes jobs, consumes results, purges history,
or changes Codex registration.

If the daemon is expected to be running, inspect `GET /health` and route status
on the loopback endpoint. A healthy endpoint proves only daemon readiness; it
does not prove that the running artifact is the reviewed build or that an
Antigravity/Gemini canary has completed.

## Route and dispatch failures

New spawns must resolve to `antigravity-flash-high` with provider `antigravity`
and model `gemini-3.8-flash-high`. OpenCode and DeepSeek are not active
providers. An unknown, disabled, stale, or conflicting route is a typed failure
before any workspace, process, or job side effect. There is no provider or
route fallback. Do not retry the same request through another model or by
changing a historical route label.

If `agy` is missing, unauthenticated, unavailable, or returns an invalid
envelope, fix that Antigravity installation/configuration or leave the job
failed closed. Do not install or start a historical provider to make the
request pass.

## MCP registration

The canonical Codex section is `[mcp_servers.subagents]`. The installer backs
up the Codex configuration and leaves legacy `subagents-mcp`,
`deepseek-subagent`, and `deepseek_subagent` sections in place for explicit
review rather than deleting them silently. The legacy names are protocol
compatibility aliases only if required by the host; they resolve exclusively
to canonical Antigravity+Gemini execution.

If the Codex configuration cannot be parsed, repair or roll back that
configuration through its owner before retrying registration. Do not silently
register a second server or change the active provider.

## Follow, delivery, and obligations

`subagents_spawn` returns an accepted pending obligation. After useful
independent work is complete, use `subagents_follow`; it waits on the persisted
completion/error event and is not a status-polling loop. Use
`subagents_status` only for a meaningful observable snapshot. Consume the
terminal result before dependent synthesis, then close the agent separately.

If same-thread delivery is unavailable, use the durable inbox or explicit
result recovery. Inbox delivery is not a provider fallback. A missing,
partial, private, or ambiguous result remains a failure and must not be
reconstructed from a display name or old session identifier.

## Historical data

The compatibility data directory is `%LOCALAPPDATA%\DeepSeek Sub-Agent`.
SQLite, WAL/SHM, results, inbox, spool, backups, and old audit labels are
preserved as read-only by install, uninstall, and doctor. OpenCode/DeepSeek is
not active. Do not delete or move historical records while the daemon or a
job is active.

If migration is explicitly authorized, first prove quiescence, then copy the
source, record sizes and SHA-256 values, validate the destination and restore
path, and only then change configuration. Malformed or ambiguous historical
records fail closed; they are never mapped to a new provider.

## Process safety

The repository scripts do not stop a daemon, kill a process, or restart
Antigravity. Installation (`scripts/install.ps1`) is idempotent and does not
start or terminate processes in this patch. If a live process must be changed,
stop at this documentation boundary and obtain the separately authorized
runtime/operator procedure.
