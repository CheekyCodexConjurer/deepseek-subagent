# Architecture

SubAgents MCP is a local bridge with one daemon, one stdio MCP process, an
Antigravity execution adapter, and a durable SQLite store. The active
provider contract is singular: Antigravity runs the authenticated
`agy` executable with Gemini 3.8 Flash High via MCP. OpenCode and DeepSeek
are not active providers.

## Runtime flow

1. A canonical `subagents_*` MCP tool calls the loopback daemon with the
   configured local bearer token. The MCP handshake remains independent of
   model execution.
2. The daemon validates the request, workspace boundary, context files, and
   the exact active route before creating any workspace, process, or job side
   effect.
3. `spawn` creates one logical agent and one job pinned to
   `antigravity-flash-high` (`antigravity` /
   `gemini-3.8-flash-high`). A remembered, unknown, disabled, or conflicting
   provider route fails closed.
4. The Antigravity adapter starts `agy` asynchronously in the prepared
   workspace. `subagents_spawn` returns an accepted pending obligation;
   `subagents_follow` observes completion, failure, approval, or abort.
5. Completion and observable activity are persisted. The complete sanitized
   result is written before delivery is attempted; the private inbox remains
   a durable delivery path, not a provider fallback.
6. Follow and recovery consume the job obligation explicitly. Closing the
   logical agent is a separate operation and never deletes its result or
   historical records.

There is no provider, model, route, or timeout failover. A timeout, invalid or
ambiguous result, unavailable `agy` executable, failed health check, or
unproven correlation is reported as a typed failure. `fallback=forbidden` is
preserved at the orchestration boundary.

## Boundaries

- `src/antigravity/**` owns the authenticated `agy` process contract, command
  argument validation, asynchronous execution, and fail-closed abort path.
- `src/service.ts` owns job identity, route pinning, state transitions, result
  persistence, delivery selection, and recovery decisions.
- `src/mcp.ts` exposes the canonical tools and the protocol compatibility aliases.
- `src/delivery/inbox.ts` owns durable local delivery when a verified
  same-thread binding is unavailable.
- The SQLite store owns transition checks, request-id idempotency, persisted
  deadlines, historical result paths, and route state.

The runtime cutover of `src/**` is outside this documentation/package
ownership. Until that owner lands the corresponding code change, this file is
the target contract and must not be read as proof that an already running
daemon has been migrated.

## State and compatibility

Agents and jobs retain their existing lifecycle and fail-closed transition
rules. New agents are pinned to the sole Antigravity/Gemini route. Existing
agents and results may contain historical provider labels, session IDs, flat
provider/model columns, or legacy route names. Those values are opaque
historical data: they are preserved strictly in read-only mode. They never
select an active provider for new work, and OpenCode/DeepSeek is not active.

The canonical MCP surface is:

- `subagents_spawn`
- `subagents_continue`
- `subagents_status`
- `subagents_follow`
- `subagents_abort`
- `subagents_close`
- `subagents_recover_result`

The legacy `deepseek_*` names and `deepseek-subagent` server name are retained
strictly as protocol compatibility aliases if required by the host. They
resolve to the same canonical Antigravity/Gemini implementation and must not
be used as active provider identities in new instructions.

## Route contract

The installable registry contains exactly one enabled/default route:

| Route | Provider | Model | Variant | State |
| --- | --- | --- | --- | --- |
| `antigravity-flash-high` | `antigravity` | `gemini-3.8-flash-high` | none | enabled and default |

The active route pointer may remain additive state in `route_state` for
backward-compatible databases, but it must resolve only to the route above.
An old pointer, explicit override, or stale config value is rejected with a
typed error; it is never silently remapped to Gemini and never used as a
fallback.

Route changes affect only new spawns. An in-flight agent keeps its persisted
route and identity. Under the target contract there is no second route to
select, and disabling or corrupting the sole route fails closed.

## Context files and execution safety

Context files are validated before any side effect: containment, existence,
regular-file status, readability, and the bounded size limit must all pass.
Antigravity receives trusted paths inside the prepared workspace instead of a
shell command line containing file contents. An oversized prompt or invalid
context file is rejected, never truncated or retried through another provider.

The adapter never reads, writes, or exposes Antigravity credentials. It
accepts one executable plus argument arrays, never an arbitrary shell command
line. Abort targets the actual owned execution tree; installation,
uninstallation, and doctor scripts do not kill processes or restart
Antigravity.

Installation is idempotent and does not start or terminate processes in this
patch. The doctor check (`scripts/doctor.ps1`) is strictly read-only and does
not generate build artifacts.

## History preservation and migration

The historical `%LOCALAPPDATA%\DeepSeek Sub-Agent` directory remains the
default compatibility location for `config.json`, SQLite, WAL/SHM files,
results, inbox, spool, backups, and audit material. Keeping that path avoids
an implicit copy or split-brain history migration. Historical data is preserved
as read-only.

Migration rules are:

1. Preserve the original files and legacy identifiers in read-only state.
2. Do not migrate while a daemon or job is active; prove quiescence first.
3. If a future explicit migration is needed, copy to a new location, create a
   manifest with sizes and SHA-256 values, validate the copy, and retain a
   restore path before changing the configured location.
4. Treat malformed, partial, private, or ambiguous records as unreadable
   historical data and fail closed. Never infer a new provider from a label.
5. Purging requires a separate, explicit operator confirmation; normal
   install, uninstall, doctor, and route operations preserve history.

The bridge's persisted result and inbox paths are compatibility storage, not
evidence that a historical provider is still installed or active.
