# Architecture

DeepSeek Sub-Agent is a local bridge with one daemon, one stdio MCP process, an OpenCode manager/client and a durable SQLite store.

## Runtime flow

1. The MCP tool calls the loopback daemon with a bearer token. The stdio MCP handshake and tool listing never wait for daemon or OpenCode startup; daemon readiness is bootstrapped lazily and memoized on the first tool call, and concurrent first calls share one bootstrap.
2. The daemon validates the request, workspace boundary and context files.
3. spawn creates one logical agent and one new OpenCode session. continue uses the same agent and session.
4. The daemon sends one asynchronous OpenCode prompt and returns an accepted response.
5. A single SSE subscription receives session events. session.idle triggers one reconciliation of messages and diff. High-volume `message.part.delta` events are not persisted, and an idle session without non-empty assistant text (tool-only or reasoning-only tails included) never becomes a completed success.
6. Observable events and bridge operations are written to `agent_activity`; consult reads this persisted table and never asks OpenCode for hidden reasoning or status polling.
7. follow registers one shared waiter per job. It stays open until the persisted completion/error/approval event, a deadline, or cancellation of that waiter. Only the deadline and grace timers are allowed; there is no job-status polling loop.
8. The full result is written before delivery is attempted. A grace-period result is marked `completed_partial`; an unresponsive worker is aborted after grace and marked `timed_out` with the last available evidence.
9. Same-chat push is experimental and disabled by default. If explicitly enabled and a live correlation exists, the result can use turn/steer or turn/start. Otherwise it is written to the private inbox and can be explicitly recovered.

In managed mode, an unexpected OpenCode child exit triggers bounded-backoff restarts on the same loopback port and credentials; the existing SSE client reconnects to that stable URL. Explicit daemon shutdown disables the restart loop.

Startup recovery performs a bounded one-time reconciliation for unfinished jobs, reconstructs persisted follow/grace and approval deadlines without extending them, retries timeout evidence capture when the first capture failed, and does not send a duplicate finalization prompt. Antigravity runs live entirely inside the daemon process (in-memory provider plus process handle), so after a daemon loss a stranded active Antigravity job (dispatching/running/following/finalizing) cannot be recovered, awaited or aborted: startup recovery terminalizes it as `failed` with a clear reason instead of leaving it dispatching/working forever, and the agent is failed accordingly. An orphaned agy child process, if any, is not controllable from the recovered daemon; the job state is terminal regardless of whatever the orphan does afterwards.

## Boundaries

- src/opencode/client.ts owns REST and SSE.
- src/opencode/manager.ts owns only bridge-started OpenCode children.
- src/service.ts owns state transitions, job identity, result persistence and delivery selection. Per-agent continuation admission and deadline revalidation keep approval/continue races fail-closed.
- src/codex/adapter.ts is an optional fail-closed integration. It can launch a separate stdio App Server or connect to an explicitly configured local `ws://` endpoint; it never assumes that either is the current Desktop thread.
- src/codex/websocket.ts owns the opt-in WebSocket JSON-RPC transport. It rejects non-loopback endpoints and does not accept Unix socket paths on Windows.
- When the Codex adapter is initialized but an MCP correlation has not arrived yet, a completed result waits up to the configured correlation window (10 seconds by default) before using the inbox. The WebSocket endpoint is a same-user local trust boundary, not an authenticated remote service; it must not be exposed or forwarded off-host.
- src/delivery/inbox.ts is the durable fallback.
- src/mcp.ts exposes seven stable tools: spawn, continue, consult, follow, abort, close and recover_result.

## State

Agents move through created, working, needs_approval, completed, completed_partial, timed_out, failed, aborted and closed. Jobs additionally use following and finalizing, then completed_partial or timed_out when the deadline path is used. SQLite transition checks reject invalid transitions, follow/grace/approval deadlines survive daemon restart, and unique request ids make MCP retries idempotent. Completed, failed and timed-out writers remain continuable until `deepseek_close`; aborted agents are non-continuable and auto-close. A closed agent is never reopened: the optional `allow_respawn` flag on `deepseek_continue` is the explicit recovery that spawns a NEW agent and a NEW OpenCode session with a persisted lineage link (`parent_agent_id`), reusing only the parent's persisted topic, workspace path, workspace strategy and pinned model route, with auditable activity on both agents and thread/turn correlation hints preserved or derived from the parent's last job. Respawn fails closed (typed 409/400) for aborted agents, closed agents without a persisted result, busy agents, permission-field answers and any scope change; there is no provider fallback and no live-config route. MCP-supplied `thread_id`/`turn_id` are persisted as validated provenance-tagged hints (`hint_thread_id`, `hint_turn_id`, `hint_source`) and are never synthesized into bindings: delivery still requires the authoritative App Server `item/completed` correlation, and doctor/status report hint-versus-binding counts.

## Workspace strategies

shared sends the OpenCode session to the requested repository directory. worktree creates a detached Git worktree under the repository’s .deepseek-worktrees directory. Worktree creation is explicit and uses argument-safe Git process execution. When mode is edit and no strategy is supplied, worktree is the default; callers may explicitly choose shared when local changes must be visible.

The bridge refuses to create a worktree from `HEAD` when the repository has uncommitted changes or no committed `HEAD`, because that would silently omit local work. Callers must choose `shared` explicitly or preserve the changes before requesting `worktree`.

## Model routes

Dispatch runs through a config-based route registry (`modelRoutes`). Each route pins provider, model and variant. The built-in registry ships `flash-max` (`opencode-go` / `deepseek-v4-flash` / `max`) enabled and default, `pro-max` (`opencode-go` / `deepseek-v4-pro` / `max`) registered but disabled, and `antigravity-flash-high` (`antigravity` / `gemini-3.7-flash-high`) registered and enabled for operator selection (never the default).

New spawns resolve the **active model route**: an operator-set pointer persisted in the bridge SQLite store (`route_state`), which effectively initializes to the configured `defaultModelRoute` (`flash-max`) when no pointer exists. The pointer is additive state — config.json is never rewritten while a daemon is live.

The active route is operator-controlled through the authenticated loopback daemon and CLI (`route list`, `route status`, `route set <route>`, with `--json`). `route set` validates that the target is registered and enabled (typed `unknown_route` / `route_disabled` 400 otherwise), persists the pointer, and applies immediately — a live-daemon set needs no daemon restart, and the effective route is exposed in `/health`, `route status` and `route list`. All route commands require the running daemon; when it is not running they fail closed with guidance and the CLI never writes the route pointer itself, so a stopped daemon cannot race a live one on the store.

The MCP `deepseek_spawn` tool never exposes or forwards `model_route`: it always uses the live active route, preventing a remembered route such as `flash-max` from conflicting after an operator switch. The lower-level HTTP spawn contract retains typed validation for legacy/direct callers: an unknown or disabled route fails closed with a stable typed 400 before any workspace, worktree, session or job side effect; another registered route is denied with a typed 403 `route_override_denied`. There is no fallback route, and a `model_route` never changes the active pointer.

The resolved route is persisted on the agent (`model_route`). Continue, approval resume/reply, graceful finalization, recovery and startup reconciliation always resolve the agent's persisted route — never the mutable live config defaults and never the mutable active-route pointer — so changing the active route or disabling a route never redirects an in-flight agent. Agents created before route pinning (or whose route was removed from the registry) keep dispatching on their persisted flat `model_provider_id`/`model_id`/`model_variant` columns, so old agents remain fully compatible.

## Antigravity provider

The `antigravity-flash-high` route runs the authenticated `agy` executable directly in the prepared workspace (never OpenCode, never a session). Dispatch preserves the MCP asynchronous contract: `deepseek_spawn` returns an accepted pending obligation as soon as the job is created, the agy process runs asynchronously in a background task, and `deepseek_follow` observes completion, failure or abort. An abort that lands between job creation and dispatch prevents the agy launch entirely and never transitions an aborted/closed agent back to working. There is never a fallback to another provider or model, and the explicit abort signal kills the actual process tree.

For Antigravity, validated `context_files` are supplied as paths inside the prepared workspace instead of copying their contents into the `agy` command line; the worker reads them directly when relevant. The bridge checks the complete command prompt against the 30,000-character safe limit before it creates a worktree, session, agent or job. A genuinely oversized task, visual context or path list returns a typed 400 early instead of accepting a job that would fail before execution.

Antigravity sandbox configuration is strictly opt-in and bounded:

- `antigravitySandbox` (boolean, default `false`): when `true`, agy runs with `--sandbox` so its filesystem access is confined; sandboxing is never implied by any other flag.
- `antigravityCommand` (string, default `null`): optional per-bridge override for the agy executable — a bare command name resolved through PATH or an absolute path to a trusted local executable. When `null`, the default `agy.exe`/`agy` PATH lookup applies. It is a single executable, never a shell command line with arguments (spawned argv-style with `shell: false`), and it deliberately avoids any global PATH change: the override applies only to this bridge's Antigravity agents. At config load, values containing whitespace are rejected unless they are absolute paths — shell-like command lines never load — while absolute executable paths that contain spaces remain allowed. The operator is responsible for pointing it at a trusted executable.
- `antigravityAddDirs` (string array, default `[]`): absolute directory paths mounted into the sandbox as writable; relative or non-absolute entries are dropped at config load, and each entry must be an explicit absolute path chosen by the operator.
- `antigravityAutoApprovePermissions` (boolean, default `false`): opt-in auto-approval — when `true`, the run passes `--dangerously-skip-permissions` so agy never blocks on interactive permission prompts. It is independent of `antigravitySandbox` and does not require the sandbox: an unsandboxed run can run with auto-approval, and a sandboxed run can run without it. It never broadens the sandbox and carries no credentials or secrets.
- No secret values are ever read, written or exposed by the bridge; the `agy` executable authenticates itself with its own locally configured credentials.

## Errors

All errors crossing the HTTP or MCP boundary are typed: a stable machine-readable code plus an explicit HTTP status (400 invalid request, 401 unauthorized, 403 route override denied, 404 missing resource, 409 state conflict, 500 internal). Route selection is operator-controlled: the MCP spawn tool always follows the active route, while legacy/direct HTTP callers that name a route still fail closed with typed `unknown_route`, `route_disabled` or `route_override_denied` errors before any side effect. HTTP bodies carry `{ error, code, status }` plus `details`, and, for busy errors, `retry: false` and the conflicting `jobId`; the MCP layer propagates applicable codes and `details` through `structuredContent` and additionally marks `retry: false` for non-retryable statuses. Consumers branch on the code, never on message text.

## Obligation and result consumption

A terminal result becomes explicitly consumed when `deepseek_follow` or `deepseek_recover_result` returns a usable final result; the consumption timestamp is persisted on the job (`result_consumed_at`). `needs_approval` follows keep the obligation pending. Closing an agent is a separate operation from consuming an obligation. Doctor and the `obligations` CLI command warn about unconsumed terminal results, terminal agents still open, open obligations, and genuinely stale follow windows — a stale window is one whose grace deadline has passed while the job is still following/finalizing and was not auto-armed, so fresh and auto-armed windows never produce false positives. Doctor never auto-closes and never auto-consumes.

## Retention

Events and `agent_activity` grow without bound, so an opt-in retention subsystem prunes them safely. Modes: `auto`, `disabled` (default), `dry-run`, `enabled`. `auto` enables pruning only when the database is provably empty (fresh install); a non-empty legacy database stays disabled until an explicit offline `retention dry-run` (preview) or `retention enabled --confirm`. The gate is intrinsic to the database, not CLI-only: the offline CLI flow writes an in-database preparation marker (`retention_meta`), so a hand-edited `retentionMode=enabled` can never arm online pruning on a non-empty legacy database. Pruning touches ONLY `events` and `agent_activity` — never agents, jobs, results, deliveries, codex bindings or the inbox. Rows linked to active, open, unconsumed or undelivered jobs are protected, fresh rows newer than the retention horizon are kept, and the newest activity rows per agent are always retained (activity floor of 50). The pass is chunked, time-bounded and idempotent; the only maintenance step is a PASSIVE WAL checkpoint, never an online VACUUM. Prune-support indexes are created cheaply for an empty database at migrate time and only by the offline CLI path for legacy databases. Dry-run and real passes report accurate protected counts (rows never eligible for pruning), so doctor can never claim zero protected rows when protected rows exist. Run retention commands while the daemon is stopped.
