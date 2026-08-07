# Architecture

DeepSeek Sub-Agent is a local bridge with one daemon, one stdio MCP process, an OpenCode manager/client and a durable SQLite store.

## Runtime flow

1. The MCP tool calls the loopback daemon with a bearer token.
2. The daemon validates the request, workspace boundary and context files.
3. spawn creates one logical agent and one new OpenCode session. continue uses the same agent and session.
4. The daemon sends one asynchronous OpenCode prompt and returns an accepted response.
5. A single SSE subscription receives session events. session.idle triggers one reconciliation of messages and diff.
6. Observable events and bridge operations are written to `agent_activity`; consult reads this persisted table and never asks OpenCode for hidden reasoning or status polling.
7. follow registers one shared waiter per job. It stays open until the persisted completion/error/approval event, a deadline, or cancellation of that waiter. Only the deadline and grace timers are allowed; there is no job-status polling loop.
8. The full result is written before delivery is attempted. A grace-period result is marked `completed_partial`; an unresponsive worker is aborted after grace and marked `timed_out` with the last available evidence.
9. Same-chat push is experimental and disabled by default. If explicitly enabled and a live correlation exists, the result can use turn/steer or turn/start. Otherwise it is written to the private inbox and can be explicitly recovered.

In managed mode, an unexpected OpenCode child exit triggers bounded-backoff restarts on the same loopback port and credentials; the existing SSE client reconnects to that stable URL. Explicit daemon shutdown disables the restart loop.

Startup recovery performs a bounded one-time reconciliation for unfinished jobs, reconstructs persisted follow/grace and approval deadlines without extending them, retries timeout evidence capture when the first capture failed, and does not send a duplicate finalization prompt.

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

Agents move through created, working, needs_approval, completed, completed_partial, timed_out, failed, aborted and closed. Jobs additionally use following and finalizing, then completed_partial or timed_out when the deadline path is used. SQLite transition checks reject invalid transitions, follow/grace/approval deadlines survive daemon restart, and unique request ids make MCP retries idempotent.

## Workspace strategies

shared sends the OpenCode session to the requested repository directory. worktree creates a detached Git worktree under the repository’s .deepseek-worktrees directory. Worktree creation is explicit and uses argument-safe Git process execution. When mode is edit and no strategy is supplied, worktree is the default; callers may explicitly choose shared when local changes must be visible.

The bridge refuses to create a worktree from `HEAD` when the repository has uncommitted changes or no committed `HEAD`, because that would silently omit local work. Callers must choose `shared` explicitly or preserve the changes before requesting `worktree`.
