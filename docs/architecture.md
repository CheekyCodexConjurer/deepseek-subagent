# Architecture

DeepSeek Sub-Agent is a local bridge with one daemon, one stdio MCP process, an OpenCode manager/client and a durable SQLite store.

## Runtime flow

1. The MCP tool calls the loopback daemon with a bearer token.
2. The daemon validates the request, workspace boundary and context files.
3. spawn creates one logical agent and one new OpenCode session. continue uses the same agent and session.
4. The daemon sends one asynchronous OpenCode prompt and returns an accepted response.
5. A single SSE subscription receives session events. session.idle triggers one reconciliation of messages and diff.
6. The full result is written before delivery is attempted.
7. If a correlated Codex App Server binding exists, the result is sent with turn/steer or turn/start. Otherwise it is written to the private inbox and can be explicitly recovered.

In managed mode, an unexpected OpenCode child exit triggers bounded-backoff restarts on the same loopback port and credentials; the existing SSE client reconnects to that stable URL. Explicit daemon shutdown disables the restart loop.

There is no status polling loop. Startup recovery performs a bounded one-time reconciliation for unfinished jobs.

## Boundaries

- src/opencode/client.ts owns REST and SSE.
- src/opencode/manager.ts owns only bridge-started OpenCode children.
- src/service.ts owns state transitions, job identity, result persistence and delivery selection.
- src/codex/adapter.ts is an optional fail-closed integration. It never assumes that a separate app-server is the current Desktop thread.
- src/delivery/inbox.ts is the durable fallback.
- src/mcp.ts exposes only the five stable tools required by the contract.

## State

Agents move through created, working, needs_approval, completed, failed, aborted and closed. Jobs move through created, dispatching, running, needs_approval, completed, delivery_pending, delivered, failed and aborted. SQLite transition checks reject invalid transitions and unique request ids make MCP retries idempotent.

## Workspace strategies

shared sends the OpenCode session to the requested repository directory. worktree creates a detached Git worktree under the repository’s .deepseek-worktrees directory. Worktree creation is explicit and uses argument-safe Git process execution. When mode is edit and no strategy is supplied, worktree is the default; callers may explicitly choose shared when local changes must be visible.

The bridge refuses to create a worktree from `HEAD` when the repository has uncommitted changes or no committed `HEAD`, because that would silently omit local work. Callers must choose `shared` explicitly or preserve the changes before requesting `worktree`.
