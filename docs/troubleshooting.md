# Troubleshooting

## Doctor

Run:

    node dist/cli.js doctor --json

Doctor separates local prerequisites, daemon reachability, OpenCode health, persisted progress/follow readiness, the Codex tool timeout and the unproven Codex correlation capability. A warning about Codex delivery means inbox fallback is active; it is not permission to substitute a provider. `Same-chat push: Experimental / Disabled` is the expected default.

## No result in the conversation

The daemon should have created data/inbox/{job id}.json below the configured data directory. Use the technical id from the MCP structured result:

    node dist/cli.js recover {job id} --json

Recovery is an explicit action, not a polling loop.

## Consult versus follow

Use `deepseek_consult` once when the user asks for progress or when an observable snapshot changes the next decision. It returns compact activity such as dispatched work, files/tests reported by the worker, OpenCode events, approvals and the last known status; it never returns private reasoning.

After all useful independent work is complete, use `deepseek_follow`. The call remains open on an internal event waiter. It does not poll `/status` or repeatedly call consult. Approval and terminal errors return immediately. Cancelling the follow call removes only the waiter; use `deepseek_abort` to stop the worker.

## MCP cannot connect to the local daemon

The MCP handshake and tool listing never wait for daemon or OpenCode startup. Daemon readiness is bootstrapped lazily and memoized on the first tool operation: the MCP process health-checks the bridge HTTP endpoint and starts the detached daemon automatically when it is offline, sharing one bootstrap across concurrent first calls. If startup still times out, the tool call returns a clear "daemon is not ready" error; inspect `data/daemon.log` below the configured data directory and run `node dist/cli.js doctor --json`.

## OpenCode startup failure

Check the configured binary and run the same-user OpenCode CLI. Managed mode searches the installed executable path and does not execute .cmd or .ps1 shims. Attach mode requires a loopback URL and a matching optional password.

## Provider or model failure

Dispatch resolves strictly through the configured route registry. The built-in default is the `flash-max` route (`opencode-go` / `deepseek-v4-flash` / `max`); `pro-max` is registered but disabled. `deepseek_spawn` accepts an optional `model_route`; an unknown or disabled route is rejected with a typed 400 (`unknown_route` / `route_disabled`) before any workspace or session side effect. There is no fallback route: the bridge never substitutes another provider or model. Inspect the persisted job error after a dispatch failure.

## Model route pinning

The route chosen at spawn is persisted on the agent. Continue, approval resume/reply, graceful finalization, recovery and restart reconciliation always use that persisted route — changing the config default or disabling a route later never redirects an in-flight agent. Agents created before route pinning keep dispatching on their persisted flat provider/model/variant columns.

## Context files rejected with 400

Context files are validated before any side effect: they must be inside the workspace, must exist, must be regular readable files, and must not exceed `maxContextFileBytes` (1 MB default). The rejection is a typed 400 (`context_file_invalid`) and no session or worktree is created. Oversized input is rejected, never truncated.

## Obligation and consumption warnings

Doctor and `node dist/cli.js obligations` warn about: terminal results with a persisted result that were never consumed by `deepseek_follow` or `deepseek_recover_result`; terminal agents still open (not closed); open job obligations; and genuinely stale follow windows (grace deadline passed while still following/finalizing and not auto-armed). Fresh windows and auto-armed windows are never flagged. Warnings are read-only: nothing is auto-closed or auto-consumed.

## Retention

Retention is opt-in and defaults to `disabled`. `node dist/cli.js retention dry-run` previews what would be pruned (nothing is deleted) and performs the explicit offline preparation; `node dist/cli.js retention enabled --confirm` arms pruning on an existing database after the preview; `auto` enables pruning only on a provably empty database. The preparation gate is intrinsic: only the offline CLI flow writes the in-database preparation marker, so hand-editing `retentionMode` to `enabled` alone never arms online pruning on a non-empty legacy database (doctor reports the reason). Pruning deletes only old `events` and `agent_activity` rows of settled, consumed jobs — agents, jobs, results, deliveries, bindings and the inbox are never touched, and active/open/unconsumed/undelivered/fresh rows plus the newest activity per agent are always protected. Run retention commands while the daemon is stopped.

## Agent lifecycle after a terminal result

A terminal follow result closes the job obligation only. The DeepSeek agent itself stays open and continuable: completed, failed and timed-out writers accept `deepseek_continue` until you call `deepseek_close` after reviewing the result. Aborted agents are not continuable and are auto-closed safely. The follow output states this explicitly so a closed obligation is never mistaken for a closed agent. Consumption is persisted separately: a terminal follow or a successful recover marks the result consumed (`result_consumed_at`); closing the agent never consumes an obligation and consuming never closes the agent.

A closed agent is not continuable and is never reopened. If the agent was closed AFTER a terminal job with a persisted result and was NOT explicitly aborted, `deepseek_continue` with `allow_respawn: true` is the explicit recovery: it automatically creates a NEW agent and a NEW OpenCode session in the same persisted workspace/topic/strategy with the same pinned model route, records the lineage (`parent_agent_id`) and auditable activity on both agents, preserves or derives thread/turn correlation hints, and returns the NEW agent/job ids to follow. `allow_respawn` is rejected (typed 409/400) for explicitly aborted agents, agents closed without a persisted result, busy agents, permission-field answers and any scope change; there is no provider fallback and no live-config route. Keep writers open until review is final to avoid needing a respawn.

## Codex delivery is unavailable

This is expected until a compatible Codex App Server connection is explicitly configured and live-tested. On Windows, the bridge accepts a local WebSocket endpoint in `codexAppServerSocket`, for example `ws://127.0.0.1:PORT`; the endpoint must be started separately with the installed Codex App Server. That server is not automatically the current Desktop process. The generated protocol can support thread/start and turn/steer, but schema availability does not prove that the current Desktop conversation can be correlated. Keep using the inbox until the probe proves both sides.

The WebSocket option is intentionally restricted to loopback and assumes the same-user local trust boundary. Do not bind it to a non-loopback host, expose it through port forwarding, or treat it as an authenticated remote endpoint. When configured, a completed result waits briefly for its `item/completed` correlation; if no valid event arrives, it falls back to the private inbox.

## Follow deadline

The default is 20 minutes of work plus 5 minutes of graceful finalization. The maximum is 60 plus 10 minutes. At the first deadline the bridge sends the same finalization prompt to the same OpenCode session. If that session is busy and rejects the prompt, the bridge aborts only the active turn, preserves the session, and resubmits the prompt asynchronously. If grace expires, the worker is aborted and the result contains `timed_out` plus the last available progress/evidence.

Follow and approval deadlines are persisted. A daemon restart resumes the remaining window instead of starting a fresh one; if timeout evidence capture was interrupted, startup or explicit recovery retries the last available messages and diff.

## Agent lifecycle after a terminal result

A terminal follow result closes the job obligation only. The DeepSeek agent itself stays open and continuable: completed, failed and timed-out writers accept `deepseek_continue` until you call `deepseek_close` after reviewing the result. Aborted agents are not continuable and are auto-closed safely. The follow output states this explicitly so a closed obligation is never mistaken for a closed agent.

A closed agent is never reopened; `deepseek_continue` with `allow_respawn: true` is the explicit lineage recovery described in the lifecycle section above.

## Unknown dispatch outcome

If a `prompt_async` dispatch fails before an HTTP response (timeout or transport failure), the prompt may still have been accepted. The bridge keeps the job and agent active and recoverable instead of failing them, blocks a duplicate continuation, and lets completion events, restart reconciliation or the follow deadline settle the outcome. A definite HTTP rejection still fails the job.
