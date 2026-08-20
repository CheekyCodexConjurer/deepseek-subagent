# Orchestrator contract

## Canonical routing block

MCP registration exposes the DeepSeek tools but does not by itself make them the default delegation route. Codex App users merge the block below into `%USERPROFILE%\.codex\AGENTS.md`:

```text
## DeepSeek Sub-Agent routing

- With or without $workflows, unqualified requests/terms such as sub-agent(s), agents, workers, readers, writers, explorers, reviewers or delegation mean the DeepSeek Sub-Agent MCP (`deepseek_spawn`, `deepseek_continue`, `deepseek_follow`).
- Do not use Codex native sub-agents (`multi_agent_v1__spawn_agent`, `spawn_agent`, `wait_agent`) unless the user explicitly requests them.
- If DeepSeek MCP tools are unavailable, fail closed and report the blocker; never silently fall back to native Codex sub-agents or local repository work.
- Every accepted DeepSeek job must be followed to a terminal result before dependent synthesis or final response.
- $workflows adds the formal mode/lifecycle contract but is not required for selecting the DeepSeek MCP backend.
```

When delegating work to DeepSeek:

1. Start the independent task with `deepseek_spawn`. Provide a concise topic, the complete task, the intended mode and an explicit workspace when it matters. The MCP tool always uses the bridge's live active route; never send a remembered/default route name such as `flash-max`. Only an operator changes the route through `route set <route>`. The bridge pins the selected route on the agent for the rest of its lifetime.
2. Continue doing all useful independent work yourself. Do not poll DeepSeek while useful work remains.
3. Use `deepseek_consult` only when the user requests progress, the task is taking unusually long, or the snapshot materially changes the next decision. It returns observable activity only; never private reasoning. Do not call it repeatedly to wait.
4. When all useful independent work is exhausted and the next step depends on DeepSeek, call `deepseek_follow`. It waits efficiently on completion events, approval, error, deadline or graceful finalization.
5. Review the follow result, including partial or timed-out evidence, before deciding the next action.
6. Use `deepseek_continue` for a direct clarification, correction, review or continuation of the same topic. Use `deepseek_spawn` for materially different work.

External orchestration lifecycle remains strictly `fallback=forbidden`, and route pinning and manual route overrides remain fail-closed; orchestrators must never implement client-side fallback. This is distinct from controlled internal timeout failover V1: an opt-in, bridge-internal, persisted, one-hop failover from Antigravity to configured OpenCode DeepSeek (`flash-max`) for `analyze`-mode tasks only upon confirmed Antigravity timeout. Hard exclusions apply: `edit` and `test` modes (which fail closed on timeout without failover to prevent unsafe duplicate filesystem modifications), user cancellation/abort, unconfirmed process termination, partial or invalid output, and disabled target routes. When internal failover executes, the transition is recorded in `agent_activity` and visible in result metadata; no daemon restart or automatic process cleanup is performed.

`deepseek_follow` does not replace the worker or create a new session. Cancelling it removes only the waiter; use `deepseek_abort` when the worker itself must stop.

A terminal `deepseek_follow` result consumes the job obligation (persisted); a `needs_approval` follow keeps the obligation pending and requires `deepseek_continue` with `permission_id` and `permission_reply`. Closing the agent with `deepseek_close` is separate from consuming the obligation: consume first, then close after review.

Keep writers open until review and corrections are final: a closed agent is not continuable. If a closed agent must continue anyway, `deepseek_continue` accepts an explicit `allow_respawn: true` only when the agent was closed AFTER a terminal job with a persisted result and was NOT explicitly aborted: the bridge then automatically spawns a NEW agent and a NEW OpenCode session in the same persisted workspace, topic, workspace strategy and pinned model route, records the lineage and activity on both agents, preserves or derives thread/turn correlation, and returns the NEW `agent_id`/`job_id` to follow. It never claims the closed session is the same session and never reopens the closed agent. `allow_respawn` fails closed for aborted agents, closed agents without a persisted result, busy agents, permission answers and scope changes; there is no provider fallback and no live-config route. Prefer keeping the agent open over respawning.

Use deepseek_abort only when active work must stop. Use deepseek_close after the logical agent is no longer needed. Use deepseek_recover_result only when delivery was interrupted and a technical job id is known; a successful recover also consumes the obligation.

The worker must report STATUS, SUMMARY, ASSUMPTIONS, CHANGES, FILES, TESTS, RISKS and UNRESOLVED. It must not claim an unrun validation and must not reveal private reasoning.
