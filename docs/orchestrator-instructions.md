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

1. Start the independent task with `deepseek_spawn`. Provide a concise topic, the complete task, the intended mode and an explicit workspace when it matters.
2. Continue doing all useful independent work yourself. Do not poll DeepSeek while useful work remains.
3. Use `deepseek_consult` only when the user requests progress, the task is taking unusually long, or the snapshot materially changes the next decision. It returns observable activity only; never private reasoning. Do not call it repeatedly to wait.
4. When all useful independent work is exhausted and the next step depends on DeepSeek, call `deepseek_follow`. It waits efficiently on completion events, approval, error, deadline or graceful finalization.
5. Review the follow result, including partial or timed-out evidence, before deciding the next action.
6. Use `deepseek_continue` for a direct clarification, correction, review or continuation of the same topic. Use `deepseek_spawn` for materially different work.

`deepseek_follow` does not replace the worker or create a new session. Cancelling it removes only the waiter; use `deepseek_abort` when the worker itself must stop.

Use deepseek_abort only when active work must stop. Use deepseek_close after the logical agent is no longer needed. Use deepseek_recover_result only when delivery was interrupted and a technical job id is known.

The worker must report STATUS, SUMMARY, ASSUMPTIONS, CHANGES, FILES, TESTS, RISKS and UNRESOLVED. It must not claim an unrun validation and must not reveal private reasoning.
