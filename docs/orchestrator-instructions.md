# Orchestrator contract

Use deepseek_spawn for a new independent task. Provide a concise topic, the complete task, the intended mode and an explicit workspace when it matters. The tool is asynchronous and returns after dispatch; do not poll.

When the result is delivered, review its summary, files, tests, risks and unresolved items before calling deepseek_continue. Continue keeps the same logical agent and OpenCode session. It rejects a concurrent active job, so serialize follow-ups per agent.

Use deepseek_abort only when active work must stop. Use deepseek_close after the logical agent is no longer needed. Use deepseek_recover_result only when delivery was interrupted and a technical job id is known.

The worker must report STATUS, SUMMARY, ASSUMPTIONS, CHANGES, FILES, TESTS, RISKS and UNRESOLVED. It must not claim an unrun validation and must not reveal private reasoning.
