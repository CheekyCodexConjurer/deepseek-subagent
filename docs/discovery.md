# Discovery evidence

Discovery was performed on 2026-08-07 in the local Windows workspace. This document separates observations from implementation inferences and unresolved claims.

## Observed

- Windows 10.0.26200, PowerShell 7.6.4, Node v24.15.0, npm/npx 11.12.1, OpenCode 1.18.11, Codex CLI 0.145.0 and Git 2.54.0.
- The workspace initially contained no application source, package manifest, test suite or Git repository. The bridge was implemented as a new local project.
- OpenCode served on loopback and returned healthy=true from GET /global/health. Its OpenAPI document was available at GET /doc; model and provider discovery were available at GET /api/model and GET /api/provider.
- A live disposable OpenCode session accepted opencode-go/deepseek-v4-flash with variant max. A request for exactly DISCOVERY_OK returned that text, finish=stop and no diff.
- The implemented managed daemon was exercised with a temporary config: HTTP spawn accepted a real task in about 71 ms, returned accepted=true and DeepSeek V4 Flash · Max, and the event-driven inbox later contained completed with REAL_DAEMON_OK.
- The OpenCode asynchronous endpoint POST /session/{id}/prompt_async returned HTTP 204. The global SSE stream emitted session status and session.idle events, so the bridge consumes that stream instead of polling session status.
- OpenCode credentials were visible only as configured provider names through its CLI. The bridge did not open, parse, copy or print auth.json.
- Codex app-server help exposed stdio, ws, unix and off transports. Generated local schemas exposed initialize, thread/start, thread/resume, thread/inject_items, turn/start, turn/steer, turn/interrupt and item/completed notifications.
- A disposable app-server initialize handshake succeeded through the bridge adapter. A command-line `-c` override did not expose a new MCP server, but an isolated temporary `CODEX_HOME` containing `[mcp_servers.deepseek_subagent]` did: `mcpServerStatus/list` returned serverInfo `deepseek-subagent`, title `DeepSeek Sub-Agent`, and all five bridge tools.
- On an ephemeral thread in that isolated app-server, `mcpServer/tool/call` invoked `deepseek_spawn` and returned `accepted=true` with a structured `jobId` in about the expected immediate path. The direct RPC did not emit an `item/completed` notification, so this is evidence of MCP exposure and structured identity only, not proof of model-originated item correlation.
- The installed MCP SDK 1.30.0 and the app-server inventory support server `title`, tool `title`, tool annotations, structured content and `_meta`; the live inventory showed the bridge server title and each technical tool name. No icon is advertised because the project has no required raster asset, and the Desktop UI's separation of technical names from display labels was not observable from this connection.
- The current Codex configuration exposed node_repl and blender MCP servers. No bridge registration or live Desktop thread correlation was present during discovery.
- Codex app-server lifecycle management reported that lifecycle is supported only on Unix. This bridge therefore keeps the adapter separate and does not claim to control the current Desktop process on Windows.

## Inferred

- Managed mode can reuse the same-user OpenCode provider authentication by starting the official OpenCode executable as a child process. The bridge needs only the provider and model identifiers, not provider secrets.
- A structured MCP result containing a job id can be correlated with an app-server item/completed notification when both processes share a compatible app-server connection.
- The durable inbox is the safe default until that correlation is configured and proven.

## Unknown or intentionally unclaimed

- Whether the installed Codex Desktop process exposes a supported local app-server transport for a separately launched bridge.
- Whether an authenticated Codex model turn or the running Desktop instance emits a usable `item/completed` event for this MCP call, and whether that independent connection can steer the originating Desktop thread.
- Whether a specific Codex Desktop build will accept a turn/start or turn/steer issued by an independent app-server connection for the originating conversation.
- Whether every OpenCode model variant supports max; the configured DeepSeek V4 Flash path was live-tested, but other models are not substituted automatically.

The validation gate treats these unknowns as capability limits. Health, configuration, generated schemas and static validators are not counted as proof of live Codex delivery.
