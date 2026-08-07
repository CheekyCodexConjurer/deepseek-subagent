# Troubleshooting

## Doctor

Run:

    node dist/cli.js doctor --json

Doctor separates local prerequisites, daemon reachability, OpenCode health and the unproven Codex correlation capability. A warning about Codex delivery means inbox fallback is active; it is not permission to substitute a provider.

## No result in the conversation

The daemon should have created data/inbox/{job id}.json below the configured data directory. Use the technical id from the MCP structured result:

    node dist/cli.js recover {job id} --json

Recovery is an explicit action, not a polling loop.

## MCP cannot connect to the local daemon

The MCP performs a one-time readiness bootstrap and starts the detached daemon automatically when it is offline. If startup still times out, inspect `data/daemon.log` below the configured data directory and run `node dist/cli.js doctor --json`. The MCP only waits for daemon readiness; it does not poll job status.

## OpenCode startup failure

Check the configured binary and run the same-user OpenCode CLI. Managed mode searches the installed executable path and does not execute .cmd or .ps1 shims. Attach mode requires a loopback URL and a matching optional password.

## Provider or model failure

The bridge uses the configured opencode-go provider, deepseek-v4-flash model and max variant by default. It does not silently fall back to another provider or model. Inspect the persisted job error after the failure.

## Codex delivery is unavailable

This is expected until a compatible Codex App Server connection is explicitly configured and live-tested. On Windows, the bridge accepts a local WebSocket endpoint in `codexAppServerSocket`, for example `ws://127.0.0.1:PORT`; the endpoint must be started separately with the installed Codex App Server. That server is not automatically the current Desktop process. The generated protocol can support thread/start and turn/steer, but schema availability does not prove that the current Desktop conversation can be correlated. Keep using the inbox until the probe proves both sides.

The WebSocket option is intentionally restricted to loopback and assumes the same-user local trust boundary. Do not bind it to a non-loopback host, expose it through port forwarding, or treat it as an authenticated remote endpoint. When configured, a completed result waits briefly for its `item/completed` correlation; if no valid event arrives, it falls back to the private inbox.
