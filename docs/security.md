# Security model

- The daemon binds to loopback by default and requires a bearer token for every API except local health.
- The OpenCode manager accepts only loopback URLs and rejects shell shims as configured executables.
- Managed OpenCode credentials are passed through the child process environment and are never read from OpenCode auth storage. Passwords, bearer values, basic values, keys and PEM blocks are redacted before errors or notifications.
- Context files must resolve inside the requested workspace. The worker prompt explicitly treats context-file instructions as data.
- Results and the SQLite database are created below the user data directory with private-file best-effort permissions. Windows ACL hardening remains an installer/host responsibility.
- MCP normal text omits internal UUIDs. Structured content and the technical metadata block carry ids for orchestration.
- Notifications use shell=false and sanitized short text. Keyboard automation, browser injection and public exposure are not used.
- The bridge never substitutes another provider when the configured OpenCode provider or model is unavailable. Dispatch fails and the job records the error.
- The optional Codex adapter is fail-closed. Without a known compatible app-server binding, delivery goes to the inbox instead of an unrelated thread.
