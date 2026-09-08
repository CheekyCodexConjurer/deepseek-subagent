> Historical recovery-branch document retained during main unification (2026-09-07). The current implementation is Antigravity-only. References below to OpenCode, fixed legacy execution deadlines, old model names, and branch-specific operating instructions describe the August baseline, not the current runtime contract.

# Design Specification: MCP Transport Recovery, Timeout Semantics, and Daemon Lifecycle Automation

- **Spec Path**: `docs/superpowers/specs/2026-08-20-mcp-transport-recovery-automation-design.md`
- **Date**: 2026-08-20
- **Branch**: `codex/mcp-transport-recovery`
- **Baseline Commit**: `19efda225d55f5257572332be525287870d65f89`
- **Status**: Approved Design Specification

---

## 1. Executive Summary & Problem Context

The DeepSeek Sub-Agent bridge coordinates local AI sub-agents across two primary execution providers:
1. **OpenCode Go**: Managed or attached loopback HTTP server (`opencode-go` provider, `deepseek-v4-flash` / `pro` models).
2. **Antigravity CLI**: Local `agy` binary execution in the agent workspace (`antigravity` provider, `gemini-3.7-flash-high` model).

The runtime architecture separates the host-facing Model Context Protocol (MCP) stdio server from the long-lived bridge daemon. The MCP process runs inside the host (e.g., Codex or Claude Desktop), exposing seven tools (`deepseek_spawn`, `deepseek_continue`, `deepseek_consult`, `deepseek_follow`, `deepseek_abort`, `deepseek_close`, `deepseek_recover_result`), while the persistent HTTP daemon (`BridgeHttpServer` / `BridgeService`) manages process execution, state machines, correlation hints, delivery mechanisms, and SQLite durability (`bridge.sqlite`).

Three critical transport, lifecycle, and timeout edge cases currently degrade resilience during disconnected or edge-case execution:

```
+----------------------------------------------------------------------------------------------------+
|                                    Current Failure Modes & Gaps                                    |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|  (A) Lost Response Recovery Gap:                                                                   |
|      Caller submits spawn/continue with unique request_id. MCP HTTP call drops / times out.         |
|      Task is accepted and finishes in daemon, but deepseek_recover_result requires both            |
|      agent_id and job_id. Caller only knows request_id, leaving completed result stranded.         |
|                                                                                                    |
|  (B) Antigravity Timeout Collapsing:                                                               |
|      Antigravity execution times out after 900,000ms (15m). runAgy rejects with                    |
|      AntigravityProcessError(kind="timeout"). runAntigravityAsync catches this and transitions      |
|      job/agent to generic "failed" with misleading "Antigravity rejected the task dispatch" log.  |
|      Status should be first-class "timed_out", retaining continuable state and accurate telemetry.  |
|                                                                                                    |
|  (C) Bootstrapping Race & Stale-Lock Takeover Race:                                                |
|      Lazy bootstrap (ensureDaemonRunning) probed /health. If /health was not ready, it immediately  |
|      spawned a new daemon, even if daemon.pid already contained a live, booting process.            |
|      Furthermore, rename-only stale daemon.pid recovery had a TOCTOU empty-slot window where        |
|      concurrent contenders could produce dual winners/ENOENT/EPERM races. Hardened with bounded     |
|      readiness wait and a serialized sidecar mutex takeover protocol (daemon.pid.lock).             |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+
```

---

## 2. Pillar A: Request-ID Result Recovery Architecture

### 2.1. Selector Invariants and Protocol Schema
`deepseek_recover_result` and the underlying `/v1/jobs/recover` HTTP endpoint must support recovery using **exactly one** selector path:
- **Selector Path 1 (Request ID)**: `request_id` (string, min 1) alone.
- **Selector Path 2 (Legacy Pair)**: `agent_id` (string, min 1) AND `job_id` (string, min 1).

Any ambiguous or mixed input (e.g., providing both `request_id` and `job_id`, or providing only `agent_id` without `job_id`) is strictly rejected as a typed `invalid_request` error (HTTP 400).

```
+----------------------------------------------------------------------------------------------------+
|                                   Pillar A: Recovery Selector Routing                              |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|                       POST /v1/jobs/recover or deepseek_recover_result                             |
|                                            |                                                       |
|                                   Inspect Input Fields                                             |
|                                            |                                                       |
|               +----------------------------+---------------------------+                           |
|               |                                                        |                           |
|      [request_id provided]                                  [job_id provided]                      |
|               |                                                        |                           |
|      Has agent_id or job_id?                                      Has agent_id?                    |
|         /           \                                               /        \                     |
|       YES            NO                                           YES         NO                   |
|        |              |                                            |           |                   |
|    REJECT 400    Look up job by                               Look up job by  REJECT 400           |
| (invalid_request) request_id in SQLite                         job_id in DB  (invalid_request)     |
|                       |                                            |                               |
|                  Found job?                                Verify job.agent_id                     |
|                   /       \                                  == agent_id                           |
|                 YES        NO                                      |                               |
|                  |          \                             +--------+--------+                      |
|                  |      REJECT 404                        |                 |                      |
|                  |      (unknown_job)                   MATCH            MISMATCH                  |
|                  |                                        |                 |                      |
|                  |                                        |             REJECT 400                 |
|                  |                                        |         (job_agent_mismatch)           |
|                  +--------------------+-------------------+                                        |
|                                       |                                                            |
|                           Check job.result_path                                                    |
|                            /                 \                                                     |
|                         EXISTS             NULL                                                    |
|                            |                 |                                                     |
|                   Read result file     Reconcile in-flight                                         |
|                   Sanitize envelope    job if active;                                              |
|                   Consume obligation   else REJECT 404                                             |
|                   Return Result        (No persisted result)                                       |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+
```

### 2.2. Tool & API Specifications

#### MCP Tool Definition: `deepseek_recover_result`
- **Tool Name**: `deepseek_recover_result`
- **Title**: `DeepSeek Sub-Agent · Recover result`
- **Description**: `"Recover a persisted asynchronous result by request_id (recommended after connection loss) or by legacy agent_id and job_id. A successful recover returns the usable final result and explicitly consumes the job obligation (persisted), separate from closing the agent. Do not use this as a status poll and never call it repeatedly to check progress."`
- **Annotations**: `{ readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }`
- **Input Schema (Zod)**:
```typescript
{
  request_id: z.string().min(1).optional(),
  agent_id: z.string().min(1).optional(),
  job_id: z.string().min(1).optional(),
}
```
- **Output Schema**:
```typescript
{
  content: [{ type: "text", text: string }],
  structuredContent: {
    result: ResultEnvelope
  }
}
```

#### HTTP Endpoint: `POST /v1/jobs/recover`
- **Authorization**: `Bearer <daemonToken>`
- **Request Body**:
```json
{
  "request_id": "req-01912345-6789-7abc-def0-123456789abc"
}
```
*or (legacy):*
```json
{
  "agent_id": "agent-01912345-6789-7abc-def0-123456789abc",
  "job_id": "job-01912345-6789-7abc-def0-123456789abc"
}
```
- **Response Codes**:
  - `200 OK`: Result recovered successfully. Body: `ResultEnvelope`.
  - `400 Bad Request` (`code: "invalid_request"`): Ambiguous selector (both styles supplied), incomplete legacy selector (missing `agent_id` or `job_id`), or empty selector.
  - `400 Bad Request` (`code: "job_agent_mismatch"`): `job_id` exists but belongs to a different `agent_id`.
  - `404 Not Found` (`code: "unknown_job"`): `request_id` or `job_id` does not match any record in `jobs`.
  - `404 Not Found` (`code: "not_found"`): Job found, but no result envelope is persisted (`result_path` is null and job is not reconcilable to a final result).

### 2.3. BridgeService Resolution Logic
In `src/service.ts`, `recoverResult` is updated to accept either a selector object or legacy arguments:
```typescript
export interface RecoverResultInput {
  requestId?: string;
  jobId?: string;
  agentId?: string;
}

async recoverResult(input: RecoverResultInput | string, legacyAgentId?: string): Promise<ResultEnvelope> {
  let requestId: string | undefined;
  let jobId: string | undefined;
  let agentId: string | undefined;

  if (typeof input === "string") {
    jobId = input;
    agentId = legacyAgentId;
  } else {
    requestId = input.requestId;
    jobId = input.jobId;
    agentId = input.agentId;
  }

  const hasRequestId = typeof requestId === "string" && requestId.trim().length > 0;
  const hasJobId = typeof jobId === "string" && jobId.trim().length > 0;
  const hasAgentId = typeof agentId === "string" && agentId.trim().length > 0;

  if (hasRequestId && (hasJobId || hasAgentId)) {
    throw new InvalidRequestError("Provide either request_id alone OR both agent_id and job_id, not both selector styles.");
  }
  if (!hasRequestId && (!hasJobId || !hasAgentId)) {
    throw new InvalidRequestError("Provide either request_id alone OR both agent_id and job_id.");
  }

  let job: JobRecord | null = null;
  if (hasRequestId) {
    job = this.store.getJobByRequestId(requestId!.trim());
    if (!job) throw new UnknownJobError(requestId!.trim());
  } else {
    job = this.store.getJob(jobId!.trim());
    if (!job) throw new UnknownJobError(jobId!.trim());
    if (job.agentId !== agentId!.trim()) {
      throw new InvalidRequestError("Job does not belong to the requested agent", "job_agent_mismatch");
    }
  }

  // Active job reconciliation fallback
  if (!job.resultPath && this.client && ["dispatching", "running", "completed", "delivery_pending"].includes(job.status)) {
    await this.reconcileJob(job);
    job = this.store.getJob(job.id);
  }
  if (!job?.resultPath && job?.status === "timed_out" && this.client) {
    const agent = this.store.getAgent(job.agentId);
    if (agent) await this.captureTimedOutEvidence(agent, job);
    job = this.store.getJob(job.id);
  }
  if (!job?.resultPath) {
    throw new NotFoundError("No persisted result is available for job " + job?.id);
  }

  const result = sanitizePersistedResult(JSON.parse(await readFile(job.resultPath, "utf8")), this.config.maxResultLength);
  this.store.consumeResult(job.id);
  return result;
}
```

### 2.4. Database Invariants
- `jobs.request_id` is already defined as `TEXT NOT NULL UNIQUE` in schema migration version 1 (`src/store.ts:76`).
- `BridgeStore.getJobByRequestId(requestId)` already exists and queries `SELECT * FROM jobs WHERE request_id = ?` (`src/store.ts:314-317`).
- Zero database schema migrations or index changes are needed.

---

## 3. Pillar B: Antigravity Timeout Semantics & Breaker Evaluation

### 3.1. Antigravity Timeout Mapping Flow

In `src/antigravity/runner.ts`, `runAgy` enforces `AGY_DEFAULT_TIMEOUT_MS = 900_000` (15 minutes). When the timeout timer triggers:
1. `killTree(child)` terminates the process subtree on Windows (`taskkill /T /F`).
2. The promise rejects with `new AntigravityProcessError("timeout", command, command + " did not finish within " + timeoutMs + "ms; the process tree was terminated")`.

In `src/service.ts`, `runAntigravityAsync` must intercept errors with `error instanceof AntigravityProcessError && error.kind === "timeout"` and apply precise `timed_out` state transitions:

```
+----------------------------------------------------------------------------------------------------+
|                                Pillar B: Antigravity Timeout Transitions                           |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|                              runAgy (timeout after 900,000ms)                                      |
|                                            |                                                       |
|                     rejects with AntigravityProcessError(kind="timeout")                           |
|                                            |                                                       |
|                                 runAntigravityAsync catch                                          |
|                                            |                                                       |
|                                 Is error.kind === "timeout"?                                       |
|                                   /                  \                                             |
|                                 YES                   NO                                           |
|                                  |                     |                                           |
|                      +-----------+-----------+    Generic Error / Abort Handling                   |
|                      |                       |    (status="failed" / "aborted")                    |
|               Job State Update        Agent State Update                                           |
|             status = "timed_out"     status = "timed_out"                                          |
|             error = timeoutMsg       lastError = timeoutMsg                                        |
|                      |                       |                                                     |
|                      +-----------+-----------+                                                     |
|                                  |                                                                 |
|                            Record Activity                                                         |
|                     activity_type = "deadline"                                                     |
|                     summary = "Antigravity process timed out after 900000ms;                        |
|                                the process tree was terminated"                                    |
|                                  |                                                                 |
|                            Resolve Follow                                                          |
|                     status = "timed_out"                                                           |
|                     deadlineReached = true                                                         |
|                     workerAborted = true                                                           |
|                     resultAvailable = false                                                        |
|                     error = timeoutMsg                                                             |
|                                  |                                                                 |
|                     Continuable: Agent remains open;                                               |
|                     orchestrator can call deepseek_continue                                        |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+
```

### 3.2. Detailed State Machine Invariants
1. **Job Transition**: `running` -> `timed_out` (explicitly permitted in `assertJobTransition` in `src/state.ts:13`).
2. **Agent Transition**: `working` -> `timed_out` (explicitly permitted in `assertAgentTransition` in `src/state.ts:51`).
3. **Continuability**: A `timed_out` agent is continuable via `deepseek_continue` without needing `allow_respawn` (per `docs/architecture.md:34`).
4. **Follow Waiter Settlement**: Any pending `deepseek_follow` promise receives `{ status: "timed_out", deadlineReached: true, workerAborted: true, resultAvailable: false, error: message }` and closes its HTTP request gracefully.
5. **Strict Prohibitions**:
   - Never increase the 900,000ms timeout.
   - Never restart the daemon process.
   - Never automatically rerun the prompt.
   - Never fall back to OpenCode or another model route.

### 3.3. Circuit Breaker Architectural Evaluation
The prompt requires: *"Include a minimal in-memory per-route breaker only if current architecture supports deterministic 3-consecutive-timeout, 5-minute cooldown, single half-open probe semantics without persistence/config migration; otherwise exclude it explicitly with rationale and keep typed timeout only."*

#### Architectural Evaluation:
1. **Determinism vs. Operator Route Control Model**:
   - In this bridge architecture, model routes are strictly operator-governed via the loopback control plane (`route set <name>`, see `docs/architecture.md:48`).
   - Automatic silent lockout of a model route (e.g. `antigravity-flash-high`) after 3 consecutive 15-minute timeouts (which represent 45 minutes of real-world worker execution) without human operator intervention conflicts with the explicit ADR-0001 principle: "No model/provider fallback is allowed; route selection is operator-controlled."
2. **State Volatility**:
   - An in-memory breaker resets whenever the daemon restarts, creating non-deterministic tripping behavior across daemon restarts. Persisting breaker state would require database schema changes and configuration migrations, which are strictly prohibited by the binding scope.
3. **Independent Agent Fronts**:
   - Long-running Antigravity tasks may legitimately run for 15 minutes due to complex user code tasks rather than infrastructure failure. Tripping the entire route would block all other unrelated agent spawns on that route.

#### Decision & Specification:
- **Core Scope**: Retain the robust, first-class **typed timeout mapping** as the binding implementation.
- **Circuit Breaker Specification (Deterministic In-Memory Model)**:
  If enabled in future operator policies, the minimal deterministic in-memory circuit breaker specification is defined as follows:
  - **Data Structure**: `Map<string, RouteBreakerState>` where `RouteBreakerState = { consecutiveTimeouts: number, state: "closed" | "open" | "half_open", openedAt: number | null, probeInFlight: boolean }`.
  - **Trip Condition**: Exactly 3 consecutive `timed_out` results on the same route transitions `state` from `"closed"` to `"open"`, recording `openedAt = Date.now()`.
  - **Cooldown**: 5 minutes (`300_000ms`). When a new spawn arrives after 5 minutes, `state` becomes `"half_open"`.
  - **Half-Open Probe**: Exactly one probe job is admitted (`probeInFlight = true`). Concurrent spawns during `"half_open"` are rejected with typed `route_circuit_open` (HTTP 503).
  - **Recovery / Trip Back**: If probe finishes as `"completed"` or `"completed_partial"`, `consecutiveTimeouts = 0`, `state = "closed"`, `probeInFlight = false`. If probe times out, `state = "open"`, `openedAt = Date.now()`, `probeInFlight = false`.

---

## 4. Pillar C: Bounded Live-Daemon Readiness Bootstrapping & Serialized Stale-Lock Takeover

### 4.1. The Root Cause in `ensureDaemonRunning`
In `src/mcp.ts:130-157`:
```typescript
export async function ensureDaemonRunning(
  config: BridgeConfig,
  client: DaemonHealthClient,
  options: DaemonBootstrapOptions = {},
): Promise<void> {
  try {
    await client.health();
    return;
  } catch (error) {
    var lastError: unknown = error;
  }

  await (options.start ?? startDetachedDaemon)(config);
  // ... loop waiting for health ...
}
```
When an MCP client issues its first tool call while a daemon is booting:
1. `client.health()` fails because the HTTP server has not called `.listen()` yet.
2. `ensureDaemonRunning` immediately executes `startDetachedDaemon(config)`.
3. The newly spawned child process executes `cli.ts daemon` -> `acquireDaemonLock(config.dataDir)`.
4. `acquireDaemonLock` detects that `daemon.pid` contains a live PID and throws:
   `"DeepSeek Sub-Agent daemon is already running (PID X). Duplicate daemon instance prevented."`
5. The duplicate process exits with code 1, polluting `daemon.log` and wasting OS resources.

### 4.2. Proved Root Cause: Stale-Lock Takeover TOCTOU Race in `acquireDaemonLock`
Prior to this fix, `acquireDaemonLock` attempted stale-lock reclamation by renaming `daemon.pid` to a unique temporary filename (`daemon.pid.stale.<pid>.<timestamp>.<nonce>`). This rename-based takeover suffered from an inherently racy TOCTOU window:
1. **Empty-Slot Window**: As soon as contender A renames `daemon.pid`, the file path `daemon.pid` ceases to exist on the filesystem.
2. **Concurrent Initial Writer Collision**: Contender B (or another newly starting daemon) attempting initial acquisition via `writePrivateFileExclusive(pidPath, ...)` sees `daemon.pid` is absent and successfully claims it.
3. **Dual Winners / Platform Locking Races**: Contender A inspects the renamed stale content, confirms the old PID was dead, and attempts `writePrivateFileExclusive(pidPath, ...)` or rename back. On Windows or under high concurrency, this created dual winners (`fulfilled.length > 1`), unhandled `ENOENT` read errors, or `EPERM` file-locking collisions.
4. **Deterministic RED Reproduction**: During stress testing with 50 concurrent stale-lock takeover iterations, this race reproduced deterministically on **run 2/50** with 2 simultaneous winners acquiring the daemon lock.

### 4.3. Hardened Readiness & Takeover Flow

```
+----------------------------------------------------------------------------------------------------+
|                         Pillar C: Hardened Daemon Bootstrapping & Serialized Takeover              |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|                                       ensureDaemonRunning                                          |
|                                                |                                                   |
|                                        1. Probe /health                                            |
|                                          /          \                                              |
|                                     HEALTHY       UNHEALTHY / OFFLINE                              |
|                                        |                 |                                         |
|                                     RETURN          2. Inspect daemon.pid                          |
|                                    SUCCESS               in dataDir                                |
|                                                          |                                         |
|                       +----------------------------------+----------------------------------+      |
|                       |                                  |                                  |      |
|                    ENOENT                          File Exists                         Corrupt     |
|                 (No Lock File)                     Valid PID                         Invalid PID   |
|                       |                                  |                                  |      |
|              Spawn Detached Daemon             Check isProcessAlive(pid)                FAIL CLOSED|
|              (startDetachedDaemon)               /               \                      Throw Error|
|                       |                        ALIVE             DEAD                       |      |
|                       |                          |                 |                        |      |
|                       |                    DO NOT SPAWN!   Spawn Detached Daemon            |      |
|                       |                  (Avoid duplicate) (Serialized Takeover in CLI)     |      |
|                       |                          |                 |                        |      |
|                       +--------------------------+-----------------+                        |      |
|                                                  |                                          |      |
|                                   3. Bounded Health Polling Loop                            |      |
|                                   (timeout: 10s-45s, interval: 100ms)                       |      |
|                                          /                \                                 |      |
|                                       HEALTHY           TIMEOUT                             |      |
|                                          |                 |                                |      |
|                                       RETURN          THROW ERROR                           |      |
|                                      SUCCESS    (Daemon did not become ready)               |      |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+
```

### 4.4. Serialized Sidecar Takeover Protocol Specification (`daemon.pid.lock`)

To eliminate the empty-slot TOCTOU race, `acquireDaemonLock` in `src/cli.ts` implements an exclusive serialized sidecar mutex protocol using `daemon.pid.lock`:

```typescript
// Exact sidecar lock path
const takeoverLockPath = path.join(dataDir, "daemon.pid.lock");
```

1. **Initial Exclusive Lock**: The process attempts `writePrivateFileExclusive(pidPath, String(pid) + "\n")` (`mode: 0o600`, flag `'wx'`). If successful, it immediately returns the lock releaser.
2. **Lock Inspection**: If initial creation fails, `acquireDaemonLock` reads `daemon.pid`:
   - If `ENOENT` (file was just released by another process), it immediately retries.
   - If content is empty, non-numeric, or not a positive safe integer (`!/^[1-9]\d*$/.test(trimmed)` or `!Number.isSafeInteger(existingPid)`), it fails closed with `"Failed to acquire exclusive daemon lock: lock file is held by another process."`.
   - If `isProcessAlive(existingPid)` is `true`, it fails closed with `"DeepSeek Sub-Agent daemon is already running (PID ${existingPid}). Duplicate daemon instance prevented."`.
3. **Exclusive Sidecar Acquisition**: If `existingPid` is dead, the contender attempts exclusive creation of the sidecar lock file:
   ```typescript
   const claimedTakeover = await writePrivateFileExclusive(takeoverLockPath, String(pid) + "\n");
   ```
4. **Mutex-Held Revalidation & Takeover**:
   - If `claimedTakeover` is `true`:
     - Inside a `try ... finally` block, the contender re-reads `daemon.pid` under the mutex to verify whether another process became alive.
     - If `freshPid` is alive (`isProcessAlive(freshPid)`), it aborts takeover and throws duplicate daemon prevention (`"DeepSeek Sub-Agent daemon is already running (PID ${freshPid}). Duplicate daemon instance prevented."`).
     - If still stale or absent, it writes its PID directly to `daemon.pid` via `writePrivateFile(pidPath, String(pid) + "\n")`.
     - Returns `createDaemonLockReleaser(pidPath, pid)`.
     - In the `finally` block, it unlinks only its own sidecar lock:
       ```typescript
       await unlink(takeoverLockPath).catch(() => undefined);
       ```
5. **Fail-Closed Sidecar Invariants & Non-Owner Rules**:
   - **Creator-Only Cleanup Invariant**: Only the contender that atomically created `daemon.pid.lock` may remove it, in its own `finally` block; non-owners **never** unlink an existing sidecar lock file.
   - **Rationale (Absence of Atomic Compare-and-Delete)**: In the standard Node.js filesystem API, there is no portable atomic compare-and-delete operation. Both read-then-unlink and rename-after-stale-read can target a replacement live sidecar created by another process immediately after the check, recreating the exact same TOCTOU race.
   - **Live Sidecar Holder Contention**: If another contender holds `takeoverLockPath` and `isProcessAlive(takeoverPid)` is `true`, the current contender performs a bounded wait (sleeps `10ms` and loops up to `maxAttempts = 5`, bounding total delay to ~50ms) to allow the active winner to finish writing its PID. If attempts are exhausted without acquisition, it fails closed with `"Failed to acquire exclusive daemon lock: lock file is held by another process."` (no retry storm, no ownership).
   - **Dead / Orphan, Empty, or Corrupt Sidecar (Fail-Closed Actionable Errors)**: If `takeoverLockPath` contains a dead PID, non-numeric or empty content, an invalid safe integer, or is unreadable (non-ENOENT), `acquireDaemonLock` immediately fails closed with an actionable error requiring manual operator intervention. Both `daemon.pid` and `takeoverLockPath` remain completely untouched on disk, producing **zero winners**:
     - *Dead / Stale Takeover PID*: `throw new Error(`Failed to acquire exclusive daemon lock: orphan or stale takeover sidecar lock encountered (PID ${takeoverPid}). Manual operator cleanup required.`);`
     - *Empty / Corrupt / Invalid PID*: `throw new Error("Failed to acquire exclusive daemon lock: orphan or corrupt takeover sidecar lock encountered. Manual operator cleanup required.");`
     - *Unreadable Sidecar Lock*: `throw new Error("Failed to acquire exclusive daemon lock: takeover sidecar lock is unreadable.");`
6. **Normal Path Automatic Behavior**:
   - The creator cleans its own short-lived sidecar in `finally`.
   - Stale `daemon.pid` with no sidecar is serialized and yields exactly one winner.
7. **Deterministic Evidence & Stress Validation**:
   - Deterministic RED reproduction for orphan and corrupt sidecar behavior.
   - CLI unit tests: 28 pass (`test/unit/cli.test.ts`).
   - Full regression suite observed: 287 pass, 3 skipped, 0 fail (290 total).
   - Repeated 5x reviewer stress runs and prior 50/50 race stress runs recorded as distinct observed evidence of single-winner correctness.

### 4.5. Lock Inspection & Bootstrapping Invariants
1. **MCP Rule (`ensureDaemonRunning`)**:
   - `ensureDaemonRunning` in `src/mcp.ts` **never** unlinks, renames, or deletes `daemon.pid`, and never kills processes.
   - For a live booting process (`isProcessAlive(pid) === true`), `ensureDaemonRunning` does not spawn a new process and proceeds directly to the bounded health polling loop.
   - For a dead PID (`isProcessAlive(pid) === false`), `ensureDaemonRunning` invokes `startDetachedDaemon` once, delegating all serialized takeover logic to `acquireDaemonLock` in `src/cli.ts`.
2. **Corrupt / Non-Numeric File Content & Read Errors**:
   - If `daemon.pid` contains empty, whitespace, negative, floating-point, zero, trailing junk, or non-safe-integer content, bootstrap fails closed immediately without launching a process and without modifying the file.
   - Non-ENOENT read errors (such as `EISDIR` or `EPERM`) fail closed immediately without spawning.
3. **Safety Guarantees**:
   - No `taskkill` or SIGKILL against live running daemons.
   - No modification to user configuration files.
   - No expansion of logging, telemetry, or external hooks.

### 4.6. Scope Expansion Rationale
This scope expansion (modifying `acquireDaemonLock` in `src/cli.ts` to implement the serialized sidecar takeover protocol) was accepted strictly because the stale-lock TOCTOU race was conclusively reproduced and root-caused during independent review of Task 3. No broader restart, retry, or process management mechanism was added.

---

## 5. State Transition & Error Code Matrix

| Operation / Event | Input Condition | Previous State | Target State | Error Code | HTTP Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `deepseek_recover_result` | `request_id` valid, result exists | Terminal | Terminal (Consumed) | None | 200 |
| `deepseek_recover_result` | `agent_id` + `job_id` valid, result exists | Terminal | Terminal (Consumed) | None | 200 |
| `deepseek_recover_result` | Both `request_id` AND `job_id` passed | Any | Unchanged | `invalid_request` | 400 |
| `deepseek_recover_result` | Only `agent_id` passed without `job_id` | Any | Unchanged | `invalid_request` | 400 |
| `deepseek_recover_result` | `request_id` not in DB | Any | Unchanged | `unknown_job` | 404 |
| `deepseek_recover_result` | `agent_id` mismatch with `job.agent_id` | Any | Unchanged | `job_agent_mismatch` | 400 |
| `deepseek_recover_result` | Job found, `result_path` is null (active) | Active | Active | `not_found` | 404 |
| Antigravity Run | `runAgy` timeout at 900,000ms | `running` | `timed_out` (Job) / `timed_out` (Agent) | None (Handled) | N/A (Event) |
| Antigravity Run | Aborted by caller via signal | `running` | `aborted` (Job) / `closed` (Agent) | None (Handled) | N/A (Event) |
| Antigravity Run | `runAgy` non-zero exit code | `running` | `failed` (Job) / `failed` (Agent) | None (Handled) | N/A (Event) |
| `ensureDaemonRunning` | Live booting PID in `daemon.pid` | Offline / Booting | Ready (after wait) | None | N/A |
| `ensureDaemonRunning` | Stale dead PID in `daemon.pid` | Stale Lock | Ready (after launch + sidecar takeover) | None | N/A |
| `ensureDaemonRunning` | Corrupt `daemon.pid` | Corrupt Lock | Offline | `corrupt_lock` | N/A |
| `acquireDaemonLock` | Concurrent stale PID takeover (no sidecar) | Contended Stale Lock | Locked (exactly 1 winner via sidecar) | None | N/A |
| `acquireDaemonLock` | Live sidecar contention | Contended Sidecar | Offline (fails closed after 5 attempts, 0 winners, files untouched) | `lock_held` ("lock file is held by another process") | N/A |
| `acquireDaemonLock` | Orphan / dead PID in sidecar (`daemon.pid.lock`) | Stale Sidecar | Offline (immediate fail closed, 0 winners, files untouched) | `orphan_sidecar` ("orphan or stale takeover sidecar lock encountered (PID X)...") | N/A |
| `acquireDaemonLock` | Corrupt / empty / invalid sidecar | Corrupt Sidecar | Offline (immediate fail closed, 0 winners, files untouched) | `corrupt_sidecar` ("orphan or corrupt takeover sidecar lock encountered...") | N/A |

---

## 6. Acceptance Criteria

1. **Pillar A Acceptance**:
   - `deepseek_recover_result` successfully retrieves and consumes results using `{ request_id }`.
   - `deepseek_recover_result` preserves backward compatibility for `{ agent_id, job_id }`.
   - Ambiguous or incomplete selectors fail with HTTP 400 `invalid_request`.
   - Mismatched `agent_id` with `job_id` fails with HTTP 400 `job_agent_mismatch`.
   - Non-existent `request_id` fails with HTTP 404 `unknown_job`.
2. **Pillar B Acceptance**:
   - When `runAgy` throws `AntigravityProcessError("timeout")`, job status is set to `timed_out` and agent status is set to `timed_out`.
   - Agent activity log records `activity_type: "deadline"` with summary indicating 900,000ms timeout and tree termination.
   - Follow waiter resolves with `status: "timed_out"` and `deadlineReached: true`.
   - Timed-out agent remains open and continuable via `deepseek_continue`.
3. **Pillar C Acceptance**:
   - If `daemon.pid` contains a live PID while `/health` is unreachable, `ensureDaemonRunning` waits up to `timeoutMs` without calling `startDetachedDaemon`.
   - If `daemon.pid` is absent or contains a dead PID, `ensureDaemonRunning` calls `startDetachedDaemon` once without unlinking `daemon.pid`.
   - `acquireDaemonLock` implements serialized sidecar takeover via `daemon.pid.lock`. Only the creating process unlinks its sidecar in `finally`; non-owners never unlink an existing sidecar lock.
   - For an existing live sidecar, contenders perform bounded contention wait (`maxAttempts = 5`, `10ms` interval) without retry storms, failing closed with zero ownership.
   - For dead/orphan, empty, or corrupt sidecars, `acquireDaemonLock` fails closed immediately with actionable operator cleanup errors, leaving `daemon.pid` and `daemon.pid.lock` untouched with zero winners (deterministic RED verified).
   - Clean single-winner serialized takeover for stale `daemon.pid` without a sidecar (verified with 50/50 race stress and repeated 5x reviewer stress runs).
   - Strict PID validation and non-ENOENT read errors fail closed without spawning or unlinking files.
   - Full regression test suite passes cleanly (exit 0, zero failures; observed baseline: 287 passed, 3 skipped, 0 failed; CLI unit tests: 28 passed).

---

## 7. Explicit Exclusions & Non-Goals

1. **No Stdio Supervisor**: No background watchdog process or separate stdio supervisor will be introduced; recovery leverages SQLite idempotency and request-ID lookups.
2. **No Database Migrations**: No schema alterations, tables, or index creations.
3. **No Timeout Expansion**: The 900,000ms (15m) timeout in `AGY_DEFAULT_TIMEOUT_MS` must not be increased.
4. **No Auto-Rerun or Daemon Restarts on Timeout**: Timeouts fail closed into `timed_out` status without restarting the bridge daemon or re-running prompts.
5. **No Broader Restart/Retry Framework**: No automated restart loops, daemon watchdog re-spawns, or unapproved retry wrappers were added beyond the bounded 5-attempt sidecar mutex lock.
6. **No Telemetry / Logging Expansion**: No new log exporters, hooks, or verbose loggers.
7. **No Live Daemon Process Operations**: No `taskkill` or SIGKILL against live running daemons.
8. **No Automatic Orphan Sidecar Deletion**: No automatic unlinking, reclaiming, or deletion of dead, orphan, or corrupt `daemon.pid.lock` sidecar files by non-owning contenders. Manual cleanup requires external operator verification of process ownership and liveness and is NOT performed automatically by this delivery.
