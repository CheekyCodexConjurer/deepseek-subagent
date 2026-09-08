> Historical recovery-branch document retained during main unification (2026-09-07). The current implementation is Antigravity-only. References below to OpenCode, fixed legacy execution deadlines, old model names, and branch-specific operating instructions describe the August baseline, not the current runtime contract.

# MCP Transport Recovery Automation Implementation Plan

> This implementation plan is an executable, end-to-end engineering guide designed for an autonomous agentic worker. Follow strict TDD: every task begins with an observable failing RED test command before any production edits, proceeds to focused GREEN implementation, followed by review of the uncommitted scoped diff, and only then local scoped commit. Do not stage or commit outside task boundaries, and do not push, merge, or restart long-running processes.

---

## Goal

Harden the DeepSeek Sub-Agent MCP bridge against transport disconnections, execution timeouts, and daemon bootstrapping races across three concrete pillars:
1. **Pillar A (`request_id` Result Recovery)**: Allow callers to recover persisted results using only `request_id` (in addition to legacy `agent_id`+`job_id`), consuming job obligations idempotently without stranded results.
2. **Pillar B (Antigravity Timeout Semantics)**: Map `runAgy` 900,000ms process timeouts to first-class `timed_out` job/agent statuses and `"deadline"` activity logging, preserving continuable agent state without misleading dispatch failure errors.
3. **Pillar C (Bounded Daemon Bootstrapping & Serialized Stale-Lock Takeover)**: Inspect `daemon.pid` in `ensureDaemonRunning` to wait on live booting daemons rather than spawning crashing duplicates, while delegating stale lock cleanup to serialized sidecar takeover (`daemon.pid.lock`) in `acquireDaemonLock`, failing closed on orphan/corrupt sidecars requiring operator cleanup.

---

## Architecture

- **MCP Stdio Layer (`src/mcp.ts`)**: Exposes MCP tools (`deepseek_recover_result`, etc.), validates tool input schemas via Zod, and lazily bootstraps daemon readiness on the first tool operation. Inspects `daemon.pid` using shared low-level liveness helpers before attempting spawn.
- **HTTP Transport Layer (`src/http-server.ts`)**: Dispatches loopback REST endpoints (`POST /v1/jobs/recover`), normalizes both camelCase and snake_case request parameters via `optionalString`, and returns typed JSON HTTP error envelopes.
- **Service Domain Core (`src/service.ts`)**: Orchestrates execution state machines, delegates result recovery by either `requestId` or legacy `jobId`+`agentId` selectors, settles follow waiter lifecycles via `this.resolveFollow`, and handles asynchronous provider errors.
- **Shared Low-Level Utilities (`src/security.ts`)**: Houses shared path, process, and file security helpers including `isProcessAlive`, avoiding circular dependencies between the CLI and MCP layers.
- **CLI & Daemon Lock Layer (`src/cli.ts`)**: Enforces exclusive lock acquisition and serialized sidecar takeover (`daemon.pid.lock`) in `acquireDaemonLock`.
- **Durability Layer (`src/store.ts`)**: Persists agents, jobs, and obligations in SQLite (`bridge.sqlite`) with existing `jobs.request_id` unique indexing.

---

## Tech Stack

- **Runtime**: Node.js >= 20.0.0 (Native test runner via `node --import tsx --test`, ESM modules)
- **Language**: TypeScript 5.x (`tsc -p tsconfig.json --noEmit` for linting, `tsc -p tsconfig.json` for compilation)
- **Protocol**: Model Context Protocol (MCP SDK `@modelcontextprotocol/sdk`)
- **Validation**: Zod 3.x
- **Storage**: SQLite 3 via `better-sqlite3`

---

## Spec

- **Design Specification**: `docs/superpowers/specs/2026-08-20-mcp-transport-recovery-automation-design.md`
- **Branch**: `codex/mcp-transport-recovery`
- **Baseline Commit**: `19efda225d55f5257572332be525287870d65f89`

---

## Global Constraints

1. **Smallest Safe Diff**: Modify only directly affected interfaces and implementation paths. Do not refactor unrelated modules or reformat untouched files.
2. **Review-Before-Commit**: Do NOT stage or commit changes before verification. Do NOT touch production (`src/`) or test (`test/`) code during the planning phase.
3. **Strict TDD Methodology**: Every task begins with an executable RED test command demonstrating the expected failure, followed by implementation, and verified with an executable GREEN test command.
4. **No Database Migrations**: Use the existing `jobs.request_id` UNIQUE column and `BridgeStore.getJobByRequestId` method.
5. **No Circular Dependencies**: Do not import `isProcessAlive` from `src/cli.ts` into `src/mcp.ts` (since `src/cli.ts` imports `src/mcp.ts`). Extract shared process liveness helpers to `src/security.ts`.
6. **No Direct Stale-Lock Deletion in Bootstrap & Fail-Closed Sidecar Takeover**: `ensureDaemonRunning` in `src/mcp.ts` must never delete a stale `daemon.pid` file directly. Dead PID detection simply proceeds to `startDetachedDaemon`, allowing `acquireDaemonLock` in `src/cli.ts` to perform serialized sidecar takeover via `daemon.pid.lock`. Non-owners never unlink an existing `daemon.pid.lock` sidecar; dead, orphan, or corrupt sidecar locks fail closed immediately with actionable operator cleanup errors, leaving files untouched with zero winners.
7. **Circuit Breaker Exclusion**: Circuit breaker is explicitly excluded from binding implementation; evidence threshold remains unmet (operator-controlled routing principles per ADR-0001, lack of durable persistence without migrations, task duration workload-dependence).
8. **Preserve Invariants**:
   - `AGY_DEFAULT_TIMEOUT_MS` remains locked at 900,000ms.
   - Never auto-rerun timed-out tasks or restart the daemon on timeout.
   - Never kill or taskkill live running daemons.
   - No automatic deletion of orphan/corrupt sidecars; manual cleanup requires external verification.
   - No expansion of logging, collectors, or telemetry.
   - Subagents must not be dispatched.

---

## File Map & Interface Changes

```
+----------------------------------------------------------------------------------------------------+
|                                      Affected Files & Interfaces                                   |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|  1. src/types.ts                                                                                   |
|     - Add RecoverResultInput interface: { requestId?: string; jobId?: string; agentId?: string }   |
|                                                                                                    |
|  2. src/service.ts                                                                                 |
|     - Import AntigravityProcessError, AGY_DEFAULT_TIMEOUT_MS from ./antigravity/runner.js          |
|     - BridgeService.recoverResult: support RecoverResultInput | string with legacy argument order  |
|       (jobId, agentId) and single-selector exclusivity validation                                 |
|     - BridgeService.runAntigravityAsync: catch AntigravityProcessError(kind="timeout"), transition |
|       job/agent to timed_out, log "deadline" activity, and settle via this.resolveFollow          |
|                                                                                                    |
|  3. src/http-server.ts                                                                             |
|     - POST /v1/jobs/recover: parse and normalize requestId / request_id, jobId / job_id,           |
|       agentId / agent_id via optionalString, forwarding RecoverResultInput to service.recoverResult|
|                                                                                                    |
|  4. src/security.ts                                                                                |
|     - Export isProcessAlive(pid: number): boolean (extracted shared low-level liveness helper)    |
|                                                                                                    |
|  5. src/cli.ts                                                                                     |
|     - Implement serialized sidecar takeover protocol (daemon.pid.lock) in acquireDaemonLock,       |
|       failing closed on orphan/corrupt sidecars without automatic deletion                         |
|     - Import isProcessAlive from ./security.js and re-export for backward compatibility           |
|                                                                                                    |
|  6. src/mcp.ts                                                                                     |
|     - Import readFile from "node:fs/promises" and isProcessAlive from ./security.js                |
|     - deepseek_recover_result: update Zod schema to allow optional request_id, agent_id, job_id    |
|     - ensureDaemonRunning: inspect daemon.pid, fail closed on corrupt content, wait on live PID,   |
|       and delegate dead PID takeover to startDetachedDaemon + acquireDaemonLock                    |
|                                                                                                    |
|  7. test/integration/http-server.test.ts, test/integration/service.test.ts, &                     |
|     test/integration/mcp.test.ts                                                                   |
|     - Integration test coverage for request_id recovery, selector conflicts, and timeout mappings |
|                                                                                                    |
|  8. test/unit/security.test.ts, test/unit/antigravity.test.ts, & test/unit/cli.test.ts             |
|     - Unit test coverage for isProcessAlive, timeout classification, acquireDaemonLock concurrent |
|       takeover with sidecar mutex, 10-contender stress, corrupt/empty lock fail-closed handling,   |
|       orphan/dead sidecar lock fail-closed handling, corrupt sidecar lock fail-closed handling,    |
|       live sidecar contention non-deletion, and normal owner cleanup in finally                    |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+
```

---

## TDD Implementation Tasks

### Task 1: Request-ID Result Recovery in MCP, HTTP Server & Daemon API (Pillar A)

#### User Story
As an orchestrator or MCP caller whose HTTP connection dropped after receiving a spawn acceptance, I want to recover the completed job result using only my original `request_id`, so that I do not leave accepted work stranded or require a non-existent `agent_id`+`job_id` pair.

#### Affected Files
- `src/types.ts`
- `src/service.ts`
- `src/http-server.ts`
- `src/mcp.ts`
- `test/integration/http-server.test.ts`
- `test/integration/service.test.ts`
- `test/integration/mcp.test.ts`

#### Test Setup (RED)
Add the following test cases:
1. In `test/integration/service.test.ts`:
   - `service.recoverResult retrieves result by requestId and consumes obligation`
   - `service.recoverResult rejects ambiguous selector when both requestId and jobId are provided`
   - `service.recoverResult rejects incomplete legacy selector when jobId is provided without agentId`
   - `service.recoverResult preserves legacy (jobId, agentId) argument order and functionality`
2. In `test/integration/http-server.test.ts`:
   - `POST /v1/jobs/recover accepts snake_case request_id and camelCase requestId`
   - `POST /v1/jobs/recover returns 400 invalid_request on ambiguous or incomplete selectors`
   - `POST /v1/jobs/recover returns 400 job_agent_mismatch when agentId does not match job`
   - `POST /v1/jobs/recover returns 404 unknown_job when request_id does not exist`
3. In `test/integration/mcp.test.ts`:
   - `deepseek_recover_result recovers by request_id alone`
   - `deepseek_recover_result recovers by legacy agent_id and job_id`
   - `deepseek_recover_result rejects ambiguous selector (both request_id and job_id)`
   - `deepseek_recover_result rejects incomplete legacy selector (job_id without agent_id)`

#### Concrete RED Verification Commands
```bash
node --import tsx --test "test/integration/http-server.test.ts" --test-name-pattern="POST /v1/jobs/recover"
node --import tsx --test "test/integration/service.test.ts" --test-name-pattern="service.recoverResult retrieves result by requestId"
node --import tsx --test "test/integration/mcp.test.ts" --test-name-pattern="deepseek_recover_result recovers by request_id alone"
```

#### Expected Failing Output
```
✖ POST /v1/jobs/recover accepts snake_case request_id and camelCase requestId
  InvalidRequestError: jobId is required
    at requiredString (src/http-server.ts:366:47)
    at BridgeHttpServer.handle (src/http-server.ts:158:9)
```

#### Step-by-Step Implementation Details
1. **Types (`src/types.ts`)**:
   Define `RecoverResultInput`:
   ```typescript
   export interface RecoverResultInput {
     requestId?: string;
     jobId?: string;
     agentId?: string;
   }
   ```
2. **Service Domain Core (`src/service.ts`)**:
   Update `recoverResult` to accept `RecoverResultInput | string` while preserving the exact existing legacy argument order `(jobId, agentId)`:
   ```typescript
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
3. **HTTP Server (`src/http-server.ts`)**:
   Update `POST /v1/jobs/recover` handler to extract and normalize both camelCase and snake_case fields via `optionalString`:
   ```typescript
   if (method === "POST" && url.pathname === "/v1/jobs/recover") {
     const value = asRecord(body);
     const result = await this.service.recoverResult({
       requestId: optionalString(value.requestId ?? value.request_id),
       jobId: optionalString(value.jobId ?? value.job_id),
       agentId: optionalString(value.agentId ?? value.agent_id),
     });
     writeJson(response, 200, result);
     return;
   }
   ```
4. **MCP Stdio Server (`src/mcp.ts`)**:
   Update `deepseek_recover_result` tool definition:
   ```typescript
   server.registerTool(
     "deepseek_recover_result",
     {
       title: "DeepSeek Sub-Agent · Recover result",
       description: "Recover a persisted asynchronous result by request_id (recommended after connection loss) or by legacy agent_id and job_id. A successful recover returns the usable final result and explicitly consumes the job obligation (persisted), separate from closing the agent. Do not use this as a status poll and never call it repeatedly to check progress.",
       inputSchema: {
         request_id: z.string().min(1).optional(),
         agent_id: z.string().min(1).optional(),
         job_id: z.string().min(1).optional(),
       },
       annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
     },
     async (input) => {
       await ensureReady();
       const result = await client.recoverResult({
         requestId: input.request_id,
         agentId: input.agent_id,
         jobId: input.job_id,
       });
       return {
         content: [{ type: "text", text: formatHumanResult(result) }],
         structuredContent: { result },
       };
     },
   );
   ```

#### Concrete GREEN Verification Commands
```bash
node --import tsx --test "test/integration/http-server.test.ts"
node --import tsx --test "test/integration/service.test.ts"
node --import tsx --test "test/integration/mcp.test.ts"
```

#### Expected Passing Output
```
exit 0, zero failures; record actual count
```

#### Task 1 Execution Sequence
1. Write RED tests in `test/integration/http-server.test.ts`, `test/integration/service.test.ts`, and `test/integration/mcp.test.ts`.
2. Execute RED verification commands to observe explicit expected failures before any production code edits.
3. Implement minimal production edits in `src/types.ts`, `src/service.ts`, `src/http-server.ts`, and `src/mcp.ts`.
4. Execute focused GREEN verification commands; confirm exit 0, zero failures.
5. Perform independent review of the uncommitted scoped diff via `git diff`.
6. Apply corrections and re-verify if any findings arise.
7. Perform local scoped git commit (do not push, do not merge, do not restart daemon/hosts).

---

### Task 2: Antigravity Timeout Classification & Status Mapping (Pillar B)

#### User Story
As an operator running Antigravity sub-agents, when an `agy` execution hits the 900,000ms limit, I want the job and agent status to transition to `timed_out` with accurate `"deadline"` activity logging, so that the agent remains continuable and telemetry correctly reflects a process timeout rather than a dispatch rejection.

#### Affected Files
- `src/service.ts`
- `test/integration/service.test.ts`
- `test/unit/antigravity.test.ts`

#### Imports & Helper Boundaries
In `src/service.ts`:
- Import `AntigravityProcessError` and `AGY_DEFAULT_TIMEOUT_MS` from `./antigravity/runner.js`:
  ```typescript
  import { AntigravityProcessError, AGY_DEFAULT_TIMEOUT_MS } from "./antigravity/runner.js";
  ```
- Settle follow waiters using the exact existing helper `this.resolveFollow(job.id, ...)` against existing `this.followLifecycles` map:
  ```typescript
  if (this.followLifecycles.has(job.id)) {
    await this.resolveFollow(job.id, {
      status: "timed_out",
      deadlineReached: true,
      workerAborted: true,
      resultAvailable: false,
      error: message,
    });
  }
  ```

#### Circuit Breaker Evaluation Rationale
Circuit breaker is explicitly excluded from binding implementation because the evidence threshold remains unmet:
1. **ADR-0001 Alignment**: Model route selection is strictly operator-controlled (`route set <name>`). Automatic tripping violates operator control.
2. **Persistence Constraints**: In-memory breaker state resets across daemon restarts; durable persistence requires schema changes and migrations, which are out of scope.
3. **Task Duration Variance**: Workload duration in user workspaces varies legitimately, meaning timeouts do not necessarily signify infrastructure failure.

#### Test Setup (RED)
Add tests to `test/integration/service.test.ts` and `test/unit/antigravity.test.ts`:
1. `Antigravity execution timeout transitions job and agent to timed_out with deadline activity`
2. `Antigravity execution timeout settles follow waiter as timed_out with deadlineReached true`
3. `Antigravity timed_out agent remains continuable with deepseek_continue`
4. `Antigravity runner AntigravityProcessError exposes kind="timeout" and command info`

#### Concrete RED Verification Command
```bash
node --import tsx --test "test/integration/service.test.ts" --test-name-pattern="Antigravity execution timeout transitions job and agent to timed_out"
```

#### Expected Failing Output
```
✖ Antigravity execution timeout transitions job and agent to timed_out
  AssertionError [ERR_ASSERTION]: Expected values to be strictly equal:
  + actual: 'failed'
  - expected: 'timed_out'
    at test/integration/service.test.ts:3240:12
```

#### Step-by-Step Implementation Details
1. **Service (`src/service.ts`)**:
   In `runAntigravityAsync`, catch `AntigravityProcessError` and distinguish timeout from generic dispatch rejections:
   ```typescript
   } catch (error) {
     const message = redactSecrets(String(error));
     const current = this.store.getJob(job.id);
     if (current?.status === "aborted" || controller.signal.aborted) {
       this.recordActivity(agent, current ?? job, "abort", "Antigravity process ended after the bridge abort signal");
       return;
     }
     if (current?.resultPath || ["completed", "completed_partial", "delivery_pending", "delivered"].includes(current?.status ?? "")) {
       this.recordActivity(agent, current ?? job, "error", "Delivery failed for persisted Antigravity result: " + message);
       this.lastStreamError = message;
       return;
     }

     const isTimeout = error instanceof AntigravityProcessError && error.kind === "timeout";
     const targetStatus = isTimeout ? "timed_out" : "failed";

     if (current && current.status !== targetStatus) {
       this.store.updateJobStatus(job.id, targetStatus, message);
     }
     const currentAgent = this.store.getAgent(agent.id);
     if (currentAgent && currentAgent.status !== "closed") {
       this.store.updateAgentStatus(agent.id, targetStatus, message);
     }

     if (isTimeout) {
       this.recordActivity(
         agent,
         job,
         "deadline",
         "Antigravity process timed out after " + AGY_DEFAULT_TIMEOUT_MS + "ms; the process tree was terminated",
       );
       if (this.followLifecycles.has(job.id)) {
         await this.resolveFollow(job.id, {
           status: "timed_out",
           deadlineReached: true,
           workerAborted: true,
           resultAvailable: false,
           error: message,
         });
       }
     } else {
       this.recordActivity(agent, job, "error", "Antigravity rejected the task dispatch");
       if (this.followLifecycles.has(job.id)) {
         await this.resolveFollow(job.id, { status: "failed", error: message });
       }
     }
   }
   ```

#### Concrete GREEN Verification Commands
```bash
node --import tsx --test "test/integration/service.test.ts" --test-name-pattern="Antigravity execution timeout"
node --import tsx --test "test/unit/antigravity.test.ts"
```

#### Expected Passing Output
```
exit 0, zero failures; record actual count
```

#### Task 2 Execution Sequence
1. Write RED tests in `test/integration/service.test.ts` and `test/unit/antigravity.test.ts`.
2. Execute RED verification command to observe explicit expected failure before production code edits.
3. Implement minimal production edits in `src/service.ts`.
4. Execute focused GREEN verification commands; confirm exit 0, zero failures.
5. Perform independent review of the uncommitted scoped diff via `git diff`.
6. Apply corrections and re-verify if needed.
7. Perform local scoped git commit (do not push, do not merge, do not restart daemon/hosts).

---

### Task 3: Bounded Live-Daemon Readiness Bootstrapping & Serialized Stale-Lock Takeover (Pillar C)

#### User Story
As an MCP server starting up concurrently with a booting daemon or contending with stale locks, I want `ensureDaemonRunning` to wait for the live PID in `daemon.pid` to finish booting, and `acquireDaemonLock` to serialize stale takeover via a private sidecar mutex (`daemon.pid.lock`), so that duplicate process crashes, empty-slot TOCTOU races, and dual winners are completely eliminated.

#### Affected Files
- `src/security.ts`
- `src/cli.ts`
- `src/mcp.ts`
- `test/unit/security.test.ts`
- `test/unit/cli.test.ts`
- `test/integration/mcp.test.ts`

#### Helper Extraction & Circular Dependency Boundary
- **Circular Dependency Prevention**: `src/cli.ts` imports `runMcp` from `./mcp.js`. Directly importing `isProcessAlive` from `cli.ts` into `mcp.ts` would create a circular dependency (`cli.ts` -> `mcp.ts` -> `cli.ts`).
- **Extraction Path**:
  1. Move `isProcessAlive(pid: number): boolean` to `src/security.ts` (the existing shared low-level utility module).
  2. In `src/cli.ts`, import `isProcessAlive` from `./security.js` and re-export it (`export { isProcessAlive } from "./security.js"`) to maintain backward compatibility.
  3. In `src/mcp.ts`, import `readFile` from `"node:fs/promises"` and `isProcessAlive` from `./security.js`.

#### Proved Root Cause: Stale-Lock Takeover TOCTOU Race
Rename-only stale `daemon.pid` recovery (renaming `daemon.pid` to a unique temp file) suffered from an empty-slot TOCTOU window:
1. When contender A renamed `daemon.pid`, the path temporarily vanished from the filesystem.
2. A concurrent initial writer (contender B) entering `acquireDaemonLock` could then successfully create `daemon.pid` via `writePrivateFileExclusive`.
3. Contender A, finding the stale PID was dead, proceeded to write or rename back, producing dual winners, `ENOENT` read errors, or Windows `EPERM` file-locking races.
4. **Observed RED Evidence**: Stress testing with 50 concurrent stale-lock takeover iterations reproduced **2 simultaneous winners on run 2/50** (`fulfilled.length === 2`).

#### Serialized Sidecar Takeover Protocol (`daemon.pid.lock`) & Fail-Closed Invariants
1. **Sidecar Path & Acquisition**: Contender attempts exclusive creation of `path.join(dataDir, "daemon.pid.lock")` writing `String(pid) + "\n"` via `writePrivateFileExclusive`.
2. **Revalidation under Mutex**: Holding `daemon.pid.lock`, the contender re-reads `daemon.pid`.
3. **Never Replace Live Owner**: If `freshPid` is alive (`isProcessAlive(freshPid)`), it aborts takeover, releases the sidecar in `finally`, and throws duplicate daemon prevention (`"DeepSeek Sub-Agent daemon is already running (PID ${freshPid}). Duplicate daemon instance prevented."`).
4. **Main PID Overwrite**: If stale confirmed, it writes its PID to `daemon.pid` while holding the sidecar lock.
5. **Creator-Only Cleanup Invariant**: In `finally`, only the contender that atomically created `daemon.pid.lock` may remove it (`await unlink(takeoverLockPath).catch(() => undefined)`). Non-owners **never** unlink an existing sidecar lock.
6. **Why No Automatic Orphan Sidecar Deletion**: There is no portable atomic compare-and-delete in the Node.js filesystem API; read-then-unlink and rename-after-stale-read both recreate TOCTOU races by potentially targeting a replacement live sidecar created immediately after the check.
7. **Live Sidecar Contention**: If another contender holds `daemon.pid.lock` with a live PID, the contender waits briefly (`10ms`) and retries up to `maxAttempts = 5` (bounding total contention delay to ~50ms), failing closed with `"Failed to acquire exclusive daemon lock: lock file is held by another process."` without retry storms or ownership.
8. **Dead / Orphan / Corrupt Sidecar Fail-Closed**: If `daemon.pid.lock` exists with a dead PID, empty content, or non-numeric/corrupt content, or is unreadable (non-ENOENT), `acquireDaemonLock` fails closed immediately with actionable errors requiring manual operator cleanup, leaving both `daemon.pid` and `daemon.pid.lock` untouched on disk with zero winners:
   - *Dead/Stale Takeover PID*: `"Failed to acquire exclusive daemon lock: orphan or stale takeover sidecar lock encountered (PID ${takeoverPid}). Manual operator cleanup required."`
   - *Empty/Corrupt/Non-Numeric Sidecar*: `"Failed to acquire exclusive daemon lock: orphan or corrupt takeover sidecar lock encountered. Manual operator cleanup required."`
   - *Unreadable Sidecar Lock*: `"Failed to acquire exclusive daemon lock: takeover sidecar lock is unreadable."`
9. **Observed Evidence**: Deterministic RED reproduction for orphan/corrupt behavior; repeated 5x reviewer stress runs and prior 50/50 race stress runs produced **100% clean single winners** without duplicates or file-locking errors.

#### Readiness Path Invariants & Stale-Lock Rules
1. **MCP Rule (`ensureDaemonRunning`)**: `ensureDaemonRunning` in `src/mcp.ts` **never** unlinks, renames, or deletes `daemon.pid`, and never kills processes.
2. **Missing PID File (`ENOENT`)**: Proceed to `startDetachedDaemon`.
3. **Corrupt / Non-Numeric PID Content**: Fail closed immediately with a descriptive error (`"Corrupt or invalid daemon PID file encountered during bootstrap"`). Do NOT spawn a duplicate process, and do NOT delete the file.
4. **Valid PID & `isProcessAlive(pid) === true`**: Live booting daemon detected. Do NOT spawn a duplicate process (`shouldSpawn = false`). Proceed directly to the bounded health polling loop (`timeoutMs`).
5. **Valid PID & `isProcessAlive(pid) === false`**: Stale dead PID detected. Do NOT delete or unlink `daemon.pid` inside `ensureDaemonRunning`. Instead, proceed to spawn detached daemon once (`shouldSpawn = true`), delegating serialized takeover to `acquireDaemonLock` in `src/cli.ts`.
6. **Scope Expansion Rationale**: This scope expansion was accepted strictly because the race was reproduced and root-caused during independent review of Task 3; no broader restart/retry mechanism was added.

#### Test Setup (RED)
Add tests:
1. In `test/unit/security.test.ts`:
   - `isProcessAlive accurately detects running process and non-existent PID`
2. In `test/unit/cli.test.ts`:
   - `acquireDaemonLock exclusive lock prevents second acquisition and cleans up on release`
   - `acquireDaemonLock safely recovers from stale dead PID`
   - `acquireDaemonLock concurrent contenders result in exactly one winner`
   - `acquireDaemonLock concurrent contenders recovering from stale dead PID result in exactly one winner`
   - `acquireDaemonLock concurrent contenders (10) recovering from stale dead PID result in exactly one winner`
   - `acquireDaemonLock fails closed when lock file content is empty or unreadable during creation`
   - `acquireDaemonLock fails closed and leaves orphan dead sidecar lock untouched with zero winners`
   - `acquireDaemonLock fails closed and leaves corrupt sidecar lock untouched`
   - `acquireDaemonLock under live sidecar contention does not delete sidecar and produces no ownership`
   - `acquireDaemonLock normal owner cleanup removes only its own sidecar lock in finally`
3. In `test/integration/mcp.test.ts`:
   - `ensureDaemonRunning waits for live booting daemon without spawning duplicate`
   - `ensureDaemonRunning spawns daemon when lock file is missing`
   - `ensureDaemonRunning triggers spawn on stale dead PID, delegating atomic takeover to acquireDaemonLock`
   - `ensureDaemonRunning fails closed on corrupt PID matrix (empty, whitespace, 0, negative, float, trailing junk, unsafe integer) without spawning`
   - `ensureDaemonRunning times out and fails when live PID daemon never becomes ready without duplicate spawn`
   - `ensureDaemonRunning fails closed on non-ENOENT read error without duplicate spawn`

#### Concrete RED Verification Commands
```bash
node --import tsx --test "test/unit/security.test.ts" --test-name-pattern="isProcessAlive"
node --import tsx --test "test/unit/cli.test.ts" --test-name-pattern="acquireDaemonLock"
node --import tsx --test "test/integration/mcp.test.ts" --test-name-pattern="ensureDaemonRunning"
```

#### Expected Failing Output
```
✖ acquireDaemonLock concurrent contenders recovering from stale dead PID result in exactly one winner
  AssertionError [ERR_ASSERTION]: expected 1 fulfilled winner, got 2

✖ acquireDaemonLock fails closed and leaves orphan dead sidecar lock untouched with zero winners
  AssertionError [ERR_ASSERTION]: Expected values to match:
  /orphan or stale takeover sidecar lock.*Manual operator cleanup required/i

✖ ensureDaemonRunning waits for live booting daemon without spawning duplicate
  AssertionError [ERR_ASSERTION]: start callback should not have been called when live PID existed
```

#### Step-by-Step Implementation Details
1. **Security Utility (`src/security.ts`)**:
   Add `isProcessAlive`:
   ```typescript
   export function isProcessAlive(pid: number): boolean {
     try {
       process.kill(pid, 0);
       return true;
     } catch (error: unknown) {
       return (error as NodeJS.ErrnoException).code === "EPERM";
     }
   }
   ```
2. **CLI Module (`src/cli.ts`)**:
   Implement serialized sidecar takeover via `daemon.pid.lock`:
   ```typescript
   import { isProcessAlive } from "./security.js";
   export { isProcessAlive };

   export async function acquireDaemonLock(dataDir: string, pid = process.pid): Promise<() => Promise<void>> {
     await ensurePrivateDir(dataDir);
     const pidPath = path.join(dataDir, "daemon.pid");
     const takeoverLockPath = path.join(dataDir, "daemon.pid.lock");

     const maxAttempts = 5;
     for (let attempt = 0; attempt < maxAttempts; attempt++) {
       const acquiredFirst = await writePrivateFileExclusive(pidPath, String(pid) + "\n");
       if (acquiredFirst) {
         return createDaemonLockReleaser(pidPath, pid);
       }

       let existingContent: string;
       try {
         existingContent = await readFile(pidPath, "utf8");
       } catch (error: unknown) {
         const code = (error as NodeJS.ErrnoException).code;
         if (code === "ENOENT") {
           // Lock file was released or claimed by another contender right after exclusive write failed; retry.
           continue;
         }
         throw new Error("Failed to acquire exclusive daemon lock: lock file is held by another process.");
       }

       const trimmed = existingContent.trim();
       if (!trimmed || !/^[1-9]\d*$/.test(trimmed)) {
         throw new Error("Failed to acquire exclusive daemon lock: lock file is held by another process.");
       }

       const existingPid = Number.parseInt(trimmed, 10);
       if (!Number.isSafeInteger(existingPid) || existingPid <= 0) {
         throw new Error("Failed to acquire exclusive daemon lock: lock file is held by another process.");
       }

       if (isProcessAlive(existingPid)) {
         throw new Error(`DeepSeek Sub-Agent daemon is already running (PID ${existingPid}). Duplicate daemon instance prevented.`);
       }

       // Existing PID is dead (stale lock). Attempt serialized takeover via exclusive sidecar lock.
       const claimedTakeover = await writePrivateFileExclusive(takeoverLockPath, String(pid) + "\n");
       if (claimedTakeover) {
         try {
           // Re-read pidPath under exclusive takeover lock to verify it is still dead or absent.
           let freshContent = "";
           try {
             freshContent = await readFile(pidPath, "utf8");
           } catch (error: unknown) {
             if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
               throw error;
             }
           }
           const freshTrimmed = freshContent.trim();
           if (freshTrimmed && /^[1-9]\d*$/.test(freshTrimmed)) {
             const freshPid = Number.parseInt(freshTrimmed, 10);
             if (Number.isSafeInteger(freshPid) && freshPid > 0 && isProcessAlive(freshPid)) {
               throw new Error(`DeepSeek Sub-Agent daemon is already running (PID ${freshPid}). Duplicate daemon instance prevented.`);
             }
           }

           // Stale confirmed; overwrite pidPath with our PID.
           await writePrivateFile(pidPath, String(pid) + "\n");
           return createDaemonLockReleaser(pidPath, pid);
         } finally {
           await unlink(takeoverLockPath).catch(() => undefined);
         }
       } else {
         // Another contender is actively performing takeover. Inspect the sidecar lock.
         // NEVER automatically unlink/reclaim an existing sidecar lock to avoid recursive TOCTOU races.
         let takeoverContent = "";
         try {
           takeoverContent = await readFile(takeoverLockPath, "utf8");
         } catch (error: unknown) {
           if ((error as NodeJS.ErrnoException).code === "ENOENT") {
             // Takeover owner unlinked the sidecar right after writePrivateFileExclusive failed; retry.
             continue;
           }
           throw new Error("Failed to acquire exclusive daemon lock: takeover sidecar lock is unreadable.");
         }

         const takeoverTrimmed = takeoverContent.trim();
         if (!takeoverTrimmed || !/^[1-9]\d*$/.test(takeoverTrimmed)) {
           throw new Error("Failed to acquire exclusive daemon lock: orphan or corrupt takeover sidecar lock encountered. Manual operator cleanup required.");
         }

         const takeoverPid = Number.parseInt(takeoverTrimmed, 10);
         if (!Number.isSafeInteger(takeoverPid) || takeoverPid <= 0) {
           throw new Error("Failed to acquire exclusive daemon lock: orphan or corrupt takeover sidecar lock encountered. Manual operator cleanup required.");
         }

         if (!isProcessAlive(takeoverPid)) {
           throw new Error(`Failed to acquire exclusive daemon lock: orphan or stale takeover sidecar lock encountered (PID ${takeoverPid}). Manual operator cleanup required.`);
         }

         // Live takeover owner is actively holding the sidecar lock; wait briefly so the winner can finish writing its new PID, then retry.
         await new Promise((resolve) => setTimeout(resolve, 10));
         continue;
       }

       // Lost or waiting on takeover race; wait briefly so the winner can finish writing its new PID, then retry.
       await new Promise((resolve) => setTimeout(resolve, 10));
     }

     throw new Error("Failed to acquire exclusive daemon lock: lock file is held by another process.");
   }
   ```
3. **MCP Module (`src/mcp.ts`)**:
   Import `readFile` from `"node:fs/promises"` and `isProcessAlive` from `./security.js`.
   Update `ensureDaemonRunning`:
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

     const pidPath = path.join(config.dataDir, "daemon.pid");
     let shouldSpawn = true;

     try {
       const content = await readFile(pidPath, "utf8");
       const trimmed = content.trim();
       if (trimmed && /^\d+$/.test(trimmed)) {
         const pid = Number.parseInt(trimmed, 10);
         if (Number.isInteger(pid) && pid > 0 && isProcessAlive(pid)) {
           shouldSpawn = false;
         }
       } else if (trimmed) {
         throw new Error("Corrupt daemon PID file encountered during bootstrap");
       }
     } catch (err: unknown) {
       const code = (err as NodeJS.ErrnoException).code;
       if (code !== "ENOENT") {
         throw err;
       }
     }

     if (shouldSpawn) {
       await (options.start ?? startDetachedDaemon)(config);
     }

     const timeoutMs = options.timeoutMs ?? Math.max(10_000, Math.min(45_000, config.opencodeStartupTimeoutMs + 5_000));
     const retryMs = options.retryMs ?? 100;
     const deadline = Date.now() + timeoutMs;
     while (Date.now() < deadline) {
       try {
         await client.health();
         return;
       } catch (error) {
         lastError = error;
       }
       await delay(Math.min(retryMs, Math.max(1, deadline - Date.now())));
     }
     throw new Error("DeepSeek Sub-Agent daemon did not become ready: " + redactSecrets(String(lastError)));
   }
   ```

#### Concrete GREEN Verification Commands
```bash
node --import tsx --test "test/unit/security.test.ts"
node --import tsx --test "test/unit/cli.test.ts"
node --import tsx --test "test/integration/mcp.test.ts" --test-name-pattern="ensureDaemonRunning"
```

#### Expected Passing Output
```
exit 0, zero failures; record actual count (observed: CLI 28 pass; full suite 287 pass / 3 skipped; repeated 5x reviewer stress and prior 50/50 race stress passed)
```

#### Task 3 Execution Sequence
1. Write RED tests in `test/unit/security.test.ts`, `test/unit/cli.test.ts`, and `test/integration/mcp.test.ts`.
2. Execute RED verification commands to observe explicit expected failures before production edits.
3. Implement minimal production edits in `src/security.ts`, `src/cli.ts` (serialized sidecar takeover via `daemon.pid.lock`), and `src/mcp.ts`.
4. Execute focused GREEN verification commands; confirm exit 0, zero failures.
5. Perform independent review of the uncommitted scoped diff via `git diff`.
6. Apply corrections and re-verify if needed.
7. Perform local scoped git commit (do not push, do not merge, do not restart daemon/hosts).

---

## Full Suite & Regression Validation Gate

Execute the complete verification gate in order:

```bash
# 1. Focused Verification for all three tasks
node --import tsx --test "test/integration/http-server.test.ts" "test/integration/service.test.ts" "test/integration/mcp.test.ts" "test/unit/security.test.ts" "test/unit/antigravity.test.ts" "test/unit/cli.test.ts"

# 2. Type Check and Linter (tsc --noEmit)
npm run lint

# 3. TypeScript Build
npm run build

# 4. Full Regression Test Suite
# Outcome assertion: exit 0, zero failures; record actual count (observed baseline: 287 passed, 3 skipped, 0 failed)
npm test

# 5. Optional Script Check
# Note: scripts/validate.ps1 is absent from the repository; do not substitute with unapproved scripts

# 6. Diff and Whitespace Hygiene Check
git diff --check

# 7. Unfinished Placeholders / TODO Scan (must return zero matches)
git grep -n -E "TODO|FIXME|\.\.\." src/ test/ || true

# 8. Scope & Working Tree Status Audit
git status -s
```

---

## Self-Review Checklist

- [x] Conforms to writing-plans contract with exact title `# MCP Transport Recovery Automation Implementation Plan` and agentic preamble.
- [x] Explicit Goal, Architecture, Tech Stack, Spec, and "## Global Constraints" sections.
- [x] Task 1 includes `test/integration/http-server.test.ts` in affected files, RED tests, focused commands, and specifies `optionalString` normalization.
- [x] Task 1 preserves exact legacy `recoverResult(jobId, agentId)` argument order while accepting typed selector.
- [x] Task 2 explicitly imports `AntigravityProcessError` and `AGY_DEFAULT_TIMEOUT_MS` from `./antigravity/runner.js` and names existing `this.resolveFollow` settlement helper.
- [x] Task 3 defines circular dependency prevention boundary by extracting `isProcessAlive` to `src/security.ts`, importing `readFile` from `"node:fs/promises"`, implementing serialized sidecar takeover (`daemon.pid.lock`) in `acquireDaemonLock`, with fail-closed non-owner sidecar invariants (no automatic orphan deletion, operator cleanup errors, bounded live wait), and delegating dead PID cleanup exclusively to `acquireDaemonLock`.
- [x] Task 3 includes `test/unit/cli.test.ts` in the file map and test suite, with unit tests for stale lock recovery, concurrent contenders, 10-contender stress, empty/corrupt lock fail-closed behavior, orphan/corrupt sidecar fail-closed behavior, live sidecar contention non-deletion, and owner cleanup.
- [x] Fabricated pass counts replaced with outcome assertions ("exit 0, zero failures; record actual count") and baseline evidence preserved (287 passed, 3 skipped, 0 failed; CLI 28 passed).
- [x] Explicit per-task sequencing defined (RED -> focused GREEN -> review uncommitted diff -> corrections -> local scoped commit; no push/merge/restart).
- [x] Final full validation gates include lint, build, test, git diff --check, placeholder scan, and literal handling of absent scripts.
- [x] Circuit breaker explicitly excluded from binding implementation with documented unmet evidence threshold.
