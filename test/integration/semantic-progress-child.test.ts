import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, stat } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";
import { createDefaultConfig } from "../../src/config.js";
import { AntigravityAdapter } from "../../src/antigravity/adapter.js";
import { AntigravitySpool } from "../../src/antigravity/spool.js";
import { DEFAULT_CODEX_CAPABILITIES, type CodexBinding, type CodexDeliveryAdapter } from "../../src/codex/adapter.js";
import type { JobRecord, OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage, WakeEnvelope } from "../../src/types.js";

const childFixturePath = fileURLToPath(new URL("../fixtures/semantic-progress/child-worker.cjs", import.meta.url));

/**
 * BOUNDARY CLI MOCK:
 * Explicitly labeled separate unproven real Codex CLI step.
 * Proves bridge delivery interface and wake envelope contract without misclaiming full E2E.
 */
class BoundaryCodexCliMock implements CodexDeliveryAdapter {
  readonly isBoundaryMock = true;
  available = true;
  reason: string | null = null;
  capabilities = { ...DEFAULT_CODEX_CAPABILITIES, authoritativeAttachment: true };
  deliveredWakes: Array<{ envelope: WakeEnvelope; binding: CodexBinding }> = [];
  deliveredJobs: Array<{ job: JobRecord; binding: CodexBinding; text: string }> = [];
  reconciledCalls: Array<{ threadId: string; marker: string }> = [];

  async start(): Promise<void> {}
  async close(): Promise<void> {}
  async deliver(job: JobRecord, binding: CodexBinding, text: string): Promise<"codex-steer" | "codex-start"> {
    this.deliveredJobs.push({ job, binding, text });
    return "codex-start";
  }
  async deliverWake(envelope: WakeEnvelope, binding: CodexBinding): Promise<"codex-steer" | "codex-start"> {
    this.deliveredWakes.push({ envelope, binding });
    return "codex-start";
  }
  async reconcileSend(threadId: string, marker: string): Promise<boolean> {
    this.reconciledCalls.push({ threadId, marker });
    return false;
  }
  onCorrelation(_listener: (correlation: any) => void): () => void {
    return () => {};
  }
}

class FakeOpenCodeClient implements OpenCodeClientLike {
  async health() { return { healthy: true, version: "fake" }; }
  async createSession() { return { id: "session_fake" }; }
  async promptAsync() {}
  async listMessages(): Promise<OpenCodeMessage[]> { return []; }
  async getDiff() { return []; }
  async abort() {}
  async replyPermission() {}
  async subscribe(_onEvent: (event: OpenCodeEvent) => Promise<void> | void, signal?: AbortSignal) {
    await new Promise<void>((resolve) => {
      signal?.addEventListener("abort", () => resolve(), { once: true });
    });
  }
}

class FakeOpenCodeManager {
  constructor(private client = new FakeOpenCodeClient()) {}
  async start() {
    return {
      serverId: "fake_server",
      baseUrl: "http://127.0.0.1:40999",
      client: this.client,
      processId: 1234,
      status: () => "attached" as const,
      stop: async () => {},
    };
  }
  async stop() {}
}

/**
 * Safely cleans up owned child processes:
 * Holds ChildProcess handles and only cleans up still-live children (never blind raw PID kill).
 * Checks exitCode/signalCode, signals if alive, and awaits exit before directory cleanup.
 */
async function safelyCleanupChildren(children: ChildProcess[]): Promise<void> {
  for (const child of children) {
    if (child.exitCode === null && child.signalCode === null && !child.killed) {
      try {
        child.kill("SIGTERM");
      } catch {}
      await new Promise<void>((resolve) => {
        if (child.exitCode !== null || child.signalCode !== null) return resolve();
        const timeout = setTimeout(() => {
          try { child.kill("SIGKILL"); } catch {}
          resolve();
        }, 1000);
        child.once("exit", () => {
          clearTimeout(timeout);
          resolve();
        });
      });
    }
  }
}

test("real spawned finite child: timed output crosses capturecap without advisory, background advisory fires on silence with unconsumed active job", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "sp-child-stream-"));
  const store = await BridgeStore.open(directory);
  const delivery = new BoundaryCodexCliMock();
  const spawnedChildren: ChildProcess[] = [];

  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
    experimentalSameChatDelivery: true,
    inactivityThresholdSeconds: 0.5,
    advisoryCheckIntervalMs: 50,
    modelRoutes: [
      {
        name: "antigravity-flash-high",
        providerId: "antigravity",
        modelId: "gemini-3.8-flash-high",
        variant: null,
        enabled: true,
        default: true,
        display: "Antigravity · Gemini 3.8 Flash High",
      },
    ],
  });

  const antigravity = new AntigravityAdapter({
    command: "node",
    spawnFn: (command, args, options) => {
      const child = spawn(process.execPath, [childFixturePath, ...args], {
        cwd: options.cwd,
        env: {
          ...process.env,
          TEST_CHUNK_BYTES: "262144", // 256 KB per chunk
          TEST_CHUNK_COUNT: "6", // 6 * 256 KB = 1,572,864 bytes > 1 MB capturecap
          TEST_CHUNK_INTERVAL_MS: "25", // 25 ms between chunks
          TEST_SILENCE_MS: "1800", // 1.8s silence window
          TEST_EXIT_CODE: "0",
        },
        shell: false,
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      });
      spawnedChildren.push(child);
      return child;
    },
  });

  const service = new BridgeService(config, {
    store,
    manager: new FakeOpenCodeManager() as any,
    codex: delivery,
    antigravity,
  });

  try {
    await service.start();

    // 1. Spawn real child via actual BridgeService -> AntigravityAdapter -> AntigravitySupervisor wiring
    const accepted = await service.spawn({
      requestId: "req_real_child_progress_1",
      topic: "Real Child Streaming Test",
      task: "Stream output past 1MB capturecap and pause in silence",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
      trustedThreadId: "thread_real_child",
    });

    assert.equal(accepted.accepted, true);
    assert.ok(accepted.jobId);
    assert.ok(accepted.agentId);

    // Bind and park the job under ALL predicate
    store.bindJob({
      jobId: accepted.jobId,
      threadId: "thread_real_child",
      originatingTurnId: "turn_1",
      originatingItemId: "item_1",
    });

    const receipt = await service.park({
      jobIds: [accepted.jobId],
      predicate: "ALL",
      threadId: "thread_real_child",
    });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.generation, 1);

    // 2. Observe streaming phase: chunks arrive rapidly, crossing capturecap (1MB)
    const streamStartDeadline = Date.now() + 3000;
    while ((store.getJob(accepted.jobId)?.progressRevision ?? 0) < 2 && Date.now() < streamStartDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }

    // Zero advisory delivered while child stream is advancing
    assert.equal(delivery.deliveredWakes.length, 0, "no advisory wake while output continues past capturecap");

    // Authoritative process ID check: real child PID must be recorded
    assert.ok(spawnedChildren.length > 0, "child process must have been spawned");
    const child = spawnedChildren[0]!;
    const childPid = child.pid!;
    console.log(`[REAL_CHILD_PID=${childPid}]`);
    const jobStreaming = store.getJob(accepted.jobId)!;
    assert.equal(jobStreaming.workerPid, childPid, "worker_pid must match actual spawned child PID");
    assert.ok((jobStreaming.progressRevision ?? 0) >= 2, "progress revision must advance during output");

    // 3. Autonomous background wake on silence: ZERO manual consult/evaluate
    // Inactivity threshold is 0.5s; silence is 1.8s. Inactivity should fire between 1.0s and 1.5s.
    const wakeDeadline = Date.now() + 3000;
    while (delivery.deliveredWakes.length === 0 && Date.now() < wakeDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    assert.equal(delivery.deliveredWakes.length, 1, "background advisory wake must fire autonomously on silence");
    const wake = delivery.deliveredWakes[0]!;
    assert.equal(wake.envelope.parkId, receipt.parkId);
    assert.deepEqual(wake.envelope.readyJobIds, []);
    assert.deepEqual(wake.envelope.advisoryJobIds, [accepted.jobId]);
    assert.equal(wake.envelope.pendingCount, 1);
    assert.equal(wake.envelope.resultHashes[accepted.jobId], undefined);
    assert.ok(wake.envelope.advisoryFingerprints?.[accepted.jobId]);
    assert.doesNotMatch(wake.envelope.instruction, /subagents_follow/);
    assert.ok(["running", "following"].includes(wake.envelope.statuses[accepted.jobId] ?? ""));

    // 4. Inspect SQLite active job: unconsumed, no resultPath, active non-terminal status, fence intact
    const jobAtWake = store.getJob(accepted.jobId)!;
    assert.ok(["running", "following"].includes(jobAtWake.status), "job must remain in active non-terminal status");
    assert.equal(jobAtWake.resultConsumedAt, null, "resultConsumedAt must be null");
    assert.equal(jobAtWake.resultPath, null, "resultPath must be null");
    assert.equal(jobAtWake.fence, 1, "fence must be 1");
    assert.equal(jobAtWake.workerPid, childPid, "workerPid must remain real child PID");
    assert.ok((jobAtWake.progressRevision ?? 0) >= 4, "progress revision must reflect streaming progress");

    assert.ok(jobAtWake.escalationProposal, "escalationProposal must be recorded on stall");
    const proposal = JSON.parse(jobAtWake.escalationProposal!);
    assert.equal(proposal.advisoryOnly, true, "proposal must be advisory only");
    assert.equal(proposal.fence, 1, "proposal fence must match");
    assert.equal(proposal.attempt, jobAtWake.attempt, "proposal attempt must match");
    assert.ok(proposal.progressRevision >= 4, "proposal progress revision must match");

    // 5. Inspect on-disk spool progress.json and capturecap bounded stdout.log
    const spool = new AntigravitySpool(directory);
    const attempts = await spool.listAttempts(accepted.jobId);
    assert.equal(attempts.length, 1, "exactly one attempt directory created in spool");
    const attempt = attempts[0]!;
    assert.equal(attempt.attemptId, jobAtWake.attempt);
    assert.equal(attempt.fence, 1);

    const progress = await spool.readProgress(attempt.attemptId, accepted.jobId);
    assert.ok(progress, "progress.json must exist in attempt directory");
    assert.equal(progress.attemptId, attempt.attemptId);
    assert.equal(progress.fence, 1);
    assert.ok(progress.progressRevision >= 4);
    assert.ok(progress.totalBytes > 1_048_576, "totalStreamBytes must exceed 1MB capturecap (actual: " + progress.totalBytes + ")");

    // Authoritative check on bounded capturecap sink:
    const stdoutStat = await stat(attempt.stdoutPath);
    assert.equal(stdoutStat.size, 1_048_576, "stdout.log must be capped at exactly maxOutputBytes (1,048,576 bytes)");

    // 6. Finite child self-exit: wait for child to complete 1.8s silence and exit 0
    const completionDeadline = Date.now() + 4000;
    while (store.getJob(accepted.jobId)?.status !== "delivered" && Date.now() < completionDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    const completedJob = store.getJob(accepted.jobId)!;
    assert.equal(completedJob.status, "delivered", "job must settle into delivered upon finite child self-exit");
    assert.ok(completedJob.resultPath !== null, "resultPath must be populated upon completion");
    assert.equal(completedJob.resultConsumedAt, null, "result remains unconsumed until explicit follow");
    const agyMetrics = antigravity.getMetrics();
    console.log(`[WRITE_AMPLIFICATION_METRICS: chunks=${agyMetrics.chunksReceived}, diskWrites=${agyMetrics.progressWritesCompleted}, coalesced=${agyMetrics.coalescedChunks}]`);
    assert.ok(agyMetrics.chunksReceived >= 6, "at least 6 chunks were received");
    assert.ok(agyMetrics.progressWritesCompleted < agyMetrics.chunksReceived, "bounded coalescing reduced disk write amplification");
    assert.ok(agyMetrics.coalescedChunks >= 1, "at least one chunk was coalesced into in-flight writer");
    const svcMetrics = service.getMetrics();
    assert.ok(svcMetrics.sqliteProgressUpdates < agyMetrics.chunksReceived, "sqlite updates coalesced");
    assert.equal(service.getActiveInactivityTimerCount(), 0, "terminal timer count must be 0 after delivered");
  } finally {
    await service.stop();
    await safelyCleanupChildren(spawnedChildren);
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("isolated actual process-backed recovery and reopen SQLite of active attempt: preserves PID, fence, progress and exact-once advisory", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "sp-child-active-recovery-"));
  const store = await BridgeStore.open(directory);
  const delivery = new BoundaryCodexCliMock();
  const spawnedChildren: ChildProcess[] = [];

  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
    experimentalSameChatDelivery: true,
    inactivityThresholdSeconds: 0.5,
    advisoryCheckIntervalMs: 50,
    modelRoutes: [
      {
        name: "antigravity-flash-high",
        providerId: "antigravity",
        modelId: "gemini-3.8-flash-high",
        variant: null,
        enabled: true,
        default: true,
        display: "Antigravity · Gemini 3.8 Flash High",
      },
    ],
  });

  const antigravity = new AntigravityAdapter({
    command: "node",
    spawnFn: (command, args, options) => {
      const child = spawn(process.execPath, [childFixturePath, ...args], {
        cwd: options.cwd,
        env: {
          ...process.env,
          TEST_CHUNK_BYTES: "262144",
          TEST_CHUNK_COUNT: "5",
          TEST_CHUNK_INTERVAL_MS: "25",
          TEST_SILENCE_MS: "3500", // Long silence: 3.5s gives ample time for recovery and advisory wake
          TEST_EXIT_CODE: "0",
        },
        shell: false,
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      });
      spawnedChildren.push(child);
      return child;
    },
  });

  let service: BridgeService | null = new BridgeService(config, {
    store,
    manager: new FakeOpenCodeManager() as any,
    codex: delivery,
    antigravity,
  });

  let service2: BridgeService | null = null;
  let store2: BridgeStore | null = null;

  try {
    await service.start();

    const accepted = await service.spawn({
      requestId: "req_active_recovery_1",
      topic: "Active Attempt Spool Recovery",
      task: "Stream output, remain active during crash/restart, and recover from disk spool",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
      trustedThreadId: "thread_active_recovery",
    });

    store.bindJob({
      jobId: accepted.jobId,
      threadId: "thread_active_recovery",
      originatingTurnId: "turn_1",
      originatingItemId: "item_1",
    });

    const receipt = await service.park({
      jobIds: [accepted.jobId],
      predicate: "ALL",
      threadId: "thread_active_recovery",
    });
    assert.equal(receipt.armed, true);

    // Wait for initial streaming to start and progress to be recorded
    const progressStartDeadline = Date.now() + 3000;
    while ((store.getJob(accepted.jobId)?.progressRevision ?? 0) < 2 && Date.now() < progressStartDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }

    assert.ok(spawnedChildren.length > 0, "child process must be spawned");
    const child = spawnedChildren[0]!;
    const childPid = child.pid!;
    console.log(`[ACTIVE_RECOVERY_CHILD_PID=${childPid}]`);

    // Verify child is actively executing
    assert.equal(child.exitCode, null, "child must still be running in OS");
    const jobBeforeCrash = store.getJob(accepted.jobId)!;
    assert.ok(["running", "following"].includes(jobBeforeCrash.status), "job must be actively running/following before crash");
    assert.equal(jobBeforeCrash.workerPid, childPid, "workerPid must match running child");

    // 1. Simulate bridge crash / restart: stop service without killing background worker processes
    await service.stop({ abortWorkers: false });
    service = null;
    store.close(); // Completely close first SQLite connection

    // Verify child process is STILL running in the background after service shutdown
    assert.equal(child.exitCode, null, "child process remains alive after service detach");

    // 2. Open fresh isolated SQLite store from disk
    store2 = await BridgeStore.open(directory);
    const delivery2 = new BoundaryCodexCliMock();

    service2 = new BridgeService(config, {
      store: store2,
      manager: new FakeOpenCodeManager() as any,
      codex: delivery2,
      antigravity,
    });

    // 3. Reopen service2 on reopened SQLite database: must reattach to live worker and reconcile progress.json
    await service2.start();

    // Authoritative check on recovered job state in reopened SQLite:
    const recoveredJob = store2.getJob(accepted.jobId)!;
    assert.ok(["running", "following"].includes(recoveredJob.status), "recovered job must remain active running/following (NOT delivered)");
    assert.equal(recoveredJob.workerPid, childPid, "workerPid preserved from live process");
    assert.equal(recoveredJob.fence, 1, "fence preserved");
    assert.ok((recoveredJob.progressRevision ?? 0) >= 2, "progressRevision reconciled from spool progress.json");
    console.log(`[ACTIVE_RECONNECT_METRIC: count=${service2.getMetrics().reconnectReconciledCount}]`);
    assert.equal(service2.getMetrics().reconnectReconciledCount, 1, "active reconnect counter incremented");

    // 4. Background advisory wake fires autonomously on reopened service during silence:
    const wakeDeadline = Date.now() + 3000;
    while (delivery2.deliveredWakes.length === 0 && Date.now() < wakeDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    assert.equal(delivery2.deliveredWakes.length, 1, "background advisory wake delivered exactly once on reopened service");
    const wake = delivery2.deliveredWakes[0]!;
    assert.equal(wake.envelope.parkId, receipt.parkId);
    assert.deepEqual(wake.envelope.readyJobIds, []);
    assert.deepEqual(wake.envelope.advisoryJobIds, [accepted.jobId]);
    assert.equal(wake.envelope.pendingCount, 1);
    assert.equal(wake.envelope.resultHashes[accepted.jobId], undefined);
    assert.ok(wake.envelope.advisoryFingerprints?.[accepted.jobId]);
    assert.doesNotMatch(wake.envelope.instruction, /subagents_follow/);

    // 5. Finite child completes silence and exits 0:
    const completionDeadline = Date.now() + 6000;
    while (store2.getJob(accepted.jobId)?.status !== "delivered" && Date.now() < completionDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    const settledJob = store2.getJob(accepted.jobId)!;
    assert.equal(settledJob.status, "delivered", "job must settle into delivered upon finite child self-exit");
    assert.equal(delivery2.deliveredJobs.length, 1, "result envelope delivered exactly once");
    assert.equal(service2.getActiveInactivityTimerCount(), 0, "inactivity timers cleared upon terminal status");
  } finally {
    if (service) await service.stop();
    if (service2) await service2.stop();
    await safelyCleanupChildren(spawnedChildren);
    try { store.close(); } catch {}
    try { if (store2) store2.close(); } catch {}
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  }
});

test("singleflight regression: concurrent runAdvisoryCheckPass executions prevent async overlap and lifecycle cancel on stop", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "sp-singleflight-"));
  const store = await BridgeStore.open(directory);
  const delivery = new BoundaryCodexCliMock();
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
    advisoryCheckIntervalMs: 50,
  });
  const antigravity = new AntigravityAdapter();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery, antigravity });
  await service.start();

  try {
    // Populate store with an armed barrier and an active job so advisory pass has async work
    const agent = store.createAgent({
      id: "agent_sf",
      title: "Singleflight Agent",
      topic: "Singleflight Test",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "fake_server",
      opencodeSessionId: "antigravity:agent_sf",
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });
    const job = store.createJob({
      id: "job_sf",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_sf",
      promptHash: "hash_sf",
      status: "running",
    });
    store.createOrUpdateParkBarrier({
      id: "park_sf",
      threadId: "thread_sf",
      generation: 1,
      armed: true,
      state: "armed",
      predicateType: "ALL",
    });
    store.setParkJobs("park_sf", [job.id]);

    // Intercept syncJobStreamProgressFromSpool with an async delay to guarantee concurrent overlap
    const originalSync = (service as any).syncJobStreamProgressFromSpool.bind(service);
    (service as any).syncJobStreamProgressFromSpool = async (job: any) => {
      await new Promise((resolve) => setTimeout(resolve, 50));
      return originalSync(job);
    };

    // Run two advisory passes concurrently
    await Promise.all([
      (service as any).runAdvisoryCheckPass(),
      (service as any).runAdvisoryCheckPass(),
    ]);

    const metrics = service.getMetrics();
    console.log(`[SINGLEFLIGHT_METRICS: started=${metrics.singleflightRunsStarted}, skipped=${metrics.singleflightRunsSkipped}]`);
    assert.ok(metrics.singleflightRunsStarted >= 1, "at least one pass started");
    assert.ok(metrics.singleflightRunsSkipped >= 1, "overlapping pass skipped by singleflight");

    // Lifecycle cancel: stopping service aborts advisory passes
    await service.stop();
    assert.equal((service as any).advisoryPassRunning, false, "advisoryPassRunning reset on stop");
  } finally {
    await service.stop();
    try { store.close(); } catch {}
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  }
});

test("boundary contract: BoundaryCodexCliMock explicitly labeled as separate unproven real Codex CLI step", () => {
  const mock = new BoundaryCodexCliMock();
  assert.equal(mock.isBoundaryMock, true);
  assert.equal(mock.capabilities.authoritativeAttachment, true);
  assert.equal(mock.deliveredWakes.length, 0);
  assert.equal(mock.deliveredJobs.length, 0);
  assert.equal(mock.reconciledCalls.length, 0);
});
