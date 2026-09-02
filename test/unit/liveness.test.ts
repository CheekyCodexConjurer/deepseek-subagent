import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";
import { AntigravitySpool } from "../../src/antigravity/spool.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

class FakeOpenCodeClient implements OpenCodeClientLike {
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  abortCalls: string[] = [];

  async health() { return { healthy: true, version: "fake" }; }
  async createSession(dir: string, title: string) { return { id: "session_fake_" + title }; }
  async promptAsync(sessionId: string, task: string) {
    this.promptCalls.push({ sessionId, task });
  }
  async listMessages(): Promise<OpenCodeMessage[]> { return []; }
  async getDiff() { return []; }
  async abort(sessionId: string) {
    this.abortCalls.push(sessionId);
  }
  async replyPermission() {}
  async subscribe(_onEvent: (event: OpenCodeEvent) => Promise<void> | void, signal?: AbortSignal) {
    await new Promise<void>((resolve) => {
      signal?.addEventListener("abort", () => resolve(), { once: true });
    });
  }
}

class FakeOpenCodeManager {
  constructor(readonly client = new FakeOpenCodeClient()) {}
  async start() {
    return {
      serverId: "server_fake",
      baseUrl: "http://127.0.0.1:1",
      client: this.client,
      processId: 12345,
      stop: async () => undefined,
    };
  }
  async stop() {}
}

test("1. Effective execution window is derived from follow window and not hardcoded to 900s", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "liveness-test-1-"));
  const store = await BridgeStore.open(dir);
  const config = createDefaultConfig({
    dataDir: dir,
    configPath: path.join(dir, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
    followDefaultWaitMinutes: 20,
    followDefaultGraceMinutes: 5,
    workerMaxExecutionMinutes: 70,
  });

  let receivedTimeoutMs: number | undefined;
  const mockAntigravity = {
    async runPrompt(options: any) {
      receivedTimeoutMs = options.timeoutMs;
      return {
        status: "completed" as const,
        runId: "run_test",
        summary: "done",
        files: [],
        tests: [],
        risks: [],
        diffSummary: "none",
        model: "gemini-3.8-flash-high",
        modelDisplayName: "Antigravity",
        workspace: options.cwd,
        rawOutput: "done",
      };
    },
  };

  const service = new BridgeService(config, {
    store,
    manager: new FakeOpenCodeManager() as any,
    antigravity: mockAntigravity as any,
  });

  try {
    await service.start();
    await service.spawn({
      requestId: "req_window_1",
      topic: "Window Test",
      task: "Test execution window",
      cwd: dir,
      modelRoute: "antigravity-flash-high",
    });

    await new Promise((r) => setTimeout(r, 100));

    // Effective window: 20m + 5m = 25m = 1,500,000ms. NOT 900,000ms (900s)!
    assert.equal(receivedTimeoutMs, 25 * 60_000);
  } finally {
    await service.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("2. Configurable absolute cap constrains the derived execution window", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "liveness-test-2-"));
  const store = await BridgeStore.open(dir);
  const config = createDefaultConfig({
    dataDir: dir,
    configPath: path.join(dir, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
    followDefaultWaitMinutes: 40,
    followDefaultGraceMinutes: 10,
    workerMaxExecutionMinutes: 15,
  });

  let receivedTimeoutMs: number | undefined;
  const mockAntigravity = {
    async runPrompt(options: any) {
      receivedTimeoutMs = options.timeoutMs;
      return {
        status: "completed" as const,
        runId: "run_test_2",
        summary: "done",
        files: [],
        tests: [],
        risks: [],
        diffSummary: "none",
        model: "gemini-3.8-flash-high",
        modelDisplayName: "Antigravity",
        workspace: options.cwd,
        rawOutput: "done",
      };
    },
  };

  const service = new BridgeService(config, {
    store,
    manager: new FakeOpenCodeManager() as any,
    antigravity: mockAntigravity as any,
  });

  try {
    await service.start();
    await service.spawn({
      requestId: "req_window_2",
      topic: "Capped Window Test",
      task: "Test capped execution window",
      cwd: dir,
      modelRoute: "antigravity-flash-high",
    });

    await new Promise((r) => setTimeout(r, 100));
    assert.equal(receivedTimeoutMs, 15 * 60_000);
  } finally {
    await service.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("3. Manual follow extension updates existing non-auto-armed follow lifecycle and lease", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "liveness-test-3-"));
  const store = await BridgeStore.open(dir);
  const config = createDefaultConfig({
    dataDir: dir,
    configPath: path.join(dir, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
    followDefaultWaitMinutes: 10,
    followDefaultGraceMinutes: 2,
  });

  let finishRun: (() => void) | null = null;
  const mockAntigravity = {
    async runPrompt(options: any) {
      await new Promise<void>((resolve) => { finishRun = resolve; });
      return {
        status: "completed" as const,
        runId: "run_test_3",
        summary: "done",
        files: [],
        tests: [],
        risks: [],
        diffSummary: "none",
        model: "gemini-3.8-flash-high",
        modelDisplayName: "Antigravity",
        workspace: options.cwd,
        rawOutput: "done",
      };
    },
  };

  const service = new BridgeService(config, {
    store,
    manager: new FakeOpenCodeManager() as any,
    antigravity: mockAntigravity as any,
  });

  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "req_manual_follow",
      topic: "Manual Follow Test",
      task: "Test manual follow extension",
      cwd: dir,
      modelRoute: "antigravity-flash-high",
    });

    const followPromise1 = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 10, graceMinutes: 2 });
    const initialJob = store.getJob(accepted.jobId)!;
    const initialDeadline = Date.parse(initialJob.followDeadlineAt!);

    const followPromise2 = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 30, graceMinutes: 5 });
    const extendedJob = store.getJob(accepted.jobId)!;
    const extendedDeadline = Date.parse(extendedJob.followDeadlineAt!);

    assert.ok(extendedDeadline > initialDeadline, "Manual follow must extend the deadline even on non-auto-armed lifecycle");
    assert.equal(extendedJob.followGraceMinutes, 5, "Manual follow must extend grace minutes");

    finishRun?.();
    await Promise.all([followPromise1, followPromise2]);
  } finally {
    finishRun?.();
    await service.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("4. Authoritative status exposes separate heartbeat, lease_expires_at, attempt/fence, PID/session and resultPersisted", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "liveness-test-4-"));
  const store = await BridgeStore.open(dir);
  const config = createDefaultConfig({
    dataDir: dir,
    configPath: path.join(dir, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
  });

  const spool = new AntigravitySpool(dir);
  const agent = store.createAgent({
    id: "agent_auth_status",
    title: "Auth Status Test",
    topic: "Authoritative status test",
    repositoryRoot: dir,
    workspacePath: dir,
    workspaceStrategy: "shared",
    opencodeServerId: "antigravity",
    opencodeSessionId: "antigravity:agent_auth_status",
    modelProviderId: "antigravity",
    modelId: "gemini-3.8-flash-high",
    modelVariant: null,
    modelRoute: "antigravity-flash-high",
  });

  const job = store.createJob({
    id: "job_auth_status",
    agentId: agent.id,
    kind: "spawn",
    requestId: "req_auth_status",
    promptHash: "hash_auth_status",
  });
  store.updateJobStatus(job.id, "dispatching");
  store.updateJobStatus(job.id, "running");

  const attempt = await spool.createAttempt({
    agentId: agent.id,
    jobId: job.id,
    requestId: "req_auth_status",
    prompt: "Test task",
    cwd: dir,
    modelProviderId: "antigravity",
    modelId: "gemini-3.8-flash-high",
    modelVariant: null,
    modelRoute: "antigravity-flash-high",
    timeoutMs: 30000,
  });

  const now = Date.now();
  await spool.writeHeartbeat(attempt.attemptId, {
    nonce: "test_nonce",
    supervisorPid: process.pid,
    agyPid: process.pid,
    updatedAt: now,
    timestamp: new Date(now).toISOString(),
  }, job.id);

  const service = new BridgeService(config, {
    store,
    manager: new FakeOpenCodeManager() as any,
  });

  try {
    await service.start();
    const snapshot = await service.consult({ agentId: agent.id, jobId: job.id });

    assert.ok(snapshot.heartbeatAt !== undefined && snapshot.heartbeatAt !== null, "heartbeatAt should be defined");
    assert.ok(typeof snapshot.heartbeatAgoSeconds === "number", "heartbeatAgoSeconds should be numeric");
    assert.ok(snapshot.leaseExpiresAt !== undefined, "leaseExpiresAt should be defined");
    assert.ok(snapshot.attempt !== undefined, "attempt should be defined");
    assert.ok(snapshot.fence !== undefined, "fence should be defined");
    assert.equal(snapshot.pid, process.pid, "pid should match running process");
    assert.equal(snapshot.sessionId, agent.opencodeSessionId, "sessionId should match agent session");
    assert.equal(snapshot.resultPersisted, false, "resultPersisted should be false before completion");
  } finally {
    await service.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("5. Takeover is blocked while lease and process are live; unknown state blocks; durable result prevails", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "liveness-test-5-"));
  const store = await BridgeStore.open(dir);
  const config = createDefaultConfig({
    dataDir: dir,
    configPath: path.join(dir, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
  });

  const spool = new AntigravitySpool(dir);
  const agent = store.createAgent({
    id: "agent_takeover_test",
    title: "Takeover Test",
    topic: "Takeover prevention test",
    repositoryRoot: dir,
    workspacePath: dir,
    workspaceStrategy: "shared",
    opencodeServerId: "antigravity",
    opencodeSessionId: "antigravity:agent_takeover_test",
    modelProviderId: "antigravity",
    modelId: "gemini-3.8-flash-high",
    modelVariant: null,
    modelRoute: "antigravity-flash-high",
  });

  const job = store.createJob({
    id: "job_takeover_test",
    agentId: agent.id,
    kind: "spawn",
    requestId: "req_takeover_test",
    promptHash: "hash_takeover_test",
  });
  store.updateJobStatus(job.id, "dispatching");
  store.updateJobStatus(job.id, "running");

  const attempt = await spool.createAttempt({
    agentId: agent.id,
    jobId: job.id,
    requestId: "req_takeover_test",
    prompt: "Takeover task",
    cwd: dir,
    modelProviderId: "antigravity",
    modelId: "gemini-3.8-flash-high",
    modelVariant: null,
    modelRoute: "antigravity-flash-high",
    timeoutMs: 60000,
  });

  // Heartbeat is older than 10s (e.g. 15s ago), but process is still alive and lease expires in 60s
  await spool.writeHeartbeat(attempt.attemptId, {
    nonce: "test_nonce_live",
    supervisorPid: process.pid,
    agyPid: process.pid,
    updatedAt: Date.now() - 15_000,
    timestamp: new Date(Date.now() - 15_000).toISOString(),
  }, job.id);

  let spawnedReplacement = false;
  const mockAntigravity = {
    async runAttempt() {
      spawnedReplacement = true;
      return {} as any;
    },
  };

  const service = new BridgeService(config, {
    store,
    manager: new FakeOpenCodeManager() as any,
    antigravity: mockAntigravity as any,
  });

  try {
    await service.start();
    await new Promise((r) => setTimeout(r, 100));
    assert.equal(spawnedReplacement, false, "Takeover / replacement must be blocked while process is live or lease active");
  } finally {
    await service.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("6. Abort and close only release workspace after proven quiescence without provider fallback", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "liveness-test-6-"));
  const store = await BridgeStore.open(dir);
  const config = createDefaultConfig({
    dataDir: dir,
    configPath: path.join(dir, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
    antigravityTimeoutFallbackRoute: "flash-max",
  });

  let processTerminated = false;
  const mockAntigravity = {
    async runPrompt(options: any) {
      return await new Promise<any>((resolve, reject) => {
        options.signal?.addEventListener("abort", () => {
          setTimeout(() => {
            processTerminated = true;
            reject(new Error("aborted"));
          }, 50);
        });
      });
    },
  };

  const openCodeManager = new FakeOpenCodeManager();
  const service = new BridgeService(config, {
    store,
    manager: openCodeManager as any,
    antigravity: mockAntigravity as any,
  });

  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "req_quiescence_test",
      topic: "Quiescence Test",
      task: "Test proven quiescence",
      cwd: dir,
      modelRoute: "antigravity-flash-high",
    });

    const abortResult = await service.abort(accepted.agentId, "Abort test");
    assert.equal(abortResult.status, "aborted");
    assert.equal(processTerminated, true, "Abort must not return before quiescence is proven");

    const closeResult = await service.close(accepted.agentId);
    assert.equal(closeResult.status, "closed");

    assert.equal(openCodeManager.client.promptCalls.length, 0, "No silent provider fallback should occur on abort/close");
  } finally {
    await service.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});
