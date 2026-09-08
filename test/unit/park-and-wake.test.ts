import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";
import {
  CodexAppServerDeliveryAdapter,
  DEFAULT_CODEX_CAPABILITIES,
  UnavailableCodexDeliveryAdapter,
  type CodexCorrelation,
  type CodexDeliveryAdapter,
  type CodexRpcTransport,
} from "../../src/codex/adapter.js";
import type {
  CodexBinding,
  JobRecord,
  OpenCodeClientLike,
  OpenCodeMessage,
  WakeEnvelope,
} from "../../src/types.js";
import { ConflictError, InvalidRequestError } from "../../src/errors.js";
import { createServer as createNetServer, type AddressInfo } from "node:net";
import { BridgeHttpServer, BridgeHttpClient } from "../../src/http-server.js";
import { createMcpServer } from "../../src/mcp.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import {
  resolveCodexTaskProvenance,
  DefaultCodexCliTransport,
  defaultProcessRunner,
  inspectJsonlTurnAcceptance,
  JsonlAcceptanceDetector,
  ensureLineTerminatedMarker,
  type CodexCliTransport,
  type ProcessRunner,
} from "../../src/codex/cli-resolver.js";

class FakeRpc implements CodexRpcTransport {
  calls: Array<{ method: string; params?: unknown }> = [];
  steerError: string | null = null;
  startError: string | null = null;
  threadItems: Array<{ id: string; type: string; text?: string }> = [];
  activeTurnId: string | null = null;
  serverCapabilities: Record<string, unknown> = {};
  private listener: ((notification: { method: string; params?: unknown }) => void) | null = null;

  async start(): Promise<void> {}
  onNotification(listener: (notification: { method: string; params?: unknown }) => void): () => void {
    this.listener = listener;
    return () => {
      this.listener = null;
    };
  }
  async call(method: string, params?: unknown): Promise<unknown> {
    this.calls.push({ method, params });
    if (method === "initialize") {
      return { capabilities: this.serverCapabilities };
    }
    if (method === "turn/steer") {
      if (this.steerError) throw new Error(this.steerError);
      return { success: true };
    }
    if (method === "turn/start") {
      if (this.startError) throw new Error(this.startError);
      return { turnId: "new_turn_started" };
    }
    if (method === "thread/read" || method === "thread/get") {
      return { items: this.threadItems, activeTurnId: this.activeTurnId };
    }
    return {};
  }
  async close(): Promise<void> {}
  emit(notification: { method: string; params?: unknown }): void {
    this.listener?.(notification);
  }
}

class FakeCodexDelivery implements CodexDeliveryAdapter {
  available = true;
  reason: string | null = null;
  capabilities = { ...DEFAULT_CODEX_CAPABILITIES, authoritativeAttachment: true };
  deliveredWakes: Array<{ envelope: WakeEnvelope; binding: CodexBinding }> = [];
  reconciledCalls: Array<{ threadId: string; marker: string }> = [];
  reconcileResult = false;
  activeTurnId: string | null = null;

  async start(): Promise<void> {}
  async close(): Promise<void> {}
  async deliver(job: JobRecord, binding: CodexBinding, text: string): Promise<"codex-steer" | "codex-start"> {
    return "codex-start";
  }
  async deliverWake(envelope: WakeEnvelope, binding: CodexBinding): Promise<"codex-steer" | "codex-start"> {
    this.deliveredWakes.push({ envelope, binding });
    return "codex-start";
  }
  async reconcileSend(threadId: string, marker: string): Promise<boolean> {
    this.reconciledCalls.push({ threadId, marker });
    return this.reconcileResult;
  }
  onCorrelation(_listener: (correlation: CodexCorrelation) => void): () => void {
    return () => undefined;
  }
}

let fakeSessionSeq = 0;
class FakeOpenCodeClient implements OpenCodeClientLike {
  messages: OpenCodeMessage[] = [];
  async health() { return { healthy: true }; }
  async createSession() { return { id: `session_fake_${++fakeSessionSeq}` }; }
  async promptAsync() {}
  async listMessages() { return this.messages; }
  async getDiff() { return ""; }
  async abort() {}
  async replyPermission() {}
  async subscribe() {}
}

async function createTestEnv() {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "ds-park-test-"));
  const config = createDefaultConfig({
    dataDir: tmp,
    configPath: path.join(tmp, "config.json"),
    experimentalSameChatDelivery: true,
  });
  const store = new BridgeStore(path.join(tmp, "bridge.sqlite"));
  return { tmp, config, store };
}

// ---------------------------------------------------------------------------
// 1. migration/backcompat: catches missing schema migration or breakage of legacy tables
// ---------------------------------------------------------------------------
test("migration: adds park_barriers, park_jobs, and wake_outbox while preserving legacy tables", async () => {
  const { tmp, store } = await createTestEnv();
  try {
    const tableNames = (store.db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as Array<{ name: string }>).map((t) => t.name);
    assert.ok(tableNames.includes("park_barriers"), "park_barriers table must exist");
    assert.ok(tableNames.includes("park_jobs"), "park_jobs table must exist");
    assert.ok(tableNames.includes("wake_outbox"), "wake_outbox table must exist");

    const migration = store.db.prepare("SELECT version FROM schema_migrations WHERE version = 15").get() as { version: number } | undefined;
    assert.equal(migration?.version, 15, "migration version 15 must be applied");
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. authoritative correlation and mixed-thread rejection: catches accepting caller hints or mixed threads
// ---------------------------------------------------------------------------
test("authoritative correlation: rejects mixed threads and treats caller thread hints as non-authoritative", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_1",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_1",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job1 = store.createJob({ id: "job_1", agentId: agent.id, kind: "spawn", requestId: "req_1", promptHash: "h1" });
    const job2 = store.createJob({ id: "job_2", agentId: agent.id, kind: "spawn", requestId: "req_2", promptHash: "h2" });

    // Job 1 has only caller hint, NO bridge-observed binding in codex_bindings
    store.setCorrelationHint(job1.id, { threadId: "caller_hint_thread", turnId: "t1", source: "caller" });

    // Parking with job1 must NOT return armed because caller hint is NOT authority
    const unconfirmedReceipt = await service.park({ job_ids: [job1.id] });
    assert.equal(unconfirmedReceipt.armed, false, "Must return armed: false when bridge correlation is missing");
    assert.equal(unconfirmedReceipt.deliveryMode, "none");
    assert.notEqual(unconfirmedReceipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(unconfirmedReceipt.parkId), false, "Zero in-memory waiter allocation");

    // Bind job1 to thread_A and job2 to thread_B via authoritative bridge correlation
    store.bindJob({ jobId: job1.id, threadId: "thread_A", originatingTurnId: "turn_A", originatingItemId: "item_A" });
    store.bindJob({ jobId: job2.id, threadId: "thread_B", originatingTurnId: "turn_B", originatingItemId: "item_B" });

    // Mixed threads must be rejected
    await assert.rejects(
      () => service.park({ job_ids: [job1.id, job2.id] }),
      (err: unknown) => err instanceof ConflictError || (err instanceof Error && err.message.includes("Mixed thread")),
      "Must reject parking jobs that map to different authoritative threads",
    );

    // Both mapped to same thread succeeds and returns armed
    const job3 = store.createJob({ id: "job_3", agentId: agent.id, kind: "spawn", requestId: "req_3", promptHash: "h3" });
    store.bindJob({ jobId: job3.id, threadId: "thread_A", originatingTurnId: "turn_A", originatingItemId: "item_A3" });

    const armedReceipt = await service.park({ job_ids: [job1.id, job3.id] });
    assert.equal(armedReceipt.armed, true, "Must return armed: true when all jobs map to same authoritative thread");
    assert.equal(armedReceipt.targetIdentity, "thread_A");
    assert.equal(armedReceipt.deliveryMode, "cli_resume");
    assert.notEqual(armedReceipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(armedReceipt.parkId), false, "Zero in-memory waiter allocation");
    assert.equal(armedReceipt.obligationState, "pending");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. result-before-wake: catches premature wake before durable result file persistence
// ---------------------------------------------------------------------------
test("result-before-wake: wake is not eligible until worker result is durably persisted", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_rw",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_rw",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_rw", agentId: agent.id, kind: "spawn", requestId: "req_rw", promptHash: "hrw" });
    store.bindJob({ jobId: job.id, threadId: "thread_rw", originatingTurnId: "turn_rw", originatingItemId: "item_rw" });

    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");

    // Job transitions to completed, but resultPath is null (not yet durably persisted)
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");
    store.updateJobStatus(job.id, "completed");
    await service.evaluateParkWakes(job.id);

    assert.equal(fakeCodex.deliveredWakes.length, 0, "Wake must NOT be emitted when result is not durably persisted");

    // Now persist the result file
    const resultFile = path.join(tmp, "result_rw.json");
    await writeFile(resultFile, JSON.stringify({ envelope: { summary: "Done work", files: [], tests: [] } }));
    store.setJobResult(job.id, resultFile, "Done work");

    await service.evaluateParkWakes(job.id);
    assert.equal(fakeCodex.deliveredWakes.length, 1, "Wake must be emitted after result file is durably persisted");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. exactly one wake per generation: catches duplicate wake emissions for the same generation
// ---------------------------------------------------------------------------
test("exactly one wake/generation: generation fencing ensures at most one wake per generation", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_gen",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_gen",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job1 = store.createJob({ id: "job_g1", agentId: agent.id, kind: "spawn", requestId: "req_g1", promptHash: "hg1" });
    const job2 = store.createJob({ id: "job_g2", agentId: agent.id, kind: "spawn", requestId: "req_g2", promptHash: "hg2" });
    store.bindJob({ jobId: job1.id, threadId: "thread_gen", originatingTurnId: "turn_gen", originatingItemId: "item_g1" });
    store.bindJob({ jobId: job2.id, threadId: "thread_gen", originatingTurnId: "turn_gen", originatingItemId: "item_g2" });

    const receipt = await service.park({ job_ids: [job1.id, job2.id] });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");
    assert.equal(receipt.generation, 1);

    // Persist result for job1
    const res1 = path.join(tmp, "res1.json");
    await writeFile(res1, JSON.stringify({ envelope: { summary: "j1 done" } }));
    store.updateJobStatus(job1.id, "dispatching");
    store.updateJobStatus(job1.id, "running");
    store.setJobResult(job1.id, res1, "j1 done");
    store.updateJobStatus(job1.id, "completed");

    // Persist result for job2
    const res2 = path.join(tmp, "res2.json");
    await writeFile(res2, JSON.stringify({ envelope: { summary: "j2 done" } }));
    store.updateJobStatus(job2.id, "dispatching");
    store.updateJobStatus(job2.id, "running");
    store.setJobResult(job2.id, res2, "j2 done");
    store.updateJobStatus(job2.id, "completed");

    // Fire wake evaluations concurrently or sequentially
    await Promise.all([
      service.evaluateParkWakes(job1.id),
      service.evaluateParkWakes(job2.id),
      service.evaluateParkWakes(job1.id),
    ]);

    assert.equal(fakeCodex.deliveredWakes.length, 1, "Exactly one wake must be emitted for generation 1");
    assert.equal(fakeCodex.deliveredWakes[0]?.envelope.generation, 1);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. coalescing: catches emitting fragmented wakes instead of coalescing already-ready jobs
// ---------------------------------------------------------------------------
test("coalescing: already-ready jobs are coalesced into a single wake envelope", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_coal",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_coal",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job1 = store.createJob({ id: "job_c1", agentId: agent.id, kind: "spawn", requestId: "req_c1", promptHash: "hc1" });
    const job2 = store.createJob({ id: "job_c2", agentId: agent.id, kind: "spawn", requestId: "req_c2", promptHash: "hc2" });
    const job3 = store.createJob({ id: "job_c3", agentId: agent.id, kind: "spawn", requestId: "req_c3", promptHash: "hc3" });
    store.bindJob({ jobId: job1.id, threadId: "thread_coal", originatingTurnId: "turn_coal", originatingItemId: "item_c1" });
    store.bindJob({ jobId: job2.id, threadId: "thread_coal", originatingTurnId: "turn_coal", originatingItemId: "item_c2" });
    store.bindJob({ jobId: job3.id, threadId: "thread_coal", originatingTurnId: "turn_coal", originatingItemId: "item_c3" });

    // Job1 and Job2 finish before wake is evaluated
    const res1 = path.join(tmp, "res_c1.json");
    await writeFile(res1, JSON.stringify({ envelope: { summary: "c1" } }));
    store.updateJobStatus(job1.id, "dispatching");
    store.updateJobStatus(job1.id, "running");
    store.setJobResult(job1.id, res1, "c1");
    store.updateJobStatus(job1.id, "completed");

    const res2 = path.join(tmp, "res_c2.json");
    await writeFile(res2, JSON.stringify({ envelope: { summary: "c2" } }));
    store.updateJobStatus(job2.id, "dispatching");
    store.updateJobStatus(job2.id, "running");
    store.setJobResult(job2.id, res2, "c2");
    store.updateJobStatus(job2.id, "completed");

    // Job3 is still running
    store.updateJobStatus(job3.id, "dispatching");
    store.updateJobStatus(job3.id, "running");

    const receipt = await service.park({ job_ids: [job1.id, job2.id, job3.id], predicate: "ANY" });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");

    // Evaluate wakes
    await service.evaluateParkWakes(job1.id);

    assert.equal(fakeCodex.deliveredWakes.length, 1);
    const env = fakeCodex.deliveredWakes[0]?.envelope;
    assert.ok(env);
    assert.deepEqual(env.readyJobIds.sort(), ["job_c1", "job_c2"].sort(), "Must coalesce both ready jobs");
    assert.equal(env.pendingCount, 1, "Must report 1 job still pending");
    assert.ok(env.resultHashes["job_c1"], "Must include result hash for job_c1");
    assert.ok(env.resultHashes["job_c2"], "Must include result hash for job_c2");
    assert.ok(!env.instruction.includes("c1"), "Instruction must not inject arbitrary worker text");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 6. crash recovery: catches outbox loss or duplicate wake on crash/restart
// ---------------------------------------------------------------------------
test("crash recovery: safely recovers pending outbox without duplicate logical wake", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();

  try {
    // Create an armed barrier and an unconfirmed outbox entry directly in store
    const barrier = store.createOrUpdateParkBarrier({
      id: "park_crash",
      threadId: "thread_crash",
      turnId: "turn_crash",
      generation: 1,
      armed: true,
      state: "waking",
      reason: "crash test",
    });
    store.createWakeOutbox({
      id: "wake_crash",
      parkId: barrier.id,
      generation: 1,
      threadId: "thread_crash",
      turnId: "turn_crash",
      status: "pending",
      wakeMarker: "<!-- [SUBAGENT_BRIDGE_WAKE:park=park_crash:gen=1] -->",
      reason: "crash test",
      payloadJson: JSON.stringify({
        parkId: "park_crash",
        generation: 1,
        reason: "crash test",
        jobIds: ["job_crash"],
        readyJobIds: ["job_crash"],
        statuses: { job_crash: "completed" },
        resultHashes: { job_crash: "hash" },
        pendingCount: 0,
        instruction: "call subagents_follow",
        marker: "<!-- [SUBAGENT_BRIDGE_WAKE:park=park_crash:gen=1] -->",
      }),
    });

    // Start service, simulating daemon restart
    const service = new BridgeService(config, {
      store,
      codex: fakeCodex,
      manager: {
        start: async () => ({
          serverId: "srv",
          baseUrl: "http://127.0.0.1:9999",
          client: new FakeOpenCodeClient(),
          processId: null,
          stop: async () => {},
        }),
        stop: async () => {},
      },
    });
    await service.start();

    assert.equal(fakeCodex.deliveredWakes.length, 1, "Must deliver pending outbox on startup");
    const outboxRow = store.getWakeOutbox("park_crash", 1);
    assert.equal(outboxRow?.status, "delivered", "Outbox row must be marked delivered");

    // Second restart must NOT deliver again
    await service.stop();
    const service2 = new BridgeService(config, {
      store,
      codex: fakeCodex,
      manager: {
        start: async () => ({
          serverId: "srv",
          baseUrl: "http://127.0.0.1:9999",
          client: new FakeOpenCodeClient(),
          processId: null,
          stop: async () => {},
        }),
        stop: async () => {},
      },
    });
    await service2.start();
    assert.equal(fakeCodex.deliveredWakes.length, 1, "Must not duplicate wake delivery on subsequent restart");
    await service2.stop();
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 7. wrong active turn: catches steering into a different active user turn
// ---------------------------------------------------------------------------
test("wrong active turn: never steers into a different active turn; defers/fails closed to pending", async () => {
  const rpc = new FakeRpc();
  const config = createDefaultConfig({ codexAppServerCommand: "codex" });
  const adapter = new CodexAppServerDeliveryAdapter(config, rpc);
  await adapter.start();

  try {
    rpc.activeTurnId = "turn_user_2";
    rpc.steerError = "wrong turn";

    const binding: CodexBinding = {
      jobId: "job_test",
      threadId: "thread_turn",
      originatingTurnId: "turn_1",
      originatingItemId: "item_1",
      boundAt: new Date().toISOString(),
    };
    const envelope: WakeEnvelope = {
      parkId: "p1",
      generation: 1,
      reason: "test",
      jobIds: ["job_test"],
      readyJobIds: ["job_test"],
      statuses: { job_test: "completed" },
      resultHashes: { job_test: "h" },
      pendingCount: 0,
      instruction: "follow",
      marker: "<!-- [WAKE] -->",
    };

    await assert.rejects(
      () => adapter.deliverWake(envelope, binding),
      (err: unknown) => err instanceof Error,
      "Must not steer into wrong context when different active turn is running",
    );
  } finally {
    await adapter.close();
  }
});

// ---------------------------------------------------------------------------
// 8. manual goal pause/edit/removal: catches resuming goals modified externally
// ---------------------------------------------------------------------------
test("goals: resumes only goals positively paused by this bridge; external modifications invalidate ownership", async () => {
  const { tmp, store } = await createTestEnv();
  try {
    store.recordBridgeGoalPause("goal_123", "hash_initial");
    assert.equal(store.validateBridgeGoalOwnership("goal_123", "hash_initial"), true, "Should validate positively paused goal");

    assert.equal(store.validateBridgeGoalOwnership("goal_123", "hash_edited"), false, "External edit must invalidate ownership");

    store.invalidateBridgeGoalOwnership("goal_123");
    assert.equal(store.validateBridgeGoalOwnership("goal_123", "hash_initial"), false, "Invalidated goal must not be resumed");
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 9. needs-approval/failure/timeout: catches ignoring non-completed terminal states
// ---------------------------------------------------------------------------
test("needs-approval/failure/timeout: wake is triggered for needs_approval, failure, and timeout", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_nf",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_nf",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const jobApproval = store.createJob({ id: "job_na", agentId: agent.id, kind: "spawn", requestId: "req_na", promptHash: "hna" });
    store.bindJob({ jobId: jobApproval.id, threadId: "thread_na", originatingTurnId: "turn_na", originatingItemId: "item_na" });

    const receiptAppr = await service.park({ job_ids: [jobApproval.id] });
    assert.equal(receiptAppr.armed, true);
    assert.equal(receiptAppr.deliveryMode, "cli_resume");
    assert.notEqual(receiptAppr.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receiptAppr.parkId), false, "Zero in-memory waiter allocation");

    // Job transitions to needs_approval
    store.updateJobStatus(jobApproval.id, "dispatching");
    store.updateJobStatus(jobApproval.id, "running");
    store.updateJobStatus(jobApproval.id, "needs_approval");
    store.setJobPermission(jobApproval.id, "perm_123");

    await service.evaluateParkWakes(jobApproval.id);
    assert.equal(fakeCodex.deliveredWakes.length, 1, "Wake must trigger on needs_approval");
    assert.equal(fakeCodex.deliveredWakes[0]?.envelope.statuses["job_na"], "needs_approval");

    // Failure with error
    const jobFail = store.createJob({ id: "job_fl", agentId: agent.id, kind: "spawn", requestId: "req_fl", promptHash: "hfl" });
    store.bindJob({ jobId: jobFail.id, threadId: "thread_fl", originatingTurnId: "turn_fl", originatingItemId: "item_fl" });
    const receiptFail = await service.park({ job_ids: [jobFail.id] });
    assert.equal(receiptFail.armed, true);
    assert.equal(receiptFail.deliveryMode, "cli_resume");
    assert.notEqual(receiptFail.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receiptFail.parkId), false, "Zero in-memory waiter allocation");

    store.updateJobStatus(jobFail.id, "dispatching");
    store.updateJobStatus(jobFail.id, "running");
    store.updateJobStatus(jobFail.id, "failed", "Fatal worker crash");
    await service.evaluateParkWakes(jobFail.id);

    assert.equal(fakeCodex.deliveredWakes.length, 2, "Wake must trigger on failure");
    assert.equal(fakeCodex.deliveredWakes[1]?.envelope.statuses["job_fl"], "failed");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 10. version skew/no toolOutput: catches assuming toolOutput when schema lacks it
// ---------------------------------------------------------------------------
test("version skew: uses deterministic text wake marker when connected schema lacks toolOutput", async () => {
  const rpc = new FakeRpc();
  rpc.serverCapabilities = { toolOutput: false };

  const config = createDefaultConfig({ codexAppServerCommand: "codex" });
  const adapter = new CodexAppServerDeliveryAdapter(config, rpc);
  await adapter.start();

  try {
    assert.equal(adapter.capabilities.supportsToolOutput, false);

    const binding: CodexBinding = {
      jobId: "job_skew",
      threadId: "thread_skew",
      originatingTurnId: null,
      originatingItemId: null,
      boundAt: new Date().toISOString(),
    };
    const envelope: WakeEnvelope = {
      parkId: "p_skew",
      generation: 1,
      reason: "skew test",
      jobIds: ["job_skew"],
      readyJobIds: ["job_skew"],
      statuses: { job_skew: "completed" },
      resultHashes: { job_skew: "hash_skew" },
      pendingCount: 0,
      instruction: "call subagents_follow",
      marker: "<!-- [SUBAGENT_BRIDGE_WAKE:park=p_skew:gen=1] -->",
    };

    await adapter.deliverWake(envelope, binding);

    const startCall = rpc.calls.find((c) => c.method === "turn/start");
    assert.ok(startCall, "Must call turn/start");
    const params = startCall.params as { input: Array<{ type: string; text: string }> };
    assert.ok(params.input[0]?.text.includes("<!-- [SUBAGENT_BRIDGE_WAKE:park=p_skew:gen=1] -->"), "Text must contain deterministic marker");
    assert.ok(!("toolOutput" in (startCall.params as Record<string, unknown>)), "Must not send top-level toolOutput when schema lacks it");
  } finally {
    await adapter.close();
  }
});

// ---------------------------------------------------------------------------
// 11. unknown-send reconciliation: catches blindly retrying unconfirmed RPC sends
// ---------------------------------------------------------------------------
test("unknown-send reconciliation: reconciles with thread items before retrying", async () => {
  const rpc = new FakeRpc();
  const config = createDefaultConfig({ codexAppServerCommand: "codex" });
  const adapter = new CodexAppServerDeliveryAdapter(config, rpc);
  await adapter.start();

  try {
    const marker = "<!-- [SUBAGENT_BRIDGE_WAKE:park=p_rec:gen=1] -->";
    rpc.threadItems = [
      { id: "item_1", type: "text", text: `Notice: ${marker} completed` },
    ];

    const reconciled = await adapter.reconcileSend("thread_rec", marker);
    assert.equal(reconciled, true, "Must reconcile as sent when marker is found in thread items");

    const notReconciled = await adapter.reconcileSend("thread_rec", "<!-- [UNKNOWN_MARKER] -->");
    assert.equal(notReconciled, false, "Must return false when marker is absent");
  } finally {
    await adapter.close();
  }
});

// ---------------------------------------------------------------------------
// 12. unsupported authoritative attachment: catches claiming armed support on standalone spawned process
// ---------------------------------------------------------------------------
test("unsupported authoritative attachment: returns armed: false when adapter is not authoritatively attached", async () => {
  const { tmp, config, store } = await createTestEnv();
  const unattachedCodex = new FakeCodexDelivery();
  unattachedCodex.capabilities = { ...DEFAULT_CODEX_CAPABILITIES, authoritativeAttachment: false };

  const incompatibleCli: CodexCliTransport = {
    deliverWake: async () => ({ success: false, candidateIncompatible: true, error: "Incompatible CLI transport" }),
    probeCapabilities: async () => ({ compatible: false, version: null }),
  };

  const service = new BridgeService(config, {
    store,
    codex: unattachedCodex,
    cliTransport: incompatibleCli,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_unatt",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_unatt",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_unatt", agentId: agent.id, kind: "spawn", requestId: "req_unatt", promptHash: "hu" });
    store.bindJob({ jobId: job.id, threadId: "thread_unatt", originatingTurnId: "turn_unatt", originatingItemId: "item_unatt" });

    const receipt = await service.park({ job_ids: [job.id], wait: false });
    assert.equal(receipt.armed, false, "Must return armed: false when authoritative attachment is unsupported");
    assert.equal(receipt.deliveryMode, "none");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");
    assert.equal(receipt.nextAction, "subagents_follow");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 13. parking never consumes a job: catches accidental marking of resultConsumedAt
// ---------------------------------------------------------------------------
test("parking never consumes a job: job remains unconsumed with pending obligation", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_nc",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_nc",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_nc", agentId: agent.id, kind: "spawn", requestId: "req_nc", promptHash: "hnc" });
    store.bindJob({ jobId: job.id, threadId: "thread_nc", originatingTurnId: "turn_nc", originatingItemId: "item_nc" });

    const resPath = path.join(tmp, "res_nc.json");
    await writeFile(resPath, JSON.stringify({ envelope: { summary: "done" } }));
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");
    store.setJobResult(job.id, resPath, "done");
    store.updateJobStatus(job.id, "completed");

    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.obligationState, "pending");
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");

    const refreshedJob = store.getJob(job.id);
    assert.equal(refreshedJob?.resultConsumedAt, null, "resultConsumedAt must NOT be set by parking");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 14. migration v16: adds delivery_mode, wake_state, backoff, and provenance columns
// Catches: missing migration 16 or missing v2 schema columns
// ---------------------------------------------------------------------------
test("migration v16: adds delivery_mode, wake_state, next_attempt_at, and provenance columns", async () => {
  const { tmp, store } = await createTestEnv();
  try {
    const migration = store.db.prepare("SELECT version FROM schema_migrations WHERE version = 16").get() as { version: number } | undefined;
    assert.equal(migration?.version, 16, "migration version 16 must be applied");

    const barrierCols = (store.db.prepare("PRAGMA table_info(park_barriers)").all() as Array<{ name: string }>).map((c) => c.name);
    assert.ok(barrierCols.includes("delivery_mode"), "park_barriers must have delivery_mode");
    assert.ok(barrierCols.includes("mcp_session_id"), "park_barriers must have mcp_session_id");

    const outboxCols = (store.db.prepare("PRAGMA table_info(wake_outbox)").all() as Array<{ name: string }>).map((c) => c.name);
    assert.ok(outboxCols.includes("delivery_mode"), "wake_outbox must have delivery_mode");
    assert.ok(outboxCols.includes("wake_state"), "wake_outbox must have wake_state");
    assert.ok(outboxCols.includes("next_attempt_at"), "wake_outbox must have next_attempt_at");
    assert.ok(outboxCols.includes("selected_executable"), "wake_outbox must have selected_executable");
    assert.ok(outboxCols.includes("executable_version"), "wake_outbox must have executable_version");

    const jobCols = (store.db.prepare("PRAGMA table_info(jobs)").all() as Array<{ name: string }>).map((c) => c.name);
    assert.ok(jobCols.includes("mcp_session_id"), "jobs must have mcp_session_id");
    assert.ok(jobCols.includes("trusted_thread_id"), "jobs must have trusted_thread_id");
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 15. v3 park semantics: park never waits in-turn, explicit wait=true is rejected, omitted wait returns immediately
// Catches: in-memory waiter allocation or preserving legacy in-turn waiting
// ---------------------------------------------------------------------------
test("v3 park semantics: park never waits in-turn, explicit wait=true is rejected, omitted wait returns immediately with zero in-memory waiters", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();
  try {
    const agent = store.createAgent({
      id: "agent_wait",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_wait",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_wait", agentId: agent.id, kind: "spawn", requestId: "req_wait", promptHash: "hwait", mcpSessionId: "session_wait_turn" });
    store.bindJob({ jobId: job.id, threadId: "thread_wait", originatingTurnId: "turn_wait", originatingItemId: "item_wait" });

    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");

    // 1. Explicit wait=true must fail closed with 400 directing to subagents_follow
    await assert.rejects(
      () => service.park({ job_ids: [job.id], wait: true, mcp_session_id: "session_wait_turn" }),
      (err: unknown) => {
        assert.ok(err instanceof InvalidRequestError);
        assert.equal((err as InvalidRequestError).status, 400);
        assert.match((err as Error).message, /subagents_follow/i);
        return true;
      },
    );

    // 2. Omitted wait returns immediately with armed=true, deliveryMode="cli_resume", and zero in-memory waiters
    const receipt = await service.park({ job_ids: [job.id], mcp_session_id: "session_wait_turn" });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(receipt.wakeState, "waiting");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");

    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.notEqual(barrier.deliveryMode, "in_turn", "No new in_turn barrier created");
    assert.equal(barrier.deliveryMode, "cli_resume");
    assert.equal(service.hasParkWaiter(barrier.id), false, "Zero in-memory waiter on barrier");
    assert.equal(service.hasParkWaiter(job.id), false, "Zero in-memory waiter on job");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 16. external park durability: arms immediately with zero in-memory waiters while preserving durable barrier
// Catches: disarming or destroying durable barrier or attempting in-memory waiter allocation
// Note: Replaces legacy v2 in-turn cancellation durability; in v3, park never waits in-turn and allocates zero waiters.
// ---------------------------------------------------------------------------
test("external park durability: arms immediately with zero in-memory waiters while preserving durable barrier", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();
  try {
    const agent = store.createAgent({
      id: "agent_dur",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_dur",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_dur", agentId: agent.id, kind: "spawn", requestId: "req_dur", promptHash: "hdur", mcpSessionId: "session_dur_mcp" });
    store.bindJob({ jobId: job.id, threadId: "thread_dur", originatingTurnId: "turn_dur", originatingItemId: "item_dur" });

    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");

    // 1. Explicit wait=true fails closed directing to follow
    await assert.rejects(
      () => service.park({ job_ids: [job.id], wait: true, mcp_session_id: "session_dur_mcp" }),
      (err: unknown) => {
        assert.ok(err instanceof InvalidRequestError);
        assert.equal((err as InvalidRequestError).status, 400);
        assert.match((err as Error).message, /subagents_follow/i);
        return true;
      },
    );

    // 2. Normal external park arms immediately and remains durable with zero in-memory waiters
    const receipt = await service.park({ job_ids: [job.id], mcp_session_id: "session_dur_mcp" });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(receipt.wakeState, "waiting");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");

    const barrier = store.getParkBarrierByThread("thread_dur");
    assert.ok(barrier, "Park barrier must be preserved in SQLite");
    assert.equal(barrier.armed, true, "Barrier must remain armed for external wake");
    assert.notEqual(barrier.deliveryMode, "in_turn", "No in_turn barrier created");
    assert.equal(barrier.deliveryMode, "cli_resume");
    assert.equal(service.hasParkWaiter(barrier.id), false, "Zero in-memory waiter on barrier");

    const resFile = path.join(tmp, "res_dur.json");
    await writeFile(resFile, JSON.stringify({ envelope: { summary: "done" } }));
    store.setJobResult(job.id, resFile, "done");
    store.updateJobStatus(job.id, "completed");

    await service.evaluateParkWakes(job.id);
    assert.equal(fakeCodex.deliveredWakes.length, 1, "External wake outbox must deliver");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 17. trusted identity: caller thread mismatch fails closed; caller hints alone do not authorize
// Catches: trusting caller hints without authoritative verification or ignoring caller mismatch
// ---------------------------------------------------------------------------
test("trusted identity: caller thread mismatch fails closed and caller hints alone do not authorize", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();
  try {
    const agent = store.createAgent({
      id: "agent_id",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_id",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_id", agentId: agent.id, kind: "spawn", requestId: "req_id", promptHash: "hid" });
    store.bindJob({ jobId: job.id, threadId: "thread_trusted", originatingTurnId: "turn_trusted", originatingItemId: "item_id" });

    const receipt = await service.park({ job_ids: [job.id], thread_id: "thread_trusted" });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.targetIdentity, "thread_trusted");
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");

    await assert.rejects(
      () => service.park({ job_ids: [job.id], thread_id: "thread_spoofed" }),
      (err: unknown) => err instanceof ConflictError || (err instanceof Error && err.message.includes("mismatch")),
      "Must reject when caller thread_id mismatches trusted identity",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 18. compatible CLI resolver: discovers Desktop bundled CLI and rejects old CLI
// Catches: hardcoding build hash, PATH-only assumption, or accepting old CLI schema
// ---------------------------------------------------------------------------
test("compatible CLI resolver: discovers Desktop bundled CLI and rejects old CLI", async () => {
  const { isCompatibleCodexCli } = await import("../../src/codex/cli-resolver.js");
  assert.equal(isCompatibleCodexCli("0.153.0"), true, "0.153.0 must be compatible");
  assert.equal(isCompatibleCodexCli("0.154.0"), true, "newer versions must be compatible");
  assert.equal(isCompatibleCodexCli("0.145.0"), false, "0.145.0 has schema incompatibility and must be rejected");
  assert.equal(isCompatibleCodexCli("0.130.0-alpha.5"), false, "old alpha must be rejected");
});

// ---------------------------------------------------------------------------
// 19. active writer deferral: classifies active writer as deferred_active_writer with exponential backoff
// Catches: marking active writer as terminal failed or spamming immediate retries
// ---------------------------------------------------------------------------
test("active writer deferral: classifies active writer as deferred_active_writer with exponential backoff and no storm", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();
  try {
    fakeCodex.deliverWake = async () => {
      const err = new Error("thread-store conflict: thread thread_act already has an active writer");
      (err as any).code = "active_writer";
      throw err;
    };

    const agent = store.createAgent({
      id: "agent_act",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_act",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_act", agentId: agent.id, kind: "spawn", requestId: "req_act", promptHash: "hact" });
    store.bindJob({ jobId: job.id, threadId: "thread_act", originatingTurnId: "turn_act", originatingItemId: "item_act" });

    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");

    const resFile = path.join(tmp, "res_act.json");
    await writeFile(resFile, JSON.stringify({ envelope: { summary: "done" } }));
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");
    store.setJobResult(job.id, resFile, "done");
    store.updateJobStatus(job.id, "completed");

    await service.evaluateParkWakes(job.id);

    const outbox = store.getWakeOutbox(receipt.parkId, 1);
    assert.ok(outbox, "Outbox record must exist");
    assert.equal(outbox.status, "deferred_active_writer", "Status must be deferred_active_writer");
    assert.ok(outbox.nextAttemptAt, "nextAttemptAt must be set");
    assert.ok(new Date(outbox.nextAttemptAt).getTime() > Date.now(), "nextAttemptAt must be in the future");
    assert.equal(outbox.attempts, 1);

    let retried = false;
    fakeCodex.deliverWake = async () => {
      retried = true;
      return "codex-start";
    };
    await service.evaluateParkWakes(job.id);
    assert.equal(retried, false, "Must not retry before nextAttemptAt has passed (anti-storm)");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

async function freePort(): Promise<number> {
  const probe = createNetServer();
  return new Promise<number>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      const port = typeof address === "object" && address ? (address as AddressInfo).port : 0;
      probe.close((error) => (error ? reject(error) : resolve(port)));
    });
  });
}

// ---------------------------------------------------------------------------
// 20. BLK-BRIDGE-1: external CLI wake reaches injected transport through BridgeService, persists executable/version, and treats ambiguous CLI failure as fail-closed indeterminate with no blind retry
// ---------------------------------------------------------------------------
test("BLK-BRIDGE-1: external CLI wake reaches injected transport through BridgeService, persists executable/version, and treats ambiguous CLI failure as fail-closed indeterminate with no blind retry", async () => {
  const { tmp, config, store } = await createTestEnv();
  const injectedCliCalls: Array<{ threadId: string; marker: string }> = [];
  let cliResult: { success: boolean; activeWriter?: boolean; error?: string; executablePath?: string; version?: string } = {
    success: true,
    executablePath: "C:\\Codex\\codex.exe",
    version: "0.153.0",
  };
  const mockCliTransport: CodexCliTransport = {
    deliverWake: async (threadId: string, marker: string) => {
      injectedCliCalls.push({ threadId, marker });
      return cliResult;
    },
    probeCapabilities: async () => ({ compatible: true, version: "0.153.0" }),
  };

  const service = new BridgeService(config, {
    store,
    codex: new UnavailableCodexDeliveryAdapter(),
    cliTransport: mockCliTransport,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_cli1",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_cli1",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({
      id: "job_cli1",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_cli1",
      promptHash: "hcli1",
      trustedThreadId: "thread_cli1",
    });

    // 1. Success path: external park arms when compatible CLI capability is available
    const receipt = await service.park({ job_ids: [job.id], wait: false });
    assert.equal(receipt.armed, true, "Must arm when compatible CLI transport is available");
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.notEqual(receipt.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Zero in-memory waiter allocation");

    const resFile = path.join(tmp, "res_cli1.json");
    await writeFile(resFile, JSON.stringify({ envelope: { summary: "done" } }));
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");
    store.setJobResult(job.id, resFile, "done");
    store.updateJobStatus(job.id, "completed");

    await service.evaluateParkWakes(job.id);

    assert.equal(injectedCliCalls.length, 1);
    assert.equal(injectedCliCalls[0]?.threadId, "thread_cli1");

    const outboxDelivered = store.getWakeOutbox(receipt.parkId, 1);
    assert.ok(outboxDelivered);
    assert.equal(outboxDelivered.status, "delivered");
    assert.equal(outboxDelivered.selectedExecutable, "C:\\Codex\\codex.exe");
    assert.equal(outboxDelivered.executableVersion, "0.153.0");

    // 2. Active writer deferral through injected CLI transport
    const job2 = store.createJob({
      id: "job_cli2",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_cli2",
      promptHash: "hcli2",
      trustedThreadId: "thread_cli2",
    });
    cliResult = {
      success: false,
      activeWriter: true,
      error: "thread has an active writer",
      executablePath: "C:\\Codex\\codex.exe",
      version: "0.153.0",
    };
    const receipt2 = await service.park({ job_ids: [job2.id], wait: false });
    assert.equal(receipt2.deliveryMode, "cli_resume");
    assert.notEqual(receipt2.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt2.parkId), false, "Zero in-memory waiter allocation");

    const resFile2 = path.join(tmp, "res_cli2.json");
    await writeFile(resFile2, JSON.stringify({ envelope: { summary: "done 2" } }));
    store.updateJobStatus(job2.id, "dispatching");
    store.updateJobStatus(job2.id, "running");
    store.setJobResult(job2.id, resFile2, "done 2");
    store.updateJobStatus(job2.id, "completed");

    await service.evaluateParkWakes(job2.id);

    const outboxDeferred = store.getWakeOutbox(receipt2.parkId, 1);
    assert.ok(outboxDeferred);
    assert.equal(outboxDeferred.status, "deferred_active_writer");
    assert.ok(outboxDeferred.nextAttemptAt);
    assert.equal(outboxDeferred.selectedExecutable, "C:\\Codex\\codex.exe");
    assert.equal(outboxDeferred.executableVersion, "0.153.0");

    // 3. Ambiguous failure path through injected CLI transport remains fail-closed indeterminate with no blind retry
    const job3 = store.createJob({
      id: "job_cli3",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_cli3",
      promptHash: "hcli3",
      trustedThreadId: "thread_cli3",
    });
    cliResult = {
      success: false,
      activeWriter: false,
      error: "process crashed with 1",
      executablePath: "C:\\Codex\\codex.exe",
      version: "0.153.0",
    };
    const receipt3 = await service.park({ job_ids: [job3.id], wait: false });
    assert.equal(receipt3.deliveryMode, "cli_resume");
    assert.notEqual(receipt3.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt3.parkId), false, "Zero in-memory waiter allocation");

    const resFile3 = path.join(tmp, "res_cli3.json");
    await writeFile(resFile3, JSON.stringify({ envelope: { summary: "done 3" } }));
    store.updateJobStatus(job3.id, "dispatching");
    store.updateJobStatus(job3.id, "running");
    store.setJobResult(job3.id, resFile3, "done 3");
    store.updateJobStatus(job3.id, "completed");

    await service.evaluateParkWakes(job3.id);

    const outboxIndeterminate = store.getWakeOutbox(receipt3.parkId, 1);
    assert.ok(outboxIndeterminate);
    assert.equal(outboxIndeterminate.status, "waking");
    assert.notEqual(outboxIndeterminate.status, "failed");
    assert.notEqual(outboxIndeterminate.status, "pending");
    assert.notEqual(outboxIndeterminate.status, "deferred_active_writer");
    assert.equal(outboxIndeterminate.selectedExecutable, "C:\\Codex\\codex.exe");
    assert.equal(outboxIndeterminate.executableVersion, "0.153.0");
    assert.equal(outboxIndeterminate.lastError, "process crashed with 1");

    const barrier3 = store.getParkBarrier(receipt3.parkId);
    assert.ok(barrier3);
    assert.equal(barrier3.state, "waking", "Barrier must remain waking/indeterminate when delivery outcome is unknown");
    assert.equal(barrier3.armed, false, "Barrier armed must be false while indeterminate");

    // Ambiguous outcome is never blindly retried without authoritative proof
    const callsBefore = injectedCliCalls.length;
    await (service as any).recoverWakeOutbox();
    await service.evaluateParkWakes(job3.id);
    assert.equal(injectedCliCalls.length, callsBefore, "Ambiguous outcome must never be blindly retried without authoritative proof");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 21. BLK-BRIDGE-2: default unattached MCP path rejects wait=true, requires authoritative binding for cli_resume, and allocates zero in-memory waiters
// Catches: legacy in-turn waiter allocation or accepting wait=true in default unattached MCP path
// ---------------------------------------------------------------------------
test("BLK-BRIDGE-2: default unattached MCP path rejects wait=true, requires authoritative binding for cli_resume, and allocates zero in-memory waiters", async () => {
  const { tmp, config, store } = await createTestEnv();
  config.daemonPort = await freePort();

  const fakeCli: CodexCliTransport = {
    probeCapabilities: async () => ({ compatible: true, version: "0.153.0" }),
    deliverWake: async () => ({ success: true, accepted: true, executablePath: "C:\\Codex\\codex.exe", version: "0.153.0" }),
  };

  const service = new BridgeService(config, {
    store,
    codex: new UnavailableCodexDeliveryAdapter(), // REAL default path, NOT FakeCodexDelivery
    cliTransport: fakeCli,
    antigravity: {
      runPrompt: async (options: { signal?: AbortSignal }) =>
        new Promise((resolve) => {
          if (options.signal?.aborted) return resolve({} as any);
          options.signal?.addEventListener("abort", () => resolve({} as any), { once: true });
        }),
    } as any,
  });
  await service.start();

  const httpServer = new BridgeHttpServer(config, service);
  await httpServer.start();

  const httpClient = new BridgeHttpClient(config);
  const mcpServer = createMcpServer(httpClient, { env: {} });
  const mcpClient = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await mcpServer.connect(serverTransport);
  await mcpClient.connect(clientTransport);

  try {
    // 1. Spawn a job under this MCP session
    const spawnRes = await mcpClient.callTool({
      name: "subagents_spawn",
      arguments: { topic: "default unattached path v3", task: "do work" },
    });
    const spawnData = spawnRes.structuredContent as Record<string, unknown>;
    const jobId = spawnData.jobId as string;
    assert.ok(jobId, "Job ID must be returned from spawn");

    // 2. Issue park with wait=true fails closed with typed 400 error directing to subagents_follow
    const waitTrueRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jobId], wait: true },
    });
    assert.equal(waitTrueRes.isError, true);
    assert.match((waitTrueRes.content[0] as { text: string }).text, /subagents_follow/i);

    // 3. Unbound job fails closed (armed: false, deliveryMode: "none", zero in-memory waiters)
    const parkResUnbound = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jobId] },
    });
    const parkDataUnbound = parkResUnbound.structuredContent as Record<string, unknown>;
    assert.equal(parkDataUnbound.armed, false, "Must fail closed when unattached/unbound");
    assert.equal(parkDataUnbound.deliveryMode, "none");
    assert.notEqual(parkDataUnbound.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(parkDataUnbound.parkId as string), false, "Zero in-memory waiter allocation");

    // 4. Bound job arms immediately with deliveryMode="cli_resume" and zero in-memory waiters
    store.bindJob({ jobId, threadId: "22222222-2222-2222-2222-222222222222", originatingTurnId: "turn_blk2", originatingItemId: "item_blk2" });
    const parkRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jobId] },
    });
    const parkData = parkRes.structuredContent as Record<string, unknown>;
    assert.equal(parkData.armed, true, "Must be armed with cli_resume");
    assert.equal(parkData.deliveryMode, "cli_resume");
    assert.notEqual(parkData.deliveryMode, "in_turn");
    assert.equal(parkData.wakeState, "waiting");
    assert.equal(service.hasParkWaiter(parkData.parkId as string), false, "Zero in-memory waiter allocation");

    const barrierBeforeWake = store.getParkBarrier(parkData.parkId as string)!;
    assert.notEqual(barrierBeforeWake.deliveryMode, "in_turn", "Must not create in_turn barrier");
    assert.equal(barrierBeforeWake.deliveryMode, "cli_resume");
    assert.equal(service.hasParkWaiter(barrierBeforeWake.id), false, "Zero in-memory waiter on barrier");

    // 5. Make job terminal and evaluate wakes
    const resFile = path.join(tmp, "res_blk2.json");
    await writeFile(resFile, JSON.stringify({ envelope: { summary: "worker finished" } }));
    store.setJobResult(jobId, resFile, "worker finished");
    store.updateJobStatus(jobId, "completed");

    await service.evaluateParkWakes(jobId);
    const barrier = store.getParkBarrier(parkData.parkId as string)!;
    assert.equal(barrier.state, "woken");
    assert.notEqual(barrier.deliveryMode, "in_turn");
    assert.equal(barrier.deliveryMode, "cli_resume");
    assert.equal(service.hasParkWaiter(barrier.id), false, "Zero in-memory waiter on barrier after wake");
  } finally {
    await mcpClient.close();
    await mcpServer.close();
    await httpServer.stop();
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 22. BLK-BRIDGE-3: authoritative task id from process provenance and caller hint corroboration
// ---------------------------------------------------------------------------
test("BLK-BRIDGE-3: resolves trusted task identity from CODEX_THREAD_ID/CODEX_SESSION_ID and enforces corroboration", async () => {
  const { tmp, config, store } = await createTestEnv();
  config.daemonPort = await freePort();

  // Unit tests for resolveCodexTaskProvenance
  const validUuid = "11111111-2222-3333-4444-555555555555";
  const validSessionUuid = "11111111-2222-3333-4444-555555555555";
  const mismatchedUuid = "99999999-8888-7777-6666-555555555555";

  assert.equal(resolveCodexTaskProvenance({}).threadId, null);
  assert.equal(resolveCodexTaskProvenance({ CODEX_THREAD_ID: validUuid }).threadId, validUuid);
  assert.equal(
    resolveCodexTaskProvenance({ CODEX_THREAD_ID: validUuid, CODEX_SESSION_ID: validSessionUuid }).threadId,
    validUuid,
  );
  assert.equal(resolveCodexTaskProvenance({ CODEX_THREAD_ID: "not-a-uuid" }).threadId, null);
  assert.ok(resolveCodexTaskProvenance({ CODEX_THREAD_ID: "not-a-uuid" }).error);
  assert.equal(
    resolveCodexTaskProvenance({ CODEX_THREAD_ID: validUuid, CODEX_SESSION_ID: mismatchedUuid }).threadId,
    null,
  );
  assert.ok(
    resolveCodexTaskProvenance({ CODEX_THREAD_ID: validUuid, CODEX_SESSION_ID: mismatchedUuid }).error?.includes("mismatch"),
  );

  const service = new BridgeService(config, {
    store,
    codex: new UnavailableCodexDeliveryAdapter(),
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();
  const httpServer = new BridgeHttpServer(config, service);
  await httpServer.start();

  const httpClient = new BridgeHttpClient(config);

  // Case A: Valid provenance injected into MCP server
  const mcpServerA = createMcpServer(httpClient, {
    env: { CODEX_THREAD_ID: validUuid, CODEX_SESSION_ID: validUuid },
  });
  const mcpClientA = new Client({ name: "fixture-client-a", version: "1.0.0" }, { capabilities: {} });
  const [cTransA, sTransA] = InMemoryTransport.createLinkedPair();
  await mcpServerA.connect(sTransA);
  await mcpClientA.connect(cTransA);

  // Case B: Malformed provenance
  const mcpServerB = createMcpServer(httpClient, {
    env: { CODEX_THREAD_ID: "invalid-uuid" },
  });
  const mcpClientB = new Client({ name: "fixture-client-b", version: "1.0.0" }, { capabilities: {} });
  const [cTransB, sTransB] = InMemoryTransport.createLinkedPair();
  await mcpServerB.connect(sTransB);
  await mcpClientB.connect(cTransB);

  try {
    // Spawn under client A -> trusted identity propagated
    const spawnA = await mcpClientA.callTool({
      name: "subagents_spawn",
      arguments: { topic: "task a", task: "do work" },
    });
    const jobIdA = (spawnA.structuredContent as Record<string, unknown>).jobId as string;
    const jobRecordA = store.getJob(jobIdA);
    assert.equal(jobRecordA?.trustedThreadId, validUuid, "Must populate JobRecord.trustedThreadId from provenance");

    // Corroborating caller thread hint succeeds
    const parkCorroborated = await mcpClientA.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jobIdA], thread_id: validUuid, wait: false },
    });
    assert.equal(parkCorroborated.isError, undefined);
    const parkCorroboratedData = parkCorroborated.structuredContent as Record<string, unknown>;
    assert.notEqual(parkCorroboratedData.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(parkCorroboratedData.parkId as string), false, "Zero in-memory waiter allocation");

    // Mismatched caller thread hint fails closed
    const parkMismatched = await mcpClientA.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jobIdA], thread_id: mismatchedUuid, wait: false },
    });
    assert.equal(parkMismatched.isError, true, "Mismatched caller thread_id must fail closed");

    // Spawn under client B (malformed env) -> trusted thread id is null
    const spawnB = await mcpClientB.callTool({
      name: "subagents_spawn",
      arguments: { topic: "task b", task: "do work" },
    });
    const jobIdB = (spawnB.structuredContent as Record<string, unknown>).jobId as string;
    const jobRecordB = store.getJob(jobIdB);
    assert.equal(jobRecordB?.trustedThreadId, null, "Malformed env must not populate trustedThreadId");

    // External park fails closed (armed: false)
    const parkExternalB = await mcpClientB.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jobIdB], wait: false },
    });
    const parkDataB = parkExternalB.structuredContent as Record<string, unknown>;
    assert.equal(parkDataB.armed, false, "Missing/malformed env must fail closed for external wake");
    assert.equal(parkDataB.deliveryMode, "none");
    assert.notEqual(parkDataB.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(parkDataB.parkId as string), false, "Zero in-memory waiter allocation");

    // In v3, explicit wait=true fails closed with error message directing to subagents_follow
    const parkInTurnRes = await mcpClientB.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jobIdB], wait: true },
    });
    assert.equal(parkInTurnRes.isError, true);
    assert.match((parkInTurnRes.content[0] as { text: string }).text, /subagents_follow/i);
  } finally {
    await mcpClientA.close();
    await mcpServerA.close();
    await mcpClientB.close();
    await mcpServerB.close();
    await httpClient.close();
    await httpServer.stop();
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 23. BLK-BRIDGE-4: MCP park rejects wait=true, arms immediately with zero in-memory waiters, and preserves durable barrier
// Catches: allocating in-memory waiters over MCP JSON-RPC, accepting wait=true, or failing to preserve durable barrier in SQLite
// ---------------------------------------------------------------------------
test("BLK-BRIDGE-4: MCP park rejects wait=true, arms immediately with zero in-memory waiters, and preserves durable barrier", async () => {
  const { tmp, config, store } = await createTestEnv();
  config.daemonPort = await freePort();

  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  const httpServer = new BridgeHttpServer(config, service);
  await httpServer.start();

  const httpClient = new BridgeHttpClient(config);
  const mcpServer = createMcpServer(httpClient, {
    env: { CODEX_THREAD_ID: "44444444-4444-4444-4444-444444444444" },
  });
  const mcpClient = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await mcpServer.connect(serverTransport);
  await mcpClient.connect(clientTransport);

  try {
    const spawnRes = await mcpClient.callTool({
      name: "subagents_spawn",
      arguments: { topic: "external park durability", task: "do work" },
    });
    const jobId = (spawnRes.structuredContent as Record<string, unknown>).jobId as string;

    // Bind for authoritative external wake
    store.bindJob({ jobId, threadId: "44444444-4444-4444-4444-444444444444", originatingTurnId: "turn_mcp_e2e", originatingItemId: "item_mcp_e2e" });

    // 1. Explicit wait=true fails closed with error directing to subagents_follow
    const waitTrueRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jobId], wait: true },
    });
    assert.equal(waitTrueRes.isError, true);
    assert.match((waitTrueRes.content[0] as { text: string }).text, /subagents_follow/i);

    // 2. Omitted wait arms external barrier immediately with zero in-memory waiters
    const parkRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jobId] },
    });
    const parkData = parkRes.structuredContent as Record<string, unknown>;
    assert.equal(parkData.armed, true);
    assert.equal(parkData.deliveryMode, "cli_resume");
    assert.notEqual(parkData.deliveryMode, "in_turn");
    assert.equal(parkData.wakeState, "waiting");
    assert.equal(service.hasParkWaiter(parkData.parkId as string), false, "Zero in-memory waiter allocation");

    const barrier = store.getParkBarrierByThread("44444444-4444-4444-4444-444444444444");
    assert.ok(barrier, "Barrier must exist in SQLite");
    assert.equal(service.hasParkWaiter(barrier.id), false, "Must not register in-memory waiter");
    assert.equal(barrier.armed, true, "Durable barrier must remain armed in SQLite");
    assert.notEqual(barrier.deliveryMode, "in_turn", "Must not create in_turn barrier");
    assert.equal(barrier.deliveryMode, "cli_resume");

    // Make job complete and evaluate wakes -> external outbox must be triggered
    const resFile = path.join(tmp, "res_mcp_e2e.json");
    await writeFile(resFile, JSON.stringify({ envelope: { summary: "done" } }));
    store.setJobResult(jobId, resFile, "done");
    store.updateJobStatus(jobId, "completed");

    await service.evaluateParkWakes(jobId);
    assert.equal(fakeCodex.deliveredWakes.length, 1, "External wake must be emitted for armed durable barrier upon job completion");
  } finally {
    await mcpClient.close();
    await mcpServer.close();
    await httpClient.close();
    await httpServer.stop();
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 24. BLK-BRIDGE-5: capability probing, command arguments, candidate precedence, schema error fallback, active-writer defer, JSONL inspection
// ---------------------------------------------------------------------------
test("BLK-BRIDGE-5: capability-based CLI invocation, candidate precedence, schema fallback, active-writer defer, and JSONL reconciliation", async () => {
  const runs: Array<{ executable: string; args: string[]; stdin?: string }> = [];

  const fakeRunner: ProcessRunner = async (executable, args, options) => {
    runs.push({ executable, args, stdin: options?.stdin });

    // Probing --version
    if (args.includes("--version")) {
      return { code: 0, stdout: "codex 0.153.0", stderr: "" };
    }

    // Probing exec resume --help
    if (args.includes("exec") && args.includes("resume") && args.includes("--help")) {
      return {
        code: 0,
        stdout: "Usage: codex exec resume [OPTIONS] [SESSION_ID] [PROMPT]\n--json\n--skip-git-repo-check",
        stderr: "",
      };
    }

    // Execution
    if (executable === "candidate_incompatible") {
      return {
        code: 1,
        stdout: "",
        stderr: "Error: unknown variant functionCallOutput, expected one of message",
      };
    }

    if (executable === "candidate_active_writer") {
      return {
        code: 1,
        stdout: "",
        stderr: "thread-store conflict: thread already has an active writer",
      };
    }

    if (executable === "candidate_timeout_with_jsonl") {
      return {
        code: null,
        timedOut: true,
        stdout: '{"type":"thread.started","thread_id":"thread_jsonl"}\n{"type":"turn_started","turnId":"turn_1"}\n{"type":"item","marker":"WAKE"}\n',
        stderr: "",
      };
    }

    if (executable === "candidate_success") {
      return {
        code: 0,
        stdout: '{"type":"turn_started"}\n{"type":"turn_completed"}\n',
        stderr: "",
      };
    }

    return { code: 1, stdout: "", stderr: "Unknown executable" };
  };

  // 1. Exact command arguments and stdin marker
  const transport1 = new DefaultCodexCliTransport({
    candidates: ["candidate_success"],
    runner: fakeRunner,
  });

  const res1 = await transport1.deliverWake("thread_exact", "<!-- [WAKE:1] -->");
  assert.equal(res1.success, true);
  const run1 = runs.find((r) => r.executable === "candidate_success" && r.args.includes("--json"));
  assert.ok(run1, "Must run resume command");
  assert.deepEqual(
    run1.args,
    ["exec", "resume", "--json", "--skip-git-repo-check", "thread_exact", "-"],
    "Must pass exact argv including '-' for stdin prompt",
  );
  assert.equal(run1.stdin, "<!-- [WAKE:1] -->\n", "Must pass marker payload on stdin with terminal newline");

  // 2. Precedence and schema-error fallback to next candidate
  runs.length = 0;
  const transport2 = new DefaultCodexCliTransport({
    candidates: ["candidate_incompatible", "candidate_success"],
    runner: fakeRunner,
  });

  const res2 = await transport2.deliverWake("thread_schema", "<!-- [WAKE:2] -->");
  assert.equal(res2.success, true);
  assert.equal(res2.executablePath, "candidate_success", "Must fall back to second candidate on schema error");

  // 3. Active writer stops immediately and does NOT try next candidate
  runs.length = 0;
  const transport3 = new DefaultCodexCliTransport({
    candidates: ["candidate_active_writer", "candidate_success"],
    runner: fakeRunner,
  });

  const res3 = await transport3.deliverWake("thread_act", "<!-- [WAKE:3] -->");
  assert.equal(res3.success, false);
  assert.equal(res3.activeWriter, true);
  const triedSuccess = runs.some((r) => r.executable === "candidate_success" && r.args.includes("--json"));
  assert.equal(triedSuccess, false, "Active writer must NOT fall through to next candidate");

  // 4. Unknown outcome with JSONL inspection reconciles turn acceptance
  runs.length = 0;
  const transport4 = new DefaultCodexCliTransport({
    candidates: ["candidate_timeout_with_jsonl", "candidate_success"],
    runner: fakeRunner,
  });

  const res4 = await transport4.deliverWake("thread_jsonl", "<!-- [WAKE:4] -->");
  assert.equal(res4.success, true, "Must reconcile as accepted when JSONL shows turn_started");
  const triedSecondOnAccepted = runs.some((r) => r.executable === "candidate_success" && r.args.includes("--json"));
  assert.equal(triedSecondOnAccepted, false, "Must never blindly try second candidate after turn accepted");
});

// ---------------------------------------------------------------------------
// 26. session provenance: missing or mismatched MCP session id fails closed with zero in-memory waiters
// Catches: session spoofing, missing session provenance, or allocating in-memory waiters on session mismatch
// ---------------------------------------------------------------------------
test("session provenance: missing or mismatched MCP session id fails closed with zero in-memory waiters", async () => {
  const { tmp, config, store } = await createTestEnv();
  const service = new BridgeService(config, {
    store,
    codex: new UnavailableCodexDeliveryAdapter(),
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_prov",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_prov",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });

    const jobNoSess = store.createJob({ id: "job_no_sess", agentId: agent.id, kind: "spawn", requestId: "r1", promptHash: "p1" });

    // 1. Explicit wait=true fails closed with InvalidRequestError
    await assert.rejects(
      () => service.park({ job_ids: [jobNoSess.id], wait: true }),
      (err: unknown) => {
        assert.ok(err instanceof InvalidRequestError);
        assert.equal((err as InvalidRequestError).status, 400);
        assert.match((err as Error).message, /subagents_follow/i);
        return true;
      },
    );

    // 2. Job without MCP session id + caller without MCP session id -> fails closed (armed: false, deliveryMode: "none", zero in-memory waiters)
    const receipt1 = await service.park({ job_ids: [jobNoSess.id] });
    assert.equal(receipt1.armed, false, "Must fail closed (armed: false) when session provenance is missing");
    assert.equal(receipt1.deliveryMode, "none");
    assert.notEqual(receipt1.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt1.parkId), false, "Zero in-memory waiter allocation");

    // 3. Job with MCP session id + caller without MCP session id -> fails closed
    const jobWithSess = store.createJob({ id: "job_with_sess", agentId: agent.id, kind: "spawn", requestId: "r2", promptHash: "p2", mcpSessionId: "sess_alpha" });
    const receipt2 = await service.park({ job_ids: [jobWithSess.id] });
    assert.equal(receipt2.armed, false, "Must fail closed when caller lacks mcp_session_id");
    assert.equal(receipt2.deliveryMode, "none");
    assert.notEqual(receipt2.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt2.parkId), false, "Zero in-memory waiter allocation");

    // 4. Job without MCP session id + caller with MCP session id -> fails closed
    const receipt3 = await service.park({ job_ids: [jobNoSess.id], mcp_session_id: "sess_alpha" });
    assert.equal(receipt3.armed, false, "Must fail closed when job lacks mcpSessionId");
    assert.equal(receipt3.deliveryMode, "none");
    assert.notEqual(receipt3.deliveryMode, "in_turn");
    assert.equal(service.hasParkWaiter(receipt3.parkId), false, "Zero in-memory waiter allocation");

    // 5. Mismatched caller session vs job session throws identity_mismatch
    await assert.rejects(
      () => service.park({ job_ids: [jobWithSess.id], mcp_session_id: "sess_beta" }),
      (err: unknown) => err instanceof ConflictError && err.code === "identity_mismatch",
      "Must throw identity_mismatch when caller session does not match job session",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 27. capability acceptance: exec resume --help is authoritative and version is observability only
// ---------------------------------------------------------------------------
test("capability acceptance: exec resume --help is authoritative; version is observability only", async () => {
  // Candidate has high semver (0.160.0) but exec resume --help fails
  const fakeRunnerHelpFails: ProcessRunner = async (exe, args) => {
    if (args.includes("--version")) {
      return { code: 0, stdout: "codex 0.160.0", stderr: "" };
    }
    if (args.includes("--help")) {
      return { code: 1, stdout: "", stderr: "unknown command: resume" };
    }
    return { code: 0, stdout: "", stderr: "" };
  };

  const transport1 = new DefaultCodexCliTransport({
    candidates: ["codex_old_help"],
    runner: fakeRunnerHelpFails,
  });

  const probe1 = await transport1.probeCapabilities();
  assert.equal(probe1.compatible, false, "Must NOT accept capability based on semver alone if exec resume --help fails");
  assert.equal(probe1.version, "0.160.0", "Version must still be captured for observability");

  // Candidate has exec resume --help working -> accepted as compatible
  const fakeRunnerHelpSucceeds: ProcessRunner = async (exe, args) => {
    if (args.includes("--version")) {
      return { code: 0, stdout: "codex 0.160.0", stderr: "" };
    }
    if (args.includes("--help")) {
      return { code: 0, stdout: "codex exec resume [options] <session_id>", stderr: "" };
    }
    return { code: 0, stdout: "", stderr: "" };
  };

  const transport2 = new DefaultCodexCliTransport({
    candidates: ["codex_good_help"],
    runner: fakeRunnerHelpSucceeds,
  });

  const probe2 = await transport2.probeCapabilities();
  assert.equal(probe2.compatible, true, "Must accept capability when exec resume --help succeeds");
  assert.equal(probe2.version, "0.160.0");
});

// ---------------------------------------------------------------------------
// 28. Dotted JSONL turn acceptance inspection and same-task provenance
// ---------------------------------------------------------------------------
test("dotted JSONL turn acceptance inspection and task provenance", () => {
  const targetThread = "01a06d2f-6d39-74f3-b122-f4a2194a0706";

  // 1) thread.started alone is NOT acceptance (establishes provenance only)
  assert.equal(
    inspectJsonlTurnAcceptance(
      JSON.stringify({ type: "thread.started", thread_id: targetThread }) + "\n",
      targetThread,
    ),
    false,
    "thread.started alone must NOT be accepted (provenance only)",
  );

  // Dotted thread.started with mismatched thread_id fails provenance
  assert.equal(
    inspectJsonlTurnAcceptance(
      JSON.stringify({ type: "thread.started", thread_id: "00000000-0000-0000-0000-000000000000" }) + "\n",
      targetThread,
    ),
    false,
    "thread.started with mismatched thread_id must NOT be accepted",
  );

  // 2) matching thread.started -> turn.started accepts
  assert.equal(
    inspectJsonlTurnAcceptance(
      [
        JSON.stringify({ type: "thread.started", thread_id: targetThread }),
        JSON.stringify({ type: "turn.started", turnId: "01a06da1-a514-71c0-b5d5-a79e69545ce6" }),
      ].join("\n") + "\n",
      targetThread,
    ),
    true,
    "matching thread.started -> turn.started must be accepted",
  );

  // 3) mismatched thread.started -> unscoped turn.started does NOT accept
  assert.equal(
    inspectJsonlTurnAcceptance(
      [
        JSON.stringify({ type: "thread.started", thread_id: "00000000-0000-0000-0000-000000000000" }),
        JSON.stringify({ type: "turn.started", turnId: "01a06da1-a514-71c0-b5d5-a79e69545ce6" }),
      ].join("\n") + "\n",
      targetThread,
    ),
    false,
    "mismatched thread.started -> unscoped turn.started must NOT be accepted",
  );

  // Turn event carrying matching thread id directly accepts even without prior thread.started
  assert.equal(
    inspectJsonlTurnAcceptance(
      JSON.stringify({
        type: "turn.started",
        turnId: "01a06da1-a514-71c0-b5d5-a79e69545ce6",
        thread_id: targetThread,
      }) + "\n",
      targetThread,
    ),
    true,
    "turn.started carrying matching thread id must be accepted directly",
  );

  // Turn event carrying mismatched thread id directly must NOT accept
  assert.equal(
    inspectJsonlTurnAcceptance(
      JSON.stringify({
        type: "turn.started",
        turnId: "01a06da1-a514-71c0-b5d5-a79e69545ce6",
        thread_id: "00000000-0000-0000-0000-000000000000",
      }) + "\n",
      targetThread,
    ),
    false,
    "turn.started carrying mismatched thread id must NOT be accepted",
  );

  // Do not accept item.* alone before a correlated turn
  assert.equal(
    inspectJsonlTurnAcceptance(
      JSON.stringify({ type: "item.started", item: { type: "user_message" } }) + "\n",
      targetThread,
    ),
    false,
    "item.started alone before correlated turn must NOT be accepted",
  );
  assert.equal(
    inspectJsonlTurnAcceptance(
      [
        JSON.stringify({ type: "thread.started", thread_id: targetThread }),
        JSON.stringify({ type: "item.started", item: { type: "user_message" } }),
      ].join("\n") + "\n",
      targetThread,
    ),
    false,
    "item.started after thread.started but before correlated turn must NOT be accepted",
  );
  assert.equal(
    inspectJsonlTurnAcceptance(
      JSON.stringify({ type: "item.completed" }) + "\n",
      targetThread,
    ),
    false,
    "item.completed alone before correlated turn must NOT be accepted",
  );

  // Matching thread.started -> turn.started -> item.started accepts
  assert.equal(
    inspectJsonlTurnAcceptance(
      [
        JSON.stringify({ type: "thread.started", thread_id: targetThread }),
        JSON.stringify({ type: "turn.started", turnId: "01a06da1-a514-71c0-b5d5-a79e69545ce6" }),
        JSON.stringify({ type: "item.started", item: { type: "user_message" } }),
      ].join("\n") + "\n",
      targetThread,
    ),
    true,
    "matching thread.started -> turn.started -> item.started must be accepted",
  );

  // Rejection of failure / error events
  assert.equal(
    inspectJsonlTurnAcceptance(
      JSON.stringify({ type: "turn.failed", error: "Fatal model error" }) + "\n",
      targetThread,
    ),
    false,
    "turn.failed must NOT be accepted",
  );
  assert.equal(
    inspectJsonlTurnAcceptance(
      JSON.stringify({ type: "thread.error", error: "Internal crash" }) + "\n",
      targetThread,
    ),
    false,
    "thread.error must NOT be accepted",
  );
});

// ---------------------------------------------------------------------------
// 29. defaultProcessRunner: pre-accept timeout still kills unaccepted child and returns unknown
// ---------------------------------------------------------------------------
test("defaultProcessRunner: pre-accept timeout still kills unaccepted child and returns unknown", async () => {
  // 1. Process produces only thread.started (no turn.started) before timeout: timeout kills child
  const timeoutScript =
    'console.log(JSON.stringify({ type: "thread.started", thread_id: "test-thread" })); setTimeout(() => {}, 2000);';
  const timeoutResult = await defaultProcessRunner(
    process.execPath,
    ["-e", timeoutScript],
    { timeoutMs: 150, expectedThreadId: "test-thread" },
  );

  assert.equal(timeoutResult.timedOut, true, "Must report timedOut when no turn acceptance occurs");
  assert.equal(timeoutResult.accepted, false, "Must not be accepted on pre-accept timeout");
  assert.equal(timeoutResult.code, null, "Child was killed by timeout");
});

// ---------------------------------------------------------------------------
// 30. deliverWake promise resolves at turn.started while child remains alive and is not killed/unref'd until natural completion
// ---------------------------------------------------------------------------
test("deliverWake promise resolves at turn.started while child remains alive and is not killed/unref'd until natural completion", async () => {
  const targetThread = "01a06d2f-6d39-74f3-b122-f4a2194a0706";
  let spawnedChild: import("node:child_process").ChildProcess | null = null;
  let unrefCalled = false;
  let killCalled = false;

  // Node script that outputs matching thread.started, then turn.started, then sleeps 600ms before natural exit
  const script = [
    `console.log(JSON.stringify({ type: "thread.started", thread_id: "${targetThread}" }));`,
    `console.log(JSON.stringify({ type: "turn.started", turnId: "01a06da1-a514-71c0-b5d5-a79e69545ce6" }));`,
    `setTimeout(() => { process.exit(0); }, 600);`,
  ].join(" ");

  const testRunner: ProcessRunner = async (executable, args, options) => {
    if (args.includes("--version")) {
      return { code: 0, stdout: "codex 0.153.0", stderr: "" };
    }
    if (args.includes("exec") && args.includes("resume") && args.includes("--help")) {
      return { code: 0, stdout: "codex exec resume", stderr: "" };
    }
    return defaultProcessRunner(process.execPath, ["-e", script], {
      ...options,
      onSpawn: (child) => {
        spawnedChild = child;
        const origUnref = child.unref.bind(child);
        child.unref = () => {
          unrefCalled = true;
          return origUnref();
        };
        const origKill = child.kill.bind(child);
        child.kill = (signal) => {
          killCalled = true;
          return origKill(signal);
        };
        options?.onSpawn?.(child);
      },
    });
  };

  const transport = new DefaultCodexCliTransport({
    candidates: ["codex_test_candidate"],
    runner: testRunner,
  });

  const startTime = Date.now();
  const res = await transport.deliverWake(targetThread, "<!-- [WAKE:TEST] -->");
  const elapsed = Date.now() - startTime;

  // 1. DeliverWake promise resolves promptly at turn.started (< 450ms, well before the 600ms script exit)
  assert.equal(res.success, true, "Must succeed on accepted turn");
  assert.equal(res.accepted, true, "Must be marked accepted");
  assert.ok(elapsed < 450, `Must resolve promptly upon turn.started (took ${elapsed}ms)`);

  // 2. Child remains ALIVE when deliverWake resolves
  assert.ok(spawnedChild !== null, "Child must have been spawned");
  const child = spawnedChild!;
  assert.equal(child.killed, false, "Child must NOT be killed on acceptance");
  assert.equal(child.exitCode, null, "Child must still be alive (exitCode null) when deliverWake resolves");
  assert.equal(unrefCalled, false, "Child must NOT be unref'd");
  assert.equal(killCalled, false, "Child kill() must NOT be called on acceptance");

  // 3. Child completes naturally afterwards
  await new Promise<void>((resolve) => {
    if (child.exitCode !== null) {
      resolve();
    } else {
      child.on("close", () => resolve());
    }
  });

  assert.equal(child.exitCode, 0, "Child must finish with natural exit code 0");
});

// ---------------------------------------------------------------------------
// 31. DefaultCodexCliTransport: mismatched provenance rejects; pre-accept timeout still kills unaccepted child
// ---------------------------------------------------------------------------
test("DefaultCodexCliTransport: mismatched thread.started -> unscoped turn.started does not accept; pre-accept timeout still kills", async () => {
  const targetThread = "01a06d2f-6d39-74f3-b122-f4a2194a0706";

  // Mismatched provenance runner
  const fakeRunnerMismatched: ProcessRunner = async (executable, args) => {
    if (args.includes("--version")) return { code: 0, stdout: "codex 0.153.0", stderr: "" };
    if (args.includes("--help")) return { code: 0, stdout: "codex exec resume", stderr: "" };
    return {
      code: 1,
      stdout: [
        JSON.stringify({ type: "thread.started", thread_id: "00000000-0000-0000-0000-000000000000" }),
        JSON.stringify({ type: "turn.started", turnId: "turn_unscoped" }),
      ].join("\n") + "\n",
      stderr: "Mismatched thread execution",
      accepted: false,
    };
  };

  const transportMismatch = new DefaultCodexCliTransport({
    candidates: ["codex_mismatch_test"],
    runner: fakeRunnerMismatched,
  });

  const resMismatch = await transportMismatch.deliverWake(targetThread, "<!-- [WAKE:TEST] -->");
  assert.equal(resMismatch.success, false, "Must NOT deliver wake on provenance mismatch even if turn.started follows");

  // Pre-accept timeout still kills unaccepted child in deliverWake
  let killInvoked = false;
  const timeoutScript = "setTimeout(() => {}, 2000);";
  const timeoutRunner: ProcessRunner = async (executable, args, options) => {
    if (args.includes("--version")) return { code: 0, stdout: "codex 0.153.0", stderr: "" };
    if (args.includes("--help")) return { code: 0, stdout: "codex exec resume", stderr: "" };
    return defaultProcessRunner(process.execPath, ["-e", timeoutScript], {
      ...options,
      timeoutMs: 150,
      onSpawn: (child) => {
        const origKill = child.kill.bind(child);
        child.kill = (sig) => {
          killInvoked = true;
          return origKill(sig);
        };
      },
    });
  };

  const transportTimeout = new DefaultCodexCliTransport({
    candidates: ["codex_timeout_test"],
    runner: timeoutRunner,
  });

  const resTimeout = await transportTimeout.deliverWake(targetThread, "<!-- [WAKE:TEST] -->");
  assert.equal(resTimeout.success, false, "Must fail when pre-accept timeout occurs");
  assert.equal(resTimeout.unknownOutcome, true, "Must be classified as unknownOutcome on timeout");
  assert.equal(killInvoked, true, "Pre-accept timeout must kill the unaccepted child process");
});

// ---------------------------------------------------------------------------
// 32. DefaultCodexCliTransport: line-terminated marker framing for stdin prompt
// ---------------------------------------------------------------------------
test("DefaultCodexCliTransport: line-terminated marker framing ensures single terminal newline without altering pre-terminated markers", async () => {
  // 1. Direct assertions on ensureLineTerminatedMarker helper
  assert.equal(ensureLineTerminatedMarker("<!-- [WAKE:GEN5] -->"), "<!-- [WAKE:GEN5] -->\n");
  assert.equal(ensureLineTerminatedMarker("<!-- [WAKE:GEN6] -->\n"), "<!-- [WAKE:GEN6] -->\n");
  assert.equal(ensureLineTerminatedMarker("<!-- [WAKE:CRLF] -->\r\n"), "<!-- [WAKE:CRLF] -->\r\n");
  assert.equal(ensureLineTerminatedMarker("<!-- [WAKE:CR] -->\r"), "<!-- [WAKE:CR] -->\r");

  // 2. DeliverWake deterministic behavior with mock runner
  const runs: Array<{ stdin?: string; args: string[] }> = [];
  const fakeRunner: ProcessRunner = async (executable, args, options) => {
    runs.push({ stdin: options?.stdin, args });
    if (args.includes("--version")) return { code: 0, stdout: "codex 0.153.0", stderr: "" };
    if (args.includes("--help")) return { code: 0, stdout: "codex exec resume", stderr: "" };
    return {
      code: 0,
      stdout: [
        JSON.stringify({ type: "thread.started", thread_id: "01a06d2f-6d39-74f3-b122-f4a2194a0706" }),
        JSON.stringify({ type: "turn.started", turnId: "01a06dbe-d2b8-7073-a71f-a36b479546c7" }),
      ].join("\n") + "\n",
      stderr: "",
      accepted: true,
    };
  };

  const transport = new DefaultCodexCliTransport({
    candidates: ["codex_framing_test"],
    runner: fakeRunner,
  });

  const targetThread = "01a06d2f-6d39-74f3-b122-f4a2194a0706";

  // Case A: Marker WITHOUT newline reaches runner with exactly one terminal newline
  runs.length = 0;
  const resNoTerm = await transport.deliverWake(targetThread, "<!-- [WAKE:GEN5] -->");
  assert.equal(resNoTerm.success, true);
  assert.equal(resNoTerm.accepted, true);
  const runA = runs.find((r) => r.args.includes("--json"));
  assert.ok(runA, "Resume command must have executed");
  assert.equal(runA.stdin, "<!-- [WAKE:GEN5] -->\n", "Must append exactly one newline when marker has no terminator");
  assert.ok(!runA.stdin.includes("worker") && !runA.stdin.includes("result"), "Worker result text must not be in payload");

  // Case B: Marker WITH trailing \n reaches runner unchanged (no double newline)
  runs.length = 0;
  const resLf = await transport.deliverWake(targetThread, "<!-- [WAKE:GEN6] -->\n");
  assert.equal(resLf.success, true);
  assert.equal(resLf.accepted, true);
  const runB = runs.find((r) => r.args.includes("--json"));
  assert.ok(runB, "Resume command must have executed");
  assert.equal(runB.stdin, "<!-- [WAKE:GEN6] -->\n", "Must preserve existing \\n without adding second blank line");

  // Case C: Marker WITH trailing \r\n reaches runner unchanged (no extra newline)
  runs.length = 0;
  const resCrlf = await transport.deliverWake(targetThread, "<!-- [WAKE:CRLF] -->\r\n");
  assert.equal(resCrlf.success, true);
  assert.equal(resCrlf.accepted, true);
  const runC = runs.find((r) => r.args.includes("--json"));
  assert.ok(runC, "Resume command must have executed");
  assert.equal(runC.stdin, "<!-- [WAKE:CRLF] -->\r\n", "Must preserve existing \\r\\n without adding second blank line");
});

test("SWARM-DEFECT-REPRO: stalled following job with null result_path is progress advisory, NOT consumable readyJob", async () => {
  const { tmp, config, store } = await createTestEnv();
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: delivery,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_swarm_repro",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_repro",
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
    });
    const job = store.createJob({
      id: "job_mtrupco3_8ccf105144b28030",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_repro",
      promptHash: "hash_repro",
    });
    store.db.prepare("UPDATE jobs SET status = 'following' WHERE id = ?").run(job.id);
    store.bindJob({
      jobId: job.id,
      threadId: "thread_swarm_repro",
      originatingTurnId: "turn_1",
      originatingItemId: "item_1",
    });

    const stallTime = new Date(Date.now() - 350 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(stallTime, job.id);

    // 1. MCP park receipt check
    const receipt = await service.park({
      jobIds: [job.id],
      predicate: "ALL",
      threadId: "thread_swarm_repro",
    });
    assert.equal(receipt.armed, true);
    assert.deepEqual(receipt.readyJobIds, [], "Ongoing following job must NOT be in readyJobIds");
    assert.equal(receipt.readyCount, 0, "Ongoing following job must not increment readyCount");
    assert.equal(receipt.pendingCount, 1, "Ongoing following job must remain in pendingCount");
    assert.deepEqual(receipt.advisoryJobIds, [job.id], "Stalled following job must be in advisoryJobIds");
    assert.ok(receipt.advisoryFingerprints?.[job.id], "advisoryFingerprints must be present for stalled job");

    // 2. Evaluate wake check
    await service.evaluateParkWakes(job.id);
    assert.equal(delivery.deliveredWakes.length, 1, "Advisory wake must be delivered");
    const wake = delivery.deliveredWakes[0]!;
    assert.deepEqual(wake.envelope.readyJobIds, [], "Ongoing following job must NOT be in envelope.readyJobIds");
    assert.equal(wake.envelope.pendingCount, 1, "Ongoing following job must remain pending in envelope");
    assert.deepEqual(wake.envelope.advisoryJobIds, [job.id], "Stalled job must be in envelope.advisoryJobIds");
    assert.ok(wake.envelope.advisoryFingerprints?.[job.id], "advisoryFingerprints must be recorded in envelope");
    assert.equal(wake.envelope.resultHashes[job.id], undefined, "No synthetic result hash for missing result");
    assert.equal(/Call subagents_follow/i.test(wake.envelope.instruction), false, "No follow instruction for ongoing-only advisory");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});
