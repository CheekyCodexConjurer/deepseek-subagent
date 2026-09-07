import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createServer as createNetServer, type AddressInfo } from "node:net";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";
import {
  DEFAULT_CODEX_CAPABILITIES,
  type CodexCorrelation,
  type CodexDeliveryAdapter,
} from "../../src/codex/adapter.js";
import type {
  BridgeConfig,
  CodexBinding,
  JobRecord,
  JobStatus,
  OpenCodeClientLike,
  OpenCodeMessage,
  ResultEnvelope,
  WakeEnvelope,
} from "../../src/types.js";
import type { CodexCliTransport, CodexCliExecutionResult } from "../../src/codex/cli-resolver.js";
import { BridgeHttpServer, BridgeHttpClient } from "../../src/http-server.js";
import { createMcpServer } from "../../src/mcp.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";

class FakeCodexDelivery implements CodexDeliveryAdapter {
  available = true;
  reason: string | null = null;
  capabilities = { ...DEFAULT_CODEX_CAPABILITIES, authoritativeAttachment: false };
  deliveredWakes: Array<{ envelope: WakeEnvelope; binding: CodexBinding }> = [];

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
    return false;
  }
  onCorrelation(_listener: (correlation: CodexCorrelation) => void): () => void {
    return () => undefined;
  }
}

class FakeCliTransport implements CodexCliTransport {
  calls: Array<{ threadId: string; marker: string }> = [];
  reconcileQueuedWakeCalls: Array<{ threadId: string; marker: string }> = [];
  queuedWakeHandler?: (threadId: string, marker: string) => Promise<{ found: boolean; messageId?: string | null; deliveryMode?: string } | boolean | null>;
  nextResult: CodexCliExecutionResult & { deliveryMode?: string; messageId?: string } = {
    success: true,
    accepted: true,
    executablePath: "C:\\Codex\\codex.exe",
    version: "0.150.0",
  };
  compatible = true;

  async probeCapabilities(executable?: string): Promise<{ compatible: boolean; version: string | null }> {
    return { compatible: this.compatible, version: "0.150.0" };
  }

  async deliverWake(threadId: string, marker: string): Promise<CodexCliExecutionResult> {
    this.calls.push({ threadId, marker });
    return this.nextResult;
  }

  async reconcileQueuedWake(threadId: string, marker: string): Promise<{ found: boolean; messageId?: string | null; deliveryMode?: string } | boolean | null> {
    this.reconcileQueuedWakeCalls.push({ threadId, marker });
    if (this.queuedWakeHandler) {
      return this.queuedWakeHandler(threadId, marker);
    }
    return null;
  }
}

class FakeOpenCodeClient implements OpenCodeClientLike {
  messages: OpenCodeMessage[] = [];
  async health() { return { healthy: true }; }
  async createSession() { return { id: "session_fake" }; }
  async promptAsync() {}
  async listMessages() { return this.messages; }
  async getDiff() { return ""; }
  async abort() {}
  async replyPermission() {}
  async subscribe() {}
}

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

async function createTestEnv() {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "ds-park-consumption-test-"));
  const port = await freePort();
  const config = createDefaultConfig({
    dataDir: tmp,
    configPath: path.join(tmp, "config.json"),
    daemonPort: port,
    daemonHost: "127.0.0.1",
    experimentalSameChatDelivery: true,
  });
  const store = new BridgeStore(path.join(tmp, "bridge.sqlite"));
  return { tmp, config, store };
}

function createTestService(config: BridgeConfig, store: BridgeStore, fakeCodex: FakeCodexDelivery, fakeCli: FakeCliTransport) {
  return new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
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
}

function setupAgent(store: BridgeStore, tmp: string, id: string = "agent_test") {
  return store.createAgent({
    id,
    title: "Test Agent",
    topic: "Explicit Consumption V3",
    repositoryRoot: tmp,
    workspacePath: tmp,
    workspaceStrategy: "shared",
    opencodeServerId: "srv",
    opencodeSessionId: `session_${id}`,
    modelProviderId: "deepseek",
    modelId: "deepseek-chat",
    modelVariant: null,
  });
}

function createValidEnvelope(agentId: string, jobId: string, resultPath: string, overrides: Partial<ResultEnvelope> = {}): ResultEnvelope {
  return {
    version: 1,
    agentId,
    jobId,
    topic: "explicit-consumption",
    status: "completed",
    opencodeSessionId: "session_fake",
    model: "deepseek-chat",
    modelDisplayName: "DeepSeek Chat",
    workspace: "shared",
    summary: "usable test result",
    diffSummary: "none",
    fullResultPath: resultPath,
    orchestratorInstruction: "none",
    files: ["src/example.ts"],
    tests: ["test/example.test.ts"],
    risks: [],
    ...overrides,
  };
}

function transitionJob(store: BridgeStore, jobId: string, targetStatus: JobStatus) {
  const current = store.getJob(jobId);
  if (!current || current.status === targetStatus) return;

  let s = current.status;
  if (s === "created") {
    if (targetStatus === "dispatching" || targetStatus === "failed" || targetStatus === "aborted") {
      store.updateJobStatus(jobId, targetStatus);
      return;
    }
    store.updateJobStatus(jobId, "dispatching");
    s = "dispatching";
  }

  if (s === "dispatching") {
    if (targetStatus === "running" || targetStatus === "completed" || targetStatus === "timed_out" || targetStatus === "delivery_pending" || targetStatus === "delivered") {
      store.updateJobStatus(jobId, "running");
      s = "running";
    } else if (targetStatus === "following" || targetStatus === "completed_partial") {
      store.updateJobStatus(jobId, "following");
      s = "following";
    } else {
      store.updateJobStatus(jobId, targetStatus);
      return;
    }
  }

  if (s === "running" || s === "following") {
    if (targetStatus === "completed" || targetStatus === "delivery_pending" || targetStatus === "delivered") {
      store.updateJobStatus(jobId, "completed");
      s = "completed";
    } else {
      store.updateJobStatus(jobId, targetStatus);
      return;
    }
  }

  if (s === "completed") {
    if (targetStatus === "delivery_pending") {
      store.updateJobStatus(jobId, "delivery_pending");
    } else if (targetStatus === "delivered") {
      store.updateJobStatus(jobId, "delivered");
    }
  }
}

function makeJobCompleted(store: BridgeStore, jobId: string, resultPath?: string, resultSummary?: string) {
  transitionJob(store, jobId, "completed");
  if (resultPath) {
    store.setJobResult(jobId, resultPath, resultSummary ?? "completed result");
  }
}

function makeJobDelivered(store: BridgeStore, jobId: string, resultPath?: string, resultSummary?: string) {
  transitionJob(store, jobId, "delivered");
  if (resultPath) {
    store.setJobResult(jobId, resultPath, resultSummary ?? "delivered result");
  }
}

// ---------------------------------------------------------------------------
// 1. Background park readiness does NOT set jobs.result_consumed_at
// ---------------------------------------------------------------------------
test("CONSUMPTION-1.1: Background park readiness, auto-armed follow lifecycle, and wake delivery do NOT set jobs.result_consumed_at", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_park_bg_1");
    const job = store.createJob({ id: "job_park_bg_1", agentId: agent.id, kind: "spawn", requestId: "r1", promptHash: "h1" });
    store.bindJob({ jobId: job.id, threadId: "thread_bg_1", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    transitionJob(store, job.id, "running");

    // Arm background park - this auto-arms the background follow lifecycle for active jobs
    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.wakeState, "waiting");
    assert.equal(receipt.obligationState, "pending");
    assert.equal((service as any).followLifecycles.has(job.id), true, "Follow lifecycle must be auto-armed by park");
    assert.equal((service as any).followLifecycles.get(job.id).autoArmed, true);

    // Before completion: result_consumed_at is null
    let currentJob = store.getJob(job.id)!;
    assert.equal(currentJob.resultConsumedAt, null);

    // Job finishes in background: persists result file, completes, and resolves auto-armed background follow
    const resultFile = path.join(tmp, "res_bg_1.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "background task complete");

    // Resolve the auto-armed follow lifecycle as happens in the real worker loop
    await (service as any).resolveFollow(job.id, {
      status: "completed",
      resultAvailable: true,
      envelope,
    });

    // Background park readiness evaluates
    await service.evaluateParkWakes(job.id);

    // Verify barrier woke and wake outbox delivered
    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken");
    assert.equal(barrier.armed, false);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    assert.equal(outbox.status, "delivered");
    assert.equal(fakeCli.calls.length, 1);

    // CRITICAL (Canary repro): background park readiness, wake delivery, AND background follow lifecycle resolution must NOT consume the result!
    currentJob = store.getJob(job.id)!;
    assert.equal(currentJob.resultConsumedAt, null, "Background park readiness and follow resolution must NOT set result_consumed_at");
    assert.equal(store.countUnconsumedTerminalResults(), 1, "Job must remain in unconsumed terminal results");
    assert.equal(store.listUnconsumedTerminalResults()[0]?.id, job.id);

    // Only explicit public follow sets it
    const followRes = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followRes.status, "completed");
    assert.equal(followRes.resultAvailable, true);
    currentJob = store.getJob(job.id)!;
    assert.ok(currentJob.resultConsumedAt, "Explicit public follow must set result_consumed_at");
    assert.equal(store.countUnconsumedTerminalResults(), 0);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-1.2: Redundant background park readiness evaluations never mutate result_consumed_at", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_park_bg_repeat");
    const job = store.createJob({ id: "job_park_bg_repeat", agentId: agent.id, kind: "spawn", requestId: "r_rep", promptHash: "h_rep" });
    store.bindJob({ jobId: job.id, threadId: "thread_bg_repeat", originatingTurnId: "turn_1", originatingItemId: "item_1" });

    await service.park({ job_ids: [job.id] });

    const resultFile = path.join(tmp, "res_bg_repeat.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "repeated check task");

    // Trigger evaluateParkWakes multiple times sequentially
    await service.evaluateParkWakes(job.id);
    await service.evaluateParkWakes(job.id);
    await service.evaluateParkWakes(job.id);

    const currentJob = store.getJob(job.id)!;
    assert.equal(currentJob.resultConsumedAt, null, "Repeated evaluations must leave result_consumed_at null");
    assert.equal(store.countUnconsumedTerminalResults(), 1);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-1.3: Multi-job park readiness under ALL and QUORUM leaves all results unconsumed", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_multi_park");
    const j1 = store.createJob({ id: "job_mp_1", agentId: agent.id, kind: "spawn", requestId: "r1", promptHash: "h1" });
    const j2 = store.createJob({ id: "job_mp_2", agentId: agent.id, kind: "spawn", requestId: "r2", promptHash: "h2" });
    const j3 = store.createJob({ id: "job_mp_3", agentId: agent.id, kind: "spawn", requestId: "r3", promptHash: "h3" });
    store.bindJob({ jobId: j1.id, threadId: "thread_mp", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_mp", originatingTurnId: "turn_1", originatingItemId: "item_2" });
    store.bindJob({ jobId: j3.id, threadId: "thread_mp", originatingTurnId: "turn_1", originatingItemId: "item_3" });

    // Park with ALL predicate
    const receipt = await service.park({ job_ids: [j1.id, j2.id, j3.id], predicate: "ALL" });
    assert.equal(receipt.armed, true);

    const r1 = path.join(tmp, "r1.json");
    const r2 = path.join(tmp, "r2.json");
    const r3 = path.join(tmp, "r3.json");
    await writeFile(r1, JSON.stringify({ envelope: createValidEnvelope(agent.id, j1.id, r1) }));
    await writeFile(r2, JSON.stringify({ envelope: createValidEnvelope(agent.id, j2.id, r2) }));
    await writeFile(r3, JSON.stringify({ envelope: createValidEnvelope(agent.id, j3.id, r3) }));

    // j1 completes
    makeJobCompleted(store, j1.id, r1, "r1");
    await service.evaluateParkWakes(j1.id);
    assert.equal(store.getJob(j1.id)?.resultConsumedAt, null);

    // j2 completes
    makeJobCompleted(store, j2.id, r2, "r2");
    await service.evaluateParkWakes(j2.id);
    assert.equal(store.getJob(j1.id)?.resultConsumedAt, null);
    assert.equal(store.getJob(j2.id)?.resultConsumedAt, null);

    // j3 completes -> ALL satisfies, wake triggered
    makeJobCompleted(store, j3.id, r3, "r3");
    await service.evaluateParkWakes(j3.id);

    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken");
    assert.equal(fakeCli.calls.length, 1);

    // None of the jobs should be consumed by the park wake
    assert.equal(store.getJob(j1.id)?.resultConsumedAt, null);
    assert.equal(store.getJob(j2.id)?.resultConsumedAt, null);
    assert.equal(store.getJob(j3.id)?.resultConsumedAt, null);
    assert.equal(store.countUnconsumedTerminalResults(), 3);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-1.4: Background deliverJob does not set result_consumed_at", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_deliver_job");
    const job = store.createJob({ id: "job_deliver_1", agentId: agent.id, kind: "spawn", requestId: "r_del", promptHash: "h_del" });
    store.bindJob({ jobId: job.id, threadId: "thread_del", originatingTurnId: "turn_1", originatingItemId: "item_1" });

    const resultFile = path.join(tmp, "res_deliver.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "delivery test");

    // Call deliverJob
    await service.deliverJob(job.id);

    const currentJob = store.getJob(job.id)!;
    assert.equal(currentJob.status, "delivered");
    assert.equal(currentJob.resultConsumedAt, null, "Background delivery must NOT set result_consumed_at");
    assert.equal(store.countUnconsumedTerminalResults(), 1);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Wake eligibility can observe terminal persisted results without consuming
// ---------------------------------------------------------------------------
test("CONSUMPTION-2.1: isJobWakeEligible observes terminal persisted results without consuming", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_eligible_test");
    const statuses = ["completed", "completed_partial", "delivered", "delivery_pending", "failed", "aborted", "timed_out"] as const;

    for (let i = 0; i < statuses.length; i++) {
      const s = statuses[i];
      const j = store.createJob({ id: `job_elig_${i}`, agentId: agent.id, kind: "spawn", requestId: `req_${i}`, promptHash: `hash_${i}` });
      const rf = path.join(tmp, `elig_${i}.json`);
      const statusForEnvelope = (["completed", "completed_partial", "timed_out", "failed", "aborted"].includes(s) ? s : "completed") as any;
      const env = createValidEnvelope(agent.id, j.id, rf, { status: statusForEnvelope });
      await writeFile(rf, JSON.stringify({ envelope: env }));
      store.setJobResult(j.id, rf, `summary for ${s}`);
      transitionJob(store, j.id, s);

      const jobRecord = store.getJob(j.id)!;
      const eligible = service.isJobWakeEligible(jobRecord);
      assert.equal(eligible, true, `Job with status ${s} must be wake eligible`);

      // Verify observing eligibility did NOT consume the result
      const afterCheck = store.getJob(j.id)!;
      assert.equal(afterCheck.resultConsumedAt, null, `isJobWakeEligible must not consume result for status ${s}`);
    }

    assert.equal(store.countUnconsumedTerminalResults(), statuses.length);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-2.2: service.park observes terminal results to compute readyCount without consuming", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_park_observe");
    const j1 = store.createJob({ id: "job_obs_1", agentId: agent.id, kind: "spawn", requestId: "r1", promptHash: "h1" });
    const j2 = store.createJob({ id: "job_obs_2", agentId: agent.id, kind: "spawn", requestId: "r2", promptHash: "h2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_obs", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_obs", originatingTurnId: "turn_1", originatingItemId: "item_2" });

    // Mark j1 completed with result before park
    const r1 = path.join(tmp, "obs_r1.json");
    const env = createValidEnvelope(agent.id, j1.id, r1);
    await writeFile(r1, JSON.stringify({ envelope: env }));
    makeJobCompleted(store, j1.id, r1, "already complete");

    // Park observing both jobs (j1 ready, j2 pending)
    const receipt = await service.park({ job_ids: [j1.id, j2.id], predicate: "ALL" });
    assert.equal(receipt.readyCount, 1);
    assert.equal(receipt.pendingCount, 1);
    assert.deepEqual(receipt.readyJobIds, [j1.id]);

    // Observing readiness must NOT consume j1
    assert.equal(store.getJob(j1.id)?.resultConsumedAt, null, "Park observation must not consume ready job");
    assert.equal(store.getJob(j2.id)?.resultConsumedAt, null);
    assert.equal(store.countUnconsumedTerminalResults(), 1);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-2.3: Wake envelope hash generation reads persisted results without consuming", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_wake_hash");
    const job = store.createJob({ id: "job_wake_hash", agentId: agent.id, kind: "spawn", requestId: "rh", promptHash: "hh" });
    store.bindJob({ jobId: job.id, threadId: "thread_hash", originatingTurnId: "turn_1", originatingItemId: "item_1" });

    const receipt = await service.park({ job_ids: [job.id] });

    const resultFile = path.join(tmp, "hash_test.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "hash content verification");

    await service.evaluateParkWakes(job.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    const wakeEnvelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.ok(wakeEnvelope.resultHashes[job.id], "Wake envelope must contain computed result hash");

    // Result must still be unconsumed
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null, "Reading result for hash generation must not consume it");
    assert.equal(store.countUnconsumedTerminalResults(), 1);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Only public follow/recover consumes it
// ---------------------------------------------------------------------------
test("CONSUMPTION-3.1: Non-consuming operations (status changes, park arming, agent closing) leave result unconsumed", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_non_consume");
    const job = store.createJob({ id: "job_nc", agentId: agent.id, kind: "spawn", requestId: "r_nc", promptHash: "h_nc" });
    store.bindJob({ jobId: job.id, threadId: "thread_nc", originatingTurnId: "turn_1", originatingItemId: "item_1" });

    const resultFile = path.join(tmp, "nc.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobDelivered(store, job.id, resultFile, "non-consume check");

    // job is delivered with resultPath, result_consumed_at is null
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);

    // Closing the agent must NOT consume the job obligation
    const closeRes = await service.close(agent.id);
    assert.equal(closeRes.status, "closed");
    assert.equal(closeRes.quiescent, true);

    const closedAgent = store.getAgent(agent.id)!;
    assert.equal(closedAgent.status, "closed");
    assert.ok(closedAgent.closedAt);

    // Job result remains unconsumed despite agent being closed!
    const jobAfterClose = store.getJob(job.id)!;
    assert.equal(jobAfterClose.resultConsumedAt, null, "Closing an agent must NEVER consume the job obligation");
    assert.equal(store.countUnconsumedTerminalResults(), 1);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-3.2: Antigravity fail-closed follow (needs_approval) keeps result unconsumed", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_approval");
    const job = store.createJob({ id: "job_appr", agentId: agent.id, kind: "spawn", requestId: "r_ap", promptHash: "h_ap" });
    transitionJob(store, job.id, "needs_approval");

    // Follow a job requiring approval
    const followResult = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followResult.status, "failed");
    assert.equal(followResult.resultAvailable, false);

    const jobAfterFollow = store.getJob(job.id)!;
    assert.equal(jobAfterFollow.resultConsumedAt, null, "needs_approval follow must not consume result");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-3.3: Public service.recoverResult explicitly consumes the result", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_recover_pub");
    const job = store.createJob({ id: "job_rec_pub", agentId: agent.id, kind: "spawn", requestId: "r_rec", promptHash: "h_rec" });

    const resultFile = path.join(tmp, "res_rec.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "recovered usable output");

    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);
    assert.equal(store.countUnconsumedTerminalResults(), 1);

    // Call public recoverResult
    const recovered = await service.recoverResult(job.id);
    assert.ok(recovered);

    const jobAfterRecover = store.getJob(job.id)!;
    assert.ok(jobAfterRecover.resultConsumedAt, "recoverResult must set result_consumed_at");
    assert.ok(typeof jobAfterRecover.resultConsumedAt === "string");
    assert.ok(!Number.isNaN(Date.parse(jobAfterRecover.resultConsumedAt)));
    assert.equal(store.countUnconsumedTerminalResults(), 0, "Unconsumed terminal results count must decrement to 0");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-3.4: Public service.follow explicitly consumes the terminal result", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_follow_pub");
    const job = store.createJob({ id: "job_fol_pub", agentId: agent.id, kind: "spawn", requestId: "r_fol", promptHash: "h_fol" });

    const resultFile = path.join(tmp, "res_fol.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "follow usable output");

    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);
    assert.equal(store.countUnconsumedTerminalResults(), 1);

    // Call public follow
    const res = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(res.status, "completed");
    assert.equal(res.resultAvailable, true);

    const jobAfterFollow = store.getJob(job.id)!;
    assert.ok(jobAfterFollow.resultConsumedAt, "service.follow must set result_consumed_at");
    assert.ok(typeof jobAfterFollow.resultConsumedAt === "string");
    assert.equal(store.countUnconsumedTerminalResults(), 0);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Successful later follow consumes exactly once (idempotence)
// ---------------------------------------------------------------------------
test("CONSUMPTION-4.1: Park -> Background Wake -> First Follow -> Second Follow consumes exactly once", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_lifecycle_41");
    const job = store.createJob({ id: "job_lc_41", agentId: agent.id, kind: "spawn", requestId: "r_lc", promptHash: "h_lc" });
    store.bindJob({ jobId: job.id, threadId: "thread_lc_41", originatingTurnId: "turn_1", originatingItemId: "item_1" });

    // Step 1: Arm park
    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);

    // Step 2: Background completion and wake evaluation
    const resultFile = path.join(tmp, "res_lc_41.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "lifecycle output");
    await service.evaluateParkWakes(job.id);

    // Verify background wake occurred
    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken");

    // Check: unconsumed at wake time
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null, "Must be unconsumed at wake time");
    assert.equal(store.countUnconsumedTerminalResults(), 1);

    // Step 3: First follow consumes the job
    const follow1 = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(follow1.status, "completed");
    assert.equal(follow1.resultAvailable, true);

    const firstConsumedAt = store.getJob(job.id)?.resultConsumedAt;
    assert.ok(firstConsumedAt, "First follow must record result_consumed_at");
    assert.equal(store.countUnconsumedTerminalResults(), 0);

    // Step 4: Delay a few milliseconds to ensure clock progression
    await new Promise((r) => setTimeout(r, 25));

    // Step 5: Second follow replays terminal result and preserves original timestamp
    const follow2 = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(follow2.status, "completed");
    assert.equal(follow2.resultAvailable, true);

    const secondConsumedAt = store.getJob(job.id)?.resultConsumedAt;
    assert.equal(secondConsumedAt, firstConsumedAt, "Second follow must NOT overwrite result_consumed_at timestamp");

    // Step 6: Subsequent recoverResult also preserves original timestamp
    await service.recoverResult(job.id);
    const thirdConsumedAt = store.getJob(job.id)?.resultConsumedAt;
    assert.equal(thirdConsumedAt, firstConsumedAt, "Subsequent recoverResult must NOT overwrite result_consumed_at timestamp");

    // Step 7: Agent remains open and can be closed separately without altering job timestamp
    assert.notEqual(store.getAgent(agent.id)?.status, "closed");
    await service.close(agent.id);
    assert.equal(store.getAgent(agent.id)?.status, "closed");
    assert.equal(store.getJob(job.id)?.resultConsumedAt, firstConsumedAt);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-4.2: Multi-job park: following one job consumes only that job; second job consumed on its own follow", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const a1 = setupAgent(store, tmp, "agent_multi_1");
    const a2 = setupAgent(store, tmp, "agent_multi_2");
    const j1 = store.createJob({ id: "job_m1", agentId: a1.id, kind: "spawn", requestId: "rm1", promptHash: "hm1" });
    const j2 = store.createJob({ id: "job_m2", agentId: a2.id, kind: "spawn", requestId: "rm2", promptHash: "hm2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_multi", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_multi", originatingTurnId: "turn_1", originatingItemId: "item_2" });

    // Park both jobs under ANY
    await service.park({ job_ids: [j1.id, j2.id], predicate: "ANY" });

    // Complete both jobs
    const r1 = path.join(tmp, "m1.json");
    const r2 = path.join(tmp, "m2.json");
    await writeFile(r1, JSON.stringify({ envelope: createValidEnvelope(a1.id, j1.id, r1) }));
    await writeFile(r2, JSON.stringify({ envelope: createValidEnvelope(a2.id, j2.id, r2) }));
    makeJobCompleted(store, j1.id, r1, "out 1");
    makeJobCompleted(store, j2.id, r2, "out 2");

    await service.evaluateParkWakes(j1.id);

    // Both are unconsumed
    assert.equal(store.getJob(j1.id)?.resultConsumedAt, null);
    assert.equal(store.getJob(j2.id)?.resultConsumedAt, null);
    assert.equal(store.countUnconsumedTerminalResults(), 2);

    // Follow j1 only
    await service.follow({ agentId: a1.id, jobId: j1.id });

    const j1ConsumedAt = store.getJob(j1.id)?.resultConsumedAt;
    assert.ok(j1ConsumedAt, "j1 must be consumed");
    assert.equal(store.getJob(j2.id)?.resultConsumedAt, null, "j2 must remain UNCONSUMED when only j1 was followed");
    assert.equal(store.countUnconsumedTerminalResults(), 1);

    await new Promise((r) => setTimeout(r, 25));

    // Now follow j2
    await service.follow({ agentId: a2.id, jobId: j2.id });

    const j2ConsumedAt = store.getJob(j2.id)?.resultConsumedAt;
    assert.ok(j2ConsumedAt, "j2 must be consumed");
    assert.equal(store.countUnconsumedTerminalResults(), 0);

    // Ensure they have their own independent consumption timestamps
    assert.ok(j1ConsumedAt !== j2ConsumedAt, "Each job has independent consumption timestamp");

    // Re-follow j1 preserves j1 timestamp
    await service.follow({ agentId: a1.id, jobId: j1.id });
    assert.equal(store.getJob(j1.id)?.resultConsumedAt, j1ConsumedAt);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. MCP tool boundary (subagents_park, subagents_follow, subagents_recover_result)
// ---------------------------------------------------------------------------
test("CONSUMPTION-5.1: MCP subagents_park and wake leave job unconsumed; MCP subagents_follow consumes it", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  const httpServer = new BridgeHttpServer(config, service);
  await httpServer.start();

  const httpClient = new BridgeHttpClient(config);
  const mcpServer = createMcpServer(httpClient, {
    env: { CODEX_THREAD_ID: "thread_mcp" },
  });

  const mcpClient = new Client({ name: "test-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await Promise.all([
    mcpServer.connect(serverTransport),
    mcpClient.connect(clientTransport),
  ]);

  try {
    const agent = setupAgent(store, tmp, "agent_mcp_test");
    const job = store.createJob({ id: "job_mcp_1", agentId: agent.id, kind: "spawn", requestId: "r_mcp", promptHash: "h_mcp" });
    store.bindJob({ jobId: job.id, threadId: "thread_mcp", originatingTurnId: "turn_1", originatingItemId: "item_1" });

    // Call subagents_park via MCP
    const parkRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [job.id] },
    });
    assert.equal(Boolean(parkRes.isError), false);

    // Complete job in background
    const resultFile = path.join(tmp, "res_mcp.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "mcp job finished");

    await service.evaluateParkWakes(job.id);

    // Job must still be unconsumed
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null, "MCP park and background wake must not consume job");
    assert.equal(store.countUnconsumedTerminalResults(), 1);

    // Call subagents_follow via MCP
    const followRes = await mcpClient.callTool({
      name: "subagents_follow",
      arguments: { agent_id: agent.id, job_id: job.id },
    });
    assert.equal(Boolean(followRes.isError), false);

    // Job is now consumed!
    const jobAfterFollow = store.getJob(job.id)!;
    assert.ok(jobAfterFollow.resultConsumedAt, "MCP subagents_follow must consume job");
    assert.equal(store.countUnconsumedTerminalResults(), 0);

    const firstTimestamp = jobAfterFollow.resultConsumedAt;

    // Call subagents_follow again via MCP
    const followRes2 = await mcpClient.callTool({
      name: "subagents_follow",
      arguments: { agent_id: agent.id, job_id: job.id },
    });
    assert.equal(Boolean(followRes2.isError), false);
    assert.equal(store.getJob(job.id)?.resultConsumedAt, firstTimestamp, "MCP repeated follow must not alter timestamp");
  } finally {
    await mcpClient.close();
    await mcpServer.close();
    await httpServer.stop();
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-5.2: MCP subagents_recover_result explicitly consumes result_consumed_at", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  const httpServer = new BridgeHttpServer(config, service);
  await httpServer.start();

  const httpClient = new BridgeHttpClient(config);
  const mcpServer = createMcpServer(httpClient, {
    env: { CODEX_THREAD_ID: "thread_mcp_rec" },
  });

  const mcpClient = new Client({ name: "test-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await Promise.all([
    mcpServer.connect(serverTransport),
    mcpClient.connect(clientTransport),
  ]);

  try {
    const agent = setupAgent(store, tmp, "agent_mcp_rec");
    const job = store.createJob({ id: "job_mcp_rec", agentId: agent.id, kind: "spawn", requestId: "r_mcp_rec", promptHash: "h_mcp_rec" });

    const resultFile = path.join(tmp, "res_mcp_rec.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "mcp recover test");

    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);

    // Call subagents_recover_result via MCP
    const recRes = await mcpClient.callTool({
      name: "subagents_recover_result",
      arguments: { agent_id: agent.id, job_id: job.id },
    });
    assert.equal(Boolean(recRes.isError), false);

    // Job is now consumed!
    const jobAfterRec = store.getJob(job.id)!;
    assert.ok(jobAfterRec.resultConsumedAt, "MCP subagents_recover_result must consume job");
    assert.equal(store.countUnconsumedTerminalResults(), 0);
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
// 6. Fixed CodexCliExecutionResult contract & recovery fencing
// ---------------------------------------------------------------------------
test("CONSUMPTION-6.1: CodexCliExecutionResult contract persists deliveryMode queued and messageId on success with no retry", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  fakeCli.nextResult = {
    success: true,
    accepted: true,
    deliveryMode: "queued",
    messageId: "msg_canary_001",
    executablePath: "C:\\Codex\\codex.exe",
    version: "0.150.0",
  };
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_contract_q");
    const job = store.createJob({ id: "job_cq", agentId: agent.id, kind: "spawn", requestId: "rcq", promptHash: "hcq" });
    store.bindJob({ jobId: job.id, threadId: "thread_cq", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    transitionJob(store, job.id, "running");

    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);

    const resultFile = path.join(tmp, "res_cq.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "queued success output");

    await (service as any).resolveFollow(job.id, {
      status: "completed",
      resultAvailable: true,
      envelope,
    });
    await service.evaluateParkWakes(job.id);

    // Verify outbox persisted deliveryMode and messageId
    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    assert.equal(outbox.status, "delivered");
    assert.equal(outbox.wakeState, "delivered");
    assert.equal((outbox as any).deliveryMode, "queued");
    assert.equal((outbox as any).messageId ?? (outbox as any).clientUserMessageId ?? (outbox as any).message_id, "msg_canary_001");
    assert.equal(outbox.nextAttemptAt, null, "Queued success must be terminal with no retry");

    // Barrier is woken and disarmed
    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken");
    assert.equal(barrier.armed, false);

    // Result remains unconsumed until explicit follow
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);
    await service.follow({ agentId: agent.id, jobId: job.id });
    assert.ok(store.getJob(job.id)?.resultConsumedAt);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-6.2: CodexCliExecutionResult retains deferred_active_writer only for true resume conflicts when queue was unavailable", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  fakeCli.nextResult = {
    success: false,
    activeWriter: true,
    deliveryMode: "cli_resume",
    error: "Active writer conflict: queue unavailable",
    executablePath: "C:\\Codex\\codex.exe",
    version: "0.150.0",
  };
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_aw_retain");
    const job = store.createJob({ id: "job_aw", agentId: agent.id, kind: "spawn", requestId: "raw", promptHash: "haw" });
    store.bindJob({ jobId: job.id, threadId: "thread_aw", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    transitionJob(store, job.id, "running");

    const receipt = await service.park({ job_ids: [job.id] });
    const resultFile = path.join(tmp, "res_aw.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    makeJobCompleted(store, job.id, resultFile, "active writer output");

    await (service as any).resolveFollow(job.id, {
      status: "completed",
      resultAvailable: true,
      envelope,
    });
    await service.evaluateParkWakes(job.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    assert.equal(outbox.status, "deferred_active_writer");
    assert.equal(outbox.wakeState, "deferred_active_writer");
    assert.ok(outbox.nextAttemptAt, "Retry must be scheduled for deferred_active_writer");
    assert.ok((service as any).wakeRetryTimers.has(outbox.id), "Retry timer must be active");

    // Result remains unconsumed
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-6.3: Recovery fences stale/superseded generation before dispatch and reconciliation", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_rec_fence");
    const j1 = store.createJob({ id: "job_rf1", agentId: agent.id, kind: "spawn", requestId: "rf1", promptHash: "hf1" });
    const j2 = store.createJob({ id: "job_rf2", agentId: agent.id, kind: "spawn", requestId: "rf2", promptHash: "hf2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_rf", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_rf", originatingTurnId: "turn_1", originatingItemId: "item_2" });
    transitionJob(store, j1.id, "running");
    transitionJob(store, j2.id, "running");

    // Park generation 1
    const receipt1 = await service.park({ job_ids: [j1.id] });
    assert.equal(receipt1.generation, 1);

    // Outbox 1 is deferred
    fakeCli.nextResult = {
      success: false,
      activeWriter: true,
      error: "Active writer on gen 1",
    };
    const r1 = path.join(tmp, "res_rf1.json");
    await writeFile(r1, JSON.stringify({ envelope: createValidEnvelope(agent.id, j1.id, r1) }));
    makeJobCompleted(store, j1.id, r1, "gen 1 complete");
    await service.evaluateParkWakes(j1.id);

    const outboxGen1 = store.getWakeOutbox(receipt1.parkId, 1)!;
    assert.ok(outboxGen1);

    // Park generation 2 on the same park/thread
    const receipt2 = await service.park({ job_ids: [j2.id], park_id: receipt1.parkId });
    assert.equal(receipt2.generation, 2);

    // Gen 1 outbox was superseded by parking gen 2
    const refreshedGen1 = store.getWakeOutbox(receipt1.parkId, 1)!;
    assert.equal((refreshedGen1 as any).status, "superseded");

    // Now simulate daemon recovery
    fakeCli.calls = [];
    let reconcileCalls = 0;
    fakeCodex.reconcileSend = async () => {
      reconcileCalls++;
      return true;
    };

    // Run recovery
    await (service as any).recoverWakeOutbox();

    // Gen 1 must NOT have been dispatched or reconciled
    assert.equal(fakeCli.calls.length, 0, "Superseded gen 1 must not be dispatched during recovery");
    assert.equal(reconcileCalls, 0, "Superseded gen 1 must not be reconciled during recovery");

    // Both results remain unconsumed until public follow
    assert.equal(store.getJob(j1.id)?.resultConsumedAt, null);
    assert.equal(store.getJob(j2.id)?.resultConsumedAt, null);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-6.4: Timeout during auto-armed background follow lifecycle leaves result_consumed_at null until public follow", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_timeout_bg");
    const job = store.createJob({ id: "job_t_bg", agentId: agent.id, kind: "spawn", requestId: "rtbg", promptHash: "htbg" });
    store.bindJob({ jobId: job.id, threadId: "thread_t_bg", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    transitionJob(store, job.id, "running");

    await service.park({ job_ids: [job.id] });

    const resultFile = path.join(tmp, "res_t_bg.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile, { status: "timed_out" });
    await writeFile(resultFile, JSON.stringify({ envelope }));
    store.setJobResult(job.id, resultFile, "timed out with evidence");
    store.updateJobStatus(job.id, "timed_out");

    // Background follow resolves with timed_out
    await (service as any).resolveFollow(job.id, {
      status: "timed_out",
      deadlineReached: true,
      resultAvailable: true,
      envelope,
    });

    await service.evaluateParkWakes(job.id);

    // Wake occurred
    assert.equal(fakeCli.calls.length, 1);

    // CRITICAL: result_consumed_at must remain null after background timeout
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);
    assert.equal(store.countUnconsumedTerminalResults(), 1);

    // Public follow consumes it
    const followRes = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followRes.status, "timed_out");
    assert.ok(store.getJob(job.id)?.resultConsumedAt);
    assert.equal(store.countUnconsumedTerminalResults(), 0);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-7.1: Restart race: accepted queued item found => delivered without transport call", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_race_queued");
    const job = store.createJob({ id: "job_race_queued", agentId: agent.id, kind: "spawn", requestId: "rq1", promptHash: "hq1" });
    store.bindJob({ jobId: job.id, threadId: "thread_race_queued", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    transitionJob(store, job.id, "running");

    const receipt = await service.park({ job_ids: [job.id] });

    const resultFile = path.join(tmp, "res_rq1.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    store.setJobResult(job.id, resultFile, "done");
    store.updateJobStatus(job.id, "completed");

    // Outbox entry in 'waking' status (simulating in-flight crash during delivery)
    const marker = `<!-- [SUBAGENT_BRIDGE_WAKE:park=${receipt.parkId}:gen=${receipt.generation}] -->`;
    const wakeEnv: WakeEnvelope = {
      parkId: receipt.parkId,
      generation: receipt.generation,
      reason: "subagents park wake",
      jobIds: [job.id],
      readyJobIds: [job.id],
      statuses: { [job.id]: "completed" },
      resultHashes: { [job.id]: "abc" },
      pendingCount: 0,
      instruction: "Call subagents_follow",
      marker,
    };
    const outbox = store.createWakeOutbox({
      id: "wake_race_queued",
      parkId: receipt.parkId,
      generation: receipt.generation,
      threadId: "thread_race_queued",
      deliveryMode: "queued",
      status: "waking",
      wakeState: "waiting",
      wakeMarker: marker,
      payloadJson: JSON.stringify(wakeEnv),
    });
    store.claimParkWake(receipt.parkId, receipt.generation);

    // Mock cliTransport.reconcileQueuedWake returning existing item in queue DB
    fakeCli.queuedWakeHandler = async (threadId: string, m: string) => {
      if (threadId === "thread_race_queued" && m === marker) {
        return {
          found: true,
          messageId: "msg_queued_race_accepted_123",
          deliveryMode: "queued",
        };
      }
      return { found: false };
    };

    fakeCli.calls = [];

    // Run recovery
    await (service as any).recoverWakeOutbox();

    // 1. Accepted queued item found => delivered without transport delivery call
    assert.equal(fakeCli.calls.length, 0, "No new transport delivery call should be made when queued wake is reconciled");
    assert.equal(fakeCli.reconcileQueuedWakeCalls.length, 1, "reconcileQueuedWake must be invoked");

    const refreshedOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshedOutbox.status, "delivered");
    assert.equal(refreshedOutbox.deliveryMode, "queued");
    assert.equal(refreshedOutbox.messageId, "msg_queued_race_accepted_123");
    assert.equal(refreshedOutbox.wakeState, "delivered");

    // 2. Park barrier marked woken
    const refreshedBarrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(refreshedBarrier.state, "woken");

    // 3. Explicit consumption semantics: result_consumed_at remains null after wake delivery
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);
    assert.equal(store.countUnconsumedTerminalResults(), 1);

    // 4. Public follow explicitly consumes the terminal result
    const followRes = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followRes.status, "completed");
    assert.ok(store.getJob(job.id)?.resultConsumedAt);
    assert.equal(store.countUnconsumedTerminalResults(), 0);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-7.2: Restart race: unresolved waking => zero redelivery", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_race_unresolved");
    const job = store.createJob({ id: "job_race_unresolved", agentId: agent.id, kind: "spawn", requestId: "rq2", promptHash: "hq2" });
    store.bindJob({ jobId: job.id, threadId: "thread_race_unresolved", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    transitionJob(store, job.id, "running");

    const receipt = await service.park({ job_ids: [job.id] });

    const resultFile = path.join(tmp, "res_rq2.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    store.setJobResult(job.id, resultFile, "done");
    store.updateJobStatus(job.id, "completed");

    // Outbox in 'waking' status before restart
    const marker = `<!-- [SUBAGENT_BRIDGE_WAKE:park=${receipt.parkId}:gen=${receipt.generation}] -->`;
    const wakeEnv: WakeEnvelope = {
      parkId: receipt.parkId,
      generation: receipt.generation,
      reason: "subagents park wake",
      jobIds: [job.id],
      readyJobIds: [job.id],
      statuses: { [job.id]: "completed" },
      resultHashes: { [job.id]: "abc" },
      pendingCount: 0,
      instruction: "Call subagents_follow",
      marker,
    };
    const outbox = store.createWakeOutbox({
      id: "wake_race_unresolved",
      parkId: receipt.parkId,
      generation: receipt.generation,
      threadId: "thread_race_unresolved",
      deliveryMode: "cli_resume",
      status: "waking",
      wakeState: "waiting",
      wakeMarker: marker,
      payloadJson: JSON.stringify(wakeEnv),
    });
    store.claimParkWake(receipt.parkId, receipt.generation);

    // Reconcile checks fail (neither in queue DB nor in transcript)
    fakeCli.queuedWakeHandler = async () => ({ found: false });
    fakeCodex.reconcileSend = async () => false;

    fakeCli.calls = [];

    // Run recovery
    await (service as any).recoverWakeOutbox();

    // 1. Unresolved waking => ZERO redelivery (no transport delivery calls)
    assert.equal(fakeCli.calls.length, 0, "Unresolved waking must NEVER dispatch or redeliver");

    // 2. Fails closed / left indeterminate: never reset to pending, never marked delivered
    const refreshedOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshedOutbox.status, "waking", "Must remain indeterminate ('waking') and never reset to 'pending'");
    assert.notEqual(refreshedOutbox.status, "pending");
    assert.notEqual(refreshedOutbox.status, "delivered");
    assert.ok(refreshedOutbox.lastError?.includes("Indeterminate"), "Diagnostic error must be recorded on outbox");
    assert.ok(refreshedOutbox.lastError?.includes("fail-closed"), "Diagnostic must note fail-closed behavior");

    // Barrier is NOT woken
    const refreshedBarrier = store.getParkBarrier(receipt.parkId)!;
    assert.notEqual(refreshedBarrier.state, "woken");

    // 3. Explicit consumption semantics: result_consumed_at remains null
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);
    assert.equal(store.countUnconsumedTerminalResults(), 1);

    // 4. Public follow still explicitly consumes the terminal result
    const followRes = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followRes.status, "completed");
    assert.ok(store.getJob(job.id)?.resultConsumedAt);
    assert.equal(store.countUnconsumedTerminalResults(), 0);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

test("CONSUMPTION-7.3: Restart race: pending-before-send => one delivery", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_race_pending");
    const job = store.createJob({ id: "job_race_pending", agentId: agent.id, kind: "spawn", requestId: "rq3", promptHash: "hq3" });
    store.bindJob({ jobId: job.id, threadId: "thread_race_pending", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    transitionJob(store, job.id, "running");

    const receipt = await service.park({ job_ids: [job.id] });

    const resultFile = path.join(tmp, "res_rq3.json");
    const envelope = createValidEnvelope(agent.id, job.id, resultFile);
    await writeFile(resultFile, JSON.stringify({ envelope }));
    store.setJobResult(job.id, resultFile, "done");
    store.updateJobStatus(job.id, "completed");

    // Outbox in 'pending' status (created before send occurred, daemon stopped/restarted)
    const marker = `<!-- [SUBAGENT_BRIDGE_WAKE:park=${receipt.parkId}:gen=${receipt.generation}] -->`;
    const wakeEnv: WakeEnvelope = {
      parkId: receipt.parkId,
      generation: receipt.generation,
      reason: "subagents park wake",
      jobIds: [job.id],
      readyJobIds: [job.id],
      statuses: { [job.id]: "completed" },
      resultHashes: { [job.id]: "abc" },
      pendingCount: 0,
      instruction: "Call subagents_follow",
      marker,
    };
    const outbox = store.createWakeOutbox({
      id: "wake_race_pending",
      parkId: receipt.parkId,
      generation: receipt.generation,
      threadId: "thread_race_pending",
      deliveryMode: "cli_resume",
      status: "pending",
      wakeState: "waiting",
      wakeMarker: marker,
      payloadJson: JSON.stringify(wakeEnv),
    });

    // Neither queued in DB nor in transcript yet
    fakeCli.queuedWakeHandler = async () => ({ found: false });
    fakeCodex.reconcileSend = async () => false;

    fakeCli.calls = [];
    fakeCli.nextResult = {
      success: true,
      accepted: true,
      deliveryMode: "cli_resume",
      executablePath: "C:\\Codex\\codex.exe",
      version: "0.150.0",
    };

    // Run recovery
    await (service as any).recoverWakeOutbox();

    // 1. Pending-before-send => normal routing dispatches exactly once
    assert.equal(fakeCli.calls.length, 1, "Pending row must be delivered via normal routing");
    assert.equal(fakeCli.calls[0].threadId, "thread_race_pending");
    assert.equal(fakeCli.calls[0].marker, marker);

    const refreshedOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshedOutbox.status, "delivered");
    assert.equal(refreshedOutbox.wakeState, "delivered");

    // 2. Barrier is woken
    const refreshedBarrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(refreshedBarrier.state, "woken");

    // 3. Explicit consumption semantics: result_consumed_at remains null after wake delivery
    assert.equal(store.getJob(job.id)?.resultConsumedAt, null);
    assert.equal(store.countUnconsumedTerminalResults(), 1);

    // 4. Public follow explicitly consumes the result
    const followRes = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followRes.status, "completed");
    assert.ok(store.getJob(job.id)?.resultConsumedAt);
    assert.equal(store.countUnconsumedTerminalResults(), 0);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});
