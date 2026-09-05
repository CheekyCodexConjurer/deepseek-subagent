import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
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
  OpenCodeClientLike,
  OpenCodeMessage,
  WakeEnvelope,
} from "../../src/types.js";
import { InvalidRequestError } from "../../src/errors.js";
import type { CodexCliTransport, CodexCliExecutionResult } from "../../src/codex/cli-resolver.js";

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
  reconcileSendCalls: Array<{ threadId: string; marker: string }> = [];
  reconcileSendResult = false;
  async reconcileSend(threadId: string, marker: string): Promise<boolean> {
    this.reconcileSendCalls.push({ threadId, marker });
    return this.reconcileSendResult;
  }
  onCorrelation(_listener: (correlation: CodexCorrelation) => void): () => void {
    return () => undefined;
  }
}

class FakeCliTransport implements CodexCliTransport {
  calls: Array<{ threadId: string; marker: string }> = [];
  nextResult: CodexCliExecutionResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };
  compatible = true;
  reconcileQueuedWakeCalls: Array<{ threadId: string; marker: string }> = [];
  queuedWakeHandler: ((threadId: string, marker: string) => Promise<{ found: boolean; messageId?: string | null; deliveryMode?: string } | boolean | null>) | null = null;

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
    return { found: false };
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

async function createTestEnv() {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "ds-wake-v3-"));
  const config = createDefaultConfig({
    dataDir: tmp,
    configPath: path.join(tmp, "config.json"),
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
    topic: "Testing v3 wake and predicates",
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

function makeJobCompleted(store: BridgeStore, jobId: string, resultPath?: string, resultSummary?: string) {
  store.updateJobStatus(jobId, "dispatching");
  store.updateJobStatus(jobId, "running");
  if (resultPath) {
    store.setJobResult(jobId, resultPath, resultSummary ?? "completed result");
  }
  store.updateJobStatus(jobId, "completed");
}

function makeJobDelivered(store: BridgeStore, jobId: string, resultPath?: string, resultSummary?: string) {
  makeJobCompleted(store, jobId, resultPath, resultSummary);
  store.updateJobStatus(jobId, "delivered");
}

// ---------------------------------------------------------------------------
// 1. Predicate ANY: wakes immediately on the first eligible job
// ---------------------------------------------------------------------------
test("WAKE-V3: Predicate ANY wakes immediately when any one job is eligible", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_any");
    const j1 = store.createJob({ id: "job_any_1", agentId: agent.id, kind: "spawn", requestId: "r1", promptHash: "h1" });
    const j2 = store.createJob({ id: "job_any_2", agentId: agent.id, kind: "spawn", requestId: "r2", promptHash: "h2" });
    const j3 = store.createJob({ id: "job_any_3", agentId: agent.id, kind: "spawn", requestId: "r3", promptHash: "h3" });
    store.bindJob({ jobId: j1.id, threadId: "thread_any", originatingTurnId: "turn_1", originatingItemId: "item_1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_any", originatingTurnId: "turn_1", originatingItemId: "item_2" });
    store.bindJob({ jobId: j3.id, threadId: "thread_any", originatingTurnId: "turn_1", originatingItemId: "item_3" });

    const receipt = await service.park({ job_ids: [j1.id, j2.id, j3.id], predicate: "ANY" });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.predicateType, "ANY");

    // Barrier starts armed with 0 ready jobs
    let barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed");
    assert.equal(store.getWakeOutbox(receipt.parkId, receipt.generation), null);

    // Only j1 completes
    const r1 = path.join(tmp, "r1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "j1 result" } }));
    makeJobCompleted(store, j1.id, r1, "j1 result");

    await service.evaluateParkWakes(j1.id);

    barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken", "ANY predicate must wake when 1 job completes");
    assert.equal(barrier.armed, false);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    assert.equal(outbox.status, "delivered");
    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.deepEqual(envelope.readyJobIds, [j1.id]);
    assert.equal(envelope.pendingCount, 2);
    assert.equal(fakeCli.calls.length, 1);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Predicate ALL: requires all jobs eligible before waking
// ---------------------------------------------------------------------------
test("WAKE-V3: Predicate ALL requires all jobs to be eligible before waking", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_all");
    const j1 = store.createJob({ id: "job_all_1", agentId: agent.id, kind: "spawn", requestId: "r1", promptHash: "h1" });
    const j2 = store.createJob({ id: "job_all_2", agentId: agent.id, kind: "spawn", requestId: "r2", promptHash: "h2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_all", originatingTurnId: "turn_all", originatingItemId: "i1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_all", originatingTurnId: "turn_all", originatingItemId: "i2" });

    const receipt = await service.park({ job_ids: [j1.id, j2.id], predicate: "ALL", wake_on_exception: false });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.predicateType, "ALL");

    // j1 completes with result
    const r1 = path.join(tmp, "r_all_1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "all 1" } }));
    makeJobCompleted(store, j1.id, r1, "all 1");

    await service.evaluateParkWakes(j1.id);

    // Barrier must still be armed (not waking or woken)
    let barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed", "ALL predicate must remain armed when only 1 of 2 jobs is ready");
    assert.equal(store.getWakeOutbox(receipt.parkId, receipt.generation), null);

    // j2 completes with result
    const r2 = path.join(tmp, "r_all_2.json");
    await writeFile(r2, JSON.stringify({ envelope: { summary: "all 2" } }));
    makeJobCompleted(store, j2.id, r2, "all 2");

    await service.evaluateParkWakes(j2.id);

    barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken", "ALL predicate must wake when all jobs are ready");
    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    assert.equal(outbox.status, "delivered");
    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.equal(envelope.readyJobIds.length, 2);
    assert.equal(envelope.pendingCount, 0);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Predicate QUORUM(k): validates bounds and wakes when threshold k is reached
// ---------------------------------------------------------------------------
test("WAKE-V3: Predicate QUORUM(k) validates count and wakes when threshold k is reached", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_quorum");
    const j1 = store.createJob({ id: "job_q1", agentId: agent.id, kind: "spawn", requestId: "rq1", promptHash: "hq1" });
    const j2 = store.createJob({ id: "job_q2", agentId: agent.id, kind: "spawn", requestId: "rq2", promptHash: "hq2" });
    const j3 = store.createJob({ id: "job_q3", agentId: agent.id, kind: "spawn", requestId: "rq3", promptHash: "hq3" });
    store.bindJob({ jobId: j1.id, threadId: "thread_q", originatingTurnId: "tq", originatingItemId: "iq1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_q", originatingTurnId: "tq", originatingItemId: "iq2" });
    store.bindJob({ jobId: j3.id, threadId: "thread_q", originatingTurnId: "tq", originatingItemId: "iq3" });

    // Validation checks
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id, j3.id], predicate: "QUORUM", quorum_count: 0 }),
      (err: unknown) => err instanceof InvalidRequestError && (err as InvalidRequestError).status === 400,
    );
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id, j3.id], predicate: "QUORUM", quorum_count: 4 }),
      (err: unknown) => err instanceof InvalidRequestError && (err as InvalidRequestError).status === 400,
    );
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id, j3.id], predicate: "QUORUM", quorum_count: -1 }),
      (err: unknown) => err instanceof InvalidRequestError,
    );
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id, j3.id], predicate: "QUORUM", quorum_count: 1.5 as any }),
      (err: unknown) => err instanceof InvalidRequestError,
    );

    // Park with QUORUM(2)
    const receipt = await service.park({
      job_ids: [j1.id, j2.id, j3.id],
      predicate: "QUORUM",
      quorum_count: 2,
      wake_on_exception: false,
    });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.predicateType, "QUORUM");
    assert.equal(receipt.quorumCount, 2);

    // j1 completes -> 1 of 2 -> still armed
    const r1 = path.join(tmp, "rq1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "q1 done" } }));
    makeJobCompleted(store, j1.id, r1, "q1 done");
    await service.evaluateParkWakes(j1.id);

    let barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed", "Quorum(2) must remain armed after 1 completion");
    assert.equal(store.getWakeOutbox(receipt.parkId, receipt.generation), null);

    // j2 completes -> 2 of 2 -> wakes!
    const r2 = path.join(tmp, "rq2.json");
    await writeFile(r2, JSON.stringify({ envelope: { summary: "q2 done" } }));
    makeJobCompleted(store, j2.id, r2, "q2 done");
    await service.evaluateParkWakes(j2.id);

    barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken", "Quorum(2) must wake when 2 jobs are ready");

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.deepEqual(envelope.readyJobIds.sort(), [j1.id, j2.id].sort());
    assert.equal(envelope.pendingCount, 1, "Remaining 1 job is pending and does not block quorum wake");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Predicate REQUIRED(job IDs): validates job IDs and wakes only when required IDs are ready
// ---------------------------------------------------------------------------
test("WAKE-V3: Predicate REQUIRED(job IDs) validates inputs and requires specific job IDs to wake", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_required");
    const j1 = store.createJob({ id: "job_req1", agentId: agent.id, kind: "spawn", requestId: "rr1", promptHash: "hr1" });
    const j2 = store.createJob({ id: "job_req2", agentId: agent.id, kind: "spawn", requestId: "rr2", promptHash: "hr2" });
    const j3 = store.createJob({ id: "job_req3", agentId: agent.id, kind: "spawn", requestId: "rr3", promptHash: "hr3" });
    store.bindJob({ jobId: j1.id, threadId: "thread_req", originatingTurnId: "tr", originatingItemId: "ir1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_req", originatingTurnId: "tr", originatingItemId: "ir2" });
    store.bindJob({ jobId: j3.id, threadId: "thread_req", originatingTurnId: "tr", originatingItemId: "ir3" });

    // Validation checks
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id, j3.id], predicate: "REQUIRED", required_job_ids: [] }),
      (err: unknown) => err instanceof InvalidRequestError,
    );
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id, j3.id], predicate: "REQUIRED", required_job_ids: ["nonexistent_id"] }),
      (err: unknown) => err instanceof InvalidRequestError,
    );
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id, j3.id], predicate: "REQUIRED", required_job_ids: "not_an_array" as any }),
      (err: unknown) => err instanceof InvalidRequestError,
    );

    // Park requiring [j2, j3]
    const receipt = await service.park({
      job_ids: [j1.id, j2.id, j3.id],
      predicate: "REQUIRED",
      required_job_ids: [j2.id, j3.id],
      wake_on_exception: false,
    });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.predicateType, "REQUIRED");
    assert.deepEqual(receipt.requiredJobIds, [j2.id, j3.id]);

    // j1 completes (non-required job) -> must remain armed
    const r1 = path.join(tmp, "rr1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "r1 done" } }));
    makeJobCompleted(store, j1.id, r1, "r1 done");
    await service.evaluateParkWakes(j1.id);

    let barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed", "REQUIRED predicate must remain armed when only non-required job finishes");

    // j2 completes (one of required) -> still missing j3
    const r2 = path.join(tmp, "rr2.json");
    await writeFile(r2, JSON.stringify({ envelope: { summary: "r2 done" } }));
    makeJobCompleted(store, j2.id, r2, "r2 done");
    await service.evaluateParkWakes(j2.id);

    barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed", "REQUIRED predicate must remain armed when only 1 of 2 required jobs finishes");

    // j3 completes (second required job) -> wakes!
    const r3 = path.join(tmp, "rr3.json");
    await writeFile(r3, JSON.stringify({ envelope: { summary: "r3 done" } }));
    makeJobCompleted(store, j3.id, r3, "r3 done");
    await service.evaluateParkWakes(j3.id);

    barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken", "REQUIRED predicate must wake when all required jobs are ready");

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.ok(envelope.readyJobIds.includes(j2.id));
    assert.ok(envelope.readyJobIds.includes(j3.id));
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. delivered + result eligibility: isJobWakeEligible contract
// ---------------------------------------------------------------------------
test("WAKE-V3: isJobWakeEligible strictly requires resultPath for delivered/completed and handles terminal statuses", async () => {
  const { tmp, config, store } = await createTestEnv();
  const service = createTestService(config, store, new FakeCodexDelivery(), new FakeCliTransport());
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_eligibility");

    // completed without result -> false
    const jCompNoRes = store.createJob({ id: "j_comp_no_res", agentId: agent.id, kind: "spawn", requestId: "e1", promptHash: "h" });
    makeJobCompleted(store, jCompNoRes.id);
    assert.equal(service.isJobWakeEligible(store.getJob(jCompNoRes.id)!), false);

    // completed with result -> true
    const jCompRes = store.createJob({ id: "j_comp_res", agentId: agent.id, kind: "spawn", requestId: "e2", promptHash: "h" });
    const rfComp = path.join(tmp, "rf_comp.json");
    await writeFile(rfComp, "{}");
    makeJobCompleted(store, jCompRes.id, rfComp, "res");
    assert.equal(service.isJobWakeEligible(store.getJob(jCompRes.id)!), true);

    // delivered without result -> false
    const jDelivNoRes = store.createJob({ id: "j_deliv_no_res", agentId: agent.id, kind: "spawn", requestId: "e3", promptHash: "h" });
    makeJobDelivered(store, jDelivNoRes.id);
    assert.equal(service.isJobWakeEligible(store.getJob(jDelivNoRes.id)!), false);

    // delivered with result -> true
    const jDelivRes = store.createJob({ id: "j_deliv_res", agentId: agent.id, kind: "spawn", requestId: "e4", promptHash: "h" });
    const rfDeliv = path.join(tmp, "rf_deliv.json");
    await writeFile(rfDeliv, "{}");
    makeJobDelivered(store, jDelivRes.id, rfDeliv, "delivered res");
    assert.equal(service.isJobWakeEligible(store.getJob(jDelivRes.id)!), true);

    // completed_partial with result -> true, without result -> false
    const jPartial = store.createJob({ id: "j_partial", agentId: agent.id, kind: "spawn", requestId: "e5", promptHash: "h" });
    store.updateJobStatus(jPartial.id, "dispatching");
    store.updateJobStatus(jPartial.id, "running");
    store.updateJobStatus(jPartial.id, "following");
    store.updateJobStatus(jPartial.id, "completed_partial");
    assert.equal(service.isJobWakeEligible(store.getJob(jPartial.id)!), false);

    const rfPartial = path.join(tmp, "rf_partial.json");
    await writeFile(rfPartial, "{}");
    store.setJobResult(jPartial.id, rfPartial, "partial res");
    assert.equal(service.isJobWakeEligible(store.getJob(jPartial.id)!), true);

    // needs_approval -> true
    const jApproval = store.createJob({ id: "j_approval", agentId: agent.id, kind: "spawn", requestId: "e6", promptHash: "h" });
    store.updateJobStatus(jApproval.id, "dispatching");
    store.updateJobStatus(jApproval.id, "needs_approval");
    assert.equal(service.isJobWakeEligible(store.getJob(jApproval.id)!), true);

    // failed, aborted, timed_out -> true
    const jFailed = store.createJob({ id: "j_failed", agentId: agent.id, kind: "spawn", requestId: "e7", promptHash: "h" });
    store.updateJobStatus(jFailed.id, "failed");
    assert.equal(service.isJobWakeEligible(store.getJob(jFailed.id)!), true);

    const jAborted = store.createJob({ id: "j_aborted", agentId: agent.id, kind: "spawn", requestId: "e8", promptHash: "h" });
    store.updateJobStatus(jAborted.id, "aborted");
    assert.equal(service.isJobWakeEligible(store.getJob(jAborted.id)!), true);

    const jTimedOut = store.createJob({ id: "j_timed_out", agentId: agent.id, kind: "spawn", requestId: "e9", promptHash: "h" });
    store.updateJobStatus(jTimedOut.id, "dispatching");
    store.updateJobStatus(jTimedOut.id, "running");
    store.updateJobStatus(jTimedOut.id, "timed_out");
    assert.equal(service.isJobWakeEligible(store.getJob(jTimedOut.id)!), true);

    // running/dispatching -> false
    const jRun = store.createJob({ id: "j_running", agentId: agent.id, kind: "spawn", requestId: "e10", promptHash: "h" });
    store.updateJobStatus(jRun.id, "dispatching");
    assert.equal(service.isJobWakeEligible(store.getJob(jRun.id)!), false);
    store.updateJobStatus(jRun.id, "running");
    assert.equal(service.isJobWakeEligible(store.getJob(jRun.id)!), false);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 6. immediate failure / needs_approval defaults (wake_on_exception default true)
// ---------------------------------------------------------------------------
test("WAKE-V3: immediate failure and needs_approval wake by default even under ALL predicate", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_exc_defaults");
    const j1 = store.createJob({ id: "job_exc_1", agentId: agent.id, kind: "spawn", requestId: "rx1", promptHash: "hx1" });
    const j2 = store.createJob({ id: "job_exc_2", agentId: agent.id, kind: "spawn", requestId: "rx2", promptHash: "hx2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_exc", originatingTurnId: "tx", originatingItemId: "ix1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_exc", originatingTurnId: "tx", originatingItemId: "ix2" });

    // wake_on_exception omitted: defaults to true
    const receipt1 = await service.park({ job_ids: [j1.id, j2.id], predicate: "ALL" });
    assert.equal(receipt1.armed, true);

    // j1 transitions to needs_approval -> immediate wake
    store.updateJobStatus(j1.id, "dispatching");
    store.updateJobStatus(j1.id, "needs_approval");
    await service.evaluateParkWakes(j1.id);

    let barrier1 = store.getParkBarrier(receipt1.parkId)!;
    assert.equal(barrier1.state, "woken", "needs_approval must wake immediately under default wake_on_exception");

    // Test failure immediate wake on second barrier
    const j3 = store.createJob({ id: "job_exc_3", agentId: agent.id, kind: "spawn", requestId: "rx3", promptHash: "hx3" });
    const j4 = store.createJob({ id: "job_exc_4", agentId: agent.id, kind: "spawn", requestId: "rx4", promptHash: "hx4" });
    store.bindJob({ jobId: j3.id, threadId: "thread_exc_2", originatingTurnId: "tx2", originatingItemId: "ix3" });
    store.bindJob({ jobId: j4.id, threadId: "thread_exc_2", originatingTurnId: "tx2", originatingItemId: "ix4" });

    const receipt2 = await service.park({ job_ids: [j3.id, j4.id], predicate: "ALL" });
    assert.equal(receipt2.armed, true);

    store.updateJobStatus(j3.id, "failed");
    await service.evaluateParkWakes(j3.id);

    let barrier2 = store.getParkBarrier(receipt2.parkId)!;
    assert.equal(barrier2.state, "woken", "failed status must wake immediately under default wake_on_exception");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 7. impossible-quorum / all-terminal escape when failure wake is disabled
// ---------------------------------------------------------------------------
test("WAKE-V3: impossible-quorum/all-terminal escape wakes when failure wake is disabled", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_esc");

    // Part A: QUORUM(2) on 3 jobs with wake_on_exception: false
    // 1 failure leaves 2 live jobs (quorum still possible) -> remains armed.
    // 2 failures leave 1 live job (quorum impossible) -> impossible-quorum escape wakes!
    const q1 = store.createJob({ id: "job_esc_q1", agentId: agent.id, kind: "spawn", requestId: "req1", promptHash: "heq1" });
    const q2 = store.createJob({ id: "job_esc_q2", agentId: agent.id, kind: "spawn", requestId: "req2", promptHash: "heq2" });
    const q3 = store.createJob({ id: "job_esc_q3", agentId: agent.id, kind: "spawn", requestId: "req3", promptHash: "heq3" });
    store.bindJob({ jobId: q1.id, threadId: "thread_esc_q", originatingTurnId: "teq", originatingItemId: "ieq1" });
    store.bindJob({ jobId: q2.id, threadId: "thread_esc_q", originatingTurnId: "teq", originatingItemId: "ieq2" });
    store.bindJob({ jobId: q3.id, threadId: "thread_esc_q", originatingTurnId: "teq", originatingItemId: "ieq3" });

    const receiptQuorum = await service.park({
      job_ids: [q1.id, q2.id, q3.id],
      predicate: "QUORUM",
      quorum_count: 2,
      wake_on_exception: false,
    });
    assert.equal(receiptQuorum.armed, true);

    // q1 fails -> 1 failure, 2 live jobs remaining (quorum of 2 still possible) -> must remain armed
    store.updateJobStatus(q1.id, "failed");
    await service.evaluateParkWakes(q1.id);

    let barrierQuorum = store.getParkBarrier(receiptQuorum.parkId)!;
    assert.equal(barrierQuorum.state, "armed", "Quorum(2) remains armed after 1 failure while 2 jobs can still satisfy quorum");

    // q2 fails -> 2 failures, only 1 live job remaining (impossible to achieve quorum of 2 successful jobs)
    // The barrier escapes and wakes!
    store.updateJobStatus(q2.id, "failed");
    await service.evaluateParkWakes(q2.id);

    barrierQuorum = store.getParkBarrier(receiptQuorum.parkId)!;
    assert.equal(barrierQuorum.state, "woken", "Impossible quorum must escape and wake the barrier");

    // Part B: All-terminal escape under ALL with wake_on_exception: false
    // Under ALL, when one job completes successfully and one fails, ALL is impossible so it escapes
    const a1 = store.createJob({ id: "job_esc_a1", agentId: agent.id, kind: "spawn", requestId: "rea1", promptHash: "hea1" });
    const a2 = store.createJob({ id: "job_esc_a2", agentId: agent.id, kind: "spawn", requestId: "rea2", promptHash: "hea2" });
    store.bindJob({ jobId: a1.id, threadId: "thread_esc_a", originatingTurnId: "tea", originatingItemId: "iea1" });
    store.bindJob({ jobId: a2.id, threadId: "thread_esc_a", originatingTurnId: "tea", originatingItemId: "iea2" });

    const receiptAll = await service.park({
      job_ids: [a1.id, a2.id],
      predicate: "ALL",
      wake_on_exception: false,
    });
    assert.equal(receiptAll.armed, true);

    // a1 completes successfully with result -> ALL is still possible with a2 pending
    const ra1 = path.join(tmp, "ra1.json");
    await writeFile(ra1, JSON.stringify({ envelope: { summary: "a1 done" } }));
    makeJobCompleted(store, a1.id, ra1, "a1 done");
    await service.evaluateParkWakes(a1.id);

    let barrierAll = store.getParkBarrier(receiptAll.parkId)!;
    assert.equal(barrierAll.state, "armed", "ALL remains armed after 1 success while remaining job is running");

    // a2 fails -> all jobs are terminal and ALL is impossible -> escapes and wakes!
    store.updateJobStatus(a2.id, "failed");
    await service.evaluateParkWakes(a2.id);

    barrierAll = store.getParkBarrier(receiptAll.parkId)!;
    assert.equal(barrierAll.state, "woken", "Must escape and wake when ALL becomes impossible / all terminal");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 8. Whole-barrier evaluation before claim
// ---------------------------------------------------------------------------
test("WAKE-V3: whole-barrier evaluation verifies satisfaction before calling claimParkWake", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_whole");
    const j1 = store.createJob({ id: "job_w1", agentId: agent.id, kind: "spawn", requestId: "rw1", promptHash: "hw1" });
    const j2 = store.createJob({ id: "job_w2", agentId: agent.id, kind: "spawn", requestId: "rw2", promptHash: "hw2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_w", originatingTurnId: "tw", originatingItemId: "iw1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_w", originatingTurnId: "tw", originatingItemId: "iw2" });

    const receipt = await service.park({
      job_ids: [j1.id, j2.id],
      predicate: "ALL",
      wake_on_exception: false,
    });

    // j1 completes -> predicate is not satisfied (1 of 2)
    const r1 = path.join(tmp, "rw1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "w1" } }));
    makeJobCompleted(store, j1.id, r1, "w1");

    await service.evaluateParkWakes(j1.id);

    // Verify claim was NOT attempted: state is still 'armed', NOT 'waking' or 'woken'
    const barrierAfterOne = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrierAfterOne.state, "armed", "Must not claim wake while predicate is unsatisfied");
    assert.equal(store.getWakeOutbox(receipt.parkId, receipt.generation), null);

    // Now j2 completes -> predicate IS satisfied -> claim succeeds
    const r2 = path.join(tmp, "rw2.json");
    await writeFile(r2, JSON.stringify({ envelope: { summary: "w2" } }));
    makeJobCompleted(store, j2.id, r2, "w2");

    await service.evaluateParkWakes(j2.id);

    const barrierAfterBoth = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrierAfterBoth.state, "woken", "Must claim and wake once predicate is satisfied");
    assert.ok(store.getWakeOutbox(receipt.parkId, receipt.generation));
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 9. Duplicate concurrent completion produces exactly one outbox row / wake
// ---------------------------------------------------------------------------
test("WAKE-V3: duplicate concurrent completion produces exactly one outbox row and wake delivery", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_dupe");
    const j1 = store.createJob({ id: "job_conc1", agentId: agent.id, kind: "spawn", requestId: "rc1", promptHash: "hc1" });
    const j2 = store.createJob({ id: "job_conc2", agentId: agent.id, kind: "spawn", requestId: "rc2", promptHash: "hc2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_conc", originatingTurnId: "tc", originatingItemId: "ic1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_conc", originatingTurnId: "tc", originatingItemId: "ic2" });

    const receipt = await service.park({
      job_ids: [j1.id, j2.id],
      predicate: "ALL",
      wake_on_exception: false,
    });

    const r1 = path.join(tmp, "rc1.json");
    const r2 = path.join(tmp, "rc2.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "c1" } }));
    await writeFile(r2, JSON.stringify({ envelope: { summary: "c2" } }));
    makeJobCompleted(store, j1.id, r1, "c1");
    makeJobCompleted(store, j2.id, r2, "c2");

    // Fire 6 concurrent evaluation calls
    await Promise.all([
      service.evaluateParkWakes(j1.id),
      service.evaluateParkWakes(j2.id),
      service.evaluateParkWakes(j1.id),
      service.evaluateParkWakes(j2.id),
      service.evaluateParkWakes(j1.id),
      service.evaluateParkWakes(j2.id),
    ]);

    // Check wake_outbox rows in DB for this park and generation
    const rows = store.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ? AND generation = ?").all(receipt.parkId, receipt.generation);
    assert.equal(rows.length, 1, "Exactly one outbox row must exist for this park and generation");

    // Check transport calls
    assert.equal(fakeCli.calls.length, 1, "CLI transport deliverWake must be called exactly once");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 10. Stale generation rejection
// ---------------------------------------------------------------------------
test("WAKE-V3: stale generation cannot claim or mutate newer barrier generation", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_gen");
    const j1 = store.createJob({ id: "job_g1", agentId: agent.id, kind: "spawn", requestId: "rg1", promptHash: "hg1" });
    store.bindJob({ jobId: j1.id, threadId: "thread_gen", originatingTurnId: "tg", originatingItemId: "ig1" });

    // Generation 1 park
    const receipt1 = await service.park({ job_ids: [j1.id] });
    assert.equal(receipt1.generation, 1);

    // Caller advances to generation 2
    const receipt2 = await service.park({ job_ids: [j1.id], park_id: receipt1.parkId });
    assert.equal(receipt2.generation, 2);

    const barrierGen2 = store.getParkBarrier(receipt1.parkId)!;
    assert.equal(barrierGen2.generation, 2);
    assert.equal(barrierGen2.state, "armed");

    // Stale generation 1 claim must return false
    const staleClaimResult = store.claimParkWake(receipt1.parkId, 1);
    assert.equal(staleClaimResult, false, "claimParkWake must reject stale generation 1");

    // Stale setParkWoken must not affect generation 2
    store.setParkWoken(receipt1.parkId, 1);
    const refreshed = store.getParkBarrier(receipt1.parkId)!;
    assert.equal(refreshed.state, "armed", "Stale setParkWoken(gen=1) must not change gen=2 state");
    assert.equal(refreshed.armed, true);

    // Active generation 2 claim succeeds
    const activeClaimResult = store.claimParkWake(receipt1.parkId, 2);
    assert.equal(activeClaimResult, true, "Active generation 2 claim must succeed");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 11. Immutable delivered status for wake_outbox and jobs
// ---------------------------------------------------------------------------
test("WAKE-V3: immutable delivered status cannot be overwritten by subsequent updates", async () => {
  const { tmp, config, store } = await createTestEnv();

  try {
    const agent = setupAgent(store, tmp, "agent_immutable");

    // Create the parent barrier first to satisfy foreign key constraint
    store.createOrUpdateParkBarrier({
      id: "park_imm_1",
      threadId: "thread_imm",
      turnId: "turn_imm",
      generation: 1,
      armed: false,
      deliveryMode: "cli_resume",
      state: "idle",
    });

    // 1. wake_outbox immutability
    const outbox = store.createWakeOutbox({
      id: "wake_imm_1",
      parkId: "park_imm_1",
      generation: 1,
      threadId: "thread_imm",
      status: "delivered",
      wakeState: "delivered",
      wakeMarker: "<!-- marker -->",
      payloadJson: "{}",
    });
    assert.equal(outbox.status, "delivered");
    assert.equal(outbox.wakeState, "delivered");

    // Attempt to mutate delivered status to failed
    store.updateWakeOutboxStatus(outbox.id, "failed", "some failure");
    let refreshedOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshedOutbox.status, "delivered", "Delivered status must not be changed to failed");
    assert.equal(refreshedOutbox.wakeState, "delivered");

    // Attempt to mutate delivered status to deferred_active_writer
    store.updateWakeOutboxStatus(outbox.id, "deferred_active_writer", "conflict");
    refreshedOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshedOutbox.status, "delivered", "Delivered status must not be changed to deferred_active_writer");

    // Attempt to mutate delivered status to waking
    store.updateWakeOutboxStatus(outbox.id, "waking");
    refreshedOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshedOutbox.status, "delivered", "Delivered status must not be changed to waking");

    // Attempt to claim delivered row via claimWakeOutbox must return false
    const claimDeliveredResult = store.claimWakeOutbox(outbox.id);
    assert.equal(claimDeliveredResult, false, "claimWakeOutbox must reject delivered status");
    refreshedOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshedOutbox.status, "delivered");

    // 2. Job delivered status immutability
    const job = store.createJob({ id: "job_imm_1", agentId: agent.id, kind: "spawn", requestId: "ri1", promptHash: "hi1" });
    const rf = path.join(tmp, "rf_imm.json");
    await writeFile(rf, "{}");
    makeJobDelivered(store, job.id, rf, "delivered summary");

    const delivJob = store.getJob(job.id)!;
    assert.equal(delivJob.status, "delivered");

    // Transitioning away from delivered must throw
    assert.throws(
      () => store.updateJobStatus(job.id, "failed"),
      (err: unknown) => (err as Error).message.includes("Invalid job transition: delivered -> failed"),
    );
    assert.throws(
      () => store.updateJobStatus(job.id, "completed"),
      (err: unknown) => (err as Error).message.includes("Invalid job transition: delivered -> completed"),
    );
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 12. Restart recovery of armed and deferred barriers
// ---------------------------------------------------------------------------
test("WAKE-V3: restart recovery restores armed barriers and completes deferred_active_writer outbox", async () => {
  const { tmp, config, store: store1 } = await createTestEnv();
  const fakeCodex1 = new FakeCodexDelivery();
  const fakeCli1 = new FakeCliTransport();

  const dbPath = path.join(tmp, "bridge.sqlite");

  try {
    const agent = setupAgent(store1, tmp, "agent_restart");
    const j1 = store1.createJob({ id: "job_rst1", agentId: agent.id, kind: "spawn", requestId: "rrst1", promptHash: "hrst1" });
    const j2 = store1.createJob({ id: "job_rst2", agentId: agent.id, kind: "spawn", requestId: "rrst2", promptHash: "hrst2" });
    store1.bindJob({ jobId: j1.id, threadId: "thread_rst", originatingTurnId: "trst", originatingItemId: "irst1" });
    store1.bindJob({ jobId: j2.id, threadId: "thread_rst", originatingTurnId: "trst", originatingItemId: "irst2" });

    // Case A: Armed barrier with jobs pending when daemon shuts down
    const service1 = createTestService(config, store1, fakeCodex1, fakeCli1);
    await service1.start();
    const receiptA = await service1.park({ job_ids: [j1.id, j2.id], predicate: "ALL", wake_on_exception: false });
    assert.equal(receiptA.armed, true);

    // Case B: Deferred active-writer outbox entry
    const j3 = store1.createJob({ id: "job_rst3", agentId: agent.id, kind: "spawn", requestId: "rrst3", promptHash: "hrst3" });
    store1.bindJob({ jobId: j3.id, threadId: "thread_rst_b", originatingTurnId: "trst_b", originatingItemId: "irst3" });

    fakeCli1.nextResult = { success: false, activeWriter: true, error: "Active writer conflict" };
    const receiptB = await service1.park({ job_ids: [j3.id] });

    const r3 = path.join(tmp, "r3_rst.json");
    await writeFile(r3, JSON.stringify({ envelope: { summary: "r3" } }));
    makeJobCompleted(store1, j3.id, r3, "r3");
    await service1.evaluateParkWakes(j3.id);

    const outboxB = store1.getWakeOutbox(receiptB.parkId, receiptB.generation)!;
    assert.ok(outboxB);
    assert.equal(outboxB.status, "deferred_active_writer");

    // Explicitly set nextAttemptAt in the past so recovery will dispatch immediately
    store1.db.prepare("UPDATE wake_outbox SET next_attempt_at = ? WHERE id = ?").run(new Date(Date.now() - 5000).toISOString(), outboxB.id);

    // Stop service 1 and close store
    await service1.stop();
    store1.close();

    // -------------------------------------------------------------
    // RESTART: Create brand new store and service pointing to same DB
    // -------------------------------------------------------------
    const store2 = new BridgeStore(dbPath);
    const fakeCodex2 = new FakeCodexDelivery();
    const fakeCli2 = new FakeCliTransport();
    // CLI is now clear and successful
    fakeCli2.nextResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };

    const service2 = createTestService(config, store2, fakeCodex2, fakeCli2);

    // Start service 2 -> recovers pending outbox
    await service2.start();

    // Verify Case B: deferred outbox was dispatched and delivered on restart recovery
    const recoveredOutboxB = store2.getWakeOutbox(receiptB.parkId, receiptB.generation)!;
    assert.equal(recoveredOutboxB.status, "delivered", "Deferred active-writer outbox must be recovered and delivered on restart");
    const recoveredBarrierB = store2.getParkBarrier(receiptB.parkId)!;
    assert.equal(recoveredBarrierB.state, "woken");

    // Verify Case A: Armed barrier from before restart is still armed and completes normally
    const barrierA = store2.getParkBarrier(receiptA.parkId)!;
    assert.equal(barrierA.state, "armed", "Armed barrier must persist across restart");

    const r1 = path.join(tmp, "r1_rst.json");
    const r2 = path.join(tmp, "r2_rst.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "r1" } }));
    await writeFile(r2, JSON.stringify({ envelope: { summary: "r2" } }));
    makeJobCompleted(store2, j1.id, r1, "r1");
    makeJobCompleted(store2, j2.id, r2, "r2");

    await service2.evaluateParkWakes(j2.id);

    const barrierAWoken = store2.getParkBarrier(receiptA.parkId)!;
    assert.equal(barrierAWoken.state, "woken", "Recovered armed barrier must wake when jobs complete");
    const outboxA = store2.getWakeOutbox(receiptA.parkId, receiptA.generation)!;
    assert.ok(outboxA);
    assert.equal(outboxA.status, "delivered");

    await service2.stop();
    store2.close();
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 13. claimWakeOutbox atomic SQLite CAS contract
// ---------------------------------------------------------------------------
test("WAKE-V3: claimWakeOutbox atomic CAS allows only pending/deferred_active_writer to waking and increments attempts", async () => {
  const { tmp, store } = await createTestEnv();

  try {
    const agent = setupAgent(store, tmp, "agent_cas");
    store.createOrUpdateParkBarrier({
      id: "park_cas_1",
      threadId: "thread_cas",
      turnId: "turn_cas",
      generation: 1,
      armed: true,
      deliveryMode: "cli_resume",
      state: "armed",
    });

    // 1. Create outbox with pending status, attempts = 0
    const outbox = store.createWakeOutbox({
      id: "wake_cas_1",
      parkId: "park_cas_1",
      generation: 1,
      threadId: "thread_cas",
      status: "pending",
      wakeState: "waiting",
      wakeMarker: "<!-- marker -->",
      payloadJson: "{}",
    });
    assert.equal(outbox.status, "pending");
    assert.equal(outbox.attempts, 0);

    // First claim: pending -> waking: must return true and increment attempts to 1
    const claim1 = store.claimWakeOutbox(outbox.id);
    assert.equal(claim1, true, "First claim from pending must succeed");
    let refreshed = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshed.status, "waking");
    assert.equal(refreshed.attempts, 1, "Attempts must be incremented to 1");

    // Second claim: already waking: must return false and NOT change attempts
    const claim2 = store.claimWakeOutbox(outbox.id);
    assert.equal(claim2, false, "Second claim while waking must fail");
    refreshed = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshed.status, "waking");
    assert.equal(refreshed.attempts, 1, "Attempts must remain 1");

    // Transition to deferred_active_writer
    store.updateWakeOutboxStatus(outbox.id, "deferred_active_writer", "conflict");
    refreshed = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshed.status, "deferred_active_writer");
    assert.equal(refreshed.attempts, 1, "updateWakeOutboxStatus must preserve attempts");

    // Third claim: deferred_active_writer -> waking: must return true and increment attempts to 2
    const claim3 = store.claimWakeOutbox(outbox.id);
    assert.equal(claim3, true, "Claim from deferred_active_writer must succeed");
    refreshed = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshed.status, "waking");
    assert.equal(refreshed.attempts, 2, "Attempts must be incremented to 2");

    // Transition to delivered
    store.updateWakeOutboxStatus(outbox.id, "delivered", null, { wakeState: "delivered" });
    refreshed = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshed.status, "delivered");
    assert.equal(refreshed.attempts, 2);

    // Fourth claim: delivered -> must return false (delivered immutable)
    const claim4 = store.claimWakeOutbox(outbox.id);
    assert.equal(claim4, false, "Claim on delivered row must fail");
    refreshed = store.getWakeOutboxById(outbox.id)!;
    assert.equal(refreshed.status, "delivered");
    assert.equal(refreshed.attempts, 2);

    // Non-existent ID -> false
    assert.equal(store.claimWakeOutbox("non_existent_id"), false);

    // Overload with (parkId, generation)
    assert.equal(store.claimWakeOutbox("park_cas_1", 1), false, "Delivered row rejected via (parkId, gen)");
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 14. Two store/service instances on one temp SQLite DB prevent double dispatch
// ---------------------------------------------------------------------------
test("WAKE-V3: two store/service instances on one temp SQLite DB prevent double dispatch", async () => {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "ds-wake-2inst-"));
  const dbPath = path.join(tmp, "bridge.sqlite");
  const config = createDefaultConfig({
    dataDir: tmp,
    configPath: path.join(tmp, "config.json"),
    experimentalSameChatDelivery: true,
  });

  const store1 = new BridgeStore(dbPath);
  const store2 = new BridgeStore(dbPath);

  const fakeCodex1 = new FakeCodexDelivery();
  const fakeCli1 = new FakeCliTransport();
  const service1 = createTestService(config, store1, fakeCodex1, fakeCli1);

  const fakeCodex2 = new FakeCodexDelivery();
  const fakeCli2 = new FakeCliTransport();
  const service2 = createTestService(config, store2, fakeCodex2, fakeCli2);

  await service1.start();
  await service2.start();

  try {
    // 1. Concurrent wake delivery from 2 instances
    const agent = setupAgent(store1, tmp, "agent_2inst");
    const j1 = store1.createJob({ id: "job_2inst_1", agentId: agent.id, kind: "spawn", requestId: "r2i1", promptHash: "h2i1" });
    const j2 = store1.createJob({ id: "job_2inst_2", agentId: agent.id, kind: "spawn", requestId: "r2i2", promptHash: "h2i2" });
    store1.bindJob({ jobId: j1.id, threadId: "thread_2inst", originatingTurnId: "t2i", originatingItemId: "i2i1" });
    store1.bindJob({ jobId: j2.id, threadId: "thread_2inst", originatingTurnId: "t2i", originatingItemId: "i2i2" });

    const receipt = await service1.park({ job_ids: [j1.id, j2.id], predicate: "ALL", wake_on_exception: false });

    const r1 = path.join(tmp, "r2i1.json");
    const r2 = path.join(tmp, "r2i2.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "r1 done" } }));
    await writeFile(r2, JSON.stringify({ envelope: { summary: "r2 done" } }));
    makeJobCompleted(store1, j1.id, r1, "r1 done");
    makeJobCompleted(store1, j2.id, r2, "r2 done");

    // Race both services concurrently on evaluation
    await Promise.all([
      service1.evaluateParkWakes(j1.id),
      service2.evaluateParkWakes(j2.id),
      service1.evaluateParkWakes(j2.id),
      service2.evaluateParkWakes(j1.id),
    ]);

    // Exactly one outbox row in the shared DB
    const outboxRows = store1.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ? AND generation = ?").all(receipt.parkId, receipt.generation);
    assert.equal(outboxRows.length, 1, "Exactly one outbox row must exist in the shared DB");

    // Exactly one delivery across both instances
    const totalDeliveries = fakeCli1.calls.length + fakeCli2.calls.length;
    assert.equal(totalDeliveries, 1, "Exactly one CLI delivery must occur across both services");

    // Barrier and outbox status delivered/woken in both store instances
    const barrier1 = store1.getParkBarrier(receipt.parkId)!;
    const barrier2 = store2.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier1.state, "woken");
    assert.equal(barrier2.state, "woken");

    const outbox1 = store1.getWakeOutbox(receipt.parkId, receipt.generation)!;
    const outbox2 = store2.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.equal(outbox1.status, "delivered");
    assert.equal(outbox2.status, "delivered");
    assert.equal(outbox1.attempts, 1);

    // 2. Concurrent deferred active-writer recovery from 2 instances
    const j3 = store1.createJob({ id: "job_2inst_3", agentId: agent.id, kind: "spawn", requestId: "r2i3", promptHash: "h2i3" });
    store1.bindJob({ jobId: j3.id, threadId: "thread_2inst_b", originatingTurnId: "t2ib", originatingItemId: "i2ib" });

    // First make CLI return activeWriter to put it in deferred state
    fakeCli1.nextResult = { success: false, activeWriter: true, error: "Active writer conflict" };
    fakeCli2.nextResult = { success: false, activeWriter: true, error: "Active writer conflict" };

    const receiptB = await service1.park({ job_ids: [j3.id] });
    const r3 = path.join(tmp, "r2i3.json");
    await writeFile(r3, JSON.stringify({ envelope: { summary: "r3 done" } }));
    makeJobCompleted(store1, j3.id, r3, "r3 done");
    await service1.evaluateParkWakes(j3.id);

    const outboxB = store1.getWakeOutbox(receiptB.parkId, receiptB.generation)!;
    assert.equal(outboxB.status, "deferred_active_writer");
    assert.equal(outboxB.attempts, 1);

    // Make nextAttemptAt in the past and CLI clear for recovery
    store1.db.prepare("UPDATE wake_outbox SET next_attempt_at = ? WHERE id = ?").run(new Date(Date.now() - 5000).toISOString(), outboxB.id);
    fakeCli1.nextResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };
    fakeCli2.nextResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };

    const cliCallsBeforeRecovery = fakeCli1.calls.length + fakeCli2.calls.length;

    // Both services run recoverWakeOutbox concurrently
    await Promise.all([
      (service1 as any).recoverWakeOutbox(),
      (service2 as any).recoverWakeOutbox(),
    ]);

    const recoveredOutboxB = store1.getWakeOutbox(receiptB.parkId, receiptB.generation)!;
    assert.equal(recoveredOutboxB.status, "delivered", "Deferred outbox must be delivered after recovery");
    assert.equal(recoveredOutboxB.attempts, 2, "Attempts must be 2 after retry delivery");

    const cliCallsAfterRecovery = fakeCli1.calls.length + fakeCli2.calls.length;
    assert.equal(cliCallsAfterRecovery - cliCallsBeforeRecovery, 1, "Only one service must win and deliver during concurrent recovery");
  } finally {
    await service1.stop();
    await service2.stop();
    store1.close();
    store2.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 15. abort() terminal transition without result wakes live barrier
// ---------------------------------------------------------------------------
test("WAKE-V3: abort() terminal transition without result triggers evaluateParkWakes and wakes live barrier", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_abort_wake");
    const job = store.createJob({ id: "job_abt1", agentId: agent.id, kind: "spawn", requestId: "rabt1", promptHash: "habt1" });
    store.bindJob({ jobId: job.id, threadId: "thread_abt", originatingTurnId: "tabt", originatingItemId: "iabt" });

    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");

    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);

    let barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed");

    // Orchestrator aborts the agent task
    const abortResult = await service.abort(agent.id, "Emergency stop requested");
    assert.equal(abortResult.status, "aborted");
    assert.equal(abortResult.jobId, job.id);

    const abortedJob = store.getJob(job.id)!;
    assert.equal(abortedJob.status, "aborted");
    assert.equal(abortedJob.resultPath, null, "Aborted job has no resultPath");

    // Barrier must have been woken by the abort() call
    barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken", "abort() must wake the parked barrier");
    assert.equal(barrier.armed, false);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox, "Wake outbox row must exist");
    assert.equal(outbox.status, "delivered");
    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.deepEqual(envelope.readyJobIds, [job.id]);
    assert.equal(envelope.statuses[job.id], "aborted");
    assert.equal(fakeCli.calls.length, 1);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 16. timeoutFollow() does not kill healthy job without authoritative proof;
//     explicit terminal event triggers evaluateParkWakes and wakes live barrier exactly once
// ---------------------------------------------------------------------------
test("WAKE-V3: timeoutFollow() does not kill healthy job without authoritative proof; explicit terminal event triggers evaluateParkWakes and wakes live barrier exactly once", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const fakeClient = new FakeOpenCodeClient();
  // Simulate capture failure so job has no result
  fakeClient.listMessages = async () => {
    throw new Error("Client disconnected; no messages available");
  };

  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: fakeClient,
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_timeout_wake");
    const job = store.createJob({ id: "job_to1", agentId: agent.id, kind: "spawn", requestId: "rto1", promptHash: "hto1" });
    store.bindJob({ jobId: job.id, threadId: "thread_to", originatingTurnId: "tto", originatingItemId: "ito" });

    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");
    store.updateJobStatus(job.id, "following");
    store.updateJobStatus(job.id, "finalizing");

    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);

    // Setup lifecycle map entry with elapsed follow and grace deadlines
    (service as any).followLifecycles.set(job.id, {
      jobId: job.id,
      deadlineAt: Date.now() - 1000,
      graceDeadlineAt: Date.now() - 100,
      autoArmed: false,
      promise: Promise.resolve(),
      resolve: () => {},
      reject: () => {},
      deadlineTimer: null,
      graceTimer: null,
      waiters: new Set(),
      settled: false,
    });

    // 1. Follow window/grace expiry alone must NOT kill a healthy job
    // Absence of PID/attempt does not prove death: job stays finalizing, barrier remains armed, no outbox
    await (service as any).timeoutFollow(job.id);

    const jobAfterClockExpiry = store.getJob(job.id)!;
    assert.equal(jobAfterClockExpiry.status, "finalizing", "Healthy job must not be killed by clock/grace expiry when PID/attempt is absent");
    assert.equal(jobAfterClockExpiry.resultPath, null);

    let barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed", "Barrier must remain armed when follow window expires without authoritative death proof");
    assert.equal(barrier.armed, true);
    assert.equal(store.getWakeOutbox(receipt.parkId, receipt.generation), null, "Outbox must not materialize without an explicit terminal event");
    assert.equal(fakeCli.calls.length, 0, "CLI wake delivery must not occur while barrier is still armed");

    // 2. Authoritative proof of dead worker process enables explicit terminal timed_out transition
    store.updateJobLiveness(job.id, { workerPid: 999999 });

    // Now timeoutFollow has authoritative proof that the worker process is dead
    await (service as any).timeoutFollow(job.id);

    const timedOutJob = store.getJob(job.id)!;
    assert.equal(timedOutJob.status, "timed_out", "Job must transition to timed_out once dead process is authoritatively proven");
    assert.equal(timedOutJob.resultPath, null, "Job must have no resultPath");

    // Barrier must have been woken by the authoritative terminal event via evaluateParkWakes
    barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken", "Explicit terminal event must wake the barrier");
    assert.equal(barrier.armed, false);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox, "Wake outbox row must exist");
    assert.equal(outbox.status, "delivered");
    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.deepEqual(envelope.readyJobIds, [job.id]);
    assert.equal(envelope.statuses[job.id], "timed_out");
    assert.equal(fakeCli.calls.length, 1);

    // 3. Subsequent/duplicate evaluation must materialize outbox exactly once
    await service.evaluateParkWakes(job.id);
    assert.equal(fakeCli.calls.length, 1, "CLI wake delivery must be invoked exactly once");
    const outboxRows = store.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ? AND generation = ?").all(receipt.parkId, receipt.generation);
    assert.equal(outboxRows.length, 1, "Exactly one outbox row must exist for this park and generation");

    // 4. Also verify explicit abort terminal event on a healthy job whose follow window expired
    const agent2 = setupAgent(store, tmp, "agent_timeout_abort");
    const job2 = store.createJob({ id: "job_to2", agentId: agent2.id, kind: "spawn", requestId: "rto2", promptHash: "hto2" });
    store.bindJob({ jobId: job2.id, threadId: "thread_to2", originatingTurnId: "tto2", originatingItemId: "ito2" });

    store.updateJobStatus(job2.id, "dispatching");
    store.updateJobStatus(job2.id, "running");
    store.updateJobStatus(job2.id, "following");
    store.updateJobStatus(job2.id, "finalizing");

    const receipt2 = await service.park({ job_ids: [job2.id] });
    assert.equal(receipt2.armed, true);

    (service as any).followLifecycles.set(job2.id, {
      jobId: job2.id,
      deadlineAt: Date.now() - 1000,
      graceDeadlineAt: Date.now() - 100,
      autoArmed: false,
      promise: Promise.resolve(),
      resolve: () => {},
      reject: () => {},
      deadlineTimer: null,
      graceTimer: null,
      waiters: new Set(),
      settled: false,
    });

    // Clock expiry alone does not kill job2 or wake receipt2
    await (service as any).timeoutFollow(job2.id);
    assert.equal(store.getJob(job2.id)!.status, "finalizing");
    assert.equal(store.getParkBarrier(receipt2.parkId)!.state, "armed");
    assert.equal(store.getWakeOutbox(receipt2.parkId, receipt2.generation), null);

    // Explicit abort terminalizes job2, calls evaluateParkWakes, and materializes outbox exactly once
    const abortResult = await service.abort(agent2.id, "Explicit abort after follow window");
    assert.equal(abortResult.status, "aborted");
    assert.equal(store.getJob(job2.id)!.status, "aborted");

    const barrier2 = store.getParkBarrier(receipt2.parkId)!;
    assert.equal(barrier2.state, "woken");
    assert.equal(barrier2.armed, false);

    const outbox2 = store.getWakeOutbox(receipt2.parkId, receipt2.generation)!;
    assert.ok(outbox2);
    assert.equal(outbox2.status, "delivered");
    const envelope2 = JSON.parse(outbox2.payloadJson) as WakeEnvelope;
    assert.deepEqual(envelope2.readyJobIds, [job2.id]);
    assert.equal(envelope2.statuses[job2.id], "aborted");
    assert.equal(fakeCli.calls.length, 2, "Second delivery occurred for second barrier");

    await service.evaluateParkWakes(job2.id);
    assert.equal(fakeCli.calls.length, 2, "Duplicate evaluateParkWakes must not re-deliver");
    const outboxRows2 = store.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ? AND generation = ?").all(receipt2.parkId, receipt2.generation);
    assert.equal(outboxRows2.length, 1, "Exactly one outbox row for receipt2");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 17. Existing-outbox guard includes pending
// ---------------------------------------------------------------------------
test("WAKE-V3: existing-outbox guard includes pending and prevents duplicate outbox/claim", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_guard_pending");
    const j1 = store.createJob({ id: "job_gp1", agentId: agent.id, kind: "spawn", requestId: "rgp1", promptHash: "hgp1" });
    store.bindJob({ jobId: j1.id, threadId: "thread_gp", originatingTurnId: "tgp", originatingItemId: "igp" });

    const receipt = await service.park({ job_ids: [j1.id] });

    const r1 = path.join(tmp, "rgp1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "gp done" } }));
    makeJobCompleted(store, j1.id, r1, "gp done");

    // Manually create a pending outbox row before evaluateParkWakes
    store.createWakeOutbox({
      id: "wake_gp_manual",
      parkId: receipt.parkId,
      generation: receipt.generation,
      threadId: "thread_gp",
      status: "pending",
      wakeState: "waiting",
      wakeMarker: "<!-- marker -->",
      payloadJson: "{}",
    });

    // Evaluate park wakes: existingOutbox with status 'pending' must trigger early return
    await service.evaluateParkWakes(j1.id);

    // Barrier state should still be armed because evaluateParkWakes exited early before claimParkWake
    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed", "Must not claim barrier wake when outbox is pending");

    // Exactly 1 outbox row remains
    const outboxRows = store.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ? AND generation = ?").all(receipt.parkId, receipt.generation);
    assert.equal(outboxRows.length, 1);
    assert.equal((outboxRows[0] as any).id, "wake_gp_manual");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 18. Ambiguous/unknown CLI outcome remains fail-closed in waking/indeterminate and is never blindly retried;
//     only an explicitly deterministic pre-accept non-delivery may become retryable if provable
// ---------------------------------------------------------------------------
test("WAKE-V3: ambiguous/unknown CLI outcome remains fail-closed in waking/indeterminate and is never blindly retried; only deterministic pre-accept is retryable", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_ambiguous_cli");
    const j1 = store.createJob({ id: "job_amb1", agentId: agent.id, kind: "spawn", requestId: "ramb1", promptHash: "hamb1" });
    store.bindJob({ jobId: j1.id, threadId: "thread_amb", originatingTurnId: "tamb", originatingItemId: "iamb" });

    const receipt = await service.park({ job_ids: [j1.id] });
    assert.equal(receipt.armed, true);

    // Part A: Ambiguous/unknown CLI outcome (e.g. timeout, exit code 1 with unproven send)
    fakeCli.nextResult = {
      success: false,
      unknownOutcome: true,
      error: "Command failed with exit code 1: transient pipe failure",
    };

    const r1 = path.join(tmp, "ramb1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "amb done" } }));
    makeJobCompleted(store, j1.id, r1, "amb done");

    // Evaluate park wakes -> triggers dispatchWakeOutbox
    await service.evaluateParkWakes(j1.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation);
    assert.ok(outbox, "Outbox record must exist");

    // Ambiguous/unknown CLI outcome MUST remain fail-closed in waking/indeterminate:
    // It must NOT be marked permanently failed (which abandons reconciliation),
    // and must NOT be marked pending/deferred for blind retry.
    assert.equal(outbox.status, "waking", "Ambiguous/unknown CLI outcome must remain fail-closed in waking/indeterminate status");
    assert.notEqual(outbox.status, "failed", "Ambiguous outcome must not be permanently failed");
    assert.notEqual(outbox.status, "pending", "Ambiguous outcome must not be reset to pending for blind retry");
    assert.notEqual(outbox.status, "deferred_active_writer", "Ambiguous outcome without activeWriter proof must not become deferred");

    // Barrier must remain fail-closed in waking/indeterminate and NOT be re-armed
    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "waking", "Barrier must remain waking/indeterminate when delivery outcome is unknown");
    assert.equal(barrier.armed, false, "Barrier armed must be false while indeterminate");

    // Ambiguous outcome is NEVER blindly retried without authoritative proof
    const callsBefore = fakeCli.calls.length;
    await (service as any).recoverWakeOutbox();
    await service.evaluateParkWakes(j1.id);
    assert.equal(fakeCli.calls.length, callsBefore, "Ambiguous outcome must never be blindly retried without authoritative proof");

    // Part B: Explicitly deterministic pre-accept non-delivery (activeWriter: true)
    // Only deterministic pre-accept non-delivery may become retryable
    const j2 = store.createJob({ id: "job_det_retry", agentId: agent.id, kind: "spawn", requestId: "rdet", promptHash: "hdet" });
    store.bindJob({ jobId: j2.id, threadId: "thread_det", originatingTurnId: "tdet", originatingItemId: "idet" });

    fakeCli.nextResult = {
      success: false,
      activeWriter: true,
      error: "Active writer conflict",
    };

    const receipt2 = await service.park({ job_ids: [j2.id] });
    const r2 = path.join(tmp, "rdet.json");
    await writeFile(r2, JSON.stringify({ envelope: { summary: "det done" } }));
    makeJobCompleted(store, j2.id, r2, "det done");

    await service.evaluateParkWakes(j2.id);

    const outbox2 = store.getWakeOutbox(receipt2.parkId, receipt2.generation)!;
    assert.ok(outbox2);
    assert.equal(outbox2.status, "deferred_active_writer", "Deterministic pre-accept non-delivery may become retryable");

    const barrier2 = store.getParkBarrier(receipt2.parkId)!;
    assert.equal(barrier2.state, "armed", "Barrier must be armed for retry on deterministic pre-accept non-delivery");

    // When transport recovers, retry succeeds
    fakeCli.nextResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };
    store.db.prepare("UPDATE wake_outbox SET next_attempt_at = ? WHERE id = ?").run(new Date(Date.now() - 5000).toISOString(), outbox2.id);
    await (service as any).recoverWakeOutbox();

    const deliveredOutbox = store.getWakeOutbox(receipt2.parkId, receipt2.generation)!;
    assert.equal(deliveredOutbox.status, "delivered");
    const deliveredBarrier = store.getParkBarrier(receipt2.parkId)!;
    assert.equal(deliveredBarrier.state, "woken");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 19. Startup with no authoritative downstream evidence remains indeterminate, not armed
// ---------------------------------------------------------------------------
test("WAKE-V3: startup with no authoritative downstream evidence remains indeterminate, not armed", async () => {
  const { tmp, config, store: store1 } = await createTestEnv();
  const dbPath = path.join(tmp, "bridge.sqlite");
  let store2: BridgeStore | null = null;
  let service2: BridgeService | null = null;

  try {
    const agent = setupAgent(store1, tmp, "agent_unproven_recov");
    const job = store1.createJob({ id: "job_upr1", agentId: agent.id, kind: "spawn", requestId: "rupr1", promptHash: "hupr1" });
    store1.bindJob({ jobId: job.id, threadId: "thread_upr", originatingTurnId: "tupr", originatingItemId: "iupr" });

    // Barrier was claimed and left in 'waking' state before restart
    store1.createOrUpdateParkBarrier({
      id: "park_upr_1",
      threadId: "thread_upr",
      turnId: "tupr",
      generation: 1,
      armed: false,
      deliveryMode: "cli_resume",
      state: "waking",
    });
    store1.setParkJobs("park_upr_1", [job.id]);

    const marker = `<!-- [SUBAGENT_BRIDGE_WAKE:park=park_upr_1:gen=1] -->`;
    const wakeEnv: WakeEnvelope = {
      parkId: "park_upr_1",
      generation: 1,
      reason: "subagents park wake",
      jobIds: [job.id],
      readyJobIds: [job.id],
      statuses: { [job.id]: "completed" },
      resultHashes: {},
      pendingCount: 0,
      instruction: "Call subagents_follow",
      marker,
    };

    // Outbox was in 'waking' status before restart (in-flight dispatch interrupted by process termination)
    store1.createWakeOutbox({
      id: "wake_upr_1",
      parkId: "park_upr_1",
      generation: 1,
      threadId: "thread_upr",
      deliveryMode: "cli_resume",
      status: "waking",
      wakeState: "waiting",
      wakeMarker: marker,
      payloadJson: JSON.stringify(wakeEnv),
      attempts: 1,
    });

    const r1 = path.join(tmp, "rupr1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "upr done" } }));
    makeJobCompleted(store1, job.id, r1, "upr done");

    store1.close();

    // Restart: fresh store and service pointing to existing DB
    store2 = new BridgeStore(dbPath);
    const fakeCodex2 = new FakeCodexDelivery();
    const fakeCli2 = new FakeCliTransport();

    // Downstream checks initially unproven (not found in queue DB or transcript)
    fakeCli2.queuedWakeHandler = async () => ({ found: false });
    fakeCodex2.reconcileSendResult = false;

    service2 = createTestService(config, store2, fakeCodex2, fakeCli2);
    await service2.start();

    // Startup with no authoritative downstream evidence remains indeterminate, not armed
    const recoveredBarrier = store2.getParkBarrier("park_upr_1")!;
    assert.equal(recoveredBarrier.state, "waking", "Startup with no downstream evidence must remain indeterminate in waking, not armed");
    assert.equal(recoveredBarrier.armed, false, "Recovered barrier armed flag must remain false");

    // Waking outbox remains fail-closed in waking/indeterminate and is never blindly retried
    const recoveredOutbox = store2.getWakeOutboxById("wake_upr_1")!;
    assert.equal(recoveredOutbox.status, "waking", "Unproven waking outbox must remain in waking/indeterminate status");
    assert.notEqual(recoveredOutbox.status, "delivered", "Unproven outbox must not be marked delivered without downstream evidence");
    assert.notEqual(recoveredOutbox.status, "pending", "Unproven outbox must not be reset to pending for blind retry");
    assert.notEqual(recoveredOutbox.status, "deferred_active_writer", "Unproven outbox must not become deferred without activeWriter proof");

    // Verify no duplicate wake outbox materialization occurs even after re-evaluation
    await service2.evaluateParkWakes(job.id);
    const outboxRows = store2.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ?").all("park_upr_1");
    assert.equal(outboxRows.length, 1, "No duplicate wake outbox materialization occurs");

    // Verify no premature or duplicate transport calls while unproven
    assert.equal(fakeCli2.calls.length, 0, "No duplicate transport calls during unproven recovery");
  } finally {
    await service2?.stop();
    store2?.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 20. Confirmed queue/transcript evidence reconciles to delivered/woken exactly once without another CLI call or duplicate outbox
// ---------------------------------------------------------------------------
test("WAKE-V3: confirmed queue/transcript evidence reconciles to delivered/woken exactly once without another CLI call or duplicate outbox", async () => {
  const { tmp, config, store: store1 } = await createTestEnv();
  const dbPath = path.join(tmp, "bridge.sqlite");
  let store2: BridgeStore | null = null;
  let service2: BridgeService | null = null;

  try {
    const agent = setupAgent(store1, tmp, "agent_term_confirm");
    const job = store1.createJob({ id: "job_term1", agentId: agent.id, kind: "spawn", requestId: "rterm1", promptHash: "hterm1" });
    store1.bindJob({ jobId: job.id, threadId: "thread_term", originatingTurnId: "tterm", originatingItemId: "iterm" });

    store1.createOrUpdateParkBarrier({
      id: "park_term_1",
      threadId: "thread_term",
      turnId: "tterm",
      generation: 1,
      armed: false,
      deliveryMode: "cli_resume",
      state: "waking",
    });
    store1.setParkJobs("park_term_1", [job.id]);

    const marker = `<!-- [SUBAGENT_BRIDGE_WAKE:park=park_term_1:gen=1] -->`;
    const wakeEnv: WakeEnvelope = {
      parkId: "park_term_1",
      generation: 1,
      reason: "subagents park wake",
      jobIds: [job.id],
      readyJobIds: [job.id],
      statuses: { [job.id]: "completed" },
      resultHashes: {},
      pendingCount: 0,
      instruction: "Call subagents_follow",
      marker,
    };

    store1.createWakeOutbox({
      id: "wake_term_1",
      parkId: "park_term_1",
      generation: 1,
      threadId: "thread_term",
      deliveryMode: "cli_resume",
      status: "waking",
      wakeState: "waiting",
      wakeMarker: marker,
      payloadJson: JSON.stringify(wakeEnv),
      attempts: 1,
    });

    const r1 = path.join(tmp, "rterm1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "term done" } }));
    makeJobCompleted(store1, job.id, r1, "term done");

    store1.close();

    store2 = new BridgeStore(dbPath);
    const fakeCodex2 = new FakeCodexDelivery();
    const fakeCli2 = new FakeCliTransport();

    // Downstream evidence initially unproven during service start
    fakeCli2.queuedWakeHandler = async () => ({ found: false });
    fakeCodex2.reconcileSendResult = false;

    service2 = createTestService(config, store2, fakeCodex2, fakeCli2);
    await service2.start();

    // Now downstream queue evidence confirms delivery (e.g. Codex queue DB records accepted wake)
    const confirmedMessageId = "msg_downstream_confirmed_888";
    fakeCli2.queuedWakeHandler = async (threadId: string, m: string) => {
      if (threadId === "thread_term" && m === marker) {
        return {
          found: true,
          messageId: confirmedMessageId,
          deliveryMode: "queued",
        };
      }
      return { found: false };
    };

    // Recovery cycle encounters confirmed downstream queue evidence
    await (service2 as any).recoverWakeOutbox();

    // 1. Delivery transitions to delivered with confirmed messageId and deliveryMode
    const outbox = store2.getWakeOutboxById("wake_term_1")!;
    assert.equal(outbox.status, "delivered", "Outbox must transition to delivered when downstream evidence confirms");
    assert.equal(outbox.deliveryMode, "queued", "Delivery mode must reflect confirmed queued delivery");
    assert.equal(outbox.messageId, confirmedMessageId, "Message ID must match confirmed downstream message ID");

    // 2. Barrier transitions to woken
    const barrier = store2.getParkBarrier("park_term_1")!;
    assert.equal(barrier.state, "woken", "Barrier must transition to woken when downstream evidence confirms");
    assert.equal(barrier.armed, false, "Barrier armed must be false once woken");

    // 3. Exactly once: zero transport delivery invocations (confirmed from downstream)
    assert.equal(fakeCli2.calls.length, 0, "No new transport deliverWake call when confirmed downstream");

    // 4. Terminal immutability: subsequent evaluateParkWakes, recovery, or status update attempts cannot mutate or re-deliver
    await service2.evaluateParkWakes(job.id);
    await (service2 as any).recoverWakeOutbox();

    assert.equal(fakeCli2.calls.length, 0, "Terminal delivery must never re-invoke transport deliverWake");
    const outboxRows = store2.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ?").all("park_term_1");
    assert.equal(outboxRows.length, 1, "Exactly one outbox record must exist");

    const terminalOutbox = store2.getWakeOutboxById("wake_term_1")!;
    assert.equal(terminalOutbox.status, "delivered", "Delivered status is immutable and terminal");
    assert.equal(terminalOutbox.attempts, outbox.attempts, "Attempts must not change after terminal confirmation");

    // 5. Also verify transcript evidence reconciliation path
    const jTrans = store2.createJob({ id: "job_trans_rec", agentId: agent.id, kind: "spawn", requestId: "rtr", promptHash: "htr" });
    store2.bindJob({ jobId: jTrans.id, threadId: "thread_trans", originatingTurnId: "ttr", originatingItemId: "itr" });
    store2.createOrUpdateParkBarrier({
      id: "park_trans_1",
      threadId: "thread_trans",
      turnId: "ttr",
      generation: 1,
      armed: false,
      deliveryMode: "cli_resume",
      state: "waking",
    });
    store2.setParkJobs("park_trans_1", [jTrans.id]);

    const transMarker = `<!-- [SUBAGENT_BRIDGE_WAKE:park=park_trans_1:gen=1] -->`;
    store2.createWakeOutbox({
      id: "wake_trans_1",
      parkId: "park_trans_1",
      generation: 1,
      threadId: "thread_trans",
      deliveryMode: "cli_resume",
      status: "waking",
      wakeState: "waiting",
      wakeMarker: transMarker,
      payloadJson: JSON.stringify({ ...wakeEnv, parkId: "park_trans_1", marker: transMarker }),
      attempts: 1,
    });

    // Transcript evidence confirms (queue check does not match)
    fakeCodex2.reconcileSendResult = true;
    await (service2 as any).recoverWakeOutbox();

    const transOutbox = store2.getWakeOutboxById("wake_trans_1")!;
    assert.equal(transOutbox.status, "delivered", "Transcript reconciliation must transition waking outbox to delivered");
    const transBarrier = store2.getParkBarrier("park_trans_1")!;
    assert.equal(transBarrier.state, "woken", "Transcript reconciliation must transition barrier to woken");
    assert.equal(transBarrier.armed, false);
    assert.equal(fakeCli2.calls.length, 0, "No CLI calls during transcript reconciliation");
  } finally {
    await service2?.stop();
    store2?.close();
    await rm(tmp, { recursive: true, force: true });
  }
});
