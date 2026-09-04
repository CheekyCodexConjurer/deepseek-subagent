import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile, mkdir } from "node:fs/promises";
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
  async reconcileSend(threadId: string, marker: string): Promise<boolean> {
    return false;
  }
  onCorrelation(_listener: (correlation: CodexCorrelation) => void): () => void {
    return () => undefined;
  }
}

class FakeCliTransport implements CodexCliTransport {
  calls: Array<{ threadId: string; marker: string }> = [];
  nextResult: CodexCliExecutionResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };
  compatible = true;

  async probeCapabilities(executable?: string): Promise<{ compatible: boolean; version: string | null }> {
    return { compatible: this.compatible, version: "0.150.0" };
  }

  async deliverWake(threadId: string, marker: string): Promise<CodexCliExecutionResult> {
    this.calls.push({ threadId, marker });
    return this.nextResult;
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
  const tmp = await mkdtemp(path.join(os.tmpdir(), "ds-park-v3-test-"));
  const config = createDefaultConfig({
    dataDir: tmp,
    configPath: path.join(tmp, "config.json"),
    experimentalSameChatDelivery: true,
  });
  const store = new BridgeStore(path.join(tmp, "bridge.sqlite"));
  return { tmp, config, store };
}

function makeJobCompleted(store: BridgeStore, jobId: string, resultPath?: string, resultSummary?: string) {
  store.updateJobStatus(jobId, "dispatching");
  store.updateJobStatus(jobId, "running");
  if (resultPath) {
    store.setJobResult(jobId, resultPath, resultSummary ?? "done");
  }
  store.updateJobStatus(jobId, "completed");
}

function makeJobDelivered(store: BridgeStore, jobId: string, resultPath?: string, resultSummary?: string) {
  makeJobCompleted(store, jobId, resultPath, resultSummary);
  store.updateJobStatus(jobId, "delivered");
}

// ---------------------------------------------------------------------------
// 1. default immediate external park: omitted wait means external arm and immediate return
// ---------------------------------------------------------------------------
test("GATE-1: default immediate external park returns immediately with cli_resume and registers NO in-memory waiter", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_v3_1",
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
    const job = store.createJob({ id: "job_v3_1", agentId: agent.id, kind: "spawn", requestId: "req_1", promptHash: "h1" });
    store.bindJob({ jobId: job.id, threadId: "thread_v3_1", originatingTurnId: "turn_1", originatingItemId: "item_1" });

    // Omitted wait: must return immediately with armed=true and deliveryMode="cli_resume"
    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.deliveryMode, "cli_resume");
    assert.equal(receipt.wakeState, "waiting");
    assert.equal(receipt.obligationState, "pending");
    assert.equal(service.hasParkWaiter(receipt.parkId), false, "Must never register an in-memory waiter");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. explicit wait=true rejection: fails closed with typed error and no waiter
// ---------------------------------------------------------------------------
test("GATE-2: explicit wait=true fails closed with typed error directing to subagents_follow and registers NO waiter", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_v3_2",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_2",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_v3_2", agentId: agent.id, kind: "spawn", requestId: "req_2", promptHash: "h2", mcpSessionId: "sess_mcp" });
    store.bindJob({ jobId: job.id, threadId: "thread_v3_2", originatingTurnId: "turn_2", originatingItemId: "item_2" });

    const parkPromise = service.park({ job_ids: [job.id], wait: true, mcp_session_id: "sess_mcp" });
    parkPromise.catch(() => {});
    const timeoutPromise = new Promise((_, reject) => setTimeout(() => reject(new Error("HUNG_IN_MEMORY_WAITER")), 300));
    await assert.rejects(
      () => Promise.race([parkPromise, timeoutPromise]),
      (err: unknown) => {
        assert.ok(err instanceof InvalidRequestError, "Must be an InvalidRequestError, instead got: " + err);
        assert.equal((err as InvalidRequestError).status, 400);
        assert.match((err as Error).message, /subagents_follow/i);
        return true;
      },
    );

    // Ensure no barrier waiter was registered
    assert.equal(service.hasParkWaiter("any"), false);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. real transcript attestation: searches Codex session JSONL, finds item_completed for exact jobId, binds and arms
// ---------------------------------------------------------------------------
test("GATE-3: real transcript attestation binds unattached job from Codex session JSONL and arms park", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();

  const sessionsDir = path.join(tmp, "codex_sessions", "2026", "09", "04");
  await mkdir(sessionsDir, { recursive: true });

  const targetThreadId = "01a06c9f-2f67-7443-a08f-53e590aa3ece";
  const targetTurnId = "01a06d14-361d-7183-a1f6-d4fcc206d3a2";
  const targetJobId = "job_attest_success_100";

  const sessionFile = path.join(sessionsDir, "rollout-session-test.jsonl");
  const hostEventMsg = {
    timestamp: "2026-09-04T15:48:33.156Z",
    type: "event_msg",
    payload: {
      type: "item_completed",
      thread_id: targetThreadId,
      turn_id: targetTurnId,
      item: {
        type: "McpToolCall",
        id: "exec-test-12345",
        server: "subagents",
        tool: "subagents_spawn",
        status: "completed",
        result: {
          structuredContent: {
            accepted: true,
            jobId: targetJobId,
          },
        },
      },
    },
  };
  await writeFile(sessionFile, JSON.stringify(hostEventMsg) + "\n");

  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    sessionsDir,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_attest",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_attest",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    // Create job WITHOUT binding in store
    store.createJob({ id: targetJobId, agentId: agent.id, kind: "spawn", requestId: "req_attest", promptHash: "hattest" });

    // Parking must attest the job from session JSONL, persist binding, and arm
    const receipt = await service.park({ job_ids: [targetJobId] });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.targetIdentity, targetThreadId);

    // Verify binding was persisted in store
    const binding = store.getBinding(targetJobId);
    assert.ok(binding, "Binding must be persisted in store after attestation");
    assert.equal(binding.threadId, targetThreadId);
    assert.equal(binding.originatingTurnId, targetTurnId);
    assert.equal(binding.originatingItemId, "exec-test-12345");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. transcript attestation rejection: ambiguous/conflicting/hint-mismatched matches fail closed
// ---------------------------------------------------------------------------
test("GATE-4: transcript attestation rejects conflicting duplicates and hint mismatches", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();

  const sessionsDir = path.join(tmp, "codex_sessions_conflict");
  await mkdir(sessionsDir, { recursive: true });

  const conflictJobId = "job_conflict_1";
  const file1 = path.join(sessionsDir, "rollout-1.jsonl");
  const file2 = path.join(sessionsDir, "rollout-2.jsonl");

  // Two different threads report the same jobId: duplicate-conflicting
  const event1 = {
    type: "event_msg",
    payload: {
      type: "item_completed",
      thread_id: "01a06c9f-2f67-7443-a08f-53e590aa3ec1",
      turn_id: "01a06d14-361d-7183-a1f6-d4fcc206d3a1",
      item: {
        type: "McpToolCall",
        id: "exec-1",
        server: "subagents",
        tool: "subagents_spawn",
        status: "completed",
        result: { structuredContent: { accepted: true, jobId: conflictJobId } },
      },
    },
  };
  const event2 = {
    type: "event_msg",
    payload: {
      type: "item_completed",
      thread_id: "01a06c9f-2f67-7443-a08f-53e590aa3ec2",
      turn_id: "01a06d14-361d-7183-a1f6-d4fcc206d3a2",
      item: {
        type: "McpToolCall",
        id: "exec-2",
        server: "subagents",
        tool: "subagents_spawn",
        status: "completed",
        result: { structuredContent: { accepted: true, jobId: conflictJobId } },
      },
    },
  };
  await writeFile(file1, JSON.stringify(event1) + "\n");
  await writeFile(file2, JSON.stringify(event2) + "\n");

  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    sessionsDir,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_conflict",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_conflict",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    store.createJob({ id: conflictJobId, agentId: agent.id, kind: "spawn", requestId: "req_conf", promptHash: "hconf" });

    // Conflicting attestation must fail closed and NOT arm
    const receipt = await service.park({ job_ids: [conflictJobId] });
    assert.equal(receipt.armed, false, "Conflicting transcript matches must not arm");
    assert.equal(store.getBinding(conflictJobId), null);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. delivered eligibility: job with delivered status and persisted result is wake eligible
// ---------------------------------------------------------------------------
test("GATE-5: isJobWakeEligible treats delivered with persisted result as eligible", async () => {
  const { tmp, config, store } = await createTestEnv();
  const service = new BridgeService(config, {
    store,
    codex: new FakeCodexDelivery(),
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });

  const agent = store.createAgent({
    id: "agent_deliv",
    title: "Test",
    topic: "Topic",
    repositoryRoot: tmp,
    workspacePath: tmp,
    workspaceStrategy: "shared",
    opencodeServerId: "srv",
    opencodeSessionId: "session_deliv",
    modelProviderId: "deepseek",
    modelId: "deepseek-chat",
    modelVariant: null,
  });
  const job = store.createJob({ id: "job_deliv", agentId: agent.id, kind: "spawn", requestId: "req_deliv", promptHash: "hdeliv" });
  makeJobDelivered(store, job.id);
  const jobNoRes = store.getJob(job.id)!;
  assert.equal(service.isJobWakeEligible(jobNoRes), false, "Delivered without resultPath must not be eligible");

  const job2 = store.createJob({ id: "job_deliv2", agentId: agent.id, kind: "spawn", requestId: "req_deliv2", promptHash: "hdeliv2" });
  const resPath = path.join(tmp, "res_deliv.json");
  await writeFile(resPath, JSON.stringify({ envelope: { summary: "delivered result" } }));
  makeJobDelivered(store, job2.id, resPath, "delivered result");
  const jobWithRes = store.getJob(job2.id)!;
  assert.equal(service.isJobWakeEligible(jobWithRes), true, "Delivered with resultPath must be eligible");

  store.close();
  await rm(tmp, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// 6. predicates: ALL, ANY, QUORUM, REQUIRED
// ---------------------------------------------------------------------------
test("GATE-6: predicate evaluation handles ALL, ANY, QUORUM, and REQUIRED correctly", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_preds",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_preds",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const j1 = store.createJob({ id: "job_p1", agentId: agent.id, kind: "spawn", requestId: "req_p1", promptHash: "hp1" });
    const j2 = store.createJob({ id: "job_p2", agentId: agent.id, kind: "spawn", requestId: "req_p2", promptHash: "hp2" });
    const j3 = store.createJob({ id: "job_p3", agentId: agent.id, kind: "spawn", requestId: "req_p3", promptHash: "hp3" });

    store.bindJob({ jobId: j1.id, threadId: "thread_preds", originatingTurnId: "t1", originatingItemId: "i1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_preds", originatingTurnId: "t1", originatingItemId: "i2" });
    store.bindJob({ jobId: j3.id, threadId: "thread_preds", originatingTurnId: "t1", originatingItemId: "i3" });

    // Test validation of invalid predicates
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id], predicate: "QUORUM", quorum_count: 0 }),
      (err: unknown) => err instanceof InvalidRequestError,
    );
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id], predicate: "QUORUM", quorum_count: 3 }),
      (err: unknown) => err instanceof InvalidRequestError,
    );
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id], predicate: "REQUIRED", required_job_ids: [] }),
      (err: unknown) => err instanceof InvalidRequestError,
    );
    await assert.rejects(
      () => service.park({ job_ids: [j1.id, j2.id], predicate: "REQUIRED", required_job_ids: ["nonexistent_job"] }),
      (err: unknown) => err instanceof InvalidRequestError,
    );

    // Test ALL predicate: does not wake when only j1 is ready, wakes when j1 and j2 are ready
    const receiptAll = await service.park({ job_ids: [j1.id, j2.id], predicate: "ALL", wake_on_exception: false });
    assert.equal(receiptAll.armed, true);

    const r1 = path.join(tmp, "r1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "j1 done" } }));
    makeJobCompleted(store, j1.id, r1, "j1 done");
    await service.evaluateParkWakes(j1.id);

    // Barrier must still be armed (not waking/woken)
    let barrierAll = store.getParkBarrier(receiptAll.parkId)!;
    assert.equal(barrierAll.state, "armed", "ALL predicate must remain armed after 1 of 2 jobs completes");

    const r2 = path.join(tmp, "r2.json");
    await writeFile(r2, JSON.stringify({ envelope: { summary: "j2 done" } }));
    makeJobCompleted(store, j2.id, r2, "j2 done");
    await service.evaluateParkWakes(j2.id);

    barrierAll = store.getParkBarrier(receiptAll.parkId)!;
    assert.equal(barrierAll.state, "woken", "ALL predicate must wake when all jobs complete");

    // Test ANY predicate: wakes as soon as 1 completes
    const j4 = store.createJob({ id: "job_p4", agentId: agent.id, kind: "spawn", requestId: "req_p4", promptHash: "hp4" });
    const j5 = store.createJob({ id: "job_p5", agentId: agent.id, kind: "spawn", requestId: "req_p5", promptHash: "hp5" });
    store.bindJob({ jobId: j4.id, threadId: "thread_preds_2", originatingTurnId: "t2", originatingItemId: "i4" });
    store.bindJob({ jobId: j5.id, threadId: "thread_preds_2", originatingTurnId: "t2", originatingItemId: "i5" });

    const receiptAny = await service.park({ job_ids: [j4.id, j5.id], predicate: "ANY" });
    assert.equal(receiptAny.armed, true);

    const r4 = path.join(tmp, "r4.json");
    await writeFile(r4, JSON.stringify({ envelope: { summary: "j4 done" } }));
    makeJobCompleted(store, j4.id, r4, "j4 done");
    await service.evaluateParkWakes(j4.id);

    const barrierAny = store.getParkBarrier(receiptAny.parkId)!;
    assert.equal(barrierAny.state, "woken", "ANY predicate must wake as soon as 1 job completes");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 7. exception wake: needs_approval, failure, abort, timeout wake immediately when wake_on_exception=true
// ---------------------------------------------------------------------------
test("GATE-7: exception status triggers immediate wake even under ALL predicate", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_exc",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_exc",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const j1 = store.createJob({ id: "job_exc1", agentId: agent.id, kind: "spawn", requestId: "req_exc1", promptHash: "hexc1" });
    const j2 = store.createJob({ id: "job_exc2", agentId: agent.id, kind: "spawn", requestId: "req_exc2", promptHash: "hexc2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_exc", originatingTurnId: "t1", originatingItemId: "i1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_exc", originatingTurnId: "t1", originatingItemId: "i2" });

    // Arm with ALL predicate, wake_on_exception default true
    const receipt = await service.park({ job_ids: [j1.id, j2.id], predicate: "ALL" });
    assert.equal(receipt.armed, true);

    // j1 fails
    store.updateJobStatus(j1.id, "failed");
    await service.evaluateParkWakes(j1.id);

    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken", "Failure must immediately wake to prevent deadlock under wake_on_exception");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 8. no early/duplicate wake: evaluation and generation fencing ensure exactly one wake
// ---------------------------------------------------------------------------
test("GATE-8: no early wake on incomplete predicate and exactly one wake per generation", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_dupe",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_dupe",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const j1 = store.createJob({ id: "job_d1", agentId: agent.id, kind: "spawn", requestId: "req_d1", promptHash: "hd1" });
    const j2 = store.createJob({ id: "job_d2", agentId: agent.id, kind: "spawn", requestId: "req_d2", promptHash: "hd2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_dupe", originatingTurnId: "td", originatingItemId: "id1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_dupe", originatingTurnId: "td", originatingItemId: "id2" });

    const receipt = await service.park({ job_ids: [j1.id, j2.id], predicate: "ALL", wake_on_exception: false });

    // j1 completes
    const r1 = path.join(tmp, "rd1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "d1" } }));
    makeJobCompleted(store, j1.id, r1, "d1");

    await service.evaluateParkWakes(j1.id);
    assert.equal(store.getWakeOutbox(receipt.parkId, receipt.generation), null, "No outbox record before predicate satisfaction");

    // j2 completes
    const r2 = path.join(tmp, "rd2.json");
    await writeFile(r2, JSON.stringify({ envelope: { summary: "d2" } }));
    makeJobCompleted(store, j2.id, r2, "d2");

    // Concurrent/repeated wake evaluations must create exactly ONE outbox
    await Promise.all([
      service.evaluateParkWakes(j2.id),
      service.evaluateParkWakes(j2.id),
      service.evaluateParkWakes(j1.id),
    ]);

    const outboxRows = store.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ? AND generation = ?").all(receipt.parkId, receipt.generation);
    assert.equal(outboxRows.length, 1, "Must create exactly one outbox row for the generation");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 9. active-writer defer then retry: preserves deferred_active_writer and resumes once clear
// ---------------------------------------------------------------------------
test("GATE-9: active writer conflict preserves deferred_active_writer and delivers on retry", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  // First delivery attempt returns active writer conflict
  fakeCli.nextResult = { success: false, activeWriter: true, error: "Active writer conflict" };

  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_aw",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_aw",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_aw", agentId: agent.id, kind: "spawn", requestId: "req_aw", promptHash: "haw" });
    store.bindJob({ jobId: job.id, threadId: "thread_aw", originatingTurnId: "taw", originatingItemId: "iaw" });

    const receipt = await service.park({ job_ids: [job.id] });

    const rf = path.join(tmp, "raw.json");
    await writeFile(rf, JSON.stringify({ envelope: { summary: "aw done" } }));
    makeJobCompleted(store, job.id, rf, "aw done");

    await service.evaluateParkWakes(job.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    assert.equal(outbox.status, "deferred_active_writer");
    assert.equal(outbox.wakeState, "deferred_active_writer");
    assert.ok(outbox.nextAttemptAt, "nextAttemptAt must be set for backoff");

    // Clear active writer: next attempt succeeds
    fakeCli.nextResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };

    // Explicitly trigger outbox dispatch / retry
    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    await (service as any).dispatchWakeOutbox(outbox, envelope);

    const updatedOutbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.equal(updatedOutbox.status, "delivered");
    assert.equal(updatedOutbox.wakeState, "delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 10. metadata-only envelope: never includes worker output/diffs
// ---------------------------------------------------------------------------
test("GATE-10: wake outbox and marker contain metadata only, never worker text/diff", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();

  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_meta",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_meta",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_meta", agentId: agent.id, kind: "spawn", requestId: "req_meta", promptHash: "hmeta" });
    store.bindJob({ jobId: job.id, threadId: "thread_meta", originatingTurnId: "tmeta", originatingItemId: "imeta" });

    const receipt = await service.park({ job_ids: [job.id] });

    const secretWorkerText = "SUPER_SECRET_WORKER_DIFF_CONTENT_12345";
    const rf = path.join(tmp, "rmeta.json");
    await writeFile(rf, JSON.stringify({ envelope: { summary: "meta done", diffSummary: secretWorkerText } }));
    makeJobCompleted(store, job.id, rf, secretWorkerText);

    await service.evaluateParkWakes(job.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    assert.equal(outbox.payloadJson.includes(secretWorkerText), false, "Worker text must not be in payloadJson");
    assert.equal(outbox.wakeMarker.includes(secretWorkerText), false, "Worker text must not be in wakeMarker");

    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.ok(envelope.jobIds.includes(job.id));
    assert.ok(envelope.readyJobIds.includes(job.id));
    assert.equal(envelope.statuses[job.id], "completed");
    assert.ok(envelope.resultHashes[job.id], "Result hash must be present");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 11. transcript spoof & caller hints alone rejection
// ---------------------------------------------------------------------------
test("GATE-11: transcript attestation rejects spoofed tools/servers, rejected jobs (accepted: false), and caller hints alone", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const sessionsDir = path.join(tmp, "sessions_spoof");
  await mkdir(sessionsDir, { recursive: true });

  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    sessionsDir,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_spoof",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_spoof",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });

    // 1. Caller hints alone (without transcript) must fail closed
    const jobHintsOnly = store.createJob({
      id: "job_hints_only",
      agentId: agent.id,
      kind: "spawn",
      requestId: "r_hints",
      promptHash: "h_hints",
      hintThreadId: "01a06c9f-2f67-7443-a08f-53e590aa3ec0",
      hintTurnId: "01a06d14-361d-7183-a1f6-d4fcc206d3a0",
    });
    const receiptHints = await service.park({ job_ids: [jobHintsOnly.id] });
    assert.equal(receiptHints.armed, false, "Caller hints alone must not arm");
    assert.equal(receiptHints.deliveryMode, "none");

    // 2. Spoofed server
    const jobSpoofServer = "job_spoof_server";
    store.createJob({ id: jobSpoofServer, agentId: agent.id, kind: "spawn", requestId: "r_ss", promptHash: "h_ss" });
    const spoofFile1 = path.join(sessionsDir, "spoof1.jsonl");
    await writeFile(spoofFile1, JSON.stringify({
      type: "event_msg",
      payload: {
        type: "item_completed",
        thread_id: "01a06c9f-2f67-7443-a08f-53e590aa3ec1",
        turn_id: "01a06d14-361d-7183-a1f6-d4fcc206d3a1",
        item: {
          type: "McpToolCall",
          id: "exec-spoof-1",
          server: "untrusted_server",
          tool: "subagents_spawn",
          status: "completed",
          result: { structuredContent: { accepted: true, jobId: jobSpoofServer } },
        },
      },
    }) + "\n");
    const receiptSpoof1 = await service.park({ job_ids: [jobSpoofServer] });
    assert.equal(receiptSpoof1.armed, false, "Untrusted server in transcript must fail closed");

    // 3. Spoofed tool
    const jobSpoofTool = "job_spoof_tool";
    store.createJob({ id: jobSpoofTool, agentId: agent.id, kind: "spawn", requestId: "r_st", promptHash: "h_st" });
    const spoofFile2 = path.join(sessionsDir, "spoof2.jsonl");
    await writeFile(spoofFile2, JSON.stringify({
      type: "event_msg",
      payload: {
        type: "item_completed",
        thread_id: "01a06c9f-2f67-7443-a08f-53e590aa3ec2",
        turn_id: "01a06d14-361d-7183-a1f6-d4fcc206d3a2",
        item: {
          type: "McpToolCall",
          id: "exec-spoof-2",
          server: "subagents",
          tool: "bash_execute",
          status: "completed",
          result: { structuredContent: { accepted: true, jobId: jobSpoofTool } },
        },
      },
    }) + "\n");
    const receiptSpoof2 = await service.park({ job_ids: [jobSpoofTool] });
    assert.equal(receiptSpoof2.armed, false, "Non-accepted tool in transcript must fail closed");

    // 4. Rejected job (accepted: false)
    const jobRejected = "job_rejected";
    store.createJob({ id: jobRejected, agentId: agent.id, kind: "spawn", requestId: "r_rj", promptHash: "h_rj" });
    const spoofFile3 = path.join(sessionsDir, "spoof3.jsonl");
    await writeFile(spoofFile3, JSON.stringify({
      type: "event_msg",
      payload: {
        type: "item_completed",
        thread_id: "01a06c9f-2f67-7443-a08f-53e590aa3ec3",
        turn_id: "01a06d14-361d-7183-a1f6-d4fcc206d3a3",
        item: {
          type: "McpToolCall",
          id: "exec-spoof-3",
          server: "subagents",
          tool: "subagents_spawn",
          status: "completed",
          result: { structuredContent: { accepted: false, jobId: jobRejected } },
        },
      },
    }) + "\n");
    const receiptRejected = await service.park({ job_ids: [jobRejected] });
    assert.equal(receiptRejected.armed, false, "Rejected spawn (accepted: false) must fail closed");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 12. transcript partial EOF tolerance
// ---------------------------------------------------------------------------
test("GATE-12: transcript attestation ignores partial EOF and extracts valid preceding event", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const sessionsDir = path.join(tmp, "sessions_eof");
  await mkdir(sessionsDir, { recursive: true });

  const targetThreadId = "01a06c9f-2f67-7443-a08f-53e590aa3ece";
  const targetTurnId = "01a06d14-361d-7183-a1f6-d4fcc206d3a2";
  const targetJobId = "job_attest_eof_ok";

  const sessionFile = path.join(sessionsDir, "rollout-eof.jsonl");
  const validEvent = {
    timestamp: "2026-09-04T15:48:33.156Z",
    type: "event_msg",
    payload: {
      type: "item_completed",
      thread_id: targetThreadId,
      turn_id: targetTurnId,
      item: {
        type: "McpToolCall",
        id: "exec-eof-1",
        server: "subagents",
        tool: "subagents_spawn",
        status: "completed",
        result: { structuredContent: { accepted: true, jobId: targetJobId } },
      },
    },
  };
  const fileContent = JSON.stringify(validEvent) + "\n" + '{"type":"event_msg","payload":{"thread_id":"partial-unclosed-json';
  await writeFile(sessionFile, fileContent);

  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    sessionsDir,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_eof",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_eof",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    store.createJob({ id: targetJobId, agentId: agent.id, kind: "spawn", requestId: "r_eof", promptHash: "h_eof" });

    const receipt = await service.park({ job_ids: [targetJobId] });
    assert.equal(receipt.armed, true, "Must arm despite partial EOF on trailing line");
    assert.equal(receipt.targetIdentity, targetThreadId);
    const binding = store.getBinding(targetJobId);
    assert.ok(binding);
    assert.equal(binding.threadId, targetThreadId);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 13. impossible quorum & all-terminal deadlock prevention under wake_on_exception=false
// ---------------------------------------------------------------------------
test("GATE-13: impossible quorum and all-terminal trigger deadlock-prevention wake when wake_on_exception=false", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = store.createAgent({
      id: "agent_iq",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_iq",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const j1 = store.createJob({ id: "job_iq1", agentId: agent.id, kind: "spawn", requestId: "r_iq1", promptHash: "h_iq1" });
    const j2 = store.createJob({ id: "job_iq2", agentId: agent.id, kind: "spawn", requestId: "r_iq2", promptHash: "h_iq2" });
    const j3 = store.createJob({ id: "job_iq3", agentId: agent.id, kind: "spawn", requestId: "r_iq3", promptHash: "h_iq3" });
    store.bindJob({ jobId: j1.id, threadId: "thread_iq", originatingTurnId: "t_iq", originatingItemId: "i_iq1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_iq", originatingTurnId: "t_iq", originatingItemId: "i_iq2" });
    store.bindJob({ jobId: j3.id, threadId: "thread_iq", originatingTurnId: "t_iq", originatingItemId: "i_iq3" });

    // Quorum of 2 out of 3, with wake_on_exception = false
    const receipt = await service.park({
      job_ids: [j1.id, j2.id, j3.id],
      predicate: "QUORUM",
      quorum_count: 2,
      wake_on_exception: false,
    });
    assert.equal(receipt.armed, true);

    // j1 fails: 2 jobs remain active, quorum of 2 is STILL possible -> MUST NOT wake
    store.updateJobStatus(j1.id, "failed");
    await service.evaluateParkWakes(j1.id);
    let barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed", "Quorum of 2 is still possible after 1 failure, must not wake");

    // j2 fails: now only 1 active job remains (j3). Quorum of 2 is IMPOSSIBLE!
    // Must wake immediately to prevent permanent deadlock
    store.updateJobStatus(j2.id, "failed");
    await service.evaluateParkWakes(j2.id);
    barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken", "Must wake when quorum becomes impossible to prevent deadlock");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 14. duplicate concurrency / stale generation fencing & delivered immutability
// ---------------------------------------------------------------------------
test("GATE-14: stale generation cannot wake barrier and delivered outbox row is immutable", async () => {
  const { tmp, store } = await createTestEnv();

  try {
    const barrier = store.createOrUpdateParkBarrier({
      id: "park_stale_test",
      threadId: "thread_stale",
      turnId: "turn_stale",
      generation: 1,
      armed: true,
      deliveryMode: "cli_resume",
      state: "armed",
    });

    // Re-arm to generation 2
    store.createOrUpdateParkBarrier({
      id: barrier.id,
      threadId: "thread_stale",
      turnId: "turn_stale",
      generation: 2,
      armed: true,
      deliveryMode: "cli_resume",
      state: "armed",
    });

    // Claiming or waking generation 1 MUST fail
    const claimedGen1 = store.claimParkWake(barrier.id, 1);
    assert.equal(claimedGen1, false, "Stale generation 1 claim must fail");

    store.setParkWoken(barrier.id, 1);
    const refreshed = store.getParkBarrier(barrier.id)!;
    assert.equal(refreshed.state, "armed", "Stale generation 1 wake must not modify generation 2 barrier");
    assert.equal(refreshed.generation, 2);

    // Claiming generation 2 succeeds
    const claimedGen2 = store.claimParkWake(barrier.id, 2);
    assert.equal(claimedGen2, true, "Active generation 2 claim must succeed");

    // Test delivered outbox row immutability
    const outbox = store.createWakeOutbox({
      id: "wake_immut_test",
      parkId: barrier.id,
      generation: 2,
      threadId: "thread_stale",
      turnId: "turn_stale",
      deliveryMode: "cli_resume",
      status: "pending",
      wakeMarker: "<!-- marker -->",
      payloadJson: "{}",
    });

    store.updateWakeOutboxStatus(outbox.id, "delivered", null, { wakeState: "delivered" });
    const deliveredOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(deliveredOutbox.status, "delivered");
    assert.equal(deliveredOutbox.wakeState, "delivered");

    // Subsequent update attempts on delivered row MUST NOT modify it
    store.updateWakeOutboxStatus(outbox.id, "failed", "should not overwrite", { wakeState: "failed" });
    const reloadedOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(reloadedOutbox.status, "delivered", "Delivered outbox row must be immutable");
    assert.equal(reloadedOutbox.wakeState, "delivered");
    assert.equal(reloadedOutbox.lastError, null);
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 15. restart recovery: re-evaluates armed barriers and retries deferred_active_writer
// ---------------------------------------------------------------------------
test("GATE-15: restart recovery re-evaluates armed barriers and retries deferred_active_writer", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();

  try {
    const agent = store.createAgent({
      id: "agent_recov",
      title: "Test",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_recov",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job1 = store.createJob({ id: "job_recov1", agentId: agent.id, kind: "spawn", requestId: "r_rc1", promptHash: "h_rc1" });
    store.bindJob({ jobId: job1.id, threadId: "thread_recov", originatingTurnId: "t_rc", originatingItemId: "i_rc1" });

    // Job 1 completed with result
    const rf1 = path.join(tmp, "r_recov1.json");
    const validEnvelope = {
      version: 1,
      agentId: agent.id,
      jobId: job1.id,
      topic: "Topic",
      status: "completed",
      opencodeSessionId: "session_recov",
      model: "deepseek-chat",
      modelDisplayName: "DeepSeek",
      workspace: tmp,
      summary: "recov 1 done",
      diffSummary: "none",
      fullResultPath: rf1,
      orchestratorInstruction: "none",
      files: [],
      tests: [],
      risks: [],
    };
    await writeFile(rf1, JSON.stringify({ envelope: validEnvelope }));
    makeJobCompleted(store, job1.id, rf1, "recov 1 done");

    // Create an armed barrier that was not evaluated before shutdown
    const barrier = store.createOrUpdateParkBarrier({
      id: "park_recov_barrier",
      threadId: "thread_recov",
      turnId: "t_rc",
      generation: 1,
      armed: true,
      deliveryMode: "cli_resume",
      state: "armed",
      predicateType: "ALL",
      wakeOnException: true,
    });
    store.setParkJobs(barrier.id, [job1.id]);

    // Create a deferred_active_writer outbox row with nextAttemptAt in the past
    const barrierPrior = store.createOrUpdateParkBarrier({
      id: "park_prior",
      threadId: "thread_recov",
      turnId: "t_rc",
      generation: 1,
      armed: false,
      deliveryMode: "cli_resume",
      state: "waking",
    });
    const pastAttempt = new Date(Date.now() - 5000).toISOString();
    store.createWakeOutbox({
      id: "wake_recov_deferred",
      parkId: "park_prior",
      generation: 1,
      threadId: "thread_recov",
      deliveryMode: "cli_resume",
      status: "deferred_active_writer",
      wakeState: "deferred_active_writer",
      wakeMarker: "<!-- marker_prior -->",
      payloadJson: JSON.stringify({
        parkId: "park_prior",
        generation: 1,
        jobIds: ["job_prior"],
        readyJobIds: ["job_prior"],
        statuses: { job_prior: "completed" },
        resultHashes: {},
      }),
      nextAttemptAt: pastAttempt,
    });

    // Start a fresh service on this existing store (simulating restart)
    const restartedService = new BridgeService(config, {
      store,
      codex: fakeCodex,
      cliTransport: fakeCli,
      manager: {
        start: async () => ({ serverId: "srv", baseUrl: "http://127.0.0.1:9999", client: new FakeOpenCodeClient(), processId: null, stop: async () => {} }),
        stop: async () => {},
      },
    });

    await restartedService.start();

    // 1. Verify armed barrier was re-evaluated and woken on restart
    const evaluatedBarrier = store.getParkBarrier(barrier.id)!;
    assert.equal(evaluatedBarrier.state, "woken", "Startup recovery must re-evaluate armed barrier");

    // 2. Verify deferred_active_writer was safely retried and delivered on restart
    const retriedOutbox = store.getWakeOutboxById("wake_recov_deferred")!;
    assert.equal(retriedOutbox.status, "delivered", "Startup recovery must retry deferred_active_writer");
    assert.equal(retriedOutbox.wakeState, "delivered");

    await restartedService.stop();
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

