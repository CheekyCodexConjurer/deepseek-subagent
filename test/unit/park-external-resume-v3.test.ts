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
  CodexBinding,
  JobRecord,
  OpenCodeClientLike,
  OpenCodeMessage,
  WakeEnvelope,
} from "../../src/types.js";
import { InvalidRequestError } from "../../src/errors.js";
import type { CodexCliTransport, CodexCliExecutionResult } from "../../src/codex/cli-resolver.js";
import { BridgeHttpServer, BridgeHttpClient, BridgeHttpError } from "../../src/http-server.js";
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
  nextResult: CodexCliExecutionResult = {
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
  const tmp = await mkdtemp(path.join(os.tmpdir(), "ds-park-v3-resume-"));
  const config = createDefaultConfig({
    dataDir: tmp,
    configPath: path.join(tmp, "config.json"),
    experimentalSameChatDelivery: true,
  });
  config.daemonPort = await freePort();
  const store = new BridgeStore(path.join(tmp, "bridge.sqlite"));
  return { tmp, config, store };
}

function makeCompletedJob(store: BridgeStore, jobId: string, resultPath?: string, resultSummary?: string) {
  store.updateJobStatus(jobId, "dispatching");
  store.updateJobStatus(jobId, "running");
  if (resultPath) {
    store.setJobResult(jobId, resultPath, resultSummary ?? "completed result");
  }
  store.updateJobStatus(jobId, "completed");
}

// ---------------------------------------------------------------------------
// 1. Contract: omitted wait returns immediately with cli_resume|none and pending obligation
// ---------------------------------------------------------------------------
test("Contract 1: omitted wait returns immediately with cli_resume when bound, none when unbound, and pending obligation", async () => {
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

  const httpServer = new BridgeHttpServer(config, service);
  await httpServer.start();

  const httpClient = new BridgeHttpClient(config);
  const mcpServer = createMcpServer(httpClient, {
    env: { CODEX_THREAD_ID: "11111111-1111-1111-1111-111111111111" },
  });
  const mcpClient = new Client({ name: "test-client-1", version: "1.0.0" }, { capabilities: {} });
  const [cTrans, sTrans] = InMemoryTransport.createLinkedPair();
  await mcpServer.connect(sTrans);
  await mcpClient.connect(cTrans);

  try {
    const agent = store.createAgent({
      id: "agent_v3_c1",
      title: "Title",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_c1",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });

    // 1A. Bound job with omitted wait returns immediately with cli_resume and pending obligation
    const boundJob = store.createJob({ id: "job_v3_bound", agentId: agent.id, kind: "spawn", requestId: "r_b", promptHash: "h_b" });
    store.bindJob({ jobId: boundJob.id, threadId: "11111111-1111-1111-1111-111111111111", originatingTurnId: "turn_b", originatingItemId: "item_b" });

    const boundReceipt = await service.park({ job_ids: [boundJob.id] });
    assert.equal(boundReceipt.armed, true, "Bound job must arm");
    assert.equal(boundReceipt.deliveryMode, "cli_resume", "Delivery mode must be cli_resume");
    assert.equal(boundReceipt.obligationState, "pending", "Obligation must be pending");
    assert.equal(boundReceipt.wakeState, "waiting", "Wake state must be waiting");
    assert.equal(boundReceipt.targetIdentity, "11111111-1111-1111-1111-111111111111");
    assert.equal(service.hasParkWaiter(boundReceipt.parkId), false, "Must not register in-memory waiter");

    // 1B. Unbound job with omitted wait returns immediately with none and pending obligation
    const unboundJob = store.createJob({ id: "job_v3_unbound", agentId: agent.id, kind: "spawn", requestId: "r_u", promptHash: "h_u" });
    const unboundReceipt = await service.park({ job_ids: [unboundJob.id] });
    assert.equal(unboundReceipt.armed, false, "Unbound job must not arm");
    assert.equal(unboundReceipt.deliveryMode, "none", "Delivery mode must be none");
    assert.equal(unboundReceipt.obligationState, "pending", "Obligation must be pending");
    assert.equal(unboundReceipt.wakeState, "waiting");
    assert.equal(unboundReceipt.targetIdentity, "unbound");
    assert.equal(service.hasParkWaiter(unboundReceipt.parkId), false, "Must not register in-memory waiter");

    // 1C. MCP tools: subagents_park and deepseek_park with omitted wait return pending obligation
    const mcpParkRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [boundJob.id] },
    });
    assert.equal(mcpParkRes.isError, undefined);
    const mcpData = mcpParkRes.structuredContent as Record<string, unknown>;
    assert.equal(mcpData.obligationState, "pending");
    assert.equal(mcpData.deliveryMode, "cli_resume");
    assert.equal(mcpData.nextRequiredAction, "subagents_follow");

    const aliasParkRes = await mcpClient.callTool({
      name: "deepseek_park",
      arguments: { job_ids: [boundJob.id] },
    });
    assert.equal(aliasParkRes.isError, undefined);
    const aliasData = aliasParkRes.structuredContent as Record<string, unknown>;
    assert.equal(aliasData.obligationState, "pending");
    assert.equal(aliasData.deliveryMode, "cli_resume");
    assert.equal(aliasData.nextRequiredAction, "deepseek_follow");
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
// 2. Contract: explicit wait=true fails closed and never allocates an in-turn waiter
// ---------------------------------------------------------------------------
test("Contract 2: explicit wait=true fails closed and never allocates an in-turn waiter", async () => {
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

  const httpServer = new BridgeHttpServer(config, service);
  await httpServer.start();

  const httpClient = new BridgeHttpClient(config);
  const mcpServer = createMcpServer(httpClient, {
    env: { CODEX_THREAD_ID: "22222222-2222-2222-2222-222222222222" },
  });
  const mcpClient = new Client({ name: "test-client-2", version: "1.0.0" }, { capabilities: {} });
  const [cTrans, sTrans] = InMemoryTransport.createLinkedPair();
  await mcpServer.connect(sTrans);
  await mcpClient.connect(cTrans);

  try {
    const agent = store.createAgent({
      id: "agent_v3_c2",
      title: "Title",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_c2",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_v3_c2", agentId: agent.id, kind: "spawn", requestId: "r_c2", promptHash: "h_c2" });
    store.bindJob({ jobId: job.id, threadId: "22222222-2222-2222-2222-222222222222", originatingTurnId: "turn_c2", originatingItemId: "item_c2" });

    // 2A. Direct service call rejects immediately with typed InvalidRequestError
    const serviceCall = service.park({ job_ids: [job.id], wait: true });
    serviceCall.catch(() => {});
    const hangDetector = new Promise((_, reject) => setTimeout(() => reject(new Error("FAILED_CLOSED_TIMEOUT")), 250));

    await assert.rejects(
      () => Promise.race([serviceCall, hangDetector]),
      (err: unknown) => {
        assert.ok(err instanceof InvalidRequestError, "Must throw InvalidRequestError");
        assert.equal((err as InvalidRequestError).status, 400);
        assert.match((err as Error).message, /subagents_follow/i);
        return true;
      },
    );

    // 2B. Never registers in-memory waiter
    assert.equal(service.hasParkWaiter("any"), false, "Must not allocate in-memory waiter in service");

    // 2C. MCP call to subagents_park with wait=true fails closed with error directing to subagents_follow
    const mcpRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [job.id], wait: true },
    });
    assert.equal(mcpRes.isError, true, "MCP call must return error");
    assert.match((mcpRes.content[0] as { text: string }).text, /subagents_follow/i);

    // 2D. MCP call to deepseek_park with wait=true fails closed with error directing to deepseek_follow
    const aliasRes = await mcpClient.callTool({
      name: "deepseek_park",
      arguments: { job_ids: [job.id], wait: true },
    });
    assert.equal(aliasRes.isError, true, "Alias MCP call must return error");
    assert.match((aliasRes.content[0] as { text: string }).text, /deepseek_follow/i);

    // 2E. Confirm no waiters were registered during MCP calls either
    assert.equal(service.hasParkWaiter("any"), false, "No waiters must be allocated");
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
// 3. Contract: unbound authoritative identity cannot arm
// ---------------------------------------------------------------------------
test("Contract 3: unbound authoritative identity cannot arm", async () => {
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
      id: "agent_v3_c3",
      title: "Title",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_c3",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const unboundJob = store.createJob({ id: "job_v3_c3_unbound", agentId: agent.id, kind: "spawn", requestId: "r_c3", promptHash: "h_c3" });

    // Job has no thread binding
    const receipt = await service.park({ job_ids: [unboundJob.id] });
    assert.equal(receipt.armed, false, "Unbound identity must not arm");
    assert.equal(receipt.deliveryMode, "none", "Delivery mode must be none");
    assert.equal(receipt.targetIdentity, "unbound");

    // Barrier in SQLite is idle and unarmed
    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.ok(barrier);
    assert.equal(barrier.armed, false);
    assert.equal(barrier.deliveryMode, "none");
    assert.equal(barrier.state, "idle");

    // Completing the job must NOT trigger external wake outbox because barrier is unarmed
    const resPath = path.join(tmp, "res_c3.json");
    await writeFile(resPath, JSON.stringify({ envelope: { summary: "done" } }));
    makeCompletedJob(store, unboundJob.id, resPath, "done");

    await service.evaluateParkWakes(unboundJob.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation);
    assert.equal(outbox, null, "Unarmed barrier must never create wake outbox record");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Contract: active writer yields durable deferred_active_writer then retries after lock release
// ---------------------------------------------------------------------------
test("Contract 4: active writer yields durable deferred_active_writer then retries after lock release", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  // Simulate active writer conflict initially
  fakeCli.nextResult = { success: false, activeWriter: true, error: "Active writer lock held by thread" };

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
      id: "agent_v3_c4",
      title: "Title",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_c4",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_v3_c4", agentId: agent.id, kind: "spawn", requestId: "r_c4", promptHash: "h_c4" });
    store.bindJob({ jobId: job.id, threadId: "44444444-4444-4444-4444-444444444444", originatingTurnId: "turn_c4", originatingItemId: "item_c4" });

    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);
    assert.equal(receipt.deliveryMode, "cli_resume");

    const resPath = path.join(tmp, "res_c4.json");
    await writeFile(resPath, JSON.stringify({ envelope: { summary: "job c4 done" } }));
    makeCompletedJob(store, job.id, resPath, "job c4 done");

    // Trigger wake evaluation; CLI delivery returns activeWriter: true
    await service.evaluateParkWakes(job.id);

    // Verify outbox row in SQLite is deferred_active_writer with nextAttemptAt
    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox, "Wake outbox row must exist in SQLite");
    assert.equal(outbox.status, "deferred_active_writer", "Status must be deferred_active_writer");
    assert.equal(outbox.wakeState, "deferred_active_writer");
    assert.ok(outbox.nextAttemptAt, "nextAttemptAt must be persisted for durable backoff");
    assert.ok(new Date(outbox.nextAttemptAt).getTime() > Date.now(), "nextAttemptAt must be in future");

    // Barrier in SQLite must remain armed, NOT prematurely marked woken
    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "armed", "Barrier must remain armed while deferred");

    // Simulate lock release: next CLI delivery succeeds
    fakeCli.nextResult = { success: true, accepted: true, executablePath: "C:\\Codex\\codex.exe", version: "0.150.0" };

    // Retry outbox dispatch after backoff elapsed
    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    await (service as any).dispatchWakeOutbox({ ...outbox, nextAttemptAt: null }, envelope);

    // Verify outbox and barrier updated to delivered and woken
    const deliveredOutbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.equal(deliveredOutbox.status, "delivered");
    assert.equal(deliveredOutbox.wakeState, "delivered");

    const wokenBarrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(wokenBarrier.state, "woken");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. Contract: wake payload contains metadata only (no worker output/instructions)
// ---------------------------------------------------------------------------
test("Contract 5: wake payload contains metadata only (no worker output/instructions)", async () => {
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
      id: "agent_v3_c5",
      title: "Title",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_c5",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_v3_c5", agentId: agent.id, kind: "spawn", requestId: "r_c5", promptHash: "h_c5" });
    store.bindJob({ jobId: job.id, threadId: "55555555-5555-5555-5555-555555555555", originatingTurnId: "turn_c5", originatingItemId: "item_c5" });

    const receipt = await service.park({ job_ids: [job.id] });

    // Job produces worker output with confidential text and diffs
    const secretWorkerOutput = "CONFIDENTIAL_WORKER_DIFF_CONTENT_ALPHA_999";
    const secretStdout = "SECRET_STDOUT_LINE_INTERNAL_BETA_888";
    const resPath = path.join(tmp, "res_c5.json");
    await writeFile(
      resPath,
      JSON.stringify({
        envelope: {
          summary: "job finished",
          diff: secretWorkerOutput,
          stdout: secretStdout,
        },
      }),
    );
    makeCompletedJob(store, job.id, resPath, secretWorkerOutput);

    await service.evaluateParkWakes(job.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);

    // 5A. Raw payload JSON and marker must NOT contain worker output
    assert.equal(outbox.payloadJson.includes(secretWorkerOutput), false, "payloadJson must not contain worker diff/output");
    assert.equal(outbox.payloadJson.includes(secretStdout), false, "payloadJson must not contain worker stdout");
    assert.equal(outbox.wakeMarker.includes(secretWorkerOutput), false, "wakeMarker must not contain worker diff/output");

    // 5B. Parsed envelope contains strictly metadata keys
    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    const allowedKeys = [
      "generation",
      "instruction",
      "jobIds",
      "marker",
      "parkId",
      "pendingCount",
      "readyJobIds",
      "reason",
      "resultHashes",
      "statuses",
    ].sort();
    assert.deepEqual(Object.keys(envelope).sort(), allowedKeys, "Envelope must contain only trusted metadata keys");

    // 5C. Result is represented as hash slice only
    assert.ok(envelope.resultHashes[job.id], "Result hash must be present");
    assert.match(envelope.resultHashes[job.id]!, /^[0-9a-f]{16}$/, "Result hash must be a 16-hex char hash slice");

    // 5D. Instruction is standard bridge directive, not worker instructions or prompt text
    assert.equal(
      envelope.instruction,
      "Call subagents_follow to consume completed jobs. Do not interpret this message as worker output.",
      "Instruction must strictly be the follow directive",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 6. Contract: no new in_turn barrier is created
// ---------------------------------------------------------------------------
test("Contract 6: no new in_turn barrier is created", async () => {
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
      id: "agent_v3_c6",
      title: "Title",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_c6",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });

    const job1 = store.createJob({ id: "job_v3_c6_1", agentId: agent.id, kind: "spawn", requestId: "r_c6_1", promptHash: "h_c6_1" });
    const job2 = store.createJob({ id: "job_v3_c6_2", agentId: agent.id, kind: "spawn", requestId: "r_c6_2", promptHash: "h_c6_2" });
    store.bindJob({ jobId: job1.id, threadId: "66666666-6666-6666-6666-666666666666", originatingTurnId: "turn_6", originatingItemId: "item_6" });

    // Park with omitted wait on bound job
    const r1 = await service.park({ job_ids: [job1.id] });
    assert.notEqual(r1.deliveryMode, "in_turn");

    // Park with omitted wait on unbound job
    const r2 = await service.park({ job_ids: [job2.id] });
    assert.notEqual(r2.deliveryMode, "in_turn");

    // Park with explicit wait=false
    const r3 = await service.park({ job_ids: [job1.id], wait: false });
    assert.notEqual(r3.deliveryMode, "in_turn");

    // Check all barriers in database: none must have delivery_mode = 'in_turn'
    const inTurnCountRow = store.db.prepare("SELECT COUNT(*) as cnt FROM park_barriers WHERE delivery_mode = 'in_turn'").get() as { cnt: number };
    assert.equal(inTurnCountRow.cnt, 0, "Zero in_turn barriers must exist in SQLite");

    const allBarriers = store.db.prepare("SELECT delivery_mode FROM park_barriers").all() as Array<{ delivery_mode: string }>;
    for (const b of allBarriers) {
      assert.ok(["cli_resume", "none"].includes(b.delivery_mode), "Delivery mode must only be cli_resume or none, got: " + b.delivery_mode);
    }
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 7. Contract: parked jobs are not consumed until follow
// ---------------------------------------------------------------------------
test("Contract 7: parked jobs are not consumed until follow", async () => {
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

  const httpServer = new BridgeHttpServer(config, service);
  await httpServer.start();

  const httpClient = new BridgeHttpClient(config);
  const mcpServer = createMcpServer(httpClient, {
    env: { CODEX_THREAD_ID: "77777777-7777-7777-7777-777777777777" },
  });
  const mcpClient = new Client({ name: "test-client-7", version: "1.0.0" }, { capabilities: {} });
  const [cTrans, sTrans] = InMemoryTransport.createLinkedPair();
  await mcpServer.connect(sTrans);
  await mcpClient.connect(cTrans);

  try {
    const agent = store.createAgent({
      id: "agent_v3_c7",
      title: "Title",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_c7",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_v3_c7", agentId: agent.id, kind: "spawn", requestId: "r_c7", promptHash: "h_c7" });
    store.bindJob({ jobId: job.id, threadId: "77777777-7777-7777-7777-777777777777", originatingTurnId: "turn_c7", originatingItemId: "item_c7" });

    // 7A. Job is completed with a result file
    const resPath = path.join(tmp, "res_c7.json");
    await writeFile(resPath, JSON.stringify({ envelope: { summary: "terminal output", status: "completed" } }));
    makeCompletedJob(store, job.id, resPath, "terminal output");

    let jobRecord = store.getJob(job.id)!;
    assert.equal(jobRecord.status, "completed");
    assert.equal(jobRecord.resultConsumedAt, null, "Result must not be consumed initially");

    // 7B. Park via service: job is NOT consumed
    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.obligationState, "pending");

    jobRecord = store.getJob(job.id)!;
    assert.equal(jobRecord.resultConsumedAt, null, "Result must not be consumed by park");

    // 7C. External wake evaluated: job is NOT consumed by wake either
    await service.evaluateParkWakes(job.id);
    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.equal(outbox.status, "delivered");

    jobRecord = store.getJob(job.id)!;
    assert.equal(jobRecord.resultConsumedAt, null, "Result must not be consumed by wake delivery");

    // 7D. MCP subagents_park: obligationState is pending, job remains unconsumed
    const mcpPark = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [job.id] },
    });
    const mcpParkData = mcpPark.structuredContent as Record<string, unknown>;
    assert.equal(mcpParkData.obligationState, "pending");
    assert.equal(store.getJob(job.id)!.resultConsumedAt, null, "Result remains unconsumed after MCP park");

    // 7E. Follow via service: explicitly consumes the result and closes obligation
    const followResult = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followResult.resultAvailable, true);
    assert.equal(followResult.status, "completed");

    jobRecord = store.getJob(job.id)!;
    assert.ok(jobRecord.resultConsumedAt !== null, "Result must be consumed after follow");

    // 7F. MCP subagents_follow on another job closes obligation
    const job2 = store.createJob({ id: "job_v3_c7_2", agentId: agent.id, kind: "spawn", requestId: "r_c7_2", promptHash: "h_c7_2" });
    store.bindJob({ jobId: job2.id, threadId: "77777777-7777-7777-7777-777777777777", originatingTurnId: "turn_c7", originatingItemId: "item_c7_2" });
    const resPath2 = path.join(tmp, "res_c7_2.json");
    await writeFile(resPath2, JSON.stringify({ envelope: { summary: "terminal 2", status: "completed" } }));
    makeCompletedJob(store, job2.id, resPath2, "terminal 2");

    assert.equal(store.getJob(job2.id)!.resultConsumedAt, null);
    const mcpFollow = await mcpClient.callTool({
      name: "subagents_follow",
      arguments: { agent_id: agent.id, job_id: job2.id },
    });
    const mcpFollowData = mcpFollow.structuredContent as Record<string, unknown>;
    assert.equal(mcpFollowData.obligationState, "closed");
    assert.ok(store.getJob(job2.id)!.resultConsumedAt !== null, "Result consumed after MCP follow");
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
// 8. Contract: v3 predicate fields and aliases forward without loss over HTTP & MCP; output schema exposes cli_resume|none only
// ---------------------------------------------------------------------------
test("Contract 8: v3 predicate fields and aliases forward without loss over HTTP & MCP; output schema exposes cli_resume|none only", async () => {
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

  const httpServer = new BridgeHttpServer(config, service);
  await httpServer.start();

  const httpClient = new BridgeHttpClient(config);
  const mcpServer = createMcpServer(httpClient, {
    env: { CODEX_THREAD_ID: "88888888-8888-8888-8888-888888888888" },
  });
  const mcpClient = new Client({ name: "test-client-8", version: "1.0.0" }, { capabilities: {} });
  const [cTrans, sTrans] = InMemoryTransport.createLinkedPair();
  await mcpServer.connect(sTrans);
  await mcpClient.connect(cTrans);

  try {
    const agent = store.createAgent({
      id: "agent_v3_c8",
      title: "Title",
      topic: "Topic",
      repositoryRoot: tmp,
      workspacePath: tmp,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_c8",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });
    const j1 = store.createJob({ id: "job_v3_c8_1", agentId: agent.id, kind: "spawn", requestId: "r_c8_1", promptHash: "h_c8_1" });
    const j2 = store.createJob({ id: "job_v3_c8_2", agentId: agent.id, kind: "spawn", requestId: "r_c8_2", promptHash: "h_c8_2" });
    const j3 = store.createJob({ id: "job_v3_c8_3", agentId: agent.id, kind: "spawn", requestId: "r_c8_3", promptHash: "h_c8_3" });
    const jUnbound = store.createJob({ id: "job_v3_c8_u", agentId: agent.id, kind: "spawn", requestId: "r_c8_u", promptHash: "h_c8_u" });
    store.bindJob({ jobId: j1.id, threadId: "88888888-8888-8888-8888-888888888888", originatingTurnId: "turn_c8", originatingItemId: "item_c8_1" });
    store.bindJob({ jobId: j2.id, threadId: "88888888-8888-8888-8888-888888888888", originatingTurnId: "turn_c8", originatingItemId: "item_c8_2" });
    store.bindJob({ jobId: j3.id, threadId: "88888888-8888-8888-8888-888888888888", originatingTurnId: "turn_c8", originatingItemId: "item_c8_3" });

    // 8A. HTTP POST /v1/jobs/park parses and forwards snake_case predicate fields without loss
    const httpQuorum = await httpClient.call<Record<string, unknown>>("/v1/jobs/park", {
      job_ids: [j1.id, j2.id, j3.id],
      predicate: "QUORUM",
      quorum_count: 2,
      wake_on_exception: false,
    });
    assert.equal(httpQuorum.predicateType, "QUORUM");
    assert.equal(httpQuorum.quorumCount, 2);
    assert.equal(httpQuorum.armed, true);
    assert.equal(httpQuorum.deliveryMode, "cli_resume");

    const barrierQuorum = store.getParkBarrier(httpQuorum.parkId as string)!;
    assert.ok(barrierQuorum);
    assert.equal(barrierQuorum.predicateType, "QUORUM");
    assert.equal(barrierQuorum.quorumCount, 2);
    assert.equal(barrierQuorum.wakeOnException, false);

    // 8B. HTTP POST /v1/jobs/park parses and forwards camelCase aliases without loss
    const httpRequired = await httpClient.call<Record<string, unknown>>("/v1/jobs/park", {
      jobIds: [j1.id, j2.id],
      predicateType: "REQUIRED",
      requiredJobIds: [j1.id],
      wakeOnException: true,
    });
    assert.equal(httpRequired.predicateType, "REQUIRED");
    assert.deepEqual(httpRequired.requiredJobIds, [j1.id]);
    assert.equal(httpRequired.deliveryMode, "cli_resume");

    const barrierRequired = store.getParkBarrier(httpRequired.parkId as string)!;
    assert.ok(barrierRequired);
    assert.equal(barrierRequired.predicateType, "REQUIRED");
    assert.deepEqual(barrierRequired.requiredJobIds, [j1.id]);
    assert.equal(barrierRequired.wakeOnException, true);

    // 8C. HTTP POST /v1/jobs/park parses predicate_type alias
    const httpAny = await httpClient.call<Record<string, unknown>>("/v1/jobs/park", {
      job_ids: [j1.id, j2.id],
      predicate_type: "ANY",
    });
    assert.equal(httpAny.predicateType, "ANY");

    // 8D. HTTP POST invalid requests fail closed with 400
    await assert.rejects(
      () => httpClient.call("/v1/jobs/park", { job_ids: [j1.id], predicate: "BOGUS" }),
      (err: unknown) => err instanceof BridgeHttpError && err.status === 400,
    );
    await assert.rejects(
      () => httpClient.call("/v1/jobs/park", { job_ids: [j1.id], predicate: "QUORUM", quorum_count: 0 }),
      (err: unknown) => err instanceof BridgeHttpError && err.status === 400,
    );
    await assert.rejects(
      () => httpClient.call("/v1/jobs/park", { job_ids: [j1.id], wake_on_exception: "not_a_bool" }),
      (err: unknown) => err instanceof BridgeHttpError && err.status === 400,
    );

    // 8E. MCP subagents_park forwards predicates over HTTP without loss
    const mcpQuorumRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: {
        job_ids: [j1.id, j2.id, j3.id],
        predicate: "QUORUM",
        quorum_count: 2,
        wake_on_exception: false,
      },
    });
    assert.equal(mcpQuorumRes.isError, undefined);
    const mcpQuorumData = mcpQuorumRes.structuredContent as Record<string, unknown>;
    assert.equal(mcpQuorumData.predicateType, "QUORUM");
    assert.equal(mcpQuorumData.quorumCount, 2);
    assert.equal(mcpQuorumData.deliveryMode, "cli_resume");
    assert.equal(mcpQuorumData.obligationState, "pending");
    assert.equal(mcpQuorumData.nextRequiredAction, "subagents_follow");

    // 8F. MCP subagents_park maps camelCase aliases deterministically
    const mcpReqRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: {
        jobIds: [j1.id, j2.id],
        predicateType: "REQUIRED",
        requiredJobIds: [j1.id],
        wakeOnException: true,
      },
    });
    assert.equal(mcpReqRes.isError, undefined);
    const mcpReqData = mcpReqRes.structuredContent as Record<string, unknown>;
    assert.equal(mcpReqData.predicateType, "REQUIRED");
    assert.deepEqual(mcpReqData.requiredJobIds, [j1.id]);
    assert.equal(mcpReqData.deliveryMode, "cli_resume");

    // 8G. MCP deepseek_park alias tool maps aliases deterministically
    const dsQuorumRes = await mcpClient.callTool({
      name: "deepseek_park",
      arguments: {
        jobIds: [j1.id, j2.id, j3.id],
        predicateType: "QUORUM",
        quorumCount: 2,
      },
    });
    assert.equal(dsQuorumRes.isError, undefined);
    const dsQuorumData = dsQuorumRes.structuredContent as Record<string, unknown>;
    assert.equal(dsQuorumData.predicateType, "QUORUM");
    assert.equal(dsQuorumData.quorumCount, 2);
    assert.equal(dsQuorumData.deliveryMode, "cli_resume");
    assert.equal(dsQuorumData.nextRequiredAction, "deepseek_follow");

    // 8H. MCP park on unbound job returns deliveryMode="none" (never in_turn)
    const mcpUnboundRes = await mcpClient.callTool({
      name: "subagents_park",
      arguments: { job_ids: [jUnbound.id] },
    });
    const mcpUnboundData = mcpUnboundRes.structuredContent as Record<string, unknown>;
    assert.equal(mcpUnboundData.armed, false);
    assert.equal(mcpUnboundData.deliveryMode, "none");

    // 8I. Output schema exposes cli_resume|none only (no in_turn)
    const tools = await mcpClient.listTools();
    const subTool = tools.tools.find((t) => t.name === "subagents_park")!;
    const dsTool = tools.tools.find((t) => t.name === "deepseek_park")!;
    assert.ok(subTool, "subagents_park must be registered");
    assert.ok(dsTool, "deepseek_park must be registered");

    const subDeliveryModeStr = JSON.stringify((subTool.outputSchema as any)?.properties?.deliveryMode ?? {});
    assert.match(subDeliveryModeStr, /cli_resume/);
    assert.match(subDeliveryModeStr, /none/);
    assert.doesNotMatch(subDeliveryModeStr, /in_turn/);

    const dsDeliveryModeStr = JSON.stringify((dsTool.outputSchema as any)?.properties?.deliveryMode ?? {});
    assert.match(dsDeliveryModeStr, /cli_resume/);
    assert.match(dsDeliveryModeStr, /none/);
    assert.doesNotMatch(dsDeliveryModeStr, /in_turn/);
  } finally {
    await mcpClient.close();
    await mcpServer.close();
    await httpServer.stop();
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});
