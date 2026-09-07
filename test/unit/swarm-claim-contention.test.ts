import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";
import { BridgeService, type ManagedOpenCodeLike, type OpenCodeManagerLike } from "../../src/service.js";
import { createDefaultConfig } from "../../src/config.js";
import { hashPrompt } from "../../src/security.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

class FakeOpenCodeClient implements OpenCodeClientLike {
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  messages: OpenCodeMessage[] = [];
  activeSessions = new Set<string>();
  private onEvent?: (event: OpenCodeEvent) => Promise<void> | void;

  async health() {
    return { healthy: true, version: "fake" };
  }
  async createSession() {
    const id = "session_fake_" + Math.random().toString(36).slice(2);
    this.activeSessions.add(id);
    return { id };
  }
  async promptAsync(sessionId: string, task: string) {
    this.promptCalls.push({ sessionId, task });
  }
  async listMessages() {
    return this.messages;
  }
  async getDiff() {
    return "";
  }
  async abort(sessionId: string) {
    this.activeSessions.delete(sessionId);
  }
  async replyPermission() {}
  async subscribe(onEvent: (event: OpenCodeEvent) => Promise<void> | void) {
    this.onEvent = onEvent;
  }
  async emit(event: OpenCodeEvent) {
    await this.onEvent?.(event);
  }
}

function makeFakeManager(client: FakeOpenCodeClient): OpenCodeManagerLike {
  return {
    async start(): Promise<ManagedOpenCodeLike> {
      return {
        serverId: "fake_server",
        baseUrl: "http://127.0.0.1:9999",
        client,
        processId: 1234,
        async stop() {},
      };
    },
    async stop() {},
  };
}

const fakeAntigravity = {
  async runPrompt() {
    return new Promise(() => {});
  },
};

function initServiceInstance(
  service: BridgeService,
  client?: FakeOpenCodeClient,
  serverId = "fake_server",
) {
  (service as any).lifecycleState = "ready";
  (service as any).running = true;
}

// ---------------------------------------------------------------------------
// 1. Dispatch Envelope Retention under Claim Contention (Deterministic RED)
// ---------------------------------------------------------------------------
test("claim contention: second failed claimant must not delete valid dispatch envelope when job already claimed into dispatching", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "swarm-claim-env-"));
  const dbPath = path.join(tmpDir, "test.sqlite");
  const store1 = new BridgeStore(dbPath);
  const store2 = new BridgeStore(dbPath);
  const client1 = new FakeOpenCodeClient();
  const client2 = new FakeOpenCodeClient();
  const config1 = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 2 });
  const config2 = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 2 });
  const service1 = new BridgeService(config1, { store: store1, antigravity: fakeAntigravity as any });
  const service2 = new BridgeService(config2, { store: store2, antigravity: fakeAntigravity as any });

  initServiceInstance(service1, client1, "server_1");
  initServiceInstance(service2, client2, "server_2");

  try {
    const promptText = "Task for envelope contention test";
    const promptHash = hashPrompt(promptText);
    const jobId = "job_env_contention_1";
    const agentId = "agent_env_contention_1";

    const { job } = store1.admitUnary({
      agent: {
        id: agentId,
        title: "Contention Agent",
        topic: "Claim contention envelope",
        repositoryRoot: tmpDir,
        workspacePath: tmpDir,
        workspaceStrategy: "shared",
        mode: "subagent",
        opencodeServerId: "antigravity",
        opencodeSessionId: "antigravity:" + agentId,
        modelProviderId: "antigravity",
        modelId: "gemini-3.8-flash-high",
        modelVariant: null,
        modelRoute: "antigravity-flash-high",
      },
      job: {
        id: jobId,
        agentId,
        kind: "spawn",
        status: "queued",
        requestId: "req_env_1",
        promptHash,
        priority: 50,
      },
      dispatchEnvelope: {
        prompt: promptText,
        promptHash,
        workerInput: { task: promptText },
        contextFiles: [],
      },
    });

    assert.ok(store1.getDispatchEnvelope(jobId), "Initial envelope must exist in store1");
    assert.ok(store2.getDispatchEnvelope(jobId), "Initial envelope must exist in store2");

    // Dispatcher 1 atomically claims the queued job into dispatching
    const claimedJob = store1.claimQueuedJobForDispatch(jobId, job.fence);
    assert.ok(claimedJob, "Dispatcher 1 must successfully claim queued job");
    assert.equal(claimedJob.status, "dispatching");

    // Dispatch envelope must still exist after the winner's atomic claim
    assert.ok(store1.getDispatchEnvelope(jobId), "Envelope must remain after winner's claim");

    // Dispatcher 2 (second claimant) receives the job snapshot and attempts to dispatch it
    await (service2 as any).dispatchQueuedJob(job);

    // RED assertion: The second failed claimant must NOT delete the valid dispatch envelope
    const envelopeAfter = store1.getDispatchEnvelope(jobId);
    assert.ok(
      envelopeAfter,
      "A second failed claimant must not delete the valid dispatch envelope when job was already claimed into dispatching",
    );
    assert.equal(envelopeAfter.promptHash, promptHash);
  } finally {
    store1.close();
    store2.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Settlement / Release Suppression under Claim Contention (Deterministic RED)
// ---------------------------------------------------------------------------
test("claim contention: second failed claimant must not call settlement or release for the winner", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "swarm-claim-settle-"));
  const dbPath = path.join(tmpDir, "test.sqlite");
  const store1 = new BridgeStore(dbPath);
  const store2 = new BridgeStore(dbPath);
  const client1 = new FakeOpenCodeClient();
  const client2 = new FakeOpenCodeClient();
  const config1 = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 2 });
  const config2 = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 2 });
  const service1 = new BridgeService(config1, { store: store1, antigravity: fakeAntigravity as any });
  const service2 = new BridgeService(config2, { store: store2, antigravity: fakeAntigravity as any });

  initServiceInstance(service1, client1, "server_1");
  initServiceInstance(service2, client2, "server_2");

  try {
    const promptText = "Task for settlement contention test";
    const promptHash = hashPrompt(promptText);
    const jobId = "job_settle_contention_1";
    const agentId = "agent_settle_contention_1";

    const { job } = store1.admitUnary({
      agent: {
        id: agentId,
        title: "Contention Agent",
        topic: "Claim contention settlement",
        repositoryRoot: tmpDir,
        workspacePath: tmpDir,
        workspaceStrategy: "shared",
        mode: "subagent",
        opencodeServerId: "antigravity",
        opencodeSessionId: "antigravity:" + agentId,
        modelProviderId: "antigravity",
        modelId: "gemini-3.8-flash-high",
        modelVariant: null,
        modelRoute: "antigravity-flash-high",
      },
      job: {
        id: jobId,
        agentId,
        kind: "spawn",
        status: "queued",
        requestId: "req_settle_1",
        promptHash,
        priority: 50,
        exclusiveResources: ["res_contention_settle"],
      },
      dispatchEnvelope: {
        prompt: promptText,
        promptHash,
        workerInput: { task: promptText },
        contextFiles: [],
      },
    });

    // Dispatcher 1 claims the job into dispatching and tracks the resource
    const claimed = store1.claimQueuedJobForDispatch(jobId, job.fence);
    assert.ok(claimed);
    (service1 as any).activeExclusiveResources.set("res_contention_settle", jobId);

    // Track onJobSettled calls on service2
    let service2SettledCalls = 0;
    const origOnJobSettled2 = (service2 as any).onJobSettled.bind(service2);
    (service2 as any).onJobSettled = (id: string, batchId?: string | null) => {
      service2SettledCalls++;
      return origOnJobSettled2(id, batchId);
    };

    // Dispatcher 2 attempts to dispatch the already-claimed job
    await (service2 as any).dispatchQueuedJob(job);

    // RED assertion: Failed claimant must not invoke settlement/release for winner's job
    assert.equal(
      service2SettledCalls,
      0,
      "Second failed claimant must not call settlement/release for the winner",
    );
  } finally {
    store1.close();
    store2.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Status Integrity under Contention (Deterministic RED)
// ---------------------------------------------------------------------------
test("claim contention: second failed claimant must not corrupt status or abort winner dispatch", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "swarm-claim-status-"));
  const dbPath = path.join(tmpDir, "test.sqlite");
  const store1 = new BridgeStore(dbPath);
  const store2 = new BridgeStore(dbPath);
  const client1 = new FakeOpenCodeClient();
  const client2 = new FakeOpenCodeClient();
  const config1 = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 2 });
  const config2 = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 2 });
  const service1 = new BridgeService(config1, { store: store1, antigravity: fakeAntigravity as any });
  const service2 = new BridgeService(config2, { store: store2, antigravity: fakeAntigravity as any });

  initServiceInstance(service1, client1, "server_1");
  initServiceInstance(service2, client2, "server_2");

  try {
    const promptText = "Task for status contention test";
    const promptHash = hashPrompt(promptText);
    const jobId = "job_status_contention_1";
    const agentId = "agent_status_contention_1";

    const { job } = store1.admitUnary({
      agent: {
        id: agentId,
        title: "Contention Agent",
        topic: "Claim contention status",
        repositoryRoot: tmpDir,
        workspacePath: tmpDir,
        workspaceStrategy: "shared",
        mode: "subagent",
        opencodeServerId: "antigravity",
        opencodeSessionId: "antigravity:" + agentId,
        modelProviderId: "antigravity",
        modelId: "gemini-3.8-flash-high",
        modelVariant: null,
        modelRoute: "antigravity-flash-high",
      },
      job: {
        id: jobId,
        agentId,
        kind: "spawn",
        status: "queued",
        requestId: "req_status_1",
        promptHash,
        priority: 50,
      },
      dispatchEnvelope: {
        prompt: promptText,
        promptHash,
        workerInput: { task: promptText },
        contextFiles: [],
      },
    });

    // Intercept store1.claimQueuedJobForDispatch to inject service2 contention
    // right after service1 successfully wins the atomic claim into dispatching
    const origClaim1 = store1.claimQueuedJobForDispatch.bind(store1);
    store1.claimQueuedJobForDispatch = (id: string, expectedFence?: number | null) => {
      const res = origClaim1(id, expectedFence);
      if (res && id === jobId) {
        // Dispatcher 1 has atomically claimed the job into 'dispatching'.
        // Dispatcher 2 now contends and fails to claim the same queued job.
        void (service2 as any).dispatchQueuedJob(job);
      }
      return res;
    };

    // Service 1 dispatches the job
    await (service1 as any).dispatchQueuedJob(job);

    // Verify job and agent status in the store
    const jobAfter = store1.getJob(jobId);
    const agentAfter = store1.getAgent(agentId);

    // RED assertion: Status must not be corrupted to failed due to missing envelope deleted by claimant 2
    assert.notEqual(
      jobAfter?.status,
      "failed",
      "Job status must not be corrupted to 'failed' because second claimant deleted the envelope",
    );
    assert.equal(
      jobAfter?.status,
      "running",
      "Winner must successfully transition job to 'running'",
    );
    assert.equal(
      agentAfter?.status,
      "working",
      "Agent status must be 'working'",
    );
  } finally {
    store1.close();
    store2.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});
