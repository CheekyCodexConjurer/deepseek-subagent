import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService, type ManagedOpenCodeLike } from "../../src/service.js";
import { OpenCodeHttpError, OpenCodeTransportError } from "../../src/opencode/client.js";
import type {
  JobStatus,
  OpenCodeClientLike,
  OpenCodeEvent,
  OpenCodeMessage,
} from "../../src/types.js";

class FakeOpenCodeClient implements OpenCodeClientLike {
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  messages: OpenCodeMessage[] = [];
  listMessagesError: Error | null = null;
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
  async listMessages(_sessionId: string): Promise<OpenCodeMessage[]> {
    if (this.listMessagesError) {
      throw this.listMessagesError;
    }
    return this.messages;
  }
  async getDiff(_sessionId: string): Promise<string> {
    return "";
  }
  async abort(sessionId: string): Promise<void> {
    this.activeSessions.delete(sessionId);
  }
  async replyPermission(): Promise<void> {}
  async subscribe(onEvent: (event: OpenCodeEvent) => Promise<void> | void): Promise<void> {
    this.onEvent = onEvent;
  }
  async emit(event: OpenCodeEvent): Promise<void> {
    await this.onEvent?.(event);
  }
}

function makeFakeManager(client: FakeOpenCodeClient) {
  return {
    async start(): Promise<ManagedOpenCodeLike> {
      return {
        serverId: "server_fake",
        baseUrl: "http://127.0.0.1:9999",
        client,
        processId: 1234,
        async stop() {},
      };
    },
    async stop() {},
  };
}

function seedAgent(store: BridgeStore, agentId: string, repoRoot: string) {
  return store.createAgent({
    id: agentId,
    title: `Agent ${agentId}`,
    topic: "Restart reconciliation test topic",
    repositoryRoot: repoRoot,
    workspacePath: repoRoot,
    workspaceStrategy: "shared",
    opencodeServerId: "server_test",
    opencodeSessionId: `session_${agentId}`,
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
  });
}

function seedJob(
  store: BridgeStore,
  agentId: string,
  jobId: string,
  status: JobStatus,
  exclusiveResources?: string[] | null,
  requestId = `req_${jobId}`,
) {
  return store.createJob({
    id: jobId,
    agentId,
    kind: "spawn",
    requestId,
    promptHash: `hash_${jobId}`,
    status,
    exclusiveResources: exclusiveResources ?? null,
  });
}

// ---------------------------------------------------------------------------
// 1. recoverPendingJobs + OpenCodeTransportError / unknown outcome
// ---------------------------------------------------------------------------
test("1. recoverPendingJobs + OpenCodeTransportError: preserves authentic active state, marks dispatchUnknown=true, no result/delivery/settlement/wake, no timers, holds credit and exclusive resources", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-restart-rec-transport-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeOpenCodeClient();
  client.listMessagesError = new OpenCodeTransportError(
    "GET",
    "http://127.0.0.1:9999/session/session_agent_t1/message",
    "connection refused",
  );

  const agent = seedAgent(store, "agent_t1", directory);
  store.updateAgentStatus(agent.id, "working");
  const job = seedJob(store, "agent_t1", "job_t1", "running", ["res-lock-t1"]);

  const barrier = store.createOrUpdateParkBarrier({
    id: "park_t1",
    threadId: "thread_t1",
    turnId: "turn_t1",
    armed: true,
    state: "armed",
    predicateType: "ALL",
  });
  store.setParkJobs(barrier.id, [job.id]);

  const config = createDefaultConfig({ dataDir: directory });
  const service = new BridgeService(config, {
    store,
    manager: makeFakeManager(client),
  });

  try {
    await service.start();

    const currentJob = store.getJob(job.id)!;
    assert.ok(currentJob, "job must exist in store");
    assert.equal(currentJob.status, "running", "job must remain in authentic running active state");
    assert.equal(currentJob.dispatchUnknown, true, "job must be marked dispatchUnknown=true");

    assert.equal(currentJob.resultPath, null, "job must not have a result path");
    assert.equal(currentJob.resultSummary, null, "job must not have a result summary");
    assert.notEqual(currentJob.status, "delivery_pending", "job must not be delivery_pending");
    assert.notEqual(currentJob.status, "delivered", "job must not be delivered");
    assert.equal(store.getDeliveryByJob(job.id), null, "no delivery records must be created");

    const currentBarrier = store.getParkBarrier(barrier.id);
    assert.equal(currentBarrier?.armed, true, "park barrier must remain armed");
    assert.equal(currentBarrier?.state, "armed", "park barrier must remain in armed state");
    assert.equal(store.listPendingWakeOutbox().length, 0, "no park wake outbox must be generated");

    assert.equal((service as any).followLifecycles.has(job.id), false, "no follow lifecycle timer must be restored");
    assert.equal(currentJob.followDeadlineAt, null, "followDeadlineAt must remain null");
    assert.equal(currentJob.graceDeadlineAt, null, "graceDeadlineAt must remain null");

    assert.equal(store.getActiveJobCount(), 1, "active scheduler credit must remain claimed");
    const claimed = (service as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(claimed.has("res-lock-t1"), "exclusive resource must remain claimed");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. OpenCodeHttpError(404): authoritative missing session
// ---------------------------------------------------------------------------
test("2. recoverPendingJobs + OpenCodeHttpError(404): authoritative missing session fails job/agent, settles/releases scheduler resource, evaluates matching park wake exactly once", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-restart-rec-404-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeOpenCodeClient();
  client.listMessagesError = new OpenCodeHttpError(
    404,
    "GET",
    "http://127.0.0.1:9999/session/session_agent_t2/message",
    JSON.stringify({ error: "Session not found" }),
  );

  const agent = seedAgent(store, "agent_t2", directory);
  store.updateAgentStatus(agent.id, "working");
  const job = seedJob(store, "agent_t2", "job_t2", "running", ["res-lock-t2"]);

  const barrier = store.createOrUpdateParkBarrier({
    id: "park_t2",
    threadId: "thread_t2",
    turnId: "turn_t2",
    armed: true,
    state: "armed",
    predicateType: "ALL",
  });
  store.setParkJobs(barrier.id, [job.id]);

  const config = createDefaultConfig({ dataDir: directory });
  const service = new BridgeService(config, {
    store,
    manager: makeFakeManager(client),
  });

  try {
    await service.start();

    const currentJob = store.getJob(job.id)!;
    assert.ok(currentJob, "job must exist in store");
    assert.equal(currentJob.status, "failed", "authoritative 404 must transition job to failed");

    const currentAgent = store.getAgent(agent.id)!;
    assert.ok(currentAgent, "agent must exist in store");
    assert.equal(currentAgent.status, "failed", "authoritative 404 must transition agent to failed");

    const claimed = (service as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(!claimed.has("res-lock-t2"), "exclusive resource must be released upon failure");
    assert.equal(store.getActiveJobCount(), 0, "active scheduler credit must be released upon settlement");

    const currentBarrier = store.getParkBarrier(barrier.id);
    assert.ok(
      currentBarrier?.state === "waking" || currentBarrier?.state === "woken",
      `barrier must transition to waking or woken, got: ${currentBarrier?.state}`,
    );
    assert.equal(currentBarrier?.armed, false, "barrier must no longer be armed");

    const wakes = store.listPendingWakeOutbox().filter((w) => w.parkId === barrier.id);
    assert.equal(wakes.length, 1, "matching park wake outbox must be created exactly once");

    // Idempotence: subsequent recovery must not re-evaluate or duplicate the wake
    await (service as any).recoverPendingJobs();
    const wakesAfter = store.listPendingWakeOutbox().filter((w) => w.parkId === barrier.id);
    assert.equal(wakesAfter.length, 1, "repeated recovery must not duplicate the park wake");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Late progression: unfinished assistant message -> terminal finish='stop'
// ---------------------------------------------------------------------------
test("3. late progression (unfinished assistant message): first recovery keeps job active, later terminal assistant info.finish='stop' reconciles once, delivers once, and settles claim", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-restart-rec-unfinished-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeOpenCodeClient();

  // First recovery: assistant message is streaming / unfinished (info.finish is absent)
  client.messages = [
    {
      info: { id: "msg_user_t3", role: "user" },
      parts: [{ type: "text", text: "Run computation" }],
    },
    {
      info: { id: "msg_asst_t3", role: "assistant" },
      parts: [{ type: "text", text: "Computing in progress..." }],
    },
  ];

  const agent = seedAgent(store, "agent_t3", directory);
  store.updateAgentStatus(agent.id, "working");
  const job = seedJob(store, "agent_t3", "job_t3", "running", ["res-lock-t3"]);

  const config = createDefaultConfig({ dataDir: directory });
  const service = new BridgeService(config, {
    store,
    manager: makeFakeManager(client),
  });

  try {
    // First recovery (restart)
    await service.start();

    // First recovery assertions: job must remain active
    const jobAfterFirst = store.getJob(job.id)!;
    assert.ok(jobAfterFirst, "job must exist in store");
    assert.equal(jobAfterFirst.status, "running", "job must remain running while assistant message is unfinished");
    assert.equal(jobAfterFirst.resultPath, null, "job must not have a resultPath while unfinished");
    assert.equal(store.getDeliveryByJob(job.id), null, "no delivery records must exist yet");
    const claimedFirst = (service as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(claimedFirst.has("res-lock-t3"), "exclusive resource must remain claimed");
    assert.equal(store.getActiveJobCount(), 1, "active scheduler credit must remain claimed");

    // Progression: assistant completes with terminal info.finish = 'stop'
    client.messages = [
      {
        info: { id: "msg_user_t3", role: "user" },
        parts: [{ type: "text", text: "Run computation" }],
      },
      {
        info: { id: "msg_asst_t3", role: "assistant", finish: "stop" },
        parts: [{ type: "text", text: "Computation completed successfully." }],
      },
    ];

    // Second recovery
    await (service as any).recoverPendingJobs();

    // Later recovery assertions: reconciles once, persists/delivers once, clears scheduler claim
    const jobAfterSecond = store.getJob(job.id)!;
    assert.ok(
      ["completed", "delivery_pending", "delivered"].includes(jobAfterSecond.status),
      `job status must transition to terminal/delivered status, got: ${jobAfterSecond.status}`,
    );
    assert.ok(jobAfterSecond.resultPath !== null, "job must have a persisted resultPath");

    const delivery = store.getDeliveryByJob(job.id);
    assert.ok(delivery, "must create delivery record");
    assert.equal(delivery.status, "delivered", "delivery must be marked delivered");
    const deliveryId = delivery.id;
    const deliveryAttempts = delivery.attempts;

    const claimedSecond = (service as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(!claimedSecond.has("res-lock-t3"), "exclusive resource claim must be cleared through settlement");
    assert.equal(store.getActiveJobCount(), 0, "active scheduler credit must be cleared through settlement");

    // Idempotence: subsequent recovery does not re-reconcile or duplicate delivery
    await (service as any).recoverPendingJobs();
    const deliveryAfterRerun = store.getDeliveryByJob(job.id);
    assert.ok(deliveryAfterRerun, "delivery record must still exist");
    assert.equal(deliveryAfterRerun.id, deliveryId, "delivery id must remain unchanged");
    assert.equal(deliveryAfterRerun.attempts, deliveryAttempts, "delivery attempts must remain unchanged");
    assert.equal(deliveryAfterRerun.status, "delivered", "delivery status must remain delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Late progression: transport unknown -> terminal finish='stop'
// ---------------------------------------------------------------------------
test("4. late progression (transport unknown): first recovery keeps job active/unknown, later terminal assistant info.finish='stop' reconciles once, delivers once, and settles claim", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-restart-rec-transport-progression-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeOpenCodeClient();

  // First recovery: transport unknown
  client.listMessagesError = new OpenCodeTransportError(
    "GET",
    "http://127.0.0.1:9999/session/session_agent_t4/message",
    "network timeout",
  );

  const agent = seedAgent(store, "agent_t4", directory);
  store.updateAgentStatus(agent.id, "working");
  const job = seedJob(store, "agent_t4", "job_t4", "running", ["res-lock-t4"]);

  const config = createDefaultConfig({ dataDir: directory });
  const service = new BridgeService(config, {
    store,
    manager: makeFakeManager(client),
  });

  try {
    // First recovery (restart)
    await service.start();

    // First recovery assertions: job must remain active/unknown
    const jobAfterFirst = store.getJob(job.id)!;
    assert.ok(jobAfterFirst, "job must exist in store");
    assert.equal(jobAfterFirst.status, "running", "job must remain running after transport failure during recovery");
    assert.equal(jobAfterFirst.dispatchUnknown, true, "job must be marked dispatchUnknown=true");
    assert.equal(jobAfterFirst.resultPath, null, "job must not have a resultPath");
    assert.equal(store.getDeliveryByJob(job.id), null, "no delivery records must exist yet");
    const claimedFirst = (service as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(claimedFirst.has("res-lock-t4"), "exclusive resource must remain claimed");
    assert.equal(store.getActiveJobCount(), 1, "active scheduler credit must remain claimed");

    // Progression: network recovers and terminal assistant message arrives
    client.listMessagesError = null;
    client.messages = [
      {
        info: { id: "msg_user_t4", role: "user" },
        parts: [{ type: "text", text: "Run migration" }],
      },
      {
        info: { id: "msg_asst_t4", role: "assistant", finish: "stop" },
        parts: [{ type: "text", text: "Migration completed successfully." }],
      },
    ];

    // Second recovery
    await (service as any).recoverPendingJobs();

    // Later recovery assertions: reconciles once, persists/delivers once, clears scheduler claim
    const jobAfterSecond = store.getJob(job.id)!;
    assert.ok(
      ["completed", "delivery_pending", "delivered"].includes(jobAfterSecond.status),
      `job status must transition to terminal/delivered status, got: ${jobAfterSecond.status}`,
    );
    assert.ok(jobAfterSecond.resultPath !== null, "job must have a persisted resultPath");

    const delivery = store.getDeliveryByJob(job.id);
    assert.ok(delivery, "must create delivery record");
    assert.equal(delivery.status, "delivered", "delivery must be marked delivered");
    const deliveryId = delivery.id;
    const deliveryAttempts = delivery.attempts;

    const claimedSecond = (service as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(!claimedSecond.has("res-lock-t4"), "exclusive resource claim must be cleared through settlement");
    assert.equal(store.getActiveJobCount(), 0, "active scheduler credit must be cleared through settlement");

    // Idempotence: subsequent recovery does not re-reconcile or duplicate delivery
    await (service as any).recoverPendingJobs();
    const deliveryAfterRerun = store.getDeliveryByJob(job.id);
    assert.ok(deliveryAfterRerun, "delivery record must still exist");
    assert.equal(deliveryAfterRerun.id, deliveryId, "delivery id must remain unchanged");
    assert.equal(deliveryAfterRerun.attempts, deliveryAttempts, "delivery attempts must remain unchanged");
    assert.equal(deliveryAfterRerun.status, "delivered", "delivery status must remain delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
