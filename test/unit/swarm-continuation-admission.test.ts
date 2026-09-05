import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeService, type ManagedOpenCodeLike } from "../../src/service.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

let globalSessionCounter = 0;

class FakeOpenCodeClient implements OpenCodeClientLike {
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  messages: OpenCodeMessage[] = [];
  activeSessions = new Set<string>();
  private onEvent?: (event: OpenCodeEvent) => Promise<void> | void;

  async health() {
    return { healthy: true, version: "fake" };
  }
  async createSession() {
    globalSessionCounter += 1;
    const id = "session_" + globalSessionCounter;
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

function makeFakeManager(client: FakeOpenCodeClient) {
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

async function setupCompletedAgent(
  service: BridgeService,
  store: BridgeStore,
  client: FakeOpenCodeClient,
  tmpDir: string,
  topic: string,
  requestId: string,
) {
  client.messages = [
    {
      info: { id: "msg_" + requestId, role: "assistant" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: Initial task done" }],
    } as any,
  ];
  const spawnResult = await service.spawn({
    requestId,
    topic,
    task: "Initial task for " + topic,
    cwd: tmpDir,
  });
  const agent = store.getAgent(spawnResult.agentId)!;
  await (service as any).completeActive(agent);
  assert.equal(store.getActiveJobCount(), 0, "Active job count must be 0 after completion");
  return agent;
}

// ---------------------------------------------------------------------------
// 1. Happy Path: Queued continueJob commits job, correlation, and envelope together
// ---------------------------------------------------------------------------
test("queued continueJob commits continuation job, correlation hint, and dispatch envelope together atomically", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-cont-happy-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 1 });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const agent = await setupCompletedAgent(service, store, client, tmpDir, "Agent Continue Happy", "req_agent_happy");

      // Blocker occupies credit ceiling (1)
      await service.spawn({
        requestId: "req_blocker_happy",
        topic: "Blocker Topic",
        task: "Blocker occupying 1 credit",
        cwd: tmpDir,
      });
      assert.equal(store.getActiveJobCount(), 1, "Blocker must occupy 1 credit");

      // Admitted continueJob behind blocker remains queued
      const receipt = await service.continueJob({
        agentId: agent.id,
        requestId: "req_cont_happy_1",
        task: "Continue task happy path",
        threadId: "thread_cont_happy_1",
        turnId: "turn_cont_happy_1",
      });

      assert.equal(receipt.accepted, true);
      assert.equal(receipt.status, "accepted");
      assert.ok(receipt.jobId);
      assert.equal(receipt.agentId, agent.id);

      const job = store.getJob(receipt.jobId);
      assert.ok(job, "Continuation job must be persisted in store");
      assert.equal(job.agentId, agent.id);
      assert.equal(job.kind, "continue");
      assert.equal(job.status, "queued", "Continuation job must remain queued while awaiting credit");
      assert.equal(job.hintThreadId, "thread_cont_happy_1");
      assert.equal(job.hintTurnId, "turn_cont_happy_1");

      const envelope = store.getDispatchEnvelope(receipt.jobId);
      assert.ok(envelope, "Dispatch envelope must commit together with continuation job");
      assert.equal(envelope.promptHash, job.promptHash);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Queued continueJob Atomicity: Late constraint failure rolls back leaving no orphan state (RED)
// ---------------------------------------------------------------------------
test("late constraint failure during queued continueJob rolls back atomically leaving no orphan continuation job, correlation, or envelope", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-cont-rollback-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 1 });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const agent = await setupCompletedAgent(service, store, client, tmpDir, "Agent Continue Rollback", "req_agent_rb");

      // Blocker occupies credit ceiling
      await service.spawn({
        requestId: "req_blocker_rb",
        topic: "Blocker Topic",
        task: "Blocker occupying 1 credit",
        cwd: tmpDir,
      });

      const initialJobsCount = store.listJobs().length;
      const initialAgentJobs = store.listJobs().filter((j) => j.agentId === agent.id).length;

      // Inject late constraint failure during dispatch_envelopes insert
      store.db.exec(`
        CREATE TRIGGER fail_late_dispatch_envelope_continue
        BEFORE INSERT ON dispatch_envelopes
        BEGIN
          SELECT RAISE(ABORT, 'injected late dispatch_envelope constraint failure for continue');
        END;
      `);

      await assert.rejects(
        async () => {
          await service.continueJob({
            agentId: agent.id,
            requestId: "req_cont_fail_late_1",
            task: "Continue task late failure",
            threadId: "thread_cont_fail_1",
            turnId: "turn_cont_fail_1",
          });
        },
        /injected late dispatch_envelope constraint failure for continue/i,
        "continueJob must reject when dispatch_envelopes insertion fails",
      );

      // PROVE ATOMIC ROLLBACK:
      // If admission was atomic, rolling back must leave:
      // - NO orphan continuation job in store for requestId
      // - NO additional jobs for the agent
      // - NO orphan correlation hints
      // - NO orphan dispatch envelopes
      assert.equal(
        store.getJobByRequestId("req_cont_fail_late_1"),
        null,
        "Atomic rollback must leave no continuation job for requestId",
      );
      assert.equal(
        store.listJobs().filter((j) => j.agentId === agent.id).length,
        initialAgentJobs,
        "Agent must have no orphan continuation jobs after rollback",
      );
      assert.equal(
        store.listJobs().length,
        initialJobsCount,
        "Total jobs count must be unchanged after rollback",
      );

      const envelopeCount = store.db.prepare("SELECT COUNT(*) AS count FROM dispatch_envelopes WHERE job_id NOT IN (SELECT id FROM jobs WHERE request_id != 'req_cont_fail_late_1')").get() as { count: number };
      assert.equal(Number(envelopeCount.count), 0, "No orphan dispatch envelopes must remain");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Queued continueJob Retry: Retrying with same requestId admits complete valid state (RED)
// ---------------------------------------------------------------------------
test("retry after late failure rollback of queued continueJob admits cleanly and produces complete valid state", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-cont-retry-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 1 });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const agent = await setupCompletedAgent(service, store, client, tmpDir, "Agent Continue Retry", "req_agent_retry");

      // Blocker occupies credit ceiling
      await service.spawn({
        requestId: "req_blocker_retry",
        topic: "Blocker Topic",
        task: "Blocker occupying 1 credit",
        cwd: tmpDir,
      });

      // 1. Inject late constraint failure
      store.db.exec(`
        CREATE TRIGGER fail_transient_envelope_cont
        BEFORE INSERT ON dispatch_envelopes
        BEGIN
          SELECT RAISE(ABORT, 'transient continuation envelope persistence failure');
        END;
      `);

      // 2. First attempt fails late during continuation admission
      await assert.rejects(
        async () => {
          await service.continueJob({
            agentId: agent.id,
            requestId: "req_cont_transient_retry_1",
            task: "Transient failure continue task",
            threadId: "thread_retry_1",
            turnId: "turn_retry_1",
          });
        },
        /transient continuation envelope persistence failure/i,
      );

      // 3. Transient failure resolves
      store.db.exec("DROP TRIGGER fail_transient_envelope_cont;");

      // 4. Retry with the SAME requestId
      const retryReceipt = await service.continueJob({
        agentId: agent.id,
        requestId: "req_cont_transient_retry_1",
        task: "Transient failure continue task",
        threadId: "thread_retry_1",
        turnId: "turn_retry_1",
      });

      assert.equal(retryReceipt.accepted, true);
      assert.ok(retryReceipt.jobId);
      assert.equal(retryReceipt.agentId, agent.id);

      const job = store.getJob(retryReceipt.jobId);
      assert.ok(job, "Retried continuation job must exist");
      assert.equal(job.status, "queued");
      assert.equal(job.hintThreadId, "thread_retry_1");
      assert.equal(job.hintTurnId, "turn_retry_1");

      const envelope = store.getDispatchEnvelope(retryReceipt.jobId);
      assert.ok(envelope, "Retried continuation job must have a persisted dispatch envelope (not an orphaned partial record)");
      assert.equal(envelope.promptHash, job.promptHash);

      const matchingJobs = store.listJobs().filter((j) => j.requestId === "req_cont_transient_retry_1");
      assert.equal(matchingJobs.length, 1, "Exactly one durable continuation job must exist for requestId");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Happy Path: Queued allowRespawn=true commits child agent, job, correlation, and envelope
// ---------------------------------------------------------------------------
test("queued allowRespawn=true commits child lineage agent, job, correlation hint, and dispatch envelope together atomically", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-respawn-happy-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 1 });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const parentAgent = await setupCompletedAgent(service, store, client, tmpDir, "Parent Agent Happy", "req_parent_happy");
      store.updateAgentStatus(parentAgent.id, "closed");

      // Blocker occupies credit ceiling
      await service.spawn({
        requestId: "req_blocker_respawn_happy",
        topic: "Blocker Topic",
        task: "Blocker occupying 1 credit",
        cwd: tmpDir,
      });

      // Respawn closed agent behind blocker
      const receipt = await service.continueJob({
        agentId: parentAgent.id,
        requestId: "req_respawn_happy_1",
        task: "Respawn task happy path",
        allowRespawn: true,
        threadId: "thread_respawn_happy_1",
        turnId: "turn_respawn_happy_1",
      });

      assert.equal(receipt.accepted, true);
      assert.equal(receipt.status, "accepted");
      assert.ok(receipt.jobId);
      assert.ok(receipt.agentId);
      assert.notEqual(receipt.agentId, parentAgent.id, "Respawn must spawn a brand-new child lineage agent");

      const childAgent = store.getAgent(receipt.agentId);
      assert.ok(childAgent, "Child lineage agent must be persisted in store");
      assert.equal(childAgent.parentAgentId, parentAgent.id, "Child agent must reference parentAgentId");

      const job = store.getJob(receipt.jobId);
      assert.ok(job, "Continuation job must be persisted in store");
      assert.equal(job.agentId, childAgent.id);
      assert.equal(job.kind, "continue");
      assert.equal(job.status, "queued", "Job must remain queued while awaiting credit");
      assert.equal(job.hintThreadId, "thread_respawn_happy_1");
      assert.equal(job.hintTurnId, "turn_respawn_happy_1");

      const envelope = store.getDispatchEnvelope(receipt.jobId);
      assert.ok(envelope, "Dispatch envelope must commit together with child agent and job");
      assert.equal(envelope.promptHash, job.promptHash);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. Queued allowRespawn Atomicity: Late constraint failure rolls back leaving no orphan lineage agent, job, or envelope (RED)
// ---------------------------------------------------------------------------
test("late constraint failure during queued allowRespawn rolls back atomically leaving no orphan lineage agent, continuation job, correlation, or envelope", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-respawn-rollback-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 1 });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const parentAgent = await setupCompletedAgent(service, store, client, tmpDir, "Parent Agent Rollback", "req_parent_rb");
      store.updateAgentStatus(parentAgent.id, "closed");

      // Blocker occupies credit ceiling
      await service.spawn({
        requestId: "req_blocker_respawn_rb",
        topic: "Blocker Topic",
        task: "Blocker occupying 1 credit",
        cwd: tmpDir,
      });

      const initialAgentsCount = store.listAgents().length;
      const initialJobsCount = store.listJobs().length;

      // Inject late constraint failure during dispatch_envelopes insert
      store.db.exec(`
        CREATE TRIGGER fail_late_dispatch_envelope_respawn
        BEFORE INSERT ON dispatch_envelopes
        BEGIN
          SELECT RAISE(ABORT, 'injected late dispatch_envelope constraint failure for respawn');
        END;
      `);

      await assert.rejects(
        async () => {
          await service.continueJob({
            agentId: parentAgent.id,
            requestId: "req_respawn_fail_late_1",
            task: "Respawn task late failure",
            allowRespawn: true,
            threadId: "thread_respawn_fail_1",
            turnId: "turn_respawn_fail_1",
          });
        },
        /injected late dispatch_envelope constraint failure for respawn/i,
        "allowRespawn continueJob must reject when dispatch_envelopes insertion fails",
      );

      // PROVE ATOMIC ROLLBACK:
      // If admission was atomic, rolling back must leave:
      // - NO orphan lineage child agent in store
      // - NO orphan continuation job for requestId
      // - NO orphan correlation hints
      // - NO orphan dispatch envelopes
      // - Unaltered total agent and job counts
      const childLineageAgents = store.listAgents().filter((a) => a.parentAgentId === parentAgent.id);
      assert.equal(
        childLineageAgents.length,
        0,
        "Atomic rollback must leave no orphan lineage child agent in store",
      );
      assert.equal(
        store.getJobByRequestId("req_respawn_fail_late_1"),
        null,
        "Atomic rollback must leave no continuation job for requestId",
      );
      assert.equal(
        store.listAgents().length,
        initialAgentsCount,
        "Total agents count must remain unchanged after rollback",
      );
      assert.equal(
        store.listJobs().length,
        initialJobsCount,
        "Total jobs count must remain unchanged after rollback",
      );

      const orphanActivities = store.db.prepare(
        "SELECT COUNT(*) AS count FROM agent_activity WHERE summary LIKE '%req_respawn_fail_late_1%' OR summary LIKE '%Closed agent resumed: spawned lineage agent%'",
      ).get() as { count: number };
      assert.equal(Number(orphanActivities.count), 0, "No orphan activity records must remain after rollback");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 6. Queued allowRespawn Retry: Retrying with same requestId admits complete valid state (RED)
// ---------------------------------------------------------------------------
test("retry after late failure rollback of queued allowRespawn admits cleanly and produces complete valid state", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-respawn-retry-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 1 });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const parentAgent = await setupCompletedAgent(service, store, client, tmpDir, "Parent Agent Retry", "req_parent_retry");
      store.updateAgentStatus(parentAgent.id, "closed");

      // Blocker occupies credit ceiling
      await service.spawn({
        requestId: "req_blocker_respawn_retry",
        topic: "Blocker Topic",
        task: "Blocker occupying 1 credit",
        cwd: tmpDir,
      });

      // 1. Inject late constraint failure
      store.db.exec(`
        CREATE TRIGGER fail_transient_envelope_respawn
        BEFORE INSERT ON dispatch_envelopes
        BEGIN
          SELECT RAISE(ABORT, 'transient respawn envelope persistence failure');
        END;
      `);

      // 2. First attempt fails late during respawn admission
      await assert.rejects(
        async () => {
          await service.continueJob({
            agentId: parentAgent.id,
            requestId: "req_respawn_transient_retry_1",
            task: "Transient failure respawn task",
            allowRespawn: true,
            threadId: "thread_respawn_retry_1",
            turnId: "turn_respawn_retry_1",
          });
        },
        /transient respawn envelope persistence failure/i,
      );

      // 3. Transient failure resolves
      store.db.exec("DROP TRIGGER fail_transient_envelope_respawn;");

      // 4. Retry with the SAME requestId
      const retryReceipt = await service.continueJob({
        agentId: parentAgent.id,
        requestId: "req_respawn_transient_retry_1",
        task: "Transient failure respawn task",
        allowRespawn: true,
        threadId: "thread_respawn_retry_1",
        turnId: "turn_respawn_retry_1",
      });

      assert.equal(retryReceipt.accepted, true);
      assert.ok(retryReceipt.jobId);
      assert.ok(retryReceipt.agentId);
      assert.notEqual(retryReceipt.agentId, parentAgent.id, "Retried respawn must spawn a child lineage agent");

      const childAgent = store.getAgent(retryReceipt.agentId);
      assert.ok(childAgent, "Retried child lineage agent must exist");
      assert.equal(childAgent.parentAgentId, parentAgent.id);

      const job = store.getJob(retryReceipt.jobId);
      assert.ok(job, "Retried continuation job must exist");
      assert.equal(job.agentId, childAgent.id);
      assert.equal(job.status, "queued");
      assert.equal(job.kind, "continue");
      assert.equal(job.hintThreadId, "thread_respawn_retry_1");
      assert.equal(job.hintTurnId, "turn_respawn_retry_1");

      const envelope = store.getDispatchEnvelope(retryReceipt.jobId);
      assert.ok(envelope, "Retried respawn job must have a persisted dispatch envelope (not an orphaned partial record)");
      assert.equal(envelope.promptHash, job.promptHash);

      const childAgents = store.listAgents().filter((a) => a.parentAgentId === parentAgent.id);
      assert.equal(childAgents.length, 1, "Exactly one child lineage agent must exist for parent");

      const matchingJobs = store.listJobs().filter((j) => j.requestId === "req_respawn_transient_retry_1");
      assert.equal(matchingJobs.length, 1, "Exactly one durable continuation job must exist for requestId");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});
