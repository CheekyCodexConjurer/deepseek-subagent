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

// ---------------------------------------------------------------------------
// 1. Happy Path: Agent + Job + Correlation Hint + Dispatch Envelope Commit Together
// ---------------------------------------------------------------------------
test("unary admission commits agent, job, correlation hint, and dispatch envelope together", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-unary-commit-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    // credit ceiling 1 + blocker ensures admitted task remains queued so dispatch envelope is retained
    const config = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 1 });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // Sched blocker to occupy credit ceiling
      await service.spawn({
        requestId: "req_blocker_1",
        topic: "Blocker Topic",
        task: "Blocker Task",
        cwd: tmpDir,
      });

      // Unary admission behind blocker
      const receipt = await service.spawn({
        requestId: "req_unary_commit_1",
        topic: "Unary Commit Topic",
        task: "Unary Commit Task",
        cwd: tmpDir,
        threadId: "thread_unary_1",
        turnId: "turn_unary_1",
      });

      assert.equal(receipt.accepted, true);
      assert.equal(receipt.status, "accepted");
      assert.ok(receipt.jobId, "Receipt must contain jobId");
      assert.ok(receipt.agentId, "Receipt must contain agentId");

      // Verify Job row committed
      const job = store.getJob(receipt.jobId);
      assert.ok(job, "Job must be durably persisted in store");
      assert.equal(job.agentId, receipt.agentId, "Job must reference the admitted agent");
      assert.equal(job.requestId, "req_unary_commit_1");
      assert.equal(job.status, "queued", "Job should remain queued while awaiting credit");

      // Verify Correlation Hint committed on Job
      assert.equal(job.hintThreadId, "thread_unary_1", "Correlation hint threadId must commit with job");
      assert.equal(job.hintTurnId, "turn_unary_1", "Correlation hint turnId must commit with job");
      assert.equal(job.hintSource, "mcp", "Correlation hint source must commit with job");

      // Verify Agent row committed
      const agent = store.getAgent(receipt.agentId);
      assert.ok(agent, "Agent must be durably persisted in store");
      assert.equal(agent.topic, "Unary Commit Topic");

      // Verify Dispatch Envelope committed
      const envelope = store.getDispatchEnvelope(receipt.jobId);
      assert.ok(envelope, "Dispatch envelope must commit together with job");
      assert.equal(envelope.promptHash, job.promptHash, "Envelope promptHash must match job promptHash");
      assert.equal(typeof envelope.workerInput, "object");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Atomicity on Late Failure: Rollback Leaves No Orphan Agent, Job, or Envelope (RED)
// ---------------------------------------------------------------------------
test("late constraint failure during unary admission rolls back atomically leaving no orphan agent, job, or envelope", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-unary-rollback-"));
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
      assert.equal(store.listJobs().length, 0, "Initial jobs must be 0");
      assert.equal(store.listAgents().length, 0, "Initial agents must be 0");

      // Inject a constraint/failure late enough: during dispatch envelope persistence
      // (after agent and job records have been created in store)
      store.db.exec(`
        CREATE TRIGGER fail_late_dispatch_envelope
        BEFORE INSERT ON dispatch_envelopes
        BEGIN
          SELECT RAISE(ABORT, 'injected late dispatch_envelope constraint failure');
        END;
      `);

      // Unary admission must fail due to the late constraint violation
      await assert.rejects(
        async () => {
          await service.spawn({
            requestId: "req_unary_fail_late_1",
            topic: "Late Failure Topic",
            task: "Late Failure Task",
            cwd: tmpDir,
            threadId: "thread_fail_1",
            turnId: "turn_fail_1",
          });
        },
        (err: unknown) => {
          return err instanceof Error && /injected late dispatch_envelope constraint failure/i.test(err.message);
        },
        "Unary admission must reject when dispatch envelope constraint fails",
      );

      // PROVE ATOMIC ROLLBACK:
      // An atomic admission must roll back all staged changes, leaving NO orphan agent,
      // NO orphan job, and NO orphan envelope.
      // Under non-atomic execution, agent and job rows were already committed before
      // the envelope insert failed, leaving dangling orphan records in SQLite.
      assert.equal(store.listJobs().length, 0, "Atomic rollback must leave no orphan jobs in store");
      assert.equal(store.listAgents().length, 0, "Atomic rollback must leave no orphan agents in store");
      assert.equal(store.getJobByRequestId("req_unary_fail_late_1"), null, "Atomic rollback must leave no job for requestId");

      const envelopeCount = store.db.prepare("SELECT COUNT(*) AS count FROM dispatch_envelopes").get() as { count: number };
      assert.equal(Number(envelopeCount.count), 0, "Atomic rollback must leave no dispatch envelopes");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Retry / Idempotency: Returns Existing Exact Job Without Duplicate Rows
// ---------------------------------------------------------------------------
test("unary admission retry with identical requestId returns existing exact job without duplicate durable rows", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-unary-idem-"));
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
      // Blocker occupies credit ceiling so admitted job remains queued with envelope
      await service.spawn({
        requestId: "req_blocker_idem",
        topic: "Blocker Topic",
        task: "Blocker Task",
        cwd: tmpDir,
      });

      // First admission
      const receipt1 = await service.spawn({
        requestId: "req_unary_idem_1",
        topic: "Idempotent Topic",
        task: "Idempotent Task",
        cwd: tmpDir,
        threadId: "thread_idem_1",
        turnId: "turn_idem_1",
      });

      assert.equal(receipt1.accepted, true);
      assert.ok(receipt1.jobId);
      assert.ok(receipt1.agentId);

      // Retry with the exact same requestId (even with mutated prompt/topic)
      const receipt2 = await service.spawn({
        requestId: "req_unary_idem_1",
        topic: "Mutated Idempotent Topic",
        task: "Mutated Idempotent Task",
        cwd: tmpDir,
        threadId: "thread_idem_1",
        turnId: "turn_idem_1",
      });

      // Must return the existing exact job
      assert.equal(receipt2.accepted, true);
      assert.equal(receipt2.jobId, receipt1.jobId, "Idempotent retry must return the identical job ID");
      assert.equal(receipt2.agentId, receipt1.agentId, "Idempotent retry must return the identical agent ID");

      // Verify no duplicate durable rows exist in database
      const matchingJobs = store.listJobs().filter((j) => j.requestId === "req_unary_idem_1");
      assert.equal(matchingJobs.length, 1, "Exactly one durable job row must exist for requestId");

      const agentRows = store.db.prepare("SELECT COUNT(*) AS count FROM agents WHERE id = ?").get(receipt1.agentId) as { count: number };
      assert.equal(Number(agentRows.count), 1, "Exactly one durable agent row must exist");

      const envelopeRows = store.db.prepare("SELECT COUNT(*) AS count FROM dispatch_envelopes WHERE job_id = ?").get(receipt1.jobId) as { count: number };
      assert.equal(Number(envelopeRows.count), 1, "Exactly one durable dispatch envelope row must exist");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Retry After Late Rollback: Retrying Admits Cleanly Without Orphan State (RED)
// ---------------------------------------------------------------------------
test("retry after late failure rollback admits cleanly and produces complete valid state", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-unary-clean-retry-"));
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
      // Blocker occupies credit ceiling so successfully admitted job remains queued with envelope
      await service.spawn({
        requestId: "req_blocker_clean_retry",
        topic: "Blocker Topic",
        task: "Blocker Task",
        cwd: tmpDir,
      });

      // 1. Inject late constraint failure
      store.db.exec(`
        CREATE TRIGGER fail_transient_envelope
        BEFORE INSERT ON dispatch_envelopes
        BEGIN
          SELECT RAISE(ABORT, 'transient envelope persistence failure');
        END;
      `);

      // 2. First attempt fails late during admission
      await assert.rejects(
        async () => {
          await service.spawn({
            requestId: "req_transient_retry_1",
            topic: "Transient Failure Topic",
            task: "Transient Failure Task",
            cwd: tmpDir,
          });
        },
        /transient envelope persistence failure/i,
      );

      // 3. Transient condition resolves (trigger removed)
      store.db.exec("DROP TRIGGER fail_transient_envelope;");

      // 4. Retry with the SAME requestId
      // If admission rolled back cleanly, retry admits fresh and persists the complete tuple.
      // If admission left an orphan job, retry either returns the broken envelope-less job or fails.
      const retryReceipt = await service.spawn({
        requestId: "req_transient_retry_1",
        topic: "Transient Failure Topic",
        task: "Transient Failure Task",
        cwd: tmpDir,
      });

      assert.equal(retryReceipt.accepted, true);
      assert.ok(retryReceipt.jobId);

      const job = store.getJob(retryReceipt.jobId);
      assert.ok(job, "Retried job must exist");

      const envelope = store.getDispatchEnvelope(retryReceipt.jobId);
      assert.ok(envelope, "Retried job must have a persisted dispatch envelope (not an orphaned partial record)");

      const totalJobs = store.listJobs().filter((j) => j.requestId === "req_transient_retry_1");
      assert.equal(totalJobs.length, 1, "Exactly one durable job must exist after successful retry");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});
