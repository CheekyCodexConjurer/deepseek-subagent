import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile, mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeService, type ManagedOpenCodeLike } from "../../src/service.js";
import { TranscriptAttestor } from "../../src/codex/transcript-attestor.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

let globalSessionCounter = 0;

class FakeOpenCodeClient implements OpenCodeClientLike {
  messages: OpenCodeMessage[] = [];
  activeSessions = new Set<string>();

  async health() {
    return { healthy: true, version: "fake" };
  }
  async createSession() {
    globalSessionCounter += 1;
    const id = "session_fake_" + globalSessionCounter;
    this.activeSessions.add(id);
    return { id };
  }
  async promptAsync() {}
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
  async subscribe() {}
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
// 1. Fail-closed: Jobs without the same atomic batch are rejected
// ---------------------------------------------------------------------------
test("jobs without the same atomic batch are rejected fail-closed when binding to the same Codex correlation tuple", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-batch-corr-fail-closed-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const agent = store.createAgent({
      id: "agent_fail_closed",
      title: "Agent Fail Closed",
      topic: "Security",
      repositoryRoot: tmpDir,
      workspacePath: tmpDir,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_fc",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });

    const threadId = "11111111-1111-4111-8111-111111111111";
    const turnId = "22222222-2222-4222-8222-222222222222";
    const itemId = "mcptoolcall_item_001";

    // 1. Unary jobs (neither belongs to any batch)
    const unaryJob1 = store.createJob({
      id: "job_unary_1",
      agentId: agent.id,
      sequence: 1,
      kind: "spawn",
      requestId: "req_unary_1",
      promptHash: "hash_u1",
      status: "queued",
    });
    const unaryJob2 = store.createJob({
      id: "job_unary_2",
      agentId: agent.id,
      sequence: 2,
      kind: "spawn",
      requestId: "req_unary_2",
      promptHash: "hash_u2",
      status: "queued",
    });

    store.bindJob({ jobId: unaryJob1.id, threadId, originatingTurnId: turnId, originatingItemId: itemId });
    assert.throws(
      () => store.bindJob({ jobId: unaryJob2.id, threadId, originatingTurnId: turnId, originatingItemId: itemId }),
      /Codex correlation tuple is already bound/,
      "Non-batch jobs must be rejected fail-closed when sharing the same correlation tuple",
    );

    // 2. Cross-batch jobs (belonging to different batches)
    const batchA = store.createBatch({ id: "batch_a", requestId: "req_batch_a", batchHash: "hash_ba" });
    const batchB = store.createBatch({ id: "batch_b", requestId: "req_batch_b", batchHash: "hash_bb" });

    const jobA = store.createJob({
      id: "job_cross_a",
      agentId: agent.id,
      sequence: 3,
      kind: "spawn",
      requestId: "req_cross_a",
      promptHash: "hash_ca",
      status: "queued",
      batchId: batchA.id,
    });
    const jobB = store.createJob({
      id: "job_cross_b",
      agentId: agent.id,
      sequence: 4,
      kind: "spawn",
      requestId: "req_cross_b",
      promptHash: "hash_cb",
      status: "queued",
      batchId: batchB.id,
    });

    const threadCross = "33333333-3333-4333-8333-333333333333";
    const turnCross = "44444444-4444-4444-8444-444444444444";
    const itemCross = "mcptoolcall_item_cross";

    store.bindJob({ jobId: jobA.id, threadId: threadCross, originatingTurnId: turnCross, originatingItemId: itemCross });
    assert.throws(
      () => store.bindJob({ jobId: jobB.id, threadId: threadCross, originatingTurnId: turnCross, originatingItemId: itemCross }),
      /Codex correlation tuple is already bound/,
      "Jobs from different batches must be rejected fail-closed when sharing the same correlation tuple",
    );

    store.close();
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Bug Reproduction: Atomic batch jobs bound to same correlation tuple and parked together
// ---------------------------------------------------------------------------
test("two or three jobs admitted in the same atomic batch can be attested/bound to the same Codex correlation tuple and parked together", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-batch-corr-regress-"));
  try {
    const sessionsDir = path.join(tmpDir, "sessions");
    await mkdir(sessionsDir, { recursive: true });

    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const attestor = new TranscriptAttestor({ sessionsDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
      transcriptAttestor: attestor,
      cliTransport: {
        probeCapabilities: async () => ({ compatible: true, version: "0.150.0" }),
        deliverWake: async () => ({ success: true, accepted: true }),
      } as any,
    });
    await service.start();

    try {
      // 1. Admit 3 jobs atomically in the same batch
      const batchReceipt = await service.spawnBatch({
        batchRequestId: "batch_atomic_regress_req",
        items: [
          { requestId: "req_batch_item_1", topic: "Worker 1", task: "Execute worker 1 task" },
          { requestId: "req_batch_item_2", topic: "Worker 2", task: "Execute worker 2 task" },
          { requestId: "req_batch_item_3", topic: "Worker 3", task: "Execute worker 3 task" },
        ],
      });

      assert.equal(batchReceipt.accepted, true);
      assert.equal(batchReceipt.items.length, 3);
      const [j1, j2, j3] = batchReceipt.items.map((it) => it.jobId);

      // Verify all 3 jobs are linked to the same batch in store
      const job1Record = store.getJob(j1);
      const job2Record = store.getJob(j2);
      const job3Record = store.getJob(j3);
      assert.ok(job1Record?.batchId, "Job 1 must have batchId");
      assert.equal(job1Record.batchId, batchReceipt.batchId);
      assert.equal(job2Record?.batchId, batchReceipt.batchId);
      assert.equal(job3Record?.batchId, batchReceipt.batchId);

      // 2. Transcript records the atomic batch tool completion (all 3 jobs in a single mcptoolcall)
      const threadId = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
      const turnId = "11111111-2222-4333-8444-555555555555";
      const itemId = "call_mcp_spawn_batch_atomic";

      const transcriptEvent = JSON.stringify({
        type: "event_msg",
        payload: {
          type: "item_completed",
          thread_id: threadId,
          turn_id: turnId,
          item: {
            id: itemId,
            type: "mcptoolcall",
            status: "completed",
            server: "subagents",
            tool: "subagents_spawn_batch",
            result: {
              structuredContent: {
                accepted: true,
                batchId: batchReceipt.batchId,
                items: [
                  { jobId: j1, agentId: batchReceipt.items[0].agentId, status: "queued" },
                  { jobId: j2, agentId: batchReceipt.items[1].agentId, status: "queued" },
                  { jobId: j3, agentId: batchReceipt.items[2].agentId, status: "queued" },
                ],
              },
            },
          },
        },
      });
      await writeFile(path.join(sessionsDir, "transcript.jsonl"), transcriptEvent + "\n");

      // 3. Park the batch jobs together: service.park must attest and bind all jobs
      // to the same (thread, turn, item) tuple and successfully arm the park barrier.
      const parkReceipt = await service.park({
        jobIds: [j1, j2, j3],
        predicate: "ALL",
      });

      assert.equal(parkReceipt.armed, true);
      assert.equal(parkReceipt.obligationState, "pending");

      // Verify all jobs are bound to the identical tuple
      const b1 = store.getBinding(j1);
      const b2 = store.getBinding(j2);
      const b3 = store.getBinding(j3);
      assert.ok(b1, "Job 1 must be bound");
      assert.ok(b2, "Job 2 must be bound");
      assert.ok(b3, "Job 3 must be bound");
      assert.equal(b1.threadId, threadId);
      assert.equal(b2.threadId, threadId);
      assert.equal(b3.threadId, threadId);
      assert.equal(b1.originatingTurnId, turnId);
      assert.equal(b2.originatingTurnId, turnId);
      assert.equal(b3.originatingTurnId, turnId);
      assert.equal(b1.originatingItemId, itemId);
      assert.equal(b2.originatingItemId, itemId);
      assert.equal(b3.originatingItemId, itemId);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});
