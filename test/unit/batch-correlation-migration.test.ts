import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { BridgeStore } from "../../src/store.js";

test("migration v21 upgrade and reopen preserves 3 shared batch bindings and maintains cross-batch rejection", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-migration-v21-"));
  const dbPath = path.join(tmpDir, "migration-test.sqlite");

  try {
    // 1. Initialize complete database up to v20 by opening store, then reverting to pre-v21 schema
    {
      const initStore = new BridgeStore(dbPath);
      initStore.close();

      const rawDb = new DatabaseSync(dbPath);
      // Revert index to pre-v21 UNIQUE index and delete migration 21 record
      rawDb.exec("DROP INDEX IF EXISTS idx_codex_bindings_correlation;");
      rawDb.exec(`
        CREATE UNIQUE INDEX idx_codex_bindings_correlation
          ON codex_bindings(thread_id, originating_turn_id, originating_item_id)
          WHERE originating_turn_id IS NOT NULL AND originating_item_id IS NOT NULL;
      `);
      rawDb.exec("DELETE FROM schema_migrations WHERE version = 21;");
      rawDb.close();
    }

    // 2. Open via BridgeStore which runs migrate() and applies v21 inside transaction
    let store = new BridgeStore(dbPath);

    // Verify v21 migration is recorded
    const v21Row = (store as unknown as { db: DatabaseSync }).db
      .prepare("SELECT version FROM schema_migrations WHERE version = 21")
      .get() as { version: number } | undefined;
    assert.ok(v21Row, "Migration v21 must be applied and recorded in schema_migrations");

    // 3. Create agent and batch with 3 jobs sharing the batchId
    const agent = store.createAgent({
      id: "agent_shared_batch",
      title: "Agent Shared Batch",
      topic: "Batch Migration Test",
      repositoryRoot: tmpDir,
      workspacePath: tmpDir,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_sb",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });

    const batch = store.createBatch({
      id: "batch_mig_shared_123",
      requestId: "batch_req_shared_123",
      batchHash: "hash_mig_shared_123",
    });

    const job1 = store.createJob({
      id: "job_mig_1",
      agentId: agent.id,
      sequence: 1,
      kind: "spawn",
      requestId: "req_mig_1",
      promptHash: "hash_1",
      status: "queued",
      batchId: batch.id,
      batchRequestId: batch.requestId,
    });
    const job2 = store.createJob({
      id: "job_mig_2",
      agentId: agent.id,
      sequence: 2,
      kind: "spawn",
      requestId: "req_mig_2",
      promptHash: "hash_2",
      status: "queued",
      batchId: batch.id,
      batchRequestId: batch.requestId,
    });
    const job3 = store.createJob({
      id: "job_mig_3",
      agentId: agent.id,
      sequence: 3,
      kind: "spawn",
      requestId: "req_mig_3",
      promptHash: "hash_3",
      status: "queued",
      batchId: batch.id,
      batchRequestId: batch.requestId,
    });

    const threadId = "thread_shared_mig_tuple";
    const turnId = "turn_shared_mig_tuple";
    const itemId = "item_shared_mig_tuple";

    // Bind all 3 jobs to the exact same Codex correlation tuple
    const b1 = store.bindJob({ jobId: job1.id, threadId, originatingTurnId: turnId, originatingItemId: itemId });
    const b2 = store.bindJob({ jobId: job2.id, threadId, originatingTurnId: turnId, originatingItemId: itemId });
    const b3 = store.bindJob({ jobId: job3.id, threadId, originatingTurnId: turnId, originatingItemId: itemId });

    assert.equal(b1.jobId, job1.id);
    assert.equal(b2.jobId, job2.id);
    assert.equal(b3.jobId, job3.id);

    // 4. Close database
    store.close();

    // 5. Reopen database via fresh BridgeStore instance
    const reopenedStore = new BridgeStore(dbPath);

    // Verify the 3 shared bindings survived reopen
    const rb1 = reopenedStore.getBinding(job1.id);
    const rb2 = reopenedStore.getBinding(job2.id);
    const rb3 = reopenedStore.getBinding(job3.id);
    assert.ok(rb1 && rb1.threadId === threadId && rb1.originatingItemId === itemId);
    assert.ok(rb2 && rb2.threadId === threadId && rb2.originatingItemId === itemId);
    assert.ok(rb3 && rb3.threadId === threadId && rb3.originatingItemId === itemId);

    // 6. Cross-batch rejection AFTER reopen:
    // Create a 4th job belonging to a DIFFERENT batch
    const batchOther = reopenedStore.createBatch({
      id: "batch_other_different_999",
      requestId: "batch_req_diff_999",
      batchHash: "hash_other_diff",
    });

    const jobDifferentBatch = reopenedStore.createJob({
      id: "job_mig_diff_batch",
      agentId: agent.id,
      sequence: 4,
      kind: "spawn",
      requestId: "req_mig_diff",
      promptHash: "hash_diff",
      status: "queued",
      batchId: batchOther.id,
      batchRequestId: batchOther.requestId,
    });

    assert.throws(
      () => {
        reopenedStore.bindJob({
          jobId: jobDifferentBatch.id,
          threadId,
          originatingTurnId: turnId,
          originatingItemId: itemId,
        });
      },
      /Codex correlation tuple is already bound to job job_mig_1/,
      "Binding a job from a different batch to the shared tuple must throw cross-batch rejection",
    );

    // 7. Unary rejection (no batchId) AFTER reopen:
    const jobUnary = reopenedStore.createJob({
      id: "job_mig_unary",
      agentId: agent.id,
      sequence: 5,
      kind: "spawn",
      requestId: "req_mig_unary",
      promptHash: "hash_unary",
      status: "queued",
    });

    assert.throws(
      () => {
        reopenedStore.bindJob({
          jobId: jobUnary.id,
          threadId,
          originatingTurnId: turnId,
          originatingItemId: itemId,
        });
      },
      /Codex correlation tuple is already bound to job job_mig_1/,
      "Binding an unary job without batch to the shared tuple must throw",
    );

    // 8. Distinct tuple binding must succeed
    const distinctItemId = "item_distinct_mig_tuple";
    const bDistinct = reopenedStore.bindJob({
      jobId: jobDifferentBatch.id,
      threadId,
      originatingTurnId: turnId,
      originatingItemId: distinctItemId,
    });
    assert.equal(bDistinct.jobId, jobDifferentBatch.id);

    reopenedStore.close();
  } finally {
    await rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
});