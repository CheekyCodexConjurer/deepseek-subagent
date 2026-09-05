import test from "node:test";
import assert from "node:assert/strict";
import { createDefaultConfig } from "../../src/config.js";
import { CodexAppServerDeliveryAdapter, type CodexRpcTransport, type CodexCorrelation } from "../../src/codex/adapter.js";

class FakeRpc implements CodexRpcTransport {
  calls: string[] = [];
  private listener: ((notification: { method: string; params?: unknown }) => void) | null = null;
  steerError = "no active turn";

  async start(): Promise<void> {}

  onNotification(listener: (notification: { method: string; params?: unknown }) => void): () => void {
    this.listener = listener;
    return () => {
      this.listener = null;
    };
  }

  async call(method: string): Promise<unknown> {
    this.calls.push(method);
    if (method === "turn/steer") throw new Error(this.steerError);
    return {};
  }

  async close(): Promise<void> {}

  emit(notification: { method: string; params?: unknown }): void {
    this.listener?.(notification);
  }
}

// ---------------------------------------------------------------------------
// 1. Defect 1: items contains only rejected jobs; fallback to jobIds must NOT accept them
// ---------------------------------------------------------------------------
test("batch with only rejected items does not fall back to jobIds to accept rejected jobs", async () => {
  const rpc = new FakeRpc();
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();

  const correlations: CodexCorrelation[] = [];
  adapter.onCorrelation((correlation) => correlations.push(correlation));

  const threadId = "thread_rej_1";
  const turnId = "turn_rej_1";
  const itemId = "item_rej_1";

  const rejectedBatchNotification = {
    method: "item/completed",
    params: {
      threadId,
      turnId,
      item: {
        id: itemId,
        type: "mcpToolCall",
        status: "completed",
        server: "subagents",
        tool: "subagents_spawn_batch",
        result: {
          structuredContent: {
            accepted: true,
            batchId: "batch_rej_1",
            batchRequestId: "batch_req_rej_1",
            items: [
              { jobId: "job_rejected_1", status: "rejected", accepted: false },
            ],
            jobIds: ["job_rejected_1"],
          },
        },
      },
    },
  };

  rpc.emit(rejectedBatchNotification);

  assert.equal(
    correlations.length,
    0,
    `Expected 0 correlations when all items are rejected, but received ${correlations.length}`,
  );

  await adapter.close();
});

// ---------------------------------------------------------------------------
// 2. Defect 2: Conflicting accepted/rejected sources must be rejected fail-closed
// ---------------------------------------------------------------------------
test("conflicting accepted states between structuredContent and meta are rejected fail-closed", async () => {
  const rpc = new FakeRpc();
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();

  const correlations: CodexCorrelation[] = [];
  adapter.onCorrelation((correlation) => correlations.push(correlation));

  // Case 2a: structuredContent is rejected, but meta claims accepted
  rpc.emit({
    method: "item/completed",
    params: {
      threadId: "thread_conf_1",
      turnId: "turn_conf_1",
      item: {
        id: "item_conf_1",
        type: "mcpToolCall",
        status: "completed",
        server: "subagents",
        tool: "subagents_spawn",
        result: {
          structuredContent: {
            accepted: false,
            status: "rejected",
            jobId: "job_bad_1",
          },
          _meta: {
            technical: {
              accepted: true,
              status: "accepted",
              jobId: "job_bad_1",
            },
          },
        },
      },
    },
  });

  assert.equal(correlations.length, 0, "Conflicting unary sources must reject fail-closed");

  // Case 2b: unary mixing identity from rejected source with accepted source
  rpc.emit({
    method: "item/completed",
    params: {
      threadId: "thread_conf_2",
      turnId: "turn_conf_2",
      item: {
        id: "item_conf_2",
        type: "mcpToolCall",
        status: "completed",
        server: "subagents",
        tool: "subagents_spawn",
        result: {
          structuredContent: {
            accepted: false,
            jobId: "job_rejected_identity",
          },
          _meta: {
            technical: {
              accepted: true,
              status: "accepted",
              jobId: "job_accepted_other",
            },
          },
        },
      },
    },
  });

  assert.equal(correlations.length, 0, "Must not mix identity from rejected source with accepted source");

  // Case 2c: batch where structuredContent accepted: false but meta accepted: true
  rpc.emit({
    method: "item/completed",
    params: {
      threadId: "thread_conf_3",
      turnId: "turn_conf_3",
      item: {
        id: "item_conf_3",
        type: "mcpToolCall",
        status: "completed",
        server: "subagents",
        tool: "subagents_spawn_batch",
        result: {
          structuredContent: {
            accepted: false,
            batchId: "batch_conf_3",
            items: [{ jobId: "job_batch_conf_1", status: "rejected" }],
          },
          _meta: {
            technical: {
              accepted: true,
              batchId: "batch_conf_3",
              items: [{ jobId: "job_batch_conf_1", status: "accepted" }],
            },
          },
        },
      },
    },
  });

  assert.equal(correlations.length, 0, "Batch with conflicting accepted state must reject fail-closed");

  await adapter.close();
});

// ---------------------------------------------------------------------------
// 3. Defect 3: Modified replay adding jobs or altering batchId must be rejected atomically
// ---------------------------------------------------------------------------
test("modified replay adding jobs or altering batchId is rejected atomically fail-closed", async () => {
  const rpc = new FakeRpc();
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();

  const correlations: CodexCorrelation[] = [];
  adapter.onCorrelation((correlation) => correlations.push(correlation));

  const threadId = "thread_replay_test";
  const turnId = "turn_replay_test";
  const itemId = "item_call_replay_test";

  const initialNotification = {
    method: "item/completed",
    params: {
      threadId,
      turnId,
      item: {
        id: itemId,
        type: "mcpToolCall",
        status: "completed",
        server: "subagents",
        tool: "subagents_spawn_batch",
        result: {
          structuredContent: {
            accepted: true,
            batchId: "batch_immutable_1",
            batchRequestId: "batch_req_1",
            items: [
              { jobId: "job_orig_1", status: "accepted" },
              { jobId: "job_orig_2", status: "accepted" },
            ],
            jobIds: ["job_orig_1", "job_orig_2"],
          },
        },
      },
    },
  };

  // 1. Initial valid batch with 2 jobs
  rpc.emit(initialNotification);
  assert.equal(correlations.length, 2, "Expected 2 initial correlations");

  // 2. Modified replay: same itemId, same batchId, but adding a 3rd job
  const modifiedReplayNotification = {
    method: "item/completed",
    params: {
      threadId,
      turnId,
      item: {
        id: itemId,
        type: "mcpToolCall",
        status: "completed",
        server: "subagents",
        tool: "subagents_spawn_batch",
        result: {
          structuredContent: {
            accepted: true,
            batchId: "batch_immutable_1",
            batchRequestId: "batch_req_1",
            items: [
              { jobId: "job_orig_1", status: "accepted" },
              { jobId: "job_orig_2", status: "accepted" },
              { jobId: "job_added_3", status: "accepted" },
            ],
            jobIds: ["job_orig_1", "job_orig_2", "job_added_3"],
          },
        },
      },
    },
  };

  rpc.emit(modifiedReplayNotification);
  assert.equal(
    correlations.length,
    2,
    `Modified replay adding jobs must be rejected atomically; expected 2 but got ${correlations.length}`,
  );

  // 3. Modified replay: same itemId, same jobs, but different batchId
  const differentBatchIdNotification = {
    method: "item/completed",
    params: {
      threadId,
      turnId,
      item: {
        id: itemId,
        type: "mcpToolCall",
        status: "completed",
        server: "subagents",
        tool: "subagents_spawn_batch",
        result: {
          structuredContent: {
            accepted: true,
            batchId: "batch_DIFFERENT_2",
            batchRequestId: "batch_req_1",
            items: [
              { jobId: "job_orig_1", status: "accepted" },
              { jobId: "job_orig_2", status: "accepted" },
            ],
            jobIds: ["job_orig_1", "job_orig_2"],
          },
        },
      },
    },
  };

  rpc.emit(differentBatchIdNotification);
  assert.equal(
    correlations.length,
    2,
    `Replay with contradictory batchId must be rejected atomically; expected 2 but got ${correlations.length}`,
  );

  await adapter.close();
});