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

test("Codex adapter emits onCorrelation once per job for subagents_spawn_batch and deduplicates on replay", async () => {
  const rpc = new FakeRpc();
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();

  const correlations: CodexCorrelation[] = [];
  adapter.onCorrelation((correlation) => correlations.push(correlation));

  const threadId = "thread_batch_target";
  const turnId = "turn_batch_target";
  const itemId = "item_call_batch_789";

  const batchNotification = {
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
            batchId: "batch_abc",
            batchRequestId: "batch_req_abc",
            items: [
              { jobId: "job_batch_1", agentId: "agent_batch_1", status: "accepted" },
              { jobId: "job_batch_2", agentId: "agent_batch_2", status: "accepted" },
              { jobId: "job_batch_3", agentId: "agent_batch_3", status: "accepted" },
            ],
            jobIds: ["job_batch_1", "job_batch_2", "job_batch_3"],
            obligationState: "pending",
            nextRequiredAction: "subagents_follow",
          },
        },
      },
    },
  };

  // 1. A single item/completed notification of subagents_spawn_batch with 3 jobs
  rpc.emit(batchNotification);

  assert.equal(
    correlations.length,
    3,
    `Expected exactly 3 correlations for 3 batched jobs, received ${correlations.length}`,
  );
  assert.deepEqual(correlations, [
    { jobId: "job_batch_1", threadId, turnId, itemId },
    { jobId: "job_batch_2", threadId, turnId, itemId },
    { jobId: "job_batch_3", threadId, turnId, itemId },
  ]);

  // 2. Replay: emitting the exact same notification must not duplicate correlations
  rpc.emit(batchNotification);

  assert.equal(
    correlations.length,
    3,
    `Replay of identical item/completed notification must not duplicate correlations, received ${correlations.length}`,
  );

  await adapter.close();
});

test("Codex adapter preserves unary spawn correlation behavior and replay deduplication", async () => {
  const rpc = new FakeRpc();
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();

  const correlations: CodexCorrelation[] = [];
  adapter.onCorrelation((correlation) => correlations.push(correlation));

  const threadId = "thread_unary_target";
  const turnId = "turn_unary_target";
  const itemId = "item_call_unary_456";

  const unaryNotification = {
    method: "item/completed",
    params: {
      threadId,
      turnId,
      item: {
        id: itemId,
        type: "mcpToolCall",
        status: "completed",
        server: "subagents",
        tool: "subagents_spawn",
        result: {
          structuredContent: {
            accepted: true,
            status: "accepted",
            jobId: "job_unary_1",
          },
        },
      },
    },
  };

  // 1. Single unary notification
  rpc.emit(unaryNotification);

  assert.equal(correlations.length, 1);
  assert.deepEqual(correlations, [
    { jobId: "job_unary_1", threadId, turnId, itemId },
  ]);

  // 2. Replay: emitting the exact same notification must not duplicate
  rpc.emit(unaryNotification);

  assert.equal(correlations.length, 1, "Replay of unary completion must not duplicate correlations");

  await adapter.close();
});
