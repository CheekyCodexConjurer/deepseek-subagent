import test from "node:test";
import assert from "node:assert/strict";
import { createDefaultConfig } from "../../src/config.js";
import { CodexAppServerDeliveryAdapter, type CodexRpcTransport } from "../../src/codex/adapter.js";
import type { CodexBinding, JobRecord } from "../../src/types.js";

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

const job = {
  id: "job_test",
  agentId: "agent_test",
  sequence: 1,
  kind: "spawn",
  requestId: "request_test",
  promptHash: "hash",
  status: "completed",
  createdAt: "",
  startedAt: null,
  completedAt: null,
  lastUserMessageId: null,
  lastAssistantMessageId: null,
  resultPath: null,
  resultSummary: null,
  error: null,
} as JobRecord;

test("Codex adapter uses steer first and falls back to start for a non-steerable turn", async () => {
  const rpc = new FakeRpc();
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();
  const binding: CodexBinding = {
    jobId: job.id,
    threadId: "thread_test",
    originatingTurnId: "turn_test",
    originatingItemId: null,
    boundAt: "",
  };
  const method = await adapter.deliver(job, binding, "result");
  assert.equal(method, "codex-start");
  assert.deepEqual(rpc.calls, ["turn/steer", "turn/start"]);
  await adapter.close();
});

test("Codex adapter deterministically extracts job correlation from MCP item completion", async () => {
  const rpc = new FakeRpc();
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();
  const correlations: unknown[] = [];
  adapter.onCorrelation((value) => correlations.push(value));
  rpc.emit({
    method: "item/completed",
    params: {
      threadId: "thread_test",
      turnId: "turn_test",
      item: {
        id: "item_test",
        type: "mcpToolCall",
        tool: "deepseek_spawn",
        result: { structuredContent: { jobId: "job_test" } },
      },
    },
  });
  assert.deepEqual(correlations, [{
    jobId: "job_test",
    threadId: "thread_test",
    turnId: "turn_test",
    itemId: "item_test",
  }]);
  await adapter.close();
});

test("Codex adapter waits for a non-steerable turn to complete before starting", async () => {
  const rpc = new FakeRpc();
  rpc.steerError = "turn is non-steerable while review is active";
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();
  const binding: CodexBinding = {
    jobId: job.id,
    threadId: "thread_test",
    originatingTurnId: "turn_test",
    originatingItemId: null,
    boundAt: "",
  };
  let settled = false;
  const pending = adapter.deliver(job, binding, "result").then(() => {
    settled = true;
  });
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(settled, false);
  rpc.emit({ method: "turn/completed", params: { threadId: "thread_test", turnId: "turn_test" } });
  await pending;
  assert.deepEqual(rpc.calls, ["turn/steer", "turn/start"]);
  await adapter.close();
});
