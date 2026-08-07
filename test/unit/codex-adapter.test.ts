import test from "node:test";
import assert from "node:assert/strict";
import { createDefaultConfig } from "../../src/config.js";
import { CodexAppServerDeliveryAdapter, type CodexRpcTransport } from "../../src/codex/adapter.js";
import { JsonRpcWebSocketClient } from "../../src/codex/websocket.js";
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
        status: "completed",
        server: "deepseek-subagent",
        tool: "deepseek_spawn",
        result: { structuredContent: { accepted: true, status: "accepted", jobId: "job_test" } },
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

test("Codex adapter ignores untrusted or incomplete MCP completion items", async () => {
  const rpc = new FakeRpc();
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();
  const correlations: unknown[] = [];
  adapter.onCorrelation((value) => correlations.push(value));
  const base = {
    id: "item_test",
    type: "mcpToolCall",
    status: "completed",
    tool: "deepseek_spawn",
    result: { structuredContent: { accepted: true, status: "accepted", jobId: "job_test" } },
  };
  rpc.emit({ method: "item/completed", params: { threadId: "thread_test", turnId: "turn_test", item: base } });
  rpc.emit({
    method: "item/completed",
    params: {
      threadId: "thread_test",
      turnId: "turn_test",
      item: { ...base, server: "other-server", result: { content: [{ type: "text", text: "job_test" }] } },
    },
  });
  rpc.emit({
    method: "item/completed",
    params: {
      threadId: "thread_test",
      turnId: "turn_test",
      item: { ...base, server: "deepseek-subagent", result: { structuredContent: { accepted: true, status: "wrong", jobId: "job_test" } } },
    },
  });
  assert.deepEqual(correlations, []);
  await adapter.close();
});

test("Codex adapter emits one correlation for duplicate completion notifications", async () => {
  const rpc = new FakeRpc();
  const adapter = new CodexAppServerDeliveryAdapter(createDefaultConfig({ codexAppServerCommand: "codex" }), rpc);
  await adapter.start();
  const correlations: unknown[] = [];
  adapter.onCorrelation((value) => correlations.push(value));
  const notification = {
    method: "item/completed",
    params: {
      threadId: "thread_test",
      turnId: "turn_test",
      item: {
        id: "item_test",
        type: "mcpToolCall",
        status: "completed",
        server: "deepseek-subagent",
        tool: "deepseek_spawn",
        result: { structuredContent: { accepted: true, status: "accepted", jobId: "job_test" } },
      },
    },
  };
  rpc.emit(notification);
  rpc.emit(notification);
  rpc.emit({
    method: "item/completed",
    params: {
      threadId: "other_thread",
      turnId: "other_turn",
      item: {
        id: "item_test",
        type: "mcpToolCall",
        status: "completed",
        server: "deepseek-subagent",
        tool: "deepseek_spawn",
        result: { structuredContent: { accepted: true, status: "accepted", jobId: "other_job" } },
      },
    },
  });
  assert.equal(correlations.length, 1);
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

test("Codex WebSocket transport rejects a response without result or error", async () => {
  const previousWebSocket = (globalThis as { WebSocket?: unknown }).WebSocket;
  class FakeWebSocket {
    static latest: FakeWebSocket | null = null;
    readonly readyState = 1;
    private readonly listeners = new Map<string, Array<(event: { data?: unknown }) => void>>();

    constructor(_endpoint: string) {
      FakeWebSocket.latest = this;
      queueMicrotask(() => this.emit("open"));
    }

    addEventListener(type: string, listener: (event: { data?: unknown }) => void): void {
      this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
    }

    removeEventListener(type: string, listener: (event: { data?: unknown }) => void): void {
      this.listeners.set(type, (this.listeners.get(type) ?? []).filter((candidate) => candidate !== listener));
    }

    send(data: string): void {
      const message = JSON.parse(data) as { id?: number; method?: string };
      if (message.method === "initialize" && message.id !== undefined) {
        this.emit("message", { data: JSON.stringify({ id: message.id, result: {} }) });
      } else if (message.id !== undefined) {
        this.emit("message", { data: JSON.stringify({ id: message.id }) });
      }
    }

    close(): void {
      this.emit("close");
    }

    private emit(type: string, event: { data?: unknown } = {}): void {
      for (const listener of this.listeners.get(type) ?? []) listener(event);
    }
  }
  Object.defineProperty(globalThis, "WebSocket", { configurable: true, value: FakeWebSocket });
  try {
    const rpc = new JsonRpcWebSocketClient();
    await rpc.start("ws://127.0.0.1:1", []);
    await assert.rejects(() => rpc.call("turn/start", {}), /response must contain exactly one valid result or error/);
    await rpc.close();
  } finally {
    Object.defineProperty(globalThis, "WebSocket", { configurable: true, value: previousWebSocket });
  }
});
