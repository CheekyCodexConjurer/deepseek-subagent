import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { createDefaultConfig } from "../../src/config.js";
import { InboxDelivery } from "../../src/delivery/inbox.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService, type ManagedOpenCodeLike, type OpenCodeManagerLike } from "../../src/service.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

const execFileAsync = promisify(execFile);

async function git(cwd: string, ...args: string[]): Promise<void> {
  await execFileAsync("git", ["-C", cwd, ...args]);
}

class FakeClient implements OpenCodeClientLike {
  sessionCount = 0;
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  aborted: string[] = [];
  messages: OpenCodeMessage[] = [];
  permissionReplies: Array<{ sessionId: string; permissionId: string; reply: string; message?: string }> = [];
  private onEvent: ((event: OpenCodeEvent) => Promise<void> | void) | null = null;
  private waiters: Array<() => void> = [];

  async health(): Promise<{ healthy: boolean; version?: string }> {
    return { healthy: true, version: "fake" };
  }
  async createSession(): Promise<{ id: string }> {
    this.sessionCount += 1;
    return { id: "session_" + this.sessionCount };
  }
  async promptAsync(sessionId: string, task: string): Promise<void> {
    this.promptCalls.push({ sessionId, task });
  }
  async listMessages(): Promise<OpenCodeMessage[]> {
    return this.messages;
  }
  async getDiff(): Promise<unknown> {
    return [];
  }
  async abort(sessionId: string): Promise<void> {
    this.aborted.push(sessionId);
  }
  async replyPermission(sessionId: string, permissionId: string, reply: "once" | "always" | "reject", message?: string): Promise<void> {
    this.permissionReplies.push({ sessionId, permissionId, reply, ...(message ? { message } : {}) });
  }
  async subscribe(onEvent: (event: OpenCodeEvent) => Promise<void> | void, signal?: AbortSignal): Promise<void> {
    this.onEvent = onEvent;
    await new Promise<void>((resolve) => {
      this.waiters.push(resolve);
      signal?.addEventListener("abort", () => resolve(), { once: true });
    });
  }
  async emit(event: OpenCodeEvent): Promise<void> {
    await this.onEvent?.(event);
  }
}

class FakeManager implements OpenCodeManagerLike {
  constructor(private readonly client: FakeClient) {}
  async start(): Promise<ManagedOpenCodeLike> {
    return {
      serverId: "server_fake",
      baseUrl: "http://127.0.0.1:1",
      client: this.client,
      processId: null,
      stop: async () => undefined,
    };
  }
  async stop(): Promise<void> {}
}

class FakeInbox extends InboxDelivery {
  delivered: string[] = [];
  constructor(directory: string) {
    super(directory, async () => undefined);
  }
  override async deliver(envelope: { jobId: string }, _humanText: string): Promise<string> {
    this.delivered.push(envelope.jobId);
    return "fake://" + envelope.jobId;
  }
}

test("spawn returns after dispatch, completes on idle, deduplicates, and continues same session", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-service-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    opencodeMode: "attach",
    opencodeUrl: "http://127.0.0.1:1",
  });
  const service = new BridgeService(config, {
    store,
    manager: new FakeManager(client),
    inbox,
  });
  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "request_one",
      topic: "Fixture task",
      task: "Inspect the fixture",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(accepted.status, "accepted");
    assert.equal(client.promptCalls.length, 1);
    assert.equal(service.getJob(accepted.jobId)?.status, "running");
    client.messages = [{
      info: { id: "assistant_one", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: Fixture completed\nFILES:\n- notes.txt\nTESTS:\n- unit smoke\nRISKS:\n- none" }],
    }];
    const idle: OpenCodeEvent = {
      type: "session.idle",
      properties: { sessionID: "session_1" },
    };
    await client.emit(idle);
    await client.emit(idle);
    assert.equal(service.getJob(accepted.jobId)?.status, "delivered");
    assert.equal(inbox.delivered.length, 1);
    const continued = await service.continueJob({
      requestId: "request_two",
      agentId: accepted.agentId,
      relation: "review",
      task: "Review the previous result",
    });
    assert.equal(continued.status, "accepted");
    assert.equal(client.promptCalls.length, 2);
    assert.equal(client.promptCalls[1]?.sessionId, "session_1");
    await assert.rejects(() => service.continueJob({
      requestId: "request_three",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "This must wait",
    }), /busy/);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("abort is explicit and does not reuse the OpenCode session", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-abort-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
  const service = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "request_abort",
      topic: "Abort fixture",
      task: "Wait",
      cwd: directory,
    });
    const stopped = await service.abort(accepted.agentId, "test");
    assert.equal(stopped.status, "aborted");
    assert.deepEqual(client.aborted, ["session_1"]);
    assert.equal(service.getJob(accepted.jobId)?.status, "aborted");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("startup reconciliation recovers a running job once without a duplicate delivery", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-recovery-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
  const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
  const first = new BridgeService(config, { store, manager: new FakeManager(client), inbox });
  try {
    await first.start();
    const accepted = await first.spawn({
      requestId: "request_recovery",
      topic: "Recovery fixture",
      task: "Wait for recovery",
      cwd: directory,
    });
    client.messages = [{
      info: { id: "assistant_recovery", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: recovered" }],
    }];
    await first.stop();
    const second = new BridgeService(config, { store, manager: new FakeManager(client), inbox });
    await second.start();
    assert.equal(second.getJob(accepted.jobId)?.status, "delivered");
    assert.equal(inbox.delivered.filter((jobId) => jobId === accepted.jobId).length, 1);
    await second.stop();
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("worktree strategy rejects dirty repositories before creating a worktree from HEAD", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-worktree-"));
  await git(directory, "init", "-q");
  await writeFile(path.join(directory, "tracked.txt"), "initial\n", "utf8");
  await git(directory, "add", "tracked.txt");
  await git(directory, "-c", "user.name=DeepSeek Test", "-c", "user.email=deepseek@example.invalid", "commit", "-qm", "initial");
  await writeFile(path.join(directory, "uncommitted.txt"), "must remain untouched\n", "utf8");

  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
  }), { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await service.start();
    await assert.rejects(() => service.spawn({
      requestId: "request_dirty_worktree",
      topic: "Dirty worktree",
      task: "Inspect without touching local changes",
      cwd: directory,
      mode: "edit",
      workspaceStrategy: "worktree",
    }), /uncommitted changes/i);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("explicit deepseek_continue permission fields reply through the OpenCode permission API", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-permission-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "request_permission",
      topic: "Permission fixture",
      task: "Wait for an explicit permission response",
      cwd: directory,
    });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "per_fixture" } } });
    assert.equal(service.getJob(accepted.jobId)?.status, "needs_approval");
    const continued = await service.continueJob({
      requestId: "request_permission_reply",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Continue after the permission response",
      permissionId: "per_fixture",
      permissionReply: "once",
      permissionMessage: "Approved for this operation.",
    });
    assert.equal(continued.status, "accepted");
    assert.deepEqual(client.permissionReplies, [{
      sessionId: "session_1",
      permissionId: "per_fixture",
      reply: "once",
      message: "Approved for this operation.",
    }]);
    assert.equal(service.getJob(accepted.jobId)?.status, "running");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
