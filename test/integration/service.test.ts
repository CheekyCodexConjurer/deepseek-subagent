import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { createDefaultConfig } from "../../src/config.js";
import type { CodexCorrelation, CodexDeliveryAdapter } from "../../src/codex/adapter.js";
import { InboxDelivery } from "../../src/delivery/inbox.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService, type ManagedOpenCodeLike, type OpenCodeManagerLike } from "../../src/service.js";
import type { CodexBinding, JobRecord, OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage, ResultEnvelope } from "../../src/types.js";

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

class BlockingInbox extends FakeInbox {
  private readonly startedPromise: Promise<void>;
  private readonly releasePromise: Promise<void>;
  private resolveStarted!: () => void;
  private resolveRelease!: () => void;

  constructor(directory: string) {
    super(directory);
    this.startedPromise = new Promise<void>((resolve) => {
      this.resolveStarted = resolve;
    });
    this.releasePromise = new Promise<void>((resolve) => {
      this.resolveRelease = resolve;
    });
  }

  override async deliver(envelope: { jobId: string }, humanText: string): Promise<string> {
    this.resolveStarted();
    await this.releasePromise;
    return super.deliver(envelope, humanText);
  }

  async waitUntilStarted(): Promise<void> {
    return this.startedPromise;
  }

  release(): void {
    this.resolveRelease();
  }
}

class FakeCodex implements CodexDeliveryAdapter {
  readonly available = true;
  readonly reason = null;
  readonly delivered: Array<{ jobId: string; threadId: string }> = [];
  private listener: ((correlation: CodexCorrelation) => void) | null = null;

  async start(): Promise<void> {}
  async close(): Promise<void> {}
  async deliver(job: JobRecord, binding: CodexBinding, _text: string): Promise<"codex-steer" | "codex-start"> {
    this.delivered.push({ jobId: job.id, threadId: binding.threadId });
    return "codex-start";
  }
  onCorrelation(listener: (correlation: CodexCorrelation) => void): () => void {
    this.listener = listener;
    return () => {
      this.listener = null;
    };
  }
  emit(correlation: CodexCorrelation): void {
    this.listener?.(correlation);
  }
}

class BlockingCodex extends FakeCodex {
  readonly startedJobs: string[] = [];
  active = 0;
  maxActive = 0;
  private readonly startedPromise: Promise<void>;
  private readonly releasePromise: Promise<void>;
  private resolveStarted!: () => void;
  private resolveRelease!: () => void;

  constructor() {
    super();
    this.startedPromise = new Promise<void>((resolve) => {
      this.resolveStarted = resolve;
    });
    this.releasePromise = new Promise<void>((resolve) => {
      this.resolveRelease = resolve;
    });
  }

  override async deliver(job: JobRecord, binding: CodexBinding, text: string): Promise<"codex-steer" | "codex-start"> {
    this.startedJobs.push(job.id);
    this.active += 1;
    this.maxActive = Math.max(this.maxActive, this.active);
    this.resolveStarted();
    try {
      await this.releasePromise;
      return await super.deliver(job, binding, text);
    } finally {
      this.active -= 1;
    }
  }

  async waitUntilStarted(): Promise<void> {
    return this.startedPromise;
  }

  release(): void {
    this.resolveRelease();
  }
}

function deliveryEnvelope(job: JobRecord): ResultEnvelope {
  return {
    version: 1,
    agentId: job.agentId,
    jobId: job.id,
    topic: job.id,
    status: "completed",
    opencodeSessionId: "session_delivery",
    model: "opencode-go/deepseek-v4-flash · max",
    modelDisplayName: "DeepSeek V4 Flash (max)",
    workspace: "E:/Repositories/deepseek-subagent",
    summary: "delivery fixture",
    files: [],
    tests: [],
    risks: [],
    diffSummary: "none",
    fullResultPath: "fixture.json",
    orchestratorInstruction: "fixture",
  };
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

test("completed result waits for a late Codex correlation before using inbox", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-correlation-window-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
  const codex = new FakeCodex();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox,
    codex,
  });
  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "request_correlation_window",
      topic: "Correlation window fixture",
      task: "Wait for the Codex item correlation",
      cwd: directory,
    });
    client.messages = [{
      info: { id: "assistant_window", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: correlation window" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    await new Promise((resolve) => setTimeout(resolve, 25));
    assert.equal(service.getJob(accepted.jobId)?.status, "delivery_pending");
    assert.deepEqual(inbox.delivered, []);
    codex.emit({ jobId: accepted.jobId, threadId: "thread_late", turnId: "turn_late", itemId: "item_late" });
    await new Promise((resolve) => setTimeout(resolve, 25));
    assert.deepEqual(codex.delivered, [{ jobId: accepted.jobId, threadId: "thread_late" }]);
    assert.equal(service.getJob(accepted.jobId)?.status, "delivered");
    assert.equal(store.getBinding(accepted.jobId)?.originatingItemId, "item_late");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("serializes a late correlation against an in-flight inbox fallback", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-correlation-race-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new BlockingInbox(directory);
  const codex = new FakeCodex();
  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    codexCorrelationWindowMs: 25,
  }), {
    store,
    manager: new FakeManager(client),
    inbox,
    codex,
  });
  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "request_correlation_race",
      topic: "Correlation race fixture",
      task: "Exercise one delivery channel",
      cwd: directory,
    });
    client.messages = [{
      info: { id: "assistant_race", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: correlation race" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    await inbox.waitUntilStarted();
    codex.emit({ jobId: accepted.jobId, threadId: "thread_race", turnId: "turn_race", itemId: "item_race" });
    inbox.release();
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(inbox.delivered, [accepted.jobId]);
    assert.deepEqual(codex.delivered, []);
    assert.equal(service.getJob(accepted.jobId)?.status, "delivered");
  } finally {
    inbox.release();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("serializes two jobs that converge on one Codex thread after a late binding", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-correlation-thread-race-"));
  const store = await BridgeStore.open(directory);
  const codex = new BlockingCodex();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(new FakeClient()),
    inbox: new FakeInbox(directory),
    codex,
  });
  const internal = service as unknown as {
    deliverEnvelope(envelope: ResultEnvelope, job: JobRecord): Promise<void>;
    deliveryLocks: Map<string, Promise<void>>;
  };
  try {
    for (const [agentId, sessionId] of [["agent_thread_one", "session_thread_one"], ["agent_thread_two", "session_thread_two"]]) {
      store.createAgent({
        id: agentId,
        title: agentId,
        topic: "Thread race fixture",
        repositoryRoot: directory,
        workspacePath: directory,
        workspaceStrategy: "shared",
        opencodeServerId: "server_" + agentId,
        opencodeSessionId: sessionId,
        modelProviderId: "opencode-go",
        modelId: "deepseek-v4-flash",
        modelVariant: "max",
      });
    }
    const first = store.createJob({
      id: "job_thread_one",
      agentId: "agent_thread_one",
      kind: "spawn",
      requestId: "request_thread_one",
      promptHash: "hash_thread_one",
    });
    const second = store.createJob({
      id: "job_thread_two",
      agentId: "agent_thread_two",
      kind: "spawn",
      requestId: "request_thread_two",
      promptHash: "hash_thread_two",
    });
    for (const job of [first, second]) {
      store.updateJobStatus(job.id, "dispatching");
      store.updateJobStatus(job.id, "running");
      store.updateJobStatus(job.id, "completed");
      store.updateJobStatus(job.id, "delivery_pending");
    }

    let releaseSeed!: () => void;
    const seed = new Promise<void>((resolve) => {
      releaseSeed = resolve;
    });
    internal.deliveryLocks.set("job:" + first.id, seed);
    const firstDelivery = internal.deliverEnvelope(deliveryEnvelope(first), first);

    store.bindJob({
      jobId: first.id,
      threadId: "thread_shared",
      originatingTurnId: "turn_one",
      originatingItemId: "item_one",
    });
    store.bindJob({
      jobId: second.id,
      threadId: "thread_shared",
      originatingTurnId: "turn_two",
      originatingItemId: "item_two",
    });
    const secondDelivery = internal.deliverEnvelope(deliveryEnvelope(second), second);
    await codex.waitUntilStarted();

    releaseSeed();
    await Promise.resolve();
    assert.deepEqual(codex.startedJobs, [second.id]);
    assert.equal(codex.maxActive, 1);

    codex.release();
    await Promise.all([firstDelivery, secondDelivery]);
    assert.deepEqual(codex.startedJobs, [second.id, first.id]);
    assert.equal(codex.maxActive, 1);
  } finally {
    codex.release();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
