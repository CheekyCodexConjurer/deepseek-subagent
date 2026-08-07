import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { execFile } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { createDefaultConfig } from "../../src/config.js";
import type { CodexCorrelation, CodexDeliveryAdapter } from "../../src/codex/adapter.js";
import { InboxDelivery } from "../../src/delivery/inbox.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeBusyError, BridgeService, FollowCancelledError, type ManagedOpenCodeLike, type OpenCodeManagerLike } from "../../src/service.js";
import type { CodexBinding, JobRecord, OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage, ResultEnvelope } from "../../src/types.js";

const execFileAsync = promisify(execFile);

async function git(cwd: string, ...args: string[]): Promise<void> {
  await execFileAsync("git", ["-C", cwd, ...args]);
}

class FakeClient implements OpenCodeClientLike {
  sessionCount = 0;
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  promptErrors: Array<Error | null> = [];
  aborted: string[] = [];
  abortCalls = 0;
  abortError: Error | null = null;
  messages: OpenCodeMessage[] = [];
  listMessagesError: Error | null = null;
  listMessagesCalls = 0;
  diffCalls = 0;
  permissionReplies: Array<{ sessionId: string; permissionId: string; reply: string; message?: string }> = [];
  replyErrors: Array<Error | null> = [];
  private promptGate: Promise<void> | null = null;
  private releasePromptGate: (() => void) | null = null;
  private abortGate: Promise<void> | null = null;
  private releaseAbortGate: (() => void) | null = null;
  private replyGate: Promise<void> | null = null;
  private releaseReplyGate: (() => void) | null = null;
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
    const error = this.promptErrors.shift();
    if (this.promptGate) await this.promptGate;
    if (error) throw error;
  }
  blockPrompt(): void {
    this.promptGate = new Promise<void>((resolve) => {
      this.releasePromptGate = resolve;
    });
  }
  releasePrompt(): void {
    this.releasePromptGate?.();
    this.releasePromptGate = null;
    this.promptGate = null;
  }
  async listMessages(): Promise<OpenCodeMessage[]> {
    this.listMessagesCalls += 1;
    const error = this.listMessagesError;
    this.listMessagesError = null;
    if (error) throw error;
    return this.messages;
  }
  async getDiff(): Promise<unknown> {
    this.diffCalls += 1;
    return [];
  }
  async abort(sessionId: string): Promise<void> {
    this.abortCalls += 1;
    if (this.abortGate) await this.abortGate;
    if (this.abortError) throw this.abortError;
    this.aborted.push(sessionId);
  }
  blockAbort(): void {
    this.abortGate = new Promise<void>((resolve) => {
      this.releaseAbortGate = resolve;
    });
  }
  releaseAbort(): void {
    this.releaseAbortGate?.();
    this.releaseAbortGate = null;
    this.abortGate = null;
  }
  async replyPermission(sessionId: string, permissionId: string, reply: "once" | "always" | "reject", message?: string): Promise<void> {
    this.permissionReplies.push({ sessionId, permissionId, reply, ...(message ? { message } : {}) });
    if (this.replyGate) await this.replyGate;
    const error = this.replyErrors.shift();
    if (error) throw error;
  }
  blockReply(): void {
    this.replyGate = new Promise<void>((resolve) => {
      this.releaseReplyGate = resolve;
    });
  }
  releaseReply(): void {
    this.releaseReplyGate?.();
    this.releaseReplyGate = null;
    this.replyGate = null;
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
  notices: string[] = [];
  constructor(directory: string) {
    super(directory, async () => undefined);
  }
  override async deliver(envelope: { jobId: string }, _humanText: string): Promise<string> {
    this.delivered.push(envelope.jobId);
    return "fake://" + envelope.jobId;
  }
  override async writeNotice(notice: { jobId: string; kind: string; agentId: string; topic: string; message: string; permissionId?: string | null }): Promise<string> {
    this.notices.push(notice.jobId + ":" + notice.kind);
    return super.writeNotice(notice);
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
  startCalls = 0;
  closeCalls = 0;
  private listener: ((correlation: CodexCorrelation) => void) | null = null;

  async start(): Promise<void> { this.startCalls += 1; }
  async close(): Promise<void> { this.closeCalls += 1; }
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

async function waitForCondition(condition: () => boolean, timeoutMs = 1_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition() && Date.now() < deadline) await delay(5);
  assert.equal(condition(), true, "condition did not become true before timeout");
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

test("follow during a pending dispatch preserves the following state", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-dispatch-race-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  client.blockPrompt();
  try {
    await service.start();
    const spawnPromise = service.spawn({
      requestId: "request_follow_dispatch_race",
      topic: "Follow dispatch race",
      task: "Wait for a pending dispatch",
      cwd: directory,
    });
    await waitForCondition(() => store.listJobs()[0]?.status === "dispatching");
    const agent = store.listAgents()[0];
    const job = store.listJobs()[0];
    assert.ok(agent);
    assert.ok(job);
    const followPromise = service.follow({ agentId: agent.id, jobId: job.id, waitMinutes: 1, graceMinutes: 1 });
    await waitForCondition(() => service.getJob(job.id)?.status === "following");
    client.releasePrompt();
    const accepted = await spawnPromise;
    assert.equal(accepted.jobId, job.id);
    assert.equal(service.getJob(job.id)?.status, "following");
    client.messages = [{
      info: { id: "assistant_follow_dispatch", role: "assistant", sessionID: agent.opencodeSessionId },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: dispatch race resolved" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: agent.opencodeSessionId } });
    assert.equal((await followPromise).status, "completed");
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("approval during a pending dispatch preserves needs_approval", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-approval-dispatch-race-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  client.blockPrompt();
  try {
    await service.start();
    const spawnPromise = service.spawn({
      requestId: "request_approval_dispatch_race",
      topic: "Approval dispatch race",
      task: "Wait for approval during dispatch",
      cwd: directory,
    });
    await waitForCondition(() => store.listJobs()[0]?.status === "dispatching");
    const agent = store.listAgents()[0];
    const job = store.listJobs()[0];
    assert.ok(agent);
    assert.ok(job);
    await client.emit({ type: "permission.asked", properties: { sessionID: agent.opencodeSessionId, permission: { id: "permission_dispatch" } } });
    assert.equal(service.getJob(job.id)?.status, "needs_approval");
    assert.equal(service.getAgent(agent.id)?.status, "needs_approval");
    client.releasePrompt();
    await spawnPromise;
    assert.equal(service.getJob(job.id)?.status, "needs_approval");
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("dispatch rejection resolves a follow waiter instead of leaving it pending", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-dispatch-error-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  client.blockPrompt();
  client.promptErrors.push(new Error("dispatch rejected"));
  try {
    await service.start();
    const spawn = service.spawn({ requestId: "request_follow_dispatch_error", topic: "Follow dispatch error", task: "Fail dispatch", cwd: directory });
    await waitForCondition(() => store.listJobs()[0]?.status === "dispatching");
    const agent = store.listAgents()[0];
    const job = store.listJobs()[0];
    assert.ok(agent);
    assert.ok(job);
    const follow = service.follow({ agentId: agent.id, jobId: job.id, waitMinutes: 1, graceMinutes: 1 });
    client.releasePrompt();
    await assert.rejects(spawn, /dispatch rejected/);
    assert.equal((await follow).status, "failed");
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("late dispatch rejection preserves a permission request", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-approval-dispatch-error-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  client.blockPrompt();
  client.promptErrors.push(new Error("dispatch failed after approval"));
  try {
    await service.start();
    const spawn = service.spawn({ requestId: "request_approval_dispatch_error", topic: "Approval dispatch error", task: "Fail after approval", cwd: directory });
    await waitForCondition(() => store.listJobs()[0]?.status === "dispatching");
    const agent = store.listAgents()[0];
    const job = store.listJobs()[0];
    assert.ok(agent);
    assert.ok(job);
    await client.emit({ type: "permission.asked", properties: { sessionID: agent.opencodeSessionId, permission: { id: "permission_late_dispatch" } } });
    client.releasePrompt();
    await assert.rejects(spawn, /dispatch failed after approval/);
    assert.equal(service.getJob(job.id)?.status, "needs_approval");
    assert.equal(service.getAgent(agent.id)?.status, "needs_approval");
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("same request id creates one spawn under concurrent retries", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-spawn-request-race-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  client.blockPrompt();
  try {
    await service.start();
    const input = { requestId: "request_spawn_same", topic: "Concurrent spawn", task: "Create one session", cwd: directory };
    const first = service.spawn(input);
    await waitForCondition(() => store.listJobs().length === 1);
    const second = service.spawn(input);
    assert.equal(client.sessionCount, 1);
    client.releasePrompt();
    const [firstAccepted, secondAccepted] = await Promise.all([first, second]);
    assert.equal(firstAccepted.jobId, secondAccepted.jobId);
    assert.equal(store.listAgents().length, 1);
    assert.equal(store.listJobs().length, 1);
    assert.equal(client.promptCalls.length, 1);
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("identical idle events are deduplicated per job, not per session", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-event-scope-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const first = await service.spawn({ requestId: "request_event_scope_first", topic: "Event scope", task: "Complete first turn", cwd: directory });
    client.messages = [{
      info: { id: "assistant_event_scope", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: first turn" }],
    }];
    const idle: OpenCodeEvent = { type: "session.idle", properties: { sessionID: "session_1" } };
    await client.emit(idle);
    const second = await service.continueJob({ requestId: "request_event_scope_second", agentId: first.agentId, relation: "continuation", task: "Complete second turn" });
    client.messages = [{
      info: { id: "assistant_event_scope_second", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: second turn" }],
    }];
    await client.emit(idle);
    assert.equal(service.getJob(second.jobId)?.status, "delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("session idle is retried after a transient reconciliation failure", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-event-retry-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_event_retry", topic: "Event retry", task: "Recover after a transient read error", cwd: directory });
    client.messages = [{
      info: { id: "assistant_event_retry", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: retry completed" }],
    }];
    client.listMessagesError = new Error("temporary message read failure");
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    await waitForCondition(() => service.getJob(accepted.jobId)?.status === "delivered", 2_000);
    assert.equal(client.listMessagesCalls, 2);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("event ledger admission failures use the bounded retry path", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-event-ledger-retry-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  let attempts = 0;
  const insertEvent = store.insertEvent.bind(store);
  store.insertEvent = ((input) => {
    attempts += 1;
    if (attempts === 1) throw new Error("temporary event ledger failure");
    return insertEvent(input);
  }) as BridgeStore["insertEvent"];
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_event_ledger_retry", topic: "Event ledger retry", task: "Retry event admission", cwd: directory });
    client.messages = [{
      info: { id: "assistant_event_ledger_retry", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: ledger retry completed" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    await waitForCondition(() => service.getJob(accepted.jobId)?.status === "delivered", 2_000);
    assert.equal(attempts, 2);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("serializes concurrent continuations and rejects the second as busy", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-continue-race-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_continue_seed", topic: "Continue race", task: "Seed the session", cwd: directory });
    client.messages = [{
      info: { id: "assistant_continue_seed", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: seed" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    client.blockPrompt();
    const first = service.continueJob({ requestId: "request_continue_one", agentId: accepted.agentId, relation: "continuation", task: "First continuation" });
    await waitForCondition(() => client.promptCalls.length === 2);
    const second = service.continueJob({ requestId: "request_continue_two", agentId: accepted.agentId, relation: "continuation", task: "Second continuation" });
    client.releasePrompt();
    const results = await Promise.allSettled([first, second]);
    assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
    const rejected = results.find((result) => result.status === "rejected");
    assert.ok(rejected && rejected.status === "rejected");
    assert.ok(rejected.reason instanceof BridgeBusyError);
    assert.equal(store.listJobs().filter((job) => job.agentId === accepted.agentId && ["dispatching", "running"].includes(job.status)).length, 1);
  } finally {
    client.releasePrompt();
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

test("restart does not reconcile a continuation from the prior assistant message", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-continuation-recovery-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
  const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
  const first = new BridgeService(config, { store, manager: new FakeManager(client), inbox });
  try {
    await first.start();
    const seed = await first.spawn({ requestId: "request_continuation_recovery_seed", topic: "Continuation recovery", task: "Finish the seed turn", cwd: directory });
    client.messages = [{
      info: { id: "assistant_continuation_previous", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: previous turn" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    const continued = await first.continueJob({
      requestId: "request_continuation_recovery_next",
      agentId: seed.agentId,
      relation: "continuation",
      task: "Start a new turn",
    });
    assert.equal(first.getJob(continued.jobId)?.lastAssistantMessageId, "assistant_continuation_previous");
    await first.stop();

    const second = new BridgeService(config, { store, manager: new FakeManager(client), inbox });
    try {
      await second.start();
      assert.equal(second.getJob(continued.jobId)?.status, "running");
      assert.equal(inbox.delivered.filter((jobId) => jobId === continued.jobId).length, 0);
      client.messages = [{
        info: { id: "assistant_continuation_current", role: "assistant", sessionID: "session_1" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: current turn" }],
      }];
      await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
      assert.equal(second.getJob(continued.jobId)?.status, "delivered");
    } finally {
      await second.stop();
    }
  } finally {
    await first.stop();
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
    const approvalDeadline = service.getJob(accepted.jobId)?.approvalDeadlineAt;
    assert.ok(approvalDeadline);
    await assert.rejects(() => service.continueJob({
      requestId: "request_permission_invalid",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Do not clear the approval timer",
      permissionId: "per_fixture",
    }), /both required/);
    assert.equal(service.getJob(accepted.jobId)?.approvalDeadlineAt, approvalDeadline);
    await assert.rejects(() => service.continueJob({
      requestId: "request_permission_message_only",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Do not resume from message only",
      permissionMessage: "Only a message was supplied",
    }), /both required/);
    assert.equal(service.getJob(accepted.jobId)?.approvalDeadlineAt, approvalDeadline);
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

test("approval continuation failures do not leave a falsely working job", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-permission-failure-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const resumed = await service.spawn({ requestId: "request_resume_failure", topic: "Resume failure", task: "Wait for approval", cwd: directory });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_resume_failure" } } });
    client.promptErrors.push(new Error("resume rejected"));
    await assert.rejects(() => service.continueJob({
      requestId: "request_resume_failure_continue",
      agentId: resumed.agentId,
      relation: "continuation",
      task: "Resume and fail",
    }), /resume rejected/);
    assert.equal(service.getJob(resumed.jobId)?.status, "failed");

    const replied = await service.spawn({ requestId: "request_reply_failure", topic: "Reply failure", task: "Wait for another approval", cwd: directory });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_2", permission: { id: "permission_reply_failure" } } });
    client.replyErrors.push(new Error("permission reply rejected"));
    await assert.rejects(() => service.continueJob({
      requestId: "request_reply_failure_continue",
      agentId: replied.agentId,
      relation: "continuation",
      task: "Reply and fail",
      permissionId: "permission_reply_failure",
      permissionReply: "once",
    }), /permission reply rejected/);
    assert.equal(service.getJob(replied.jobId)?.status, "failed");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("permission.replied is not treated as a new approval request", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-permission-replied-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox,
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_permission_replied", topic: "Permission replied", task: "Ignore response events", cwd: directory });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_replied" } } });
    await service.continueJob({
      requestId: "request_permission_replied_answer",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Answer the approval",
      permissionId: "permission_replied",
      permissionReply: "once",
    });
    await client.emit({ type: "permission.replied", properties: { sessionID: "session_1", permission: { id: "permission_replied" } } });
    assert.equal(service.getJob(accepted.jobId)?.status, "running");
    assert.equal(service.getJob(accepted.jobId)?.permissionId, null);
    assert.deepEqual(inbox.notices, [accepted.jobId + ":needs_approval"]);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("distinct approval requests get distinct notices and restart stays idempotent", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-approval-generations-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
  const firstInbox = new FakeInbox(directory);
  const first = new BridgeService(config, { store, manager: new FakeManager(client), inbox: firstInbox });
  try {
    await first.start();
    const accepted = await first.spawn({ requestId: "request_approval_generations", topic: "Approval generations", task: "Handle two approvals", cwd: directory });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_generation_one" } } });
    await first.continueJob({
      requestId: "request_approval_generation_one_reply",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Approve the first request",
      permissionId: "permission_generation_one",
      permissionReply: "once",
    });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_generation_two" } } });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_generation_two" } } });
    assert.deepEqual(firstInbox.notices, [accepted.jobId + ":needs_approval", accepted.jobId + ":needs_approval"]);
    assert.equal(await firstInbox.noticeExists(accepted.jobId, "needs_approval", "permission_generation_one"), true);
    assert.equal(await firstInbox.noticeExists(accepted.jobId, "needs_approval", "permission_generation_two"), true);
    await first.stop();

    const secondInbox = new FakeInbox(directory);
    const second = new BridgeService(config, { store, manager: new FakeManager(client), inbox: secondInbox });
    try {
      await second.start();
      assert.deepEqual(secondInbox.notices, []);
    } finally {
      await second.stop();
    }
  } finally {
    await first.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("a second approval arriving during reply is preserved", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-approval-rpc-race-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox,
  });
  client.blockReply();
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_approval_rpc_race", topic: "Approval RPC race", task: "Preserve a second approval", cwd: directory });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_rpc_one" } } });
    const reply = service.continueJob({
      requestId: "request_approval_rpc_race_reply",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Reply to the first approval",
      permissionId: "permission_rpc_one",
      permissionReply: "once",
    });
    await waitForCondition(() => client.permissionReplies.length === 1);
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_rpc_two" } } });
    assert.equal(service.getJob(accepted.jobId)?.status, "needs_approval");
    assert.equal(service.getJob(accepted.jobId)?.permissionId, "permission_rpc_two");
    client.releaseReply();
    await reply;
    assert.equal(service.getJob(accepted.jobId)?.status, "needs_approval");
    assert.equal(service.getJob(accepted.jobId)?.permissionId, "permission_rpc_two");
    assert.deepEqual(inbox.notices, [accepted.jobId + ":needs_approval", accepted.jobId + ":needs_approval"]);
  } finally {
    client.releaseReply();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("recover_result returns the allowlisted persisted message projection", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-recover-projection-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_recover_projection", topic: "Recovery projection", task: "Persist visible output", cwd: directory });
    client.messages = [{
      info: { id: "assistant_recover_projection", role: "assistant", sessionID: "session_1" },
      parts: [
        { type: "reasoning", text: "private recovery reasoning" },
        { type: "tool", text: "private recovery tool payload" },
        { type: "text", text: "STATUS: completed\nSUMMARY: visible recovery result" },
      ],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    const recovered = await service.recoverResult(accepted.jobId);
    const serialized = JSON.stringify(recovered);
    assert.doesNotMatch(serialized, /private recovery reasoning|private recovery tool payload/);
    assert.match(serialized, /visible recovery result/);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("recover_result sanitizes a legacy result before returning it", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-recover-legacy-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_recover_legacy", topic: "Legacy recovery", task: "Read a legacy result", cwd: directory });
    const resultPath = path.join(directory, "results", accepted.jobId + ".json");
    await mkdir(path.dirname(resultPath), { recursive: true });
    await writeFile(resultPath, JSON.stringify({
      envelope: { version: 1, jobId: accepted.jobId, status: "completed", summary: "legacy visible" },
      rawAssistantText: "legacy visible text",
      messages: [{
        info: { id: "legacy_assistant", role: "assistant" },
        parts: [
          { type: "reasoning", text: "legacy private reasoning" },
          { type: "tool", hiddenPayload: "legacy tool payload" },
          { type: "text", text: "legacy visible text" },
        ],
      }],
      diff: { hiddenPayload: "legacy tool payload", file: "src/example.ts" },
    }, null, 2));
    store.setJobResult(accepted.jobId, resultPath, "legacy visible");
    const recovered = await service.recoverResult(accepted.jobId);
    const serialized = JSON.stringify(recovered);
    assert.doesNotMatch(serialized, /legacy private reasoning|legacy tool payload|hiddenPayload/);
    assert.match(serialized, /legacy visible text/);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("automatic delivery sanitizes a legacy envelope before writing inbox", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-deliver-legacy-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new InboxDelivery(directory, async () => undefined),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_deliver_legacy", topic: "Legacy delivery", task: "Deliver a legacy result safely", cwd: directory });
    const resultPath = path.join(directory, "results", accepted.jobId + ".json");
    await mkdir(path.dirname(resultPath), { recursive: true });
    await writeFile(resultPath, JSON.stringify({
      envelope: {
        version: 1,
        agentId: accepted.agentId,
        jobId: accepted.jobId,
        topic: "Legacy delivery",
        status: "completed",
        opencodeSessionId: "session_1",
        model: "opencode-go/deepseek-v4-flash · max",
        modelDisplayName: "DeepSeek V4 Flash · Max",
        workspace: directory,
        summary: "token=legacy-secret",
        files: [],
        tests: [],
        risks: [],
        diffSummary: "token=legacy-secret",
        fullResultPath: resultPath,
        orchestratorInstruction: "fixture",
      },
    }, null, 2));
    store.setJobResult(accepted.jobId, resultPath, "token=legacy-secret");
    store.updateJobStatus(accepted.jobId, "completed");
    await service.deliverJob(accepted.jobId);
    const inboxContent = await readFile(path.join(directory, "inbox", accepted.jobId + ".json"), "utf8");
    assert.doesNotMatch(inboxContent, /legacy-secret/);
    assert.match(inboxContent, /\[REDACTED\]/);
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
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json"), experimentalSameChatDelivery: true }), {
    store,
    manager: new FakeManager(client),
    inbox,
    codex,
  });
  try {
    await service.start();
    assert.equal(codex.startCalls, 1);
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
    experimentalSameChatDelivery: true,
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
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json"), experimentalSameChatDelivery: true }), {
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

test("consult returns one immediate observable snapshot with bounded activity", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-consult-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_consult", topic: "Observable progress", task: "Inspect the fixture", cwd: directory });
    const snapshot = await service.consult({ agentId: accepted.agentId, activityLimit: 1 });
    assert.equal(snapshot.agentId, accepted.agentId);
    assert.equal(snapshot.jobId, accepted.jobId);
    assert.equal(snapshot.status, "running");
    assert.equal(snapshot.recentActivity.length, 1);
    assert.match(snapshot.currentActivity, /OpenCode|task/i);
    assert.doesNotMatch(JSON.stringify(snapshot), /reasoning|chain.of.thought|private/i);
    await assert.rejects(() => service.consult({ agentId: accepted.agentId, activityLimit: 21 }), /between 1 and 20/);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("follow waits on session.idle without polling and returns the persisted result", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-event-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    followDefaultWaitMinutes: 12,
    followDefaultGraceMinutes: 4,
  }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_follow_event", topic: "Event follow", task: "Wait for completion", cwd: directory });
    const follow = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    await delay(25);
    assert.equal(client.listMessagesCalls, 0);
    assert.equal(client.diffCalls, 0);
    const following = service.getJob(accepted.jobId);
    assert.equal(following?.status, "following");
    assert.ok(Math.abs(Date.parse(following?.followDeadlineAt ?? "") - Date.parse(following?.followStartedAt ?? "") - 12 * 60_000) < 1_000);
    client.messages = [{
      info: { id: "assistant_follow_event", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: event-driven follow" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    const result = await follow;
    assert.equal(result.status, "completed");
    assert.equal(result.resultAvailable, true);
    assert.equal(result.result?.envelope.summary, "event-driven follow");
    assert.equal(client.listMessagesCalls, 1);
    assert.equal(client.diffCalls, 1);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("follow deadline uses the same session for graceful finalization and returns completed_partial", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-partial-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number): { promise: Promise<{ status: string; deadlineReached: boolean; gracefulFinalize: boolean; partial: boolean }> };
  };
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_follow_partial", topic: "Partial follow", task: "Run until the controlled deadline", cwd: directory });
    const job = service.getJob(accepted.jobId);
    assert.ok(job);
    const lifecycle = internal.ensureFollowLifecycle(job, 0, 0.02);
    await waitForCondition(() => client.promptCalls.some((call) => call.task.includes("Pare de expandir")));
    client.messages = [{
      info: { id: "assistant_follow_partial", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: partial after deadline" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    const result = await lifecycle.promise;
    assert.equal(result.status, "completed_partial");
    assert.equal(result.deadlineReached, true);
    assert.equal(result.gracefulFinalize, true);
    assert.equal(result.partial, true);
    assert.deepEqual(client.promptCalls.filter((call) => call.task.includes("Pare de expandir")).map((call) => call.sessionId), ["session_1"]);
    assert.equal(service.getJob(accepted.jobId)?.status, "delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("busy graceful finalization aborts only the active turn and resubmits to the same session", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-busy-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number): { promise: Promise<{ status: string }> };
  };
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_follow_busy", topic: "Busy finalize", task: "Run until busy finalization", cwd: directory });
    client.promptErrors.push(new Error("HTTP 409 conflict: session busy"));
    const job = service.getJob(accepted.jobId);
    assert.ok(job);
    const lifecycle = internal.ensureFollowLifecycle(job, 0, 0.05);
    await waitForCondition(() => client.aborted.length === 1);
    assert.equal(client.aborted[0], "session_1");
    assert.deepEqual(client.promptCalls.slice(-2).map((call) => call.sessionId), ["session_1", "session_1"]);
    client.messages = [{
      info: { id: "assistant_follow_busy", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: busy recovery" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    assert.equal((await lifecycle.promise).status, "completed_partial");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("follow grace timeout aborts the worker and returns partial timeout evidence", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-timeout-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number): { promise: Promise<{ status: string; workerAborted: boolean; resultAvailable: boolean }> };
  };
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_follow_timeout", topic: "Timeout follow", task: "Run until timeout", cwd: directory });
    const job = service.getJob(accepted.jobId);
    assert.ok(job);
    const lifecycle = internal.ensureFollowLifecycle(job, 0, 0.001);
    const result = await lifecycle.promise;
    assert.equal(result.status, "timed_out");
    assert.equal(result.workerAborted, true);
    assert.equal(result.resultAvailable, true);
    assert.deepEqual(client.aborted, ["session_1"]);
    assert.equal(JSON.parse(await readFile(service.getJob(accepted.jobId)?.resultPath ?? "", "utf8")).envelope.status, "timed_out");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("restart recovers a timed-out job when initial evidence capture failed", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-timeout-recovery-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
  const first = new BridgeService(config, {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = first as unknown as {
    ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number): { promise: Promise<{ status: string; resultAvailable: boolean }> };
  };
  try {
    await first.start();
    const accepted = await first.spawn({ requestId: "request_follow_timeout_recovery", topic: "Timeout recovery", task: "Recover timeout evidence", cwd: directory });
    const job = first.getJob(accepted.jobId);
    assert.ok(job);
    client.listMessagesError = new Error("transient evidence read failure");
    const lifecycle = internal.ensureFollowLifecycle(job, 0, 0.001);
    const result = await lifecycle.promise;
    assert.equal(result.status, "timed_out");
    assert.equal(result.resultAvailable, false);
    assert.equal(first.getJob(accepted.jobId)?.resultPath, null);
    await first.stop();

    client.messages = [{
      info: { id: "assistant_timeout_recovery", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: recovered timeout evidence" }],
    }];
    const secondInbox = new FakeInbox(directory);
    const second = new BridgeService(config, {
      store,
      manager: new FakeManager(client),
      inbox: secondInbox,
    });
    try {
      await second.start();
      assert.ok(second.getJob(accepted.jobId)?.resultPath);
      assert.equal(second.getJob(accepted.jobId)?.status, "delivered");
      assert.deepEqual(secondInbox.delivered, [accepted.jobId]);
    } finally {
      await second.stop();
    }
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("cancelling follow removes only the waiter and does not abort DeepSeek", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-cancel-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_follow_cancel", topic: "Cancel follow", task: "Keep working after waiter cancellation", cwd: directory });
    const controller = new AbortController();
    const follow = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 1, graceMinutes: 1 }, controller.signal);
    controller.abort();
    await assert.rejects(follow, (error: unknown) => error instanceof FollowCancelledError);
    assert.deepEqual(client.aborted, []);
    client.messages = [{
      info: { id: "assistant_follow_cancel", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: worker continued" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    assert.equal(service.getJob(accepted.jobId)?.status, "delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("approval and session errors resolve follow immediately", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-terminal-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const approved = await service.spawn({ requestId: "request_follow_approval", topic: "Approval follow", task: "Wait for approval", cwd: directory });
    const approvalFollow = service.follow({ agentId: approved.agentId, jobId: approved.jobId, waitMinutes: 1, graceMinutes: 1 });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_follow" } } });
    const approval = await approvalFollow;
    assert.equal(approval.status, "needs_approval");
    assert.equal(approval.permissionId, "permission_follow");

    const failed = await service.spawn({ requestId: "request_follow_error", topic: "Error follow", task: "Wait for an error", cwd: directory });
    const errorFollow = service.follow({ agentId: failed.agentId, jobId: failed.jobId, waitMinutes: 1, graceMinutes: 1 });
    await client.emit({ type: "session.error", properties: { sessionID: "session_2", error: "controlled failure" } });
    const error = await errorFollow;
    assert.equal(error.status, "failed");
    assert.match(error.error ?? "", /controlled|terminal|OpenCode/i);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("approval continuation wins over an in-flight timeout abort", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-approval-timeout-race-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    expireApproval(agentId: string, jobId: string): Promise<void>;
  };
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_approval_timeout_race", topic: "Approval timeout race", task: "Wait for approval", cwd: directory });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_timeout_race" } } });
    store.setApprovalDeadline(accepted.jobId, new Date(Date.now() - 1).toISOString());
    client.blockAbort();
    const expiration = internal.expireApproval(accepted.agentId, accepted.jobId);
    await waitForCondition(() => client.abortCalls === 1);
    await service.continueJob({
      requestId: "request_approval_timeout_race_continue",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Approve before the stale timeout can fail the job",
      permissionId: "permission_timeout_race",
      permissionReply: "once",
    });
    client.releaseAbort();
    await expiration;
    assert.equal(service.getJob(accepted.jobId)?.status, "running");
    assert.equal(service.getAgent(accepted.agentId)?.status, "working");
  } finally {
    client.releaseAbort();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("a new approval during timeout abort renews its own window", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-approval-timeout-generation-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox,
  });
  const internal = service as unknown as {
    expireApproval(agentId: string, jobId: string): Promise<void>;
  };
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_approval_timeout_generation", topic: "Approval timeout generation", task: "Preserve a new approval", cwd: directory });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_timeout_one" } } });
    store.setApprovalDeadline(accepted.jobId, new Date(Date.now() - 1).toISOString());
    client.blockAbort();
    const expiration = internal.expireApproval(accepted.agentId, accepted.jobId);
    await waitForCondition(() => client.abortCalls === 1);
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_timeout_two" } } });
    client.releaseAbort();
    await expiration;
    const job = service.getJob(accepted.jobId);
    assert.equal(job?.status, "needs_approval");
    assert.equal(job?.permissionId, "permission_timeout_two");
    assert.ok(Date.parse(job?.approvalDeadlineAt ?? "") > Date.now());
    assert.deepEqual(inbox.notices, [accepted.jobId + ":needs_approval", accepted.jobId + ":needs_approval"]);
  } finally {
    client.releaseAbort();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("two follow calls share one deadline and one graceful finalizer", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-shared-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number): void;
  };
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_follow_shared", topic: "Shared follow", task: "Use one finalizer", cwd: directory });
    const job = service.getJob(accepted.jobId);
    assert.ok(job);
    internal.ensureFollowLifecycle(job, 0, 0.02);
    const first = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 1, graceMinutes: 1 });
    const second = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 1, graceMinutes: 1 });
    await waitForCondition(() => client.promptCalls.filter((call) => call.task.includes("Pare de expandir")).length === 1);
    client.messages = [{
      info: { id: "assistant_follow_shared", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: shared finalizer" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    const results = await Promise.all([first, second]);
    assert.deepEqual(results.map((result) => result.status), ["completed_partial", "completed_partial"]);
    assert.equal(client.promptCalls.filter((call) => call.task.includes("Pare de expandir")).length, 1);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("same-chat delivery is disabled by default even when an App Server command is configured", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-same-chat-default-"));
  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    codexAppServerCommand: "codex",
  }));
  try {
    const status = service.status();
    assert.equal(status.experimentalSameChatDelivery, false);
    assert.equal(status.codexDelivery.available, false);
    assert.match(status.codexDelivery.reason ?? "", /disabled by default/i);
  } finally {
    await service.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("same-chat flag also blocks an injected adapter lifecycle when disabled", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-same-chat-injected-"));
  const client = new FakeClient();
  const codex = new FakeCodex();
  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
  }), {
    manager: new FakeManager(client),
    codex,
  });
  try {
    await service.start();
    assert.equal(codex.startCalls, 0);
    assert.equal(service.status().codexDelivery.available, false);
    await service.stop();
    assert.equal(codex.closeCalls, 0);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("daemon restart reconstructs finalizing follow without sending a duplicate prompt", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-restart-"));
  const store = await BridgeStore.open(directory);
  const firstClient = new FakeClient();
  const first = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(firstClient),
    inbox: new FakeInbox(directory),
  });
  const internal = first as unknown as {
    ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number): { promise: Promise<unknown> };
  };
  try {
    await first.start();
    const accepted = await first.spawn({ requestId: "request_follow_restart", topic: "Restart follow", task: "Persist follow state", cwd: directory });
    const job = first.getJob(accepted.jobId);
    assert.ok(job);
    const lifecycle = internal.ensureFollowLifecycle(job, 0, 0.2);
    assert.equal(first.getJob(accepted.jobId)?.followGraceMinutes, 0.2);
    lifecycle.promise.catch(() => undefined);
    await waitForCondition(() => first.getJob(accepted.jobId)?.status === "finalizing");
    assert.equal(firstClient.promptCalls.filter((call) => call.task.includes("Pare de expandir")).length, 1);
    await first.stop();

    const secondClient = new FakeClient();
    const second = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
      store,
      manager: new FakeManager(secondClient),
      inbox: new FakeInbox(directory),
    });
    try {
      await second.start();
      assert.equal(secondClient.promptCalls.filter((call) => call.task.includes("Pare de expandir")).length, 0);
      assert.equal(second.getJob(accepted.jobId)?.status, "finalizing");
      assert.equal(second.getJob(accepted.jobId)?.followGraceMinutes, 0.2);
    } finally {
      await second.stop();
    }
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("restart derives the remaining grace window when its marker was not persisted", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-grace-recovery-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
  const first = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await first.start();
    const accepted = await first.spawn({ requestId: "request_follow_grace_recovery", topic: "Grace recovery", task: "Persist the remaining grace window", cwd: directory });
    const now = Date.now();
    const followDeadline = new Date(now - 1_000).toISOString();
    await Promise.resolve();
    store.updateJobStatus(accepted.jobId, "following");
    store.setFollowWindow(accepted.jobId, {
      startedAt: new Date(now - 61_000).toISOString(),
      deadlineAt: followDeadline,
      graceMinutes: 0.2,
      graceDeadlineAt: null,
      gracefulFinalizeAttempted: false,
    });
    store.updateJobStatus(accepted.jobId, "finalizing");
    await first.stop();

    const second = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
    try {
      await second.start();
      const recovered = second.getJob(accepted.jobId);
      assert.equal(recovered?.status, "finalizing");
      assert.equal(recovered?.followGraceMinutes, 0.2);
      assert.equal(Date.parse(recovered?.graceDeadlineAt ?? ""), Date.parse(followDeadline) + 0.2 * 60_000);
    } finally {
      await second.stop();
    }
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("daemon restart rehydrates approval without duplicating the notice", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-approval-restart-"));
  const store = await BridgeStore.open(directory);
  const firstClient = new FakeClient();
  const firstInbox = new FakeInbox(directory);
  const first = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(firstClient),
    inbox: firstInbox,
  });
  try {
    await first.start();
    const accepted = await first.spawn({ requestId: "request_approval_restart", topic: "Approval restart", task: "Wait for approval", cwd: directory });
    await firstClient.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_restart" } } });
    assert.deepEqual(firstInbox.notices, [accepted.jobId + ":needs_approval"]);
    const approvalDeadline = first.getJob(accepted.jobId)?.approvalDeadlineAt;
    assert.ok(approvalDeadline);
    const activityCount = store.listActivity(accepted.agentId).length;
    await first.stop();
    store.updateAgentStatus(accepted.agentId, "working");

    const secondClient = new FakeClient();
    const secondInbox = new FakeInbox(directory);
    const second = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
      store,
      manager: new FakeManager(secondClient),
      inbox: secondInbox,
    });
    try {
      await second.start();
      assert.equal(second.getJob(accepted.jobId)?.status, "needs_approval");
      assert.equal(second.getJob(accepted.jobId)?.approvalDeadlineAt, approvalDeadline);
      assert.deepEqual(secondInbox.notices, []);
      assert.equal(store.listActivity(accepted.agentId).length, activityCount);
    } finally {
      await second.stop();
    }
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("visual context is embedded only when supplied, for spawn and continue", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-visual-context-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const spawnPlain = await service.spawn({
      requestId: "request_visual_spawn_plain",
      topic: "Visual context",
      task: "Inspect without visual context",
      cwd: directory,
    });
    assert.doesNotMatch(client.promptCalls[0]?.task ?? "", /VISUAL CONTEXT FROM CODEX/);

    const spawnWith = await service.spawn({
      requestId: "request_visual_spawn_with",
      topic: "Visual context",
      task: "Inspect with visual context",
      cwd: directory,
      visualContext: [
        "Direct observations: the dialog shows a red error banner",
        "Interpretation: the build failed on the parser step",
        "Uncertainty: the stack trace is partially cut off",
      ].join("\n"),
    });
    const spawnPrompt = client.promptCalls[1]?.task ?? "";
    assert.match(spawnPrompt, /VISUAL CONTEXT FROM CODEX/);
    assert.match(spawnPrompt, /original pixels are not available/);
    assert.match(spawnPrompt, /interpretation as a hypothesis/);
    assert.match(spawnPrompt, /Direct observations:\nthe dialog shows a red error banner/);
    assert.match(spawnPrompt, /Interpretation:\nthe build failed on the parser step/);
    assert.match(spawnPrompt, /Uncertainty:\nthe stack trace is partially cut off/);
    assert.doesNotMatch(spawnPrompt, /data:image|image data|base64/i);

    client.messages = [{
      info: { id: "assistant_visual_plain", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: seed turn" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });

    const continuePlain = await service.continueJob({
      requestId: "request_visual_continue_plain",
      agentId: spawnPlain.agentId,
      relation: "continuation",
      task: "Continue without visual context",
    });
    assert.doesNotMatch(client.promptCalls[2]?.task ?? "", /VISUAL CONTEXT FROM CODEX/);

    client.messages = [{
      info: { id: "assistant_visual_continue_plain", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: continue seed turn" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });

    const continueWith = await service.continueJob({
      requestId: "request_visual_continue_with",
      agentId: spawnPlain.agentId,
      relation: "continuation",
      task: "Continue with visual context",
      visualContext: "Direct observations: the chart shows a spike\nInterpretation: the spike is a cache miss\nUncertainty: the axis scale is unclear",
    });
    const continuePrompt = client.promptCalls[3]?.task ?? "";
    assert.match(continuePrompt, /VISUAL CONTEXT FROM CODEX/);
    assert.match(continuePrompt, /Direct observations:\nthe chart shows a spike/);
    assert.match(continuePrompt, /Interpretation:\nthe spike is a cache miss/);
    assert.match(continuePrompt, /Uncertainty:\nthe axis scale is unclear/);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("visual context is redacted and truncated deterministically before dispatch", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-visual-context-limit-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    await service.spawn({
      requestId: "request_visual_limit",
      topic: "Visual context limit",
      task: "Bound the visual context",
      cwd: directory,
      visualContext: "Direct observations: api_key=supersecret " + "x".repeat(25_000) + "\nInterpretation: beyond the limit",
    });
    const prompt = client.promptCalls[0]?.task ?? "";
    assert.doesNotMatch(prompt, /supersecret/);
    assert.match(prompt, /api_key=\[REDACTED\]/);
    const block = prompt.split("VISUAL CONTEXT FROM CODEX")[1] ?? "";
    assert.ok(block.length < 20_500);
    assert.match(prompt, /\[visual context was truncated at the configured limit\]/);
    assert.match(prompt, /Direct observations:/);
    assert.match(prompt, /Interpretation:\nNone provided\./);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
