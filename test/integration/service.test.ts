import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { execFile, spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { createDefaultConfig } from "../../src/config.js";
import type { CodexCorrelation, CodexDeliveryAdapter } from "../../src/codex/adapter.js";
import { BridgeError } from "../../src/errors.js";
import { InboxDelivery } from "../../src/delivery/inbox.js";
import { OpenCodeHttpError, OpenCodeTransportError } from "../../src/opencode/client.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeBusyError, BridgeService, FollowCancelledError, type ManagedOpenCodeLike, type OpenCodeManagerLike } from "../../src/service.js";
import { AntigravityAdapter } from "../../src/antigravity/adapter.js";
import { AGY_COMMAND } from "../../src/antigravity/args.js";
import type { CodexBinding, JobRecord, OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage, ResultEnvelope } from "../../src/types.js";

const execFileAsync = promisify(execFile);

const agyFixturePath = fileURLToPath(new URL("../fixtures/agy.cjs", import.meta.url));

function agyFixtureSpawn(behavior: string, calls: string[] = []) {
  return (command: string, args: string[], options: { cwd: string; env?: NodeJS.ProcessEnv; shell: false; windowsHide: boolean; stdio: ReadonlyArray<"ignore" | "pipe"> }) => {
    calls.push(command);
    return spawn(process.execPath, [agyFixturePath, ...args], {
      cwd: options.cwd,
      env: { ...process.env, ...(options.env ?? {}), AGY_FIXTURE: behavior },
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
  };
}

async function git(cwd: string, ...args: string[]): Promise<void> {
  await execFileAsync("git", ["-C", cwd, ...args]);
}

class FakeClient implements OpenCodeClientLike {
  sessionCount = 0;
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  promptOptions: Array<Record<string, unknown>> = [];
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
  async promptAsync(sessionId: string, task: string, options?: Record<string, unknown>): Promise<void> {
    this.promptCalls.push({ sessionId, task });
    if (options) this.promptOptions.push({ ...options });
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
      info: { id: "assistant_event_scope", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: first turn" }],
    }, {
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
        info: { id: "assistant_continuation_previous", role: "assistant", sessionID: "session_1" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: previous turn" }],
      }, {
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

test("follow floors short wait/grace values at the configured defaults", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-minimum-window-"));
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
    const accepted = await service.spawn({ requestId: "request_follow_minimum_window", topic: "Minimum follow window", task: "Keep the default worker budget", cwd: directory });
    const controller = new AbortController();
    const follow = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 1, graceMinutes: 1 }, controller.signal);
    await waitForCondition(() => service.getJob(accepted.jobId)?.status === "following");
    const following = service.getJob(accepted.jobId);
    assert.ok(following);
    assert.ok(Math.abs(Date.parse(following.followDeadlineAt ?? "") - Date.parse(following.followStartedAt ?? "") - 12 * 60_000) < 1_000);
    assert.equal(following.followGraceMinutes, 4);
    controller.abort();
    await assert.rejects(follow, (error: unknown) => error instanceof FollowCancelledError);
    assert.deepEqual(client.aborted, []);
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

test("restart raises a pre-fix short follow window to the configured defaults", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-short-window-restart-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    followDefaultWaitMinutes: 12,
    followDefaultGraceMinutes: 4,
  });
  const first = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await first.start();
    const accepted = await first.spawn({ requestId: "request_follow_short_window_restart", topic: "Pre-fix window", task: "Persist a pre-fix short window", cwd: directory });
    const now = Date.now();
    store.updateJobStatus(accepted.jobId, "following");
    store.setFollowWindow(accepted.jobId, {
      startedAt: new Date(now - 60_000).toISOString(),
      deadlineAt: new Date(now - 1_000).toISOString(),
      graceMinutes: 1,
      graceDeadlineAt: null,
      gracefulFinalizeAttempted: false,
    });
    await first.stop();

    const second = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
    try {
      await second.start();
      const recovered = second.getJob(accepted.jobId);
      assert.ok(recovered);
      assert.equal(recovered.status, "following");
      assert.ok(Math.abs(Date.parse(recovered.followDeadlineAt ?? "") - Date.parse(recovered.followStartedAt ?? "") - 12 * 60_000) < 1_000);
      assert.equal(recovered.followGraceMinutes, 4);
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
      info: { id: "assistant_visual_plain", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: seed turn" }],
    }, {
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

test("transport-failed spawn dispatch resolves accepted with followable IDs and no duplicate dispatch", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-dispatch-unknown-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  client.blockPrompt();
  client.promptErrors.push(new OpenCodeTransportError("POST", "/session/session_1/prompt_async", "timed out"));
  try {
    await service.start();
    const spawn = service.spawn({ requestId: "request_dispatch_unknown", topic: "Unknown dispatch", task: "Transport may have accepted", cwd: directory });
    await waitForCondition(() => client.promptCalls.length === 1);
    client.releasePrompt();
    const accepted = await spawn;
    assert.equal(accepted.accepted, true, "an unknown transport outcome must resolve as an accepted bridge obligation");
    assert.equal(accepted.outcome, "dispatch_unknown");
    assert.equal(accepted.agentId, store.listAgents()[0]?.id);
    assert.equal(accepted.jobId, store.listJobs()[0]?.id);
    const job = service.getJob(accepted.jobId);
    assert.ok(job);
    assert.equal(job.status, "following", "mandatory follow must arm the deadline for an unaccepted prompt");
    assert.ok(Math.abs(Date.parse(job.followDeadlineAt ?? "") - Date.parse(job.followStartedAt ?? "") - 20 * 60_000) < 1_000);
    assert.equal(job.followGraceMinutes, 5);
    assert.equal(service.getAgent(accepted.agentId)?.status, "working", "the agent must not be marked failed");
    assert.match(job.error ?? "", /outcome unknown/i);
    await assert.rejects(() => service.continueJob({
      requestId: "request_dispatch_unknown_duplicate",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Must not create a duplicate continuation",
    }), /busy/);
    assert.equal(client.promptCalls.length, 1, "no second prompt dispatch may occur");
    const follow = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    const aborted = await service.abort(accepted.agentId, "settle by abort");
    assert.equal(aborted.jobId, accepted.jobId);
    assert.equal((await follow).status, "aborted");
    assert.equal(service.getJob(accepted.jobId)?.status, "aborted");
    assert.equal(client.promptCalls.length, 1);
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("transport-failed continue dispatch resolves accepted and settles through the armed follow deadline", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-dispatch-unknown-continue-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    timeoutFollow(jobId: string): Promise<void>;
  };
  try {
    await service.start();
    const seed = await service.spawn({ requestId: "request_dispatch_unknown_seed", topic: "Unknown continue", task: "Seed the session", cwd: directory });
    client.messages = [{
      info: { id: "assistant_unknown_continue_seed", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: seed turn" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    assert.equal(service.getJob(seed.jobId)?.status, "delivered");
    client.blockPrompt();
    client.promptErrors.push(new OpenCodeTransportError("POST", "/session/session_1/prompt_async", "connection reset"));
    const continuation = service.continueJob({
      requestId: "request_dispatch_unknown_continue",
      agentId: seed.agentId,
      relation: "continuation",
      task: "Transport may have accepted",
    });
    await waitForCondition(() => client.promptCalls.length === 2);
    client.releasePrompt();
    const accepted = await continuation;
    assert.equal(accepted.accepted, true, "an unknown continue outcome must resolve as an accepted bridge obligation");
    assert.equal(accepted.outcome, "dispatch_unknown");
    assert.equal(accepted.jobId, store.listJobs()[0]?.id);
    const job = service.getJob(accepted.jobId);
    assert.ok(job);
    assert.equal(job.status, "following");
    assert.ok(job.followDeadlineAt);
    assert.equal(job.lastAssistantMessageId, "assistant_unknown_continue_seed", "the continuation baseline must be preserved");
    assert.equal(client.promptCalls.length, 2, "seed plus the single uncertain continuation, no duplicates");
    const follow = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    store.updateJobStatus(accepted.jobId, "finalizing");
    await internal.timeoutFollow(accepted.jobId);
    const result = await follow;
    assert.equal(result.status, "timed_out", "the armed follow deadline must settle an unaccepted prompt");
    assert.equal(service.getAgent(accepted.agentId)?.status, "timed_out");
    assert.equal(client.promptCalls.length, 2, "settlement must not dispatch the task a second time");
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("definite HTTP dispatch rejection still fails the job and agent", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-dispatch-definite-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  client.promptErrors.push(new OpenCodeHttpError(400, "POST", "http://127.0.0.1:1/session/session_1/prompt_async", "bad request"));
  try {
    await service.start();
    await assert.rejects(() => service.spawn({ requestId: "request_dispatch_definite", topic: "Definite dispatch", task: "Fail loudly", cwd: directory }), /HTTP 400/);
    const job = store.listJobs()[0];
    assert.ok(job);
    assert.equal(job.status, "failed");
    assert.equal(service.getAgent(job.agentId)?.status, "failed");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("restart recovery keeps an unknown-outcome job under its armed follow and settles on events", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-dispatch-unknown-recovery-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
  const first = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  client.blockPrompt();
  client.promptErrors.push(new OpenCodeTransportError("POST", "/prompt_async", "connection reset"));
  try {
    await first.start();
    const spawn = first.spawn({ requestId: "request_dispatch_unknown_recovery", topic: "Unknown recovery", task: "Survive restart", cwd: directory });
    await waitForCondition(() => client.promptCalls.length === 1);
    client.releasePrompt();
    const accepted = await spawn;
    assert.equal(accepted.accepted, true);
    assert.equal(accepted.outcome, "dispatch_unknown");
    assert.equal(first.getJob(accepted.jobId)?.status, "following");
    client.messages = [{
      info: { id: "assistant_unknown_recovery", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: finished before the daemon restarted" }],
    }];
    await first.stop();

    const second = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
    try {
      await second.start();
      assert.equal(second.getJob(accepted.jobId)?.status, "following", "restart must re-arm the follow for the active unknown-outcome job");
      assert.ok(second.getJob(accepted.jobId)?.followDeadlineAt);
      await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
      assert.equal(second.getJob(accepted.jobId)?.status, "delivered");
    } finally {
      await second.stop();
    }
  } finally {
    client.releasePrompt();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("message.part.delta events are not persisted while meaningful events remain", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-delta-suppression-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const countEvents = () => (store.db.prepare("SELECT COUNT(*) AS count FROM events").get() as { count: number }).count;
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_delta_suppression", topic: "Delta suppression", task: "Ignore streaming deltas", cwd: directory });
    const activityBefore = store.listActivity(accepted.agentId).length;
    const eventsBefore = countEvents();
    for (let index = 0; index < 25; index += 1) {
      await client.emit({ type: "message.part.delta", properties: { sessionID: "session_1", delta: { text: "partial" } } });
    }
    assert.equal(store.listActivity(accepted.agentId).length, activityBefore, "deltas must not add activity rows");
    assert.equal(countEvents(), eventsBefore, "deltas must not add event ledger rows");
    client.messages = [{
      info: { id: "assistant_delta_suppression", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: completed after deltas" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    assert.equal(service.getJob(accepted.jobId)?.status, "delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("idle with tool-only assistant output never completes; real output completes later", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-empty-tail-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_empty_tail", topic: "Empty tail", task: "Produce no visible output", cwd: directory });
    client.messages = [{
      info: { id: "assistant_tool_only", role: "assistant", sessionID: "session_1" },
      parts: [
        { type: "reasoning", text: "private reasoning" },
        { type: "tool", text: "tool payload" },
      ],
    }];
    await client.emit({ type: "session.idle", id: "idle_empty_tail_one", properties: { sessionID: "session_1" } });
    assert.equal(service.getJob(accepted.jobId)?.status, "running", "a tool-only tail must not complete the job");
    assert.equal(service.getAgent(accepted.agentId)?.status, "working");
    client.messages = [{
      info: { id: "assistant_tool_only", role: "assistant", sessionID: "session_1" },
      parts: [
        { type: "reasoning", text: "private reasoning" },
        { type: "tool", text: "tool payload" },
        { type: "text", text: "STATUS: completed\nSUMMARY: real output after the empty tail" },
      ],
    }];
    await client.emit({ type: "session.idle", id: "idle_empty_tail_two", properties: { sessionID: "session_1" } });
    assert.equal(service.getJob(accepted.jobId)?.status, "delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("restart reconciliation does not complete an empty-tail job", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-empty-tail-recovery-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
  const first = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await first.start();
    const accepted = await first.spawn({ requestId: "request_empty_tail_recovery", topic: "Empty tail recovery", task: "Stay fail-closed", cwd: directory });
    client.messages = [{
      info: { id: "assistant_empty_tail", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "tool", text: "no visible text" }],
    }];
    await client.emit({ type: "session.idle", id: "idle_empty_tail_recovery_one", properties: { sessionID: "session_1" } });
    assert.equal(first.getJob(accepted.jobId)?.status, "running");
    await first.stop();

    const second = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
    try {
      await second.start();
      assert.equal(second.getJob(accepted.jobId)?.status, "running", "reconciliation must not turn an empty-tail job into a completed success");
      client.messages = [{
        info: { id: "assistant_empty_tail_text", role: "assistant", sessionID: "session_1" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: visible output after recovery" }],
      }];
      await client.emit({ type: "session.idle", id: "idle_empty_tail_recovery_two", properties: { sessionID: "session_1" } });
      assert.equal(second.getJob(accepted.jobId)?.status, "delivered");
    } finally {
      await second.stop();
    }
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("abort auto-closes the agent and keeps it non-continuable", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-abort-close-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_abort_close", topic: "Abort close", task: "End cleanly", cwd: directory });
    await service.abort(accepted.agentId, "test stop");
    assert.equal(service.getAgent(accepted.agentId)?.status, "closed", "aborted agents are non-continuable and auto-close safely");
    assert.equal(service.getJob(accepted.jobId)?.status, "aborted");
    await assert.rejects(() => service.continueJob({
      requestId: "request_abort_close_continue",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Must be rejected",
    }), /not continuable/);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("failed and timed-out writers remain continuable until closed", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-writer-lifecycle-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number): { promise: Promise<unknown> };
  };
  try {
    await service.start();
    const failed = await service.spawn({ requestId: "request_writer_failed_seed", topic: "Failed writer", task: "Fail the seed", cwd: directory });
    client.messages = [{
      info: { id: "assistant_writer_failed_seed", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: seed turn" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    client.promptErrors.push(new OpenCodeHttpError(503, "POST", "/prompt_async", "unavailable"));
    await assert.rejects(() => service.continueJob({
      requestId: "request_writer_failed_second",
      agentId: failed.agentId,
      relation: "continuation",
      task: "Trigger a definite failure",
    }), /HTTP 503/);
    assert.equal(service.getAgent(failed.agentId)?.status, "failed");
    const continuedAfterFailure = await service.continueJob({
      requestId: "request_writer_failed_continue",
      agentId: failed.agentId,
      relation: "continuation",
      task: "A failed writer stays continuable",
    });
    assert.equal(continuedAfterFailure.status, "accepted");

    const timedOut = await service.spawn({ requestId: "request_writer_timeout_seed", topic: "Timeout writer", task: "Hit the grace deadline", cwd: directory });
    const job = service.getJob(timedOut.jobId);
    assert.ok(job);
    const lifecycle = internal.ensureFollowLifecycle(job, 0, 0.001);
    assert.equal((await lifecycle.promise).status, "timed_out");
    assert.equal(service.getAgent(timedOut.agentId)?.status, "timed_out");
    const continuedAfterTimeout = await service.continueJob({
      requestId: "request_writer_timeout_continue",
      agentId: timedOut.agentId,
      relation: "continuation",
      task: "A timed-out writer stays continuable",
    });
    assert.equal(continuedAfterTimeout.status, "accepted");

    await service.close(failed.agentId);
    await assert.rejects(() => service.continueJob({
      requestId: "request_writer_closed_continue",
      agentId: failed.agentId,
      relation: "continuation",
      task: "Closed writers are not continuable",
    }), /not continuable/);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("allow_respawn resumes a closed agent through a new lineage agent and session", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-respawn-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const parent = await service.spawn({
      requestId: "request_respawn_parent",
      topic: "Lineage fixture",
      task: "Seed the parent",
      cwd: directory,
      mode: "analyze",
      threadId: "thread_lineage",
      turnId: "turn_lineage",
    });
    const parentSession = service.getAgent(parent.agentId)?.opencodeSessionId;
    assert.ok(parentSession);
    client.messages = [{
      info: { id: "assistant_respawn_parent", role: "assistant", sessionID: parentSession },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: parent seed turn" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: parentSession } });
    assert.equal(service.getJob(parent.jobId)?.status, "delivered");

    await service.close(parent.agentId);
    assert.equal(service.getAgent(parent.agentId)?.status, "closed");
    await assert.rejects(() => service.continueJob({
      requestId: "request_respawn_no_flag",
      agentId: parent.agentId,
      relation: "correction",
      task: "Must be rejected without the flag",
    }), /not continuable/);

    const resumed = await service.continueJob({
      requestId: "request_respawn_child",
      agentId: parent.agentId,
      relation: "correction",
      task: "Fix the review findings",
      allowRespawn: true,
    });
    assert.equal(resumed.status, "accepted");
    assert.notEqual(resumed.agentId, parent.agentId, "the child is a new agent");
    assert.notEqual(resumed.jobId, parent.jobId, "the child gets a new job obligation");

    const parentRecord = service.getAgent(parent.agentId);
    const child = service.getAgent(resumed.agentId);
    assert.ok(parentRecord);
    assert.ok(child);
    assert.equal(child.parentAgentId, parent.agentId, "lineage is recorded on the child");
    assert.equal(parentRecord.parentAgentId, null, "the parent has no lineage");
    assert.equal(child.topic, parentRecord.topic, "topic is inherited");
    assert.equal(child.workspacePath, parentRecord.workspacePath, "workspace is reused");
    assert.equal(child.workspaceStrategy, parentRecord.workspaceStrategy, "strategy is inherited");
    assert.equal(child.modelProviderId, parentRecord.modelProviderId, "pinned provider is inherited");
    assert.equal(child.modelId, parentRecord.modelId, "pinned model is inherited");
    assert.equal(child.modelRoute, parentRecord.modelRoute, "pinned route label is inherited");
    assert.notEqual(child.opencodeSessionId, parentRecord.opencodeSessionId, "the closed session is never reused");
    assert.equal(client.promptCalls.at(-1)?.sessionId, child.opencodeSessionId, "dispatch targets the new session");

    const childJob = service.getJob(resumed.jobId);
    assert.ok(childJob);
    assert.equal(childJob.kind, "continue");
    assert.equal(childJob.hintThreadId, "thread_lineage", "parent correlation hint is derived");
    assert.equal(childJob.hintTurnId, "turn_lineage");
    assert.equal(childJob.hintSource, "mcp", "the derived hint keeps its original provenance");

    const parentResumeEvents = store.listActivity(parent.agentId, 20).filter((activity) => /resumed/.test(activity.summary));
    const childLineageEvents = store.listActivity(child.id, 20).filter((activity) => /closed agent/.test(activity.summary));
    assert.equal(parentResumeEvents.length, 1, "the parent has an auditable resume event");
    assert.equal(childLineageEvents.length, 1, "the child has an auditable lineage event");

    await assert.rejects(() => service.continueJob({
      requestId: "request_respawn_permission",
      agentId: parent.agentId,
      relation: "continuation",
      task: "Permission fields do not apply to a resume",
      allowRespawn: true,
      permissionId: "permission_x",
      permissionReply: "once",
    }), /not applicable/i);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("allow_respawn with the same request_id creates exactly one lineage child", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-respawn-dedupe-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const parent = await service.spawn({
      requestId: "request_respawn_dedupe_parent",
      topic: "Dedupe fixture",
      task: "Seed the parent",
      cwd: directory,
    });
    const parentSession = service.getAgent(parent.agentId)?.opencodeSessionId;
    assert.ok(parentSession);
    client.messages = [{
      info: { id: "assistant_respawn_dedupe", role: "assistant", sessionID: parentSession },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: dedupe seed turn" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: parentSession } });
    await service.close(parent.agentId);

    const input = {
      requestId: "request_respawn_dedupe_same",
      agentId: parent.agentId,
      relation: "continuation" as const,
      task: "Resume exactly once",
      allowRespawn: true,
    };
    const first = await service.continueJob(input);
    const second = await service.continueJob(input);
    assert.equal(second.agentId, first.agentId, "the same lineage child is returned");
    assert.equal(second.jobId, first.jobId, "the same obligation is returned");
    const children = service.listAgents().filter((agent) => agent.parentAgentId === parent.agentId);
    assert.equal(children.length, 1, "a duplicate recovery never creates a second child");
    assert.equal(client.sessionCount, 2, "only the parent session plus one new session exist");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("allow_respawn never resumes an explicitly aborted agent", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-respawn-abort-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const parent = await service.spawn({
      requestId: "request_respawn_aborted_parent",
      topic: "Aborted fixture",
      task: "Seed the parent",
      cwd: directory,
    });
    await service.abort(parent.agentId, "test stop");
    assert.equal(service.getAgent(parent.agentId)?.status, "closed");
    assert.equal(service.getJob(parent.jobId)?.status, "aborted");
    await assert.rejects(() => service.continueJob({
      requestId: "request_respawn_aborted_child",
      agentId: parent.agentId,
      relation: "continuation",
      task: "Must not resume after an explicit abort",
      allowRespawn: true,
    }), /explicitly aborted/);
    assert.equal(service.listAgents().length, 1, "no lineage child is created");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("allow_respawn fails closed when the closed agent has no persisted result", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-respawn-noresult-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    client.promptErrors.push(new OpenCodeHttpError(503, "POST", "/prompt_async", "unavailable"));
    await assert.rejects(() => service.spawn({
      requestId: "request_respawn_noresult_parent",
      topic: "No result fixture",
      task: "Seed the parent",
      cwd: directory,
    }), /HTTP 503/);
    const parent = store.listAgents()[0];
    assert.ok(parent);
    assert.equal(parent.status, "failed");
    await service.close(parent.id);
    assert.equal(service.getAgent(parent.id)?.status, "closed");
    assert.equal(store.listJobs()[0]?.resultPath, null, "no result was ever persisted");
    await assert.rejects(() => service.continueJob({
      requestId: "request_respawn_noresult_child",
      agentId: parent.id,
      relation: "continuation",
      task: "Must not resume without a persisted result",
      allowRespawn: true,
    }), /without a persisted result/);
    assert.equal(service.listAgents().length, 1, "no lineage child is created");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("spawn and continue persist validated MCP thread/turn hints without authorizing delivery", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-correlation-hints-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
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
      requestId: "request_hint_spawn",
      topic: "Hint fixture",
      task: "Record MCP hints",
      cwd: directory,
      threadId: "thread_mcp",
      turnId: "turn_mcp",
    });
    const hinted = service.getJob(accepted.jobId);
    assert.equal(hinted?.hintThreadId, "thread_mcp");
    assert.equal(hinted?.hintTurnId, "turn_mcp");
    assert.equal(hinted?.hintSource, "mcp");
    assert.equal(store.getBinding(accepted.jobId), null, "hints must never be synthesized into bindings");

    client.messages = [{
      info: { id: "assistant_hint_seed", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: hint seed turn" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });

    const continued = await service.continueJob({
      requestId: "request_hint_continue",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Record a turn-only hint",
      turnId: "turn_continuation",
    });
    const hintedContinue = service.getJob(continued.jobId);
    assert.equal(hintedContinue?.hintThreadId, null);
    assert.equal(hintedContinue?.hintTurnId, "turn_continuation");
    assert.equal(hintedContinue?.hintSource, "mcp");

    const plain = await service.spawn({
      requestId: "request_hint_plain",
      topic: "Plain fixture",
      task: "Record no hints",
      cwd: directory,
    });
    assert.equal(service.getJob(plain.jobId)?.hintThreadId, null);
    assert.equal(service.getJob(plain.jobId)?.hintSource, null);

    const status = service.status();
    assert.equal(status.correlation.hints, 2);
    assert.equal(status.correlation.bindings, 0);

    await waitForCondition(() => inbox.delivered.includes(accepted.jobId), 1_000);
    assert.deepEqual(codex.delivered, [], "a hint alone must not authorize Codex delivery");
    assert.equal(store.getBinding(accepted.jobId), null);
    assert.equal(service.getJob(accepted.jobId)?.status, "delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("approval-resume with unknown transport resolves accepted and settles through the armed deadline", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-approval-resume-unknown-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    timeoutFollow(jobId: string): Promise<void>;
  };
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_resume_unknown", topic: "Resume unknown", task: "Wait for approval", cwd: directory });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_resume_unknown" } } });
    assert.equal(service.getJob(accepted.jobId)?.status, "needs_approval");
    client.blockPrompt();
    client.promptErrors.push(new OpenCodeTransportError("POST", "/prompt_async", "timed out"));
    const continuation = service.continueJob({
      requestId: "request_resume_unknown_continue",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Resume the approval",
    });
    await waitForCondition(() => client.promptCalls.length === 2);
    client.releasePrompt();
    const resumed = await continuation;
    assert.equal(resumed.accepted, true, "an unknown approval-resume must resolve accepted");
    assert.equal(resumed.outcome, "dispatch_unknown");
    assert.equal(resumed.jobId, accepted.jobId, "the original job id must be preserved");
    const job = service.getJob(accepted.jobId);
    assert.equal(job?.status, "following", "approval-resume must arm the follow deadline");
    assert.ok(job?.followDeadlineAt);
    assert.equal(job?.permissionId, null, "the resumed permission is cleared");
    assert.equal(job?.dispatchUnknown, true);
    assert.equal(client.promptCalls.length, 2, "no second submission on the resume path");
    await assert.rejects(() => service.continueJob({
      requestId: "request_resume_unknown_duplicate",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Duplicate resume must be blocked",
    }), /busy/);
    assert.equal(client.promptCalls.length, 2);
    const follow = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    store.updateJobStatus(accepted.jobId, "finalizing");
    await internal.timeoutFollow(accepted.jobId);
    assert.equal((await follow).status, "timed_out", "the armed deadline must settle an unaccepted resume");
    assert.equal(client.promptCalls.length, 2, "settlement must not resubmit the resume prompt");
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("approval-reply with unknown transport resolves accepted and settles through the armed deadline", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-approval-reply-unknown-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    timeoutFollow(jobId: string): Promise<void>;
  };
  client.replyErrors.push(new OpenCodeTransportError("POST", "/api/session/session_1/permission/permission_reply_unknown/reply", "timed out"));
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_reply_unknown", topic: "Reply unknown", task: "Wait for approval", cwd: directory });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_1", permission: { id: "permission_reply_unknown" } } });
    const replied = await service.continueJob({
      requestId: "request_reply_unknown_continue",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Answer the approval",
      permissionId: "permission_reply_unknown",
      permissionReply: "once",
    });
    assert.equal(replied.accepted, true, "an unknown approval-reply must resolve accepted");
    assert.equal(replied.outcome, "dispatch_unknown");
    assert.equal(replied.jobId, accepted.jobId, "the original job id must be preserved");
    const job = service.getJob(accepted.jobId);
    assert.equal(job?.status, "following", "approval-reply must arm the follow deadline");
    assert.ok(job?.followDeadlineAt);
    assert.equal(job?.permissionId, null, "the answered permission is cleared like the success path");
    assert.equal(job?.dispatchUnknown, true);
    assert.equal(client.permissionReplies.length, 1, "exactly one reply attempt");
    assert.equal(client.promptCalls.length, 1, "no prompt was submitted");
    await assert.rejects(() => service.continueJob({
      requestId: "request_reply_unknown_duplicate",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Duplicate reply must be blocked",
      permissionId: "permission_reply_unknown",
      permissionReply: "once",
    }), /busy/);
    assert.equal(client.permissionReplies.length, 1, "no second reply submission");
    const follow = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    store.updateJobStatus(accepted.jobId, "finalizing");
    await internal.timeoutFollow(accepted.jobId);
    assert.equal((await follow).status, "timed_out", "the armed deadline must settle an unaccepted reply");
    assert.equal(client.permissionReplies.length, 1, "settlement must not resubmit the reply");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("explicit follow extends an auto-armed window without a second lifecycle or timer", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-follow-extension-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  const internal = service as unknown as {
    followLifecycles: Map<string, unknown>;
    timeoutFollow(jobId: string): Promise<void>;
  };
  client.blockPrompt();
  client.promptErrors.push(new OpenCodeTransportError("POST", "/prompt_async", "timed out"));
  try {
    await service.start();
    const spawn = service.spawn({ requestId: "request_follow_extension", topic: "Follow extension", task: "Auto-armed window", cwd: directory });
    await waitForCondition(() => client.promptCalls.length === 1);
    client.releasePrompt();
    const accepted = await spawn;
    assert.equal(accepted.outcome, "dispatch_unknown");
    const autoArmed = service.getJob(accepted.jobId);
    assert.ok(autoArmed);
    assert.equal(autoArmed.status, "following");
    assert.ok(Math.abs(Date.parse(autoArmed.followDeadlineAt ?? "") - Date.parse(autoArmed.followStartedAt ?? "") - 20 * 60_000) < 1_000);
    assert.equal(internal.followLifecycles.size, 1);

    const follow = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 60, graceMinutes: 10 });
    const extended = service.getJob(accepted.jobId);
    assert.ok(extended);
    assert.ok(Math.abs(Date.parse(extended.followDeadlineAt ?? "") - Date.parse(extended.followStartedAt ?? "") - 60 * 60_000) < 1_000,
      "a larger explicit follow must extend the persisted deadline");
    assert.equal(extended.followGraceMinutes, 10, "a larger explicit follow must extend the persisted grace");
    assert.equal(internal.followLifecycles.size, 1, "extension must not create a second lifecycle");

    const smaller = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 1, graceMinutes: 1 });
    const notShrunk = service.getJob(accepted.jobId);
    assert.ok(notShrunk);
    assert.ok(Math.abs(Date.parse(notShrunk.followDeadlineAt ?? "") - Date.parse(notShrunk.followStartedAt ?? "") - 60 * 60_000) < 1_000,
      "smaller requested values must not shrink the active window");
    assert.equal(notShrunk.followGraceMinutes, 10);
    assert.equal(internal.followLifecycles.size, 1);

    store.updateJobStatus(accepted.jobId, "finalizing");
    await internal.timeoutFollow(accepted.jobId);
    assert.equal((await Promise.all([follow, smaller])).every((result) => result.status === "timed_out"), true,
      "the single extended lifecycle must settle through the deadline");
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("request_id retry for an active dispatch_unknown job keeps the original outcome without redispatch", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-request-dedup-unknown-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  client.blockPrompt();
  client.promptErrors.push(new OpenCodeTransportError("POST", "/prompt_async", "timed out"));
  try {
    await service.start();
    const spawn = service.spawn({ requestId: "request_dedup_spawn", topic: "Dedup spawn", task: "Unknown outcome", cwd: directory });
    await waitForCondition(() => client.promptCalls.length === 1);
    client.releasePrompt();
    const accepted = await spawn;
    assert.equal(accepted.outcome, "dispatch_unknown");
    const retry = await service.spawn({ requestId: "request_dedup_spawn", topic: "Dedup spawn", task: "Retry must not redispatch", cwd: directory });
    assert.equal(retry.accepted, true);
    assert.equal(retry.outcome, "dispatch_unknown", "the persisted outcome must be retained on retry");
    assert.equal(retry.jobId, accepted.jobId);
    assert.match(retry.message, /uncertain|transport failure/i);
    assert.equal(client.sessionCount, 1, "no second session on retry");
    assert.equal(client.promptCalls.length, 1, "no redispatch on retry");
    assert.equal(store.getJob(accepted.jobId)?.dispatchUnknown, true);

    const seed = await service.spawn({ requestId: "request_dedup_seed", topic: "Dedup seed", task: "Seed the continue path", cwd: directory });
    client.messages = [{
      info: { id: "assistant_dedup_seed", role: "assistant", sessionID: "session_2" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: seed turn" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_2" } });
    client.blockPrompt();
    client.promptErrors.push(new OpenCodeTransportError("POST", "/prompt_async", "connection reset"));
    const continuation = service.continueJob({
      requestId: "request_dedup_continue",
      agentId: seed.agentId,
      relation: "continuation",
      task: "Unknown continue outcome",
    });
    await waitForCondition(() => client.promptCalls.length === 3);
    client.releasePrompt();
    const continued = await continuation;
    assert.equal(continued.outcome, "dispatch_unknown");
    const retryContinue = await service.continueJob({
      requestId: "request_dedup_continue",
      agentId: seed.agentId,
      relation: "continuation",
      task: "Retry must not redispatch",
    });
    assert.equal(retryContinue.accepted, true);
    assert.equal(retryContinue.outcome, "dispatch_unknown", "the continue retry must retain the persisted outcome");
    assert.equal(retryContinue.jobId, continued.jobId);
    assert.match(retryContinue.message, /uncertain|transport failure/i);
    assert.equal(client.promptCalls.length, 3, "no redispatch on the continue retry");
    assert.equal(store.getJob(continued.jobId)?.dispatchUnknown, true);
  } finally {
    client.releasePrompt();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("spawn pins the route on the agent and dispatches with the pinned route options", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-default-"));
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
      requestId: "request_route_default",
      topic: "Route default",
      task: "Run on the default route",
      cwd: directory,
      mode: "analyze",
    });
    const agent = store.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.modelRoute, "flash-max");
    assert.equal(agent.modelProviderId, "opencode-go");
    assert.equal(agent.modelId, "deepseek-v4-flash");
    assert.equal(agent.modelVariant, "max");
    assert.equal(client.promptOptions[0]?.providerId, "opencode-go");
    assert.equal(client.promptOptions[0]?.modelId, "deepseek-v4-flash");
    assert.equal(client.promptOptions[0]?.variant, "max");
    assert.equal(accepted.modelDisplayName, "DeepSeek V4 Flash · Max");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("an explicit model_route matching the active route is accepted and pins that route", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-custom-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-pro", variant: "max", enabled: true, default: false, display: "DeepSeek V4 Pro · Max" },
    ],
  });
  const service = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await service.start();
    const switched = service.setActiveRoute("pro-max");
    assert.equal(switched.activeRoute?.name, "pro-max");
    assert.equal(switched.source, "operator-set");
    const accepted = await service.spawn({
      requestId: "request_route_custom",
      topic: "Route custom",
      task: "Run on pro",
      cwd: directory,
      mode: "analyze",
      modelRoute: "pro-max",
    });
    const agent = store.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.modelRoute, "pro-max");
    assert.equal(client.promptOptions[0]?.modelId, "deepseek-v4-pro");
    assert.equal(accepted.modelDisplayName, "DeepSeek V4 Pro · Max");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("spawn with an unknown route fails closed typed 400 before any side effect", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-unknown-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    await assert.rejects(() => service.spawn({
      requestId: "request_route_unknown",
      topic: "Route unknown",
      task: "Must not run",
      cwd: directory,
      mode: "analyze",
      modelRoute: "nonsense-route",
    }), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 400);
      assert.equal(error.code, "unknown_route");
      return true;
    });
    assert.equal(store.listAgents().length, 0);
    assert.equal(store.listJobs().length, 0);
    assert.equal(client.sessionCount, 0);
    assert.equal(client.promptCalls.length, 0);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("spawn with a disabled route fails closed typed 400 with no fallback", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-disabled-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    await assert.rejects(() => service.spawn({
      requestId: "request_route_disabled",
      topic: "Route disabled",
      task: "Must not run",
      cwd: directory,
      mode: "analyze",
      modelRoute: "pro-max",
    }), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 400);
      assert.equal(error.code, "route_disabled");
      return true;
    });
    assert.equal(store.listAgents().length, 0);
    assert.equal(client.promptCalls.length, 0);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("spawn naming a registered enabled route other than the active route fails closed typed 403 route_override_denied", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-antigravity-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    await assert.rejects(() => service.spawn({
      requestId: "request_route_antigravity",
      topic: "Route antigravity",
      task: "Must not run",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
    }), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 403);
      assert.equal(error.code, "route_override_denied");
      assert.deepEqual(error.details, { route: "antigravity-flash-high", activeRoute: "flash-max" });
      return true;
    });
    assert.equal(store.listAgents().length, 0);
    assert.equal(store.listJobs().length, 0);
    assert.equal(client.sessionCount, 0);
    assert.equal(client.promptCalls.length, 0);
    assert.equal(store.getActiveRoute(), null, "a denied spawn never changes the active route pointer");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("route set validates registered and enabled targets typed and persists nothing on failure", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-set-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    await assert.rejects(async () => service.setActiveRoute("nonsense-route"), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 400);
      assert.equal(error.code, "unknown_route");
      return true;
    });
    await assert.rejects(async () => service.setActiveRoute("pro-max"), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 400);
      assert.equal(error.code, "route_disabled");
      return true;
    });
    assert.equal(store.getActiveRoute(), null, "failed sets never persist a pointer");
    assert.equal(service.routeStatus().source, "configured-default");
    assert.equal(service.routeStatus().activeRoute?.name, "flash-max");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("operator active-route switch routes new spawns without a restart while existing agents stay pinned", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-switch-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-pro", variant: "max", enabled: true, default: false, display: "DeepSeek V4 Pro · Max" },
    ],
  });
  const service = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await service.start();
    const first = await service.spawn({
      requestId: "request_route_switch_before",
      topic: "Before switch",
      task: "Pinned on flash-max",
      cwd: directory,
      mode: "analyze",
    });
    const firstAgent = store.getAgent(first.agentId);
    assert.ok(firstAgent);
    assert.equal(firstAgent.modelRoute, "flash-max");
    assert.equal(client.promptOptions[0]?.modelId, "deepseek-v4-flash");

    const status = service.routeStatus();
    assert.equal(status.activeRoute?.name, "flash-max");
    assert.equal(status.source, "configured-default", "effective route starts as the configured default");
    assert.equal(service.status().activeRoute?.name, "flash-max");
    assert.equal(service.status().activeRouteSource, "configured-default");

    const switched = service.setActiveRoute("pro-max");
    assert.equal(switched.activeRoute?.name, "pro-max");
    assert.equal(switched.source, "operator-set");
    assert.equal(switched.defaultModelRoute, "flash-max", "the configured default is unchanged; only the pointer moved");
    assert.equal(store.getActiveRoute(), "pro-max", "persisted before it becomes effective");
    assert.equal(service.status().activeRouteSource, "operator-set");

    const second = await service.spawn({
      requestId: "request_route_switch_after",
      topic: "After switch",
      task: "Active route applies without restart",
      cwd: directory,
      mode: "analyze",
    });
    const secondAgent = store.getAgent(second.agentId);
    assert.ok(secondAgent);
    assert.equal(secondAgent.modelRoute, "pro-max", "new spawns follow the switched active route");
    assert.equal(client.promptOptions[1]?.modelId, "deepseek-v4-pro");

    client.messages = [{
      info: { id: "assistant_first", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: First agent done\nFILES:\n- notes.txt\nTESTS:\n- unit smoke\nRISKS:\n- none" }],
    }];
    const idle: OpenCodeEvent = {
      type: "session.idle",
      properties: { sessionID: "session_1" },
    };
    await client.emit(idle);
    await client.emit(idle);
    assert.equal(service.getJob(first.jobId)?.status, "delivered");

    const continued = await service.continueJob({
      requestId: "request_route_switch_continue",
      agentId: first.agentId,
      relation: "continuation",
      task: "Existing agent must stay pinned",
    });
    assert.equal(continued.accepted, true);
    assert.equal(client.promptOptions[2]?.modelId, "deepseek-v4-flash", "existing agents never migrate to the new active route");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("antigravityCommand from config propagates to the Antigravity adapter while omission keeps the PATH lookup", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-agy-command-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  try {
    const defaultService = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
      store,
      manager: new FakeManager(client),
      inbox: new FakeInbox(directory),
    });
    const configured = new BridgeService(createDefaultConfig({
      dataDir: directory,
      configPath: path.join(directory, "config.json"),
      antigravityCommand: "C:\\Users\\lab\\antigravity\\staging\\agy.exe",
    }), {
      store,
      manager: new FakeManager(client),
      inbox: new FakeInbox(directory),
    });
    const defaultAdapter = (defaultService as unknown as { antigravity: { command: string } }).antigravity;
    const configuredAdapter = (configured as unknown as { antigravity: { command: string } }).antigravity;
    assert.equal(defaultAdapter.command, AGY_COMMAND, "omitted antigravityCommand keeps the default PATH lookup");
    assert.equal(
      configuredAdapter.command,
      "C:\\Users\\lab\\antigravity\\staging\\agy.exe",
      "the configured executable is used for new Antigravity agents",
    );
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("antigravity sandbox and auto-approval propagate independently from config to the adapter", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-agy-flags-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  try {
    const defaults = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
      store,
      manager: new FakeManager(client),
      inbox: new FakeInbox(directory),
    });
    const defaultAdapter = (defaults as unknown as { antigravity: { sandbox: boolean; dangerouslySkipPermissions: boolean } }).antigravity;
    assert.equal(defaultAdapter.sandbox, false);
    assert.equal(defaultAdapter.dangerouslySkipPermissions, false);

    const unsandboxed = new BridgeService(createDefaultConfig({
      dataDir: directory,
      configPath: path.join(directory, "config.json"),
      antigravitySandbox: false,
      antigravityAutoApprovePermissions: true,
    }), {
      store,
      manager: new FakeManager(client),
      inbox: new FakeInbox(directory),
    });
    const autoApproveAdapter = (unsandboxed as unknown as { antigravity: { sandbox: boolean; dangerouslySkipPermissions: boolean } }).antigravity;
    assert.equal(autoApproveAdapter.sandbox, false);
    assert.equal(
      autoApproveAdapter.dangerouslySkipPermissions,
      true,
      "auto-approval must reach the adapter even when the sandbox is off",
    );

    const sandboxed = new BridgeService(createDefaultConfig({
      dataDir: directory,
      configPath: path.join(directory, "config.json"),
      antigravitySandbox: true,
      antigravityAutoApprovePermissions: false,
    }), {
      store,
      manager: new FakeManager(client),
      inbox: new FakeInbox(directory),
    });
    const sandboxOnlyAdapter = (sandboxed as unknown as { antigravity: { sandbox: boolean; dangerouslySkipPermissions: boolean } }).antigravity;
    assert.equal(sandboxOnlyAdapter.sandbox, true);
    assert.equal(sandboxOnlyAdapter.dangerouslySkipPermissions, false, "the sandbox alone must never imply auto-approval");
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("antigravityCommand really spawns the configured executable: node.exe rejects the agy argument contract", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-agy-realspawn-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  // No injected Antigravity adapter: the real adapter is constructed from
  // config, with process.execPath (a trusted absolute executable available in
  // the test runtime) as antigravityCommand. Spawning node.exe with the agy
  // argument contract (`--model ... -p ... --print-timeout 15m`) makes node
  // itself fail with a distinctive "bad option: --model" error, proving the
  // configured executable was actually spawned and not merely stored.
  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    antigravityCommand: process.execPath,
  }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_route_agy_realspawn",
      topic: "Real spawn",
      task: "Run through the configured executable",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(accepted.accepted, true);
    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "failed", 5_000);
    const job = store.getJob(accepted.jobId);
    assert.ok(job);
    assert.match(
      job.error ?? "",
      /bad option: --model/,
      "the configured executable was actually spawned and rejected the agy --model argument",
    );
    await waitForCondition(() => store.getAgent(accepted.agentId)?.status === "failed", 2_000);
    const followed = await service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    assert.equal(followed.status, "failed");
    assert.match(followed.error ?? "", /bad option: --model/);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("startup recovery terminalizes stranded active Antigravity jobs instead of leaving them dispatching forever", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-agy-recovery-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  // Simulate a daemon loss while an Antigravity run was active: the store
  // persists a "working" agent and a "dispatching" job, but the in-memory
  // provider/process handle no longer exists.
  const agent = store.createAgent({
    id: "agent_stranded",
    title: "Stranded",
    topic: "Stranded antigravity job",
    repositoryRoot: directory,
    workspacePath: directory,
    workspaceStrategy: "shared",
    opencodeServerId: "antigravity",
    opencodeSessionId: "antigravity:agent_stranded",
    modelProviderId: "antigravity",
    modelId: "gemini-3.7-flash-high",
    modelVariant: null,
    modelRoute: "antigravity-flash-high",
  });
  const job = store.createJob({ id: "job_stranded", agentId: agent.id, kind: "spawn", requestId: "request_stranded", promptHash: "h" });
  store.updateJobStatus(job.id, "dispatching");
  store.updateAgentStatus(agent.id, "working");
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const recovered = store.getJob(job.id);
    assert.ok(recovered);
    assert.equal(recovered.status, "failed", "the stranded job must not remain dispatching after recovery");
    assert.match(recovered.error ?? "", /stranded|cannot be recovered/i, "the failure carries a clear reason");
    assert.equal(store.getAgent(agent.id)?.status, "failed", "the stranded agent is failed, not left working");
    const followed = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followed.status, "failed");
    assert.match(followed.error ?? "", /stranded|cannot be recovered/i);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("the enabled antigravity route is selectable as the active route and new spawns dispatch through agy without OpenCode", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-active-agy-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("ok", agyCalls) }),
  });
  try {
    await service.start();
    const switched = service.setActiveRoute("antigravity-flash-high");
    assert.equal(switched.activeRoute?.name, "antigravity-flash-high");
    assert.equal(switched.activeRoute?.providerId, "antigravity");
    const accepted = await service.spawn({
      requestId: "request_route_active_agy",
      topic: "Active antigravity",
      task: "Run on the active antigravity route",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(agyCalls.length, 1, "exactly one agy spawn");
    assert.equal(client.sessionCount, 0, "no OpenCode session was created");
    assert.equal(client.promptCalls.length, 0, "no OpenCode prompt was dispatched");
    const agent = store.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.modelRoute, "antigravity-flash-high");
    assert.equal(agent.modelProviderId, "antigravity");
    assert.equal(agent.modelId, "gemini-3.7-flash-high");
    assert.equal(accepted.modelDisplayName, "Antigravity · Gemini 3.7 Flash High");
    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "delivered", 2_000);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("spawn with the enabled antigravity route runs exactly one agy spawn, never OpenCode, and delivers the literal result", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-antigravity-on-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.7-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.7 Flash High" },
    ],
  });
  const service = new BridgeService(config, {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("ok", agyCalls) }),
  });
  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_route_antigravity_on",
      topic: "Route antigravity on",
      task: "Run on antigravity",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
    });
    assert.equal(agyCalls.length, 1, "exactly one agy spawn");
    assert.equal(client.sessionCount, 0, "no OpenCode session was created");
    assert.equal(client.promptCalls.length, 0, "no OpenCode prompt was dispatched");
    assert.equal(accepted.modelDisplayName, "Antigravity · Gemini 3.7 Flash High");
    const agent = store.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.modelRoute, "antigravity-flash-high");
    assert.equal(agent.modelProviderId, "antigravity");
    assert.equal(agent.modelId, "gemini-3.7-flash-high");
    assert.equal(agent.opencodeSessionId, "antigravity:" + agent.id);
    assert.ok(
      ["dispatching", "running"].includes(store.getJob(accepted.jobId)?.status ?? ""),
      "spawn returns an accepted pending obligation before the agy run completes",
    );
    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "delivered", 2_000);
    assert.equal(store.getAgent(accepted.agentId)?.status, "completed", "completion is asynchronous, observed after the agy run finished");
    const job = store.getJob(accepted.jobId);
    assert.ok(job);
    assert.equal(job.status, "delivered");
    assert.ok(job.resultPath);
    const persisted = JSON.parse(await readFile(job.resultPath, "utf8")) as { envelope: ResultEnvelope };
    assert.equal(persisted.envelope.status, "completed");
    assert.equal(persisted.envelope.summary, "Fixture summary: task completed without quota.");
    assert.deepEqual(persisted.envelope.files, ["src/example.ts"]);
    assert.deepEqual(persisted.envelope.tests, ["npm test"]);
    assert.equal(persisted.envelope.model, "gemini-3.7-flash-high");
    assert.equal(persisted.envelope.modelDisplayName, "Antigravity · gemini-3.7-flash-high");
    const followed = await service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    assert.equal(followed.status, "completed");
    assert.equal(followed.result?.envelope.summary, "Fixture summary: task completed without quota.");
    assert.equal((store.listAgents().find((candidate) => candidate.id === accepted.agentId))?.status, "completed");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("antigravity route failure marks the job failed after exactly one agy spawn with no OpenCode fallback", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-antigravity-fail-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.7-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.7 Flash High" },
    ],
  });
  const service = new BridgeService(config, {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("fail", agyCalls) }),
  });
  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_route_antigravity_fail",
      topic: "Route antigravity fail",
      task: "Must fail",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
    });
    assert.equal(accepted.accepted, true, "spawn stays async: acceptance happens before the agy failure");
    assert.equal(agyCalls.length, 1, "exactly one agy spawn, no retry");
    assert.equal(client.sessionCount, 0, "no OpenCode session was created");
    assert.equal(client.promptCalls.length, 0, "no OpenCode prompt was dispatched");
    await waitForCondition(() => store.listJobs()[0]?.status === "failed", 2_000);
    const job = store.listJobs()[0];
    assert.ok(job);
    assert.equal(job.status, "failed");
    assert.match(job.error ?? "", /quota exceeded/);
    await waitForCondition(() => store.listAgents()[0]?.status === "failed", 2_000);
    const agent = store.listAgents()[0];
    assert.ok(agent);
    assert.equal(agent.status, "failed");
    const followed = await service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    assert.equal(followed.status, "failed");
    assert.match(followed.error ?? "", /quota exceeded/);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("antigravity spawn returns accepted while agy is still executing; deepseek_follow observes the asynchronous completion", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-agy-async-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("slow", agyCalls) }),
  });
  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_route_agy_async",
      topic: "Async antigravity",
      task: "Run slowly on antigravity",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(accepted.accepted, true);
    assert.equal(agyCalls.length, 1, "exactly one agy spawn started");
    assert.ok(
      ["dispatching", "running"].includes(store.getJob(accepted.jobId)?.status ?? ""),
      "the job stays active while agy executes in the background; spawn did not wait for completion",
    );
    const following = service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    const activity = store.listActivity(accepted.agentId, 20);
    assert.ok(
      activity.some((entry) => /Follow mode started; waiting for the Antigravity run to complete/.test(entry.summary)),
      "antigravity follow must record provider-accurate wording",
    );
    assert.ok(
      !activity.some((entry) => /waiting for an OpenCode completion event/.test(entry.summary)),
      "antigravity follow must never claim it waits for an OpenCode completion event",
    );
    const followed = await following;
    assert.equal(followed.status, "completed");
    assert.equal(followed.result?.envelope.summary, "Fixture summary: task completed without quota.");
    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "delivered", 2_000);
    assert.equal(store.getAgent(accepted.agentId)?.status, "completed");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("antigravity follow grace timeout aborts the live process controller, never a bogus OpenCode session, and keeps the job terminal", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-agy-follow-timeout-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("hang", agyCalls) }),
  });
  const internal = service as unknown as {
    ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number): { promise: Promise<{ status: string; workerAborted: boolean; resultAvailable: boolean }> };
    antigravityAbortControllers: Map<string, AbortController>;
  };
  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_route_agy_follow_timeout",
      topic: "Follow timeout antigravity",
      task: "Hang through the follow grace expiry",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(agyCalls.length, 1, "exactly one agy spawn is running");
    await waitForCondition(() => internal.antigravityAbortControllers.has(accepted.jobId));
    const controller = internal.antigravityAbortControllers.get(accepted.jobId);
    assert.ok(controller, "the live Antigravity abort controller is registered");
    const job = service.getJob(accepted.jobId);
    assert.ok(job);
    const lifecycle = internal.ensureFollowLifecycle(job, 0, 0.001);
    const result = await lifecycle.promise;
    assert.equal(result.status, "timed_out");
    assert.equal(result.workerAborted, true);
    assert.equal(controller.signal.aborted, true, "the follow grace timeout must abort the live Antigravity controller");
    assert.equal(store.getJob(accepted.jobId)?.status, "timed_out");
    assert.equal(store.getAgent(accepted.agentId)?.status, "timed_out");
    assert.deepEqual(client.aborted, [], "no bogus OpenCode abort may be issued for an Antigravity session");
    assert.equal(client.abortCalls, 0);
    const activities = store.listActivity(accepted.agentId, 30);
    assert.ok(
      activities.some((entry) => /Sent abort signal to the Antigravity process tree/.test(entry.summary)),
      "provider-accurate abort activity is recorded",
    );
    assert.ok(
      !activities.some((entry) => /Worker abort failed after the follow grace period/.test(entry.summary)),
      "no OpenCode worker abort is claimed for an Antigravity run",
    );
    // The killed agy child settles in the background task; the late process
    // exit must never persist or deliver a result after the follow timed out.
    await waitForCondition(
      () => store.listActivity(accepted.agentId, 30).some((entry) => /Antigravity process ended after the bridge abort signal/.test(entry.summary)),
      3_000,
    );
    assert.equal(store.getJob(accepted.jobId)?.resultPath, null, "a late Antigravity run must never persist a result after timeout");
    assert.equal(store.getJob(accepted.jobId)?.status, "timed_out", "the job stays terminal after the late process exit");
    assert.equal(store.getAgent(accepted.agentId)?.status, "timed_out", "the agent stays terminal after the late process exit");
    const followed = await service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    assert.equal(followed.status, "timed_out");
    assert.equal(followed.workerAborted, true);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("abort between job creation and Antigravity dispatch prevents the launch and never re-activates the agent", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-agy-predispatch-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("ok", agyCalls) }),
  });
  try {
    await service.start();
    // Exact state of the race window: the job exists in "created" and the
    // Antigravity dispatch (controller registration + launch) has not run.
    const agent = store.createAgent({
      id: "agent_predispatch",
      title: "Pre-dispatch",
      topic: "Pre-dispatch abort",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "antigravity:agent_predispatch",
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });
    const job = store.createJob({ id: "job_predispatch", agentId: agent.id, kind: "spawn", requestId: "request_predispatch", promptHash: "h" });
    const aborted = await service.abort(agent.id, "abort before dispatch");
    assert.equal(aborted.jobId, job.id, "the pre-dispatch created job is terminalized by the abort");
    assert.equal(store.getJob(job.id)?.status, "aborted");
    assert.equal(store.getAgent(agent.id)?.status, "closed");
    assert.equal(agyCalls.length, 0, "agy was never launched for the aborted pre-dispatch window");
    assert.equal(store.getJob(job.id)?.resultPath, null, "no result is ever delivered for the aborted job");
    const followed = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followed.status, "aborted");
    assert.equal(store.getAgent(agent.id)?.status, "closed", "the agent never transitions back to working or completed");
    assert.equal(agyCalls.length, 0, "no late dispatch can resurrect the aborted job");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("abort signals an active antigravity process tree and leaves the job terminally aborted", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-antigravity-abort-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.7-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.7 Flash High" },
    ],
  });
  const service = new BridgeService(config, {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("hang", agyCalls) }),
  });
  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const pendingSpawn = service.spawn({
      requestId: "request_route_antigravity_abort",
      topic: "Route antigravity abort",
      task: "Wait until aborted",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
    });
    await waitForCondition(() => store.listAgents()[0]?.status === "working");
    const agent = store.listAgents()[0];
    assert.ok(agent);
    const aborted = await service.abort(agent.id, "test abort");
    assert.equal(aborted.status, "aborted");
    const accepted = await pendingSpawn;
    assert.equal(accepted.agentId, agent.id);
    assert.equal(agyCalls.length, 1, "one agy spawn was signalled rather than replaced");
    // The bridge abort kills the process tree asynchronously; wait until the
    // background task observed the termination so no child still holds the
    // workspace directory when the test cleans up.
    await waitForCondition(
      () => store.listActivity(agent.id, 20).some((activity) => /ended after the bridge abort signal/.test(activity.summary)),
      2_000,
    );
    assert.equal(store.getJob(aborted.jobId as string)?.status, "aborted");
    assert.equal(store.getAgent(agent.id)?.status, "closed");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("an aborted Antigravity run is never recorded as a rejected dispatch and recovery settles the stranded job", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-agy-abort-classify-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("hang", agyCalls) }),
  });
  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_route_agy_abort_classify",
      topic: "Abort classification",
      task: "Hang until the daemon stops",
      cwd: directory,
      mode: "analyze",
    });
    await waitForCondition(() => store.listAgents()[0]?.status === "working");
    // Daemon stop aborts the in-memory controllers while the job row is still
    // active: the background task must classify the outcome as an abort, never
    // as a rejected dispatch.
    await service.stop();
    await waitForCondition(
      () => store.listActivity(accepted.agentId, 20).some((activity) => /ended after the bridge abort signal/.test(activity.summary)),
      2_000,
    );
    const activities = store.listActivity(accepted.agentId, 20);
    assert.ok(
      !activities.some((activity) => /rejected the task dispatch/.test(activity.summary)),
      "an abort signal must never be recorded as a rejected dispatch",
    );
    assert.equal(store.getJob(accepted.jobId)?.status, "dispatching", "stop() leaves the in-flight job as-is for startup recovery");
    // The next daemon start terminalizes the stranded job.
    await service.start();
    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "failed", 2_000);
    assert.match(store.getJob(accepted.jobId)?.error ?? "", /stranded|cannot be recovered/i);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("spawn without a model_route fails closed when the default route is disabled", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-default-disabled-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: false, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-pro", variant: "max", enabled: false, default: false, display: "DeepSeek V4 Pro · Max" },
    ],
  });
  const service = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await service.start();
    await assert.rejects(() => service.spawn({
      requestId: "request_route_no_default",
      topic: "No default",
      task: "Must not run",
      cwd: directory,
      mode: "analyze",
    }), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 400);
      assert.equal(error.code, "route_disabled");
      return true;
    });
    assert.equal(store.listAgents().length, 0);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("continue and approval resume use the persisted agent route, not mutable live defaults", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-pinned-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const proConfig = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-pro", variant: "max", enabled: true, default: false, display: "DeepSeek V4 Pro · Max" },
    ],
  });
  const first = new BridgeService(proConfig, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  let accepted: Awaited<ReturnType<typeof first.spawn>>;
  try {
    await first.start();
    first.setActiveRoute("pro-max");
    accepted = await first.spawn({
      requestId: "request_route_pinned",
      topic: "Route pinned",
      task: "Seed the pinned route",
      cwd: directory,
      mode: "analyze",
      modelRoute: "pro-max",
    });
    assert.equal(store.getAgent(accepted.agentId)?.modelRoute, "pro-max");
    client.messages = [{
      info: { id: "assistant_route_pinned", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: seeded on pro" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    assert.equal(store.getJob(accepted.jobId)?.status, "delivered");
  } finally {
    await first.stop();
  }
  // A second daemon with the default registry (pro-max disabled) must still
  // continue the pinned agent on the persisted pro-max route.
  const second = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await second.start();
    const continued = await second.continueJob({
      requestId: "request_route_pinned_continue",
      agentId: accepted.agentId,
      relation: "continuation",
      task: "Continue on the pinned route",
    });
    assert.equal(continued.accepted, true);
    const options = client.promptOptions.at(-1);
    assert.equal(options?.modelId, "deepseek-v4-pro");
    assert.equal(options?.variant, "max");
  } finally {
    await second.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("mutating or repointing the live config route never redirects a pinned pro agent", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-repoint-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const spawnConfig = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-pro", variant: "max", enabled: true, default: false, display: "DeepSeek V4 Pro · Max" },
    ],
  });
  const first = new BridgeService(spawnConfig, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  let proAgentId: string;
  try {
    await first.start();
    first.setActiveRoute("pro-max");
    const accepted = await first.spawn({
      requestId: "request_route_repoint",
      topic: "Route repoint",
      task: "Pin the pro route",
      cwd: directory,
      mode: "analyze",
      modelRoute: "pro-max",
    });
    proAgentId = accepted.agentId;
    assert.equal(store.getAgent(proAgentId)?.modelRoute, "pro-max");
    client.messages = [{
      info: { id: "assistant_route_repoint", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: pinned on pro" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    assert.equal(store.getJob(accepted.jobId)?.status, "delivered");
  } finally {
    await first.stop();
  }

  // Live config mutation: the pro-max route now points at a different model,
  // flash-max is repointed as well, and the agent's route name was deleted
  // from the registry entirely in the second half. Dispatch must still use
  // the persisted spawn-time identity, with no fallback.
  const repointedConfig = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-other", variant: "max", enabled: true, default: true, display: "Renamed" },
      { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-gamma", variant: "max", enabled: false, default: false, display: "Repointed" },
    ],
  });
  const second = new BridgeService(repointedConfig, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await second.start();
    const continued = await second.continueJob({
      requestId: "request_route_repoint_continue",
      agentId: proAgentId,
      relation: "continuation",
      task: "Continue on the pinned identity",
    });
    assert.equal(continued.accepted, true);
    const options = client.promptOptions.at(-1);
    assert.equal(options?.providerId, "opencode-go", "provider stays the persisted spawn-time provider");
    assert.equal(options?.modelId, "deepseek-v4-pro", "model stays the persisted spawn-time model");
    assert.equal(options?.variant, "max", "variant stays the persisted spawn-time variant");
  } finally {
    await second.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("deleting the route from the live registry still dispatches the persisted identity without fallback", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-delete-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const spawnConfig = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-pro", variant: "max", enabled: true, default: false, display: "DeepSeek V4 Pro · Max" },
    ],
  });
  const first = new BridgeService(spawnConfig, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  let proAgentId: string;
  try {
    await first.start();
    first.setActiveRoute("pro-max");
    const accepted = await first.spawn({
      requestId: "request_route_delete",
      topic: "Route delete",
      task: "Pin the pro route",
      cwd: directory,
      mode: "analyze",
      modelRoute: "pro-max",
    });
    proAgentId = accepted.agentId;
    client.messages = [{
      info: { id: "assistant_route_delete", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: pinned on pro" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
  } finally {
    await first.stop();
  }

  const deletedConfig = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
    ],
  });
  const second = new BridgeService(deletedConfig, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await second.start();
    const continued = await second.continueJob({
      requestId: "request_route_delete_continue",
      agentId: proAgentId,
      relation: "continuation",
      task: "Continue after the route was deleted",
    });
    assert.equal(continued.accepted, true);
    const options = client.promptOptions.at(-1);
    assert.equal(options?.modelId, "deepseek-v4-pro", "no fallback: deleted route must not redirect to the default route");
    assert.equal(options?.variant, "max");
  } finally {
    await second.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("a default-spawned flash agent stays pinned when the live default route is repointed", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-flash-pinned-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const first = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  let flashAgentId: string;
  try {
    await first.start();
    const accepted = await first.spawn({
      requestId: "request_route_flash_pinned",
      topic: "Route flash pinned",
      task: "Pin the default flash route",
      cwd: directory,
      mode: "analyze",
    });
    flashAgentId = accepted.agentId;
    assert.equal(store.getAgent(flashAgentId)?.modelRoute, "flash-max");
    assert.equal(store.getAgent(flashAgentId)?.modelId, "deepseek-v4-flash");
    client.messages = [{
      info: { id: "assistant_route_flash_pinned", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: pinned on flash" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
  } finally {
    await first.stop();
  }

  const repointedConfig = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-other", variant: "max", enabled: true, default: true, display: "Repointed" },
      { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-pro", variant: "max", enabled: false, default: false, display: "DeepSeek V4 Pro · Max" },
    ],
  });
  const second = new BridgeService(repointedConfig, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await second.start();
    const continued = await second.continueJob({
      requestId: "request_route_flash_pinned_continue",
      agentId: flashAgentId,
      relation: "continuation",
      task: "Continue on the persisted flash identity",
    });
    assert.equal(continued.accepted, true);
    const options = client.promptOptions.at(-1);
    assert.equal(options?.modelId, "deepseek-v4-flash", "default-spawned agents stay pinned to their persisted flash identity");
    assert.equal(options?.variant, "max");
  } finally {
    await second.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("legacy agents without a persisted route keep dispatching on their flat columns", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-legacy-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const agent = store.createAgent({
      id: "agent_legacy_route",
      title: "Legacy",
      topic: "Legacy route topic",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "server_legacy",
      opencodeSessionId: "session_legacy_route",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-pro",
      modelVariant: "max",
    });
    const job = store.createJob({ id: "job_legacy_route", agentId: agent.id, kind: "continue", requestId: "request_legacy_route", promptHash: "hash" });
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");
    store.setJobPermission(job.id, "permission_legacy");
    store.updateJobStatus(job.id, "needs_approval");
    store.updateAgentStatus(agent.id, "working");
    store.updateAgentStatus(agent.id, "needs_approval");
    const continued = await service.continueJob({
      requestId: "request_legacy_route_continue",
      agentId: agent.id,
      relation: "continuation",
      task: "Continue a legacy agent",
    });
    assert.equal(continued.accepted, true);
    const options = client.promptOptions.at(-1);
    assert.equal(options?.modelId, "deepseek-v4-pro");
    assert.equal(store.getAgent(agent.id)?.modelRoute, null);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("context file validation rejects missing, oversized and non-regular files before side effects", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-context-validate-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
  });
  const service = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await service.start();
    await writeFile(path.join(directory, "small.txt"), "small", "utf8");
    await writeFile(path.join(directory, "large.txt"), "x".repeat(2_000_000), "utf8");
    await mkdir(path.join(directory, "folder.txt"));

    await assert.rejects(() => service.spawn({
      requestId: "request_context_missing",
      topic: "Context missing",
      task: "Must not run",
      cwd: directory,
      mode: "analyze",
      contextFiles: ["does-not-exist.txt"],
    }), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 400);
      assert.equal(error.code, "context_file_invalid");
      assert.equal((error.details as { reason?: string })?.reason, "missing");
      return true;
    });

    await assert.rejects(() => service.spawn({
      requestId: "request_context_large",
      topic: "Context large",
      task: "Must not run",
      cwd: directory,
      mode: "analyze",
      contextFiles: ["large.txt"],
    }), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 400);
      assert.equal((error.details as { reason?: string })?.reason, "too_large");
      return true;
    });

    await assert.rejects(() => service.spawn({
      requestId: "request_context_folder",
      topic: "Context folder",
      task: "Must not run",
      cwd: directory,
      mode: "analyze",
      contextFiles: ["folder.txt"],
    }), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal((error.details as { reason?: string })?.reason, "not_a_regular_file");
      return true;
    });

    assert.equal(store.listAgents().length, 0);
    assert.equal(store.listJobs().length, 0);
    assert.equal(client.sessionCount, 0);

    const accepted = await service.spawn({
      requestId: "request_context_ok",
      topic: "Context ok",
      task: "Use the small context file",
      cwd: directory,
      mode: "analyze",
      contextFiles: ["small.txt"],
    });
    assert.equal(accepted.accepted, true);
    assert.equal(client.promptCalls.length, 1);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("context file rejection creates no orphan worktree", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-context-worktree-"));
  await git(directory, "init", "-q");
  await writeFile(path.join(directory, "tracked.txt"), "initial\n", "utf8");
  await git(directory, "add", "tracked.txt");
  await git(directory, "-c", "user.name=DeepSeek Test", "-c", "user.email=deepseek@example.invalid", "commit", "-qm", "initial");

  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    await assert.rejects(() => service.spawn({
      requestId: "request_context_worktree_bad",
      topic: "Context worktree bad",
      task: "Must not create a worktree",
      cwd: directory,
      mode: "edit",
      workspaceStrategy: "worktree",
      contextFiles: ["missing.txt"],
    }), /context file/);
    const worktrees = path.join(directory, ".deepseek-worktrees");
    const entries = await readdir(worktrees).catch(() => []);
    assert.deepEqual(entries, [], "no orphan worktree may be created for a rejected context");
    assert.equal(store.listAgents().length, 0);
    assert.equal(client.sessionCount, 0);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("valid tracked context files work with worktree strategy and resolve inside the worktree without path leakage", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-context-worktree-valid-"));
  await git(directory, "init", "-q");
  await writeFile(path.join(directory, "tracked.txt"), "tracked context content\n", "utf8");
  await git(directory, "add", "tracked.txt");
  await git(directory, "-c", "user.name=DeepSeek Test", "-c", "user.email=deepseek@example.invalid", "commit", "-qm", "initial");

  // The bridge store must live OUTSIDE the repository so the worktree
  // cleanliness check (untracked files included) never sees the database.
  const dataDir = path.join(path.dirname(directory), path.basename(directory) + "-data");
  await mkdir(dataDir, { recursive: true });
  const store = await BridgeStore.open(dataDir);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir, configPath: path.join(dataDir, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(dataDir),
  });
  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "request_context_worktree_valid",
      topic: "Context worktree valid",
      task: "Use a tracked context file",
      cwd: directory,
      mode: "edit",
      workspaceStrategy: "worktree",
      contextFiles: ["tracked.txt"],
    });
    assert.equal(accepted.accepted, true);
    assert.equal(accepted.outcome, undefined, "valid context must dispatch normally, not fail");
    assert.equal(client.promptCalls.length, 1);
    const prompt = client.promptCalls[0]?.task ?? "";
    const agent = store.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.workspaceStrategy, "worktree");
    const worktreeFilePath = path.normalize(path.join(agent.workspacePath, "tracked.txt"));
    assert.ok(prompt.includes("FILE: " + worktreeFilePath), "the context FILE path must point inside the worktree");
    assert.equal(
      prompt.includes("FILE: " + path.normalize(path.join(directory, "tracked.txt"))),
      false,
      "the main repository path must not leak into the prompt",
    );
    const worktrees = path.join(directory, ".deepseek-worktrees");
    assert.equal((await readdir(worktrees)).length, 1, "exactly one worktree exists");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("escaping context paths with worktree strategy fail typed 400 before any worktree", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-context-worktree-escape-"));
  await git(directory, "init", "-q");
  await writeFile(path.join(directory, "tracked.txt"), "initial\n", "utf8");
  await git(directory, "add", "tracked.txt");
  await git(directory, "-c", "user.name=DeepSeek Test", "-c", "user.email=deepseek@example.invalid", "commit", "-qm", "initial");
  await writeFile(path.join(path.dirname(directory), "outside.txt"), "outside\n", "utf8");

  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    await assert.rejects(() => service.spawn({
      requestId: "request_context_worktree_escape",
      topic: "Context worktree escape",
      task: "Must not run",
      cwd: directory,
      mode: "edit",
      workspaceStrategy: "worktree",
      contextFiles: ["../outside.txt"],
    }), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 400);
      assert.equal(error.code, "context_file_invalid");
      assert.equal((error.details as { reason?: string })?.reason, "outside_workspace");
      return true;
    });
    const worktrees = path.join(directory, ".deepseek-worktrees");
    assert.deepEqual(await readdir(worktrees).catch(() => []), [], "no worktree may exist after a rejected context");
    assert.equal(store.listAgents().length, 0);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("a terminal follow consumes the obligation while needs_approval stays pending", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-obligation-consumed-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_obligation_consumed", topic: "Consumed follow", task: "Finish normally", cwd: directory, mode: "analyze" });
    client.messages = [{
      info: { id: "assistant_consumed", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: consumed" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    assert.equal(store.getJob(accepted.jobId)?.resultConsumedAt, null);
    const followed = await service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    assert.equal(followed.status, "completed");
    assert.ok(store.getJob(accepted.jobId)?.resultConsumedAt, "terminal follow must persist consumption");

    const approved = await service.spawn({ requestId: "request_obligation_approval", topic: "Approval follow", task: "Wait for approval", cwd: directory, mode: "analyze" });
    await client.emit({ type: "permission.asked", properties: { sessionID: "session_2", permission: { id: "permission_obligation" } } });
    const approvalFollow = await service.follow({ agentId: approved.agentId, jobId: approved.jobId });
    assert.equal(approvalFollow.status, "needs_approval");
    assert.equal(store.getJob(approved.jobId)?.resultConsumedAt, null, "needs_approval keeps the obligation pending");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("recover_result persists consumption of the returned terminal result", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-obligation-recover-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
  });
  try {
    await service.start();
    const accepted = await service.spawn({ requestId: "request_obligation_recover", topic: "Recover consumed", task: "Finish normally", cwd: directory, mode: "analyze" });
    client.messages = [{
      info: { id: "assistant_recover_consumed", role: "assistant", sessionID: "session_1" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: recover consumed" }],
    }];
    await client.emit({ type: "session.idle", properties: { sessionID: "session_1" } });
    assert.equal(store.getJob(accepted.jobId)?.resultConsumedAt, null);
    const recovered = await service.recoverResult(accepted.jobId);
    assert.ok(recovered);
    assert.ok(store.getJob(accepted.jobId)?.resultConsumedAt, "successful recover must persist consumption");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("enabled retention never auto-prunes a legacy database without the offline preparation marker", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-retention-gate-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const old = new Date(Date.now() - 60 * 24 * 60 * 60_000).toISOString();
  store.createAgent({
    id: "agent_retention_gate",
    title: "Gate",
    topic: "Gate topic",
    repositoryRoot: directory,
    workspacePath: directory,
    workspaceStrategy: "shared",
    opencodeServerId: "server_gate",
    opencodeSessionId: "session_gate",
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    modelRoute: "flash-max",
  });
  const job = store.createJob({ id: "job_retention_gate", agentId: "agent_retention_gate", kind: "spawn", requestId: "request_retention_gate", promptHash: "h" });
  store.updateJobStatus(job.id, "dispatching");
  store.updateJobStatus(job.id, "running");
  store.updateJobStatus(job.id, "completed");
  store.updateJobStatus(job.id, "delivery_pending");
  store.updateJobStatus(job.id, "delivered");
  store.setJobResult(job.id, path.join(directory, "results", "job_retention_gate.json"), "gate");
  store.consumeResult(job.id);
  for (let index = 0; index < 3; index += 1) {
    store.insertEvent({ source: "opencode", sourceEventId: "gate_" + index, eventType: "session.idle", sessionId: "session_gate", jobId: job.id });
  }
  store.db.prepare("UPDATE events SET received_at = ? WHERE job_id = ?").run(old, job.id);

  const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json"), retentionMode: "enabled" });
  const first = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await first.start();
    assert.equal(first.status().retention.pruningEnabled, false, "hand-edited enabled mode must not arm online pruning on a legacy DB");
    const remaining = store.db.prepare("SELECT COUNT(*) AS count FROM events WHERE job_id = ?").get(job.id) as { count: number | bigint };
    assert.equal(Number(remaining.count), 3, "no events may be pruned without explicit offline preparation");
  } finally {
    await first.stop();
  }

  store.markRetentionPrepared();
  const second = new BridgeService(config, { store, manager: new FakeManager(client), inbox: new FakeInbox(directory) });
  try {
    await second.start();
    assert.equal(second.status().retention.pruningEnabled, true, "the offline preparation marker arms pruning on the legacy DB");
    const remaining = store.db.prepare("SELECT COUNT(*) AS count FROM events WHERE job_id = ?").get(job.id) as { count: number | bigint };
    assert.equal(Number(remaining.count), 0, "the daemon pruned the eligible old events after preparation");
  } finally {
    await second.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
