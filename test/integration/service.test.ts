import test from "node:test";
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
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
import { AGY_COMMAND, AGY_MAX_PROMPT_LENGTH } from "../../src/antigravity/args.js";
import { AntigravitySpool } from "../../src/antigravity/spool.js";
import { runRetentionPrune } from "../../src/retention.js";
import type { CodexBinding, JobRecord, OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage, ResultEnvelope } from "../../src/types.js";

const execFileAsync = promisify(execFile);

const agyFixturePath = fileURLToPath(new URL("../fixtures/agy.cjs", import.meta.url));

function agyFixtureSpawn(behavior: string, calls: string[] = [], prompts: string[] = []) {
  return (command: string, args: string[], options: { cwd: string; env?: NodeJS.ProcessEnv; shell: false; windowsHide: boolean; stdio: ReadonlyArray<"ignore" | "pipe"> }) => {
    calls.push(command);
    const promptIndex = args.indexOf("-p");
    if (promptIndex >= 0) prompts.push(args[promptIndex + 1] ?? "");
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


test("Antigravity references large context files from the workspace instead of overflowing the CLI prompt", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-agy-context-budget-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const prompts: string[] = [];
  await writeFile(path.join(directory, "large-context.txt"), "x".repeat(AGY_MAX_PROMPT_LENGTH), "utf8");
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(directory),
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("ok", agyCalls, prompts) }),
  });
  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_agy_large_context",
      topic: "Large context",
      task: "Inspect the supplied context file.",
      cwd: directory,
      mode: "analyze",
      contextFiles: ["large-context.txt"],
    });
    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "delivered", 2_000);
    assert.equal(agyCalls.length, 1, "the worker must be launched once instead of failing prompt validation");
    assert.equal(prompts.length, 1);
    assert.ok(prompts[0].length <= AGY_MAX_PROMPT_LENGTH, "the agy command argument stays within its safe limit");
    assert.match(prompts[0], /FILE: .*large-context\.txt/);
    assert.doesNotMatch(prompts[0], /x{100}/, "the file content is read by the worker from disk, not copied into argv");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});


test("Antigravity rejects an oversized complete prompt before creating a job or process", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-agy-prompt-preflight-"));
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
    service.setActiveRoute("antigravity-flash-high");
    await assert.rejects(() => service.spawn({
      requestId: "request_agy_oversized_prompt",
      topic: "Oversized prompt",
      task: "x".repeat(AGY_MAX_PROMPT_LENGTH),
      cwd: directory,
      mode: "analyze",
    }), (error: unknown) => {
      assert.ok(error instanceof BridgeError);
      assert.equal(error.status, 400);
      assert.match(error.message, /maximum safe argument length/);
      return true;
    });
    assert.equal(store.listAgents().length, 0);
    assert.equal(store.listJobs().length, 0);
    assert.equal(agyCalls.length, 0);
  } finally {
    await service.stop();
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


test("visual context is redacted and truncated deterministically before dispatch", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-visual-context-limit-"));
  const store = await BridgeStore.open(directory);
  const agyCalls: string[] = [];
  const prompts: string[] = [];
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") }), {
    store,
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("ok", agyCalls, prompts) }),
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
    await waitForCondition(() => prompts.length === 1);
    const prompt = prompts[0] ?? "";
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
    await waitForCondition(() => agyCalls.length === 1);
    assert.equal(agyCalls.length, 1, "exactly one agy spawn");
    assert.equal(client.sessionCount, 0, "no OpenCode session was created");
    assert.equal(client.promptCalls.length, 0, "no OpenCode prompt was dispatched");
    const agent = store.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.modelRoute, "antigravity-flash-high");
    assert.equal(agent.modelProviderId, "antigravity");
    assert.equal(agent.modelId, "gemini-3.8-flash-high");
    assert.equal(accepted.modelDisplayName, "Antigravity · Gemini 3.8 Flash High");
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
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.8-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.8 Flash High" },
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
    await waitForCondition(() => agyCalls.length === 1);
    assert.equal(agyCalls.length, 1, "exactly one agy spawn");
    assert.equal(client.sessionCount, 0, "no OpenCode session was created");
    assert.equal(client.promptCalls.length, 0, "no OpenCode prompt was dispatched");
    assert.equal(accepted.modelDisplayName, "Antigravity · Gemini 3.8 Flash High");
    const agent = store.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.modelRoute, "antigravity-flash-high");
    assert.equal(agent.modelProviderId, "antigravity");
    assert.equal(agent.modelId, "gemini-3.8-flash-high");
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
    assert.ok(job.startedAt, "startedAt must be persisted for antigravity jobs");
    assert.ok(job.completedAt, "completedAt must be persisted for antigravity jobs");
    assert.ok(Date.parse(job.startedAt) <= Date.parse(job.completedAt), "startedAt must not be later than completedAt");
    assert.ok(job.resultPath);
    const persisted = JSON.parse(await readFile(job.resultPath, "utf8")) as { envelope: ResultEnvelope };
    assert.equal(persisted.envelope.status, "completed");
    assert.equal(persisted.envelope.summary, "Fixture summary: task completed without quota.");
    assert.deepEqual(persisted.envelope.files, ["src/example.ts"]);
    assert.deepEqual(persisted.envelope.tests, ["npm test"]);
    assert.equal(persisted.envelope.model, "gemini-3.8-flash-high");
    assert.equal(persisted.envelope.modelDisplayName, "Antigravity · gemini-3.8-flash-high");
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
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.8-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.8 Flash High" },
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
    await waitForCondition(() => agyCalls.length === 1);
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
    await waitForCondition(() => agyCalls.length === 1);
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


test("antigravity follow grace does not kill process with healthy liveness and only explicit abort terminalizes and cleans up", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-route-agy-follow-timeout-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const service = new BridgeService(createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json"), workerMaxExecutionMinutes: 1 }), {
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
    await waitForCondition(() => agyCalls.length === 1);
    assert.equal(agyCalls.length, 1, "exactly one agy spawn is running");
    await waitForCondition(() => internal.antigravityAbortControllers.has(accepted.jobId));
    const controller = internal.antigravityAbortControllers.get(accepted.jobId);
    assert.ok(controller, "the live Antigravity abort controller is registered");
    const job = service.getJob(accepted.jobId);
    assert.ok(job);

    // Follow lifecycle with zero wait to trigger deadline timer evaluation immediately
    const lifecycle = internal.ensureFollowLifecycle(job, 0, 0.001);

    // Allow deadline timer to evaluate without killing healthy liveness
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Follow/grace MUST NOT abort or kill process while liveness is healthy
    assert.equal(controller.signal.aborted, false, "follow/grace must not abort controller while liveness is healthy");
    assert.notEqual(store.getJob(accepted.jobId)?.status, "timed_out", "job must not time out while liveness is healthy");
    assert.notEqual(store.getJob(accepted.jobId)?.status, "aborted", "job must not abort while liveness is healthy");
    assert.equal(store.getAgent(accepted.agentId)?.status, "working", "agent remains working");

    // Only explicit abort terminalizes and cleans up
    const abortResult = await service.abort(accepted.agentId, "Operator cancelled hanging task");
    assert.equal(abortResult.status, "aborted");
    assert.equal(controller.signal.aborted, true, "explicit abort must abort the live Antigravity controller");
    assert.equal(store.getJob(accepted.jobId)?.status, "aborted");
    assert.equal(store.getAgent(accepted.agentId)?.status, "closed");
    assert.deepEqual(client.aborted, [], "no bogus OpenCode abort may be issued for an Antigravity session");
    assert.equal(client.abortCalls, 0);

    const followResult = await lifecycle.promise;
    assert.equal(followResult.status, "aborted");
    assert.equal(followResult.workerAborted, true);

    const activities = store.listActivity(accepted.agentId, 30);
    assert.ok(
      activities.some((entry) => /Sent abort signal to the/.test(entry.summary)),
      "provider-accurate abort activity is recorded",
    );
    assert.ok(
      !activities.some((entry) => /Worker abort failed after the follow grace period/.test(entry.summary)),
      "no OpenCode worker abort is claimed for an Antigravity run",
    );

    await waitForCondition(
      () => store.listActivity(accepted.agentId, 30).some((entry) => /Antigravity process ended after the bridge abort signal/.test(entry.summary)),
      3_000,
    );
    assert.equal(store.getJob(accepted.jobId)?.resultPath, null, "a late Antigravity run must never persist a result after abort");
    assert.equal(store.getJob(accepted.jobId)?.status, "aborted", "the job stays terminal after the late process exit");
    assert.equal(store.getAgent(accepted.agentId)?.status, "closed", "the agent stays closed after the late process exit");

    const followed = await service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    assert.equal(followed.status, "aborted");
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
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.8-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.8 Flash High" },
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
    await waitForCondition(() => agyCalls.length === 1);
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


test("an aborted Antigravity run is never recorded as a rejected dispatch and recovery settles the aborted job", async () => {
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
    assert.equal(store.getJob(accepted.jobId)?.status, "running", "stop() leaves the in-flight job as-is for startup recovery");
    // The next daemon start settles the aborted job.
    await service.start();
    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "aborted", 2_000);
    assert.match(store.getJob(accepted.jobId)?.error ?? "", /cancelled|aborted/i);
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

  const dataDir = path.join(path.dirname(directory), path.basename(directory) + "-data");
  await mkdir(dataDir, { recursive: true });
  const store = await BridgeStore.open(dataDir);
  const agyCalls: string[] = [];
  const prompts: string[] = [];
  const service = new BridgeService(createDefaultConfig({ dataDir, configPath: path.join(dataDir, "config.json") }), {
    store,
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("ok", agyCalls, prompts) }),
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
    await waitForCondition(() => prompts.length === 1);
    const prompt = prompts[0] ?? "";
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
    runRetentionPrune(store, {});
    const remaining = store.db.prepare("SELECT COUNT(*) AS count FROM events WHERE job_id = ?").get(job.id) as { count: number | bigint };
    assert.equal(Number(remaining.count), 0, "the daemon pruned the eligible old events after preparation");
  } finally {
    await second.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});


test("startup recovery routes dispatching job with resultPath through valid transitions without error", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-recover-dispatching-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
  try {
    const agent = store.createAgent({
      id: "agent_recover_dispatching",
      title: "Recover Dispatching",
      topic: "Recover Dispatching Topic",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "server_rec_disp",
      opencodeSessionId: "session_rec_disp",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
      modelRoute: "flash-max",
    });
    const job = store.createJob({
      id: "job_recover_dispatching",
      agentId: agent.id,
      kind: "spawn",
      requestId: "request_recover_dispatching",
      promptHash: "hash_rec_disp",
    });
    store.updateJobStatus(job.id, "dispatching");
    assert.equal(store.getJob(job.id)?.status, "dispatching");

    const resultPath = path.join(directory, "results", `${job.id}.json`);
    await mkdir(path.dirname(resultPath), { recursive: true });
    const envelope: ResultEnvelope = {
      version: 1,
      agentId: agent.id,
      jobId: job.id,
      topic: agent.topic,
      status: "completed",
      opencodeSessionId: agent.opencodeSessionId,
      model: "opencode-go/deepseek-v4-flash",
      modelDisplayName: "DeepSeek V4 Flash",
      workspace: directory,
      summary: "Recovered dispatching result",
      files: [],
      tests: [],
      risks: [],
      diffSummary: "",
      fullResultPath: resultPath,
      orchestratorInstruction: "",
    };
    await writeFile(resultPath, JSON.stringify({ envelope, rawAssistantText: "STATUS: completed\nSUMMARY: Recovered dispatching result" }), "utf8");
    store.setJobResult(job.id, resultPath, envelope.summary);

    const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
    const service = new BridgeService(config, { store, manager: new FakeManager(client), inbox });

    // Starting the service should recover the dispatching job with resultPath
    // without throwing "Invalid job transition: dispatching -> completed".
    await service.start();
    const recoveredJob = store.getJob(job.id);
    assert.ok(recoveredJob);
    assert.equal(recoveredJob.status, "delivered", "recovered dispatching job must transition through running/completed to delivery_pending/delivered");
    assert.ok(inbox.delivered.includes(job.id), "envelope was delivered");
    await service.stop();
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});


test("startup recovery preserves completed_partial and timed_out statuses when job has resultPath", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-recover-partial-timeout-"));
  const store = await BridgeStore.open(directory);
  const client = new FakeClient();
  const inbox = new FakeInbox(directory);
  try {
    const agent = store.createAgent({
      id: "agent_partial_timeout",
      title: "Partial Timeout",
      topic: "Partial Timeout Topic",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "server_pt",
      opencodeSessionId: "session_pt",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
      modelRoute: "flash-max",
    });

    const partialJob = store.createJob({
      id: "job_partial_rec",
      agentId: agent.id,
      kind: "spawn",
      requestId: "request_partial_rec",
      promptHash: "hash_pt_1",
    });
    store.updateJobStatus(partialJob.id, "dispatching");
    store.updateJobStatus(partialJob.id, "running");
    store.updateJobStatus(partialJob.id, "following");
    store.updateJobStatus(partialJob.id, "completed_partial");
    const partialResultPath = path.join(directory, "results", `${partialJob.id}.json`);
    await mkdir(path.dirname(partialResultPath), { recursive: true });
    const partialEnvelope: ResultEnvelope = {
      version: 1,
      agentId: agent.id,
      jobId: partialJob.id,
      topic: agent.topic,
      status: "completed_partial",
      opencodeSessionId: agent.opencodeSessionId,
      model: "opencode-go/deepseek-v4-flash",
      modelDisplayName: "DeepSeek V4 Flash",
      workspace: directory,
      summary: "Partial result",
      files: [],
      tests: [],
      risks: [],
      diffSummary: "",
      fullResultPath: partialResultPath,
      orchestratorInstruction: "",
      partial: true,
    };
    await writeFile(partialResultPath, JSON.stringify({ envelope: partialEnvelope, rawAssistantText: "partial output" }), "utf8");
    store.setJobResult(partialJob.id, partialResultPath, partialEnvelope.summary);

    const timedOutJob = store.createJob({
      id: "job_timeout_rec",
      agentId: agent.id,
      kind: "spawn",
      requestId: "request_timeout_rec",
      promptHash: "hash_pt_2",
    });
    store.updateJobStatus(timedOutJob.id, "dispatching");
    store.updateJobStatus(timedOutJob.id, "running");
    store.updateJobStatus(timedOutJob.id, "timed_out");
    const timedOutResultPath = path.join(directory, "results", `${timedOutJob.id}.json`);
    const timedOutEnvelope: ResultEnvelope = {
      version: 1,
      agentId: agent.id,
      jobId: timedOutJob.id,
      topic: agent.topic,
      status: "timed_out",
      opencodeSessionId: agent.opencodeSessionId,
      model: "opencode-go/deepseek-v4-flash",
      modelDisplayName: "DeepSeek V4 Flash",
      workspace: directory,
      summary: "Timed out result",
      files: [],
      tests: [],
      risks: [],
      diffSummary: "",
      fullResultPath: timedOutResultPath,
      orchestratorInstruction: "",
      deadlineReached: true,
    };
    await writeFile(timedOutResultPath, JSON.stringify({ envelope: timedOutEnvelope, rawAssistantText: "timed out output" }), "utf8");
    store.setJobResult(timedOutJob.id, timedOutResultPath, timedOutEnvelope.summary);

    const config = createDefaultConfig({ dataDir: directory, configPath: path.join(directory, "config.json") });
    const service = new BridgeService(config, { store, manager: new FakeManager(client), inbox });

    await service.start();
    assert.equal(store.getJob(partialJob.id)?.status, "delivered");
    assert.equal(store.getJob(timedOutJob.id)?.status, "delivered");
    assert.ok(inbox.delivered.includes(partialJob.id));
    assert.ok(inbox.delivered.includes(timedOutJob.id));
    await service.stop();
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});


test("MCP or governance task automatically includes global GEMINI.md when present", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-context-gov-"));
  const globalDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-global-gemini-"));
  const globalGeminiPath = path.join(globalDir, "GEMINI.md");
  await writeFile(globalGeminiPath, "# Global Gemini Governance Rules\n", "utf8");

  const store = await BridgeStore.open(directory);
  const agyCalls: string[] = [];
  const prompts: string[] = [];
  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    globalGeminiContextPath: globalGeminiPath,
  }), {
    store,
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("ok", agyCalls, prompts) }),
  });
  try {
    await service.start();

    // 1. MCP task should include the global file
    const mcpAccepted = await service.spawn({
      requestId: "request_gov_mcp",
      topic: "MCP Configuration",
      task: "Configure the weather MCP server",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(mcpAccepted.accepted, true);
    await waitForCondition(() => prompts.length === 1);
    const mcpPrompt = prompts[0] ?? "";
    assert.ok(mcpPrompt.includes(path.normalize(globalGeminiPath)), "MCP task must include global GEMINI.md path");

    // 2. PromptPad task should include the global file
    const promptPadAccepted = await service.spawn({
      requestId: "request_gov_promptpad",
      topic: "PromptPad Integration",
      task: "Update PromptPad templates for prompt formatting",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(promptPadAccepted.accepted, true);
    await waitForCondition(() => prompts.length === 2);
    const promptPadPrompt = prompts[1] ?? "";
    assert.ok(promptPadPrompt.includes(path.normalize(globalGeminiPath)), "PromptPad task must include global GEMINI.md path");

    // 3. Skill governance task should include the global file
    const skillAccepted = await service.spawn({
      requestId: "request_gov_skill",
      topic: "Workflow Policy Update",
      task: "Review skill governance guidelines",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(skillAccepted.accepted, true);
    await waitForCondition(() => prompts.length === 3);
    const skillPrompt = prompts[2] ?? "";
    assert.ok(skillPrompt.includes(path.normalize(globalGeminiPath)), "Skill governance task must include global GEMINI.md path");

    // 4. Regular task with unrelated topic should NOT include the global file
    const regularAccepted = await service.spawn({
      requestId: "request_gov_regular",
      topic: "Data Analysis",
      task: "Parse and aggregate the metrics CSV",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(regularAccepted.accepted, true);
    await waitForCondition(() => prompts.length === 4);
    const regularPrompt = prompts[3] ?? "";
    assert.equal(regularPrompt.includes(path.normalize(globalGeminiPath)), false, "Unrelated task must NOT include global GEMINI.md");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
    await rm(globalDir, { recursive: true, force: true });
  }
});

test("worktree strategy preserves global GEMINI.md canonical path without escape error", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-gov-worktree-"));
  await git(directory, "init", "-q");
  await writeFile(path.join(directory, "tracked.txt"), "tracked content\n", "utf8");
  await git(directory, "add", "tracked.txt");
  await git(directory, "-c", "user.name=DeepSeek Test", "-c", "user.email=deepseek@example.invalid", "commit", "-qm", "initial");

  const globalDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-global-gemini-wt-"));
  const globalGeminiPath = path.join(globalDir, "GEMINI.md");
  await writeFile(globalGeminiPath, "# Global Gemini Worktree Rules\n", "utf8");

  const dataDir = path.join(path.dirname(directory), path.basename(directory) + "-data");
  await mkdir(dataDir, { recursive: true });
  const store = await BridgeStore.open(dataDir);
  const agyCalls: string[] = [];
  const prompts: string[] = [];
  const service = new BridgeService(createDefaultConfig({
    dataDir,
    configPath: path.join(dataDir, "config.json"),
    globalGeminiContextPath: globalGeminiPath,
  }), {
    store,
    antigravity: new AntigravityAdapter({ command: "node", spawnFn: agyFixtureSpawn("ok", agyCalls, prompts) }),
  });
  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "request_gov_worktree",
      topic: "MCP Configuration in Worktree",
      task: "Set up the governance rules inside a worktree",
      cwd: directory,
      mode: "edit",
      workspaceStrategy: "worktree",
      contextFiles: ["tracked.txt"],
    });
    assert.equal(accepted.accepted, true);
    await waitForCondition(() => prompts.length === 1);
    const prompt = prompts[0] ?? "";
    const canonicalGlobalPath = path.resolve(globalGeminiPath);
    assert.ok(
      prompt.includes(canonicalGlobalPath),
      "global GEMINI.md path must remain the canonical external path",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
    await rm(globalDir, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("antigravity timeout does not switch to OpenCode, enforcing zero fallback and fixed route", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-timeout-nofallback-"));
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-timeout-nofallback-data-"));
  const store = await BridgeStore.open(dataDir);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  await writeFile(path.join(directory, "context.txt"), "Important analyze context data", "utf8");

  const config = createDefaultConfig({
    dataDir,
    configPath: path.join(dataDir, "config.json"),
    antigravityTimeoutFallbackRoute: "flash-max",
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.8-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.8 Flash High" },
    ],
  });

  const service = new BridgeService(config, {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(dataDir),
    antigravity: new AntigravityAdapter({
      command: "node",
      timeoutMs: 100,
      spawnFn: agyFixtureSpawn("hang", agyCalls),
    }),
  });

  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_timeout_nofallback",
      topic: "Analyze timeout failover",
      task: "Inspect repository and analyze architecture",
      cwd: directory,
      mode: "analyze",
      contextFiles: ["context.txt"],
      modelRoute: "antigravity-flash-high",
    });

    assert.equal(accepted.accepted, true);
    assert.equal(accepted.modelDisplayName, "Antigravity · Gemini 3.8 Flash High");

    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "failed", 3_000);

    assert.equal(agyCalls.length, 1, "exactly one agy execution was attempted");
    assert.equal(client.sessionCount, 0, "zero OpenCode session created; no fallback switch");
    assert.equal(client.promptCalls.length, 0, "zero OpenCode prompt dispatched; route is fixed");

    const agent = store.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.modelRoute, "antigravity-flash-high", "primary route preserved on agent");
    assert.equal(agent.modelProviderId, "antigravity", "primary provider preserved on agent");
    assert.equal(agent.modelId, "gemini-3.8-flash-high", "primary model id preserved on agent");
    assert.equal(agent.status, "failed", "agent is marked failed on timeout");

    const failedJob = store.getJob(accepted.jobId);
    assert.ok(failedJob);
    assert.equal(failedJob.status, "failed");
    assert.match(failedJob.error ?? "", /did not finish within/);
    assert.equal(failedJob.fallbackCount ?? 0, 0, "zero fallback attempted");
    assert.equal(failedJob.fallbackFrom, null, "no fallback from route");
    assert.equal(failedJob.fallbackTo, null, "no fallback to route");

    const followed = await service.follow({ agentId: accepted.agentId, jobId: accepted.jobId });
    assert.equal(followed.status, "failed");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});


test("antigravity non-timeout errors do not trigger fallback and keep route fixed", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-no-fallback-exit-"));
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-no-fallback-exit-data-"));
  const store = await BridgeStore.open(dataDir);
  const client = new FakeClient();
  const agyCalls: string[] = [];

  const config = createDefaultConfig({
    dataDir,
    configPath: path.join(dataDir, "config.json"),
    antigravityTimeoutFallbackRoute: "flash-max",
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.8-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.8 Flash High" },
    ],
  });

  const service = new BridgeService(config, {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(dataDir),
    antigravity: new AntigravityAdapter({
      command: "node",
      spawnFn: agyFixtureSpawn("fail", agyCalls),
    }),
  });

  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_no_fallback_exit_fail",
      topic: "No fallback exit failure",
      task: "Analyze task",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
    });

    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "failed", 2_000);
    assert.equal(client.sessionCount, 0, "no OpenCode fallback for exit error");
    assert.equal(client.promptCalls.length, 0);

    const agent = store.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.modelRoute, "antigravity-flash-high", "route preserved on agent");
    assert.equal(agent.modelProviderId, "antigravity", "provider preserved on agent");

    const job = store.getJob(accepted.jobId);
    assert.equal(job?.status, "failed");
    assert.match(job?.error ?? "", /quota exceeded/);
    assert.equal(job?.fallbackCount ?? 0, 0);
    assert.equal(job?.fallbackFrom, null);
    assert.equal(job?.fallbackTo, null);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});


test("abort racing Antigravity timeout never launches OpenCode fallback", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-abort-racing-fallback-"));
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-abort-racing-fallback-data-"));
  const store = await BridgeStore.open(dataDir);
  const client = new FakeClient();
  const agyCalls: string[] = [];

  const config = createDefaultConfig({
    dataDir,
    configPath: path.join(dataDir, "config.json"),
    antigravityTimeoutFallbackRoute: "flash-max",
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.8-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.8 Flash High" },
    ],
  });

  const service = new BridgeService(config, {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(dataDir),
    antigravity: new AntigravityAdapter({
      command: "node",
      timeoutMs: 500,
      spawnFn: agyFixtureSpawn("hang", agyCalls),
    }),
  });

  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");
    const accepted = await service.spawn({
      requestId: "request_abort_racing_fallback",
      topic: "Abort racing fallback",
      task: "Analyze task",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
    });

    // Abort shortly after spawn while agy is hanging
    await new Promise((resolve) => setTimeout(resolve, 50));
    await service.abort(accepted.agentId, "Cancelled by user");

    await waitForCondition(() => store.getJob(accepted.jobId)?.status === "aborted", 2_000);
    // Wait a bit to ensure timeout doesn't launch fallback post-abort
    await new Promise((resolve) => setTimeout(resolve, 600));

    assert.equal(client.sessionCount, 0, "no OpenCode session created for aborted job");
    assert.equal(client.promptCalls.length, 0, "no OpenCode prompt dispatched for aborted job");

    const job = store.getJob(accepted.jobId);
    assert.equal(job?.status, "aborted");
    assert.equal(job?.fallbackCount ?? 0, 0);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});


test("normal Antigravity first spawn persists one linked attempt, reaches terminal status, and removes transient prompt", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-agy-first-spawn-cwd-"));
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-agy-first-spawn-data-"));
  const store = await BridgeStore.open(dataDir);
  const client = new FakeClient();
  const agyCalls: string[] = [];
  const prompts: string[] = [];

  const config = createDefaultConfig({
    dataDir,
    configPath: path.join(dataDir, "config.json"),
    modelRoutes: [
      { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: false, display: "DeepSeek V4 Flash · Max" },
      { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.8-flash-high", variant: null, enabled: true, default: true, display: "Antigravity · Gemini 3.8 Flash High" },
    ],
  });

  const service = new BridgeService(config, {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(dataDir),
    antigravity: new AntigravityAdapter({
      command: "node",
      spawnFn: agyFixtureSpawn("ok", agyCalls, prompts),
    }),
  });

  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");

    const accepted = await service.spawn({
      requestId: "request_agy_normal_first_spawn",
      topic: "Normal Antigravity spawn",
      task: "Analyze repository architecture",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
    });

    assert.equal(accepted.accepted, true);
    assert.equal(accepted.status, "accepted");
    assert.ok(accepted.agentId);
    assert.ok(accepted.jobId);

    await waitForCondition(() => {
      const job = store.getJob(accepted.jobId);
      return job !== null && ["completed", "delivery_pending", "delivered"].includes(job.status);
    }, 2_000);

    const job = store.getJob(accepted.jobId);
    assert.ok(job);
    assert.equal(job.status, "delivered");
    assert.equal(agyCalls.length, 1, "exactly one agy execution was spawned");

    const spool = new AntigravitySpool(dataDir);
    const attempts = await spool.listAttempts(accepted.jobId);
    assert.equal(attempts.length, 1, "exactly one spool attempt must be persisted");

    const attempt = attempts[0]!;
    assert.equal(attempt.agentId, accepted.agentId);
    assert.equal(attempt.jobId, accepted.jobId);
    assert.equal(attempt.requestId, "request_agy_normal_first_spawn");
    assert.equal(attempt.modelRoute, "antigravity-flash-high");
    assert.equal(attempt.modelId, "gemini-3.8-flash-high");
    assert.equal(attempt.cwd, directory);

    const attemptStatus = await spool.readStatus(attempt.attemptId, accepted.jobId);
    assert.ok(attemptStatus);
    assert.equal(attemptStatus.status, "completed");
    assert.equal(attemptStatus.exitCode, 0);

    assert.equal(existsSync(attempt.promptPath), false, "transient prompt.txt must be removed after terminal status");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});


test("existing persisted agent with historical gemini-3.7-flash-high retains pinned identity while new spawns receive 3.8", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-historical-pinning-"));
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-historical-pinning-data-"));
  const store = await BridgeStore.open(dataDir);
  const client = new FakeClient();
  const agyCalls: string[] = [];

  // Create an existing historical agent directly in store with 3.7
  const historicalAgent = store.createAgent({
    id: "agent_historical_37",
    title: "Historical Agent",
    topic: "Historical Gemini 3.7 task",
    repositoryRoot: directory,
    workspacePath: directory,
    workspaceStrategy: "shared",
    opencodeServerId: "antigravity",
    opencodeSessionId: "antigravity:agent_historical_37",
    modelProviderId: "antigravity",
    modelId: "gemini-3.7-flash-high",
    modelVariant: null,
    modelRoute: "antigravity-flash-high",
  });
  store.updateAgentStatus(historicalAgent.id, "working");
  store.updateAgentStatus(historicalAgent.id, "completed");

  const service = new BridgeService(createDefaultConfig({ dataDir, configPath: path.join(dataDir, "config.json") }), {
    store,
    manager: new FakeManager(client),
    inbox: new FakeInbox(dataDir),
    antigravity: new AntigravityAdapter({
      command: "node",
      spawnFn: agyFixtureSpawn("ok", agyCalls),
    }),
  });

  try {
    await service.start();
    service.setActiveRoute("antigravity-flash-high");

    // 1. Verify persisted agent retains historical 3.7 model identity
    const reloadedAgent = store.getAgent(historicalAgent.id);
    assert.ok(reloadedAgent);
    assert.equal(reloadedAgent.modelId, "gemini-3.7-flash-high");
    assert.equal(reloadedAgent.modelRoute, "antigravity-flash-high");

    // 2. New spawn receives promoted 3.8 model identity
    const newSpawn = await service.spawn({
      requestId: "req_new_spawn_38",
      topic: "New spawn on 3.8",
      task: "Task on promoted model",
      cwd: directory,
      mode: "analyze",
      modelRoute: "antigravity-flash-high",
    });

    assert.equal(newSpawn.accepted, true);
    assert.equal(newSpawn.modelDisplayName, "Antigravity · Gemini 3.8 Flash High");

    const newAgent = store.getAgent(newSpawn.agentId);
    assert.ok(newAgent);
    assert.equal(newAgent.modelId, "gemini-3.8-flash-high");
    assert.equal(newAgent.modelRoute, "antigravity-flash-high");

    await waitForCondition(() => store.getJob(newSpawn.jobId)?.status === "delivered", 2_000);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});
