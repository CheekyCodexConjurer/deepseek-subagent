import test from "node:test";
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { BridgeStore } from "../../src/store.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeService as BaseBridgeService, type ManagedOpenCodeLike } from "../../src/service.js";
import { hashPrompt } from "../../src/security.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";
import type { AntigravityRunResult } from "../../src/antigravity/types.js";

const execFileAsync = promisify(execFile);

let globalSessionCounter = 0;

class MockAntigravity {
  isMock = true;
  promptCalls: Array<{ sessionId: string; task: string; prompt: string }> = [];
  promptErrors: Array<Error | null> = [];
  activeSessions = new Set<string>();
  pendingRuns = new Map<string, {
    resolve: (res: AntigravityRunResult) => void;
    reject: (err: any) => void;
    agentId: string;
    jobId: string;
    workspace: string;
  }>();

  constructor(public client?: FakeOpenCodeClient) {}

  async runPrompt(options: {
    prompt: string;
    cwd: string;
    model: string;
    signal?: AbortSignal;
    dataDir: string;
    agentId: string;
    jobId: string;
    requestId?: string;
    timeoutMs?: number | null;
    fence?: number;
    onHeartbeat?: any;
    onProgress?: any;
  }): Promise<AntigravityRunResult> {
    const call = {
      sessionId: options.agentId,
      task: options.prompt,
      prompt: options.prompt,
    };
    this.promptCalls.push(call);
    this.activeSessions.add(options.jobId);
    this.activeSessions.add(options.agentId);

    if (this.client) {
      this.client.promptCalls.push({ sessionId: options.agentId, task: options.prompt });
      this.client.activeSessions.add(options.agentId);
      this.client.activeSessions.add(options.jobId);
      const clientErr = this.client.promptErrors.shift();
      if (clientErr) {
        this.activeSessions.delete(options.jobId);
        this.activeSessions.delete(options.agentId);
        this.client.activeSessions.delete(options.jobId);
        this.client.activeSessions.delete(options.agentId);
        throw clientErr;
      }
    }

    const err = this.promptErrors.shift();
    if (err) {
      this.activeSessions.delete(options.jobId);
      this.activeSessions.delete(options.agentId);
      if (this.client) {
        this.client.activeSessions.delete(options.jobId);
        this.client.activeSessions.delete(options.agentId);
      }
      throw err;
    }

    return new Promise((resolve, reject) => {
      const entry = {
        resolve: (res: AntigravityRunResult) => {
          this.activeSessions.delete(options.jobId);
          this.activeSessions.delete(options.agentId);
          if (this.client) {
            this.client.activeSessions.delete(options.jobId);
            this.client.activeSessions.delete(options.agentId);
          }
          this.pendingRuns.delete(options.jobId);
          this.pendingRuns.delete(options.agentId);
          resolve(res);
        },
        reject: (error: any) => {
          this.activeSessions.delete(options.jobId);
          this.activeSessions.delete(options.agentId);
          if (this.client) {
            this.client.activeSessions.delete(options.jobId);
            this.client.activeSessions.delete(options.agentId);
          }
          this.pendingRuns.delete(options.jobId);
          this.pendingRuns.delete(options.agentId);
          reject(error);
        },
        agentId: options.agentId,
        jobId: options.jobId,
        workspace: options.cwd,
      };

      this.pendingRuns.set(options.jobId, entry);
      this.pendingRuns.set(options.agentId, entry);

      if (options.signal) {
        if (options.signal.aborted) {
          entry.reject(new Error("aborted"));
          return;
        }
        options.signal.addEventListener("abort", () => {
          entry.reject(new Error("aborted"));
        });
      }
    });
  }

  async completeActive(target: { id: string; workspacePath?: string }) {
    for (let i = 0; i < 50; i++) {
      if (this.pendingRuns.has(target.id)) break;
      await new Promise((r) => setTimeout(r, 10));
    }
    const entry = this.pendingRuns.get(target.id);
    if (entry) {
      entry.resolve({
        status: "completed",
        runId: "run_" + entry.jobId,
        summary: "STATUS: completed\nSUMMARY: Finished task",
        fullText: "STATUS: completed\nSUMMARY: Finished task",
        files: [],
        tests: [],
        risks: [],
        diffSummary: "",
        model: "gemini-3.8-flash-high",
        modelDisplayName: "Gemini 3.8 Flash High",
        workspace: target.workspacePath ?? entry.workspace,
        rawOutput: "Finished task",
      });
      await new Promise((r) => setTimeout(r, 25));
    }
  }

  async completeSession(sessionId: string) {
    const agentId = sessionId.startsWith("antigravity:") ? sessionId.slice("antigravity:".length) : sessionId;
    for (let i = 0; i < 50; i++) {
      if (this.pendingRuns.has(agentId) || this.pendingRuns.has(sessionId)) break;
      await new Promise((r) => setTimeout(r, 10));
    }
    const entry = this.pendingRuns.get(agentId) ?? this.pendingRuns.get(sessionId);
    if (entry) {
      entry.resolve({
        status: "completed",
        runId: "run_" + entry.jobId,
        summary: "STATUS: completed\nSUMMARY: Finished task",
        fullText: "STATUS: completed\nSUMMARY: Finished task",
        files: [],
        tests: [],
        risks: [],
        diffSummary: "",
        model: "gemini-3.8-flash-high",
        modelDisplayName: "Gemini 3.8 Flash High",
        workspace: entry.workspace,
        rawOutput: "Finished task",
      });
      await new Promise((r) => setTimeout(r, 25));
    }
  }
}

class FakeOpenCodeClient implements OpenCodeClientLike {
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  promptErrors: Array<Error | null> = [];
  createdSessions: Array<{ id: string; directory?: string; title?: string }> = [];
  messages: OpenCodeMessage[] = [];
  activeSessions = new Set<string>();
  antigravity: MockAntigravity;
  private onEvent?: (event: OpenCodeEvent) => Promise<void> | void;

  constructor() {
    this.antigravity = new MockAntigravity(this);
  }

  async health() {
    return { healthy: true, version: "fake" };
  }
  async createSession(directory?: string, title?: string) {
    globalSessionCounter += 1;
    const id = "session_" + globalSessionCounter;
    this.activeSessions.add(id);
    this.createdSessions.push({ id, directory, title });
    return { id };
  }
  async promptAsync(sessionId: string, task: string) {
    this.promptCalls.push({ sessionId, task });
  }
  async listMessages() {
    return this.messages;
  }
  async getDiff() {
    return "";
  }
  async abort(sessionId: string) {
    this.activeSessions.delete(sessionId);
    this.antigravity.activeSessions.delete(sessionId);
  }
  async replyPermission() {}
  async subscribe(onEvent: (event: OpenCodeEvent) => Promise<void> | void) {
    this.onEvent = onEvent;
  }
  async emit(event: OpenCodeEvent) {
    if (event.type === "session.idle") {
      const sessId = (event.properties as any)?.sessionID ?? (event.properties as any)?.sessionId;
      if (sessId) {
        await this.antigravity.completeSession(sessId);
      }
    }
    await this.onEvent?.(event);
  }
}

class BridgeService extends BaseBridgeService {
  constructor(config: any, dependencies: any = {}) {
    const antigravity = dependencies.antigravity ?? dependencies.manager?.client?.antigravity ?? new MockAntigravity();
    super(config, {
      ...dependencies,
      antigravity,
    });
  }

  async completeActive(agent: any) {
    if ((this.antigravity as any)?.completeActive) {
      await (this.antigravity as any).completeActive(agent);
    }
    for (let i = 0; i < 50; i++) {
      const j = this.store.listJobs().find((job: any) => job.agentId === agent.id && job.status === "running");
      if (!j) break;
      await new Promise((r) => setTimeout(r, 10));
    }
  }
}

function makeFakeManager(client: FakeOpenCodeClient) {
  return {
    async start(): Promise<ManagedOpenCodeLike> {
      return {
        serverId: "fake_server",
        baseUrl: "http://127.0.0.1:9999",
        client,
        processId: 1234,
        async stop() {},
      };
    },
    async stop() {},
  };
}

async function git(cwd: string, ...args: string[]): Promise<void> {
  await execFileAsync("git", ["-C", cwd, ...args]);
}

async function createCleanGitRepo(dir: string, files: Record<string, string>): Promise<void> {
  await git(dir, "init", "-q");
  await git(dir, "config", "user.name", "DeepSeek Test");
  await git(dir, "config", "user.email", "deepseek@example.invalid");
  for (const [relativePath, content] of Object.entries(files)) {
    const fullPath = path.join(dir, relativePath);
    await mkdir(path.dirname(fullPath), { recursive: true });
    await writeFile(fullPath, content, "utf8");
    await git(dir, "add", relativePath);
  }
  await git(dir, "-c", "user.name=DeepSeek Test", "-c", "user.email=deepseek@example.invalid", "commit", "-qm", "initial commit");
}

// ---------------------------------------------------------------------------
// 1. spawnBatch with worktree strategy and contextFiles:
//    - Admits atomically without ENOENT
//    - Persists empty/deferred prompt envelope at admission
//    - Leaves worktree uncreated before dispatch
//    - Prepares worktree and inlines context at dispatch
// ---------------------------------------------------------------------------
test("spawnBatch with worktree strategy and contextFiles admits atomically without ENOENT, persists empty/deferred prompt envelope at admission, then prepares worktree and inlines context at dispatch", async () => {
  const repoDir = await mkdtemp(path.join(os.tmpdir(), "ds-batch-wt-repo-"));
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "ds-batch-wt-data-"));
  try {
    await createCleanGitRepo(repoDir, {
      "notes.txt": "Committed notes for worktree batch test\n",
    });

    const store = new BridgeStore(path.join(dataDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // 1. Blocker occupies credit ceiling so batch items remain queued
      const blocker = await service.spawn({
        requestId: "req_blocker",
        topic: "Blocker Topic",
        task: "Blocker Task",
        cwd: repoDir,
        workspaceStrategy: "shared",
      });
      assert.equal(blocker.accepted, true);

      // 2. Admission Phase: spawnBatch must admit atomically without ENOENT
      const batchReceipt = await service.spawnBatch({
        batchRequestId: "batch_worktree_deferred",
        items: [
          {
            requestId: "req_worktree_item_1",
            topic: "Worktree Item 1",
            task: "Perform worktree task with context",
            cwd: repoDir,
            workspaceStrategy: "worktree",
            contextFiles: ["notes.txt"],
          },
        ],
      });

      assert.equal(batchReceipt.accepted, true);
      assert.equal(batchReceipt.items.length, 1);
      const item1 = batchReceipt.items[0];
      assert.ok(item1);
      assert.equal(item1.status, "queued");

      // Verify Job committed with empty prompt hash at admission
      const job = store.getJob(item1.jobId);
      assert.ok(job, "Job must be durably persisted in store");
      assert.equal(job.status, "queued");
      assert.equal(job.promptHash, hashPrompt(""), "Job promptHash at admission must match hash of empty string");

      // Verify Dispatch Envelope persisted with empty/deferred prompt
      const envelope = store.getDispatchEnvelope(item1.jobId);
      assert.ok(envelope, "Dispatch envelope must be persisted at admission");
      assert.equal(envelope.prompt, "", "Envelope prompt must be empty string at admission for worktree strategy");
      assert.equal(envelope.promptHash, hashPrompt(""), "Envelope promptHash must match empty prompt hash");
      assert.ok(envelope.workerInput, "Envelope workerInput must be preserved for deferred construction");

      // Verify Agent committed and worktree NOT yet prepared before dispatch
      const agent = store.getAgent(item1.agentId);
      assert.ok(agent, "Agent must be durably persisted in store");
      assert.equal(agent.workspaceStrategy, "worktree");
      assert.equal(
        existsSync(agent.workspacePath),
        false,
        "Worktree directory must not be created on disk during admission before dispatch",
      );

      // 3. Dispatch Phase: Free credit ceiling slot to trigger dispatch of queued worktree item
      store.updateJobStatus(blocker.jobId, "completed");
      await (service as any).drainQueue();

      // Verify worktree prepared upon dispatch
      assert.equal(
        existsSync(agent.workspacePath),
        true,
        "Worktree directory must exist on disk after dispatch",
      );
      assert.equal(
        existsSync(path.join(agent.workspacePath, "notes.txt")),
        true,
        "Tracked context file must exist inside the prepared worktree",
      );

      // Verify real prompt constructed at dispatch via Antigravity worktree-context path
      const promptCall = (service as any).antigravity.promptCalls.find(
        (c: any) => c.sessionId === agent.id || c.sessionId === agent.opencodeSessionId,
      );
      assert.ok(promptCall, "Antigravity must receive prompt call for the agent session upon dispatch");
      const dispatchedPrompt = promptCall.prompt;

      const expectedWorktreeFile = path.normalize(path.join(agent.workspacePath, "notes.txt"));
      assert.ok(
        dispatchedPrompt.includes("FILE: " + expectedWorktreeFile),
        "Dispatched prompt must reference file path inside the worktree",
      );
      const repoFile = path.normalize(path.join(repoDir, "notes.txt"));
      assert.equal(
        dispatchedPrompt.includes(repoFile),
        false,
        "Main repository path must not leak into the prompt",
      );

      // Verify job transitioned to running and dispatch envelope deleted
      const runningJob = store.getJob(item1.jobId);
      assert.equal(runningJob?.status, "running");
      assert.equal(
        store.getDispatchEnvelope(item1.jobId),
        null,
        "Dispatch envelope must be consumed upon dispatch",
      );
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(repoDir, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. spawnBatch with multiple worktree items:
//    - Admits all items with deferred prompt envelopes
//    - Dispatches into separate isolated worktrees
//    - Constructs separate inlined prompts per worktree context
// ---------------------------------------------------------------------------
test("spawnBatch admits multiple worktree items with empty envelopes and dispatches each into isolated worktrees with separate inlined context", async () => {
  const repoDir = await mkdtemp(path.join(os.tmpdir(), "ds-batch-wt-multi-repo-"));
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "ds-batch-wt-multi-data-"));
  try {
    await createCleanGitRepo(repoDir, {
      "taskA.txt": "Context A payload\n",
      "taskB.txt": "Context B payload\n",
    });

    const store = new BridgeStore(path.join(dataDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const blocker = await service.spawn({
        requestId: "req_blocker_multi",
        topic: "Blocker Multi",
        task: "Blocker Task",
        cwd: repoDir,
        workspaceStrategy: "shared",
      });
      assert.equal(blocker.accepted, true);

      const batchReceipt = await service.spawnBatch({
        batchRequestId: "batch_worktree_multi",
        items: [
          {
            requestId: "req_multi_a",
            topic: "Multi Item A",
            task: "Task A with context",
            cwd: repoDir,
            workspaceStrategy: "worktree",
            contextFiles: ["taskA.txt"],
          },
          {
            requestId: "req_multi_b",
            topic: "Multi Item B",
            task: "Task B with context",
            cwd: repoDir,
            workspaceStrategy: "worktree",
            contextFiles: ["taskB.txt"],
          },
        ],
      });

      assert.equal(batchReceipt.accepted, true);
      assert.equal(batchReceipt.items.length, 2);

      const [itemA, itemB] = batchReceipt.items;
      assert.ok(itemA && itemB);
      assert.equal(itemA.status, "queued");
      assert.equal(itemB.status, "queued");

      // Verify both jobs have empty prompt hashes
      const jobA = store.getJob(itemA.jobId);
      const jobB = store.getJob(itemB.jobId);
      assert.equal(jobA?.promptHash, hashPrompt(""));
      assert.equal(jobB?.promptHash, hashPrompt(""));

      // Verify both dispatch envelopes are empty
      const envA = store.getDispatchEnvelope(itemA.jobId);
      const envB = store.getDispatchEnvelope(itemB.jobId);
      assert.equal(envA?.prompt, "");
      assert.equal(envB?.prompt, "");
      assert.equal(envA?.promptHash, hashPrompt(""));
      assert.equal(envB?.promptHash, hashPrompt(""));

      // Verify neither worktree exists yet
      const agentA = store.getAgent(itemA.agentId)!;
      const agentB = store.getAgent(itemB.agentId)!;
      assert.notEqual(agentA.workspacePath, agentB.workspacePath);
      assert.equal(existsSync(agentA.workspacePath), false);
      assert.equal(existsSync(agentB.workspacePath), false);

      // Complete blocker to dispatch item A
      store.updateJobStatus(blocker.jobId, "completed");
      await (service as any).drainQueue();

      assert.equal(existsSync(agentA.workspacePath), true);
      assert.equal(existsSync(agentB.workspacePath), false);

      const callA = (service as any).antigravity.promptCalls.find(
        (c: any) => c.sessionId === agentA.id || c.sessionId === agentA.opencodeSessionId,
      );
      assert.ok(callA);
      const expectedWorktreeFileA = path.normalize(path.join(agentA.workspacePath, "taskA.txt"));
      assert.ok(
        callA.prompt.includes("FILE: " + expectedWorktreeFileA),
        "Dispatched prompt A must reference file path inside worktree A",
      );
      assert.equal(callA.prompt.includes("taskB.txt"), false);

      // Complete item A to dispatch item B
      store.updateJobStatus(itemA.jobId, "completed");
      await (service as any).drainQueue();

      assert.equal(existsSync(agentB.workspacePath), true);

      const callB = (service as any).antigravity.promptCalls.find(
        (c: any) => c.sessionId === agentB.id || c.sessionId === agentB.opencodeSessionId,
      );
      assert.ok(callB);
      const expectedWorktreeFileB = path.normalize(path.join(agentB.workspacePath, "taskB.txt"));
      assert.ok(
        callB.prompt.includes("FILE: " + expectedWorktreeFileB),
        "Dispatched prompt B must reference file path inside worktree B",
      );
      assert.equal(callB.prompt.includes("taskA.txt"), false);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(repoDir, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Atomicity on Late Failure: Rollback leaves no orphan worktrees, agents, jobs, or envelopes
// ---------------------------------------------------------------------------
test("late failure during spawnBatch admission rolls back atomically leaving no orphan worktrees, agents, jobs, or envelopes", async () => {
  const repoDir = await mkdtemp(path.join(os.tmpdir(), "ds-batch-wt-rollback-repo-"));
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "ds-batch-wt-rollback-data-"));
  try {
    await createCleanGitRepo(repoDir, {
      "context.txt": "Committed context\n",
    });

    const store = new BridgeStore(path.join(dataDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      assert.equal(store.listJobs().length, 0);
      assert.equal(store.listAgents().length, 0);

      // Inject late constraint failure on dispatch_envelopes insertion
      store.db.exec(`
        CREATE TRIGGER fail_late_batch_envelope
        BEFORE INSERT ON dispatch_envelopes
        BEGIN
          SELECT RAISE(ABORT, 'injected late batch dispatch_envelope constraint failure');
        END;
      `);

      await assert.rejects(
        async () => {
          await service.spawnBatch({
            batchRequestId: "batch_fail_late",
            items: [
              {
                requestId: "req_fail_late_1",
                topic: "Fail Late Topic",
                task: "Fail Late Task",
                cwd: repoDir,
                workspaceStrategy: "worktree",
                contextFiles: ["context.txt"],
              },
            ],
          });
        },
        (err: unknown) => {
          return (
            err instanceof Error &&
            (/injected late batch dispatch_envelope constraint failure/i.test(err.message) ||
              /no such file or directory/i.test(err.message))
          );
        },
      );

      // Rollback must leave zero rows and zero worktrees
      assert.equal(store.listJobs().length, 0, "Atomic rollback must leave no orphan jobs");
      assert.equal(store.listAgents().length, 0, "Atomic rollback must leave no orphan agents");
      assert.equal(store.getBatchByRequestId("batch_fail_late"), null, "Atomic rollback must leave no batch");

      const envelopeCount = store.db.prepare("SELECT COUNT(*) AS count FROM dispatch_envelopes").get() as { count: number };
      assert.equal(Number(envelopeCount.count), 0, "Atomic rollback must leave no dispatch envelopes");

      const worktreesDir = path.join(repoDir, ".deepseek-worktrees");
      const worktreeEntries = existsSync(worktreesDir) ? await readdir(worktreesDir) : [];
      assert.equal(worktreeEntries.length, 0, "Atomic rollback must leave no worktrees on disk");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(repoDir, { recursive: true, force: true });
    await rm(dataDir, { recursive: true, force: true });
  }
});
