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
import { BridgeService, type ManagedOpenCodeLike } from "../../src/service.js";
import { hashPrompt } from "../../src/security.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

const execFileAsync = promisify(execFile);

let globalSessionCounter = 0;

class FakeOpenCodeClient implements OpenCodeClientLike {
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  createdSessions: Array<{ id: string; directory?: string; title?: string }> = [];
  messages: OpenCodeMessage[] = [];
  activeSessions = new Set<string>();
  private onEvent?: (event: OpenCodeEvent) => Promise<void> | void;

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
  }
  async replyPermission() {}
  async subscribe(onEvent: (event: OpenCodeEvent) => Promise<void> | void) {
    this.onEvent = onEvent;
  }
  async emit(event: OpenCodeEvent) {
    await this.onEvent?.(event);
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

      // Verify real prompt constructed at dispatch via OpenCode inline-context path
      const promptCall = client.promptCalls.find((c) => c.sessionId === agent.opencodeSessionId);
      assert.ok(promptCall, "Client must receive prompt call for the agent session upon dispatch");
      const dispatchedPrompt = promptCall.task;

      assert.ok(
        dispatchedPrompt.includes("Committed notes for worktree batch test"),
        "Dispatched prompt must inline the context file content from the worktree",
      );
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

      const callA = client.promptCalls.find((c) => c.sessionId === agentA.opencodeSessionId);
      assert.ok(callA);
      assert.ok(callA.task.includes("Context A payload"));
      assert.equal(callA.task.includes("Context B payload"), false);

      // Complete item A to dispatch item B
      store.updateJobStatus(itemA.jobId, "completed");
      await (service as any).drainQueue();

      assert.equal(existsSync(agentB.workspacePath), true);

      const callB = client.promptCalls.find((c) => c.sessionId === agentB.opencodeSessionId);
      assert.ok(callB);
      assert.ok(callB.task.includes("Context B payload"));
      assert.equal(callB.task.includes("Context A payload"), false);
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
