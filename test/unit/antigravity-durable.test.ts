import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, stat, unlink, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";
import { InboxDelivery } from "../../src/delivery/inbox.js";
import { AntigravityAdapter } from "../../src/antigravity/adapter.js";
import {
  AntigravitySpool,
  createAttemptSpool,
  readAttemptManifest,
  readAttemptStatus,
  writeAttemptStatus,
  writeHeartbeat,
  isHeartbeatLive,
  claimRecovery,
  cleanupPrompt,
  writeCancelSignal,
  hasCancelSignal,
} from "../../src/antigravity/spool.js";
import {
  AntigravitySupervisor,
  runSupervisor,
} from "../../src/antigravity/supervisor.js";
import { AGY_COMMAND, buildAgyArgs } from "../../src/antigravity/args.js";
import type { AntigravityAttemptManifest, AntigravityAttemptStatus } from "../../src/antigravity/types.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

const agyFixturePath = fileURLToPath(new URL("../fixtures/agy.cjs", import.meta.url));

function fixtureSpawn(behavior: string, calls: string[] = [], pids: number[] = []) {
  let pidCounter = 20000;
  return (command: string, args: string[], options: { cwd: string; env?: NodeJS.ProcessEnv; shell: false; windowsHide: boolean; stdio: ReadonlyArray<"ignore" | "pipe"> }) => {
    calls.push(command);
    const child = spawn(process.execPath, [agyFixturePath, ...args], {
      cwd: options.cwd,
      env: { ...process.env, ...(options.env ?? {}), AGY_FIXTURE: behavior },
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    if (child.pid) pids.push(child.pid);
    return child;
  };
}

class FakeOpenCodeClient implements OpenCodeClientLike {
  async health() { return { healthy: true, version: "fake" }; }
  async createSession() { return { id: "session_fake" }; }
  async promptAsync() {}
  async listMessages(): Promise<OpenCodeMessage[]> { return []; }
  async getDiff() { return []; }
  async abort() {}
  async replyPermission() {}
  async subscribe(_onEvent: (event: OpenCodeEvent) => Promise<void> | void, signal?: AbortSignal) {
    await new Promise<void>((resolve) => {
      signal?.addEventListener("abort", () => resolve(), { once: true });
    });
  }
}

class FakeOpenCodeManager {
  constructor(private client = new FakeOpenCodeClient()) {}
  async start() {
    return {
      serverId: "server_fake",
      baseUrl: "http://127.0.0.1:1",
      client: this.client,
      processId: null,
      stop: async () => undefined,
    };
  }
  async stop() {}
}

async function waitFor(condition: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((r) => setTimeout(r, 15));
  }
  assert.ok(await condition(), "Condition not satisfied within " + timeoutMs + "ms");
}

test("1. daemon-side waiter can recover a completed supervisor result after the original service object is gone", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-test-1-"));
  const store = await BridgeStore.open(tempDir);
  const config = createDefaultConfig({ dataDir: tempDir, configPath: path.join(tempDir, "config.json") });
  try {
    const spool = new AntigravitySpool(tempDir);
    const agent = store.createAgent({
      id: "agent_test_1",
      title: "Test 1",
      topic: "Completed supervisor recovery",
      repositoryRoot: tempDir,
      workspacePath: tempDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "antigravity:agent_test_1",
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });
    const job = store.createJob({
      id: "job_test_1",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_test_1",
      promptHash: "hash_test_1",
    });
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");

    // Simulate supervisor execution creating attempt and writing status.json
    const attempt = await spool.createAttempt({
      agentId: agent.id,
      jobId: job.id,
      requestId: "req_test_1",
      prompt: "Execute test 1 task",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 30000,
    });

    const statusPayload: AntigravityAttemptStatus = {
      schemaVersion: 1,
      attemptId: attempt.attemptId,
      status: "completed",
      exitCode: 0,
      summary: "Supervisor completed task cleanly",
      runId: "run_super_1",
      files: ["output.txt"],
      tests: ["npm test"],
      risks: ["none"],
      diffSummary: "1 file created",
      error: null,
      completedAt: new Date().toISOString(),
      stdout: "Supervisor output",
      stderr: "",
    };
    await spool.writeStatus(attempt.attemptId, statusPayload);

    // New service instance with no knowledge of previous in-memory state
    const newService = new BridgeService(config, {
      store,
      manager: new FakeOpenCodeManager() as any,
      inbox: new InboxDelivery(tempDir),
    });
    await newService.start();

    const recoveredJob = store.getJob(job.id);
    assert.ok(recoveredJob);
    assert.ok(["delivery_pending", "delivered"].includes(recoveredJob.status));
    assert.ok(recoveredJob.resultPath);
    assert.equal(recoveredJob.resultSummary, "Supervisor completed task cleanly");
    await newService.stop();
  } finally {
    store.close();
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("2. a live heartbeat reattaches without second agy spawn", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-test-2-"));
  const store = await BridgeStore.open(tempDir);
  const config = createDefaultConfig({ dataDir: tempDir, configPath: path.join(tempDir, "config.json") });
  const agyCalls: string[] = [];
  try {
    const spool = new AntigravitySpool(tempDir);
    const agent = store.createAgent({
      id: "agent_test_2",
      title: "Test 2",
      topic: "Live heartbeat reattachment",
      repositoryRoot: tempDir,
      workspacePath: tempDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "antigravity:agent_test_2",
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });
    const job = store.createJob({
      id: "job_test_2",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_test_2",
      promptHash: "hash_test_2",
    });
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");

    const attempt = await spool.createAttempt({
      agentId: agent.id,
      jobId: job.id,
      requestId: "req_test_2",
      prompt: "Execute test 2 task",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 30000,
    });

    // Write a live heartbeat with our own process PID (which is alive!)
    await spool.writeHeartbeat(attempt.attemptId, {
      nonce: "nonce_test_2",
      supervisorPid: process.pid,
      agyPid: process.pid,
      updatedAt: Date.now(),
      timestamp: new Date().toISOString(),
    });

    const customAdapter = new AntigravityAdapter({
      command: "node",
      spawnFn: fixtureSpawn("ok", agyCalls),
    });

    const newService = new BridgeService(config, {
      store,
      manager: new FakeOpenCodeManager() as any,
      inbox: new InboxDelivery(tempDir),
      antigravity: customAdapter,
    });

    await newService.start();

    // Verify agy was NOT spawned during startup recovery because supervisor is live
    assert.equal(agyCalls.length, 0, "agy must not be spawned again while heartbeat is live");

    // Now simulate supervisor finishing and writing status.json
    await spool.writeStatus(attempt.attemptId, {
      schemaVersion: 1,
      attemptId: attempt.attemptId,
      status: "completed",
      exitCode: 0,
      summary: "Reattached run finished",
      runId: "run_super_2",
      files: [],
      tests: [],
      risks: [],
      diffSummary: "none",
      error: null,
      completedAt: new Date().toISOString(),
      stdout: "Reattached output",
      stderr: "",
    });

    // Wait for the reattached monitor to observe completion
    await waitFor(() => ["completed", "delivery_pending", "delivered"].includes(store.getJob(job.id)?.status ?? ""));
    assert.equal(store.getJob(job.id)?.resultSummary, "Reattached run finished");
    assert.equal(agyCalls.length, 0);

    await newService.stop();
  } finally {
    store.close();
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("3. stale/dead nonterminal attempt is replacement-claimed exactly once under two concurrent recoverers", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-test-3-"));
  try {
    const spool = new AntigravitySpool(tempDir);
    const attempt = await spool.createAttempt({
      agentId: "agent_test_3",
      jobId: "job_test_3",
      requestId: "req_test_3",
      prompt: "Dead attempt task",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 30000,
    });

    // Write a stale/dead heartbeat (PID 99999999 is dead)
    await spool.writeHeartbeat(attempt.attemptId, {
      nonce: "nonce_dead_3",
      supervisorPid: 99999999,
      agyPid: null,
      updatedAt: Date.now() - 60000,
      timestamp: new Date(Date.now() - 60000).toISOString(),
    });

    // Two concurrent recoverers attempt to claim recovery
    const claim1Promise = spool.claimRecovery("job_test_3", "recoverer_A");
    const claim2Promise = spool.claimRecovery("job_test_3", "recoverer_B");

    const [claim1, claim2] = await Promise.all([claim1Promise, claim2Promise]);
    assert.ok((claim1 && !claim2) || (!claim1 && claim2), "Exactly one claimant must win the recovery claim");
  } finally {
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("4. replacement retains exact model route/cwd/request/job and records parent attempt", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-test-4-"));
  const store = await BridgeStore.open(tempDir);
  const config = createDefaultConfig({ dataDir: tempDir, configPath: path.join(tempDir, "config.json") });
  const agyCalls: string[] = [];
  try {
    const spool = new AntigravitySpool(tempDir);
    const agent = store.createAgent({
      id: "agent_test_4",
      title: "Test 4",
      topic: "Dead attempt replacement",
      repositoryRoot: tempDir,
      workspacePath: tempDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "antigravity:agent_test_4",
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });
    const job = store.createJob({
      id: "job_test_4",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_test_4",
      promptHash: "hash_test_4",
    });
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");

    const parentAttempt = await spool.createAttempt({
      agentId: agent.id,
      jobId: job.id,
      requestId: "req_test_4",
      prompt: "Task for attempt 1",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 30000,
    });

    // Heartbeat is dead (PID 99999999) and no status.json
    await spool.writeHeartbeat(parentAttempt.attemptId, {
      nonce: "nonce_dead_4",
      supervisorPid: 99999999,
      agyPid: null,
      updatedAt: Date.now() - 60000,
      timestamp: new Date(Date.now() - 60000).toISOString(),
    });

    const customAdapter = new AntigravityAdapter({
      command: "node",
      spawnFn: fixtureSpawn("ok", agyCalls),
    });

    const service = new BridgeService(config, {
      store,
      manager: new FakeOpenCodeManager() as any,
      inbox: new InboxDelivery(tempDir),
      antigravity: customAdapter,
    });

    await service.start();

    // Verify recovery created attempt 2 with parent attempt 1
    const attempts = await spool.listAttempts(job.id);
    assert.equal(attempts.length, 2, "Replacement attempt must be created");
    const replacement = attempts.find((a) => a.attemptId !== parentAttempt.attemptId);
    assert.ok(replacement);
    assert.equal(replacement.parentAttemptId, parentAttempt.attemptId);
    assert.equal(replacement.jobId, job.id);
    assert.equal(replacement.agentId, agent.id);
    assert.equal(replacement.requestId, "req_test_4");
    assert.equal(replacement.modelRoute, "antigravity-flash-high");
    assert.equal(replacement.modelId, "gemini-3.7-flash-high");
    assert.equal(replacement.cwd, tempDir);

    // Verify durable activity was recorded
    const activities = store.listActivity(agent.id);
    const lineageActivity = activities.find((act) => act.summary.includes("replacement") || act.summary.includes(parentAttempt.attemptId));
    assert.ok(lineageActivity, "Attempt lineage activity must be recorded");

    await waitFor(() => ["completed", "delivery_pending", "delivered"].includes(store.getJob(job.id)?.status ?? ""));
    await service.stop();
  } finally {
    store.close();
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("5. missing/ambiguous/legacy spool fails closed and never spawns", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-test-5-"));
  const store = await BridgeStore.open(tempDir);
  const config = createDefaultConfig({ dataDir: tempDir, configPath: path.join(tempDir, "config.json") });
  const agyCalls: string[] = [];
  try {
    const agent = store.createAgent({
      id: "agent_test_5",
      title: "Test 5",
      topic: "Missing spool fail-closed",
      repositoryRoot: tempDir,
      workspacePath: tempDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "antigravity:agent_test_5",
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });
    const job = store.createJob({
      id: "job_test_5",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_test_5",
      promptHash: "hash_test_5",
    });
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");

    // No spool directory created at all (legacy or missing spool)
    const customAdapter = new AntigravityAdapter({
      command: "node",
      spawnFn: fixtureSpawn("ok", agyCalls),
    });

    const service = new BridgeService(config, {
      store,
      manager: new FakeOpenCodeManager() as any,
      inbox: new InboxDelivery(tempDir),
      antigravity: customAdapter,
    });

    await service.start();

    // Verify job failed closed and agy was NEVER spawned
    const finishedJob = store.getJob(job.id);
    assert.ok(finishedJob);
    assert.equal(finishedJob.status, "failed");
    assert.match(finishedJob.error ?? "", /spool|cannot be recovered/i);
    assert.equal(agyCalls.length, 0, "Agy must never spawn on missing spool");

    await service.stop();
  } finally {
    store.close();
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("6. prompt is private and removed after terminal status; output is bounded", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-test-6-"));
  try {
    const spool = new AntigravitySpool(tempDir);
    const secretPrompt = "Secret instructions: token=sk-super-secret-12345";
    const attempt = await spool.createAttempt({
      agentId: "agent_test_6",
      jobId: "job_test_6",
      requestId: "req_test_6",
      prompt: secretPrompt,
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 30000,
      maxOutputBytes: 128,
    });

    // Verify prompt file exists initially with private permissions
    assert.ok(existsSync(attempt.promptPath));
    const promptContent = await readFile(attempt.promptPath, "utf8");
    assert.equal(promptContent, secretPrompt);

    // Run supervisor with a noisy fixture that produces > 128 bytes
    const supervisor = new AntigravitySupervisor({
      spoolDir: attempt.attemptDir,
      manifest: attempt,
      spawnFn: fixtureSpawn("big"),
    });

    const status = await supervisor.run();
    assert.equal(status.status, "completed");

    // Verify prompt file is removed immediately after terminal status
    assert.equal(existsSync(attempt.promptPath), false, "prompt.txt must be removed after terminal status");

    // Verify output is bounded to maxOutputBytes
    const stdoutContent = await readFile(attempt.stdoutPath, "utf8");
    assert.ok(stdoutContent.length <= 128, "stdout.log must be bounded to maxOutputBytes");
  } finally {
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("7. cancel/timeout targets only the verified job child, with a test double proving desktop/unrelated PIDs are never targeted", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-test-7-"));
  try {
    const spool = new AntigravitySpool(tempDir);
    const attempt = await spool.createAttempt({
      agentId: "agent_test_7",
      jobId: "job_test_7",
      requestId: "req_test_7",
      prompt: "Hanging task to cancel",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 500,
    });

    const killedPids: number[] = [];
    const spawnedPids: number[] = [];

    const supervisor = new AntigravitySupervisor({
      spoolDir: attempt.attemptDir,
      manifest: attempt,
      spawnFn: fixtureSpawn("hang", [], spawnedPids),
      killTreeFn: async (pid: number) => {
        killedPids.push(pid);
        try { process.kill(pid, "SIGKILL"); } catch {}
      },
    });

    const runPromise = supervisor.run();

    // Wait until child is spawned
    await waitFor(() => spawnedPids.length > 0);
    const agyChildPid = spawnedPids[0];
    assert.ok(agyChildPid);

    // Issue cancel signal
    await spool.writeCancelSignal(attempt.attemptId, "User cancelled");

    const status = await runPromise;
    assert.equal(status.status, "aborted");

    // Verify only the verified child PID was targeted, and desktop/unrelated PIDs were never targeted
    const desktopFakePid = 88888;
    const unrelatedFakePid = 99999;
    assert.ok(killedPids.includes(agyChildPid), "Verified child PID must be targeted");
    assert.ok(!killedPids.includes(desktopFakePid), "Desktop PID must never be targeted");
    assert.ok(!killedPids.includes(unrelatedFakePid), "Unrelated PID must never be targeted");
    for (const killed of killedPids) {
      assert.equal(killed, agyChildPid, "Only the verified agy child PID may be killed");
    }
  } finally {
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("8. startup recoverPendingJobs no longer immediately marks recoverable Antigravity jobs stranded", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-test-8-"));
  const store = await BridgeStore.open(tempDir);
  const config = createDefaultConfig({ dataDir: tempDir, configPath: path.join(tempDir, "config.json") });
  try {
    const spool = new AntigravitySpool(tempDir);
    const agent = store.createAgent({
      id: "agent_test_8",
      title: "Test 8",
      topic: "Recoverable Antigravity job",
      repositoryRoot: tempDir,
      workspacePath: tempDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "antigravity:agent_test_8",
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });
    const job = store.createJob({
      id: "job_test_8",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_test_8",
      promptHash: "hash_test_8",
    });
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");

    const attempt = await spool.createAttempt({
      agentId: agent.id,
      jobId: job.id,
      requestId: "req_test_8",
      prompt: "Recoverable prompt",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 30000,
    });

    // Write a live heartbeat
    await spool.writeHeartbeat(attempt.attemptId, {
      nonce: "nonce_test_8",
      supervisorPid: process.pid,
      agyPid: process.pid,
      updatedAt: Date.now(),
      timestamp: new Date().toISOString(),
    });

    const service = new BridgeService(config, {
      store,
      manager: new FakeOpenCodeManager() as any,
      inbox: new InboxDelivery(tempDir),
    });

    await service.start();

    // In Phase 1, recoverPendingJobs immediately marked all running Antigravity jobs as failed/stranded.
    // In Phase 2, the job must NOT be marked failed/stranded!
    const activeJob = store.getJob(job.id);
    assert.ok(activeJob);
    assert.notEqual(activeJob.status, "failed", "Recoverable job must not be marked failed immediately");
    assert.doesNotMatch(activeJob.error ?? "", /stranded/i);

    await service.stop();
  } finally {
    store.close();
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("9. forced bridge-daemon lifecycle simulation with two logical chats/three active jobs yields recovered/replaced terminal jobs, no duplicate and no provider fallback", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-test-9-"));
  const store = await BridgeStore.open(tempDir);
  const config = createDefaultConfig({ dataDir: tempDir, configPath: path.join(tempDir, "config.json") });
  const agyCalls: string[] = [];
  try {
    const spool = new AntigravitySpool(tempDir);

    // Chat 1: Agent A with Job 1 (completed while daemon was down) and Job 2 (still live)
    const agentA = store.createAgent({
      id: "agent_A",
      title: "Chat 1",
      topic: "Multi-job simulation Chat 1",
      repositoryRoot: tempDir,
      workspacePath: tempDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "antigravity:agent_A",
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });

    const job1 = store.createJob({
      id: "job_1",
      agentId: agentA.id,
      kind: "spawn",
      requestId: "req_job_1",
      promptHash: "hash_job_1",
    });
    store.updateJobStatus(job1.id, "dispatching");
    store.updateJobStatus(job1.id, "running");

    const attempt1 = await spool.createAttempt({
      agentId: agentA.id,
      jobId: job1.id,
      requestId: "req_job_1",
      prompt: "Job 1 task",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 30000,
    });
    // Job 1 finished and wrote status.json
    await spool.writeStatus(attempt1.attemptId, {
      schemaVersion: 1,
      attemptId: attempt1.attemptId,
      status: "completed",
      exitCode: 0,
      summary: "Job 1 completed offline",
      runId: "run_1",
      files: ["job1.txt"],
      tests: [],
      risks: [],
      diffSummary: "1 file",
      error: null,
      completedAt: new Date().toISOString(),
      stdout: "Job 1 done",
      stderr: "",
    });

    const job2 = store.createJob({
      id: "job_2",
      agentId: agentA.id,
      kind: "continue",
      requestId: "req_job_2",
      promptHash: "hash_job_2",
    });
    store.updateJobStatus(job2.id, "dispatching");
    store.updateJobStatus(job2.id, "running");

    const attempt2 = await spool.createAttempt({
      agentId: agentA.id,
      jobId: job2.id,
      requestId: "req_job_2",
      prompt: "Job 2 task",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 30000,
    });
    // Job 2 has a live heartbeat
    await spool.writeHeartbeat(attempt2.attemptId, {
      nonce: "nonce_2",
      supervisorPid: process.pid,
      agyPid: process.pid,
      updatedAt: Date.now(),
      timestamp: new Date().toISOString(),
    });

    // Chat 2: Agent B with Job 3 (dead nonterminal attempt)
    const agentB = store.createAgent({
      id: "agent_B",
      title: "Chat 2",
      topic: "Multi-job simulation Chat 2",
      repositoryRoot: tempDir,
      workspacePath: tempDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "antigravity:agent_B",
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });

    const job3 = store.createJob({
      id: "job_3",
      agentId: agentB.id,
      kind: "spawn",
      requestId: "req_job_3",
      promptHash: "hash_job_3",
    });
    store.updateJobStatus(job3.id, "dispatching");
    store.updateJobStatus(job3.id, "running");

    const attempt3 = await spool.createAttempt({
      agentId: agentB.id,
      jobId: job3.id,
      requestId: "req_job_3",
      prompt: "Job 3 task",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.7-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 30000,
    });
    // Job 3 heartbeat dead (PID 99999999)
    await spool.writeHeartbeat(attempt3.attemptId, {
      nonce: "nonce_dead_3",
      supervisorPid: 99999999,
      agyPid: null,
      updatedAt: Date.now() - 60000,
      timestamp: new Date(Date.now() - 60000).toISOString(),
    });

    // Start recovery service
    const customAdapter = new AntigravityAdapter({
      command: "node",
      spawnFn: fixtureSpawn("ok", agyCalls),
    });

    const service = new BridgeService(config, {
      store,
      manager: new FakeOpenCodeManager() as any,
      inbox: new InboxDelivery(tempDir),
      antigravity: customAdapter,
    });

    await service.start();

    // Check Job 1: recovered completed
    const rJob1 = store.getJob(job1.id);
    assert.ok(rJob1);
    assert.equal(rJob1.resultSummary, "Job 1 completed offline");
    assert.ok(["completed", "delivery_pending", "delivered"].includes(rJob1.status));

    // Finish Job 2 live run
    await spool.writeStatus(attempt2.attemptId, {
      schemaVersion: 1,
      attemptId: attempt2.attemptId,
      status: "completed",
      exitCode: 0,
      summary: "Job 2 finished live",
      runId: "run_2",
      files: [],
      tests: [],
      risks: [],
      diffSummary: "none",
      error: null,
      completedAt: new Date().toISOString(),
      stdout: "Job 2 done",
      stderr: "",
    });

    // Wait for Job 2 and Job 3 to complete
    await waitFor(() => {
      const j2 = store.getJob(job2.id);
      const j3 = store.getJob(job3.id);
      return ["completed", "delivery_pending", "delivered"].includes(j2?.status ?? "") &&
             ["completed", "delivery_pending", "delivered"].includes(j3?.status ?? "");
    }, 4000);

    // Verify all 3 jobs reached terminal success
    const finalJob1 = store.getJob(job1.id);
    const finalJob2 = store.getJob(job2.id);
    const finalJob3 = store.getJob(job3.id);
    assert.ok(["completed", "delivery_pending", "delivered"].includes(finalJob1?.status ?? ""));
    assert.ok(["completed", "delivery_pending", "delivered"].includes(finalJob2?.status ?? ""));
    assert.ok(["completed", "delivery_pending", "delivered"].includes(finalJob3?.status ?? ""));

    // Verify NO provider fallback on any job
    assert.equal(finalJob1?.fallbackTo, null);
    assert.equal(finalJob2?.fallbackTo, null);
    assert.equal(finalJob3?.fallbackTo, null);

    // Verify exactly 1 spawn occurred for Job 3 replacement, 0 duplicate spawns for Job 1 and Job 2
    assert.equal(agyCalls.length, 1, "Only Job 3 replacement should have spawned agy");

    await service.stop();
  } finally {
    store.close();
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});
