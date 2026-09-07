import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";
import { createDefaultConfig } from "../../src/config.js";
import { AntigravitySpool } from "../../src/antigravity/spool.js";
import { DEFAULT_CODEX_CAPABILITIES, type CodexBinding, type CodexDeliveryAdapter } from "../../src/codex/adapter.js";
import type { AgentRecord, JobRecord, OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage, WakeEnvelope } from "../../src/types.js";
import { ConflictError } from "../../src/errors.js";
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
      serverId: "fake_server",
      baseUrl: "http://127.0.0.1:40999",
      client: this.client,
      processId: 1234,
      status: () => "attached" as const,
      stop: async () => {},
    };
  }
  async stop() {}
}
class FakeCodexDelivery implements CodexDeliveryAdapter {
  available = true;
  reason: string | null = null;
  capabilities = { ...DEFAULT_CODEX_CAPABILITIES, authoritativeAttachment: true };
  deliveredWakes: Array<{ envelope: WakeEnvelope; binding: CodexBinding }> = [];
  reconciledCalls: Array<{ threadId: string; marker: string }> = [];
  async start(): Promise<void> {}
  async close(): Promise<void> {}
  async deliver(job: JobRecord, binding: CodexBinding, text: string): Promise<"codex-steer" | "codex-start"> {
    return "codex-start";
  }
  async deliverWake(envelope: WakeEnvelope, binding: CodexBinding): Promise<"codex-steer" | "codex-start"> {
    this.deliveredWakes.push({ envelope, binding });
    return "codex-start";
  }
  async reconcileSend(threadId: string, marker: string): Promise<boolean> {
    this.reconciledCalls.push({ threadId, marker });
    return false;
  }
  onCorrelation(_listener: (correlation: any) => void): () => void {
    return () => {};
  }
}
function fixtureAgent(agentId: string): AgentRecord {
  const now = new Date().toISOString();
  return {
    id: agentId,
    title: "Test Agent " + agentId,
    topic: "Semantic Progress Topic",
    repositoryRoot: "C:\\work",
    workspacePath: "C:\\work",
    workspaceStrategy: "shared",
    opencodeServerId: "antigravity",
    opencodeSessionId: "antigravity:" + agentId,
    modelProviderId: "antigravity",
    modelId: "gemini-3.8-flash-high",
    modelVariant: null,
    modelRoute: "antigravity-flash-high",
    parentAgentId: null,
    status: "working",
    createdAt: now,
    updatedAt: now,
    closedAt: null,
    lastError: null,
  };
}
function fixtureJob(jobId: string, agentId: string, sequence = 1, kind: "spawn" | "continue" = "spawn", fence = 1): JobRecord {
  const now = new Date().toISOString();
  return {
    id: jobId,
    agentId,
    sequence,
    kind,
    requestId: "req_" + jobId,
    promptHash: "hash_" + jobId,
    status: "running",
    createdAt: now,
    startedAt: now,
    completedAt: null,
    lastUserMessageId: null,
    lastAssistantMessageId: null,
    permissionId: null,
    resultPath: null,
    resultSummary: null,
    error: null,
    followStartedAt: null,
    followDeadlineAt: null,
    followGraceMinutes: null,
    graceDeadlineAt: null,
    gracefulFinalizeAttempted: false,
    approvalDeadlineAt: null,
    hintThreadId: "thread_authoritative",
    hintTurnId: "turn_1",
    hintSource: "test",
    dispatchUnknown: false,
    resultConsumedAt: null,
    leaseExpiresAt: null,
    attempt: "attempt_1",
    fence,
    workerPid: null,
    heartbeatAt: null,
  };
}
test("current job activity excludes prior job result and does not stage completed for running job", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-exclude-prior-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_prior_test"));
    const job1 = store.createJob(fixtureJob("job_prior_1", agent.id, 1, "spawn"));
    store.recordActivity({
      agentId: agent.id,
      jobId: job1.id,
      sessionId: agent.opencodeSessionId,
      activityType: "result",
      summary: "Antigravity run completed and the result was persisted",
    });
    store.updateJobStatus(job1.id, "completed");

    const job2 = store.createJob(fixtureJob("job_prior_2", agent.id, 2, "continue"));
    const job2StartTime = new Date(Date.now() - 5_000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ?, created_at = ? WHERE id = ?").run(job2StartTime, job2StartTime, job2.id);

    const snapshot = await service.consult({
      agentId: agent.id,
      jobId: job2.id,
    });

    assert.ok(snapshot.semanticProgress, "semanticProgress must be defined");
    assert.notEqual(snapshot.semanticProgress.stage, "completed", "running job must not stage completed from prior job result");
    assert.equal(snapshot.semanticProgress.stage, "executing", "running job with no activity should default to executing");
    assert.equal(snapshot.semanticProgress.isStalled, false, "job started 5s ago is not stalled");
    assert.ok(snapshot.lastActivityAgoSeconds !== null && snapshot.lastActivityAgoSeconds <= 10, "lastActivityAgoSeconds measured from job2 start");

    const hasJob1Result = snapshot.recentActivity.some((a) => a.summary.includes("run completed and the result was persisted"));
    assert.equal(hasJob1Result, false, "recentActivity for job2 must exclude job1 result");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("no heartbeat counts as work: fresh supervisor heartbeat separates from job progress and reports stall warning", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-no-heartbeat-work-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_hb_work_test"));
    const job = store.createJob(fixtureJob("job_hb_work_1", agent.id, 1, "spawn", 2));

    const startedAt = new Date(Date.now() - 350 * 1000).toISOString();
    const heartbeatAt = new Date(Date.now() - 2 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ?, heartbeat_at = ?, worker_pid = ?, attempt = ?, fence = ? WHERE id = ?")
      .run(startedAt, heartbeatAt, process.pid, "attempt_simulated", 2, job.id);

    const spool = new AntigravitySpool(directory);
    const attemptDir = spool.attemptDir(job.id, "attempt_simulated");
    await mkdir(attemptDir, { recursive: true });
    await writeFile(path.join(attemptDir, "manifest.json"), JSON.stringify({
      schemaVersion: 1,
      attemptId: "attempt_simulated",
      jobId: job.id,
      agentId: agent.id,
      requestId: job.requestId,
      createdAt: startedAt,
      cwd: directory,
      heartbeatPath: path.join(attemptDir, "heartbeat.json"),
      attemptDir,
    }));
    await spool.writeHeartbeat(attemptDir, {
      nonce: "test_nonce",
      supervisorPid: process.pid,
      agyPid: process.pid,
      updatedAt: Date.now() - 2 * 1000,
      timestamp: heartbeatAt,
    });

    const snapshot = await service.consult({
      agentId: agent.id,
      jobId: job.id,
    });

    assert.ok(snapshot.semanticProgress, "semanticProgress must be populated");
    assert.equal(snapshot.authoritativeStatus?.isLive, true, "supervisor process is live");
    assert.ok(snapshot.authoritativeStatus?.heartbeatAgoSeconds !== null && snapshot.authoritativeStatus.heartbeatAgoSeconds <= 5, "heartbeat is fresh");

    assert.notEqual(snapshot.semanticProgress.lastActiveAt, heartbeatAt, "no heartbeat counts as work: lastActiveAt must not be heartbeatAt");
    assert.equal(snapshot.semanticProgress.isStalled, true, "job with no activity for >300s is stalled even though process is live");
    assert.equal(snapshot.semanticProgress.stage, "stalled", "stage must be stalled");

    assert.ok(snapshot.semanticProgress.diagnosticEvidence, "diagnosticEvidence must be populated");
    assert.match(snapshot.semanticProgress.diagnosticEvidence!, /inactivity warning/i, "diagnostic evidence must mention inactivity warning");
    assert.match(snapshot.semanticProgress.diagnosticEvidence!, /not an execution timeout/i, "must specify NOT an execution timeout");
    assert.match(snapshot.semanticProgress.diagnosticEvidence!, /fence: 2/i, "must include current fence");
    assert.match(snapshot.semanticProgress.diagnosticEvidence!, /attempt_simulated/i, "must include current attempt");

    const refreshed = store.getJob(job.id);
    assert.equal(refreshed?.status, "running", "alert no jobterminalization: job must remain in running state");
    assert.equal(refreshed?.completedAt, null, "completedAt must be null");
    assert.equal(refreshed?.resultPath, null, "resultPath must be null");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("healthy slow progressing job produces no alert even after >900s", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-healthy-slow-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_healthy_slow"));
    const job = store.createJob(fixtureJob("job_healthy_slow_1", agent.id, 1, "spawn"));

    const startedAt = new Date(Date.now() - 1200 * 1000).toISOString();
    const heartbeatAt = new Date(Date.now() - 5 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ?, heartbeat_at = ?, worker_pid = ? WHERE id = ?")
      .run(startedAt, heartbeatAt, process.pid, job.id);

    store.recordActivity({
      agentId: agent.id,
      jobId: job.id,
      sessionId: agent.opencodeSessionId,
      activityType: "event",
      summary: "Modifying file src/service.ts with incremental patch",
    });

    const snapshot = await service.consult({
      agentId: agent.id,
      jobId: job.id,
    });

    assert.ok(snapshot.semanticProgress, "semanticProgress must be populated");
    assert.equal(snapshot.semanticProgress.isStalled, false, "healthy slow progressing job must NOT be stalled");
    assert.equal(snapshot.semanticProgress.stage, "modifying", "stage derived from recent activity");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("simulated waiting child triggers durable deduplicated metadata-only wake exception for parked ALL:/REQUIRED/QUORUM", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-wake-exception-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
    experimentalSameChatDelivery: true,
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_park_stall"));
    const job1 = store.createJob(fixtureJob("job_park_stall_1", agent.id, 1, "spawn", 3));
    const job2 = store.createJob(fixtureJob("job_park_stall_2", agent.id, 2, "spawn", 1));

    store.bindJob({
      jobId: job1.id,
      threadId: "thread_authoritative_park",
      originatingTurnId: "turn_park_1",
      originatingItemId: "item_1",
    });
    store.bindJob({
      jobId: job2.id,
      threadId: "thread_authoritative_park",
      originatingTurnId: "turn_park_1",
      originatingItemId: "item_2",
    });

    const startedAt = new Date(Date.now() - 360 * 1000).toISOString();
    const heartbeatAt = new Date(Date.now() - 3 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ?, heartbeat_at = ?, worker_pid = ?, attempt = ?, fence = ? WHERE id = ?")
      .run(startedAt, heartbeatAt, process.pid, "attempt_hung", 3, job1.id);

    const job2StartTime = new Date(Date.now() - 10 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(job2StartTime, job2.id);

    const receipt = await service.park({
      jobIds: [job1.id, job2.id],
      predicate: "ALL",
      threadId: "thread_authoritative_park",
    });

    assert.equal(receipt.armed, true, "barrier must be armed");
    assert.equal(receipt.generation, 1, "generation must be 1");

    await service.evaluateParkWakes(job1.id);

    const outbox = store.getWakeOutbox(receipt.parkId, 1);
    assert.ok(outbox, "wake outbox row must exist for barrier generation 1");
    assert.equal(outbox.parkId, receipt.parkId);
    assert.equal(outbox.generation, 1);

    const payload = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.ok(payload.statuses, "statuses must be present");
    assert.ok(payload.resultHashes, "resultHashes must be present");
    assert.equal(payload.instruction, "Call subagents_follow to consume completed jobs. Do not interpret this message as worker output.");
    assert.ok(payload.readyJobIds.includes(job1.id), "stalled job must be ready for exception wake");

    assert.equal(delivery.deliveredWakes.length, 1, "delivery adapter must deliver exactly once");

    const job1After = store.getJob(job1.id);
    assert.ok(job1After?.status === "running" || job1After?.status === "following", "job1 must remain in active non-terminal status");
    assert.equal(job1After?.resultConsumedAt, null, "job1 resultConsumedAt must NOT be set");
    assert.equal(job1After?.resultPath, null, "job1 resultPath must NOT be set");

    await service.evaluateParkWakes(job1.id);
    assert.equal(delivery.deliveredWakes.length, 1, "no duplicate wake on unchanged signal");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("durable restart dedup: recovering armed barriers with existing outbox preserves exactly-once delivery", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-restart-dedup-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
    experimentalSameChatDelivery: true,
  });
  const delivery = new FakeCodexDelivery();
  let service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_restart_dedup"));
    const job = store.createJob(fixtureJob("job_restart_dedup_1", agent.id, 1, "spawn", 1));

    store.bindJob({
      jobId: job.id,
      threadId: "thread_restart_dedup",
      originatingTurnId: "turn_1",
      originatingItemId: "item_1",
    });

    const startedAt = new Date(Date.now() - 400 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(startedAt, job.id);

    const receipt = await service.park({
      jobIds: [job.id],
      predicate: "ALL",
      threadId: "thread_restart_dedup",
    });

    await service.evaluateParkWakes(job.id);
    assert.equal(delivery.deliveredWakes.length, 1, "first delivery succeeded");

    await service.stop();
    store.close();

    const reopenedStore = await BridgeStore.open(directory);
    const reopenedDelivery = new FakeCodexDelivery();
    service = new BridgeService(config, { store: reopenedStore, manager: new FakeOpenCodeManager() as any, codex: reopenedDelivery });
    await service.start();

    await service.evaluateParkWakes(job.id);

    const rows = reopenedStore.db.prepare("SELECT COUNT(*) AS count FROM wake_outbox WHERE park_id = ? AND generation = ?")
      .get(receipt.parkId, 1) as { count: number };
    assert.equal(rows.count, 1, "exactly one outbox row exists across restart");
    assert.equal(reopenedDelivery.deliveredWakes.length, 0, "no duplicate wake delivered on restart");

    await service.stop();
    reopenedStore.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("stale fence ignored: advisory diagnostic update with stale fence does not crash or terminate job", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-stale-fence-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_stale_fence"));
    const job = store.createJob(fixtureJob("job_stale_fence_1", agent.id, 1, "spawn", 5));
    store.db.prepare("UPDATE jobs SET fence = 5 WHERE id = ?").run(job.id);

    assert.throws(() => {
      store.setJobEscalation(job.id, JSON.stringify({ reason: "stale write", advisoryOnly: true }), 2);
    }, ConflictError, "store throws ConflictError on stale fence");

    const startedAt = new Date(Date.now() - 350 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(startedAt, job.id);

    const snapshot = await service.consult({
      agentId: agent.id,
      jobId: job.id,
    });

    assert.ok(snapshot.semanticProgress, "snapshot must be generated without crashing");
    assert.equal(snapshot.semanticProgress.isStalled, true, "job is stalled");

    const current = store.getJob(job.id);
    assert.equal(current?.status, "running", "job remains in running state");
    assert.equal(current?.fence, 5, "fence remains intact");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("autonomous background wake fires without parent consult or manual evaluateParkWakes", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-autonomous-wake-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
    experimentalSameChatDelivery: true,
    inactivityThresholdSeconds: 0.2,
    advisoryCheckIntervalMs: 50,
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_auto_wake"));
    const job = store.createJob(fixtureJob("job_auto_wake_1", agent.id, 1, "spawn", 1));

    store.bindJob({
      jobId: job.id,
      threadId: "thread_auto_wake",
      originatingTurnId: "turn_1",
      originatingItemId: "item_1",
    });

    const receipt = await service.park({
      jobIds: [job.id],
      predicate: "ALL",
      threadId: "thread_auto_wake",
    });
    assert.equal(receipt.armed, true);

    const pastTime = new Date(Date.now() - 10_000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ?, created_at = ? WHERE id = ?").run(pastTime, pastTime, job.id);

    // Autonomous wait: do NOT call service.consult() or service.evaluateParkWakes()!
    const deadline = Date.now() + 3000;
    while (delivery.deliveredWakes.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    assert.equal(delivery.deliveredWakes.length, 1, "background wake must deliver autonomously without consult or evaluateParkWakes");
    const delivered = delivery.deliveredWakes[0]!;
    assert.equal(delivered.envelope.parkId, receipt.parkId);
    assert.ok(delivered.envelope.readyJobIds.includes(job.id));

    const refreshed = store.getJob(job.id);
    assert.ok(refreshed && ["running", "following"].includes(refreshed.status), "job must remain in active non-terminal status");
    assert.equal(refreshed?.resultConsumedAt, null, "resultConsumedAt must be null");
    assert.equal(refreshed?.resultPath, null, "resultPath must be null");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("advancing output beyond byte cap keeps slow job healthy after >900s", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-byte-cap-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_byte_cap"));
    const job = store.createJob(fixtureJob("job_byte_cap_1", agent.id, 1, "spawn"));

    const startedAt = new Date(Date.now() - 1200 * 1000).toISOString();
    const freshProgressAt = new Date(Date.now() - 5 * 1000).toISOString();

    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(startedAt, job.id);
    store.updateJobProgress(job.id, {
      lastProgressAt: freshProgressAt,
      progressRevision: 50,
      fence: 1,
    });

    const refreshed = store.getJob(job.id)!;
    assert.equal(refreshed.progressRevision, 50);
    assert.equal(refreshed.lastProgressAt, freshProgressAt);
    assert.equal(service.isJobStalled(refreshed), false, "advancing output with fresh progress revision must not stall");

    const snapshot = await service.consult({
      agentId: agent.id,
      jobId: job.id,
    });

    assert.equal(snapshot.semanticProgress?.isStalled, false, "consult snapshot must report not stalled");
    assert.equal(snapshot.semanticProgress?.stage, "executing");
    assert.equal(refreshed.escalationProposal, null, "no escalation proposal should be created");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("recovered progress clears suspicion and permits new alert on subsequent stall", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-recovery-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_recovery"));
    const job = store.createJob(fixtureJob("job_recovery_1", agent.id, 1, "spawn", 1));

    const stallTime = new Date(Date.now() - 350 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(stallTime, job.id);

    const initialJob = store.getJob(job.id)!;
    assert.equal(service.isJobStalled(initialJob), true, "initial inactivity must report stalled");

    const stalledJob = store.getJob(job.id)!;
    assert.ok(stalledJob.escalationProposal, "escalation proposal must be recorded on stall");
    const initialProposal = JSON.parse(stalledJob.escalationProposal!);
    assert.equal(initialProposal.advisoryOnly, true);

    const recoveryTime = new Date().toISOString();
    store.updateJobProgress(job.id, {
      lastProgressAt: recoveryTime,
      progressRevision: 1,
      fence: 1,
    });

    const recoveredJob = store.getJob(job.id)!;
    assert.equal(recoveredJob.escalationProposal, null, "updateJobProgress must clear escalationProposal");
    assert.equal(service.isJobStalled(recoveredJob), false, "recovered job must NOT be stalled");

    const subsequentStallTime = new Date(Date.now() - 360 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET last_progress_at = ? WHERE id = ?").run(subsequentStallTime, job.id);

    const subsequentJob = store.getJob(job.id)!;
    assert.equal(service.isJobStalled(subsequentJob), true, "subsequent stall must report stalled again");

    const reStalledJob = store.getJob(job.id)!;
    assert.ok(reStalledJob.escalationProposal, "new escalation proposal must be recorded on subsequent stall");
    const newProposal = JSON.parse(reStalledJob.escalationProposal!);
    assert.equal(newProposal.progressRevision, 1, "proposal must capture current progress revision");
    assert.notEqual(newProposal.alertedAt, initialProposal.alertedAt, "alertedAt must be refreshed");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("fresh attempt ignores stale proposal from prior attempt and fence", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-fresh-attempt-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_fresh_attempt"));
    const job = store.createJob(fixtureJob("job_fresh_attempt_1", agent.id, 1, "spawn", 1));

    const staleProposal = {
      reason: "Inactivity warning for attempt_1",
      advisoryOnly: true,
      suggestedAction: "inspect_worker_process",
      diagnosticEvidence: "stale evidence",
      attempt: "attempt_1",
      fence: 1,
      progressRevision: 0,
      alertedAt: new Date(Date.now() - 500 * 1000).toISOString(),
    };
    store.setJobEscalation(job.id, JSON.stringify(staleProposal), 1);

    const freshStartTime = new Date(Date.now() - 5 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET attempt = ?, fence = ?, started_at = ?, last_progress_at = NULL WHERE id = ?")
      .run("attempt_2", 2, freshStartTime, job.id);

    const freshJob = store.getJob(job.id)!;
    assert.equal(freshJob.attempt, "attempt_2");
    assert.equal(freshJob.fence, 2);

    assert.equal(service.isJobStalled(freshJob), false, "fresh attempt must ignore stale proposal from prior attempt/fence");

    const snapshot = await service.consult({
      agentId: agent.id,
      jobId: job.id,
    });
    assert.equal(snapshot.semanticProgress?.isStalled, false, "consult must report not stalled for fresh attempt");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("ALL waiting advisory wakes without consuming results and leaves jobs in active state", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-all-waiting-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
    experimentalSameChatDelivery: true,
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_all_waiting"));
    const job1 = store.createJob(fixtureJob("job_all_1", agent.id, 1, "spawn", 1));
    const job2 = store.createJob(fixtureJob("job_all_2", agent.id, 2, "spawn", 1));

    store.bindJob({
      jobId: job1.id,
      threadId: "thread_all_waiting",
      originatingTurnId: "turn_1",
      originatingItemId: "item_1",
    });
    store.bindJob({
      jobId: job2.id,
      threadId: "thread_all_waiting",
      originatingTurnId: "turn_1",
      originatingItemId: "item_2",
    });

    const stallTime = new Date(Date.now() - 350 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(stallTime, job1.id);
    const healthyTime = new Date(Date.now() - 10 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(healthyTime, job2.id);

    const receipt = await service.park({
      jobIds: [job1.id, job2.id],
      predicate: "ALL",
      threadId: "thread_all_waiting",
    });
    assert.equal(receipt.armed, true);

    await service.evaluateParkWakes(job1.id);

    assert.equal(delivery.deliveredWakes.length, 1, "wake must be delivered for stalled child in ALL predicate");
    const wake = delivery.deliveredWakes[0]!;
    assert.deepEqual(wake.envelope.readyJobIds, [job1.id]);
    assert.equal(wake.envelope.pendingCount, 1);

    const refreshed1 = store.getJob(job1.id)!;
    const refreshed2 = store.getJob(job2.id)!;
    assert.ok(["running", "following"].includes(refreshed1.status), "job1 must remain in active non-terminal status");
    assert.equal(refreshed1.resultConsumedAt, null, "job1 resultConsumedAt must be null");
    assert.equal(refreshed1.resultPath, null, "job1 resultPath must be null");
    assert.ok(["running", "following"].includes(refreshed2.status), "job2 must remain in active non-terminal status");
    assert.equal(refreshed2.resultConsumedAt, null, "job2 resultConsumedAt must be null");
    assert.equal(refreshed2.resultPath, null, "job2 resultPath must be null");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("unchanged repark generations deduplicated across reparks", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "semantic-progress-repark-dedup-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
    experimentalSameChatDelivery: true,
  });
  const delivery = new FakeCodexDelivery();
  const service = new BridgeService(config, { store, manager: new FakeOpenCodeManager() as any, codex: delivery });
  await service.start();

  try {
    const agent = store.createAgent(fixtureAgent("agent_repark"));
    const job = store.createJob(fixtureJob("job_repark_1", agent.id, 1, "spawn", 1));

    store.bindJob({
      jobId: job.id,
      threadId: "thread_repark",
      originatingTurnId: "turn_1",
      originatingItemId: "item_1",
    });

    const stallTime = new Date(Date.now() - 350 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(stallTime, job.id);

    const receipt1 = await service.park({
      jobIds: [job.id],
      predicate: "ALL",
      threadId: "thread_repark",
    });
    assert.equal(receipt1.generation, 1);

    await service.evaluateParkWakes(job.id);
    assert.equal(delivery.deliveredWakes.length, 1, "generation 1 delivered");

    const receipt2 = await service.park({
      jobIds: [job.id],
      predicate: "ALL",
      threadId: "thread_repark",
    });
    assert.equal(receipt2.generation, 2);

    await service.evaluateParkWakes(job.id);
    assert.equal(delivery.deliveredWakes.length, 1, "generation 2 wake must be deduplicated when stall state is unchanged");

    store.updateJobProgress(job.id, {
      lastProgressAt: new Date(Date.now() - 360 * 1000).toISOString(),
      progressRevision: 1,
      fence: 1,
    });
    service.isJobStalled(store.getJob(job.id)!);

    await service.evaluateParkWakes(job.id);
    assert.equal(delivery.deliveredWakes.length, 2, "generation 2 wake delivers once stall state changes");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
