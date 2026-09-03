import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";
import { persistResult, persistAntigravityResult, sanitizePersistedEnvelope } from "../../src/result.js";
import { parseAgyOutput } from "../../src/antigravity/parser.js";
import { ConflictError } from "../../src/errors.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeService } from "../../src/service.js";
import type { AgentRecord, JobRecord, OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

function fixtureAgent(agentId: string, modelRoute = "flash-max"): AgentRecord {
  return {
    id: agentId,
    title: agentId,
    topic: "Adaptive Test Topic",
    repositoryRoot: "C:\\work",
    workspacePath: "C:\\work",
    workspaceStrategy: "shared",
    opencodeServerId: "server_test",
    opencodeSessionId: "session_" + agentId,
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    modelRoute,
    parentAgentId: null,
    status: "working",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    closedAt: null,
    lastError: null,
  };
}

function fixtureJob(jobId: string, agentId: string, fence = 1): JobRecord {
  return {
    id: jobId,
    agentId,
    sequence: 1,
    kind: "spawn",
    requestId: "req_" + jobId,
    promptHash: "hash_" + jobId,
    status: "running",
    createdAt: new Date().toISOString(),
    startedAt: new Date().toISOString(),
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
    hintThreadId: null,
    hintTurnId: null,
    hintSource: null,
    dispatchUnknown: false,
    resultConsumedAt: null,
    leaseExpiresAt: null,
    attempt: "1",
    fence,
    workerPid: null,
    heartbeatAt: null,
  };
}

test("migração idempotente: schema migration 14 and quick_check pass on fresh and repeated runs", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-store-migration-"));
  const store = await BridgeStore.open(directory);
  try {
    assert.equal(store.integrityCheck({ full: true }), "ok");
    const migration14 = store.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 14").get() as { found?: number } | undefined;
    assert.equal(migration14?.found, 1, "migration 14 must be recorded");

    const columns = store.db.prepare("PRAGMA table_info(jobs)").all() as Array<{ name: string }>;
    assert.ok(columns.some((c) => c.name === "early_exit_at"), "early_exit_at column exists");
    assert.ok(columns.some((c) => c.name === "early_exit_reason"), "early_exit_reason column exists");
    assert.ok(columns.some((c) => c.name === "escalation_proposal"), "escalation_proposal column exists");

    assert.doesNotThrow(() => store.migrate());
    assert.doesNotThrow(() => store.migrate());
    assert.equal(store.integrityCheck({ full: true }), "ok");

    store.close();
    const reopened = await BridgeStore.open(directory);
    try {
      assert.equal(reopened.integrityCheck({ full: true }), "ok");
      const recheck = reopened.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 14").get() as { found?: number } | undefined;
      assert.equal(recheck?.found, 1);
    } finally {
      reopened.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("stale fence rejeitado: updateJobLiveness rejects write with lower fence", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-store-fence-"));
  const store = await BridgeStore.open(directory);
  try {
    const agent = store.createAgent(fixtureAgent("agent_fence"));
    const job = store.createJob({
      id: "job_fence",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_fence",
      promptHash: "hash_fence",
    });

    store.updateJobLiveness(job.id, { fence: 2 });
    assert.equal(store.getJob(job.id)?.fence, 2);

    assert.doesNotThrow(() => {
      store.updateJobLiveness(job.id, { fence: 2, heartbeatAt: new Date().toISOString() });
    });
    assert.doesNotThrow(() => {
      store.updateJobLiveness(job.id, { fence: 3 });
    });
    assert.equal(store.getJob(job.id)?.fence, 3);

    assert.throws(() => {
      store.updateJobLiveness(job.id, { fence: 2, heartbeatAt: new Date().toISOString() });
    }, (err: unknown) => {
      assert.ok(err instanceof ConflictError);
      assert.equal(err.code, "state_conflict");
      assert.match(err.message, /Stale write rejected/);
      return true;
    });

    assert.throws(() => {
      store.updateJobLiveness(job.id, { fence: 1 });
    }, (err: unknown) => {
      assert.ok(err instanceof ConflictError);
      return true;
    });
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("receipt shape comum: OpenCode and Antigravity produce unified ExecutionReceipt", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-receipt-shape-"));
  const agent = fixtureAgent("agent_receipt");
  const job = fixtureJob("job_receipt", agent.id, 2);
  try {
    const messages: OpenCodeMessage[] = [{
      info: { id: "asst_1", role: "assistant" },
      parts: [{
        type: "text",
        text: "STATUS: completed\nSUMMARY: Receipt test passed.\nFILES:\n- src/test.ts\nTESTS:\n- test passed",
      }],
    }];
    const opencodeResult = await persistResult(directory, agent, job, messages, null, 20_000);
    assert.ok(opencodeResult.envelope.receipt, "OpenCode envelope must include receipt");
    const ocReceipt = opencodeResult.envelope.receipt;

    assert.equal(ocReceipt.jobId, job.id);
    assert.equal(ocReceipt.agentId, agent.id);
    assert.equal(ocReceipt.provider, "opencode");
    assert.equal(ocReceipt.status, "completed");
    assert.equal(ocReceipt.workspace, agent.workspacePath);
    assert.equal(typeof ocReceipt.completedAt, "string");
    assert.equal(ocReceipt.quiescent, true);
    assert.equal(ocReceipt.fence, 2);
    assert.equal(ocReceipt.earlyExit, false);
    assert.equal(ocReceipt.filesCount, 1);
    assert.equal(ocReceipt.testsCount, 1);
    assert.equal(typeof ocReceipt.outputHash, "string");
    assert.equal(ocReceipt.outputHash.length, 64, "outputHash must be sha256 hex");

    const agyJob = fixtureJob("job_agy_receipt", "agent_agy", 3);
    const agyAgent = { ...fixtureAgent("agent_agy"), modelProviderId: "antigravity", modelId: "gemini-3.8-flash-high" };
    const agyRunResult = {
      status: "completed" as const,
      runId: "run_123",
      summary: "Antigravity receipt summary.",
      files: ["src/agy.ts"],
      tests: ["unit agy"],
      risks: [],
      diffSummary: "diff",
      model: "gemini-3.8-flash-high",
      modelDisplayName: "Antigravity · Gemini 3.8 Flash High",
      workspace: "C:\\work",
      rawOutput: "raw",
    };
    const agyResult = await persistAntigravityResult(directory, agyAgent, agyJob, agyRunResult, 20_000);
    assert.ok(agyResult.envelope.receipt, "Antigravity envelope must include receipt");
    const agyReceipt = agyResult.envelope.receipt;

    const expectedKeys = [
      "jobId", "agentId", "provider", "model", "status", "workspace",
      "startedAt", "completedAt", "durationMs", "attempt", "fence",
      "outputHash", "quiescent", "earlyExit", "filesCount", "testsCount",
    ].sort();
    assert.deepEqual(Object.keys(ocReceipt).sort(), expectedKeys);
    assert.deepEqual(Object.keys(agyReceipt).sort(), expectedKeys);
    assert.equal(agyReceipt.provider, "antigravity");
    assert.equal(agyReceipt.fence, 3);
    assert.equal(agyReceipt.quiescent, true);
    assert.equal(agyReceipt.outputHash.length, 64);

    const sanitized = sanitizePersistedEnvelope(opencodeResult.envelope);
    assert.ok(sanitized?.receipt);
    assert.equal(sanitized.receipt.jobId, job.id);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("parser e sanitização: structured markers for evidence, early-exit, and escalation", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-markers-"));
  const agent = fixtureAgent("agent_markers");
  const job = fixtureJob("job_markers", agent.id);
  try {
    const messages: OpenCodeMessage[] = [{
      info: { id: "asst_markers", role: "assistant" },
      parts: [{
        type: "text",
        text: [
          "STATUS: completed",
          "SUMMARY: Task finished with early satisfaction.",
          "EARLY_EXIT: goal_achieved_verified",
          "ESCALATION: pro-max recommended for deep performance review",
          "EVIDENCE:",
          "- [test] all 15 unit tests pass deterministically",
          "- [metric] latency reduced by 40%",
          "FILES:",
          "- src/core.ts",
          "TESTS:",
          "- npm test",
        ].join("\n"),
      }],
    }];
    const stored = await persistResult(directory, agent, job, messages, null, 20_000);
    const env = stored.envelope;

    assert.ok(env.earlyExit, "earlyExit must be parsed");
    assert.equal(env.earlyExit.triggered, true);
    assert.equal(env.earlyExit.reason, "goal_achieved_verified");

    assert.ok(env.escalation, "escalation must be parsed");
    assert.equal(env.escalation.advisoryOnly, true, "escalation must always be advisoryOnly");
    assert.match(env.escalation.reason, /pro-max recommended/);

    assert.ok(env.evidence, "evidence must be parsed");
    assert.equal(env.evidence.items.length, 2);
    assert.match(env.evidence.items[0]?.claim ?? "", /all 15 unit tests pass/);

    assert.equal(env.receipt?.earlyExit, true);

    const sanitized = sanitizePersistedEnvelope(env);
    assert.equal(sanitized?.earlyExit?.triggered, true);
    assert.equal(sanitized?.escalation?.advisoryOnly, true);
    assert.equal(sanitized?.evidence?.items.length, 2);

    const agyJsonStdout = JSON.stringify({
      status: "completed",
      summary: "Antigravity completed with early exit",
      files: ["agy.ts"],
      tests: ["agy test"],
      earlyExit: { triggered: true, reason: "fast_path_settled" },
      escalation: { reason: "consider pro-max for full audit", recommendedRoute: "pro-max" },
      evidence: { items: [{ type: "check", claim: "typecheck passes", verified: true }] },
    });
    const parsedAgy = parseAgyOutput(agyJsonStdout, "");
    assert.equal(parsedAgy.earlyExit?.triggered, true);
    assert.equal(parsedAgy.earlyExit?.reason, "fast_path_settled");
    assert.equal(parsedAgy.escalation?.advisoryOnly, true);
    assert.equal(parsedAgy.escalation?.recommendedRoute, "pro-max");
    assert.equal(parsedAgy.evidence?.items.length, 1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("escalation sem mudar rota: advisory proposal never mutates pinned route or provider", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-escalation-route-"));
  const store = await BridgeStore.open(directory);
  try {
    const agent = store.createAgent(fixtureAgent("agent_pinned_route", "flash-max"));
    const job = store.createJob({
      id: "job_pinned",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_pinned",
      promptHash: "hash_pinned",
    });

    const messages: OpenCodeMessage[] = [{
      info: { id: "asst_esc", role: "assistant" },
      parts: [{
        type: "text",
        text: "STATUS: completed\nSUMMARY: Needs higher model.\nESCALATION: recommend switching to pro-max\nFILES:\nnone\nTESTS:\nnone",
      }],
    }];
    const stored = await persistResult(directory, agent, job, messages, null, 20_000);

    assert.ok(stored.envelope.escalation);
    assert.equal(stored.envelope.escalation.advisoryOnly, true);

    const currentAgent = store.getAgent(agent.id);
    assert.equal(currentAgent?.modelRoute, "flash-max", "pinned route must never change automatically");
    assert.equal(currentAgent?.modelProviderId, "opencode-go", "provider must never change automatically");
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("regressão legacy: results and envelopes without adaptive markers function unchanged", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-legacy-regress-"));
  const agent = fixtureAgent("agent_legacy");
  const job = fixtureJob("job_legacy", agent.id);
  try {
    const legacyMessages: OpenCodeMessage[] = [{
      info: { id: "asst_leg", role: "assistant" },
      parts: [{
        type: "text",
        text: "STATUS: completed\nSUMMARY: Ordinary legacy completion.\nFILES:\n- src/old.ts\nTESTS:\n- passed",
      }],
    }];
    const stored = await persistResult(directory, agent, job, legacyMessages, null, 20_000);
    assert.equal(stored.envelope.summary, "Ordinary legacy completion.");
    assert.deepEqual(stored.envelope.files, ["src/old.ts"]);
    assert.equal(stored.envelope.earlyExit, undefined);
    assert.equal(stored.envelope.escalation, undefined);
    assert.equal(stored.envelope.evidence, undefined);
    assert.ok(stored.envelope.receipt);
    assert.equal(stored.envelope.receipt.earlyExit, false);

    const sanitized = sanitizePersistedEnvelope(stored.envelope);
    assert.equal(sanitized?.summary, "Ordinary legacy completion.");
    assert.equal(sanitized?.earlyExit, undefined);
    assert.equal(sanitized?.escalation, undefined);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

class FakeOpenCodeClient implements OpenCodeClientLike {
  async health() { return { healthy: true, version: "fake" }; }
  async createSession(dir: string, title: string) { return { id: "session_fake_" + title }; }
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
  constructor(readonly client = new FakeOpenCodeClient()) {}
  async start() {
    return {
      serverId: "server_fake",
      baseUrl: "http://127.0.0.1:1",
      client: this.client,
      processId: 12345,
      stop: async () => undefined,
    };
  }
  async stop() {}
}

test("progresso semântico: derived from activity/heartbeats, 900s is not proof of death", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-semantic-progress-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "flash-max",
  });
  const service = new BridgeService(config, {
    store,
    manager: new FakeOpenCodeManager() as any,
  });

  try {
    await service.start();
    const agent = store.createAgent(fixtureAgent("agent_sem_prog"));
    const job = store.createJob({
      id: "job_sem_prog",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_sem_prog",
      promptHash: "hash_sem_prog",
    });
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");

    // Simulate job started 1200 seconds ago (>900s), but with active heartbeat 5s ago
    const startedAt = new Date(Date.now() - 1200 * 1000).toISOString();
    const heartbeatAt = new Date(Date.now() - 5 * 1000).toISOString();
    store.db.prepare("UPDATE jobs SET started_at = ?, heartbeat_at = ? WHERE id = ?").run(startedAt, heartbeatAt, job.id);
    store.recordActivity({
      agentId: agent.id,
      jobId: job.id,
      sessionId: agent.opencodeSessionId,
      activityType: "tool",
      summary: "Running unit test suite with pytest",
    });

    const snapshot = await service.consult({
      agentId: agent.id,
      jobId: job.id,
    });

    assert.ok(snapshot.semanticProgress, "semanticProgress must be populated");
    assert.equal(snapshot.semanticProgress.stage, "testing", "stage derived from activity summary");
    assert.equal(snapshot.semanticProgress.isStalled, false, "900s elapsed is NOT proof of death when heartbeat is fresh");
    assert.equal(snapshot.semanticProgress.earlyExitTriggered, false);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("early-exit sem esperar deadline: follow resolves immediately without waiting for deadline", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-early-exit-deadline-"));
  const store = await BridgeStore.open(directory);
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: "antigravity-flash-high",
    followDefaultWaitMinutes: 20,
    followDefaultGraceMinutes: 5,
  });

  const mockAntigravity = {
    async runPrompt(options: any) {
      return {
        status: "completed" as const,
        runId: "run_fast_early_exit",
        summary: "Early exit completed immediately.",
        files: ["src/fast.ts"],
        tests: ["fast test"],
        risks: [],
        diffSummary: "none",
        model: "gemini-3.8-flash-high",
        modelDisplayName: "Antigravity",
        workspace: options.cwd,
        rawOutput: "done",
        earlyExit: { triggered: true, reason: "fast_path_verified" },
      };
    },
  };

  const service = new BridgeService(config, {
    store,
    manager: new FakeOpenCodeManager() as any,
    antigravity: mockAntigravity as any,
  });

  try {
    await service.start();
    const spawnRes = await service.spawn({
      requestId: "req_early_exit_fast",
      topic: "Fast Early Exit",
      task: "Complete task with early exit",
      cwd: directory,
      modelRoute: "antigravity-flash-high",
    });

    const startFollowTime = Date.now();
    const follow = await service.follow({
      agentId: spawnRes.agentId,
      jobId: spawnRes.jobId,
      waitMinutes: 20,
      graceMinutes: 5,
    });
    const followDurationMs = Date.now() - startFollowTime;

    assert.ok(followDurationMs < 10_000, `Follow should settle immediately on early exit, took ${followDurationMs}ms`);
    assert.equal(follow.status, "completed");
    assert.equal(follow.deadlineReached, false);
    assert.equal(follow.result?.envelope.earlyExit?.triggered, true);
    assert.equal(follow.result?.envelope.earlyExit?.reason, "fast_path_verified");
    assert.equal(follow.receipt?.earlyExit, true);
    assert.equal(follow.earlyExit?.triggered, true);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("takeover com fence antigo: conclusão zumbi rejeitada atomicamente e novo resultado preservado", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-takeover-fence-"));
  const store = await BridgeStore.open(directory);
  try {
    const agent = store.createAgent(fixtureAgent("agent_takeover"));
    const job = store.createJob({
      id: "job_takeover",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_takeover",
      promptHash: "hash_takeover",
    });

    // Attempt 1 starts with fence 1
    store.updateJobLiveness(job.id, { fence: 1, attempt: "attempt_1" });
    store.updateJobStatus(job.id, "dispatching", null, 1);
    store.updateJobStatus(job.id, "running", null, 1);
    assert.equal(store.getJob(job.id)?.fence, 1);

    // Takeover happens: replacement attempt 2 advances fence to 2
    store.updateJobLiveness(job.id, { fence: 2, attempt: "attempt_2" });
    assert.equal(store.getJob(job.id)?.fence, 2);

    // Attempt 2 completes and writes terminal result with expectedFence = 2
    const nowIso = new Date().toISOString();
    store.setJobResult(job.id, "results/new_attempt2.json", "Winner result from attempt 2", 2);
    store.setJobEarlyExit(job.id, { earlyExitAt: nowIso, reason: "fast_exit_attempt_2" }, 2);
    store.setJobEscalation(job.id, JSON.stringify({ reason: "escalation_attempt_2", advisoryOnly: true }), 2);
    store.updateJobStatus(job.id, "completed", null, 2);

    const winnerJob = store.getJob(job.id);
    assert.equal(winnerJob?.resultPath, "results/new_attempt2.json");
    assert.equal(winnerJob?.resultSummary, "Winner result from attempt 2");
    assert.equal(winnerJob?.earlyExitReason, "fast_exit_attempt_2");
    assert.match(winnerJob?.escalationProposal ?? "", /escalation_attempt_2/);
    assert.equal(winnerJob?.status, "completed");

    // Zombie Attempt 1 wakes up and tries to overwrite with stale fence 1 -> all must be rejected with ConflictError
    assert.throws(() => {
      store.setJobResult(job.id, "results/zombie_attempt1.json", "Zombie result from attempt 1", 1);
    }, (err: unknown) => {
      assert.ok(err instanceof ConflictError);
      assert.equal(err.code, "state_conflict");
      assert.match(err.message, /Stale write rejected/);
      return true;
    });

    assert.throws(() => {
      store.setJobEarlyExit(job.id, { earlyExitAt: nowIso, reason: "zombie_early_exit" }, 1);
    }, (err: unknown) => {
      assert.ok(err instanceof ConflictError);
      assert.equal(err.code, "state_conflict");
      assert.match(err.message, /Stale write rejected/);
      return true;
    });

    assert.throws(() => {
      store.setJobEscalation(job.id, JSON.stringify({ reason: "zombie_escalation", advisoryOnly: true }), 1);
    }, (err: unknown) => {
      assert.ok(err instanceof ConflictError);
      assert.equal(err.code, "state_conflict");
      assert.match(err.message, /Stale write rejected/);
      return true;
    });

    assert.throws(() => {
      store.updateJobStatus(job.id, "completed", null, 1);
    }, (err: unknown) => {
      assert.ok(err instanceof ConflictError);
      assert.equal(err.code, "state_conflict");
      assert.match(err.message, /Stale write rejected/);
      return true;
    });

    // Verify winner result remained completely intact
    const preservedJob = store.getJob(job.id);
    assert.equal(preservedJob?.resultPath, "results/new_attempt2.json");
    assert.equal(preservedJob?.resultSummary, "Winner result from attempt 2");
    assert.equal(preservedJob?.earlyExitReason, "fast_exit_attempt_2");
    assert.match(preservedJob?.escalationProposal ?? "", /escalation_attempt_2/);
    assert.equal(preservedJob?.status, "completed");

    // Verify legacy calls without expectedFence succeed (backward compatibility)
    store.updateJobStatus(job.id, "delivery_pending");
    assert.equal(store.getJob(job.id)?.status, "delivery_pending");
    store.setJobResult(job.id, "results/legacy_override.json", "Legacy summary");
    assert.equal(store.getJob(job.id)?.resultPath, "results/legacy_override.json");
    store.setJobEarlyExit(job.id, { earlyExitAt: nowIso, reason: "legacy_early_exit" });
    assert.equal(store.getJob(job.id)?.earlyExitReason, "legacy_early_exit");
    store.setJobEscalation(job.id, JSON.stringify({ reason: "legacy_escalation", advisoryOnly: true }));
    assert.match(store.getJob(job.id)?.escalationProposal ?? "", /legacy_escalation/);
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("redação de segredos, truncamento e bounding de itens no JSON nativo de Antigravity", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "adaptive-agy-redact-"));
  const agent = { ...fixtureAgent("agent_agy_redact"), modelProviderId: "antigravity", modelId: "gemini-3.8-flash-high" };
  const job = fixtureJob("job_agy_redact", agent.id, 1);
  try {
    const rawSecretApiKey = "sk-ant-api03-abcdef1234567890abcdef123456";
    const rawSecretBearer = "Bearer secret_jwt_token_12345";
    const rawSecretGhToken = "ghp_123456789012345678901234567890123456";

    // Build raw JSON with 200 files, 200 tests, 200 risks, 200 evidence items and tokens embedded
    const manyFiles = Array.from({ length: 200 }, (_, i) => `src/file_${i}_${rawSecretApiKey}.ts`);
    const manyTests = Array.from({ length: 200 }, (_, i) => `test case ${i} with ${rawSecretBearer}`);
    const manyRisks = Array.from({ length: 200 }, (_, i) => `risk ${i} token: ${rawSecretGhToken}`);
    const manyEvidence = Array.from({ length: 200 }, (_, i) => ({
      id: `ev_${i}`,
      type: "audit",
      claim: `claim ${i} verified with secret: ${rawSecretApiKey}`,
      source: `https://example.com/source_${i}?token=${rawSecretApiKey}`,
      snippet: `snippet text ${i} Authorization: Bearer secret_pass_123`,
    }));

    const hugeSummary = "Antigravity result summary with " + rawSecretApiKey + " " + "x".repeat(10_000);
    const hugeDiff = "diff line with " + rawSecretBearer + " " + "d".repeat(20_000);

    const agyNativeJson = JSON.stringify({
      status: "completed",
      runId: `run_${rawSecretApiKey}`,
      summary: hugeSummary,
      files: manyFiles,
      tests: manyTests,
      risks: manyRisks,
      diffSummary: hugeDiff,
      earlyExit: {
        triggered: true,
        reason: `Early exit triggered with token: ${rawSecretApiKey}`,
        evidenceSnippet: `Evidence snippet with ${rawSecretBearer}`,
      },
      escalation: {
        reason: `Escalate to pro-max with password: ${rawSecretApiKey}`,
        targetRole: `Lead Architect token=${rawSecretApiKey}`,
        recommendedRoute: "pro-max",
      },
      evidence: {
        summary: `Evidence summary with ${rawSecretBearer}`,
        items: manyEvidence,
      },
    });

    const parsed = parseAgyOutput(agyNativeJson, "");

    // Verify token redaction in parsed agy output
    assert.doesNotMatch(parsed.summary, new RegExp(rawSecretApiKey));
    assert.match(parsed.summary, /\[REDACTED\]/);
    assert.ok(parsed.summary.length <= 4_001);

    assert.doesNotMatch(parsed.diffSummary, new RegExp(rawSecretBearer));
    assert.match(parsed.diffSummary, /\[REDACTED\]/);
    assert.ok(parsed.diffSummary.length <= 10_001);

    assert.doesNotMatch(parsed.runId ?? "", new RegExp(rawSecretApiKey));

    // Verify bounds on item counts (capped at 100)
    assert.equal(parsed.files.length, 100);
    assert.equal(parsed.tests.length, 100);
    assert.equal(parsed.risks.length, 100);
    assert.equal(parsed.evidence?.items.length, 100);

    // Verify redaction inside list items
    assert.doesNotMatch(parsed.files[0] ?? "", new RegExp(rawSecretApiKey));
    assert.match(parsed.files[0] ?? "", /\[REDACTED\]/);

    assert.doesNotMatch(parsed.tests[0] ?? "", new RegExp(rawSecretBearer));
    assert.match(parsed.tests[0] ?? "", /\[REDACTED\]/);

    assert.doesNotMatch(parsed.risks[0] ?? "", new RegExp(rawSecretGhToken));
    assert.match(parsed.risks[0] ?? "", /\[REDACTED\]/);

    assert.doesNotMatch(parsed.evidence?.items[0]?.claim ?? "", new RegExp(rawSecretApiKey));
    assert.match(parsed.evidence?.items[0]?.claim ?? "", /\[REDACTED\]/);

    assert.doesNotMatch(parsed.earlyExit?.reason ?? "", new RegExp(rawSecretApiKey));
    assert.match(parsed.earlyExit?.reason ?? "", /\[REDACTED\]/);

    assert.doesNotMatch(parsed.escalation?.reason ?? "", new RegExp(rawSecretApiKey));
    assert.match(parsed.escalation?.reason ?? "", /\[REDACTED\]/);

    // Persist to disk and verify the stored file contains NO raw secrets
    const runResult = {
      status: "completed" as const,
      runId: parsed.runId,
      summary: parsed.summary,
      files: parsed.files,
      tests: parsed.tests,
      risks: parsed.risks,
      diffSummary: parsed.diffSummary,
      model: "gemini-3.8-flash-high",
      modelDisplayName: "Antigravity · Gemini 3.8 Flash High",
      workspace: "C:\\work",
      rawOutput: "rawOutput with " + rawSecretApiKey,
      evidence: parsed.evidence,
      earlyExit: parsed.earlyExit,
      escalation: parsed.escalation,
    };

    const stored = await persistAntigravityResult(directory, agent, job, runResult, 20_000);
    const diskContent = await import("node:fs/promises").then((fs) => fs.readFile(stored.resultPath, "utf8"));

    assert.doesNotMatch(diskContent, new RegExp(rawSecretApiKey), "persisted JSON must not leak rawSecretApiKey");
    assert.doesNotMatch(diskContent, new RegExp(rawSecretBearer), "persisted JSON must not leak rawSecretBearer");
    assert.doesNotMatch(diskContent, new RegExp(rawSecretGhToken), "persisted JSON must not leak rawSecretGhToken");
    assert.match(diskContent, /\[REDACTED\]/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("early-exit documentado e testado como sinal pós-turno sem cancelamento ativo especulativo", async () => {
  // Verify early-exit is structured as post-turn output data without process interruption
  const agyText = [
    "STATUS: completed",
    "SUMMARY: Unit work satisfied early.",
    "EARLY_EXIT: goal_met_early",
    "FILES:",
    "- src/index.ts",
    "TESTS:",
    "- passed",
  ].join("\n");

  const parsed = parseAgyOutput(agyText, "");
  assert.equal(parsed.earlyExit?.triggered, true);
  assert.equal(parsed.earlyExit?.reason, "goal_met_early");
  // Confirms it's delivered as part of ParsedAgyOutput result envelope, never invoking abort signals
  assert.equal(parsed.status, null); // plain text has null status until mapped by caller post-run
});
