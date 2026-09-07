import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";
import { createDefaultConfig } from "../../src/config.js";
import { InboxDelivery } from "../../src/delivery/inbox.js";

const ACTIVE_ROUTE = {
  name: "antigravity-flash-high",
  providerId: "antigravity",
  modelId: "gemini-3.8-flash-high",
  variant: null,
  enabled: true,
  default: true,
  display: "Antigravity · Gemini 3.8 Flash High",
};

function successfulRun(cwd: string) {
  return {
    status: "completed" as const,
    runId: "run_service_contract",
    summary: "Gemini completed the task",
    fullText: "SUMMARY: Gemini completed the task",
    files: [],
    tests: [],
    risks: [],
    diffSummary: "none",
    model: ACTIVE_ROUTE.modelId,
    modelDisplayName: "Antigravity · " + ACTIVE_ROUTE.modelId,
    workspace: cwd,
    rawOutput: "SUMMARY: Gemini completed the task",
  };
}

test("service startup ignores the legacy manager and dispatches active work only through Antigravity", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "antigravity-service-contract-"));
  const store = await BridgeStore.open(directory);
  let managerStarts = 0;
  let antigravityRuns = 0;
  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: ACTIVE_ROUTE.name,
    modelRoutes: [ACTIVE_ROUTE],
  }), {
    store,
    manager: {
      start: async () => {
        managerStarts += 1;
        throw new Error("legacy manager must not start");
      },
      stop: async () => undefined,
    },
    inbox: new InboxDelivery(directory),
    antigravity: {
      runPrompt: async (options) => {
        antigravityRuns += 1;
        assert.equal(options.model, ACTIVE_ROUTE.modelId);
        return successfulRun(options.cwd);
      },
    },
  });
  try {
    await service.start();
    assert.equal(service.isReady(), true);
    assert.equal(managerStarts, 0);
    const accepted = await service.spawn({
      requestId: "request_service_antigravity_only",
      topic: "Antigravity service",
      task: "Run only through the active Gemini route",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(accepted.accepted, true);
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline && (antigravityRuns === 0 || store.getJob(accepted.jobId)?.status !== "delivered")) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(antigravityRuns, 1);
    assert.equal(store.getAgent(accepted.agentId)?.modelProviderId, "antigravity");
    assert.equal(store.getJob(accepted.jobId)?.status, "delivered");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
test("startup marks a historical non-Antigravity job failed without touching a provider session", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "antigravity-service-history-"));
  const store = await BridgeStore.open(directory);
  let antigravityRuns = 0;
  const agent = store.createAgent({
    id: "agent_historical_service",
    title: "Historical worker",
    topic: "Historical OpenCode worker",
    repositoryRoot: directory,
    workspacePath: directory,
    workspaceStrategy: "shared",
    opencodeServerId: "legacy-server",
    opencodeSessionId: "legacy-session",
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    modelRoute: "flash-max",
  });
  const job = store.createJob({
    id: "job_historical_service",
    agentId: agent.id,
    kind: "spawn",
    requestId: "request_historical_service",
    promptHash: "historical-hash",
  });
  store.updateJobStatus(job.id, "dispatching");
  store.updateAgentStatus(agent.id, "working");
  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: ACTIVE_ROUTE.name,
    modelRoutes: [ACTIVE_ROUTE],
  }), {
    store,
    manager: {
      start: async () => { throw new Error("legacy manager must not start"); },
      stop: async () => undefined,
    },
    antigravity: {
      runPrompt: async () => {
        antigravityRuns += 1;
        return successfulRun(directory);
      },
    },
  });
  try {
    await service.start();
    const recovered = store.getJob(job.id);
    assert.equal(antigravityRuns, 0);
    assert.equal(recovered?.status, "failed");
    assert.match(recovered?.error ?? "", /legacy|provider|unsupported|cannot be recovered/i);
    await assert.rejects(() => service.recoverResult(job.id), /No persisted result/i);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("historical sessions fail closed on continue, abort, close, and follow without provider contact", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "antigravity-service-failclosed-"));
  const store = await BridgeStore.open(directory);
  let antigravityRuns = 0;
  let managerStarts = 0;

  const agent = store.createAgent({
    id: "agent_hist_ops",
    title: "Historical worker",
    topic: "Historical OpenCode worker",
    repositoryRoot: directory,
    workspacePath: directory,
    workspaceStrategy: "shared",
    opencodeServerId: "legacy-server-123",
    opencodeSessionId: "legacy-session-456",
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    modelRoute: "flash-max",
  });
  const job = store.createJob({
    id: "job_hist_ops",
    agentId: agent.id,
    kind: "spawn",
    requestId: "request_hist_ops",
    promptHash: "historical-hash",
  });
  store.updateJobStatus(job.id, "dispatching");
  store.updateJobStatus(job.id, "running");
  store.updateAgentStatus(agent.id, "working");

  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: ACTIVE_ROUTE.name,
    modelRoutes: [ACTIVE_ROUTE],
  }), {
    store,
    manager: {
      start: async () => { managerStarts += 1; throw new Error("legacy manager must not start"); },
      stop: async () => undefined,
    },
    antigravity: {
      runPrompt: async () => {
        antigravityRuns += 1;
        return successfulRun(directory);
      },
    },
  });

  try {
    await service.start();
    assert.equal(managerStarts, 0);

    // Verify opaque schema columns are preserved in read mode
    const readAgent = service.getAgent(agent.id);
    assert.equal(readAgent?.opencodeServerId, "legacy-server-123");
    assert.equal(readAgent?.opencodeSessionId, "legacy-session-456");

    // Execution: continueJob on historical agent must fail closed
    await assert.rejects(
      () => service.continueJob({
        agentId: agent.id,
        task: "Continue historical task",
        requestId: "req_hist_continue",
      }),
      /Historical provider sessions are read-only/i
    );

    // Follow on active historical job: marks failed closed without contacting provider
    const followRes = await service.follow({ agentId: agent.id, jobId: job.id });
    assert.equal(followRes.status, "failed");
    assert.match(followRes.error ?? "", /Historical provider sessions are read-only/i);

    // Abort on historical agent: aborts locally and returns quiescent proof
    const abortRes = await service.abort(agent.id, "Stop historical");
    assert.equal(abortRes.status, "aborted");
    assert.equal(abortRes.quiescent, true);

    // Close on historical agent: marks closed and returns quiescent proof
    const closeRes = await service.close(agent.id);
    assert.equal(closeRes.status, "closed");
    assert.equal(closeRes.quiescent, true);

    assert.equal(antigravityRuns, 0);
    assert.equal(managerStarts, 0);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("historical job with persisted result can be read and consumed in read-only mode without provider contact", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "antigravity-service-result-"));
  const store = await BridgeStore.open(directory);

  const agent = store.createAgent({
    id: "agent_hist_result",
    title: "Historical result worker",
    topic: "Historical result task",
    repositoryRoot: directory,
    workspacePath: directory,
    workspaceStrategy: "shared",
    opencodeServerId: "legacy-server-result",
    opencodeSessionId: "legacy-session-result",
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    modelRoute: "flash-max",
  });
  const job = store.createJob({
    id: "job_hist_result",
    agentId: agent.id,
    kind: "spawn",
    requestId: "request_hist_result",
    promptHash: "historical-result-hash",
  });

  const resultFilePath = path.join(directory, "hist_result.json");
  await writeFile(resultFilePath, JSON.stringify({
    envelope: {
      status: "completed",
      summary: "Historical task completed successfully",
      files: ["test.txt"],
      tests: ["test 1 passed"],
      risks: ["none"],
    },
    summary: "Historical task completed successfully",
  }));

  store.updateJobStatus(job.id, "dispatching");
  store.updateJobStatus(job.id, "running");
  store.setJobResult(job.id, resultFilePath, "Historical task completed successfully");
  store.updateJobStatus(job.id, "completed");
  store.updateAgentStatus(agent.id, "working");
  store.updateAgentStatus(agent.id, "completed");

  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: ACTIVE_ROUTE.name,
    modelRoutes: [ACTIVE_ROUTE],
  }), {
    store,
    manager: {
      start: async () => { throw new Error("legacy manager must not start"); },
      stop: async () => undefined,
    },
  });

  try {
    await service.start();

    // Read and consume the persisted historical result
    const recovered = await service.recoverResult(job.id) as { envelope?: { summary?: string } };
    assert.equal(recovered.envelope?.summary, "Historical task completed successfully");

    // Verify result was marked consumed in store
    const jobAfter = store.getJob(job.id);
    assert.ok(jobAfter?.resultConsumedAt);
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("new agent spawn preserves opaque schema columns opencodeServerId and opencodeSessionId", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "antigravity-service-schema-"));
  const store = await BridgeStore.open(directory);
  let antigravityRuns = 0;

  const service = new BridgeService(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    defaultModelRoute: ACTIVE_ROUTE.name,
    modelRoutes: [ACTIVE_ROUTE],
  }), {
    store,
    inbox: new InboxDelivery(directory),
    antigravity: {
      runPrompt: async (options) => {
        antigravityRuns += 1;
        return successfulRun(options.cwd);
      },
    },
  });

  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "request_schema_check",
      topic: "Schema check task",
      task: "Verify schema column preservation",
      cwd: directory,
      mode: "analyze",
    });
    assert.equal(accepted.accepted, true);

    const agent = service.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.opencodeServerId, "antigravity");
    assert.equal(agent.opencodeSessionId, "antigravity:" + accepted.agentId);
    assert.equal(agent.modelProviderId, "antigravity");
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
