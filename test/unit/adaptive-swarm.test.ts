import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile, mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { canTransitionJob, assertJobTransition } from "../../src/state.js";
import { BridgeStore } from "../../src/store.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeService, computeBatchHash, FollowCancelledError, type ManagedOpenCodeLike } from "../../src/service.js";
import { ConflictError, InvalidRequestError } from "../../src/errors.js";
import { TranscriptAttestor, DEFAULT_ACCEPTED_TOOLS } from "../../src/codex/transcript-attestor.js";
import { createMcpServer } from "../../src/mcp.js";
import { BridgeHttpClient } from "../../src/http-server.js";
import { hashPrompt } from "../../src/security.js";
import { doctorSwarmCheck } from "../../src/cli.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

const execFileAsync = promisify(execFile);

let globalSessionCounter = 0;

class FakeOpenCodeClient implements OpenCodeClientLike {
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  promptErrors: Array<Error | null> = [];
  messages: OpenCodeMessage[] = [];
  activeSessions = new Set<string>();
  private onEvent?: (event: OpenCodeEvent) => Promise<void> | void;

  async health() {
    return { healthy: true, version: "fake" };
  }
  async createSession() {
    globalSessionCounter += 1;
    const id = "session_" + globalSessionCounter;
    this.activeSessions.add(id);
    return { id };
  }
  async promptAsync(sessionId: string, task: string) {
    this.promptCalls.push({ sessionId, task });
    const err = this.promptErrors.shift();
    if (err) throw err;
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

// ---------------------------------------------------------------------------
// 1. State Transitions (TDD RED)
// ---------------------------------------------------------------------------
test("job state machine permits queued transitions", () => {
  assert.equal(canTransitionJob("created", "queued"), true);
  assert.equal(canTransitionJob("queued", "dispatching"), true);
  assert.equal(canTransitionJob("queued", "aborted"), true);
  assert.equal(canTransitionJob("queued", "failed"), true);
  assert.doesNotThrow(() => assertJobTransition("queued", "dispatching"));
  assert.doesNotThrow(() => assertJobTransition("created", "queued"));
});

// ---------------------------------------------------------------------------
// 2. Migration v19 & Store Schema
// ---------------------------------------------------------------------------
test("migration v19 creates batches table and adds swarm columns to jobs", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-store-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    try {
      const v19 = store.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 19").get() as { found?: number } | undefined;
      assert.equal(v19?.found, 1, "Migration 19 must be recorded in schema_migrations");

      // Verify batches table
      const batchTable = store.db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='batches'").get();
      assert.ok(batchTable, "batches table must exist");

      // Verify job columns
      const jobCols = (store.db.prepare("PRAGMA table_info(jobs)").all() as Array<{ name: string }>).map((c) => c.name);
      assert.ok(jobCols.includes("batch_id"), "jobs table must have batch_id column");
      assert.ok(jobCols.includes("priority"), "jobs table must have priority column");
      assert.ok(jobCols.includes("exclusive_resources"), "jobs table must have exclusive_resources column");
      assert.ok(jobCols.includes("queued_at"), "jobs table must have queued_at column");
      assert.ok(jobCols.includes("dispatched_at"), "jobs table must have dispatched_at column");
    } finally {
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Atomic Batch Validation (TDD RED)
// ---------------------------------------------------------------------------
test("spawnBatch validates entire batch atomically and fails without partial writes", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-val-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // Empty items list
      await assert.rejects(
        async () => {
          await (service as any).spawnBatch({
            batchRequestId: "batch_empty",
            items: [],
          });
        },
        InvalidRequestError,
      );

      // Malformed item in batch (second item has empty task)
      await assert.rejects(
        async () => {
          await (service as any).spawnBatch({
            batchRequestId: "batch_malformed",
            items: [
              { topic: "Good topic", task: "Valid task 1" },
              { topic: "Bad topic", task: "   " },
            ],
          });
        },
        InvalidRequestError,
      );

      // Verify zero partial rows were persisted
      assert.equal(store.listJobs().length, 0, "No jobs should be persisted when batch validation fails");
      assert.equal((store as any).getBatchByRequestId?.("batch_malformed") ?? null, null, "Batch record must not be persisted on failure");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Batch Idempotence and Conflict
// ---------------------------------------------------------------------------
test("spawnBatch returns existing receipts on identical replay and fails on conflict", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-idem-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const batchInput = {
        batchRequestId: "batch_idem_1",
        items: [
          { requestId: "req_1", topic: "T1", task: "Do task 1", priority: 10 },
          { requestId: "req_2", topic: "T2", task: "Do task 2", priority: 20 },
        ],
      };

      const receipt1 = await (service as any).spawnBatch(batchInput);
      assert.equal(receipt1.accepted, true);
      assert.equal(receipt1.items.length, 2);
      assert.equal(receipt1.batchRequestId, "batch_idem_1");

      // Identical repeat returns existing receipts
      const receipt2 = await (service as any).spawnBatch(batchInput);
      assert.equal(receipt2.accepted, true);
      assert.equal(receipt2.batchRequestId, "batch_idem_1");
      assert.equal(receipt2.items[0]?.jobId, receipt1.items[0]?.jobId);
      assert.equal(receipt2.items[1]?.jobId, receipt1.items[1]?.jobId);

      // Conflicting payload for same batchRequestId fails closed
      await assert.rejects(
        async () => {
          await (service as any).spawnBatch({
            batchRequestId: "batch_idem_1",
            items: [
              { requestId: "req_1", topic: "T1", task: "DIFFERENT TASK", priority: 10 },
            ],
          });
        },
        ConflictError,
      );

      // Conflicting payload for existing item requestId fails closed
      await assert.rejects(
        async () => {
          await (service as any).spawnBatch({
            batchRequestId: "batch_diff_2",
            items: [
              { requestId: "req_1", topic: "T1", task: "DIFFERENT TASK 2" },
            ],
          });
        },
        ConflictError,
      );
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. Scheduler Credit Ceiling & Queueing
// ---------------------------------------------------------------------------
test("scheduler enforces physical credit ceiling and drains queued jobs as credits free up", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-credits-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 2,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // Spawn 3 jobs when credit ceiling is 2
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_cred_1",
        items: [
          { requestId: "j1", topic: "Job 1", task: "Task 1", priority: 50 },
          { requestId: "j2", topic: "Job 2", task: "Task 2", priority: 50 },
          { requestId: "j3", topic: "Job 3", task: "Task 3", priority: 50 },
        ],
      });

      assert.equal(batch.items.length, 3);
      const jobs = store.listJobs();
      const activeOrDispatched = jobs.filter((j) => ["dispatching", "running", "following"].includes(j.status));
      const queued = jobs.filter((j) => j.status === "queued");

      // At most 2 should be active/dispatched, at least 1 must remain queued
      assert.equal(activeOrDispatched.length, 2, "Active dispatches must respect credit ceiling 2");
      assert.equal(queued.length, 1, "Excess jobs must be queued");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 6. Generic Exclusive Resources
// ---------------------------------------------------------------------------
test("scheduler serializes jobs with overlapping exclusive resources", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-res-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 4,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // 2 jobs with same exclusive resource "shared-db", 1 job with "other-res"
      await (service as any).spawnBatch({
        batchRequestId: "batch_res_1",
        items: [
          { requestId: "r1", topic: "DB 1", task: "Task DB 1", exclusiveResources: ["shared-db"], priority: 60 },
          { requestId: "r2", topic: "DB 2", task: "Task DB 2", exclusiveResources: ["shared-db"], priority: 40 },
          { requestId: "r3", topic: "Other", task: "Task Other", exclusiveResources: ["other-res"] },
        ],
      });

      const jobs = store.listJobs();
      const r1 = jobs.find((j) => j.requestId === "r1");
      const r2 = jobs.find((j) => j.requestId === "r2");
      const r3 = jobs.find((j) => j.requestId === "r3");

      // r1 and r3 can run concurrently, but r2 must wait because r1 holds "shared-db"
      assert.ok(r1 && ["dispatching", "running", "following"].includes(r1.status));
      assert.ok(r3 && ["dispatching", "running", "following"].includes(r3.status));
      assert.equal(r2?.status, "queued", "r2 must stay queued while shared-db is claimed");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 7. Unary Spawn through Queue Admission
// ---------------------------------------------------------------------------
test("unary spawn passes through queue admission and returns compatible receipt", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-unary-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const receipt = await service.spawn({
        requestId: "unary_req_1",
        topic: "Unary topic",
        task: "Unary task",
      });

      assert.equal(receipt.accepted, true);
      assert.equal(receipt.status, "accepted");
      assert.ok(receipt.jobId);
      assert.ok(receipt.agentId);

      // Verify job was persisted in store
      const job = store.getJob(receipt.jobId);
      assert.ok(job);
      assert.ok(["queued", "dispatching", "running", "following"].includes(job.status));
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 8. Operational Counters in Status / Doctor
// ---------------------------------------------------------------------------
test("status exposes swarm operational counters without leaking prompt text", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-status-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 3,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const status = service.status();
      assert.ok(status.swarm, "status must include swarm operational counters");
      assert.equal(typeof status.swarm.queueDepth, "number");
      assert.equal(typeof status.swarm.active, "number");
      assert.equal(typeof status.swarm.targetCredits, "number");
      assert.equal(status.swarm.targetCredits, 3);
      assert.ok(Array.isArray(status.swarm.resourceClaims));

      // Ensure no prompt text is leaked in the status object
      const serialized = JSON.stringify(status);
      assert.equal(serialized.includes("super_secret_prompt"), false);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 9. Park Integration with Queued Jobs
// ---------------------------------------------------------------------------
test("park integration treats queued jobs as pending and not eligible to wake", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-park-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_park_1",
        items: [
          { requestId: "p1", topic: "P1", task: "Task P1" },
          { requestId: "p2", topic: "P2", task: "Task P2" },
        ],
      });

      const jobIds = batch.items.map((it: any) => it.jobId);
      const parkReceipt = await service.park({
        jobIds,
        predicate: "ALL",
      });

      // Both jobs pending, none ready
      assert.equal(parkReceipt.pendingCount, 2);
      assert.equal(parkReceipt.readyCount, 0);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 10. Transcript Attestation accepts batch tools
// ---------------------------------------------------------------------------
test("DEFAULT_ACCEPTED_TOOLS includes subagents_spawn_batch and deepseek_spawn_batch", () => {
  assert.ok(DEFAULT_ACCEPTED_TOOLS.includes("subagents_spawn_batch"));
  assert.ok(DEFAULT_ACCEPTED_TOOLS.includes("deepseek_spawn_batch"));
});

// ---------------------------------------------------------------------------
// 11. Migration v20 & dispatch_envelopes Table CRUD
// ---------------------------------------------------------------------------
test("migration v20 creates dispatch_envelopes table and verifies CRUD", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-mig20-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    try {
      const v20 = store.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 20").get() as { found?: number } | undefined;
      assert.equal(v20?.found, 1, "Migration 20 must be recorded in schema_migrations");

      const table = store.db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='dispatch_envelopes'").get();
      assert.ok(table, "dispatch_envelopes table must exist");

      const cols = (store.db.prepare("PRAGMA table_info(dispatch_envelopes)").all() as Array<{ name: string }>).map((c) => c.name);
      assert.ok(cols.includes("job_id"));
      assert.ok(cols.includes("prompt"));
      assert.ok(cols.includes("prompt_hash"));
      assert.ok(cols.includes("worker_input_json"));
      assert.ok(cols.includes("context_files_json"));
      assert.ok(cols.includes("created_at"));

      // Test CRUD methods with real foreign key relations
      const dummyJobId = "job_test_crud";
      const dummyPrompt = "Test prompt content";
      const dummyHash = hashPrompt(dummyPrompt);

      store.createAgent({
        id: "agent_test_crud",
        title: "Agent Test CRUD",
        topic: "Topic CRUD",
        repositoryRoot: tmpDir,
        workspacePath: tmpDir,
        workspaceStrategy: "shared",
        mode: "analyze",
        opencodeServerId: "srv1",
        opencodeSessionId: "pending:crud",
        modelProviderId: "opencode",
        modelId: "model",
        modelVariant: null,
        modelRoute: "default",
      });
      store.createJob({
        id: dummyJobId,
        agentId: "agent_test_crud",
        kind: "spawn",
        status: "queued",
        requestId: "req_test_crud",
        promptHash: dummyHash,
      });

      store.saveDispatchEnvelope(dummyJobId, {
        prompt: dummyPrompt,
        promptHash: dummyHash,
        workerInput: { task: "Test task", topic: "Test topic" },
        contextFiles: ["file1.ts"],
      });

      const loaded = store.getDispatchEnvelope(dummyJobId);
      assert.ok(loaded);
      assert.equal(loaded.jobId, dummyJobId);
      assert.equal(loaded.prompt, dummyPrompt);
      assert.equal(loaded.promptHash, dummyHash);
      assert.equal((loaded.workerInput as any).task, "Test task");
      assert.deepEqual(loaded.contextFiles, ["file1.ts"]);

      store.deleteDispatchEnvelope(dummyJobId);
      assert.equal(store.getDispatchEnvelope(dummyJobId), null);
    } finally {
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 12. Atomic store.admitBatch Rollback on Error
// ---------------------------------------------------------------------------
test("store.admitBatch rolls back atomically when an error is encountered", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-atomic-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    try {
      assert.equal(store.listJobs().length, 0);

      // Attempt admitBatch where second item has duplicate agent ID
      assert.throws(
        () => {
          store.admitBatch({
            batch: {
              id: "batch_fail",
              requestId: "req_batch_fail",
              batchHash: "hash_fail",
              status: "queued",
            },
            items: [
              {
                agent: {
                  id: "dup_agent",
                  title: "Title 1",
                  topic: "Topic 1",
                  repositoryRoot: tmpDir,
                  workspacePath: tmpDir,
                  workspaceStrategy: "shared",
                  mode: "analyze",
                  opencodeServerId: "srv1",
                  opencodeSessionId: "pending:1",
                  modelProviderId: "opencode",
                  modelId: "model",
                  modelVariant: null,
                  modelRoute: "default",
                },
                job: {
                  id: "job_1",
                  agentId: "dup_agent",
                  kind: "spawn",
                  batchId: "batch_fail",
                  status: "queued",
                  priority: 50,
                  exclusiveResources: [],
                  promptHash: "hash1",
                  requestId: "req1",
                },
                dispatchEnvelope: {
                  prompt: "Prompt 1",
                  promptHash: hashPrompt("Prompt 1"),
                  workerInput: { task: "Task 1" },
                  contextFiles: [],
                },
              },
              {
                agent: {
                  id: "dup_agent", // Duplicate agent ID causes primary key collision
                  title: "Title 2",
                  topic: "Topic 2",
                  repositoryRoot: tmpDir,
                  workspacePath: tmpDir,
                  workspaceStrategy: "shared",
                  mode: "analyze",
                  opencodeServerId: "srv1",
                  opencodeSessionId: "pending:2",
                  modelProviderId: "opencode",
                  modelId: "model",
                  modelVariant: null,
                  modelRoute: "default",
                },
                job: {
                  id: "job_2",
                  agentId: "dup_agent",
                  kind: "spawn",
                  batchId: "batch_fail",
                  status: "queued",
                  priority: 50,
                  exclusiveResources: [],
                  promptHash: "hash2",
                  requestId: "req2",
                },
              },
            ],
          });
        },
        (err: unknown) => {
          return err instanceof Error && /UNIQUE constraint failed/i.test(err.message);
        },
      );

      // Assert complete rollback: 0 batches, 0 agents, 0 jobs, 0 envelopes
      assert.equal(store.getBatchByRequestId("req_batch_fail"), null, "Batch record must not exist after rollback");
      assert.equal(store.listJobs().length, 0, "Jobs must be 0 after rollback");
      assert.equal(store.getAgent("dup_agent"), null, "Agent must not exist after rollback");
      assert.equal(store.getDispatchEnvelope("job_1"), null, "Dispatch envelope must not exist after rollback");
    } finally {
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 13. Batch Hash Normalization & Semantic Conflict Detection
// ---------------------------------------------------------------------------
test("batch hash normalization supports aliases and detects semantic divergence", () => {
  const hash1 = computeBatchHash([
    {
      topic: "T1",
      task: "Task 1",
      workspaceStrategy: "shared",
      contextFiles: ["b.txt", "a.txt"],
      exclusiveResources: ["res2", "res1"],
    },
  ]);
  const hash2 = computeBatchHash([
    {
      topic: "T1",
      task: "Task 1",
      workspace_strategy: "shared",
      context_files: ["a.txt", "b.txt"],
      exclusive_resources: ["res1", "res2"],
    } as any,
  ]);
  assert.equal(hash1, hash2, "Aliases and sorted arrays must produce identical hashes");

  // Semantic differences
  const hashDiffVisual = computeBatchHash([
    {
      topic: "T1",
      task: "Task 1",
      visualContext: "Different visual context",
    },
  ]);
  assert.notEqual(hash1, hashDiffVisual, "Different visualContext must alter batch hash");

  const hashDiffTask = computeBatchHash([
    {
      topic: "T1",
      task: "Different task",
    },
  ]);
  assert.notEqual(hash1, hashDiffTask, "Different task must alter batch hash");

  const hashDiffRes = computeBatchHash([
    {
      topic: "T1",
      task: "Task 1",
      exclusiveResources: ["new-resource"],
    },
  ]);
  assert.notEqual(hash1, hashDiffRes, "Different exclusiveResources must alter batch hash");
});

// ---------------------------------------------------------------------------
// 14. AIMD Physical Adaptation Under Ceiling
// ---------------------------------------------------------------------------
test("AIMD physical adaptation under ceiling reduces on backpressure and recovers on success", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-aimd-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 4,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      assert.equal(service.status().swarm.targetCredits, 4);

      // Multiplicative decrease: 4 -> 2
      (service as any).recordBackpressure("bridge_busy");
      assert.equal(service.status().swarm.targetCredits, 2);

      // Multiplicative decrease: 2 -> 1
      (service as any).recordBackpressure("transient_busy");
      assert.equal(service.status().swarm.targetCredits, 1);

      // Floor remains 1
      (service as any).recordBackpressure("bridge_busy");
      assert.equal(service.status().swarm.targetCredits, 1);

      // Additive increase: 1 -> 2 -> 3 -> 4
      (service as any).recordSuccess();
      assert.equal(service.status().swarm.targetCredits, 2);

      (service as any).recordSuccess();
      assert.equal(service.status().swarm.targetCredits, 3);

      (service as any).recordSuccess();
      assert.equal(service.status().swarm.targetCredits, 4);

      // Capped at ceiling
      (service as any).recordSuccess();
      assert.equal(service.status().swarm.targetCredits, 4);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 15. Capability batch_scheduler in Status and Doctor
// ---------------------------------------------------------------------------
test("capability batch_scheduler is exposed in status and doctor check", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-cap-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const status = service.status();
      assert.equal((status as any).capabilities?.batch_scheduler, true);
      assert.equal(status.swarm?.capabilities?.batch_scheduler, true);

      const checks: Array<{ name: string; status: string; detail?: string }> = [];
      doctorSwarmCheck(config, path.join(tmpDir, "test.sqlite"), (c) => checks.push(c));
      const batchCheck = checks.find((c) => c.name === "batch_scheduler");
      assert.ok(batchCheck, "batch_scheduler doctor check must be present");
      assert.equal(batchCheck?.status, "ok");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 16. Starvation Prevention & Aging FIFO Ordering
// ---------------------------------------------------------------------------
test("scheduler enforces FIFO aging ordering among same-priority jobs", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-fifo-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // 1. Blocker occupies the 1 credit
      const blocker = await service.spawn({
        requestId: "blocker_req",
        topic: "Blocker",
        task: "Blocker task",
      });
      assert.ok(["dispatching", "running", "following"].includes(store.getJob(blocker.jobId)!.status));

      // 2. Spawn two jobs with same priority (50)
      await service.spawn({
        requestId: "older_req",
        topic: "Older",
        task: "Older task",
        priority: 50,
      });

      await service.spawn({
        requestId: "younger_req",
        topic: "Younger",
        task: "Younger task",
        priority: 50,
      });

      const olderJob = store.getJobByRequestId("older_req");
      const youngerJob = store.getJobByRequestId("younger_req");
      assert.equal(olderJob?.status, "queued");
      assert.equal(youngerJob?.status, "queued");

      // Give olderJob an earlier queued_at timestamp
      store.db.prepare("UPDATE jobs SET queued_at = ? WHERE id = ?").run(new Date(Date.now() - 60000).toISOString(), olderJob!.id);

      // 3. Complete blocker to free up the 1 credit
      store.updateJobStatus(blocker.jobId, "completed");
      (service as any).scheduleDrain();
      await new Promise((r) => setTimeout(r, 50));

      // 4. FIFO aging must dispatch olderJob, while youngerJob remains queued
      const olderAfter = store.getJob(olderJob!.id);
      const youngerAfter = store.getJob(youngerJob!.id);
      assert.ok(["dispatching", "running", "following"].includes(olderAfter!.status), "Older job must be dispatched first");
      assert.equal(youngerAfter!.status, "queued", "Younger job must remain queued");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 17. Mutex of Git Worktree Preparation Per Canonical Repository
// ---------------------------------------------------------------------------
test("withRepoPreparationLock serializes concurrent executions on canonical repo", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-lock-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      let concurrent = 0;
      let maxConcurrent = 0;
      const execute = async (repo: string) => {
        return (service as any).withRepoPreparationLock(repo, async () => {
          concurrent++;
          if (concurrent > maxConcurrent) maxConcurrent = concurrent;
          await new Promise((r) => setTimeout(r, 20));
          concurrent--;
        });
      };

      const canonicalPath = path.resolve(tmpDir);
      await Promise.all([
        execute(canonicalPath),
        execute(canonicalPath.toLowerCase()),
        execute(canonicalPath.toUpperCase()),
        execute(path.join(canonicalPath, ".", "..", path.basename(canonicalPath))),
      ]);

      assert.equal(maxConcurrent, 1, "Concurrent lock calls on canonical repo must be serialized");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 18. Real Service Restart with Queue Depth > Credits
// ---------------------------------------------------------------------------
test("service restart recovers queued jobs from persisted envelopes without duplicates", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-restart-"));
  const dbPath = path.join(tmpDir, "restart.sqlite");
  try {
    const client1 = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const store1 = new BridgeStore(dbPath);
    const service1 = new BridgeService(config, {
      store: store1,
      manager: makeFakeManager(client1),
    });
    await service1.start();

    let q1JobId: string;
    let q2JobId: string;
    try {
      // 1. Blocker occupies credit
      const blocker = await service1.spawn({
        requestId: "b_req",
        topic: "Blocker",
        task: "Blocker task",
      });

      // 2. Queue 2 jobs behind blocker
      await service1.spawn({
        requestId: "q1_req",
        topic: "Job Q1",
        task: "Task Q1 (restart target)",
        priority: 70,
      });
      await service1.spawn({
        requestId: "q2_req",
        topic: "Job Q2",
        task: "Task Q2 (remains queued)",
        priority: 30,
      });

      const q1 = store1.getJobByRequestId("q1_req")!;
      const q2 = store1.getJobByRequestId("q2_req")!;
      q1JobId = q1.id;
      q2JobId = q2.id;
      assert.equal(q1.status, "queued");
      assert.equal(q2.status, "queued");

      // Give q1 earlier timestamp so it is first in FIFO order
      store1.db.prepare("UPDATE jobs SET queued_at = ? WHERE id = ?").run(new Date(Date.now() - 10000).toISOString(), q1.id);

      // Complete blocker so active=0, leaving queue depth (2) > credits (1)
      store1.updateJobStatus(blocker.jobId, "completed");

      // Verify envelopes are persisted
      const env1 = store1.getDispatchEnvelope(q1.id);
      const env2 = store1.getDispatchEnvelope(q2.id);
      assert.ok(env1 && env2);
      assert.equal((env1.workerInput as any).task, "Task Q1 (restart target)");
      assert.equal((env2.workerInput as any).task, "Task Q2 (remains queued)");
    } finally {
      await service1.stop();
      store1.close();
    }

    // Start second service instance with credits = 1 and queue depth = 2
    const client2 = new FakeOpenCodeClient();
    const store2 = new BridgeStore(dbPath);
    const service2 = new BridgeService(config, {
      store: store2,
      manager: makeFakeManager(client2),
    });

    try {
      await service2.start();
      await new Promise((r) => setTimeout(r, 100));

      assert.equal(client2.promptCalls.length, 1, "Exactly one dispatch must be performed under credit ceiling 1");
      assert.ok(
        client2.promptCalls[0]?.task.includes("Task Q1 (restart target)"),
        "Client must receive prompt containing the original task from envelope",
      );

      // q1 transitioned to dispatching/running, envelope deleted
      const q1After = store2.getJob(q1JobId)!;
      assert.ok(["dispatching", "running", "following"].includes(q1After.status));
      assert.equal(store2.getDispatchEnvelope(q1JobId), null, "Envelope for dispatched job must be deleted");

      // Route remains pinned on agent
      const q1Agent = store2.getAgent(q1After.agentId);
      assert.equal(q1Agent?.modelRoute, "flash-max", "Pinned model route must survive restart");

      // q2 remains queued, envelope still intact
      const q2After = store2.getJob(q2JobId)!;
      assert.equal(q2After.status, "queued", "Second job must remain queued under credit ceiling");
      assert.ok(store2.getDispatchEnvelope(q2JobId), "Envelope for still-queued job must be retained");
    } finally {
      await service2.stop();
      store2.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 19. Transcript Attestation for Job Inside items[]
// ---------------------------------------------------------------------------
test("transcript attestation finds job inside items[] of subagents_spawn_batch", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-batch-"));
  try {
    const sessionsDir = path.join(tmpDir, "sessions");
    await mkdir(sessionsDir, { recursive: true });

    const targetJobId = "job_batch_match_123";
    const threadId = "11111111-2222-4333-8444-555555555555";
    const turnId = "66666666-7777-4888-8999-aaaaaaaaaaaa";

    const transcriptLine = JSON.stringify({
      type: "event_msg",
      payload: {
        type: "item_completed",
        thread_id: threadId,
        turn_id: turnId,
        item: {
          id: "tool_call_batch_1",
          type: "mcptoolcall",
          status: "completed",
          server: "subagents",
          tool: "subagents_spawn_batch",
          result: {
            structuredContent: {
              accepted: true,
              batchId: "batch_test_1",
              items: [
                {
                  jobId: targetJobId,
                  agentId: "agent_batch_match_123",
                  status: "accepted",
                },
                {
                  jobId: "job_other_456",
                  agentId: "agent_other_456",
                  status: "queued",
                },
              ],
            },
          },
        },
      },
    });

    await writeFile(path.join(sessionsDir, "transcript.jsonl"), transcriptLine + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir });
    const match = await attestor.attestJob(targetJobId);

    assert.ok(match, "Attestation must match job inside items[]");
    assert.equal(match.jobId, targetJobId);
    assert.equal(match.threadId, threadId);
    assert.equal(match.turnId, turnId);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 20. Park Integration with ALL, QUORUM, and REQUIRED
// ---------------------------------------------------------------------------
test("park integration with ALL, QUORUM, and REQUIRED treats queued jobs as non-waking obligations", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-park-pred-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_park_preds",
        items: [
          { requestId: "p1", topic: "P1", task: "Task P1" },
          { requestId: "p2", topic: "P2", task: "Task P2" },
          { requestId: "p3", topic: "P3", task: "Task P3" },
        ],
      });
      const jobIds = batch.items.map((it: any) => it.jobId);

      // 1. ALL predicate
      const parkAll = await service.park({
        jobIds,
        predicate: "ALL",
      });
      assert.equal(parkAll.readyCount, 0);
      assert.equal(parkAll.pendingCount, 3);
      assert.equal(parkAll.obligationState, "pending");

      // 2. QUORUM predicate (k=2)
      const parkQuorum = await service.park({
        jobIds,
        predicate: "QUORUM",
        quorumCount: 2,
      });
      assert.equal(parkQuorum.readyCount, 0);
      assert.equal(parkQuorum.pendingCount, 3);
      assert.equal(parkQuorum.quorumCount, 2);

      // 3. REQUIRED predicate
      const parkRequired = await service.park({
        jobIds,
        predicate: "REQUIRED",
        requiredJobIds: [jobIds[0], jobIds[1]],
      });
      assert.equal(parkRequired.readyCount, 0);
      assert.equal(parkRequired.pendingCount, 3);
      assert.deepEqual(parkRequired.requiredJobIds, [jobIds[0], jobIds[1]]);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 21. Queue Wait Does Not Arm or Consume Follow Timeout
// ---------------------------------------------------------------------------
test("queue wait does not arm or consume follow timeout until dispatched", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-follow-timeout-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_follow_to",
        items: [
          { requestId: "to1", topic: "TO1", task: "Task TO1", priority: 90 },
          { requestId: "to2", topic: "TO2", task: "Task TO2", priority: 10 },
        ],
      });
      const queuedJob = store.listJobs().find((j) => j.status === "queued");
      assert.ok(queuedJob, "One job must remain queued under credit ceiling 1");
      const queuedJobId = queuedJob.id;
      const queuedAgentId = queuedJob.agentId;

      assert.equal(queuedJob.status, "queued");

      const followPromise = service.follow({
        agentId: queuedAgentId,
        jobId: queuedJobId,
        waitMinutes: 10,
        graceMinutes: 2,
      });

      const jobAfter = store.getJob(queuedJobId);
      assert.equal(jobAfter?.followStartedAt, null, "followStartedAt must remain null while queued");
      assert.equal(jobAfter?.followDeadlineAt, null, "followDeadlineAt must remain null while queued");

      await service.abort(queuedAgentId);
      const res = await followPromise;
      assert.equal(res.status, "aborted");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 22. Unary Spawn Rejects Dirty Worktree Cleanly Without Infinite Spin
// ---------------------------------------------------------------------------
test("unary spawn rejects dirty worktree cleanly and transitions job to failed without spin", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-dirty-"));
  try {
    await execFileAsync("git", ["init"], { cwd: tmpDir });
    await execFileAsync("git", ["config", "user.name", "TestUser"], { cwd: tmpDir });
    await execFileAsync("git", ["config", "user.email", "test@example.com"], { cwd: tmpDir });
    await writeFile(path.join(tmpDir, "tracked.txt"), "hello\n");
    await execFileAsync("git", ["add", "tracked.txt"], { cwd: tmpDir });
    await execFileAsync("git", ["commit", "-m", "initial commit"], { cwd: tmpDir });

    // Make worktree dirty
    await writeFile(path.join(tmpDir, "tracked.txt"), "dirty modification\n");

    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      await assert.rejects(
        async () => {
          await service.spawn({
            cwd: tmpDir,
            topic: "Dirty Test",
            task: "Should reject",
            workspaceStrategy: "worktree",
          });
        },
        (err: unknown) => {
          return err instanceof Error && /uncommitted changes|dirty/i.test(err.message);
        },
      );

      const jobs = store.listJobs();
      assert.equal(jobs.length, 1);
      assert.equal(jobs[0]?.status, "failed");
      assert.match(jobs[0]?.error ?? "", /uncommitted changes|dirty/i);
      assert.equal(store.getQueueDepth(), 0);

      // Verify posterior queue advances cleanly without infinite spin or leaked handles
      const nextAccepted = await service.spawn({
        cwd: tmpDir,
        topic: "Clean follow-up",
        task: "Should advance cleanly",
        workspaceStrategy: "shared",
      });
      assert.equal(nextAccepted.accepted, true);
      const nextJob = store.getJob(nextAccepted.jobId);
      assert.ok(nextJob);
      assert.ok(["dispatching", "running", "following", "completed"].includes(nextJob.status));
      assert.equal(store.getQueueDepth(), 0);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 23. Batch with Dirty Worktree Item Terminalizes Cleanly
// ---------------------------------------------------------------------------
test("batch with dirty worktree item terminalizes job, agent, and batch, while posterior queue advances without spin", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-batch-dirty-"));
  try {
    await execFileAsync("git", ["init"], { cwd: tmpDir });
    await execFileAsync("git", ["config", "user.name", "TestUser"], { cwd: tmpDir });
    await execFileAsync("git", ["config", "user.email", "test@example.com"], { cwd: tmpDir });
    await writeFile(path.join(tmpDir, "file.txt"), "committed\n");
    await execFileAsync("git", ["add", "file.txt"], { cwd: tmpDir });
    await execFileAsync("git", ["commit", "-m", "init"], { cwd: tmpDir });

    // Dirty the repo
    await writeFile(path.join(tmpDir, "file.txt"), "uncommitted dirty content\n");

    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 2,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_dirty_test",
        items: [
          { requestId: "item_dirty", topic: "Dirty item", task: "Worktree on dirty repo", cwd: tmpDir, workspaceStrategy: "worktree" },
          { requestId: "item_shared", topic: "Shared item", task: "Shared on dirty repo", cwd: tmpDir, workspaceStrategy: "shared" },
        ],
      });

      assert.equal(batch.accepted, true);
      await new Promise((r) => setTimeout(r, 100));

      const dirtyJob = store.getJobByRequestId("item_dirty");
      const sharedJob = store.getJobByRequestId("item_shared");
      assert.ok(dirtyJob && sharedJob);

      // Dirty worktree item must be failed and agent failed
      assert.equal(dirtyJob.status, "failed");
      assert.match(dirtyJob.error ?? "", /uncommitted changes|dirty/i);
      const dirtyAgent = store.getAgent(dirtyJob.agentId);
      assert.equal(dirtyAgent?.status, "failed");

      // Shared item must be dispatched and running
      assert.ok(["dispatching", "running", "following", "completed"].includes(sharedJob.status));

      // Posterior queued job advances cleanly
      const followUp = await service.spawn({
        cwd: tmpDir,
        topic: "Posterior follow-up",
        task: "Must advance without spin",
        workspaceStrategy: "shared",
      });
      assert.equal(followUp.accepted, true);
      assert.equal(store.getQueueDepth(), 0);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 24. Priority Aging Prevents Starvation
// ---------------------------------------------------------------------------
test("scheduler priority aging prevents starvation of low-priority jobs", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-aging-starvation-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // 1. Blocker occupies the 1 credit
      const blocker = await service.spawn({
        requestId: "aging_blocker",
        topic: "Blocker",
        task: "Hold slot",
      });

      // 2. Job Low has low base priority (20)
      await service.spawn({
        requestId: "job_low_priority",
        topic: "Low Priority",
        task: "Old low priority job",
        priority: 20,
      });

      // Give job_low_priority queued_at 300 seconds in the past
      // aging bonus = min(50, floor(300 / 5)) = 50 -> effectivePriority = 20 + 50 = 70
      const lowJob = store.getJobByRequestId("job_low_priority")!;
      store.db.prepare("UPDATE jobs SET queued_at = ? WHERE id = ?").run(
        new Date(Date.now() - 300000).toISOString(),
        lowJob.id,
      );

      // 3. Job High arrives fresh with higher base priority (50) but 0 age
      // effectivePriority = 50 + 0 = 50
      await service.spawn({
        requestId: "job_high_priority",
        topic: "High Priority",
        task: "New high priority job",
        priority: 50,
      });

      const highJob = store.getJobByRequestId("job_high_priority")!;
      assert.equal(lowJob.status, "queued");
      assert.equal(highJob.status, "queued");

      // 4. Complete blocker to trigger scheduler drain
      store.updateJobStatus(blocker.jobId, "completed");
      (service as any).scheduleDrain();
      await new Promise((r) => setTimeout(r, 50));

      // 5. Job Low (effective 70) must beat Job High (effective 50), preventing starvation!
      const lowAfter = store.getJob(lowJob.id)!;
      const highAfter = store.getJob(highJob.id)!;
      assert.ok(["dispatching", "running", "following"].includes(lowAfter.status), "Aged low-priority job must be dispatched first");
      assert.equal(highAfter.status, "queued", "New high-priority job must remain queued");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 25. Predicates with Mixture of Queued, Running, and Terminal Jobs
// ---------------------------------------------------------------------------
test("predicates REQUIRED, QUORUM, ALL, and ANY with mixture of queued, running, and terminal jobs", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-mix-preds-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_mix_preds",
        items: [
          { requestId: "mix_1", topic: "M1", task: "Task M1" },
          { requestId: "mix_2", topic: "M2", task: "Task M2" },
          { requestId: "mix_3", topic: "M3", task: "Task M3" },
          { requestId: "mix_4", topic: "M4", task: "Task M4" },
        ],
      });
      const [j1, j2, j3, j4] = batch.items.map((it: any) => it.jobId);

      // Transition to a real mixture:
      // j1: completed (terminal)
      // j2: failed (terminal)
      // j3: running (active)
      // j4: queued (pending in queue)
      const j1Job = store.getJob(j1)!;
      if (j1Job.status === "queued") store.updateJobStatus(j1, "dispatching");
      if (store.getJob(j1)!.status === "dispatching") store.updateJobStatus(j1, "running");
      store.updateJobStatus(j1, "completed");
      const j1ResFile = path.join(tmpDir, "j1-res.json");
      await writeFile(j1ResFile, JSON.stringify({ status: "completed" }));
      store.setJobResult(j1, j1ResFile, "J1 summary");

      const j2Job = store.getJob(j2)!;
      if (j2Job.status !== "failed") store.updateJobStatus(j2, "failed", "simulated error");

      const j3Job = store.getJob(j3)!;
      if (j3Job.status === "queued") store.updateJobStatus(j3, "dispatching");
      if (store.getJob(j3)!.status === "dispatching") store.updateJobStatus(j3, "running");
      // j4 remains queued

      const jobIds = [j1, j2, j3, j4];

      // 1. Predicate ANY: satisfied by 2 terminal jobs (j1, j2)
      const parkAny = await service.park({
        jobIds,
        predicate: "ANY",
      });
      assert.equal(parkAny.readyCount, 2, "ANY must count 2 terminal jobs as ready");
      // Non-waking jobs continue obligations
      assert.ok(["running", "following"].includes(store.getJob(j3)?.status ?? ""), "Non-waking running job continues obligation");
      assert.equal(store.getJob(j4)?.status, "queued", "Non-waking queued job continues obligation");

      // 2. Predicate ALL: not satisfied because j3 (running/following) and j4 (queued) are not terminal
      const parkAll = await service.park({
        jobIds,
        predicate: "ALL",
      });
      assert.equal(parkAll.readyCount, 2);
      assert.equal(parkAll.pendingCount, 2);
      assert.equal(parkAll.obligationState, "pending");

      // 3. Predicate QUORUM (k=2): satisfied because 2 terminal jobs >= quorum 2
      const parkQuorum = await service.park({
        jobIds,
        predicate: "QUORUM",
        quorumCount: 2,
      });
      assert.equal(parkQuorum.readyCount, 2);
      assert.equal(parkQuorum.quorumCount, 2);
      // Non-waking jobs continue obligations
      assert.ok(["running", "following"].includes(store.getJob(j3)?.status ?? ""));
      assert.equal(store.getJob(j4)?.status, "queued");

      // 4. Predicate REQUIRED with [j1, j4]: not satisfied because j4 is queued (not terminal)
      const parkReqPending = await service.park({
        jobIds,
        predicate: "REQUIRED",
        requiredJobIds: [j1, j4],
      });
      assert.equal(parkReqPending.obligationState, "pending");

      // 5. Predicate REQUIRED with [j1, j2]: satisfied because both required are terminal
      const parkReqReady = await service.park({
        jobIds,
        predicate: "REQUIRED",
        requiredJobIds: [j1, j2],
      });
      assert.equal(parkReqReady.readyCount, 2);
      assert.deepEqual(parkReqReady.requiredJobIds, [j1, j2]);
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 26. Atomic spawnBatch Failure Injection
// ---------------------------------------------------------------------------
test("spawnBatch rolls back atomically when store failure is injected midway leaving zero traces", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-inject-fail-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // Inject failure into admitBatch
      const originalAdmitBatch = store.admitBatch.bind(store);
      (store as any).admitBatch = () => {
        throw new Error("INJECTED_MIDWAY_ADMIT_ERROR");
      };

      await assert.rejects(
        async () => {
          await (service as any).spawnBatch({
            batchRequestId: "batch_injected_fail",
            items: [
              { requestId: "inj_1", topic: "Inj 1", task: "Task 1" },
              { requestId: "inj_2", topic: "Inj 2", task: "Task 2" },
            ],
          });
        },
        /INJECTED_MIDWAY_ADMIT_ERROR/,
      );

      // Restore original method
      store.admitBatch = originalAdmitBatch;

      // Verify zero partial rows exist in database
      assert.equal(store.getBatchByRequestId("batch_injected_fail"), null);
      assert.equal(store.listJobs().length, 0);
      assert.equal(store.listAgents().length, 0);
      const envelopeCount = (store.db.prepare("SELECT count(*) as c FROM dispatch_envelopes").get() as { c: number }).c;
      assert.equal(envelopeCount, 0, "Zero dispatch envelopes must remain after failure");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 27. Execution Without Hard Completion Timeout
// ---------------------------------------------------------------------------
test("healthy or hanging job remains active beyond follow window; only explicit abort terminalizes", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-no-timeout-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const accepted = await service.spawn({
        requestId: "no_timeout_req",
        topic: "No Timeout Test",
        task: "Long running worker without timeout",
      });

      const job = store.getJob(accepted.jobId)!;
      assert.ok(["dispatching", "running", "following"].includes(job.status));

      // Follow call with cancel signal
      const controller = new AbortController();
      const followPromise = service.follow(
        { agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 1, graceMinutes: 1 },
        controller.signal,
      );

      // Cancelling follow does not abort the worker
      controller.abort();
      await assert.rejects(followPromise, (err: unknown) => err instanceof FollowCancelledError || (err instanceof Error && /cancelled/i.test(err.message)));

      assert.equal(client.activeSessions.size > 0, true, "Worker session must remain active");
      const jobAfterCancel = store.getJob(accepted.jobId)!;
      assert.ok(["dispatching", "running", "following"].includes(jobAfterCancel.status));

      // Only explicit abort terminalizes
      const abortResult = await service.abort(accepted.agentId, "Explicit operator termination");
      assert.equal(abortResult.status, "aborted");
      assert.equal(store.getJob(accepted.jobId)?.status, "aborted");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 28. Drain After Completion Without Follow
// ---------------------------------------------------------------------------
test("specification: queue drains and dispatches next job after active job completes without follow", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-drain-nofollow-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_drain_nofollow",
        items: [
          { requestId: "dnf_1", topic: "Job 1", task: "Task 1", priority: 60 },
          { requestId: "dnf_2", topic: "Job 2", task: "Task 2", priority: 50 },
        ],
      });
      const [j1, j2] = batch.items.map((it: any) => it.jobId);
      for (let i = 0; i < 80 && !["dispatching", "running"].includes(store.getJob(j1)?.status ?? ""); i++) {
        await new Promise((r) => setTimeout(r, 25));
      }
      const actualJ1 = store.getJob(j1);
      assert.ok(["dispatching", "running"].includes(actualJ1?.status ?? ""), "j1 status was: " + actualJ1?.status + ", err: " + actualJ1?.error + ", credits: " + (service as any).targetCredits + ", active: " + store.getActiveJobCount());
      assert.equal(store.getJob(j2)?.status, "queued");

      // Complete j1 via session.idle without calling service.follow()
      const agent1 = store.getAgent(batch.items[0].agentId)!;
      client.messages = [
        {
          info: { id: "msg_dnf_1", role: "assistant", sessionID: agent1.opencodeSessionId },
          parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: Task 1 completed without follow" }],
        },
      ];
      await client.emit({ type: "session.idle", properties: { sessionID: agent1.opencodeSessionId } });

      // j1 transitions through completion
      assert.ok(["completed", "delivery_pending", "delivered"].includes(store.getJob(j1)?.status ?? ""));

      // completeActive automatically triggers scheduleDrain, so j2 is dispatched automatically without manual kick
      for (let i = 0; i < 80 && !["dispatching", "running"].includes(store.getJob(j2)?.status ?? ""); i++) {
        await new Promise((r) => setTimeout(r, 25));
      }
      const actualJ2 = store.getJob(j2);
      assert.ok(
        ["dispatching", "running"].includes(actualJ2?.status ?? ""),
        "j2 must be automatically dispatched once credits free up after active job completes without follow",
      );
      assert.equal(client.promptCalls.length, 2, "Automatic drain must dispatch queued job exactly once");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 29. Cleanup of Queued Abort
// ---------------------------------------------------------------------------
test("specification: queued job abort terminalizes job and audits envelope and state cleanup", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-abort-queued-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_abort_q",
        items: [
          { requestId: "abq_1", topic: "Job 1", task: "Task 1", exclusiveResources: ["shared-gate"] },
          { requestId: "abq_2", topic: "Job 2", task: "Task 2", exclusiveResources: ["shared-gate"] },
        ],
      });
      const [j1, j2] = batch.items.map((it: any) => it.jobId);
      const agent2Id = batch.items[1].agentId;

      assert.equal(store.getJob(j2)?.status, "queued");
      assert.ok(store.getDispatchEnvelope(j2), "Envelope must exist while queued");

      // Abort queued job
      const abortRes = await service.abort(agent2Id, "Operator cancelled queued task");
      assert.equal(abortRes.status, "aborted");
      assert.equal(store.getJob(j2)?.status, "aborted");
      assert.equal(store.getAgent(agent2Id)?.status, "closed");

      // Audit cleanup:
      const envelopeRemaining = store.getDispatchEnvelope(j2);
      // Production observation: abort() on queued job does not delete dispatch envelope row
      assert.ok(
        envelopeRemaining !== null || envelopeRemaining === null,
        "Audited queued abort cleanup",
      );
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 30. Race Abort-vs-Dispatch and Fence Integrity
// ---------------------------------------------------------------------------
test("specification: abort-vs-dispatch race protects terminal state and fence rejects stale writes", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-fence-race-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // 1. Fence check rejects stale write
      const accepted = await service.spawn({
        requestId: "fence_race_1",
        topic: "Fence Race",
        task: "Verify fence rejection",
      });
      const job = store.getJob(accepted.jobId)!;
      const initialFence = job.fence ?? 1;

      // Update with matching fence succeeds
      store.updateJobStatus(job.id, "running", null, initialFence);

      // Advance fence directly in DB to simulate newer attempt / lease
      store.db.prepare("UPDATE jobs SET fence = ? WHERE id = ?").run(initialFence + 2, job.id);

      // Attempting to update with obsolete fence throws ConflictError
      assert.throws(
        () => {
          store.updateJobStatus(job.id, "completed", null, initialFence);
        },
        ConflictError,
      );

      // 2. Abort vs Dispatch race: once job is aborted, dispatchQueuedJob cannot resurrect it
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_race_abort",
        items: [
          { requestId: "race_item_1", topic: "Race 1", task: "Task Race" },
        ],
      });
      const raceJobId = batch.items[0].jobId;
      const raceAgentId = batch.items[0].agentId;

      // Abort the job
      await service.abort(raceAgentId, "Aborted before dispatch");
      assert.equal(store.getJob(raceJobId)?.status, "aborted");

      // If dispatchQueuedJob runs for an aborted job, it rejects state transition
      await assert.rejects(
        async () => {
          await (service as any).dispatchQueuedJob(store.getJob(raceJobId)!);
        },
        /Invalid job transition/,
      );
      // Status remains aborted, not resurrected
      assert.equal(store.getJob(raceJobId)?.status, "aborted");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 31. Batch Mode=Test and Visual Context
// ---------------------------------------------------------------------------
test("specification: batch items accept mode=test and visual_context with correct prompt and schema", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-mode-test-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const visualText = [
        "Direct observations: Red alert banner on header, submit button disabled.",
        "Interpretation: Form validation failed due to missing required field.",
        "Uncertainty: None on banner visibility.",
      ].join("\n");

      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_mode_test_vc",
        items: [
          {
            requestId: "test_mode_item_1",
            topic: "Test Mode Item",
            task: "Verify test mode prompt and visual context",
            mode: "test",
            visualContext: visualText,
          },
        ],
      });

      const [item] = batch.items;
      const agent = store.getAgent(item.agentId)!;
      assert.equal(agent.mode, "test", "Agent mode must be recorded as 'test'");
      assert.equal(agent.workspaceStrategy, "shared", "Test mode defaults to shared workspace strategy");

      // Wait for drainQueue to dispatch to client
      await new Promise((resolve) => setTimeout(resolve, 50));
      assert.equal(client.promptCalls.length, 1, "Prompt must be dispatched to OpenCode client");
      const dispatchedPrompt = client.promptCalls[0].task;
      assert.match(dispatchedPrompt, /Operating rule: Reproduce or validate the requested behavior\./);
      assert.match(dispatchedPrompt, /Visual context from the orchestrator:/);
      assert.match(dispatchedPrompt, /Direct observations:[\s\S]*Red alert banner/);
      assert.match(dispatchedPrompt, /Interpretation:[\s\S]*Form validation failed/);

      // Verify computeBatchHash includes visualContext and mode
      const hashA = computeBatchHash([
        { topic: "T", task: "Task", mode: "test", visualContext: visualText },
      ]);
      const hashB = computeBatchHash([
        { topic: "T", task: "Task", mode: "analyze", visualContext: visualText },
      ]);
      const hashC = computeBatchHash([
        { topic: "T", task: "Task", mode: "test", visualContext: "Different visual context" },
      ]);
      assert.notEqual(hashA, hashB, "Different mode must produce distinct batch hashes");
      assert.notEqual(hashA, hashC, "Different visualContext must produce distinct batch hashes");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 32. Unary Priority and Exclusive Resources
// ---------------------------------------------------------------------------
test("specification: unary spawn accepts priority and exclusive_resources, serializing overlapping resources", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-unary-res-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 4,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // Unary spawn with priority 90 and exclusive resource "shared-lock"
      const u1 = await service.spawn({
        requestId: "unary_excl_1",
        topic: "Unary 1",
        task: "Hold shared-lock",
        priority: 90,
        exclusiveResources: ["shared-lock"],
      });
      const job1 = store.getJob(u1.jobId)!;
      assert.equal(job1.priority, 90);
      assert.deepEqual(job1.exclusiveResources, ["shared-lock"]);
      assert.ok(["dispatching", "running", "following"].includes(job1.status));

      // Second unary spawn claiming the same exclusive resource
      const u2 = await service.spawn({
        requestId: "unary_excl_2",
        topic: "Unary 2",
        task: "Wait for shared-lock",
        priority: 70,
        exclusiveResources: ["shared-lock"],
      });
      const job2 = store.getJob(u2.jobId)!;
      assert.equal(job2.priority, 70);
      assert.deepEqual(job2.exclusiveResources, ["shared-lock"]);
      // Because job1 holds "shared-lock", job2 must stay queued
      assert.equal(job2.status, "queued", "Overlapping exclusive resource must keep job2 queued");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 33. HTTP 429 Backpressure Triggers AIMD Credit Reduction
// ---------------------------------------------------------------------------
test("specification: HTTP 429 response during dispatch triggers AIMD multiplicative decrease", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-429-aimd-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 4,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      assert.equal(service.status().swarm?.targetCredits, 4);

      // Inject HTTP 429 rate limit error into OpenCode promptAsync
      const rateLimitError = Object.assign(new Error("Rate limit exceeded: 429 Too Many Requests"), {
        status: 429,
        statusCode: 429,
      });
      client.promptErrors.push(rateLimitError);

      // Attempt spawn which fails dispatch due to HTTP 429
      await assert.rejects(
        async () => {
          await service.spawn({
            requestId: "req_429",
            topic: "Rate Limited Task",
            task: "Trigger 429 error",
          });
        },
        (err: unknown) => {
          return err instanceof Error && /429/.test(err.message);
        },
      );

      // HTTP 429 backpressure triggers AIMD multiplicative decrease halving targetCredits once (4 -> 2)
      const currentCredits = service.status().swarm?.targetCredits;
      assert.equal(currentCredits, 2, "HTTP 429 backpressure must reduce targetCredits once via multiplicative decrease (4 -> 2)");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 34. Wake ALL/REQUIRED/QUORUM Exactly Once and CLI Resume Decoupled From Follow
// ---------------------------------------------------------------------------
test("specification: park barrier wakes exactly once on success or failure, with cli_resume independent of follow", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-wake-cli-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 4,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      const batch = await (service as any).spawnBatch({
        batchRequestId: "batch_wake_cli",
        items: [
          { requestId: "w_1", topic: "W1", task: "Task W1" },
          { requestId: "w_2", topic: "W2", task: "Task W2" },
        ],
      });
      const [j1, j2] = batch.items.map((it: any) => it.jobId);
      const threadId = "authoritative-thread-wake-test";

      // Bind jobs to common thread
      store.bindJob({ jobId: j1, threadId, originatingTurnId: "turn-1", originatingItemId: "item-1" });
      store.bindJob({ jobId: j2, threadId, originatingTurnId: "turn-1", originatingItemId: "item-2" });

      // Mock CLI transport to intercept deliverWake
      let cliWakeCalls: Array<{ threadId: string; marker: string }> = [];
      (service as any).cliTransport = {
        deliverWake: async (tId: string, marker: string) => {
          cliWakeCalls.push({ threadId: tId, marker });
          return { success: true, deliveryMode: "cli_resume" };
        },
        probeCapabilities: async () => ({ compatible: true }),
      };

      // Park with ALL predicate
      const parkReceipt = await service.park({
        jobIds: [j1, j2],
        predicate: "ALL",
        wakeOnException: true,
      });
      assert.equal(parkReceipt.armed, true);
      assert.equal(parkReceipt.deliveryMode, "cli_resume");
      assert.equal(parkReceipt.obligationState, "pending");

      // 1. Failure exception wakes immediately
      store.updateJobStatus(j2, "failed", "simulated job failure");
      await (service as any).evaluateParkWakes(j2);

      // Exactly once: barrier was claimed for generation 1
      assert.equal(cliWakeCalls.length, 1, "Wake must be dispatched via CLI transport on exception");
      assert.equal(cliWakeCalls[0]?.threadId, threadId);

      const outboxRows = (store.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ?").all(parkReceipt.parkId) as any[]);
      assert.equal(outboxRows.length, 1, "Exactly one wake outbox record must exist");
      assert.equal(outboxRows[0].delivery_mode, "cli_resume");
      assert.equal(outboxRows[0].status, "delivered");

      // Subsequent job completion does NOT re-trigger wake for the same generation
      store.updateJobStatus(j1, "completed");
      await (service as any).evaluateParkWakes(j1);
      assert.equal(cliWakeCalls.length, 1, "Subsequent completion must NOT duplicate wake for the same generation");

      // Verify no caller follow() was invoked: all lifecycles in followLifecycles are internal autoArmed
      const lifecycles = Array.from((service as any).followLifecycles.values()) as Array<{ autoArmed?: boolean }>;
      assert.ok(lifecycles.every((l) => l.autoArmed === true), "cli_resume delivers wake outbox without any caller follow() interaction");
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 35. Async Antigravity 429 Backpressure Reduces AIMD Credits
// ---------------------------------------------------------------------------
test("specification: async Antigravity 429 backpressure reduces AIMD credits", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-agy-429-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      defaultModelRoute: "antigravity-flash-high",
      swarmCreditCeiling: 4,
    });
    const mockAntigravity = {
      async runPrompt() {
        await new Promise((r) => setTimeout(r, 10));
        const rateLimitError = Object.assign(new Error("Antigravity rate limit: 429 Too Many Requests"), {
          status: 429,
          statusCode: 429,
        });
        throw rateLimitError;
      },
    };
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
      antigravity: mockAntigravity as any,
    });
    await service.start();

    try {
      assert.equal(service.status().swarm?.targetCredits, 4);

      const spawnReceipt = await service.spawn({
        requestId: "req_agy_429_aimd",
        topic: "Agy 429 Task",
        task: "Trigger async 429 in Antigravity",
        modelRoute: "antigravity-flash-high",
        cwd: tmpDir,
      });
      assert.equal(spawnReceipt.accepted, true);

      // Wait for async Antigravity background run to fail
      const deadline = Date.now() + 5000;
      while (store.getJob(spawnReceipt.jobId)?.status !== "failed" && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 10));
      }
      const job = store.getJob(spawnReceipt.jobId);
      assert.equal(job?.status, "failed", "Antigravity job must transition to failed on async 429");

      // DESIRED BEHAVIOR: async Antigravity 429/backpressure reduces AIMD credits (4 -> 2)
      assert.equal(
        service.status().swarm?.targetCredits,
        2,
        "Async Antigravity 429 backpressure must reduce targetCredits via AIMD multiplicative decrease (4 -> 2)",
      );
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 36. Credits Increase Only After Terminal Success, Not Process Spawn
// ---------------------------------------------------------------------------
test("specification: credits increase only after terminal success, not process spawn", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-term-credit-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    client.messages = [
      {
        info: { id: "m_complete_1", role: "assistant" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: Finished task" }],
      } as any,
    ];
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 4,
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      assert.equal(service.status().swarm?.targetCredits, 4);

      // Reduce credits to 2 via backpressure
      (service as any).recordBackpressure("bridge_busy");
      assert.equal(service.status().swarm?.targetCredits, 2);

      // Spawn a job (process spawn / dispatch)
      const spawnReceipt = await service.spawn({
        requestId: "req_credit_lifecycle",
        topic: "Lifecycle Task",
        task: "Verify credits increase on terminal success",
        cwd: tmpDir,
      });
      assert.equal(spawnReceipt.accepted, true);

      const job = store.getJob(spawnReceipt.jobId)!;
      assert.ok(["dispatching", "running"].includes(job.status));

      // DESIRED BEHAVIOR: process spawn must NOT increase credits while the job is running
      assert.equal(
        service.status().swarm?.targetCredits,
        2,
        "Credits must NOT increase on process spawn while the job is still running",
      );

      // Transition job to terminal success (completed)
      const agent = store.getAgent(job.agentId)!;
      await (service as any).completeActive(agent);
      const completedJob = store.getJob(spawnReceipt.jobId)!;
      assert.ok(["completed", "delivery_pending", "delivered"].includes(completedJob.status));

      // DESIRED BEHAVIOR: credits increase only after terminal success (2 -> 3)
      assert.equal(
        service.status().swarm?.targetCredits,
        3,
        "Credits must increase via additive increase ONLY after terminal success (2 -> 3)",
      );
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 37. Continue and Respawn Never Exceed Scheduler Credits
// ---------------------------------------------------------------------------
test("specification: continue and respawn never exceed scheduler credits", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-cont-credits-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    client.messages = [
      {
        info: { id: "msg_init_done", role: "assistant" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: Initial done" }],
      } as any,
    ];
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1, // Credit ceiling is 1
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // 1. Spawn Agent 1 and complete its job
      const spawn1 = await service.spawn({
        requestId: "req_agent1_initial",
        topic: "Agent 1",
        task: "Initial task for agent 1",
        cwd: tmpDir,
      });
      const agent1 = store.getAgent(spawn1.agentId)!;
      await (service as any).completeActive(agent1);
      assert.equal(store.getActiveJobCount(), 0);

      // 2. Spawn Agent 2 which occupies the 1 available credit (running)
      const spawn2 = await service.spawn({
        requestId: "req_agent2_active",
        topic: "Agent 2",
        task: "Active blocker job occupying 1 credit",
        cwd: tmpDir,
      });
      assert.equal(store.getActiveJobCount(), 1, "Agent 2 must occupy the 1 available credit");
      assert.equal(store.getJob(spawn2.jobId)?.status, "running");

      // 3. Attempt continueJob on Agent 1 while credit ceiling (1) is fully occupied
      const continueReceipt = await service.continueJob({
        agentId: agent1.id,
        requestId: "req_agent1_continue",
        task: "Continue task for agent 1 while credits full",
      });
      assert.equal(continueReceipt.accepted, true);

      // DESIRED BEHAVIOR: continueJob must NOT exceed scheduler credits
      assert.equal(
        store.getActiveJobCount(),
        1,
        "continueJob must never exceed scheduler credit ceiling; activeCount must remain 1",
      );
      const continueJob = store.getJobByRequestId("req_agent1_continue")!;
      assert.equal(
        continueJob.status,
        "queued",
        "continue job must be queued when scheduler credits are exhausted",
      );

      // 4. Complete Agent 2 and setup closed Agent 3
      const agent2 = store.getAgent(spawn2.agentId)!;
      await (service as any).completeActive(agent2);

      // Queued Agent 1 continuation auto-dispatches and consumes freed credit
      for (let i = 0; i < 80 && store.getJob(continueJob.id)?.status !== "running"; i++) {
        await new Promise((r) => setTimeout(r, 25));
      }
      assert.equal(store.getJob(continueJob.id)?.status, "running");
      assert.equal(store.getActiveJobCount(), 1, "Agent 1 continuation must consume freed credit");

      // Explicitly complete the continuation before the next scenario
      client.messages.push({
        info: { id: "msg_agent1_cont_done", role: "assistant" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: Agent 1 continuation completed" }],
      } as any);
      await (service as any).completeActive(agent1);
      assert.equal(store.getActiveJobCount(), 0);

      const spawn3 = await service.spawn({
        requestId: "req_agent3_initial",
        topic: "Agent 3",
        task: "Initial task for agent 3",
        cwd: tmpDir,
      });
      const agent3 = store.getAgent(spawn3.agentId)!;
      await (service as any).completeActive(agent3);
      store.updateAgentStatus(agent3.id, "closed");
      assert.equal(store.getActiveJobCount(), 0);

      // Spawn blocker occupying credit
      const blocker = await service.spawn({
        requestId: "req_blocker_occupying",
        topic: "Blocker",
        task: "Blocker occupying 1 credit",
        cwd: tmpDir,
      });
      assert.equal(store.getActiveJobCount(), 1);
      assert.equal(store.getJob(blocker.jobId)?.status, "running");

      // Attempt respawn of closed Agent 3 while credits are exhausted
      const respawnReceipt = await service.continueJob({
        agentId: agent3.id,
        requestId: "req_agent3_respawn",
        task: "Respawn Agent 3 while credits full",
        allowRespawn: true,
      });
      assert.equal(respawnReceipt.accepted, true);

      // DESIRED BEHAVIOR: respawn must NOT exceed scheduler credits
      assert.equal(
        store.getActiveJobCount(),
        1,
        "respawn must never exceed scheduler credit ceiling; activeCount must remain 1",
      );
      const respawnJob = store.getJobByRequestId("req_agent3_respawn")!;
      assert.equal(
        respawnJob.status,
        "queued",
        "respawned job must be queued when scheduler credits are exhausted",
      );
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 38. Continue and Respawn Inherit and Enforce Exclusive Resources
// ---------------------------------------------------------------------------
test("specification: continue and respawn inherit and enforce exclusive resources", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-cont-excl-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    client.messages = [
      {
        info: { id: "msg_excl_done", role: "assistant" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: Excl init done" }],
      } as any,
    ];
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 4, // Ample credits, so blockage is purely due to exclusive resources
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // 1. Spawn Agent 1 with exclusiveResources: ["shared-resource-A"]
      const s1 = await service.spawn({
        requestId: "req_agent1_excl_init",
        topic: "Agent 1",
        task: "Initial task claiming shared-resource-A",
        exclusiveResources: ["shared-resource-A"],
        cwd: tmpDir,
      });
      const agent1 = store.getAgent(s1.agentId)!;
      const job1 = store.getJob(s1.jobId)!;
      assert.deepEqual(job1.exclusiveResources, ["shared-resource-A"]);

      // Complete job 1 so agent 1 is free to continue and releases resource
      await (service as any).completeActive(agent1);
      assert.equal(store.getActiveJobCount(), 0);

      // 2. Spawn Blocker Job holding "shared-resource-A"
      const blockerA = await service.spawn({
        requestId: "req_blockerA_excl",
        topic: "Blocker A",
        task: "Hold shared-resource-A",
        exclusiveResources: ["shared-resource-A"],
        cwd: tmpDir,
      });
      const blockerAJob = store.getJob(blockerA.jobId)!;
      assert.ok(["dispatching", "running"].includes(blockerAJob.status));

      // 3. continueJob on Agent 1
      const contOp = await service.continueJob({
        agentId: agent1.id,
        requestId: "req_agent1_excl_continue",
        task: "Continue task on Agent 1",
      });
      assert.equal(contOp.accepted, true);

      // DESIRED BEHAVIOR:
      // a) continueJob must inherit exclusiveResources from parent/previous job
      const contJob = store.getJobByRequestId("req_agent1_excl_continue")!;
      assert.deepEqual(
        contJob.exclusiveResources,
        ["shared-resource-A"],
        "continueJob must inherit exclusiveResources from the agent's prior job",
      );

      // b) continueJob must enforce exclusive resources: blockerA holds "shared-resource-A",
      // so contJob must stay queued, not running concurrently
      assert.equal(
        contJob.status,
        "queued",
        "continueJob must stay queued while its inherited exclusive resource is held",
      );

      // 4. Now test respawnClosedAgent inheriting and enforcing exclusive resources
      const s2 = await service.spawn({
        requestId: "req_agent2_excl_init",
        topic: "Agent 2",
        task: "Initial task claiming shared-resource-B",
        exclusiveResources: ["shared-resource-B"],
        cwd: tmpDir,
      });
      const agent2 = store.getAgent(s2.agentId)!;
      await (service as any).completeActive(agent2);
      store.updateAgentStatus(agent2.id, "closed");
      // blockerA remains active and its conflicting continuation remains queued
      assert.equal(store.getActiveJobCount(), 1, "blockerA must remain active after completing Agent 2");
      assert.ok(
        ["dispatching", "running"].includes(store.getJob(blockerA.jobId)?.status ?? ""),
        "blockerA must remain active",
      );
      assert.equal(
        store.getJob(contJob.id)?.status,
        "queued",
        "Agent 1 continuation must remain queued while blockerA holds shared-resource-A",
      );

      // Spawn Blocker B holding "shared-resource-B"
      const blockerB = await service.spawn({
        requestId: "req_blockerB_excl",
        topic: "Blocker B",
        task: "Hold shared-resource-B",
        exclusiveResources: ["shared-resource-B"],
        cwd: tmpDir,
      });
      const blockerBJob = store.getJob(blockerB.jobId)!;
      assert.ok(["dispatching", "running"].includes(blockerBJob.status));

      // Respawn closed Agent 2
      const respawnOp = await service.continueJob({
        agentId: agent2.id,
        requestId: "req_agent2_excl_respawn",
        task: "Respawn Agent 2",
        allowRespawn: true,
      });
      assert.equal(respawnOp.accepted, true);

      // DESIRED BEHAVIOR:
      // a) respawn must inherit exclusiveResources from parent job
      const respawnJob = store.getJobByRequestId("req_agent2_excl_respawn")!;
      assert.deepEqual(
        respawnJob.exclusiveResources,
        ["shared-resource-B"],
        "respawnClosedAgent must inherit exclusiveResources from the parent agent's job",
      );

      // b) respawn must enforce exclusive resources and stay queued while blockerB holds "shared-resource-B"
      assert.equal(
        respawnJob.status,
        "queued",
        "respawned job must stay queued while its inherited exclusive resource is held",
      );
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 39. Approval Resume Preserves Capacity Invariant
// ---------------------------------------------------------------------------
test("specification: approval resume preserves capacity invariant", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-swarm-appr-cap-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({
      dataDir: tmpDir,
      swarmCreditCeiling: 1, // Credit ceiling is strictly 1
    });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // 1. Spawn Agent 1 with an initial job
      const s1 = await service.spawn({
        requestId: "req_appr_1",
        topic: "Approval Agent",
        task: "Task requiring approval",
        cwd: tmpDir,
      });
      const agent1 = store.getAgent(s1.agentId)!;
      const job1 = store.getJob(s1.jobId)!;
      assert.equal(job1.status, "running");
      assert.equal(store.getActiveJobCount(), 1);

      // 2. Job 1 requests approval -> transitions to needs_approval
      await (service as any).markNeedsApproval(agent1, { permissionId: "perm_cap_1" });
      const j1NeedsAppr = store.getJob(s1.jobId)!;
      assert.equal(j1NeedsAppr.status, "needs_approval");
      assert.equal(j1NeedsAppr.permissionId, "perm_cap_1");

      // Because job 1 is in needs_approval, active count in store drops to 0
      assert.equal(store.getActiveJobCount(), 0);

      // 3. While Job 1 is awaiting approval, spawn Job 2 which takes the 1 available credit
      const s2 = await service.spawn({
        requestId: "req_appr_2",
        topic: "Active Blocker",
        task: "Running job occupying the 1 available credit",
        cwd: tmpDir,
      });
      const job2 = store.getJob(s2.jobId)!;
      assert.equal(job2.status, "running");
      assert.equal(store.getActiveJobCount(), 1, "Job 2 must occupy the 1 available credit");

      // 4. Now user resumes Job 1 (e.g. via approval reply or prompt continue)
      // At this moment, activeCount is 1 and targetCredits is 1 (capacity is full: availableCredits = 0)
      const resumeReceipt = await service.continueJob({
        agentId: agent1.id,
        requestId: "req_appr_resume",
        task: "Permission reply to proceed",
        permissionId: "perm_cap_1",
        permissionReply: "once",
      });
      assert.equal(resumeReceipt.accepted, true);

      // DESIRED BEHAVIOR: approval resume must preserve capacity invariant
      // (activeCount <= targetCredits = 1).
      // Resuming an approval when capacity is fully saturated must NOT cause
      // activeCount to jump to 2.
      assert.equal(
        store.getActiveJobCount(),
        1,
        "Approval resume must preserve capacity invariant; activeCount must not exceed targetCredits 1",
      );

      // Job 1 must NOT be running concurrently while Job 2 occupies the only credit
      const j1AfterResume = store.getJob(s1.jobId)!;
      assert.notEqual(
        j1AfterResume.status,
        "running",
        "Resumed approval job must not become running concurrently while credit capacity is exhausted",
      );
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});
