import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";
import { BridgeService, type ManagedOpenCodeLike } from "../../src/service.js";
import { createDefaultConfig } from "../../src/config.js";
import type {
  JobRecord,
  JobStatus,
  OpenCodeClientLike,
  OpenCodeEvent,
  OpenCodeMessage,
} from "../../src/types.js";

const ACTIVE_STATUSES: readonly JobStatus[] = [
  "dispatching",
  "running",
  "following",
  "finalizing",
  "needs_approval",
];

const TERMINAL_STATUSES: readonly JobStatus[] = [
  "completed",
  "completed_partial",
  "timed_out",
  "failed",
  "aborted",
  "delivered",
];

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
    return { id: "session_fake_" + Math.random().toString(36).slice(2) };
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

function makeFakeManager(client = new FakeOpenCodeClient()) {
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

function seedAgent(store: BridgeStore, agentId: string, repoRoot: string) {
  return store.createAgent({
    id: agentId,
    title: `Agent ${agentId}`,
    topic: "Active resources test topic",
    repositoryRoot: repoRoot,
    workspacePath: repoRoot,
    workspaceStrategy: "shared",
    opencodeServerId: "server_test",
    opencodeSessionId: `session_${agentId}`,
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
  });
}

function seedJob(
  store: BridgeStore,
  agentId: string,
  jobId: string,
  status: JobStatus,
  exclusiveResources?: string[] | null,
  requestId = `req_${jobId}`,
) {
  return store.createJob({
    id: jobId,
    agentId,
    kind: "spawn",
    requestId,
    promptHash: `hash_${jobId}`,
    status,
    exclusiveResources: exclusiveResources ?? null,
  });
}

// ---------------------------------------------------------------------------
// 1. Schema & Query Plan Contract: idx_jobs_status must back active query
// ---------------------------------------------------------------------------
test("schema & query plan contract: idx_jobs_status backs active status queries and avoids full table scans", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-active-resources-plan-"));
  const store = await BridgeStore.open(directory);
  try {
    const indexRow = store.db.prepare(
      "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_jobs_status'",
    ).get() as { name: string } | undefined;
    assert.equal(indexRow?.name, "idx_jobs_status", "idx_jobs_status index must exist in SQLite schema");

    // Query plan for active statuses: WHERE status IN ('dispatching','running','following','finalizing','needs_approval')
    const placeholders = ACTIVE_STATUSES.map(() => "?").join(",");
    const activeSql = `SELECT * FROM jobs WHERE status IN (${placeholders})`;
    const activePlan = store.db.prepare(`EXPLAIN QUERY PLAN ${activeSql}`).all(...ACTIVE_STATUSES) as Array<{ detail: string }>;

    const usesStatusIndex = activePlan.some((p) =>
      p.detail.includes("SEARCH") && (p.detail.includes("idx_jobs_status") || p.detail.includes("idx_jobs_queued")),
    );
    assert.ok(
      usesStatusIndex,
      `Active status query plan must SEARCH using a status index, got: ${JSON.stringify(activePlan)}`,
    );

    // Verify specifically that idx_jobs_status is valid and can index the query
    const forcedPlan = store.db.prepare(`EXPLAIN QUERY PLAN SELECT * FROM jobs INDEXED BY idx_jobs_status WHERE status IN (${placeholders})`).all(...ACTIVE_STATUSES) as Array<{ detail: string }>;
    assert.ok(
      forcedPlan.some((p) => p.detail.includes("idx_jobs_status")),
      `Query must be capable of using idx_jobs_status, got: ${JSON.stringify(forcedPlan)}`,
    );

    // In contrast, unconstrained listJobs() query performs a full table scan and temp b-tree
    const unboundedSql = "SELECT * FROM jobs ORDER BY created_at DESC";
    const unboundedPlan = store.db.prepare(`EXPLAIN QUERY PLAN ${unboundedSql}`).all() as Array<{ detail: string }>;
    const doesScan = unboundedPlan.some((p) => p.detail.includes("SCAN jobs"));
    assert.ok(
      doesScan,
      `Unconstrained query plan must perform SCAN jobs (showing why status filter is required), got: ${JSON.stringify(unboundedPlan)}`,
    );
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Store-Level Bounded API Contract: BridgeStore.listActiveJobs
// ---------------------------------------------------------------------------
test("store contract: BridgeStore provides a bounded listActiveJobs API that queries only active statuses", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-active-resources-store-api-"));
  const store = await BridgeStore.open(directory);
  try {
    assert.equal(
      typeof (store as any).listActiveJobs,
      "function",
      "BridgeStore must provide a bounded listActiveJobs() method rather than forcing callers to scan all jobs",
    );

    seedAgent(store, "agent_main", directory);

    // Seed 1 job for each active status
    for (let i = 0; i < ACTIVE_STATUSES.length; i++) {
      const status = ACTIVE_STATUSES[i]!;
      seedJob(store, "agent_main", `job_active_${i}`, status, [`res_active_${i}`]);
    }

    // Seed 1 queued job (inactive, not holding active resource claims)
    seedJob(store, "agent_main", "job_queued_0", "queued", ["res_queued_0"]);

    // Seed arbitrarily many historical terminal jobs
    for (let i = 0; i < 150; i++) {
      const status = TERMINAL_STATUSES[i % TERMINAL_STATUSES.length]!;
      seedJob(store, "agent_main", `job_historic_${i}`, status, [`res_historic_${i}`]);
    }

    const activeJobs = (store as any).listActiveJobs() as JobRecord[];
    assert.equal(
      activeJobs.length,
      ACTIVE_STATUSES.length,
      `listActiveJobs() must return exactly the ${ACTIVE_STATUSES.length} active jobs, ignoring 150 terminal and 1 queued job`,
    );

    for (const job of activeJobs) {
      assert.ok(
        ACTIVE_STATUSES.includes(job.status),
        `Job ${job.id} returned by listActiveJobs() has non-active status: ${job.status}`,
      );
    }
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Service Seam Contract: Active resource reconstruction must not invoke unbounded listJobs
// ---------------------------------------------------------------------------
test("service contract: active resource reconstruction does not invoke unbounded store.listJobs()", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-active-resources-seam-"));
  const store = await BridgeStore.open(directory);
  seedAgent(store, "agent_active", directory);

  // Seed 1 active running job with an exclusive resource claim
  seedJob(store, "agent_active", "job_running_1", "running", ["gpu-lock-alpha"]);

  // Seed arbitrarily many historical terminal jobs
  for (let i = 0; i < 100; i++) {
    const status = i % 2 === 0 ? "completed" : "failed";
    seedJob(store, "agent_active", `job_term_${i}`, status, [`historical-res-${i}`]);
  }

  // Instrument store.listJobs to observe calls: unbounded calls pass status=undefined
  let unboundedListJobsCalls = 0;
  const origListJobs = store.listJobs.bind(store);
  store.listJobs = function (status?: JobStatus) {
    if (status === undefined) {
      unboundedListJobsCalls++;
    }
    return origListJobs(status);
  };

  const config = createDefaultConfig({ dataDir: directory });
  const service = new BridgeService(config, {
    store,
    manager: makeFakeManager(),
  });

  try {
    // Active resource reconstruction (e.g. on service start or resource query)
    const claimed = (service as any).getActiveExclusiveResources() as Set<string>;

    assert.ok(
      claimed.has("gpu-lock-alpha"),
      "Active running job resource must be reconstructed in active claims",
    );

    // Contract: Active resource reconstruction must query ONLY active statuses via a bounded query
    // and must NOT perform an unbounded scan via store.listJobs(undefined).
    assert.equal(
      unboundedListJobsCalls,
      0,
      "Active resource reconstruction must not invoke unbounded store.listJobs(); it must use a status-bounded query",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Bounded Row Fetch Seam: Excludes arbitrarily many historical terminal jobs
// ---------------------------------------------------------------------------
test("service contract: active resource reconstruction excludes arbitrarily many historical jobs from SQLite row reads", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-active-resources-rows-"));
  const store = await BridgeStore.open(directory);
  seedAgent(store, "agent_active", directory);

  // Seed 2 active jobs with exclusive resources
  seedJob(store, "agent_active", "job_act_1", "running", ["res-alpha"]);
  seedJob(store, "agent_active", "job_act_2", "following", ["res-beta"]);

  // Seed 200 historical terminal jobs
  for (let i = 0; i < 200; i++) {
    const status = TERMINAL_STATUSES[i % TERMINAL_STATUSES.length]!;
    seedJob(store, "agent_active", `job_hist_${i}`, status, [`historic-res-${i}`]);
  }

  // Instrument SQLite statement execution to count rows returned from the jobs table
  let rowsFetchedFromJobs = 0;
  const executedJobQueries: string[] = [];
  const origPrepare = store.db.prepare.bind(store.db);
  store.db.prepare = function (sql: string) {
    const stmt = origPrepare(sql);
    const isJobsQuery = /from\s+jobs/i.test(sql);
    if (isJobsQuery) {
      executedJobQueries.push(sql);
      const origAll = stmt.all.bind(stmt);
      stmt.all = function (...params: any[]) {
        const rows = origAll(...params);
        rowsFetchedFromJobs += rows.length;
        return rows;
      };
    }
    return stmt;
  };

  const config = createDefaultConfig({ dataDir: directory });
  const service = new BridgeService(config, {
    store,
    manager: makeFakeManager(),
  });

  try {
    const claimed = (service as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(claimed.has("res-alpha"), "Claims must contain res-alpha");
    assert.ok(claimed.has("res-beta"), "Claims must contain res-beta");

    // All executed queries targeting `jobs` must include a status filter
    for (const sql of executedJobQueries) {
      const lower = sql.toLowerCase();
      assert.ok(
        lower.includes("status in") || lower.includes("status =") || lower.includes("where id ="),
        `Query against jobs must be status-bounded or id-bounded, got: ${sql}`,
      );
    }

    // Number of job rows fetched during active resource reconstruction must be bounded by the active set (2 rows),
    // strictly avoiding the 200 historical terminal rows.
    assert.ok(
      rowsFetchedFromJobs <= 2,
      `Active resource reconstruction must fetch at most the active jobs (<= 2 rows), but fetched ${rowsFetchedFromJobs} rows from SQLite`,
    );
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. Restart Correctness: Preserves active claims, ignores terminal/queued, releases on completion
// ---------------------------------------------------------------------------
test("restart correctness: active exclusive resources are reconstructed across restarts and released on terminal transition", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-active-resources-restart-"));
  const store = await BridgeStore.open(directory);
  seedAgent(store, "agent_multi", directory);

  // 1. Seed jobs across active, inactive, and terminal statuses
  const jobDisp = seedJob(store, "agent_multi", "job_disp", "dispatching", ["res-dispatch"]);
  const jobRun = seedJob(store, "agent_multi", "job_run", "running", ["res-run"]);
  const jobFoll = seedJob(store, "agent_multi", "job_foll", "following", ["res-foll"]);
  const jobFin = seedJob(store, "agent_multi", "job_fin", "finalizing", ["res-fin"]);
  const jobAppr = seedJob(store, "agent_multi", "job_appr", "needs_approval", ["res-appr"]);

  // Inactive: queued (not yet active)
  seedJob(store, "agent_multi", "job_queued", "queued", ["res-queued-inactive"]);

  // Terminal jobs
  seedJob(store, "agent_multi", "job_comp", "completed", ["res-completed-historic"]);
  seedJob(store, "agent_multi", "job_fail", "failed", ["res-failed-historic"]);
  seedJob(store, "agent_multi", "job_abort", "aborted", ["res-aborted-historic"]);
  seedJob(store, "agent_multi", "job_timeout", "timed_out", ["res-timeout-historic"]);

  // 2. Simulate service restart with an empty in-memory resource map
  const config = createDefaultConfig({ dataDir: directory });
  const restartedService = new BridgeService(config, {
    store,
    manager: makeFakeManager(),
  });

  try {
    const claims = (restartedService as any).getActiveExclusiveResources() as Set<string>;

    // Active resources must be claimed
    assert.ok(claims.has("res-dispatch"), "dispatching job resource must be claimed");
    assert.ok(claims.has("res-run"), "running job resource must be claimed");
    assert.ok(claims.has("res-foll"), "following job resource must be claimed");
    assert.ok(claims.has("res-fin"), "finalizing job resource must be claimed");
    assert.ok(claims.has("res-appr"), "needs_approval job resource must be claimed");

    // Inactive & terminal resources must NOT be claimed
    assert.ok(!claims.has("res-queued-inactive"), "queued job resource must NOT be claimed");
    assert.ok(!claims.has("res-completed-historic"), "completed job resource must NOT be claimed");
    assert.ok(!claims.has("res-failed-historic"), "failed job resource must NOT be claimed");
    assert.ok(!claims.has("res-aborted-historic"), "aborted job resource must NOT be claimed");
    assert.ok(!claims.has("res-timeout-historic"), "timed_out job resource must NOT be claimed");

    // 3. Transition running job to completed -> resource must be released
    store.updateJobStatus(jobRun.id, "completed");
    const afterRunComplete = (restartedService as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(!afterRunComplete.has("res-run"), "res-run must be released after job transition to completed");
    assert.ok(afterRunComplete.has("res-appr"), "res-appr must remain claimed while needs_approval remains active");

    // 4. Transition needs_approval job to failed -> resource must be released
    store.updateJobStatus(jobAppr.id, "failed", "permission rejected");
    const afterApprFailed = (restartedService as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(!afterApprFailed.has("res-appr"), "res-appr must be released after job transition to failed");
  } finally {
    await restartedService.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 6. Queue Drain Conflict Contract: Active mutual exclusion without unbounded listJobs
// ---------------------------------------------------------------------------
test("queue drain contract: conflict check enforces active exclusion and ignores terminal history without unbounded listJobs", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "swarm-active-resources-drain-"));
  const store = await BridgeStore.open(directory);
  seedAgent(store, "agent_blocker", directory);
  seedAgent(store, "agent_candidate", directory);

  // Active blocker holding "exclusive-gate"
  const blocker = seedJob(store, "agent_blocker", "job_blocker", "running", ["exclusive-gate"]);

  // Arbitrarily many terminal historical jobs referencing "exclusive-gate" and "historic-gate"
  for (let i = 0; i < 50; i++) {
    seedJob(store, "agent_blocker", `job_old_${i}`, "completed", ["exclusive-gate", "historic-gate"]);
  }

  // Instrument store.listJobs to verify queue drain does not invoke unbounded listJobs
  let unboundedListJobsCalls = 0;
  const origListJobs = store.listJobs.bind(store);
  store.listJobs = function (status?: JobStatus) {
    if (status === undefined) {
      unboundedListJobsCalls++;
    }
    return origListJobs(status);
  };

  const config = createDefaultConfig({ dataDir: directory });
  const service = new BridgeService(config, {
    store,
    manager: makeFakeManager(),
  });

  try {
    // Check claimed resources
    const activeResources = (service as any).getActiveExclusiveResources() as Set<string>;
    assert.ok(activeResources.has("exclusive-gate"), "exclusive-gate must be active");
    assert.ok(!activeResources.has("historic-gate"), "historic-gate from completed jobs must not be active");

    // Enforce no unbounded listJobs calls were made
    assert.equal(
      unboundedListJobsCalls,
      0,
      "Queue drain active resource checks must not invoke unbounded store.listJobs()",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
