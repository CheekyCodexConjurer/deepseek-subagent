import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";
import type { JobRecord } from "../../src/types.js";
import { createLegacyPruneIndexes, evaluateRetentionPolicy, runRetentionPrune, RETAIN_ACTIVITY_FLOOR_PER_AGENT } from "../../src/retention.js";

const OLD = new Date(Date.now() - 60 * 24 * 60 * 60_000).toISOString();

function createAgent(store: BridgeStore, id: string): void {
  store.createAgent({
    id,
    title: id,
    topic: id,
    repositoryRoot: "C:\\deepseek-retention",
    workspacePath: "C:\\deepseek-retention",
    workspaceStrategy: "shared",
    opencodeServerId: "server_" + id,
    opencodeSessionId: "session_" + id,
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    modelRoute: "flash-max",
  });
}

function createSettledJob(store: BridgeStore, id: string): JobRecord {
  createAgent(store, "agent_" + id);
  const job = store.createJob({ id, agentId: "agent_" + id, kind: "spawn", requestId: "request_" + id, promptHash: "hash" });
  store.updateJobStatus(job.id, "dispatching");
  store.updateJobStatus(job.id, "running");
  store.updateJobStatus(job.id, "completed");
  store.updateJobStatus(job.id, "delivery_pending");
  store.updateJobStatus(job.id, "delivered");
  store.setJobResult(job.id, "C:\\deepseek-retention\\results\\" + id + ".json", id);
  return job;
}

function createActiveJob(store: BridgeStore, id: string): JobRecord {
  createAgent(store, "agent_" + id);
  const job = store.createJob({ id, agentId: "agent_" + id, kind: "spawn", requestId: "request_" + id, promptHash: "hash" });
  store.updateJobStatus(job.id, "dispatching");
  return job;
}

function seedRows(store: BridgeStore, jobId: string, activityCount: number, eventCount: number, fresh: boolean): void {
  const agentId = "agent_" + jobId;
  for (let index = 0; index < activityCount; index += 1) {
    store.recordActivity({ agentId, jobId, sessionId: "session_" + jobId, activityType: "event", summary: "activity " + index, metadata: {} });
  }
  for (let index = 0; index < eventCount; index += 1) {
    store.insertEvent({ source: "opencode", sourceEventId: jobId + "_" + index, eventType: "session.idle", sessionId: "session_" + jobId, jobId });
  }
  if (!fresh) {
    store.db.prepare("UPDATE agent_activity SET created_at = ? WHERE job_id = ?").run(OLD, jobId);
    store.db.prepare("UPDATE events SET received_at = ? WHERE job_id = ?").run(OLD, jobId);
  }
}

function countRows(store: BridgeStore, table: "events" | "agent_activity"): number {
  const row = store.db.prepare("SELECT COUNT(*) AS count FROM " + table).get() as { count: number | bigint };
  return Number(row.count);
}

function countRowsForJob(store: BridgeStore, table: "events" | "agent_activity", jobId: string): number {
  const row = store.db.prepare("SELECT COUNT(*) AS count FROM " + table + " WHERE job_id = ?").get(jobId) as { count: number | bigint };
  return Number(row.count);
}

test("auto enables pruning only on a provably empty database", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-retention-auto-"));
  const store = await BridgeStore.open(directory);
  try {
    assert.equal(evaluateRetentionPolicy(store, "auto").pruningEnabled, true);
    assert.equal(evaluateRetentionPolicy(store, "auto").dbState, "empty");
    createSettledJob(store, "job_auto_check");
    const policy = evaluateRetentionPolicy(store, "auto");
    assert.equal(policy.pruningEnabled, false, "non-empty legacy database must stay disabled in auto mode");
    assert.equal(policy.dbState, "legacy");
    assert.equal(evaluateRetentionPolicy(store, "disabled").pruningEnabled, false);
    assert.equal(evaluateRetentionPolicy(store, "enabled").pruningEnabled, false, "enabled alone must not arm pruning on a legacy database");
    assert.match(evaluateRetentionPolicy(store, "enabled").reason ?? "", /offline preparation/);
    store.markRetentionPrepared();
    assert.equal(evaluateRetentionPolicy(store, "enabled").pruningEnabled, true, "the offline preparation marker arms enabled pruning");
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("enabled pruning on an empty database needs no preparation marker", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-retention-enabled-empty-"));
  const store = await BridgeStore.open(directory);
  try {
    assert.equal(evaluateRetentionPolicy(store, "enabled").pruningEnabled, true);
    assert.equal(evaluateRetentionPolicy(store, "enabled").dbState, "empty");
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("prune removes only old events and activity of settled consumed jobs and never touches protected rows", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-retention-prune-"));
  const store = await BridgeStore.open(directory);
  try {
    createSettledJob(store, "job_old_settled");
    seedRows(store, "job_old_settled", 60, 5, false);
    store.consumeResult("job_old_settled");

    createSettledJob(store, "job_unconsumed");
    seedRows(store, "job_unconsumed", 4, 3, false);

    createActiveJob(store, "job_active");
    seedRows(store, "job_active", 4, 3, false);

    createSettledJob(store, "job_fresh");
    seedRows(store, "job_fresh", 2, 2, true);
    store.consumeResult("job_fresh");

    const agentsBefore = store.listAgents().length;
    const jobsBefore = store.listJobs().length;
    const result = runRetentionPrune(store, { now: Date.now(), budgetMs: 60_000 });

    assert.equal(result.prunedEvents, 5, "old events of the settled consumed job must be pruned");
    assert.equal(result.prunedActivity, 10, "old activity above the per-agent floor must be pruned");
    assert.equal(store.listAgents().length, agentsBefore, "agents are never pruned");
    assert.equal(store.listJobs().length, jobsBefore, "jobs are never pruned");
    assert.ok(store.getJob("job_unconsumed"), "unconsumed terminal results keep their job rows");
    assert.equal(store.getJob("job_active")?.status, "dispatching", "active job rows are untouched");
    assert.equal(countRowsForJob(store, "events", "job_old_settled"), 0);
    assert.equal(countRowsForJob(store, "agent_activity", "job_old_settled"), RETAIN_ACTIVITY_FLOOR_PER_AGENT, "the newest activity rows per agent survive as the floor");
    assert.equal(countRowsForJob(store, "events", "job_unconsumed"), 3, "unconsumed result protects its events");
    assert.equal(countRowsForJob(store, "agent_activity", "job_unconsumed"), 4, "unconsumed result protects its activity");
    assert.equal(countRowsForJob(store, "events", "job_active"), 3, "active job protects its events");
    assert.equal(countRowsForJob(store, "agent_activity", "job_active"), 4, "active job protects its activity");
    assert.equal(countRowsForJob(store, "events", "job_fresh"), 2, "fresh events are protected");
    assert.equal(countRowsForJob(store, "agent_activity", "job_fresh"), 2, "fresh activity is protected");
    assert.match(result.checkpoint, /^busy=/, "a real pass runs a PASSIVE wal checkpoint");
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("dry-run deletes nothing and reports the same eligibility counts as a real pass", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-retention-dryrun-"));
  const store = await BridgeStore.open(directory);
  try {
    createSettledJob(store, "job_dryrun");
    seedRows(store, "job_dryrun", 60, 6, false);
    store.consumeResult("job_dryrun");
    const dryRun = runRetentionPrune(store, { dryRun: true, now: Date.now(), budgetMs: 60_000 });
    assert.equal(dryRun.prunedEvents, 6);
    assert.equal(dryRun.prunedActivity, 10);
    assert.equal(dryRun.checkpoint, "none", "dry-run must never checkpoint or delete");
    const before = { events: countRows(store, "events"), activity: countRows(store, "agent_activity") };
    const real = runRetentionPrune(store, { now: Date.now(), budgetMs: 60_000 });
    assert.equal(real.prunedEvents, dryRun.prunedEvents, "dry-run counts must match the real pass");
    assert.equal(real.prunedActivity, dryRun.prunedActivity);
    const after = { events: countRows(store, "events"), activity: countRows(store, "agent_activity") };
    assert.equal(before.events - after.events, 6);
    assert.equal(before.activity - after.activity, 10);
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("dry-run and real passes report accurate protected counters, never a false zero", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-retention-protected-count-"));
  const store = await BridgeStore.open(directory);
  try {
    // Settled + consumed job with old rows: fully eligible.
    createSettledJob(store, "job_eligible");
    seedRows(store, "job_eligible", 60, 4, false);
    store.consumeResult("job_eligible");
    // Unconsumed terminal result with old rows: fully protected.
    createSettledJob(store, "job_protected");
    seedRows(store, "job_protected", 3, 2, false);

    const dryRun = runRetentionPrune(store, { dryRun: true, now: Date.now(), budgetMs: 60_000 });
    assert.equal(dryRun.prunedEvents, 4, "only the eligible job's events would be pruned");
    assert.equal(dryRun.prunedActivity, 10, "eligible activity above the per-agent floor would be pruned");
    assert.equal(dryRun.protectedEvents, 2, "the protected job's events must be counted as protected in dry-run");
    assert.equal(dryRun.protectedActivity, 53, "protected activity includes the per-agent floor rows and the protected job's rows");

    const real = runRetentionPrune(store, { now: Date.now(), budgetMs: 60_000 });
    assert.equal(real.prunedEvents, 4);
    assert.equal(real.prunedActivity, 10);
    assert.equal(real.protectedEvents, 2, "protected counts stay stable after the real pass");
    assert.equal(real.protectedActivity, 53);
    assert.equal(countRowsForJob(store, "events", "job_protected"), 2, "protected rows survive");
    assert.equal(countRowsForJob(store, "agent_activity", "job_protected"), 3);
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("pruning is idempotent: a second pass deletes nothing", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-retention-idempotent-"));
  const store = await BridgeStore.open(directory);
  try {
    createSettledJob(store, "job_idem");
    seedRows(store, "job_idem", 60, 6, false);
    store.consumeResult("job_idem");
    runRetentionPrune(store, { now: Date.now(), budgetMs: 60_000 });
    const second = runRetentionPrune(store, { now: Date.now(), budgetMs: 60_000 });
    assert.equal(second.prunedEvents, 0);
    assert.equal(second.prunedActivity, 0);
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("legacy prune indexes are created only by the explicit offline helper", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-retention-indexes-"));
  const store = await BridgeStore.open(directory);
  try {
    createSettledJob(store, "job_index_check");
    const countIndexes = (): number => Number((store.db.prepare(
      "SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'index' AND name IN ('idx_events_received_at','idx_agent_activity_created_at')",
    ).get() as { count: number }).count);
    const before = countIndexes();
    createLegacyPruneIndexes(store);
    const after = countIndexes();
    assert.ok(after >= before);
    assert.ok(after >= 1);
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
