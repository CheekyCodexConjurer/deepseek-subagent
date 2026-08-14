import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";

test("persists agents, jobs, bindings and idempotent events", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-store-"));
  const store = await BridgeStore.open(directory);
  try {
    store.createAgent({
      id: "agent_test",
      title: "Test",
      topic: "Test topic",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "server_test",
      opencodeSessionId: "session_test",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
    });
    const job = store.createJob({
      id: "job_test",
      agentId: "agent_test",
      kind: "spawn",
      requestId: "request_test",
      promptHash: "hash",
    });
    store.bindJob({
      jobId: job.id,
      threadId: "thread_test",
      originatingTurnId: "turn_test",
      originatingItemId: null,
    });
    assert.doesNotThrow(() => store.bindJob({
      jobId: job.id,
      threadId: "thread_test",
      originatingTurnId: "turn_test",
      originatingItemId: null,
    }));
    assert.throws(() => store.bindJob({
      jobId: job.id,
      threadId: "other_thread",
      originatingTurnId: "turn_test",
      originatingItemId: null,
    }), /Conflicting Codex binding/);
    assert.equal(store.getLatestBindingForAgent("agent_test")?.threadId, "thread_test");
    assert.equal(store.insertEvent({
      source: "opencode",
      sourceEventId: "event_test",
      eventType: "session.idle",
      sessionId: "session_test",
      jobId: job.id,
    }), true);
    assert.equal(store.insertEvent({
      source: "opencode",
      sourceEventId: "event_test",
      eventType: "session.idle",
      sessionId: "session_test",
      jobId: job.id,
    }), false);
    store.recordActivity({
      agentId: "agent_test",
      jobId: job.id,
      sessionId: "session_test",
      activityType: "event",
      summary: "Read src/example.ts",
      metadata: { privateReasoning: "must never be returned" },
    });
    const activities = store.listActivity("agent_test", 1);
    assert.equal(activities.length, 1);
    assert.equal(activities[0]?.summary, "Read src/example.ts");
    assert.doesNotMatch(JSON.stringify(activities[0]), /privateReasoning|reasoning/);
    store.recordActivity({
      agentId: "agent_test",
      jobId: job.id,
      sessionId: "session_test",
      activityType: "approval",
      summary: "Approval requested",
    });
    for (let index = 0; index < 25; index += 1) {
      store.recordActivity({
        agentId: "agent_test",
        jobId: job.id,
        sessionId: "session_test",
        activityType: "event",
        summary: "Event " + index,
      });
    }
    assert.equal(store.hasActivity({ agentId: "agent_test", jobId: job.id, activityType: "approval" }), true);
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("default integrity check performs a fast access/schema check without quick_check", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-store-health-"));
  const store = await BridgeStore.open(directory);
  try {
    const prepare = store.db.prepare;
    const statements: string[] = [];
    store.db.prepare = ((sql: string) => {
      statements.push(sql);
      return prepare.call(store.db, sql);
    }) as typeof prepare;
    assert.equal(store.integrityCheck(), "ok");
    assert.ok(statements.some((sql) => sql.includes("schema_migrations")), "health check must run fast schema/access check");
    assert.ok(!statements.includes("PRAGMA quick_check"), "default check must not run PRAGMA quick_check");
    assert.ok(!statements.includes("PRAGMA integrity_check"), "default check must not run PRAGMA integrity_check");
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("full integrity check runs PRAGMA quick_check", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-store-health-full-"));
  const store = await BridgeStore.open(directory);
  try {
    const prepare = store.db.prepare;
    const statements: string[] = [];
    store.db.prepare = ((sql: string) => {
      statements.push(sql);
      return prepare.call(store.db, sql);
    }) as typeof prepare;
    assert.equal(store.integrityCheck({ full: true }), "ok");
    assert.ok(statements.includes("PRAGMA quick_check"), "full check must run PRAGMA quick_check");
    assert.ok(!statements.includes("PRAGMA integrity_check"), "full check must not run a full integrity scan");
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("active route pointer is additive state: absent by default, persisted through reopen", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-store-route-"));
  const dbPath = path.join(directory, "bridge.sqlite");
  try {
    const first = new BridgeStore(dbPath);
    assert.equal(first.getActiveRoute(), null, "no explicit pointer means the configured default applies");
    first.setActiveRoute("antigravity-flash-high");
    assert.equal(first.getActiveRoute(), "antigravity-flash-high");
    first.close();
    const second = new BridgeStore(dbPath);
    assert.equal(second.getActiveRoute(), "antigravity-flash-high", "pointer survives reopen without touching config.json");
    second.setActiveRoute("flash-max");
    assert.equal(second.getActiveRoute(), "flash-max", "the pointer is mutable");
    second.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("rejects a duplicate Codex correlation after reopening the store", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-store-correlation-"));
  const store = await BridgeStore.open(directory);
  let storeClosed = false;
  try {
    store.createAgent({
      id: "agent_correlation",
      title: "Correlation",
      topic: "Correlation topic",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "server_correlation",
      opencodeSessionId: "session_correlation",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
    });
    const first = store.createJob({
      id: "job_correlation_first",
      agentId: "agent_correlation",
      kind: "spawn",
      requestId: "request_correlation_first",
      promptHash: "hash_first",
    });
    const second = store.createJob({
      id: "job_correlation_second",
      agentId: "agent_correlation",
      kind: "continue",
      requestId: "request_correlation_second",
      promptHash: "hash_second",
    });
    store.bindJob({
      jobId: first.id,
      threadId: "thread_persisted",
      originatingTurnId: "turn_persisted",
      originatingItemId: "item_persisted",
    });
    store.close();
    storeClosed = true;

    const reopened = await BridgeStore.open(directory);
    try {
      assert.throws(() => reopened.bindJob({
        jobId: second.id,
        threadId: "thread_persisted",
        originatingTurnId: "turn_persisted",
        originatingItemId: "item_persisted",
      }), /already bound to job job_correlation_first/);
    } finally {
      reopened.close();
    }
  } finally {
    if (!storeClosed) store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("persists correlation hints with provenance, counters, and migration columns", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-store-hints-"));
  const store = await BridgeStore.open(directory);
  let storeClosed = false;
  try {
    store.createAgent({
      id: "agent_hints",
      title: "Hints",
      topic: "Hints topic",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "server_hints",
      opencodeSessionId: "session_hints",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
    });
    const job = store.createJob({
      id: "job_hints",
      agentId: "agent_hints",
      kind: "spawn",
      requestId: "request_hints",
      promptHash: "hash_hints",
    });
    const plain = store.createJob({
      id: "job_hints_plain",
      agentId: "agent_hints",
      kind: "continue",
      requestId: "request_hints_plain",
      promptHash: "hash_hints_plain",
    });
    store.setCorrelationHint(job.id, { threadId: "thread_hint", turnId: "turn_hint", source: "mcp" });
    const hinted = store.getJob(job.id);
    assert.equal(hinted?.hintThreadId, "thread_hint");
    assert.equal(hinted?.hintTurnId, "turn_hint");
    assert.equal(hinted?.hintSource, "mcp");
    store.setCorrelationHint(job.id, { turnId: "turn_hint_two", source: "mcp" });
    assert.equal(store.getJob(job.id)?.hintThreadId, "thread_hint", "hints are additive");
    assert.equal(store.getJob(job.id)?.hintTurnId, "turn_hint_two");
    assert.equal(store.getJob(job.id)?.hintSource, "mcp");
    assert.equal(store.getJob(plain.id)?.hintSource, null, "no hint is synthesized when absent");
    assert.equal(store.countJobsWithCorrelationHints(), 1);
    assert.equal(store.countCodexBindings(), 0);
    store.setJobError(job.id, "Dispatch outcome unknown after a transport failure: timeout");
    assert.equal(store.getJob(job.id)?.error, "Dispatch outcome unknown after a transport failure: timeout");
    store.bindJob({ jobId: job.id, threadId: "thread_authoritative", originatingTurnId: null, originatingItemId: null });
    assert.equal(store.countCodexBindings(), 1);
    assert.equal(store.getBinding(job.id)?.threadId, "thread_authoritative");
    store.close();
    storeClosed = true;

    const reopened = await BridgeStore.open(directory);
    try {
      const marker = reopened.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 5").get();
      assert.ok(marker, "migration version 5 must be recorded");
      const migrated = reopened.getJob(job.id);
      assert.equal(migrated?.hintThreadId, "thread_hint");
      assert.equal(migrated?.hintSource, "mcp");
      assert.equal(reopened.countJobsWithCorrelationHints(), 1);
      assert.equal(reopened.countCodexBindings(), 1);
    } finally {
      reopened.close();
    }
  } finally {
    if (!storeClosed) store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("lineage migration adds parent_agent_id and round-trips createAgent", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-store-lineage-"));
  const store = await BridgeStore.open(directory);
  let storeClosed = false;
  try {
    const parent = store.createAgent({
      id: "agent_lineage_parent",
      title: "Lineage Parent",
      topic: "Lineage topic",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "server_lineage",
      opencodeSessionId: "session_lineage_parent",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
      modelRoute: "flash-max",
    });
    const child = store.createAgent({
      id: "agent_lineage_child",
      title: "Lineage Child",
      topic: "Lineage topic",
      repositoryRoot: directory,
      workspacePath: directory,
      workspaceStrategy: "shared",
      opencodeServerId: "server_lineage",
      opencodeSessionId: "session_lineage_child",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
      modelRoute: "flash-max",
      parentAgentId: "agent_lineage_parent",
    });
    assert.equal(parent.parentAgentId, null, "a root agent has no lineage");
    assert.equal(child.parentAgentId, "agent_lineage_parent", "the child records its lineage");
    const columns = store.db.prepare("PRAGMA table_info(agents)").all() as Array<{ name: string }>;
    assert.ok(columns.some((column) => column.name === "parent_agent_id"), "the column exists");
    store.close();
    storeClosed = true;

    const reopened = await BridgeStore.open(directory);
    try {
      const marker = reopened.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 9").get();
      assert.ok(marker, "migration version 9 must be recorded");
      assert.equal(reopened.getAgent("agent_lineage_child")?.parentAgentId, "agent_lineage_parent");
      assert.equal(reopened.getAgent("agent_lineage_parent")?.parentAgentId, null);
    } finally {
      reopened.close();
    }
  } finally {
    if (!storeClosed) store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
