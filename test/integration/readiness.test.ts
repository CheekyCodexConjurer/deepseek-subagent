import test from "node:test";
import assert from "node:assert/strict";
import { createServer, type AddressInfo } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeHttpClient, BridgeHttpError, BridgeHttpServer } from "../../src/http-server.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";

async function freePort(): Promise<number> {
  const probe = createServer();
  return new Promise<number>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      const port = typeof address === "object" && address ? (address as AddressInfo).port : 0;
      probe.close((error) => (error ? reject(error) : resolve(port)));
    });
  });
}

test("Phase 1: daemon binds socket before recovery, /health reports lifecycle state, and non-health tool endpoints return 503 while starting", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-phase1-readiness-"));
  const store = await BridgeStore.open(directory);
  const port = await freePort();
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: port,
    daemonToken: "phase1-token",
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    retentionMode: "auto",
  });

  const service = new BridgeService(config, {
    store,
  });
  // Delayed recovery simulates slow recovery
  const originalRecover = (service as any).recoverPendingJobs.bind(service);
  (service as any).recoverPendingJobs = async () => {
    await new Promise((resolve) => setTimeout(resolve, 150));
    return originalRecover();
  };
  const http = new BridgeHttpServer(config, service);

  // 1. Start HTTP server first
  await http.start();

  try {
    // 2. Start service in background
    const startPromise = service.start();

    // 3. Immediately while starting/recovering: socket is listening and /health returns 200 with state="starting" or "recovering", ready=false
    const healthRes = await fetch(`http://${config.daemonHost}:${config.daemonPort}/health`);
    assert.equal(healthRes.status, 200);
    const healthBody = await healthRes.json() as Record<string, unknown>;
    assert.equal(healthBody.displayName, "DeepSeek Sub-Agent");
    assert.ok(healthBody.state === "starting" || healthBody.state === "recovering", `expected starting or recovering, got ${healthBody.state}`);
    assert.equal(healthBody.ready, false);
    assert.equal((healthBody.status as Record<string, unknown>)?.running, false);

    // 4. Non-health tool endpoints return 503 retryable while not ready
    const spawnRes = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/spawn`, {
      method: "POST",
      headers: {
        authorization: "Bearer " + config.daemonToken,
        "content-type": "application/json",
      },
      body: JSON.stringify({ topic: "topic", task: "task" }),
    });
    assert.equal(spawnRes.status, 503);
    const spawnBody = await spawnRes.json() as Record<string, unknown>;
    assert.equal(spawnBody.code, "service_unavailable");
    assert.equal(spawnBody.status, 503);
    assert.equal(spawnBody.retry, true);
    assert.equal(spawnBody.ready, false);

    // 5. Await service readiness
    await startPromise;

    // 6. After readiness: /health returns state="ready", ready=true, status.running=true
    const readyHealthRes = await fetch(`http://${config.daemonHost}:${config.daemonPort}/health`);
    assert.equal(readyHealthRes.status, 200);
    const readyHealthBody = await readyHealthRes.json() as Record<string, unknown>;
    assert.equal(readyHealthBody.state, "ready");
    assert.equal(readyHealthBody.ready, true);
    assert.equal((readyHealthBody.status as Record<string, unknown>)?.running, true);

    // 7. Non-health tool endpoints now succeed / pass 503 gate
    const httpClient = new BridgeHttpClient(config);
    const spawnAccepted = await httpClient.call<Record<string, unknown>>("/v1/jobs/spawn", {
      topic: "topic",
      task: "task",
    });
    assert.equal(spawnAccepted.accepted, true);
  } finally {
    await http.stop();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Phase 1: service startup failure leaves HTTP server in degraded state without crash", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-phase1-degraded-"));
  const store = await BridgeStore.open(directory);
  const port = await freePort();
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: port,
    daemonToken: "phase1-token",
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
  });

  const service = new BridgeService(config, {
    store,
  });
  // Startup recovery failure puts daemon into degraded state
  (service as any).recoverPendingJobs = async () => {
    throw new Error("Antigravity recovery failed (simulated error)");
  };
  const http = new BridgeHttpServer(config, service);

  await http.start();
  try {
    // service.start() should not crash the process or throw unhandled error
    await service.start();

    // /health returns 200 with state="degraded", ready=false, and redacted error
    const healthRes = await fetch(`http://${config.daemonHost}:${config.daemonPort}/health`);
    assert.equal(healthRes.status, 200);
    const healthBody = await healthRes.json() as Record<string, unknown>;
    assert.equal(healthBody.state, "degraded");
    assert.equal(healthBody.ready, false);
    assert.match(String(healthBody.error), /Antigravity recovery failed/);

    // Non-health endpoints return 503 with retry=false for degraded state
    const spawnRes = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/spawn`, {
      method: "POST",
      headers: {
        authorization: "Bearer " + config.daemonToken,
        "content-type": "application/json",
      },
      body: JSON.stringify({ topic: "topic", task: "task" }),
    });
    assert.equal(spawnRes.status, 503);
    const spawnBody = await spawnRes.json() as Record<string, unknown>;
    assert.equal(spawnBody.code, "service_unavailable");
    assert.equal(spawnBody.retry, false);
  } finally {
    await http.stop();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Phase 1: retention policy scheduling does not execute synchronous pruning during startup microtask or block /health", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-phase1-retention-readiness-"));
  const store = await BridgeStore.open(directory);
  const port = await freePort();

  // Populate store with an agent, consumed job, and old events eligible for pruning
  const old = new Date(Date.now() - 60 * 24 * 60 * 60_000).toISOString();
  store.createAgent({
    id: "agent_retention_readiness",
    title: "Retention Readiness",
    topic: "Readiness topic",
    repositoryRoot: directory,
    workspacePath: directory,
    workspaceStrategy: "shared",
    opencodeServerId: "server_readiness",
    opencodeSessionId: "session_readiness",
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    modelRoute: "flash-max",
  });
  const job = store.createJob({
    id: "job_retention_readiness",
    agentId: "agent_retention_readiness",
    kind: "spawn",
    requestId: "request_retention_readiness",
    promptHash: "h",
  });
  store.updateJobStatus(job.id, "dispatching");
  store.updateJobStatus(job.id, "running");
  store.updateJobStatus(job.id, "completed");
  store.updateJobStatus(job.id, "delivery_pending");
  store.updateJobStatus(job.id, "delivered");
  store.setJobResult(job.id, path.join(directory, "results", "job_retention_readiness.json"), "readiness");
  store.consumeResult(job.id);
  for (let index = 0; index < 3; index += 1) {
    store.insertEvent({
      source: "opencode",
      sourceEventId: "readiness_" + index,
      eventType: "session.idle",
      sessionId: "session_readiness",
      jobId: job.id,
    });
  }
  store.db.prepare("UPDATE events SET received_at = ? WHERE job_id = ?").run(old, job.id);
  store.markRetentionPrepared();

  // Track if pruning queries execute during startup
  let pruneQueryExecutedDuringStartup = false;
  const originalPrepare = store.db.prepare.bind(store.db);
  store.db.prepare = ((sql: string, ...rest: unknown[]) => {
    if (typeof sql === "string" && (sql.includes("FROM events WHERE received_at < ?") || sql.includes("DELETE FROM events"))) {
      pruneQueryExecutedDuringStartup = true;
    }
    return (originalPrepare as (...args: unknown[]) => unknown)(sql, ...rest);
  }) as typeof store.db.prepare;

  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: port,
    daemonToken: "retention-readiness-token",
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    retentionMode: "enabled",
  });

  const service = new BridgeService(config, {
    store,
  });
  const http = new BridgeHttpServer(config, service);

  await http.start();
  try {
    // Start service
    await service.start();

    // Invariant: startup reaches ready without running synchronous retention prune queries in startup microtask turn
    assert.equal(
      pruneQueryExecutedDuringStartup,
      false,
      "retention pruning queries must not execute synchronously during service startup microtask",
    );

    // /health is immediately reachable and reports ready=true with pruning enabled in policy
    const healthRes = await fetch(`http://${config.daemonHost}:${config.daemonPort}/health`);
    assert.equal(healthRes.status, 200);
    const healthBody = await healthRes.json() as Record<string, unknown>;
    assert.equal(healthBody.state, "ready");
    assert.equal(healthBody.ready, true);
    const status = healthBody.status as Record<string, unknown>;
    assert.equal(status?.running, true);
    assert.equal((status?.retention as Record<string, unknown>)?.pruningEnabled, true);

    // Events were not pruned during startup: maintenance is deferred to scheduled interval
    const remaining = store.db.prepare("SELECT COUNT(*) AS count FROM events WHERE job_id = ?").get(job.id) as { count: number | bigint };
    assert.equal(Number(remaining.count), 3, "events must remain intact right after startup without immediate pruning pass");
  } finally {
    await http.stop();
    await service.stop();
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
