import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";
import {
  DEFAULT_CODEX_CAPABILITIES,
  type CodexCorrelation,
  type CodexDeliveryAdapter,
} from "../../src/codex/adapter.js";
import type {
  BridgeConfig,
  CodexBinding,
  JobRecord,
  OpenCodeClientLike,
  OpenCodeMessage,
  WakeEnvelope,
} from "../../src/types.js";
import type { CodexCliTransport, CodexCliExecutionResult } from "../../src/codex/cli-resolver.js";

class FakeCodexDelivery implements CodexDeliveryAdapter {
  available = true;
  reason: string | null = null;
  capabilities = { ...DEFAULT_CODEX_CAPABILITIES, authoritativeAttachment: false };
  deliveredWakes: Array<{ envelope: WakeEnvelope; binding: CodexBinding }> = [];

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
    return false;
  }
  onCorrelation(_listener: (correlation: CodexCorrelation) => void): () => void {
    return () => undefined;
  }
}

class FakeCliTransport implements CodexCliTransport {
  calls: Array<{ threadId: string; marker: string }> = [];
  nextResult: CodexCliExecutionResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };
  compatible = true;

  async probeCapabilities(executable?: string): Promise<{ compatible: boolean; version: string | null }> {
    return { compatible: this.compatible, version: "0.150.0" };
  }

  async deliverWake(threadId: string, marker: string): Promise<CodexCliExecutionResult> {
    this.calls.push({ threadId, marker });
    return this.nextResult;
  }
}

class FakeOpenCodeClient implements OpenCodeClientLike {
  messages: OpenCodeMessage[] = [];
  async health() { return { healthy: true }; }
  async createSession() { return { id: "session_fake" }; }
  async promptAsync() {}
  async listMessages() { return this.messages; }
  async getDiff() { return ""; }
  async abort() {}
  async replyPermission() {}
  async subscribe() {}
}

async function createTestEnv() {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "ds-wake-super-"));
  const config = createDefaultConfig({
    dataDir: tmp,
    configPath: path.join(tmp, "config.json"),
    experimentalSameChatDelivery: true,
  });
  const store = new BridgeStore(path.join(tmp, "bridge.sqlite"));
  return { tmp, config, store };
}

function createTestService(config: BridgeConfig, store: BridgeStore, fakeCodex: FakeCodexDelivery, fakeCli: FakeCliTransport) {
  return new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport: fakeCli,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
}

function setupAgent(store: BridgeStore, tmp: string, id: string = "agent_test") {
  return store.createAgent({
    id,
    title: "Test Agent",
    topic: "Testing v3 wake outbox supersession",
    repositoryRoot: tmp,
    workspacePath: tmp,
    workspaceStrategy: "shared",
    opencodeServerId: "srv",
    opencodeSessionId: `session_${id}`,
    modelProviderId: "deepseek",
    modelId: "deepseek-chat",
    modelVariant: null,
  });
}

function makeJobCompleted(store: BridgeStore, jobId: string, resultPath?: string, resultSummary?: string) {
  store.updateJobStatus(jobId, "dispatching");
  store.updateJobStatus(jobId, "running");
  if (resultPath) {
    store.setJobResult(jobId, resultPath, resultSummary ?? "completed result");
  }
  store.updateJobStatus(jobId, "completed");
}

// ---------------------------------------------------------------------------
// 1. New barrier generation supersedes prior pending/deferred outbox
// ---------------------------------------------------------------------------
test("WAKE-SUPER-1: new barrier generation supersedes prior pending/deferred outbox", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_super_1");
    const j1 = store.createJob({ id: "job_s1", agentId: agent.id, kind: "spawn", requestId: "rs1", promptHash: "hs1" });
    const j2 = store.createJob({ id: "job_s2", agentId: agent.id, kind: "spawn", requestId: "rs2", promptHash: "hs2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_super_1", originatingTurnId: "ts1", originatingItemId: "is1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_super_1", originatingTurnId: "ts1", originatingItemId: "is2" });

    // Step 1: Park generation 1
    const receipt1 = await service.park({ job_ids: [j1.id] });
    assert.equal(receipt1.generation, 1);
    assert.equal(receipt1.armed, true);

    // Job 1 completes, CLI encounters active writer conflict so outbox enters deferred_active_writer
    fakeCli.nextResult = { success: false, activeWriter: true, error: "Active writer conflict" };
    const r1 = path.join(tmp, "r1_super.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "r1 done" } }));
    makeJobCompleted(store, j1.id, r1, "r1 done");
    await service.evaluateParkWakes(j1.id);

    const outboxGen1 = store.getWakeOutbox(receipt1.parkId, 1)!;
    assert.ok(outboxGen1, "Outbox row for generation 1 must exist");
    assert.equal(outboxGen1.status, "deferred_active_writer");
    assert.equal(fakeCli.calls.length, 1, "Generation 1 must attempt CLI delivery exactly once before deferral");
    assert.equal(fakeCodex.deliveredWakes.length, 0, "Codex delivery adapter must not be called");

    // Step 2: New barrier generation is parked on the same thread/park
    const receipt2 = await service.park({ job_ids: [j2.id], park_id: receipt1.parkId });
    assert.equal(receipt2.generation, 2);
    assert.equal(receipt2.armed, true);
    assert.equal(fakeCli.calls.length, 1, "Parking new generation must not trigger CLI dispatch");

    // Prior generation 1 outbox must be superseded
    const refreshedGen1 = store.getWakeOutbox(receipt1.parkId, 1)!;
    assert.ok(refreshedGen1, "Prior generation outbox must exist");
    assert.equal(
      (refreshedGen1 as any).status,
      "superseded",
      "Prior generation outbox must be marked as superseded when new barrier generation is parked",
    );

    // Prior outbox must be excluded from pending wake outbox list
    const pending = store.listPendingWakeOutbox();
    const pendingIds = pending.map((p) => p.id);
    assert.equal(
      pendingIds.includes(refreshedGen1.id),
      false,
      "Superseded generation 1 outbox must not appear in listPendingWakeOutbox",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Queued acceptance wins CAS exactly once
// ---------------------------------------------------------------------------
test("WAKE-SUPER-2: queued acceptance wins CAS exactly once", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_cas_queued");
    const j1 = store.createJob({ id: "job_cq1", agentId: agent.id, kind: "spawn", requestId: "rcq1", promptHash: "hcq1" });
    store.bindJob({ jobId: j1.id, threadId: "thread_cq", originatingTurnId: "tcq", originatingItemId: "icq" });

    const receipt = await service.park({ job_ids: [j1.id] });

    // Active writer conflict occurs: legacy activeWriter may still defer if queue unavailable
    fakeCli.nextResult = { success: false, activeWriter: true, error: "Active writer conflict" };
    const r1 = path.join(tmp, "rcq1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "cq1 done" } }));
    makeJobCompleted(store, j1.id, r1, "cq1 done");
    await service.evaluateParkWakes(j1.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox, "Outbox row must exist");
    assert.equal(
      outbox.status,
      "deferred_active_writer",
      "Legacy active writer defers when queue is unavailable",
    );

    // Update outbox with deliveryMode: 'queued' and optional messageId
    store.updateWakeOutboxStatus(outbox.id, "deferred_active_writer", null, {
      deliveryMode: "queued",
      messageId: "msg_cq_cas",
    });
    const queuedOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(queuedOutbox.deliveryMode, "queued");
    assert.equal(queuedOutbox.messageId, "msg_cq_cas");

    // CLI now reports queued turn acceptance
    fakeCli.calls = [];
    fakeCli.nextResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };

    // Multiple concurrent workers attempt to dispatch and finalize queued acceptance
    const envelope = JSON.parse(queuedOutbox.payloadJson) as WakeEnvelope;
    await Promise.all([
      (service as any).dispatchWakeOutbox(queuedOutbox, envelope),
      (service as any).dispatchWakeOutbox(queuedOutbox, envelope),
      (service as any).dispatchWakeOutbox(queuedOutbox, envelope),
    ]);

    // Queued acceptance must win CAS exactly once: exactly one CLI dispatch call
    assert.equal(fakeCli.calls.length, 1, "Queued acceptance must trigger delivery exactly once across races");
    assert.equal(fakeCli.calls[0].threadId, "thread_cq");
    assert.equal(fakeCodex.deliveredWakes.length, 0, "Codex adapter wake deliveries must remain 0");

    const deliveredOutbox = store.getWakeOutboxById(outbox.id);
    assert.ok(deliveredOutbox, "Delivered outbox row must exist");
    assert.equal(deliveredOutbox.status, "delivered");
    assert.equal(deliveredOutbox.attempts, 2, "Attempts incremented exactly once by winning CAS");

    const barrier = store.getParkBarrier(receipt.parkId);
    assert.ok(barrier, "Barrier must exist");
    assert.equal(barrier.state, "woken");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Crash-interrupted waking/deferred rows recover safely after restart
// ---------------------------------------------------------------------------
test("WAKE-SUPER-3: crash-interrupted waking/deferred rows recover safely after restart", async () => {
  const { tmp, config, store: store1 } = await createTestEnv();
  const dbPath = path.join(tmp, "bridge.sqlite");
  const fakeCodex1 = new FakeCodexDelivery();
  const fakeCli1 = new FakeCliTransport();

  try {
    const agent = setupAgent(store1, tmp, "agent_crash");

    // 1. Setup Barrier A with row in 'waking' status (simulating crash mid-delivery)
    const j1 = store1.createJob({ id: "job_crash_1", agentId: agent.id, kind: "spawn", requestId: "rc1", promptHash: "hc1" });
    store1.bindJob({ jobId: j1.id, threadId: "thread_crash_a", originatingTurnId: "tc_a", originatingItemId: "ic_a" });
    const r1 = path.join(tmp, "r_crash_1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "c1" } }));
    makeJobCompleted(store1, j1.id, r1, "c1");

    store1.createOrUpdateParkBarrier({
      id: "park_crash_a",
      threadId: "thread_crash_a",
      turnId: "tc_a",
      generation: 1,
      armed: true,
      deliveryMode: "cli_resume",
      state: "waking",
    });
    store1.setParkJobs("park_crash_a", [j1.id]);

    const outboxA = store1.createWakeOutbox({
      id: "wake_crash_waking",
      parkId: "park_crash_a",
      generation: 1,
      threadId: "thread_crash_a",
      turnId: "tc_a",
      deliveryMode: "cli_resume",
      status: "pending",
      wakeState: "waiting",
      wakeMarker: "<!-- wake marker a -->",
      payloadJson: JSON.stringify({ readyJobIds: [j1.id], jobIds: [j1.id], parkId: "park_crash_a", generation: 1 }),
    });
    // Force status = 'waking' directly in SQLite to simulate mid-delivery crash right after claimWakeOutbox
    store1.db.prepare("UPDATE wake_outbox SET status = 'waking', attempts = 1 WHERE id = ?").run(outboxA.id);

    // 2. Setup Barrier B with row in 'deferred_active_writer' status with past next_attempt_at
    const j2 = store1.createJob({ id: "job_crash_2", agentId: agent.id, kind: "spawn", requestId: "rc2", promptHash: "hc2" });
    store1.bindJob({ jobId: j2.id, threadId: "thread_crash_b", originatingTurnId: "tc_b", originatingItemId: "ic_b" });
    const r2 = path.join(tmp, "r_crash_2.json");
    await writeFile(r2, JSON.stringify({ envelope: { summary: "c2" } }));
    makeJobCompleted(store1, j2.id, r2, "c2");

    store1.createOrUpdateParkBarrier({
      id: "park_crash_b",
      threadId: "thread_crash_b",
      turnId: "tc_b",
      generation: 1,
      armed: true,
      deliveryMode: "cli_resume",
      state: "armed",
    });
    store1.setParkJobs("park_crash_b", [j2.id]);

    const outboxB = store1.createWakeOutbox({
      id: "wake_crash_deferred",
      parkId: "park_crash_b",
      generation: 1,
      threadId: "thread_crash_b",
      turnId: "tc_b",
      deliveryMode: "cli_resume",
      status: "deferred_active_writer",
      wakeState: "deferred_active_writer",
      wakeMarker: "<!-- wake marker b -->",
      payloadJson: JSON.stringify({ readyJobIds: [j2.id], jobIds: [j2.id], parkId: "park_crash_b", generation: 1 }),
      nextAttemptAt: new Date(Date.now() - 10000).toISOString(),
    });

    // Crash simulation: close store 1 without clean shutdown
    store1.close();

    // -------------------------------------------------------------------
    // RESTART: Start new service pointing to same SQLite database
    // -------------------------------------------------------------------
    const store2 = new BridgeStore(dbPath);
    const fakeCodex2 = new FakeCodexDelivery();
    const fakeCli2 = new FakeCliTransport();
    fakeCli2.nextResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };

    const service2 = createTestService(config, store2, fakeCodex2, fakeCli2);
    await service2.start();

    try {
      // Mandatory call counts: restart must NOT invoke delivery again for unresolved waking row A;
      // only deferred row B with expired retry delay is dispatched to CLI.
      assert.equal(
        fakeCli2.calls.length,
        1,
        "Restart must dispatch only deferred outbox B; unresolved waking row A must not be redelivered",
      );
      assert.equal(fakeCli2.calls[0].threadId, "thread_crash_b");
      assert.equal(fakeCodex2.deliveredWakes.length, 0, "No Codex adapter wake deliveries");

      // Unresolved waking row A remains in waking status without redelivery; attempts unchanged
      const unresolvedA = store2.getWakeOutboxById(outboxA.id);
      assert.ok(unresolvedA, "Outbox A must exist in store");
      assert.equal(
        unresolvedA.status,
        "waking",
        "Unresolved waking row must remain in waking status without redelivery",
      );
      assert.equal(
        unresolvedA.attempts,
        1,
        "Attempts must not be incremented for unresolved waking row",
      );

      const barrierA = store2.getParkBarrier("park_crash_a");
      assert.ok(barrierA, "Barrier A must exist in store");
      assert.equal(
        barrierA.state,
        "waking",
        "Barrier A must remain in waking state while waking row is unresolved",
      );

      // Deferred row B is claimed and delivered
      const recoveredB = store2.getWakeOutboxById(outboxB.id);
      assert.ok(recoveredB, "Outbox B must exist in store");
      assert.equal(
        recoveredB.status,
        "delivered",
        "Interrupted deferred_active_writer row must be recovered and delivered after daemon restart",
      );
      assert.equal(recoveredB.attempts, 1, "Deferred row attempts incremented by successful claim");

      const barrierB = store2.getParkBarrier("park_crash_b");
      assert.ok(barrierB, "Barrier B must exist in store");
      assert.equal(barrierB.state, "woken");

      // Queued marker reconciliation later terminalizes unresolved waking row via service
      let reconcileCalls = 0;
      fakeCodex2.reconcileSend = async (threadId: string, marker: string) => {
        reconcileCalls++;
        if (threadId === "thread_crash_a" && marker === "<!-- wake marker a -->") {
          return true;
        }
        return false;
      };

      // Trigger recovery pass via service
      await (service2 as any).recoverWakeOutbox();

      // Mandatory call counts: no extra CLI deliveries, exactly one reconciliation call
      assert.equal(
        fakeCli2.calls.length,
        1,
        "Marker reconciliation must terminalize outbox without invoking delivery again",
      );
      assert.equal(
        reconcileCalls,
        1,
        "Reconciliation must query codex for unresolved waking marker",
      );

      // Outbox A and Barrier A must now be terminalized to delivered / woken
      const terminalA = store2.getWakeOutboxById(outboxA.id);
      assert.ok(terminalA, "Outbox A must exist");
      assert.equal(
        terminalA.status,
        "delivered",
        "Crash-interrupted waking row must be terminalized to delivered via queued marker reconciliation",
      );
      assert.equal(terminalA.wakeState, "delivered");
      assert.equal(terminalA.attempts, 1, "Attempts must remain 1 after reconciliation");

      const finalBarrierA = store2.getParkBarrier("park_crash_a");
      assert.ok(finalBarrierA, "Barrier A must exist");
      assert.equal(
        finalBarrierA.state,
        "woken",
        "Barrier A must transition to woken after marker reconciliation",
      );

      // Verify listPendingWakeOutbox no longer returns outboxA or outboxB
      const remainingPending = store2.listPendingWakeOutbox();
      assert.equal(
        remainingPending.some((row) => row.id === outboxA.id || row.id === outboxB.id),
        false,
        "Delivered rows must not appear in listPendingWakeOutbox",
      );
    } finally {
      await service2.stop();
      store2.close();
    }
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Active-writer resume conflict defers when queue unavailable, and persists queued delivery mode and messageId
// ---------------------------------------------------------------------------
test("WAKE-SUPER-4: active-writer resume conflict defers when queue unavailable, and persists queued delivery mode and messageId", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_aw_queue");
    const j1 = store.createJob({ id: "job_aw1", agentId: agent.id, kind: "spawn", requestId: "raw1", promptHash: "haw1" });
    store.bindJob({ jobId: j1.id, threadId: "thread_aw_q", originatingTurnId: "taw", originatingItemId: "iaw" });

    const receipt = await service.park({ job_ids: [j1.id] });
    assert.equal(receipt.armed, true);

    // CLI returns activeWriter conflict on resume attempt
    fakeCli.nextResult = { success: false, activeWriter: true, error: "Active writer conflict on thread" };

    const r1 = path.join(tmp, "raw1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "aw done" } }));
    makeJobCompleted(store, j1.id, r1, "aw done");

    // Evaluate park wake: encounters active writer conflict
    await service.evaluateParkWakes(j1.id);
    assert.equal(fakeCli.calls.length, 1, "Resume attempt must invoke CLI delivery exactly once before active writer deferral");
    assert.equal(fakeCodex.deliveredWakes.length, 0, "Direct Codex adapter must not receive wake calls");

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation);
    assert.ok(outbox, "Wake outbox row must exist");

    // Legacy activeWriter may still defer if queue unavailable:
    assert.equal(
      outbox.status,
      "deferred_active_writer",
      "Legacy active writer defers when queue unavailable",
    );
    assert.equal(
      outbox.wakeState,
      "deferred_active_writer",
      "wakeState must be deferred_active_writer",
    );

    // Persist transport result with deliveryMode: 'queued' and optional messageId:
    store.updateWakeOutboxStatus(outbox.id, "delivered", null, {
      deliveryMode: "queued",
      messageId: "msg_aw_queue_1",
      wakeState: "delivered",
    });

    const deliveredOutbox = store.getWakeOutboxById(outbox.id)!;
    assert.equal(deliveredOutbox.deliveryMode, "queued", "deliveryMode 'queued' must be persisted");
    assert.equal(deliveredOutbox.messageId, "msg_aw_queue_1", "messageId must be persisted");
    assert.equal(deliveredOutbox.status, "delivered");

    // Delivered immutability: delivered rows must never be overwritten
    store.updateWakeOutboxStatus(outbox.id, "deferred_active_writer", null, {
      deliveryMode: "cli_resume",
    });
    const stillDelivered = store.getWakeOutboxById(outbox.id)!;
    assert.equal(stillDelivered.status, "delivered", "Delivered status must be immutable");
    assert.equal(stillDelivered.deliveryMode, "queued", "Delivered deliveryMode must be immutable");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. Obsolete wake cannot later fire
// ---------------------------------------------------------------------------
test("WAKE-SUPER-5: obsolete wake cannot later fire", async () => {
  const { tmp, config, store } = await createTestEnv();
  const fakeCodex = new FakeCodexDelivery();
  const fakeCli = new FakeCliTransport();
  const service = createTestService(config, store, fakeCodex, fakeCli);
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_obsolete");
    const j1 = store.createJob({ id: "job_obs1", agentId: agent.id, kind: "spawn", requestId: "robs1", promptHash: "hobs1" });
    const j2 = store.createJob({ id: "job_obs2", agentId: agent.id, kind: "spawn", requestId: "robs2", promptHash: "hobs2" });
    store.bindJob({ jobId: j1.id, threadId: "thread_obs", originatingTurnId: "tobs", originatingItemId: "iobs1" });
    store.bindJob({ jobId: j2.id, threadId: "thread_obs", originatingTurnId: "tobs", originatingItemId: "iobs2" });

    // Generation 1 park
    const receipt1 = await service.park({ job_ids: [j1.id] });
    assert.equal(receipt1.generation, 1);

    // Generation 1 encounters active writer
    fakeCli.nextResult = { success: false, activeWriter: true, error: "Active writer" };
    const r1 = path.join(tmp, "robs1.json");
    await writeFile(r1, JSON.stringify({ envelope: { summary: "obs 1" } }));
    makeJobCompleted(store, j1.id, r1, "obs 1");
    await service.evaluateParkWakes(j1.id);

    const outboxGen1 = store.getWakeOutbox(receipt1.parkId, 1)!;
    assert.ok(outboxGen1);

    // Advance to generation 2 (superseding generation 1)
    const receipt2 = await service.park({ job_ids: [j2.id], park_id: receipt1.parkId });
    assert.equal(receipt2.generation, 2);

    // Clear recorded CLI calls
    fakeCli.calls = [];
    fakeCli.nextResult = { success: true, accepted: true, executablePath: "codex.exe", version: "0.150.0" };

    // Attempting to claim the obsolete generation 1 outbox row directly must be rejected by CAS
    const claimGen1 = store.claimWakeOutbox(receipt1.parkId, 1);
    assert.equal(
      claimGen1,
      false,
      "Obsolete generation 1 wake outbox must not be claimable once superseded by newer generation",
    );

    // Attempting to trigger recovery or dispatch for obsolete generation 1 must not fire any CLI transport delivery
    const envelopeGen1 = JSON.parse(outboxGen1.payloadJson);
    await (service as any).dispatchWakeOutbox(outboxGen1, envelopeGen1);
    assert.equal(fakeCli.calls.length, 0, "Obsolete wake must not dispatch to CLI transport");

    // Ensure generation 2 barrier remains armed and unaffected by generation 1
    const barrierGen2 = store.getParkBarrier(receipt1.parkId)!;
    assert.equal(barrierGen2.generation, 2);
    assert.equal(barrierGen2.state, "armed");
    assert.equal(barrierGen2.armed, true);
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 6. Migration v18: adds message_id and superseded status to wake_outbox
// ---------------------------------------------------------------------------
test("WAKE-SUPER-6: migration v18 adds message_id and superseded status while preserving existing data", async () => {
  const { tmp, store } = await createTestEnv();
  try {
    const migration = store.db.prepare("SELECT version FROM schema_migrations WHERE version = 18").get() as { version: number } | undefined;
    assert.ok(migration, "migration record 18 must exist");
    assert.equal(migration.version, 18, "migration version 18 must be applied");

    const outboxCols = (store.db.prepare("PRAGMA table_info(wake_outbox)").all() as Array<{ name: string }>).map((c) => c.name);
    assert.ok(outboxCols.includes("message_id"), "wake_outbox must include message_id column");
    assert.ok(outboxCols.includes("delivery_mode"), "wake_outbox must include delivery_mode column");
    assert.ok(outboxCols.includes("wake_state"), "wake_outbox must include wake_state column");

    // Can insert and retrieve with status 'superseded' and messageId
    store.createOrUpdateParkBarrier({
      id: "park_mig18",
      threadId: "thread_mig18",
      turnId: "turn_mig18",
      generation: 1,
      armed: false,
      deliveryMode: "queued",
      state: "idle",
    });

    const outbox = store.createWakeOutbox({
      id: "wake_mig18",
      parkId: "park_mig18",
      generation: 1,
      threadId: "thread_mig18",
      turnId: "turn_mig18",
      deliveryMode: "queued",
      status: "superseded",
      wakeState: "superseded",
      wakeMarker: "<!-- marker mig18 -->",
      payloadJson: "{}",
      messageId: "msg_mig_18",
    });

    assert.equal(outbox.deliveryMode, "queued");
    assert.equal(outbox.status, "superseded");
    assert.equal(outbox.wakeState, "superseded");
    assert.equal(outbox.messageId, "msg_mig_18");

    const fetched = store.getWakeOutboxById("wake_mig18");
    assert.ok(fetched, "Inserted outbox record must exist");
    assert.equal(fetched.deliveryMode, "queued");
    assert.equal(fetched.status, "superseded");
    assert.equal(fetched.wakeState, "superseded");
    assert.equal(fetched.messageId, "msg_mig_18");
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 7. listPendingWakeOutbox preserves waking status without reset and claimWakeOutbox cannot reclaim waking
// ---------------------------------------------------------------------------
test("WAKE-SUPER-7: listPendingWakeOutbox preserves waking status without reset and claimWakeOutbox cannot reclaim waking", async () => {
  const { tmp, store } = await createTestEnv();
  try {
    store.createOrUpdateParkBarrier({
      id: "park_waking_no_reset",
      threadId: "thread_waking_test",
      turnId: "turn_waking_test",
      generation: 1,
      armed: true,
      deliveryMode: "cli_resume",
      state: "waking",
    });

    const outbox = store.createWakeOutbox({
      id: "wake_test_waking_row",
      parkId: "park_waking_no_reset",
      generation: 1,
      threadId: "thread_waking_test",
      turnId: "turn_waking_test",
      deliveryMode: "cli_resume",
      status: "pending",
      wakeState: "waiting",
      wakeMarker: "<!-- marker waking test -->",
      payloadJson: "{}",
    });

    // Directly simulate row in waking status mid-delivery
    store.db.prepare("UPDATE wake_outbox SET status = 'waking', attempts = 1 WHERE id = ?").run(outbox.id);

    // 1. listPendingWakeOutbox must return the current-generation waking row
    const pendingList1 = store.listPendingWakeOutbox();
    assert.equal(pendingList1.length, 1, "listPendingWakeOutbox must return current-generation waking row");
    assert.equal(pendingList1[0].id, outbox.id);
    assert.equal(pendingList1[0].status, "waking");

    // 2. listPendingWakeOutbox must NOT have reset status in SQLite
    const refreshed = store.getWakeOutboxById(outbox.id);
    assert.ok(refreshed, "Wake outbox row must exist");
    assert.equal(refreshed.status, "waking", "listPendingWakeOutbox must not reset waking status to pending");
    assert.equal(refreshed.attempts, 1, "Attempts must remain 1");

    // 3. claimWakeOutbox cannot reclaim waking status
    const claimedById = store.claimWakeOutbox(outbox.id);
    assert.equal(claimedById, false, "claimWakeOutbox(id) must reject row in waking status");

    const claimedByGen = store.claimWakeOutbox("park_waking_no_reset", 1);
    assert.equal(claimedByGen, false, "claimWakeOutbox(parkId, gen) must reject row in waking status");

    // Attempts must remain 1
    const afterClaim = store.getWakeOutboxById(outbox.id);
    assert.ok(afterClaim);
    assert.equal(afterClaim.attempts, 1, "Attempts must not increment on rejected claim");
    assert.equal(afterClaim.status, "waking");

    // 4. Stale generation is excluded from listPendingWakeOutbox
    store.createOrUpdateParkBarrier({
      id: "park_waking_no_reset",
      threadId: "thread_waking_test",
      turnId: "turn_waking_test",
      generation: 2,
      armed: true,
      deliveryMode: "cli_resume",
      state: "armed",
    });

    const pendingList2 = store.listPendingWakeOutbox();
    assert.equal(pendingList2.length, 0, "Superseded generation 1 waking row must be excluded from listPendingWakeOutbox");
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});
