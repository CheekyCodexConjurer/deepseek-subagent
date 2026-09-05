import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { BridgeStore } from "../../src/store.js";
import { BridgeService, type ManagedOpenCodeLike } from "../../src/service.js";
import { createDefaultConfig } from "../../src/config.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

class FakeOpenCodeClient implements OpenCodeClientLike {
  promptCalls: Array<{ sessionId: string; task: string }> = [];
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

function makeFakeManager(client = new FakeOpenCodeClient()): {
  start(): Promise<ManagedOpenCodeLike>;
  stop(): Promise<void>;
} {
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

function createDeferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

// ---------------------------------------------------------------------------
// Test A: Independent service.spawn calls with omitted requestId do not share
// the undefined request lock.
// ---------------------------------------------------------------------------
test("(A) independent service.spawn calls with omitted requestId do not share undefined request lock", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-lock-waiter-a-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 8 });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    const call1AtGate = createDeferred();
    const call1Release = createDeferred();
    try {
      let call1InCritical = false;
      let call2RanWhileCall1Holding = false;
      let undefinedLockUsed = false;
      let lockContentionOnUndefined = false;
      let callCount = 0;

      const activeLocks = new Set<unknown>();
      const originalWithRequestIdLock = (service as any).withRequestIdLock.bind(service);
      (service as any).withRequestIdLock = function (key: unknown, operation: () => Promise<unknown>) {
        callCount++;
        const currentCall = callCount;
        if (key === undefined) {
          undefinedLockUsed = true;
          if (activeLocks.has(undefined)) {
            lockContentionOnUndefined = true;
            // Contention detected: Call 2 is attempting to acquire the undefined lock held by Call 1.
            // Release Call 1 gate to avoid deadlocking the test runner while preserving failure evidence.
            call1Release.resolve();
          }
        }
        activeLocks.add(key);

        return originalWithRequestIdLock(key, async () => {
          if (currentCall === 1) {
            call1InCritical = true;
            call1AtGate.resolve();
            await call1Release.promise;
            call1InCritical = false;
          } else if (currentCall === 2) {
            if (call1InCritical) {
              call2RanWhileCall1Holding = true;
              // In non-sharing implementation, Call 2 proceeds concurrently while Call 1 is held.
              // Release Call 1 so both settle.
              call1Release.resolve();
            }
          }
          try {
            return await operation();
          } finally {
            activeLocks.delete(key);
          }
        });
      };

      // Launch Call 1 without requestId
      const p1 = service.spawn({
        topic: "Topic 1",
        task: "Task 1",
        cwd: tmpDir,
      });

      // Wait until Call 1 enters critical section and reaches gate
      await call1AtGate.promise;

      // Launch Call 2 without requestId while Call 1 is held
      const p2 = service.spawn({
        topic: "Topic 2",
        task: "Task 2",
        cwd: tmpDir,
      });

      // Await both spawns to settle
      await Promise.all([p1, p2]);

      assert.equal(
        lockContentionOnUndefined,
        false,
        "independent service.spawn calls with omitted requestId must not share the undefined request lock"
      );
      assert.equal(
        call2RanWhileCall1Holding,
        true,
        "independent service.spawn calls with omitted requestId must execute concurrently without lock serialization"
      );
      assert.equal(
        undefinedLockUsed,
        false,
        "service.spawn must not acquire request lock keyed by undefined when requestId is omitted"
      );
    } finally {
      call1Release.resolve();
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
});

// ---------------------------------------------------------------------------
// Test B: Unary spawn that returns accepted while queued leaves no entry in
// private dispatchWaiters.
// ---------------------------------------------------------------------------
test("(B) a unary spawn that returns accepted while queued leaves no entry in private dispatchWaiters", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-lock-waiter-b-"));
  try {
    const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const client = new FakeOpenCodeClient();
    const config = createDefaultConfig({ dataDir: tmpDir, swarmCreditCeiling: 1 });
    const service = new BridgeService(config, {
      store,
      manager: makeFakeManager(client),
    });
    await service.start();

    try {
      // Blocker job consumes the single available swarm credit
      const blockerReceipt = await service.spawn({
        topic: "Blocker Topic",
        task: "Blocker Task",
        cwd: tmpDir,
      });
      assert.equal(blockerReceipt.accepted, true);

      // Unary spawn behind blocker: admitted but remains queued
      const queuedReceipt = await service.spawn({
        topic: "Queued Topic",
        task: "Queued Task",
        cwd: tmpDir,
      });

      assert.equal(queuedReceipt.accepted, true);
      assert.equal(queuedReceipt.status, "accepted");

      const queuedJob = store.getJob(queuedReceipt.jobId);
      assert.ok(queuedJob, "Queued job must exist in store");
      assert.equal(queuedJob.status, "queued", "Job must remain queued while awaiting credit");

      const dispatchWaiters = (service as any).dispatchWaiters as Map<string, unknown>;
      assert.equal(
        dispatchWaiters.has(queuedReceipt.jobId),
        false,
        "a unary spawn that returns accepted while queued leaves no entry in private dispatchWaiters"
      );
      assert.equal(
        dispatchWaiters.size,
        0,
        "dispatchWaiters must not retain entries for jobs that returned while queued"
      );
    } finally {
      await service.stop();
      store.close();
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
});
