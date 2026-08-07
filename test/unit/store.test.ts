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
  } finally {
    store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
