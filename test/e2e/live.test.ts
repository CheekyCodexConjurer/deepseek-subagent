import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { createDefaultConfig } from "../../src/config.js";
import { OpenCodeManager } from "../../src/opencode/manager.js";
import { BridgeService } from "../../src/service.js";
import { rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

test("live OpenCode DeepSeek V4 Flash max smoke", { skip: process.env.BRIDGE_LIVE_E2E !== "1" }, async () => {
  const config = createDefaultConfig({
    opencodeMode: "managed",
    opencodeStartupTimeoutMs: 45_000,
    opencodeEventReconnectMaxMs: 5_000,
  });
  const manager = new OpenCodeManager(config);
  const managed = await manager.start(process.cwd());
  const abort = new AbortController();
  let idle = false;
  const stream = managed.client.subscribe(async (event) => {
    if (event.type === "session.idle") idle = true;
  }, abort.signal);
  try {
    const session = await managed.client.createSession(process.cwd(), "Live DeepSeek smoke");
    await delay(500);
    await managed.client.promptAsync(session.id, "Reply with exactly LIVE_BRIDGE_OK and nothing else.", {
      providerId: "opencode-go",
      modelId: "deepseek-v4-flash",
      variant: "max",
      agent: "build",
    });
    const deadline = Date.now() + 60_000;
    while (!idle && Date.now() < deadline) await delay(250);
    assert.equal(idle, true);
    const messages = await managed.client.listMessages(session.id);
    const text = JSON.stringify(messages);
    assert.match(text, /LIVE_BRIDGE_OK/);
    const assistant = messages.find((message) => message.info?.role === "assistant");
    assert.equal(assistant?.info?.providerID, "opencode-go");
    assert.equal(assistant?.info?.modelID, "deepseek-v4-flash");
    assert.equal(assistant?.info?.variant, "max");
  } finally {
    abort.abort();
    await Promise.race([stream, delay(2_000)]);
    await managed.stop();
  }
});

test("live bridge spawn consult follow uses OpenCode SSE completion", { skip: process.env.BRIDGE_LIVE_E2E !== "1" }, async () => {
  const directory = await (await import("node:fs/promises")).mkdtemp(path.join(os.tmpdir(), "deepseek-live-follow-"));
  const config = createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    opencodeMode: "managed",
    opencodeStartupTimeoutMs: 45_000,
    opencodeEventReconnectMaxMs: 5_000,
  });
  const service = new BridgeService(config);
  try {
    await service.start();
    const accepted = await service.spawn({
      requestId: "live_follow_" + Date.now(),
      topic: "Live event-driven follow",
      task: "Inspect the workspace lightly and return a concise report. Do not make edits. Run no long tests.",
      cwd: process.cwd(),
      mode: "analyze",
    });
    const snapshot = await service.consult({ agentId: accepted.agentId, jobId: accepted.jobId, activityLimit: 10 });
    assert.equal(snapshot.agentId, accepted.agentId);
    const result = await service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 1, graceMinutes: 1 });
    assert.equal(result.agentId, accepted.agentId);
    assert.ok(["completed", "completed_partial"].includes(result.status));
    assert.equal(result.resultAvailable, true);
    assert.equal(result.progress.jobId, accepted.jobId);
  } finally {
    await service.stop();
    await rm(directory, { recursive: true, force: true });
  }
});
