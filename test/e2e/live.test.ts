import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { createDefaultConfig } from "../../src/config.js";
import { OpenCodeManager } from "../../src/opencode/manager.js";

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
