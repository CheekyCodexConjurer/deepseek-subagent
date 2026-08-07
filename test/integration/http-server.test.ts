import test from "node:test";
import assert from "node:assert/strict";
import { createServer, type AddressInfo } from "node:net";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeHttpServer } from "../../src/http-server.js";
import type { BridgeService } from "../../src/service.js";

async function freePort(): Promise<number> {
  const probe = createServer();
  return new Promise<number>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      const port = typeof address === "object" && address ? (address as AddressInfo).port : 0;
      probe.close((error) => error ? reject(error) : resolve(port));
    });
  });
}

test("HTTP maps visual_context to spawn and continue inputs", async () => {
  const calls: Array<{ kind: string; input: unknown }> = [];
  const service = {
    spawn: async (input: unknown) => {
      calls.push({ kind: "spawn", input });
      return { accepted: true };
    },
    continueJob: async (input: unknown) => {
      calls.push({ kind: "continue", input });
      return { accepted: true };
    },
  } as unknown as BridgeService;
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: await freePort(),
    daemonToken: "http-test-token",
    dataDir: "C:\\deepseek-http-test-data",
    configPath: "C:\\deepseek-http-test-data\\config.json",
  });
  const server = new BridgeHttpServer(config, service);
  await server.start();
  try {
    const headers = {
      authorization: "Bearer " + config.daemonToken,
      "content-type": "application/json",
    };
    const spawnResponse = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/spawn`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        topic: "visual spawn",
        task: "inspect the screenshot",
        visual_context: "Direct observations: a red banner is visible",
      }),
    });
    assert.equal(spawnResponse.status, 202);

    const continueResponse = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/continue`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        agent_id: "agent_visual",
        task: "continue from the screenshot",
        visual_context: "Interpretation: the banner indicates a failed build",
      }),
    });
    assert.equal(continueResponse.status, 202);

    assert.equal(calls.length, 2);
    assert.equal((calls[0]?.input as { visualContext?: string }).visualContext, "Direct observations: a red banner is visible");
    assert.equal((calls[1]?.input as { visualContext?: string }).visualContext, "Interpretation: the banner indicates a failed build");
  } finally {
    await server.stop();
  }
});
