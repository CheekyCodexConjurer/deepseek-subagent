import test from "node:test";
import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeHttpClient } from "../../src/http-server.js";
import { createMcpServer, ensureDaemonRunning } from "../../src/mcp.js";

test("MCP exposes the stable DeepSeek Sub-Agent identity and five tools", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const bridgeClient = new BridgeHttpClient(config);
  const server = createMcpServer(bridgeClient);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const result = await client.listTools();
    const tools = result.tools;
    assert.deepEqual(tools.map((tool) => tool.name), [
      "deepseek_spawn",
      "deepseek_continue",
      "deepseek_abort",
      "deepseek_close",
      "deepseek_recover_result",
    ]);
    assert.equal(tools[0]?.title, "DeepSeek Sub-Agent · Spawn");
    assert.match(tools[0]?.description ?? "", /asynchronous/i);
    assert.match(tools[0]?.description ?? "", /do not poll/i);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP startup recovers an offline local daemon before exposing tools", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let ready = false;
  let healthCalls = 0;
  let starts = 0;
  const client = {
    async health(): Promise<unknown> {
      healthCalls += 1;
      if (!ready) throw new Error("connect ECONNREFUSED");
      return { status: { running: true } };
    },
  };
  await ensureDaemonRunning(config, client, {
    start: async () => {
      starts += 1;
      ready = true;
    },
    timeoutMs: 100,
    retryMs: 1,
  });
  assert.equal(starts, 1);
  assert.equal(healthCalls, 2);
});
