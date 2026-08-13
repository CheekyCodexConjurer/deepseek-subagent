import test from "node:test";
import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeHttpClient, BridgeHttpError } from "../../src/http-server.js";
import { createLazyDaemonBootstrap, createMcpServer, ensureDaemonRunning } from "../../src/mcp.js";

function acceptedCallFixture(): { call: (pathname: string) => Promise<Record<string, unknown>> } {
  return {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/spawn" || pathname === "/v1/jobs/continue") {
        return {
          accepted: true,
          status: "accepted",
          topic: "test topic",
          modelDisplayName: "DeepSeek V4 Flash · Max",
          agentId: "agent_1",
          jobId: "job_1",
          state: "Starting",
        };
      }
      if (pathname === "/v1/jobs/consult") {
        return {
          agentId: "agent_1",
          jobId: "job_1",
          topic: "test topic",
          status: "running",
          elapsedSeconds: 1,
          lastActivityAgoSeconds: 1,
          currentActivity: "Working",
          recentActivity: [],
          filesTouched: [],
          testSummary: "No test result observed yet.",
          resultAvailable: false,
        };
      }
      throw new Error("Unexpected endpoint: " + pathname);
    },
  };
}

test("MCP exposes the stable DeepSeek Sub-Agent identity and seven tools", async () => {
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
      "deepseek_consult",
      "deepseek_follow",
      "deepseek_abort",
      "deepseek_close",
      "deepseek_recover_result",
    ]);
    assert.equal(tools[0]?.title, "DeepSeek Sub-Agent · Spawn");
    assert.match(tools[0]?.description ?? "", /asynchronous/i);
    assert.match(tools[0]?.description ?? "", /do not poll/i);
    const consult = tools.find((tool) => tool.name === "deepseek_consult");
    const follow = tools.find((tool) => tool.name === "deepseek_follow");
    assert.equal(consult?.title, "DeepSeek Sub-Agent · Consult");
    assert.match(consult?.description ?? "", /observable/i);
    assert.match(consult?.description ?? "", /never exposes private reasoning/i);
    assert.equal(follow?.title, "DeepSeek Sub-Agent · Follow");
    assert.match(follow?.description ?? "", /without polling/i);
    const followProperties = (follow?.inputSchema as { properties?: Record<string, { default?: number }> } | undefined)?.properties ?? {};
    assert.equal(followProperties.wait_minutes?.default, undefined);
    assert.equal(followProperties.grace_minutes?.default, undefined);
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

test("MCP handshake and tool listing complete without waiting for daemon startup; first operations wait", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let ready = false;
  let starts = 0;
  const healthClient = {
    async health(): Promise<unknown> {
      if (!ready) throw new Error("connect ECONNREFUSED");
      return { status: { running: true } };
    },
  };
  const server = createMcpServer(acceptedCallFixture() as unknown as BridgeHttpClient, {
    ensureReady: createLazyDaemonBootstrap(config, healthClient, {
      start: async () => {
        starts += 1;
        ready = true;
      },
      timeoutMs: 200,
      retryMs: 1,
    }),
  });
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const result = await client.listTools();
    assert.equal(result.tools.length, 7);
    assert.equal(starts, 0, "tool listing must not bootstrap the daemon");
    const first = client.callTool({ name: "deepseek_spawn", arguments: { topic: "test topic", task: "test task" } });
    const second = client.callTool({ name: "deepseek_consult", arguments: { agent_id: "agent_1" } });
    const [spawned, consulted] = await Promise.all([first, second]);
    assert.equal(spawned.isError, undefined);
    assert.equal(consulted.isError, undefined);
    assert.equal(starts, 1, "concurrent first operations must share one bootstrap");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP bootstrap failure surfaces a clear readiness error on tool operations", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const healthClient = {
    async health(): Promise<unknown> {
      throw new Error("connect ECONNREFUSED");
    },
  };
  const server = createMcpServer(acceptedCallFixture() as unknown as BridgeHttpClient, {
    ensureReady: createLazyDaemonBootstrap(config, healthClient, {
      start: async () => {
        throw new Error("daemon refused to start");
      },
      timeoutMs: 20,
      retryMs: 1,
    }),
  });
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const result = await client.callTool({ name: "deepseek_spawn", arguments: { topic: "test topic", task: "test task" } });
    assert.equal(result.isError, true);
    const text = (result.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(text, /daemon is not ready/i);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP exposes visual_context as an optional string on spawn and continue", async () => {
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
    const spawn = tools.find((tool) => tool.name === "deepseek_spawn");
    const continueTool = tools.find((tool) => tool.name === "deepseek_continue");
    const spawnProperties = (spawn?.inputSchema as { properties?: Record<string, { type?: string; default?: unknown }> } | undefined)?.properties ?? {};
    const continueProperties = (continueTool?.inputSchema as { properties?: Record<string, { type?: string; default?: unknown }> } | undefined)?.properties ?? {};
    assert.equal(spawnProperties.visual_context?.type, "string");
    assert.equal(spawnProperties.visual_context?.default, undefined);
    assert.equal(continueProperties.visual_context?.type, "string");
    assert.equal(continueProperties.visual_context?.default, undefined);
    assert.match(spawn?.description ?? "", /visual_context/);
    assert.match(spawn?.description ?? "", /Direct observations/);
    assert.match(spawn?.description ?? "", /Uncertainty/);
    assert.match(continueTool?.description ?? "", /visual_context/);
    assert.match(continueTool?.description ?? "", /never receives pixels/);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP exposes allow_respawn as an optional boolean on continue only", async () => {
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
    const spawn = tools.find((tool) => tool.name === "deepseek_spawn");
    const continueTool = tools.find((tool) => tool.name === "deepseek_continue");
    const spawnProperties = (spawn?.inputSchema as { properties?: Record<string, { type?: string; default?: unknown }> } | undefined)?.properties ?? {};
    const continueProperties = (continueTool?.inputSchema as { properties?: Record<string, { type?: string; default?: unknown }> } | undefined)?.properties ?? {};
    assert.equal(continueProperties.allow_respawn?.type, "boolean");
    assert.equal(continueProperties.allow_respawn?.default, undefined, "respawn is opt-in, never implicit");
    assert.equal(spawnProperties.allow_respawn, undefined, "spawn has no respawn flag");
    assert.match(continueTool?.description ?? "", /allow_respawn/);
    assert.match(continueTool?.description ?? "", /NEW agent/i);
    assert.match(continueTool?.description ?? "", /NEW OpenCode session/i);
    assert.match(continueTool?.description ?? "", /never claims the closed session/i);
    assert.match(continueTool?.description ?? "", /explicitly aborted/i);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP accepted text warns and names the exact job when dispatch outcome is uncertain", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const bridgeClient = {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/spawn") {
        return {
          accepted: true,
          status: "accepted",
          topic: "test topic",
          modelDisplayName: "DeepSeek V4 Flash · Max",
          agentId: "agent_1",
          jobId: "job_1",
          state: "Starting",
          outcome: "dispatch_unknown",
        };
      }
      throw new Error("Unexpected endpoint: " + pathname);
    },
  } as unknown as BridgeHttpClient;
  const server = createMcpServer(bridgeClient);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const result = await client.callTool({ name: "deepseek_spawn", arguments: { topic: "test topic", task: "test task" } });
    assert.equal(result.isError, undefined, "an uncertain dispatch must still resolve as an accepted tool result");
    const text = (result.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(text, /Pending DeepSeek job: job_1/);
    assert.match(text, /uncertain|transport failure/i);
    assert.match(text, /deepseek_follow/);
    const structured = result.structuredContent as Record<string, unknown>;
    assert.equal(structured.accepted, true);
    assert.equal(structured.agentId, "agent_1");
    assert.equal(structured.jobId, "job_1");
    assert.equal(structured.obligationState, "pending");
    assert.equal(structured.nextRequiredAction, "deepseek_follow");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP spawn and continue report a pending obligation in content and structuredContent", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const bridgeClient = {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/spawn" || pathname === "/v1/jobs/continue") {
        return {
          accepted: true,
          status: "accepted",
          topic: "test topic",
          modelDisplayName: "DeepSeek V4 Flash · Max",
          agentId: "agent_1",
          jobId: "job_1",
          state: "Starting",
        };
      }
      throw new Error("Unexpected endpoint: " + pathname);
    },
  } as unknown as BridgeHttpClient;
  const server = createMcpServer(bridgeClient);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    for (const toolName of ["deepseek_spawn", "deepseek_continue"] as const) {
      const result = await client.callTool({
        name: toolName,
        arguments: toolName === "deepseek_spawn"
          ? { topic: "test topic", task: "test task" }
          : { agent_id: "agent_1", task: "continue task" },
      });
      assert.equal(result.isError, undefined);
      const text = (result.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
      assert.match(text, /Pending DeepSeek job created: job_1/);
      assert.match(text, /Accepted is not a result/);
      assert.match(text, /Do not duplicate this delegated front locally/);
      assert.match(text, /consume the job with deepseek_follow/);
      assert.match(text, /abort\/close it/);
      const structured = result.structuredContent as Record<string, unknown>;
      assert.equal(structured.obligationState, "pending");
      assert.equal(structured.nextRequiredAction, "deepseek_follow");
      assert.equal(structured.accepted, true);
      assert.equal(structured.status, "accepted");
      assert.equal(structured.topic, "test topic");
      assert.equal(structured.modelDisplayName, "DeepSeek V4 Flash · Max");
      assert.equal(structured.agentId, "agent_1");
      assert.equal(structured.jobId, "job_1");
      assert.equal(structured.state, "Starting");
    }
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP follow terminal results close the obligation", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const statuses = ["completed", "aborted"];
  const bridgeClient = {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/follow") {
        const status = statuses.shift() ?? "completed";
        return {
          agentId: "agent_1",
          jobId: "job_1",
          status,
          deadlineReached: false,
          gracefulFinalize: false,
          partial: false,
          workerAborted: false,
          resultAvailable: true,
          progress: {
            agentId: "agent_1",
            jobId: "job_1",
            topic: "test topic",
            status,
            elapsedSeconds: 1,
            lastActivityAgoSeconds: 1,
            currentActivity: "Done",
            recentActivity: [],
            filesTouched: [],
            testSummary: "No test result observed yet.",
            resultAvailable: true,
          },
        };
      }
      throw new Error("Unexpected endpoint: " + pathname);
    },
  } as unknown as BridgeHttpClient;
  const server = createMcpServer(bridgeClient);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    for (const expectedStatus of ["completed", "aborted"]) {
      const result = await client.callTool({ name: "deepseek_follow", arguments: { agent_id: "agent_1" } });
      assert.equal(result.isError, undefined);
      const text = (result.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
      assert.match(text, /terminal result/);
      assert.doesNotMatch(text, /approval/i);
      const structured = result.structuredContent as Record<string, unknown>;
      assert.equal(structured.obligationState, "closed");
      assert.equal(structured.nextRequiredAction, undefined);
      assert.equal(structured.agentId, "agent_1");
      assert.equal(structured.jobId, "job_1");
      assert.equal(structured.status, expectedStatus);
      assert.equal(structured.resultAvailable, true);
    }
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP follow needs_approval keeps the obligation pending", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const bridgeClient = {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/follow") {
        return {
          agentId: "agent_1",
          jobId: "job_1",
          status: "needs_approval",
          deadlineReached: false,
          gracefulFinalize: false,
          partial: false,
          workerAborted: false,
          resultAvailable: false,
          permissionId: "permission_7",
          message: "DeepSeek requires explicit approval before continuing.",
          progress: {
            agentId: "agent_1",
            jobId: "job_1",
            topic: "test topic",
            status: "needs_approval",
            elapsedSeconds: 1,
            lastActivityAgoSeconds: 1,
            currentActivity: "Waiting for approval",
            recentActivity: [],
            filesTouched: [],
            testSummary: "No test result observed yet.",
            resultAvailable: false,
          },
        };
      }
      throw new Error("Unexpected endpoint: " + pathname);
    },
  } as unknown as BridgeHttpClient;
  const server = createMcpServer(bridgeClient);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const result = await client.callTool({ name: "deepseek_follow", arguments: { agent_id: "agent_1" } });
    assert.equal(result.isError, undefined);
    const text = (result.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(text, /requires explicit approval/);
    assert.match(text, /deepseek_continue/);
    assert.doesNotMatch(text, /terminal result/);
    const structured = result.structuredContent as Record<string, unknown>;
    assert.equal(structured.obligationState, "pending");
    assert.equal(structured.nextRequiredAction, "deepseek_continue");
    assert.equal(structured.status, "needs_approval");
    assert.equal(structured.permissionId, "permission_7");
    assert.equal(structured.message, "DeepSeek requires explicit approval before continuing.");
    assert.equal(structured.resultAvailable, false);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP abort and close end the obligation", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const bridgeClient = {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/abort") {
        return { agentId: "agent_1", jobId: "job_1", status: "aborted" };
      }
      if (pathname === "/v1/jobs/close") {
        return { agentId: "agent_1", status: "closed" };
      }
      throw new Error("Unexpected endpoint: " + pathname);
    },
  } as unknown as BridgeHttpClient;
  const server = createMcpServer(bridgeClient);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const abortResult = await client.callTool({ name: "deepseek_abort", arguments: { agent_id: "agent_1" } });
    assert.equal(abortResult.isError, undefined);
    const abortStructured = abortResult.structuredContent as Record<string, unknown>;
    assert.equal(abortStructured.obligationState, "closed");
    assert.equal(abortStructured.status, "aborted");
    assert.equal(abortStructured.state, "Stopped");
    const closeResult = await client.callTool({ name: "deepseek_close", arguments: { agent_id: "agent_1" } });
    assert.equal(closeResult.isError, undefined);
    const closeStructured = closeResult.structuredContent as Record<string, unknown>;
    assert.equal(closeStructured.obligationState, "closed");
    assert.equal(closeStructured.status, "closed");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP obligation metadata: descriptions, readOnlyHint and output schemas", async () => {
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
    const spawn = tools.find((tool) => tool.name === "deepseek_spawn");
    const continueTool = tools.find((tool) => tool.name === "deepseek_continue");
    const consult = tools.find((tool) => tool.name === "deepseek_consult");
    const follow = tools.find((tool) => tool.name === "deepseek_follow");
    const abort = tools.find((tool) => tool.name === "deepseek_abort");
    const close = tools.find((tool) => tool.name === "deepseek_close");
    const outputProperties = (tool: typeof spawn) =>
      (tool?.outputSchema?.properties ?? {}) as Record<string, { type?: string; const?: string }>;
    assert.equal(outputProperties(spawn).obligationState?.const, "pending");
    assert.equal(outputProperties(spawn).nextRequiredAction?.const, "deepseek_follow");
    assert.equal(outputProperties(continueTool).obligationState?.const, "pending");
    assert.equal(outputProperties(continueTool).nextRequiredAction?.const, "deepseek_follow");
    assert.equal(follow?.annotations?.readOnlyHint, false);
    assert.match(follow?.description ?? "", /before a dependent gate or a final response/);
    assert.match(follow?.description ?? "", /may be aborted after the grace period/);
    assert.doesNotMatch(follow?.description ?? "", /no useful independent work|no more independent work/);
    assert.equal(consult?.annotations?.readOnlyHint, true);
    assert.match(abort?.description ?? "", /end its pending obligation/);
    assert.match(close?.description ?? "", /ending any pending obligation/);
    assert.equal(abort?.annotations?.destructiveHint, true);
    assert.equal(close?.annotations?.destructiveHint, false);
    assert.equal(spawn?.annotations?.readOnlyHint, false);
    assert.equal(continueTool?.annotations?.readOnlyHint, false);
    assert.match(spawn?.description ?? "", /pending obligation/);
    assert.match(spawn?.description ?? "", /consume the job with deepseek_follow/);
    assert.match(spawn?.description ?? "", /deepseek_abort or deepseek_close/);
    assert.match(continueTool?.description ?? "", /pending obligation/);
    assert.match(continueTool?.description ?? "", /consume the job with deepseek_follow/);
    assert.match(continueTool?.description ?? "", /deepseek_abort or deepseek_close/);
    const followObligation = JSON.stringify(follow?.outputSchema?.properties?.obligationState ?? {});
    assert.match(followObligation, /pending/);
    assert.match(followObligation, /closed/);
    const followNextAction = JSON.stringify(follow?.outputSchema?.properties?.nextRequiredAction ?? {});
    assert.match(followNextAction, /deepseek_continue/);
    assert.equal(outputProperties(abort).obligationState?.const, "closed");
    assert.equal(outputProperties(close).obligationState?.const, "closed");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP exposes model_route as an optional string on spawn only", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const server = createMcpServer(new BridgeHttpClient(config));
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const tools = await client.listTools();
    const spawn = tools.tools.find((tool) => tool.name === "deepseek_spawn");
    const continueTool = tools.tools.find((tool) => tool.name === "deepseek_continue");
    const spawnProperties = (spawn?.inputSchema as { properties?: Record<string, unknown> })?.properties ?? {};
    const continueProperties = (continueTool?.inputSchema as { properties?: Record<string, unknown> })?.properties ?? {};
    assert.ok("model_route" in spawnProperties, "spawn must accept model_route");
    assert.equal("model_route" in continueProperties, false, "model_route is only valid on spawn");
    assert.match(spawn?.description ?? "", /model_route/i);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP propagates structured error codes without sniffing message text", async () => {
  const failing = {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/spawn") {
        throw new BridgeHttpError(400, "route_disabled", "Model route is disabled: pro-max", { route: "pro-max" });
      }
      if (pathname === "/v1/jobs/abort") {
        throw new BridgeHttpError(404, "unknown_agent", "Unknown agent: agent_1");
      }
      if (pathname === "/v1/jobs/continue") {
        throw new BridgeHttpError(409, "busy", "Agent is busy with job job_1", { jobId: "job_1" });
      }
      throw new Error("Unexpected endpoint: " + pathname);
    },
  } as unknown as BridgeHttpClient;
  const server = createMcpServer(failing);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const routeResult = await client.callTool({ name: "deepseek_spawn", arguments: { topic: "t", task: "t", model_route: "pro-max" } });
    assert.equal(routeResult.isError, true);
    assert.equal((routeResult.structuredContent as Record<string, unknown>)?.code, "route_disabled");
    assert.equal((routeResult.structuredContent as Record<string, unknown>)?.status, 400);

    const agentResult = await client.callTool({ name: "deepseek_abort", arguments: { agent_id: "agent_1" } });
    assert.equal(agentResult.isError, true);
    assert.equal((agentResult.structuredContent as Record<string, unknown>)?.code, "unknown_agent");

    const busyResult = await client.callTool({ name: "deepseek_continue", arguments: { agent_id: "agent_1", task: "t" } });
    assert.equal(busyResult.isError, true);
    const busyStructured = busyResult.structuredContent as Record<string, unknown>;
    assert.equal(busyStructured.code, "busy");
    assert.equal(busyStructured.retry, false);
  } finally {
    await client.close();
    await server.close();
  }
});
