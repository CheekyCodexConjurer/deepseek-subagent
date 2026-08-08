import test from "node:test";
import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeHttpClient } from "../../src/http-server.js";
import { createMcpServer, ensureDaemonRunning } from "../../src/mcp.js";

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
