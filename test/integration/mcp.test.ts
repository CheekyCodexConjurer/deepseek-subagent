import test from "node:test";
import assert from "node:assert/strict";
import { createServer, type AddressInfo } from "node:net";
import { mkdtemp, rm, writeFile, mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeHttpClient, BridgeHttpError, BridgeHttpServer, BridgeTransportError } from "../../src/http-server.js";
import { createLazyDaemonBootstrap, createMcpServer, ensureDaemonRunning } from "../../src/mcp.js";
import { TranscriptAttestor } from "../../src/codex/transcript-attestor.js";
import type { BridgeService } from "../../src/service.js";

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
      if (pathname === "/v1/jobs/spawn-batch") {
        return {
          accepted: true,
          batchId: "batch_1",
          batchRequestId: "batch_req_1",
          items: [
            {
              jobId: "job_1",
              agentId: "agent_1",
              requestId: "req_1",
              status: "accepted",
            },
          ],
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

test("MCP exposes SubAgents MCP canonical identity, nine canonical tools and deepseek_* migration aliases", async () => {
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
      "subagents_spawn",
      "subagents_spawn_batch",
      "subagents_continue",
      "subagents_status",
      "subagents_follow",
      "subagents_park",
      "subagents_abort",
      "subagents_close",
      "subagents_recover_result",
      "deepseek_spawn",
      "deepseek_spawn_batch",
      "deepseek_continue",
      "deepseek_consult",
      "deepseek_follow",
      "deepseek_park",
      "deepseek_abort",
      "deepseek_close",
      "deepseek_recover_result",
    ]);
    assert.equal(tools[0]?.title, "SubAgents MCP · Spawn");
    assert.match(tools[0]?.description ?? "", /asynchronous/i);
    assert.match(tools[0]?.description ?? "", /do not poll/i);
    assert.doesNotMatch(tools[0]?.description ?? "", /DeepSeek V4 Flash/, "spawn description must be provider-neutral; the route decides the provider");
    assert.match(tools[0]?.description ?? "", /active model route/i);
    assert.match(tools[0]?.description ?? "", /operator-only/i);

    const spawn = tools.find((tool) => tool.name === "subagents_spawn");
    const spawnBatch = tools.find((tool) => tool.name === "subagents_spawn_batch");
    const continueTool = tools.find((tool) => tool.name === "subagents_continue");
    const statusTool = tools.find((tool) => tool.name === "subagents_status");
    const follow = tools.find((tool) => tool.name === "subagents_follow");
    const park = tools.find((tool) => tool.name === "subagents_park");
    const abort = tools.find((tool) => tool.name === "subagents_abort");
    const close = tools.find((tool) => tool.name === "subagents_close");
    const recover = tools.find((tool) => tool.name === "subagents_recover_result");

    assert.equal(spawn?.title, "SubAgents MCP · Spawn");
    assert.equal(spawnBatch?.title, "SubAgents MCP · Spawn Batch");
    assert.equal(continueTool?.title, "SubAgents MCP · Continue");
    assert.equal(statusTool?.title, "SubAgents MCP · Status");
    assert.equal(follow?.title, "SubAgents MCP · Follow");
    assert.equal(park?.title, "SubAgents MCP · Park");
    assert.equal(abort?.title, "SubAgents MCP · Abort");
    assert.equal(close?.title, "SubAgents MCP · Close");
    assert.equal(recover?.title, "SubAgents MCP · Recover result");

    assert.match(spawn?.description ?? "", /subagents_follow/i);
    assert.match(spawnBatch?.description ?? "", /subagents_follow/i);
    assert.match(continueTool?.description ?? "", /subagents_follow/i);
    assert.match(statusTool?.description ?? "", /observable/i);
    assert.match(statusTool?.description ?? "", /never exposes private reasoning/i);
    assert.match(follow?.description ?? "", /without polling/i);
    assert.match(park?.description ?? "", /subagents_follow/i);
    const followProperties = (follow?.inputSchema as { properties?: Record<string, { default?: number }> } | undefined)?.properties ?? {};
    assert.equal(followProperties.wait_minutes?.default, undefined);
    assert.equal(followProperties.grace_minutes?.default, undefined);

    // Verify migration aliases are preserved
    const legacySpawn = tools.find((tool) => tool.name === "deepseek_spawn");
    const legacySpawnBatch = tools.find((tool) => tool.name === "deepseek_spawn_batch");
    const legacyConsult = tools.find((tool) => tool.name === "deepseek_consult");
    const legacyFollow = tools.find((tool) => tool.name === "deepseek_follow");
    const legacyPark = tools.find((tool) => tool.name === "deepseek_park");
    assert.equal(legacySpawn?.title, "DeepSeek Sub-Agent · Spawn");
    assert.equal(legacySpawnBatch?.title, "DeepSeek Sub-Agent · Spawn Batch");
    assert.equal(legacyConsult?.title, "DeepSeek Sub-Agent · Consult");
    assert.equal(legacyFollow?.title, "DeepSeek Sub-Agent · Follow");
    assert.equal(legacyPark?.title, "DeepSeek Sub-Agent · Park");
    assert.match(legacyPark?.description ?? "", /deepseek_follow/i);
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
    assert.equal(result.tools.length, 18);
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

test("MCP canonical subagents_spawn, subagents_continue, and subagents_status report obligations and provenance", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const consultedArgs: unknown[] = [];
  const bridgeClient = {
    call: async (pathname: string, body?: unknown) => {
      if (pathname === "/v1/jobs/spawn" || pathname === "/v1/jobs/continue") {
        return {
          accepted: true,
          status: "accepted",
          topic: "canonical topic",
          modelDisplayName: "DeepSeek V4 Flash · Max",
          agentId: "agent_sub_1",
          jobId: "job_sub_1",
          state: "Starting",
        };
      }
      if (pathname === "/v1/jobs/consult") {
        consultedArgs.push(body);
        return {
          agentId: "agent_sub_1",
          jobId: "job_sub_1",
          topic: "canonical topic",
          status: "running",
          elapsedSeconds: 5,
          lastActivityAgoSeconds: 2,
          currentActivity: "Running task",
          recentActivity: [],
          filesTouched: [],
          testSummary: "All tests pass",
          resultAvailable: false,
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
    for (const toolName of ["subagents_spawn", "subagents_continue"] as const) {
      const result = await client.callTool({
        name: toolName,
        arguments: toolName === "subagents_spawn"
          ? { topic: "canonical topic", task: "spawn task" }
          : { agent_id: "agent_sub_1", task: "continue task" },
      });
      assert.equal(result.isError, undefined);
      const text = (result.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
      assert.match(text, /SubAgents MCP accepted the task/);
      assert.match(text, /DeepSeek V4 Flash · Max/);
      assert.match(text, /consume.*subagents_follow/);
      const structured = result.structuredContent as Record<string, unknown>;
      assert.equal(structured.obligationState, "pending");
      assert.equal(structured.nextRequiredAction, "subagents_follow");
      assert.equal(structured.accepted, true);
      assert.equal(structured.status, "accepted");
      assert.equal(structured.topic, "canonical topic");
      assert.equal(structured.modelDisplayName, "DeepSeek V4 Flash · Max");
      assert.equal(structured.agentId, "agent_sub_1");
      assert.equal(structured.jobId, "job_sub_1");
    }

    const statusResult = await client.callTool({
      name: "subagents_status",
      arguments: { agent_id: "agent_sub_1", job_id: "job_sub_1" },
    });
    assert.equal(statusResult.isError, undefined);
    assert.deepEqual(consultedArgs, [{ agent_id: "agent_sub_1", job_id: "job_sub_1", activity_limit: 10 }]);
    const statusStructured = statusResult.structuredContent as Record<string, unknown>;
    assert.equal(statusStructured.agentId, "agent_sub_1");
    assert.equal(statusStructured.status, "running");
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

test("MCP canonical subagents_follow terminal and needs_approval results manage obligations", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let followCall = 0;
  const bridgeClient = {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/follow") {
        followCall += 1;
        if (followCall === 1) {
          return {
            agentId: "agent_sub_1",
            jobId: "job_sub_1",
            status: "completed",
            deadlineReached: false,
            gracefulFinalize: false,
            partial: false,
            workerAborted: false,
            resultAvailable: true,
            progress: {
              agentId: "agent_sub_1",
              jobId: "job_sub_1",
              topic: "canonical follow topic",
              status: "completed",
              elapsedSeconds: 10,
              lastActivityAgoSeconds: 1,
              currentActivity: "Done",
              recentActivity: [],
              filesTouched: [],
              testSummary: "Passed",
              resultAvailable: true,
            },
          };
        }
        return {
          agentId: "agent_sub_1",
          jobId: "job_sub_1",
          status: "needs_approval",
          deadlineReached: false,
          gracefulFinalize: false,
          partial: false,
          workerAborted: false,
          resultAvailable: false,
          permissionId: "permission_sub_9",
          message: "SubAgents MCP requires explicit approval before continuing.",
          progress: {
            agentId: "agent_sub_1",
            jobId: "job_sub_1",
            topic: "canonical follow topic",
            status: "needs_approval",
            elapsedSeconds: 5,
            lastActivityAgoSeconds: 1,
            currentActivity: "Waiting",
            recentActivity: [],
            filesTouched: [],
            testSummary: "Pending",
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
    // 1. Terminal result closes obligation
    const termResult = await client.callTool({ name: "subagents_follow", arguments: { agent_id: "agent_sub_1" } });
    assert.equal(termResult.isError, undefined);
    const termText = (termResult.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(termText, /SubAgents MCP follow returned a terminal result/);
    assert.match(termText, /subagents_close/);
    const termStructured = termResult.structuredContent as Record<string, unknown>;
    assert.equal(termStructured.obligationState, "closed");
    assert.equal(termStructured.status, "completed");

    // 2. Needs approval keeps obligation pending and points to subagents_continue
    const approvalResult = await client.callTool({ name: "subagents_follow", arguments: { agent_id: "agent_sub_1" } });
    assert.equal(approvalResult.isError, undefined);
    const approvalText = (approvalResult.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(approvalText, /SubAgents MCP follow requires explicit approval/);
    assert.match(approvalText, /subagents_continue/);
    assert.match(approvalText, /subagents_abort or subagents_close/);
    const approvalStructured = approvalResult.structuredContent as Record<string, unknown>;
    assert.equal(approvalStructured.obligationState, "pending");
    assert.equal(approvalStructured.nextRequiredAction, "subagents_continue");
    assert.equal(approvalStructured.permissionId, "permission_sub_9");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP canonical subagents_abort, subagents_close, and subagents_recover_result end obligations", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  const bridgeClient = {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/abort") {
        return { agentId: "agent_sub_1", jobId: "job_sub_1", status: "aborted" };
      }
      if (pathname === "/v1/jobs/close") {
        return { agentId: "agent_sub_1", status: "closed" };
      }
      if (pathname === "/v1/jobs/recover") {
        return { agentId: "agent_sub_1", jobId: "job_sub_1", summary: "Recovered summary" };
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
    const abortResult = await client.callTool({ name: "subagents_abort", arguments: { agent_id: "agent_sub_1" } });
    assert.equal(abortResult.isError, undefined);
    const abortText = (abortResult.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(abortText, /SubAgents MCP task stopped/);
    const abortStructured = abortResult.structuredContent as Record<string, unknown>;
    assert.equal(abortStructured.obligationState, "closed");
    assert.equal(abortStructured.status, "aborted");

    const closeResult = await client.callTool({ name: "subagents_close", arguments: { agent_id: "agent_sub_1" } });
    assert.equal(closeResult.isError, undefined);
    const closeText = (closeResult.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(closeText, /SubAgents MCP agent closed/);
    const closeStructured = closeResult.structuredContent as Record<string, unknown>;
    assert.equal(closeStructured.obligationState, "closed");
    assert.equal(closeStructured.status, "closed");

    const recoverResult = await client.callTool({ name: "subagents_recover_result", arguments: { agent_id: "agent_sub_1", job_id: "job_sub_1" } });
    assert.equal(recoverResult.isError, undefined);
    const recoverText = (recoverResult.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(recoverText, /Persisted SubAgents MCP result recovered/);
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

    // Canonical tools metadata verification
    const subSpawn = tools.find((tool) => tool.name === "subagents_spawn");
    const subContinue = tools.find((tool) => tool.name === "subagents_continue");
    const subStatus = tools.find((tool) => tool.name === "subagents_status");
    const subFollow = tools.find((tool) => tool.name === "subagents_follow");
    const subAbort = tools.find((tool) => tool.name === "subagents_abort");
    const subClose = tools.find((tool) => tool.name === "subagents_close");

    assert.equal(outputProperties(subSpawn).obligationState?.const, "pending");
    assert.equal(outputProperties(subSpawn).nextRequiredAction?.const, "subagents_follow");
    assert.equal(outputProperties(subContinue).obligationState?.const, "pending");
    assert.equal(outputProperties(subContinue).nextRequiredAction?.const, "subagents_follow");
    assert.equal(subFollow?.annotations?.readOnlyHint, false);
    assert.match(subFollow?.description ?? "", /before a dependent gate or a final response/);
    assert.equal(subStatus?.annotations?.readOnlyHint, true);
    assert.match(subAbort?.description ?? "", /end its pending obligation/);
    assert.match(subClose?.description ?? "", /ending any pending obligation/);
    assert.equal(subAbort?.annotations?.destructiveHint, true);
    assert.equal(subClose?.annotations?.destructiveHint, false);
    assert.equal(subSpawn?.annotations?.readOnlyHint, false);
    assert.equal(subContinue?.annotations?.readOnlyHint, false);

    const subFollowObligation = JSON.stringify(subFollow?.outputSchema?.properties?.obligationState ?? {});
    assert.match(subFollowObligation, /pending/);
    assert.match(subFollowObligation, /closed/);
    const subFollowNextAction = JSON.stringify(subFollow?.outputSchema?.properties?.nextRequiredAction ?? {});
    assert.match(subFollowNextAction, /subagents_continue/);
    assert.equal(outputProperties(subAbort).obligationState?.const, "closed");
    assert.equal(outputProperties(subClose).obligationState?.const, "closed");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP spawn does not expose a model-route override", async () => {
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
    assert.equal("model_route" in spawnProperties, false, "ordinary MCP callers must use the bridge active route");
    assert.equal("model_route" in continueProperties, false, "continuations keep their already-pinned route");
    assert.match(spawn?.description ?? "", /active model route/i);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP drops a legacy model_route argument and dispatches on the active route", async () => {
  let payload: Record<string, unknown> | null = null;
  const bridge = {
    call: async (pathname: string, body: unknown) => {
      assert.equal(pathname, "/v1/jobs/spawn");
      payload = body as Record<string, unknown>;
      return {
        accepted: true,
        status: "accepted",
        topic: "t",
        modelDisplayName: "Antigravity · Gemini 3.8 Flash High",
        agentId: "agent_active_route",
        jobId: "job_active_route",
        state: "Starting",
      };
    },
  } as unknown as BridgeHttpClient;
  const server = createMcpServer(bridge);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const result = await client.callTool({
      name: "deepseek_spawn",
      arguments: { topic: "t", task: "t", model_route: "flash-max" },
    });
    assert.notEqual(result.isError, true);
    assert.ok(payload);
    assert.equal("model_route" in payload, false);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP propagates route_override_denied as a typed 403 with retry false", async () => {
  const failing = {
    call: async (pathname: string) => {
      if (pathname === "/v1/jobs/spawn") {
        throw new BridgeHttpError(403, "route_override_denied", "Model route override denied: only the active route flash-max is selectable at spawn; pro-max is not active", { route: "pro-max", activeRoute: "flash-max" });
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
    const result = await client.callTool({ name: "deepseek_spawn", arguments: { topic: "t", task: "t", model_route: "pro-max" } });
    assert.equal(result.isError, true);
    const structured = result.structuredContent as Record<string, unknown>;
    assert.equal(structured.code, "route_override_denied");
    assert.equal(structured.status, 403);
    assert.equal(structured.retry, false);
    assert.deepEqual(structured.details, { route: "pro-max", activeRoute: "flash-max" });
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

test("MCP post-bootstrap transport failure triggers one recovery and a successful retry", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let ready = true;
  let starts = 0;
  let callAttempts = 0;
  const healthClient = {
    async health(): Promise<unknown> {
      if (!ready) throw new Error("connect ECONNREFUSED");
      return { status: { running: true } };
    },
  };
  const bridgeClient = {
    call: async () => {
      callAttempts += 1;
      if (callAttempts === 1) {
        return { agentId: "agent_1", jobId: "job_1", topic: "t", status: "running", elapsedSeconds: 1, lastActivityAgoSeconds: 1, currentActivity: "Working", recentActivity: [], filesTouched: [], testSummary: "", resultAvailable: false };
      }
      if (callAttempts === 2) {
        ready = false;
        throw new BridgeTransportError("connect ECONNREFUSED");
      }
      return { agentId: "agent_1", jobId: "job_1", topic: "t", status: "running", elapsedSeconds: 2, lastActivityAgoSeconds: 1, currentActivity: "Working", recentActivity: [], filesTouched: [], testSummary: "", resultAvailable: false };
    },
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient, {
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
    const first = await client.callTool({ name: "deepseek_consult", arguments: { agent_id: "agent_1" } });
    assert.equal(first.isError, undefined);
    assert.equal(starts, 0);
    assert.equal(callAttempts, 1);

    const second = await client.callTool({ name: "deepseek_consult", arguments: { agent_id: "agent_1" } });
    assert.equal(second.isError, undefined);
    assert.equal(starts, 1, "recovery must invoke detached start exactly once");
    assert.equal(callAttempts, 3, "must retry the original call once after recovery");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP concurrent post-bootstrap transport failures share one recovery attempt", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let ready = true;
  let starts = 0;
  let callCount = 0;
  const healthClient = {
    async health(): Promise<unknown> {
      if (!ready) throw new Error("connect ECONNREFUSED");
      return { status: { running: true } };
    },
  };
  const bridgeClient = {
    call: async () => {
      callCount += 1;
      if (callCount === 1) {
        return { agentId: "agent_1", jobId: "job_1", topic: "t", status: "running", elapsedSeconds: 1, lastActivityAgoSeconds: 1, currentActivity: "Working", recentActivity: [], filesTouched: [], testSummary: "", resultAvailable: false };
      }
      if (callCount === 2 || callCount === 3) {
        ready = false;
        throw new BridgeTransportError("connect ECONNREFUSED");
      }
      return { agentId: "agent_1", jobId: "job_1", topic: "t", status: "running", elapsedSeconds: 2, lastActivityAgoSeconds: 1, currentActivity: "Working", recentActivity: [], filesTouched: [], testSummary: "", resultAvailable: false };
    },
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient, {
    ensureReady: createLazyDaemonBootstrap(config, healthClient, {
      start: async () => {
        starts += 1;
        await new Promise((resolve) => setTimeout(resolve, 20));
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
    const initial = await client.callTool({ name: "deepseek_consult", arguments: { agent_id: "agent_1" } });
    assert.equal(initial.isError, undefined);
    assert.equal(starts, 0);

    const op1 = client.callTool({ name: "deepseek_consult", arguments: { agent_id: "agent_1" } });
    const op2 = client.callTool({ name: "deepseek_consult", arguments: { agent_id: "agent_1" } });
    const [res1, res2] = await Promise.all([op1, op2]);

    assert.equal(res1.isError, undefined);
    assert.equal(res2.isError, undefined);
    assert.equal(starts, 1, "concurrent transport failures must share a single recovery start");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP non-transport error never triggers recovery", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let starts = 0;
  const healthClient = {
    async health(): Promise<unknown> {
      return { status: { running: true } };
    },
  };
  const bridgeClient = {
    call: async () => {
      throw new BridgeHttpError(404, "unknown_agent", "Agent not found");
    },
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient, {
    ensureReady: createLazyDaemonBootstrap(config, healthClient, {
      start: async () => {
        starts += 1;
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
    const result = await client.callTool({ name: "deepseek_abort", arguments: { agent_id: "nonexistent" } });
    assert.equal(result.isError, true);
    assert.equal((result.structuredContent as Record<string, unknown>)?.code, "unknown_agent");
    assert.equal(starts, 0, "non-transport error must never trigger recovery start");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP spawn and continue retry preserves and reuses the stable request_id across recovery", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let ready = true;
  let starts = 0;
  const recordedSpawnPayloads: Array<Record<string, unknown>> = [];
  const recordedContinuePayloads: Array<Record<string, unknown>> = [];
  const healthClient = {
    async health(): Promise<unknown> {
      if (!ready) throw new Error("connect ECONNREFUSED");
      return { status: { running: true } };
    },
  };
  const bridgeClient = {
    call: async (pathname: string, body?: unknown) => {
      if (pathname === "/v1/jobs/spawn") {
        recordedSpawnPayloads.push(body as Record<string, unknown>);
        if (recordedSpawnPayloads.length === 1) {
          ready = false;
          throw new BridgeTransportError("connect ECONNREFUSED");
        }
        return { accepted: true, status: "accepted", topic: "t", modelDisplayName: "m", agentId: "a1", jobId: "j1", state: "Starting" };
      }
      if (pathname === "/v1/jobs/continue") {
        recordedContinuePayloads.push(body as Record<string, unknown>);
        if (recordedContinuePayloads.length === 1) {
          ready = false;
          throw new BridgeTransportError("connect ECONNREFUSED");
        }
        return { accepted: true, status: "accepted", topic: "t", modelDisplayName: "m", agentId: "a1", jobId: "j2", state: "Starting" };
      }
      throw new Error("Unexpected endpoint: " + pathname);
    },
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient, {
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
    // 1. Spawn without supplied request_id
    const spawnRes = await client.callTool({ name: "deepseek_spawn", arguments: { topic: "topic", task: "task" } });
    assert.equal(spawnRes.isError, undefined);
    assert.equal(recordedSpawnPayloads.length, 2, "spawn should have been called twice (initial + retry)");
    const generatedRequestId = recordedSpawnPayloads[0]?.request_id;
    assert.ok(typeof generatedRequestId === "string" && (generatedRequestId as string).startsWith("request_"));
    assert.equal(recordedSpawnPayloads[1]?.request_id, generatedRequestId, "retry must reuse the exact same generated request_id");

    // 2. Continue with supplied request_id
    const continueRes = await client.callTool({ name: "deepseek_continue", arguments: { agent_id: "a1", task: "task", request_id: "custom_req_999" } });
    assert.equal(continueRes.isError, undefined);
    assert.equal(recordedContinuePayloads.length, 2, "continue should have been called twice (initial + retry)");
    assert.equal(recordedContinuePayloads[0]?.request_id, "custom_req_999");
    assert.equal(recordedContinuePayloads[1]?.request_id, "custom_req_999", "retry must preserve supplied request_id");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP post-bootstrap recovery remains bounded to one retry and propagates persistent failures", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let ready = true;
  let starts = 0;
  let callAttempts = 0;
  const healthClient = {
    async health(): Promise<unknown> {
      if (!ready) throw new Error("connect ECONNREFUSED");
      return { status: { running: true } };
    },
  };
  const bridgeClient = {
    call: async () => {
      callAttempts += 1;
      ready = false;
      throw new BridgeTransportError("connect ECONNREFUSED");
    },
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient, {
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
    const result = await client.callTool({ name: "deepseek_consult", arguments: { agent_id: "agent_1" } });
    assert.equal(result.isError, true);
    assert.equal(starts, 1, "must attempt recovery exactly once");
    assert.equal(callAttempts, 2, "must attempt initial call and exactly one retry (no infinite loop)");
    const text = (result.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(text, /ECONNREFUSED/);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP recovery failure propagates readiness error and resets memo", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let ready = true;
  let starts = 0;
  let callAttempts = 0;
  const healthClient = {
    async health(): Promise<unknown> {
      if (!ready) throw new Error("connect ECONNREFUSED");
      return { status: { running: true } };
    },
  };
  const bridgeClient = {
    call: async () => {
      callAttempts += 1;
      ready = false;
      throw new BridgeTransportError("connect ECONNREFUSED");
    },
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient, {
    ensureReady: createLazyDaemonBootstrap(config, healthClient, {
      start: async () => {
        starts += 1;
        throw new Error("daemon failed to start");
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
    const result = await client.callTool({ name: "deepseek_consult", arguments: { agent_id: "agent_1" } });
    assert.equal(result.isError, true);
    assert.equal(starts, 1);
    assert.equal(callAttempts, 1, "if recovery fails, the retried call is not made");
    const text = (result.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(text, /daemon is not ready/i);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP plain ensureReady callback without recovery propagates transport error without retrying", async () => {
  let readinessCalls = 0;
  let callAttempts = 0;
  const customEnsureReady = async (): Promise<void> => {
    readinessCalls += 1;
  };
  const bridgeClient = {
    call: async () => {
      callAttempts += 1;
      throw new BridgeTransportError("connect ECONNREFUSED");
    },
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient, {
    ensureReady: customEnsureReady,
  });
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const result = await client.callTool({ name: "deepseek_consult", arguments: { agent_id: "agent_1" } });
    assert.equal(result.isError, true);
    assert.equal(readinessCalls, 1, "plain ensureReady must be called only once for initial readiness check");
    assert.equal(callAttempts, 1, "plain ensureReady must not trigger retry on BridgeTransportError");
    const text = (result.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(text, /ECONNREFUSED/);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP bootstrap distinguishes reachable-but-recovering from absent daemon and prevents duplicate start", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let pollCount = 0;
  let starts = 0;
  const healthClient = {
    async health(): Promise<unknown> {
      pollCount += 1;
      if (pollCount <= 2) {
        return { displayName: "DeepSeek Sub-Agent", state: "recovering", ready: false, status: { running: false, state: "recovering", ready: false } };
      }
      return { displayName: "DeepSeek Sub-Agent", state: "ready", ready: true, status: { running: true, state: "ready", ready: true } };
    },
  };

  await ensureDaemonRunning(config, healthClient, {
    start: async () => {
      starts += 1;
    },
    timeoutMs: 500,
    retryMs: 10,
  });

  assert.equal(starts, 0, "must not start a duplicate daemon when health endpoint is reachable in recovering state");
  assert.ok(pollCount >= 3, "must wait boundedly for readiness");
});

test("MCP bootstrap fails immediately when daemon is degraded", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });
  let starts = 0;
  const healthClient = {
    async health(): Promise<unknown> {
      return {
        displayName: "DeepSeek Sub-Agent",
        state: "degraded",
        ready: false,
        error: "Fatal OpenCode startup error",
        status: { running: false, state: "degraded", ready: false, error: "Fatal OpenCode startup error" },
      };
    },
  };

  await assert.rejects(
    () => ensureDaemonRunning(config, healthClient, {
      start: async () => {
        starts += 1;
      },
      timeoutMs: 500,
      retryMs: 10,
    }),
    /degraded.*Fatal OpenCode startup error/,
  );
  assert.equal(starts, 0, "must not attempt to restart a degraded daemon");
});

test("MCP subagents_follow and deepseek_follow expose receipt, earlyExit, escalation, semanticProgress in outputSchema and compact summary", async () => {
  const mockReceipt = {
    jobId: "job_sub_1",
    agentId: "agent_sub_1",
    provider: "antigravity",
    model: "gemini-3.8-flash-high",
    status: "completed",
    workspace: "C:\\work",
    startedAt: "2026-09-02T10:00:00.000Z",
    completedAt: "2026-09-02T10:00:00.150Z",
    durationMs: 150,
    attempt: "attempt_1",
    fence: 1,
    outputHash: "hash123",
    quiescent: true,
    earlyExit: true,
    filesCount: 3,
    testsCount: 2,
  };
  const mockEarlyExit = {
    triggered: true,
    reason: "goal_reached_early",
    confidence: "high",
    evidenceSnippet: "all tests passed",
    signaledAt: "2026-09-02T10:00:00.150Z",
  };
  const mockEscalation = {
    reason: "complexity exceeded",
    recommendedRoute: "pro-max",
    targetRole: "Senior Architect",
    advisoryOnly: true,
  };
  const mockSemanticProgress = {
    stage: "verification",
    percent: 90,
    summary: "Verifying tests",
  };

  const bridgeClient = {
    call: async (pathname: string, body?: any) => {
      if (pathname === "/v1/jobs/follow") {
        if (body?.agent_id === "agent_approval") {
          return {
            agentId: "agent_approval",
            jobId: "job_app_1",
            status: "needs_approval",
            resultAvailable: false,
            permissionId: "perm_1",
            message: "Need permission to edit files",
            escalation: mockEscalation,
            semanticProgress: mockSemanticProgress,
          };
        }
        return {
          agentId: "agent_sub_1",
          jobId: "job_sub_1",
          status: "completed",
          resultAvailable: true,
          receipt: mockReceipt,
          earlyExit: mockEarlyExit,
          escalation: mockEscalation,
          semanticProgress: mockSemanticProgress,
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
    // 1. Verify tools list includes outputSchema with optional adaptive fields
    const { tools } = await client.listTools();
    const subFollow = tools.find((t) => t.name === "subagents_follow");
    const dsFollow = tools.find((t) => t.name === "deepseek_follow");

    assert.ok(subFollow?.outputSchema, "subagents_follow must have outputSchema");
    assert.ok(dsFollow?.outputSchema, "deepseek_follow must have outputSchema");

    const subProperties = (subFollow.outputSchema as any).properties;
    assert.ok(subProperties.receipt, "subagents_follow outputSchema must have receipt");
    assert.ok(subProperties.earlyExit, "subagents_follow outputSchema must have earlyExit");
    assert.ok(subProperties.escalation, "subagents_follow outputSchema must have escalation");
    assert.ok(subProperties.semanticProgress, "subagents_follow outputSchema must have semanticProgress");

    const dsProperties = (dsFollow.outputSchema as any).properties;
    assert.ok(dsProperties.receipt, "deepseek_follow outputSchema must have receipt");
    assert.ok(dsProperties.earlyExit, "deepseek_follow outputSchema must have earlyExit");
    assert.ok(dsProperties.escalation, "deepseek_follow outputSchema must have escalation");
    assert.ok(dsProperties.semanticProgress, "deepseek_follow outputSchema must have semanticProgress");

    // 2. subagents_follow terminal result includes compact summary and structured fields
    const subRes = await client.callTool({ name: "subagents_follow", arguments: { agent_id: "agent_sub_1" } });
    assert.equal(subRes.isError, undefined);
    const subText = (subRes.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";

    assert.match(subText, /SubAgents MCP follow returned a terminal result/);
    assert.match(subText, /\[receipt: completed \(150ms\) \| earlyExit: goal_reached_early \| escalation: complexity exceeded -> pro-max \| stage: verification\]/);

    const subStruct = subRes.structuredContent as Record<string, unknown>;
    assert.equal(subStruct.obligationState, "closed");
    assert.deepEqual(subStruct.receipt, mockReceipt);
    assert.deepEqual(subStruct.earlyExit, mockEarlyExit);
    assert.deepEqual(subStruct.escalation, mockEscalation);
    assert.deepEqual(subStruct.semanticProgress, mockSemanticProgress);

    // 3. deepseek_follow (alias) includes compact summary and preserves legacy naming
    const dsRes = await client.callTool({ name: "deepseek_follow", arguments: { agent_id: "agent_sub_1" } });
    assert.equal(dsRes.isError, undefined);
    const dsText = (dsRes.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";

    assert.match(dsText, /DeepSeek Sub-Agent follow returned a terminal result/);
    assert.match(dsText, /DeepSeek agent itself remains open and continuable\. Close it with deepseek_close after reviewing the result\./);
    assert.match(dsText, /\[receipt: completed \(150ms\) \| earlyExit: goal_reached_early \| escalation: complexity exceeded -> pro-max \| stage: verification\]/);

    const dsStruct = dsRes.structuredContent as Record<string, unknown>;
    assert.equal(dsStruct.obligationState, "closed");
    assert.deepEqual(dsStruct.receipt, mockReceipt);
    assert.deepEqual(dsStruct.earlyExit, mockEarlyExit);
    assert.deepEqual(dsStruct.escalation, mockEscalation);
    assert.deepEqual(dsStruct.semanticProgress, mockSemanticProgress);

    // 4. needs_approval follow includes compact summary without breaking nextRequiredAction
    const appRes = await client.callTool({ name: "subagents_follow", arguments: { agent_id: "agent_approval" } });
    const appText = (appRes.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";

    assert.match(appText, /SubAgents MCP follow requires explicit approval before continuing/);
    assert.match(appText, /subagents_continue/);
    assert.match(appText, /\[escalation: complexity exceeded -> pro-max \| stage: verification\]/);

    const appStruct = appRes.structuredContent as Record<string, unknown>;
    assert.equal(appStruct.obligationState, "pending");
    assert.equal(appStruct.nextRequiredAction, "subagents_continue");
    assert.deepEqual(appStruct.escalation, mockEscalation);
    assert.deepEqual(appStruct.semanticProgress, mockSemanticProgress);
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP subagents_park and deepseek_park park turn, preserve pending obligation, and guide to follow", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\\\deepseek-test-data",
    configPath: "C:\\\\deepseek-test-data\\\\config.json",
  });

  const calls: Array<{ pathname: string; body: unknown }> = [];
  const bridgeClient = {
    call: async (pathname: string, body?: unknown) => {
      calls.push({ pathname, body });
      if (pathname === "/v1/jobs/park") {
        const value = body as Record<string, unknown>;
        const isAlias = Boolean(value.is_alias);
        return {
          parkId: "park_100",
          generation: 1,
          armed: true,
          targetIdentity: "thread_authoritative",
          obligationState: "pending",
          nextAction: isAlias ? "deepseek_follow" : "subagents_follow",
          jobIds: value.job_ids ?? ["job_100"],
          reason: value.reason ?? null,
          pendingCount: 1,
          readyCount: 0,
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
    // 1. subagents_park
    const subRes = await client.callTool({
      name: "subagents_park",
      arguments: { job_ids: ["job_100"], reason: "waiting for worker" },
    });
    assert.equal(subRes.isError, undefined);
    const subText = (subRes.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(subText, /SubAgents MCP parked turn on barrier park_100/);
    assert.match(subText, /subagents_follow/);

    const subStruct = subRes.structuredContent as Record<string, unknown>;
    assert.equal(subStruct.parkId, "park_100");
    assert.equal(subStruct.armed, true);
    assert.equal(subStruct.obligationState, "pending");
    assert.equal(subStruct.nextRequiredAction, "subagents_follow");

    // 2. deepseek_park (alias)
    const dsRes = await client.callTool({
      name: "deepseek_park",
      arguments: { job_ids: ["job_100"] },
    });
    assert.equal(dsRes.isError, undefined);
    const dsText = (dsRes.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(dsText, /DeepSeek Sub-Agent parked turn on barrier park_100/);
    assert.match(dsText, /deepseek_follow/);

    const dsStruct = dsRes.structuredContent as Record<string, unknown>;
    assert.equal(dsStruct.parkId, "park_100");
    assert.equal(dsStruct.armed, true);
    assert.equal(dsStruct.obligationState, "pending");
    assert.equal(dsStruct.nextRequiredAction, "deepseek_follow");
  } finally {
    await client.close();
    await server.close();
  }
});

test("MCP subagents_spawn_batch and deepseek_spawn_batch validate schema, dispatch batch and return pending obligation", async () => {
  let lastCall: { pathname: string; body: unknown } | null = null;
  const bridgeClient = {
    call: async (pathname: string, body?: unknown) => {
      lastCall = { pathname, body };
      if (pathname === "/v1/jobs/spawn-batch") {
        return {
          accepted: true,
          batchId: "batch_42",
          batchRequestId: "batch_req_42",
          items: [
            {
              jobId: "job_b1",
              agentId: "agent_b1",
              requestId: "req_b1",
              status: "accepted",
            },
            {
              jobId: "job_b2",
              agentId: "agent_b2",
              requestId: "req_b2",
              status: "queued",
            },
          ],
        };
      }
      throw new Error("Unexpected pathname: " + pathname);
    },
  };
  const server = createMcpServer(bridgeClient as unknown as BridgeHttpClient);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);

  try {
    // 1. Canonical subagents_spawn_batch
    const subRes = await client.callTool({
      name: "subagents_spawn_batch",
      arguments: {
        batch_request_id: "batch_req_custom",
        items: [
          { topic: "Topic 1", task: "Task 1", priority: 80, exclusive_resources: ["res-a"] },
          { topic: "Topic 2", task: "Task 2", priority: 40 },
        ],
      },
    });
    assert.equal(subRes.isError, undefined);
    assert.equal(lastCall?.pathname, "/v1/jobs/spawn-batch");
    const subText = (subRes.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(subText, /SubAgents MCP accepted batch batch_42 \(2 items\)/);
    assert.match(subText, /Jobs: job_b1, job_b2/);
    assert.match(subText, /Follow each job/);

    const subStruct = subRes.structuredContent as Record<string, unknown>;
    assert.equal(subStruct.accepted, true);
    assert.equal(subStruct.batchId, "batch_42");
    assert.equal(subStruct.batchRequestId, "batch_req_42");
    assert.deepEqual(subStruct.jobIds, ["job_b1", "job_b2"]);
    assert.equal(subStruct.obligationState, "pending");
    assert.equal(subStruct.nextRequiredAction, "subagents_follow");

    // 2. Migration alias deepseek_spawn_batch
    const dsRes = await client.callTool({
      name: "deepseek_spawn_batch",
      arguments: {
        batch_request_id: "batch_req_custom_2",
        items: [
          { topic: "Legacy 1", task: "Legacy task 1" },
        ],
      },
    });
    assert.equal(dsRes.isError, undefined);
    const dsText = (dsRes.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(dsText, /DeepSeek Sub-Agent accepted batch batch_42/);
    assert.match(dsText, /Follow each job/);

    const dsStruct = dsRes.structuredContent as Record<string, unknown>;
    assert.equal(dsStruct.accepted, true);
    assert.equal(dsStruct.batchId, "batch_42");
    assert.equal(dsStruct.obligationState, "pending");
    assert.equal(dsStruct.nextRequiredAction, "deepseek_follow");
  } finally {
    await client.close();
    await server.close();
  }
});

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

test("HTTP endpoint /v1/jobs/spawn-batch handles batch requests and returns accepted receipts", async () => {
  let capturedInput: unknown = null;
  const service = {
    isReady: () => true,
    status: () => ({ state: "ready", ready: true }),
    spawnBatch: async (input: unknown) => {
      capturedInput = input;
      return {
        accepted: true,
        batchId: "http_batch_123",
        batchRequestId: "req_http_b1",
        items: [
          {
            jobId: "job_h1",
            agentId: "agent_h1",
            requestId: "r1",
            status: "accepted",
          },
          {
            jobId: "job_h2",
            agentId: "agent_h2",
            requestId: "r2",
            status: "queued",
          },
        ],
      };
    },
  } as unknown as BridgeService;

  const port = await freePort();
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: port,
    daemonToken: "test-token",
    dataDir: "C:\\test-data",
    configPath: "C:\\test-data\\config.json",
  });
  const server = new BridgeHttpServer(config, service);
  await server.start();
  const client = new BridgeHttpClient(config);

  try {
    const res = await client.call("/v1/jobs/spawn-batch", {
      batch_request_id: "req_http_b1",
      items: [
        { topic: "Topic 1", task: "Task 1", priority: 80, exclusive_resources: ["gpu"] },
        { topic: "Topic 2", task: "Task 2", priority: 20 },
      ],
    });

    assert.equal(res.accepted, true);
    assert.equal(res.batchId, "http_batch_123");
    assert.equal(res.batchRequestId, "req_http_b1");
    assert.equal(Array.isArray(res.items), true);
    assert.equal((res.items as unknown[]).length, 2);

    const typedInput = capturedInput as { batchRequestId?: string; items: Array<{ priority?: number; exclusiveResources?: string[] }> };
    assert.equal(typedInput.batchRequestId, "req_http_b1");
    assert.equal(typedInput.items[0]?.priority, 80);
    assert.deepEqual(typedInput.items[0]?.exclusiveResources, ["gpu"]);
    assert.equal(typedInput.items[1]?.priority, 20);
  } finally {
    await server.stop();
  }
});

test("TranscriptAttestor attests each individual job inside items[] from batch spawn tools", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-batch-tools-"));
  try {
    const sessionsDir = path.join(tmpDir, "sessions");
    await mkdir(sessionsDir, { recursive: true });

    const job1 = "job_batch_item_1";
    const job2 = "job_batch_item_2";
    const threadId = "22222222-3333-4444-8555-666666666666";
    const turnId = "77777777-8888-4999-9aaa-bbbbbbbbbbbb";

    const transcriptLines = [
      JSON.stringify({
        type: "event_msg",
        payload: {
          type: "item_completed",
          thread_id: threadId,
          turn_id: turnId,
          item: {
            id: "tool_batch_call_1",
            type: "mcptoolcall",
            status: "completed",
            server: "subagents",
            tool: "subagents_spawn_batch",
            result: {
              structuredContent: {
                accepted: true,
                batchId: "batch_mcp_attest",
                items: [
                  { jobId: job1, agentId: "agent_1", status: "accepted" },
                  { jobId: job2, agentId: "agent_2", status: "queued" },
                ],
              },
            },
          },
        },
      }),
    ];

    await writeFile(path.join(sessionsDir, "transcript.jsonl"), transcriptLines.join("\n") + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir });
    const match1 = await attestor.attestJob(job1);
    assert.ok(match1, "Must attest job1 inside items[]");
    assert.equal(match1.jobId, job1);
    assert.equal(match1.threadId, threadId);
    assert.equal(match1.turnId, turnId);

    const match2 = await attestor.attestJob(job2);
    assert.ok(match2, "Must attest job2 inside items[]");
    assert.equal(match2.jobId, job2);
    assert.equal(match2.threadId, threadId);
    assert.equal(match2.turnId, turnId);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("MCP subagents_spawn_batch accepts mode=test and unary subagents_spawn accepts priority/exclusive_resources", async () => {
  let lastCall: { pathname: string; body: unknown } | null = null;
  const bridgeClient = {
    call: async (pathname: string, body?: unknown) => {
      lastCall = { pathname, body };
      return { accepted: true, status: "accepted", topic: "Topic Unary Prio", agentId: "a1", jobId: "j1", batchId: "b1", items: [{ jobId: "j1", agentId: "a1", status: "accepted" }] };
    },
  };
  const server = createMcpServer(bridgeClient as unknown as BridgeHttpClient);
  const client = new Client({ name: "fixture-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);

  try {
    // 1. Verify subagents_spawn_batch with mode="test":
    const batchRes = await client.callTool({
      name: "subagents_spawn_batch",
      arguments: {
        batch_request_id: "audit_batch_mode_test",
        items: [
          { topic: "Topic Test", task: "Task Test", mode: "test" },
        ],
      },
    });
    assert.equal(batchRes.isError, undefined, "subagents_spawn_batch must accept mode='test'");
    assert.equal(lastCall?.pathname, "/v1/jobs/spawn-batch");

    // 2. Verify subagents_spawn with priority and exclusive_resources:
    const unaryRes = await client.callTool({
      name: "subagents_spawn",
      arguments: {
        topic: "Topic Unary Prio",
        task: "Task Unary Prio",
        priority: 85,
        exclusive_resources: ["gpu"],
      },
    });
    assert.equal(unaryRes.isError, undefined, "subagents_spawn must accept priority and exclusive_resources");
    assert.equal(lastCall?.pathname, "/v1/jobs/spawn");
    const unaryBody = (lastCall?.body ?? {}) as Record<string, unknown>;
    assert.equal(unaryBody.priority, 85);
    assert.deepEqual(unaryBody.exclusive_resources, ["gpu"]);
  } finally {
    await client.close();
    await server.close();
  }
});
