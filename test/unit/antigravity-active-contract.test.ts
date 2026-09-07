import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createDefaultConfig } from "../../src/config.js";
import { runRouteCommand } from "../../src/cli.js";
import { InboxDelivery } from "../../src/delivery/inbox.js";
import { createMcpServer } from "../../src/mcp.js";
import { buildWorkerPrompt } from "../../src/prompts.js";
import { formatHumanResult, persistAntigravityResult } from "../../src/result.js";
import { normalizeTitle } from "../../src/security.js";
import type { AntigravityRunResult } from "../../src/antigravity/types.js";
import type { AgentRecord, JobRecord, ResultEnvelope } from "../../src/types.js";

const CANONICAL_TOOLS = [
  "subagents_spawn",
  "subagents_spawn_batch",
  "subagents_continue",
  "subagents_status",
  "subagents_follow",
  "subagents_park",
  "subagents_abort",
  "subagents_close",
  "subagents_recover_result",
];

const ALIAS_TOOLS = [
  "deepseek_spawn",
  "deepseek_spawn_batch",
  "deepseek_continue",
  "deepseek_consult",
  "deepseek_follow",
  "deepseek_park",
  "deepseek_abort",
  "deepseek_close",
  "deepseek_recover_result",
];

function fakeBridge(calls: string[] = []): any {
  return {
    call: async (pathname: string) => {
      calls.push(pathname);
      return {
        accepted: true,
        jobId: "job_alias_1",
        agentId: "agent_alias_1",
        topic: "contract test",
        modelDisplayName: "Antigravity · gemini-3.8-flash-high",
        modelProviderId: "antigravity",
      };
    },
  };
}

function fixtureAgent(id: string): AgentRecord {
  return {
    id,
    title: "Antigravity agent",
    topic: "Antigravity contract",
    repositoryRoot: "C:\\repo",
    workspacePath: "C:\\repo",
    workspaceStrategy: "shared",
    opencodeServerId: "legacy-server",
    opencodeSessionId: "antigravity:" + id,
    modelProviderId: "antigravity",
    modelId: "gemini-3.8-flash-high",
    modelVariant: null,
    modelRoute: "antigravity-flash-high",
    parentAgentId: null,
    status: "working",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    closedAt: null,
    lastError: null,
  };
}

function fixtureJob(id: string, agentId: string): JobRecord {
  return {
    id,
    agentId,
    sequence: 1,
    kind: "spawn",
    requestId: "request_" + id,
    promptHash: "hash_" + id,
    status: "running",
    createdAt: new Date().toISOString(),
    startedAt: new Date().toISOString(),
    completedAt: null,
    lastUserMessageId: null,
    lastAssistantMessageId: null,
    permissionId: null,
    resultPath: null,
    resultSummary: null,
    error: null,
    followStartedAt: null,
    followDeadlineAt: null,
    followGraceMinutes: null,
    graceDeadlineAt: null,
    gracefulFinalizeAttempted: false,
    approvalDeadlineAt: null,
    hintThreadId: null,
    hintTurnId: null,
    hintSource: null,
    dispatchUnknown: false,
    resultConsumedAt: null,
  };
}

function fixtureEnvelope(): ResultEnvelope {
  return {
    version: 1,
    agentId: "agent_inbox",
    jobId: "job_inbox",
    topic: "Inbox contract",
    status: "completed",
    opencodeSessionId: "antigravity:agent_inbox",
    model: "gemini-3.8-flash-high",
    modelDisplayName: "Antigravity · gemini-3.8-flash-high",
    workspace: "C:\\repo",
    summary: "A Gemini result",
    files: [],
    tests: [],
    risks: [],
    diffSummary: "none",
    fullResultPath: "C:\\results\\job_inbox.json",
    orchestratorInstruction: "Continue with subagents_continue after reviewing this result.",
    receipt: {
      jobId: "job_inbox",
      agentId: "agent_inbox",
      provider: "antigravity",
      model: "gemini-3.8-flash-high",
      status: "completed",
      workspace: "C:\\repo",
      startedAt: null,
      completedAt: new Date().toISOString(),
      durationMs: null,
      attempt: null,
      fence: null,
      outputHash: "hash",
      quiescent: true,
      earlyExit: false,
      filesCount: 0,
      testsCount: 0,
    },
  };
}

test("MCP exposes canonical subagents_* surface and deepseek_* migration aliases without reaching legacy provider", async () => {
  const calls: Array<{ pathname: string; body?: unknown }> = [];
  const server = createMcpServer({
    call: async (pathname: string, body?: unknown) => {
      calls.push({ pathname, body });
      return {
        accepted: true,
        jobId: "job_alias_1",
        agentId: "agent_alias_1",
        topic: "contract test",
        modelDisplayName: "Antigravity · gemini-3.8-flash-high",
        modelProviderId: "antigravity",
      };
    },
  } as any);
  const client = new Client({ name: "contract-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const listed = await client.listTools();
    const toolNames = listed.tools.map((tool) => tool.name);
    assert.deepEqual(toolNames.slice(0, CANONICAL_TOOLS.length), CANONICAL_TOOLS, "canonical tools list must be correct and primary");
    assert.deepEqual(toolNames.slice(CANONICAL_TOOLS.length), ALIAS_TOOLS, "migration aliases must exist and be registered");

    const canonicalTools = listed.tools.filter((tool) => tool.name.startsWith("subagents_"));
    assert.doesNotMatch(canonicalTools.map((tool) => tool.description ?? "").join("\n"), /DeepSeek|OpenCode/i);

    const legacyResult = await client.callTool({ name: "deepseek_spawn", arguments: { topic: "legacy", task: "transparent dispatch" } });
    assert.equal(legacyResult.isError, undefined, "migration alias must be accepted");
    assert.equal(calls[0]?.pathname, "/v1/jobs/spawn", "migration alias must map to canonical bridge endpoint");
    const meta = (legacyResult as any)._meta?.technical;
    assert.equal(meta?.provider, "antigravity", "must execute on active Antigravity provider rather than legacy provider");

    const unknownResult = await client.callTool({ name: "unknown_legacy_tool", arguments: { topic: "legacy", task: "must fail" } });
    assert.equal(unknownResult.isError, true, "truly unknown tools must fail closed");
  } finally {
    await client.close();
    await server.close();
  }
});

test("worker prompts identify the active Antigravity Gemini worker without legacy provider wording", async () => {
  const prompt = await buildWorkerPrompt(
    { task: "Inspect the fixture", mode: "analyze", workspaceStrategy: "shared" } as never,
    "E:\\work",
    { maxLength: 100_000 },
  );
  assert.match(prompt, /Antigravity/i);
  assert.match(prompt, /Gemini/i);
  assert.doesNotMatch(prompt, /DeepSeek|OpenCode/i);
});

test("active Antigravity result formatting uses canonical recovery and identity", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "antigravity-active-contract-"));
  try {
    const agent = fixtureAgent("agent_result_contract");
    const job = fixtureJob("job_result_contract", agent.id);
    const result: AntigravityRunResult = {
      status: "completed",
      runId: "run_contract",
      summary: "Gemini completed the task",
      fullText: "SUMMARY: Gemini completed the task",
      files: [],
      tests: [],
      risks: [],
      diffSummary: "none",
      model: "gemini-3.8-flash-high",
      modelDisplayName: "Antigravity · gemini-3.8-flash-high",
      workspace: directory,
      rawOutput: "SUMMARY: Gemini completed the task",
    };
    const stored = await persistAntigravityResult(directory, agent, job, result, 100_000);
    assert.equal(stored.envelope.receipt?.provider, "antigravity");
    assert.match(stored.envelope.orchestratorInstruction, /subagents_continue/);
    const human = formatHumanResult(stored.envelope);
    assert.match(human, /^Antigravity Sub-Agent · /);
    assert.match(human, /\[ANTIGRAVITY_SUBAGENT_RESULT v1\]/);
    assert.match(human, /subagents_recover_result/);
    assert.doesNotMatch(human, /DeepSeek|OpenCode|deepseek_/i);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("inbox notifications use the active Antigravity identity", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "antigravity-inbox-contract-"));
  try {
    const notifications: Array<[string, string]> = [];
    const delivery = new InboxDelivery(directory, async (title, message) => {
      notifications.push([title, message]);
    });
    await delivery.deliver(fixtureEnvelope(), "Antigravity result");
    assert.equal(notifications[0]?.[0], "Antigravity Sub-Agent");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("empty task titles use the active worker identity", () => {
  assert.equal(normalizeTitle("!!!"), "Antigravity Task");
});

test("CLI rejects legacy route mutation before any bridge call", async () => {
  const calls: string[] = [];
  const config = createDefaultConfig({
    dataDir: "C:\\antigravity-contract-data",
    configPath: "C:\\antigravity-contract-data\\config.json",
  });
  await assert.rejects(
    () => runRouteCommand(config, "set", "flash-max", true, fakeBridge(calls)),
    /legacy|Antigravity|unsupported/i,
  );
  assert.deepEqual(calls, [], "legacy route rejection must be fail-closed and side-effect free");
});
