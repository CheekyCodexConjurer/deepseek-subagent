import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { formatHumanResult, persistResult, sanitizePersistedResult } from "../../src/result.js";
import type { AgentRecord, JobRecord, ResultEnvelope } from "../../src/types.js";

test("human result starts with the friendly identity and includes the private report path", () => {
  const envelope: ResultEnvelope = {
    version: 1,
    agentId: "agent_internal",
    jobId: "job_internal",
    topic: "WebSocket Reconnect",
    status: "completed",
    opencodeSessionId: "session_internal",
    model: "opencode-go/deepseek-v4-flash · max",
    modelDisplayName: "DeepSeek V4 Flash · Max",
    workspace: "E:\\workspace",
    summary: "Found the reconnect race.",
    files: ["src/client.ts"],
    tests: ["npm test"],
    risks: ["none"],
    diffSummary: "src/client.ts +4 -1",
    fullResultPath: "C:\\Users\\mathe\\AppData\\Local\\DeepSeek Sub-Agent\\results\\job_internal.json",
    orchestratorInstruction: "Continue this agent only with deepseek_continue after reviewing this result.",
  };
  const text = formatHumanResult(envelope);
  assert.match(text, /^DeepSeek Sub-Agent · WebSocket Reconnect · Completed/);
  assert.match(text, /\[OPENCODE_SUBAGENT_RESULT v1\]/);
  assert.match(text, /FULL RESULT\nC:\\Users\\mathe\\AppData\\Local\\DeepSeek Sub-Agent\\results\\job_internal\.json/);
  assert.match(text, /TECHNICAL METADATA\nagent_id: agent_internal\njob_id: job_internal\nopencode_session_id: session_internal/);
  assert.doesNotMatch(text.split("\n", 3).join("\n"), /agent_internal|job_internal|session_internal/);
});

test("persisted result redacts structured and raw evidence fields", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-result-redaction-"));
  const agent = {
    id: "agent_redaction",
    title: "Redaction",
    topic: "Redaction token=secret-topic",
    repositoryRoot: directory,
    workspacePath: directory,
    workspaceStrategy: "shared",
    opencodeServerId: "server_redaction",
    opencodeSessionId: "session_redaction",
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    status: "working",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    closedAt: null,
    lastError: null,
  } as AgentRecord;
  const job = {
    id: "job_redaction",
    agentId: agent.id,
    sequence: 1,
    kind: "spawn",
    requestId: "request_redaction",
    promptHash: "hash",
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
    graceDeadlineAt: null,
    gracefulFinalizeAttempted: false,
  } as JobRecord;
  try {
    const stored = await persistResult(directory, agent, job, [{
      info: { id: "assistant_redaction", role: "assistant", sessionID: agent.opencodeSessionId, token: "secret-message" },
      parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: token=secret-summary\nFILES:\n- apiKey=secret-file" }],
    }], { authorization: "Bearer secret-diff", path: "src/index.ts" }, 20_000);
    const content = await readFile(stored.resultPath, "utf8");
    assert.doesNotMatch(content, /secret-(?:topic|message|summary|file|diff)/);
    assert.match(content, /\[REDACTED\]/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("persisted recovery projects only visible text and excludes reasoning and tool parts", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-result-reasoning-"));
  const agent = {
    id: "agent_reasoning",
    title: "Reasoning",
    topic: "Reasoning projection",
    repositoryRoot: directory,
    workspacePath: directory,
    workspaceStrategy: "shared",
    opencodeServerId: "server_reasoning",
    opencodeSessionId: "session_reasoning",
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    status: "working",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    closedAt: null,
    lastError: null,
  } as AgentRecord;
  const job = {
    id: "job_reasoning",
    agentId: agent.id,
    sequence: 1,
    kind: "spawn",
    requestId: "request_reasoning",
    promptHash: "hash",
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
  } as JobRecord;
  try {
    const stored = await persistResult(directory, agent, job, [{
      info: { id: "assistant_reasoning", role: "assistant", sessionID: agent.opencodeSessionId },
      parts: [
        { type: "reasoning", text: "private chain of thought must not persist" },
        { type: "tool", text: "hidden tool payload must not persist" },
        { type: "text", text: "STATUS: completed\nSUMMARY: visible result" },
      ],
    }], [], 20_000);
    const content = await readFile(stored.resultPath, "utf8");
    assert.doesNotMatch(content, /private chain of thought|hidden tool payload/);
    assert.match(content, /visible result/);
    assert.match(content, /"type": "text"/);
    assert.doesNotMatch(content, /"type": "reasoning"|"type": "tool"/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("legacy raw assistant text is omitted without non-empty visible parts", () => {
  for (const messages of [[], [{ parts: [] }]]) {
    const recovered = sanitizePersistedResult({ rawAssistantText: "legacy private text", messages });
    assert.doesNotMatch(JSON.stringify(recovered), /legacy private text/);
  }
});
