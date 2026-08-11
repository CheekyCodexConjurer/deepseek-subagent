import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { assistantTextAfterBaseline, formatHumanResult, persistResult, sanitizePersistedResult } from "../../src/result.js";
import type { AgentRecord, JobRecord, OpenCodeMessage, ResultEnvelope } from "../../src/types.js";

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

function fixtureAgent(agentId: string): AgentRecord {
  return {
    id: agentId,
    title: agentId,
    topic: "Fixture",
    repositoryRoot: "C:\\work",
    workspacePath: "C:\\work",
    workspaceStrategy: "shared",
    opencodeServerId: "server_fixture",
    opencodeSessionId: "session_fixture",
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    status: "working",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    closedAt: null,
    lastError: null,
  };
}

function fixtureJob(jobId: string, baseline: string | null): JobRecord {
  return {
    id: jobId,
    agentId: "agent_fixture",
    sequence: 1,
    kind: baseline ? "continue" : "spawn",
    requestId: "request_fixture_" + jobId,
    promptHash: "hash",
    status: "running",
    createdAt: new Date().toISOString(),
    startedAt: new Date().toISOString(),
    completedAt: null,
    lastUserMessageId: null,
    lastAssistantMessageId: baseline,
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
  };
}

test("multiline SUMMARY parses without leaking output from a prior continuation", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-result-summary-"));
  try {
    const stored = await persistResult(directory, fixtureAgent("agent_summary"), fixtureJob("job_summary", "baseline_summary"), [
      {
        info: { id: "baseline_summary", role: "assistant", sessionID: "session_fixture" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: output from the prior continuation\nFILES:\n- old.ts" }],
      },
      {
        info: { id: "current_summary", role: "assistant", sessionID: "session_fixture" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: first line\ncontinued on the second line\nand a third line\nFILES:\n- new.ts" }],
      },
    ], [], 20_000);
    assert.equal(stored.envelope.summary, "first line continued on the second line and a third line");
    assert.doesNotMatch(stored.envelope.summary, /prior continuation/);
    assert.deepEqual(stored.envelope.files, ["new.ts"]);
    assert.equal(stored.parsed.hasText, true);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("multiline heading termination recognizes only protocol headings; NOTE inside a summary stays", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-result-note-"));
  try {
    const stored = await persistResult(directory, fixtureAgent("agent_note"), fixtureJob("job_note", null), [
      {
        info: { id: "assistant_note", role: "assistant", sessionID: "session_fixture" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: first line\nNOTE: keep this inside the summary\nlast line\nFILES:\n- src/note.ts\nTESTS:\n- unit smoke" }],
      },
    ], [], 20_000);
    assert.equal(stored.envelope.summary, "first line NOTE: keep this inside the summary last line");
    assert.deepEqual(stored.envelope.files, ["src/note.ts"]);
    assert.deepEqual(stored.envelope.tests, ["unit smoke"]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a missing continuation baseline fails closed without leaking earlier turns", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-result-missing-baseline-"));
  try {
    const stored = await persistResult(directory, fixtureAgent("agent_missing_baseline"), fixtureJob("job_missing_baseline", "baseline_never_present"), [
      {
        info: { id: "prior_turn", role: "assistant", sessionID: "session_fixture" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: earlier continuation output\nFILES:\n- old.ts" }],
      },
    ], [], 20_000);
    assert.equal(stored.parsed.hasText, false, "a non-null baseline that is not found must yield no relevant text");
    assert.doesNotMatch(stored.envelope.summary, /earlier continuation output|old\.ts/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("STATUS resolves from the first trimmed token and survives explanation lines", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-result-status-token-"));
  try {
    for (const [statusText, expected] of [
      ["STATUS: failed\nbecause the build broke\nSUMMARY: s", "failed"],
      ["STATUS: aborted\nby the user after review\nSUMMARY: s", "aborted"],
      ["STATUS: completed\nwith caveats noted below\nSUMMARY: s", "completed"],
    ] as const) {
      const stored = await persistResult(directory, fixtureAgent("agent_status_" + expected), fixtureJob("job_status_" + expected, null), [
        {
          info: { id: "assistant_status_" + expected, role: "assistant", sessionID: "session_fixture" },
          parts: [{ type: "text", text: statusText }],
        },
      ], [], 20_000);
      assert.equal(stored.envelope.status, expected, "STATUS must compare the first token only");
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("the latest protocol report wins over an intermediate milestone report", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-result-milestone-"));
  try {
    const milestoneAndFinal = [
      "STATUS: completed",
      "SUMMARY: milestone progress report",
      "FILES:",
      "- src/mid.ts",
      "STATUS: failed",
      "SUMMARY: final report after the follow-up failure",
      "FILES:",
      "- src/final.ts",
      "RISKS:",
      "- final risk",
    ].join("\n");
    const stored = await persistResult(directory, fixtureAgent("agent_milestone"), fixtureJob("job_milestone", null), [
      {
        info: { id: "assistant_milestone", role: "assistant", sessionID: "session_fixture" },
        parts: [{ type: "text", text: milestoneAndFinal }],
      },
    ], [], 20_000);
    assert.equal(stored.envelope.status, "failed", "the final STATUS must win");
    assert.equal(stored.envelope.summary, "final report after the follow-up failure", "the final SUMMARY must win");
    assert.deepEqual(stored.envelope.files, ["src/final.ts"], "the final FILES must win");
    assert.deepEqual(stored.envelope.risks, ["final risk"]);

    const finalWithoutFiles = [
      "STATUS: completed",
      "SUMMARY: first report",
      "FILES:",
      "- src/kept.ts",
      "STATUS: completed",
      "SUMMARY: final report without files",
    ].join("\n");
    const preserved = await persistResult(directory, fixtureAgent("agent_preserve"), fixtureJob("job_preserve", null), [
      {
        info: { id: "assistant_preserve", role: "assistant", sessionID: "session_fixture" },
        parts: [{ type: "text", text: finalWithoutFiles }],
      },
    ], [], 20_000);
    assert.equal(preserved.envelope.summary, "final report without files");
    assert.deepEqual(preserved.envelope.files, ["src/kept.ts"], "earlier fields must be preserved when no later occurrence exists");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("assistantTextAfterBaseline keeps all assistants only for a genuine new spawn", () => {
  const messages: OpenCodeMessage[] = [
    {
      info: { id: "assistant_first", role: "assistant", sessionID: "session_fixture" },
      parts: [{ type: "text", text: "first turn output" }],
    },
    {
      info: { id: "assistant_second", role: "assistant", sessionID: "session_fixture" },
      parts: [{ type: "text", text: "second turn output" }],
    },
  ];
  const spawn = assistantTextAfterBaseline(messages, null);
  assert.equal(spawn.hasText, true, "a genuine new spawn keeps all assistants");
  assert.match(spawn.text, /first turn output/);
  assert.match(spawn.text, /second turn output/);
  assert.equal(assistantTextAfterBaseline(messages, "baseline_not_present").hasText, false);
  assert.equal(assistantTextAfterBaseline(messages, "assistant_first").text, "second turn output");
});

test("empty assistant tails are skipped and tool-only tails are not usable text", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-result-tails-"));
  try {
    const withText = await persistResult(directory, fixtureAgent("agent_tails"), fixtureJob("job_tails", "baseline_tails"), [
      {
        info: { id: "baseline_tails", role: "assistant", sessionID: "session_fixture" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: baseline text" }],
      },
      {
        info: { id: "text_tail", role: "assistant", sessionID: "session_fixture" },
        parts: [{ type: "text", text: "STATUS: completed\nSUMMARY: visible output" }],
      },
      {
        info: { id: "empty_tail", role: "assistant", sessionID: "session_fixture" },
        parts: [{ type: "tool", text: "hidden tool payload" }],
      },
    ], [], 20_000);
    assert.equal(withText.envelope.summary, "visible output");
    assert.equal(withText.parsed.hasText, true);
    assert.doesNotMatch(JSON.stringify(withText.envelope), /hidden tool payload/);

    const toolOnly = await persistResult(directory, fixtureAgent("agent_tool_only"), fixtureJob("job_tool_only", null), [
      {
        info: { id: "tool_only_tail", role: "assistant", sessionID: "session_fixture" },
        parts: [
          { type: "reasoning", text: "private reasoning" },
          { type: "tool", text: "tool payload" },
        ],
      },
    ], [], 20_000);
    assert.equal(toolOnly.parsed.hasText, false, "a tool-only tail must never parse as usable assistant text");
    const recovered = sanitizePersistedResult(JSON.parse(await readFile(toolOnly.resultPath, "utf8")));
    assert.doesNotMatch(JSON.stringify(recovered), /tool payload|private reasoning/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
