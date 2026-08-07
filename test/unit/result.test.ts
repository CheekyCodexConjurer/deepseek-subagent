import test from "node:test";
import assert from "node:assert/strict";
import { formatHumanResult } from "../../src/result.js";
import type { ResultEnvelope } from "../../src/types.js";

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
