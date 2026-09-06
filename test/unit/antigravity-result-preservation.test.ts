import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { parseAgyOutput } from "../../src/antigravity/parser.js";
import { AntigravitySupervisor } from "../../src/antigravity/supervisor.js";
import { AntigravitySpool } from "../../src/antigravity/spool.js";
import { AntigravityAdapter } from "../../src/antigravity/adapter.js";
import type { AntigravityRunResult } from "../../src/antigravity/types.js";
import { persistAntigravityResult, sanitizePersistedResult, persistResult } from "../../src/result.js";
import type { AgentRecord, JobRecord } from "../../src/types.js";

function makeLongOutput(prefix = "Analysis step"): {
  text: string;
  finalMarker: string;
  secret: string;
  multibyteSample: string;
} {
  const secret = "token=secret_super_confidential_123456";
  const multibyteSample = "Português: validação com acentuação (á, é, í, ó, ú, ç, ã, õ), símbolos 🚀 e japonês 達成";
  const finalMarker = "[FINAL_RESULT_MARKER: SUBAGENT_COMPLETED_SUCCESSFULLY_AT_END]";

  const blocks: string[] = [
    `BEGIN TASK REPORT: ${multibyteSample}`,
    `Credential check: ${secret}`,
  ];

  // Generate ~6,000 characters
  let index = 1;
  while (blocks.join("\n\n").length < 5_500) {
    blocks.push(`${prefix} ${index}: Multibyte text content ${multibyteSample} repeating section ${index} to test payload size expansion beyond 4000 characters limit.`);
    index++;
  }

  blocks.push(finalMarker);
  const text = blocks.join("\n\n");
  return { text, finalMarker, secret, multibyteSample };
}

function fixtureAgent(id: string): AgentRecord {
  return {
    id,
    title: "Test Agent",
    topic: "Test Antigravity Agent",
    repositoryRoot: "C:\\repo",
    workspacePath: "C:\\repo",
    workspaceStrategy: "shared",
    opencodeServerId: "srv_test",
    opencodeSessionId: "antigravity:" + id,
    modelProviderId: "antigravity",
    modelId: "gemini-3.8-flash-high",
    modelVariant: null,
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
    requestId: "req_" + id,
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
    graceDeadlineAt: null,
    gracefulFinalizeAttempted: false,
  };
}

test("parseAgyOutput: plaintext > 4000 chars retains compact preview summary and intact fullText with final marker", () => {
  const { text, finalMarker, secret, multibyteSample } = makeLongOutput("Plaintext section");
  assert.ok(text.length > 5_000, "Generated text must exceed 5000 chars");

  const parsed = parseAgyOutput(text, "") as any;

  // Compact preview summary must be capped at 4,000 chars
  assert.ok(parsed.summary.length <= 4_000, "Summary preview must not exceed 4000 chars");
  assert.ok(parsed.summary.endsWith("…"), "Summary preview must end with ellipsis");
  assert.ok(!parsed.summary.includes(finalMarker), "Summary preview must not contain trailing marker");
  assert.ok(!parsed.summary.includes(secret), "Summary preview must redact secrets");

  // Full response text must be intact, preserving text > 4000 and the final marker
  assert.ok(parsed.fullText, "fullText must be defined on parsed output");
  assert.ok(parsed.fullText.length > 5_000, "fullText must retain full response length");
  assert.ok(parsed.fullText.includes(finalMarker), "fullText must retain the terminal marker");
  assert.ok(!parsed.fullText.includes(secret), "fullText must have secrets redacted");
  assert.ok(parsed.fullText.includes("[REDACTED]"), "fullText must contain redaction placeholder");
  assert.ok(parsed.fullText.includes(multibyteSample), "fullText must preserve multibyte characters intact");
});

test("parseAgyOutput: JSON envelope > 4000 chars retains compact preview summary and intact fullText", () => {
  const { text, finalMarker, secret } = makeLongOutput("JSON content section");
  const jsonEnvelope = JSON.stringify({
    status: "completed",
    summary: "Short preview of the task",
    output: text,
    files: ["src/example.ts"],
    diffSummary: "1 file changed",
  });

  const parsed = parseAgyOutput(jsonEnvelope, "") as any;

  assert.equal(parsed.status, "completed");
  assert.equal(parsed.summary, "Short preview of the task");
  assert.ok(parsed.fullText, "fullText must be extracted from JSON output/text");
  assert.ok(parsed.fullText.length > 5_000, "fullText must retain full response length > 5000");
  assert.ok(parsed.fullText.includes(finalMarker), "fullText must include final marker");
  assert.ok(!parsed.fullText.includes(secret), "fullText must have secrets redacted");
});

test("parseAgyOutput: JSON envelope where summary alone is > 4000 chars produces compact summary and fullText", () => {
  const { text, finalMarker, secret } = makeLongOutput("JSON summary section");
  const jsonEnvelope = JSON.stringify({
    status: "completed",
    summary: text,
  });

  const parsed = parseAgyOutput(jsonEnvelope, "") as any;

  assert.equal(parsed.status, "completed");
  assert.ok(parsed.summary.length <= 4_000, "Summary must be compact <= 4000");
  assert.ok(parsed.fullText.length > 5_000, "fullText must retain full length");
  assert.ok(parsed.fullText.includes(finalMarker), "fullText must retain final marker");
  assert.ok(!parsed.fullText.includes(secret));
});

test("persistAntigravityResult & sanitizePersistedResult: preserves fullText > 4000, redacts secrets, recovers rawAssistantText", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-result-preservation-"));
  try {
    const { text, finalMarker, secret, multibyteSample } = makeLongOutput("Persisted section");
    const agent = fixtureAgent("agent_persist_1");
    const job = fixtureJob("job_persist_1", agent.id);

    const parsed = parseAgyOutput(text, "") as any;
    const runResult: AntigravityRunResult = {
      status: "completed",
      runId: "run_test_1",
      summary: parsed.summary,
      fullText: parsed.fullText,
      files: [],
      tests: [],
      risks: [],
      diffSummary: "none",
      model: "gemini-3.8-flash-high",
      modelDisplayName: "Antigravity · gemini-3.8-flash-high",
      workspace: tempDir,
      rawOutput: text,
    };

    const stored = await persistAntigravityResult(tempDir, agent, job, runResult, 2_000_000);
    const rawFileContent = await readFile(stored.resultPath, "utf8");
    const parsedFile = JSON.parse(rawFileContent) as Record<string, any>;

    // 1. Check persisted envelope has compact summary <= 4000
    assert.ok(parsedFile.envelope.summary.length <= 4_000, "Persisted envelope summary must be <= 4000");
    assert.ok(!parsedFile.envelope.summary.includes(finalMarker), "Envelope summary should not include final marker");

    // 2. Check persisted rawAssistantText has full length > 5000 and includes final marker
    assert.ok(typeof parsedFile.rawAssistantText === "string", "rawAssistantText must be a string");
    assert.ok(parsedFile.rawAssistantText.length > 5_000, "rawAssistantText must exceed 5000 chars on disk");
    assert.ok(parsedFile.rawAssistantText.includes(finalMarker), "rawAssistantText must retain final marker");
    assert.ok(!parsedFile.rawAssistantText.includes(secret), "rawAssistantText must not leak secrets");
    assert.ok(parsedFile.rawAssistantText.includes(multibyteSample), "rawAssistantText must retain multibyte");

    // 3. Test recovery via sanitizePersistedResult (as used by recoverResult)
    const recovered = sanitizePersistedResult(parsedFile, 2_000_000) as Record<string, any>;
    assert.ok(recovered.envelope, "Recovered result must have envelope");
    assert.ok(recovered.envelope.summary.length <= 4_000, "Recovered envelope summary must be <= 4000");
    assert.ok(typeof recovered.rawAssistantText === "string", "Recovered result must include rawAssistantText for Antigravity");
    assert.ok(recovered.rawAssistantText.length > 5_000, "Recovered rawAssistantText must be > 5000 chars");
    assert.ok(recovered.rawAssistantText.includes(finalMarker), "Recovered rawAssistantText must include final marker");
    assert.ok(!recovered.rawAssistantText.includes(secret), "Recovered rawAssistantText must not leak secrets");
    assert.ok(recovered.rawAssistantText.includes(multibyteSample), "Recovered rawAssistantText must have multibyte intact");
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});

test("sanitizePersistedResult preserves OpenCode behavior and omits legacy rawAssistantText without visible parts", () => {
  // Legacy non-Antigravity run with empty messages: rawAssistantText must be omitted
  for (const messages of [[], [{ parts: [] }]]) {
    const recovered = sanitizePersistedResult({ rawAssistantText: "legacy private text", messages }) as Record<string, any>;
    assert.equal(recovered.rawAssistantText, undefined, "Legacy private text must be omitted for non-Antigravity");
  }

  // OpenCode run with visible text: rawAssistantText must be preserved
  const opencodeRun = {
    envelope: {
      version: 1,
      agentId: "ag_opencode",
      jobId: "job_opencode",
      topic: "Topic",
      status: "completed",
      opencodeSessionId: "session_opencode",
      model: "deepseek-v4-flash",
      modelDisplayName: "DeepSeek V4 Flash",
      workspace: "C:\\work",
      summary: "OpenCode summary",
      files: [],
      tests: [],
      risks: [],
      diffSummary: "none",
      fullResultPath: "C:\\results\\job.json",
      orchestratorInstruction: "None",
    },
    rawAssistantText: "OpenCode visible response text",
    messages: [
      {
        info: { id: "m1", role: "assistant" },
        parts: [{ type: "text", text: "OpenCode visible response text" }],
      },
    ],
  };
  const opencodeRecovered = sanitizePersistedResult(opencodeRun) as Record<string, any>;
  assert.equal(opencodeRecovered.rawAssistantText, "OpenCode visible response text");
});

test("Operational test via real isolated child process: emits >4000 chars with multibyte and marker, supervisor and persistence preserve full text", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-child-process-test-"));
  try {
    const { text, finalMarker, secret, multibyteSample } = makeLongOutput("Real process output");
    const spool = new AntigravitySpool(tempDir);
    const manifest = await spool.createAttempt({
      agentId: "agent_child_proc",
      jobId: "job_child_proc",
      requestId: "req_child_proc",
      prompt: "test prompt",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      // Real isolated child process: node writes the long output to stdout and exits 0
      command: process.execPath,
      timeoutMs: 15_000,
      sandbox: false,
      addDirs: [],
      dangerouslySkipPermissions: false,
    });

    // Override manifest args to run node inline script emitting the >4000 chars payload
    const childScript = `
      const payload = Buffer.from(${JSON.stringify(Buffer.from(text, "utf8").toString("base64"))}, "base64").toString("utf8");
      process.stdout.write(payload);
    `;
    manifest.args = ["-e", childScript];

    const supervisor = new AntigravitySupervisor({
      spoolDir: manifest.attemptDir,
      manifest,
    });

    const status = await supervisor.run();
    assert.equal(status.status, "completed");
    assert.equal(status.exitCode, 0);

    const adapter = new AntigravityAdapter();
    const runResult = adapter.mapAttemptStatusToResult(manifest, status);

    // Verify preview is compact
    assert.ok(runResult.summary.length <= 4_000, "Adapter summary preview must be <= 4000");

    // Verify fullText is preserved > 5000
    assert.ok(runResult.fullText, "Adapter runResult must have fullText");
    assert.ok(runResult.fullText.length > 5_000, "fullText must exceed 5000 chars");
    assert.ok(runResult.fullText.includes(finalMarker), "fullText must include final marker");
    assert.ok(!runResult.fullText.includes(secret), "fullText must have secrets redacted");
    assert.ok(runResult.fullText.includes(multibyteSample), "fullText must preserve multibyte");

    // Persist and recover through the full persistence pipeline
    const agent = fixtureAgent(manifest.agentId);
    const job = fixtureJob(manifest.jobId, agent.id);
    const stored = await persistAntigravityResult(tempDir, agent, job, runResult, 2_000_000);
    const fileContent = JSON.parse(await readFile(stored.resultPath, "utf8"));
    const recovered = sanitizePersistedResult(fileContent, 2_000_000) as Record<string, any>;

    assert.ok(recovered.rawAssistantText, "Recovered result from real child process must have rawAssistantText");
    assert.ok(recovered.rawAssistantText.length > 5_000, "Recovered rawAssistantText must be > 5000 chars");
    assert.ok(recovered.rawAssistantText.includes(finalMarker), "Recovered rawAssistantText must include final marker");
    assert.ok(!recovered.rawAssistantText.includes(secret), "Recovered rawAssistantText must redact secrets");
    assert.ok(recovered.rawAssistantText.includes(multibyteSample), "Recovered rawAssistantText must preserve multibyte");
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});

test("COUNTEREXAMPLE 1: parseAgyOutput with private reasoning and absent recognized response does not expose raw JSON", () => {
  const sentinel = "PRIVATE_SENTINEL";
  const jsonEnvelope = JSON.stringify({ status: "completed", reasoning: sentinel });

  const parsed = parseAgyOutput(jsonEnvelope, "");

  assert.equal(parsed.status, "completed");
  assert.equal(parsed.hasJson, true);
  assert.equal(parsed.summary, "", "Summary must fail closed to empty string when recognized response is absent");
  assert.equal(parsed.fullText, "", "fullText must fail closed to empty string when recognized response is absent");
  assert.ok(!parsed.summary.includes(sentinel), "Summary must not leak private sentinel");
  assert.ok(!parsed.fullText.includes(sentinel), "fullText must not leak private sentinel");
});

test("parseAgyOutput: ambiguous / malformed machine envelope fails closed rather than exposing raw contents", () => {
  const sentinel = "PRIVATE_SENTINEL";
  const ambiguousPayload = `{\n  "status": "completed",\n  "reasoning": "${sentinel}"\n`; // unclosed brace

  const parsed = parseAgyOutput(ambiguousPayload, "");

  assert.equal(parsed.summary, "", "Summary must fail closed for ambiguous machine envelope");
  assert.equal(parsed.fullText, "", "fullText must fail closed for ambiguous machine envelope");
  assert.ok(!parsed.summary.includes(sentinel), "Ambiguous envelope must not expose sentinel in summary");
  assert.ok(!parsed.fullText.includes(sentinel), "Ambiguous envelope must not expose sentinel in fullText");
});

test("COUNTEREXAMPLE 2: sanitizePersistedResult rejects displayName-only trust and never bypasses explicit private message parts", () => {
  const sentinel = "PRIVATE_SENTINEL";

  // Case A: Counterexample from user - displayName-only spoof with private reasoning message part
  const spoofWithReasoning = {
    envelope: { modelDisplayName: "Antigravity · fake" },
    messages: [{ parts: [{ type: "reasoning", text: sentinel }] }],
    rawAssistantText: sentinel,
  };
  const recoveredA = sanitizePersistedResult(spoofWithReasoning) as Record<string, any>;
  assert.equal(recoveredA.rawAssistantText, undefined, "DisplayName spoof with reasoning part must drop rawAssistantText");

  // Case B: Genuine Antigravity diff, but explicit private reasoning message part present
  const genuineDiffWithReasoning = {
    diff: { source: "antigravity" },
    envelope: { modelDisplayName: "Antigravity · real" },
    messages: [{ parts: [{ type: "reasoning", text: sentinel }] }],
    rawAssistantText: sentinel,
  };
  const recoveredB = sanitizePersistedResult(genuineDiffWithReasoning) as Record<string, any>;
  assert.equal(recoveredB.rawAssistantText, undefined, "Genuine Antigravity diff must NEVER bypass explicit private message parts");

  // Case C: Genuine Antigravity receipt, but explicit private reasoning message part present
  const genuineReceiptWithReasoning = {
    envelope: {
      receipt: {
        provider: "antigravity",
        jobId: "j1",
        agentId: "a1",
        model: "m1",
        status: "completed",
        workspace: "C:\\work",
        completedAt: new Date().toISOString(),
        outputHash: "hash",
      },
    },
    messages: [{ parts: [{ type: "reasoning", text: sentinel }] }],
    rawAssistantText: sentinel,
  };
  const recoveredC = sanitizePersistedResult(genuineReceiptWithReasoning) as Record<string, any>;
  assert.equal(recoveredC.rawAssistantText, undefined, "Genuine Antigravity receipt must NEVER bypass explicit private message parts");

  // Case D: DisplayName spoof with empty messages: displayName-only trust must be rejected
  const spoofEmptyMessages = {
    envelope: { modelDisplayName: "Antigravity · fake" },
    messages: [],
    rawAssistantText: sentinel,
  };
  const recoveredD = sanitizePersistedResult(spoofEmptyMessages) as Record<string, any>;
  assert.equal(recoveredD.rawAssistantText, undefined, "DisplayName-only trust must be rejected even with empty messages");
});

test("Operational test via real child process: child emitting private reasoning envelope fails closed across entire pipeline", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-sentinel-child-"));
  try {
    const sentinel = "PRIVATE_SENTINEL_CHILD";
    const spool = new AntigravitySpool(tempDir);
    const manifest = await spool.createAttempt({
      agentId: "agent_sentinel_child",
      jobId: "job_sentinel_child",
      requestId: "req_sentinel_child",
      prompt: "test prompt",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      command: process.execPath,
      timeoutMs: 15_000,
      sandbox: false,
      addDirs: [],
      dangerouslySkipPermissions: false,
    });

    const childScript = `
      process.stdout.write(JSON.stringify({ status: "completed", reasoning: ${JSON.stringify(sentinel)} }));
    `;
    manifest.args = ["-e", childScript];

    const supervisor = new AntigravitySupervisor({
      spoolDir: manifest.attemptDir,
      manifest,
    });

    const status = await supervisor.run();
    assert.equal(status.status, "completed");
    assert.ok(!status.summary.includes(sentinel), "Supervisor status summary must not leak sentinel");
    assert.ok(!status.fullText?.includes(sentinel), "Supervisor status fullText must not leak sentinel");

    const adapter = new AntigravityAdapter();
    const runResult = adapter.mapAttemptStatusToResult(manifest, status);
    assert.ok(!runResult.summary.includes(sentinel), "Adapter runResult summary must not leak sentinel");
    assert.ok(!runResult.fullText?.includes(sentinel), "Adapter runResult fullText must not leak sentinel");

    const agent = fixtureAgent(manifest.agentId);
    const job = fixtureJob(manifest.jobId, agent.id);
    const stored = await persistAntigravityResult(tempDir, agent, job, runResult, 2_000_000);
    const fileContent = JSON.parse(await readFile(stored.resultPath, "utf8"));
    assert.ok(!fileContent.envelope.summary.includes(sentinel), "Persisted envelope summary must not leak sentinel");
    assert.ok(!fileContent.rawAssistantText?.includes(sentinel), "Persisted rawAssistantText must not leak sentinel");

    const recovered = sanitizePersistedResult(fileContent, 2_000_000) as Record<string, any>;
    assert.ok(!recovered.envelope.summary.includes(sentinel), "Recovered summary must not leak sentinel");
    assert.ok(!recovered.rawAssistantText?.includes(sentinel), "Recovered rawAssistantText must not leak sentinel");
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});

test("RED: parseAgyOutput preserves Markdown links and does not treat bracketed links as machine envelopes", () => {
  const sample = "[doc](https://example.com) resposta final";
  const parsed = parseAgyOutput(sample, "");
  assert.equal(parsed.hasJson, false);
  assert.equal(parsed.status, null);
  assert.equal(parsed.summary, sample, "Summary must preserve Markdown link");
  assert.equal(parsed.fullText, sample, "fullText must preserve Markdown link");
});

test("RED: parseAgyOutput preserves bracketed plaintext tags like [STATUS]", () => {
  const sample = "[STATUS] Concluído";
  const parsed = parseAgyOutput(sample, "");
  assert.equal(parsed.hasJson, false);
  assert.equal(parsed.status, null);
  assert.equal(parsed.summary, sample, "Summary must preserve [STATUS] tag");
  assert.equal(parsed.fullText, sample, "fullText must preserve [STATUS] tag");
});

test("RED: parseAgyOutput preserves plaintext lists and array notations", () => {
  const sample = "[1, 2, 3] e [\"item1\", \"item2\"]";
  const parsed = parseAgyOutput(sample, "");
  assert.equal(parsed.hasJson, false);
  assert.equal(parsed.status, null);
  assert.equal(parsed.summary, sample, "Summary must preserve list notation");
  assert.equal(parsed.fullText, sample, "fullText must preserve list notation");
});

test("RED: parseAgyOutput preserves legitimate JSON code examples according to contract", () => {
  const sample = "```json\n{\n  \"name\": \"bridge-service\",\n  \"version\": \"1.0.0\"\n}\n```";
  const parsed = parseAgyOutput(sample, "");
  assert.equal(parsed.hasJson, false);
  assert.equal(parsed.status, null);
  assert.equal(parsed.summary, sample, "Summary must preserve legitimate JSON code block");
  assert.equal(parsed.fullText, sample, "fullText must preserve legitimate JSON code block");
});

test("RED: sanitizePersistedResult preserves bracketed and list plaintext for Antigravity results", () => {
  const samples = [
    "[doc](https://example.com) resposta final",
    "[STATUS] Concluído",
    "[1, 2, 3]",
    "```json\n{\n  \"name\": \"bridge-service\"\n}\n```",
  ];
  for (const sample of samples) {
    const recovered = sanitizePersistedResult({
      diff: { source: "antigravity" },
      rawAssistantText: sample,
    }) as Record<string, any>;
    assert.equal(recovered.rawAssistantText, sample, `rawAssistantText must be preserved for: ${sample}`);
  }
});
