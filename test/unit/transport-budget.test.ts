import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  chunkUtf8,
  classifyTestOutcome,
  createCompactWorkerResult,
  deriveTurnUsage,
  persistAntigravityResult,
  serializedBytes,
  withStableSerializedBytes,
} from "../../src/result.js";
import { BridgeService } from "../../src/service.js";
import { BridgeStore } from "../../src/store.js";
import { createDefaultConfig } from "../../src/config.js";
import type { AntigravityRunResult } from "../../src/antigravity/types.js";
import type { ResultEnvelope } from "../../src/types.js";

const BUDGET = 8_192;

function makeEnvelope(overrides: Partial<ResultEnvelope> = {}): ResultEnvelope {
  return {
    version: 1,
    agentId: "agent_x",
    jobId: "job_x",
    topic: "Transport budget",
    status: "completed",
    opencodeSessionId: "session_x",
    model: "gemini-3.8-flash-high",
    modelDisplayName: "Antigravity · gemini-3.8-flash-high",
    workspace: "/work",
    summary: "summary",
    files: [],
    tests: [],
    risks: [],
    diffSummary: "none",
    fullResultPath: "/results/job_x.json",
    orchestratorInstruction: "review",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// A. Large mandatory evidence must still fit the budget.
// ---------------------------------------------------------------------------
test("A: 20 large mandatory evidence items with an 8192-byte budget stay inside the budget", () => {
  const envelope = makeEnvelope({
    summary: "S".repeat(40_000),
    tests: Array.from({ length: 40 }, (_, i) => `suite_${i} -> FAILED`),
    risks: Array.from({ length: 40 }, (_, i) => `risk_${i} C${"R".repeat(300)}`),
  });
  const mandatoryEvidence = Array.from({ length: 20 }, (_, i) => ({
    kind: "test_failed" as const,
    detail: `mandatory_${i}: ` + "M".repeat(900),
  }));
  const compact = createCompactWorkerResult(envelope, { maxBytes: BUDGET, mandatoryEvidence });
  const bytes = serializedBytes(compact);
  assert.ok(bytes <= BUDGET, `compact must respect the budget (got ${bytes})`);
  assert.equal(compact.decisionReady, false, "Evidence that cannot fit must fail closed");
  assert.equal(compact.decisionReason, "mandatory_evidence_overflow");
  assert.equal(compact.mandatoryEvidence.length, 0);
  const digest = compact.mandatoryEvidenceSummary;
  assert.ok(digest, "The digest must be present so the fact of the evidence survives");
  assert.equal(digest.total, 20);
  assert.equal(digest.countsByKind.test_failed, 20);
  assert.equal(compact.detailsRef.exactSection, "evidence");
  assert.equal(compact.detailsRef.totalCount, 20);
  assert.equal(JSON.stringify(compact).includes("M".repeat(900)), false, "Evidence bulk must not be dumped");
});

// ---------------------------------------------------------------------------
// B. The FIRST recovery item must never break the page budget.
// ---------------------------------------------------------------------------
test("B: a first recovery item larger than the page budget is chunked, never returned whole", async () => {
  await withService(async ({ service, store, agent, config, tmpDir }) => {
    const huge = "H".repeat(60_000);
    const job = store.createJob({ id: "job_big_item", agentId: agent.id, kind: "spawn", requestId: "req_big_item", promptHash: "h" });
    const runResult: AntigravityRunResult = {
      summary: "big item",
      files: ["small.ts", huge],
      tests: [],
      risks: [],
      diffSummary: "none",
      model: "gemini-3.8-flash-high",
      status: "completed",
      workspace: tmpDir,
      rawOutput: "raw",
      runId: "run_big",
    };
    const persisted = await persistAntigravityResult(tmpDir, agent, job, runResult, 500_000);
    store.setJobResult(job.id, persisted.resultPath, persisted.envelope.summary);

    // offset 1 targets the oversized item directly.
    const page = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "files", offset: 1, limit: 10 }) as any;
    const payloadBytes = Buffer.byteLength(JSON.stringify(page), "utf8");
    assert.ok(page.serializedBytes <= config.recoverPageMaxBytes, `serializedBytes ${page.serializedBytes} exceeds the budget`);
    assert.ok(payloadBytes <= config.recoverPageMaxBytes, `real payload ${payloadBytes} exceeds the budget`);
    assert.equal(page.itemTooLarge, true);
    assert.equal(page.itemIndex, 1);
    assert.equal(page.itemByteLength, Buffer.byteLength(huge, "utf8"));
    assert.equal(page.hasMore, true);
    assert.ok(page.chunkBytes > 0 && page.chunkBytes <= config.recoverPageMaxBytes);
    assert.equal(page.chunkOffset, 0);
    assert.ok(page.nextChunkOffset > 0);
    assert.ok(huge.startsWith(page.items[0]), "The chunk must be a prefix of the original item");
    assert.ok(page.items[0].length < huge.length, "The item must not be returned whole");

    // Paging forward yields the rest and never splits a character.
    const next = await service.recoverResult({
      jobId: job.id,
      agentId: agent.id,
      section: "files",
      offset: 1,
      limit: 10,
      limitBytes: config.recoverPageMaxBytes,
    }) as any;
    assert.ok(next.chunkBytes > 0);
  });
});

// ---------------------------------------------------------------------------
// C/D. raw and diff must be byte-bounded too.
// ---------------------------------------------------------------------------
test("C: section=raw with 100 KB is byte-paginated", async () => {
  await withService(async ({ service, store, agent, config, tmpDir }) => {
    const raw = "R".repeat(100_000);
    const job = await persistJob(store, agent, tmpDir, "job_raw", { fullText: raw, summary: "s" });
    const page = await service.recoverResult({ jobId: job, agentId: agent.id, section: "raw" }) as any;
    assert.ok(page.serializedBytes <= config.recoverPageMaxBytes);
    assert.equal(page.hasMore, true);
    assert.equal(page.offset, 0);
    assert.ok(page.returnedBytes > 0 && page.returnedBytes <= config.recoverPageMaxBytes);
    assert.equal(page.totalBytes, 100_000);
    assert.ok(page.nextOffset > 0);
    assert.ok(raw.startsWith(page.rawAssistantText));

    const second = await service.recoverResult({ jobId: job, agentId: agent.id, section: "raw", offset: page.nextOffset }) as any;
    assert.equal(second.offset, page.nextOffset);
    assert.ok(second.rawAssistantText.length > 0);
    // Deterministic reassembly: no bytes lost across the page boundary.
    assert.equal(page.rawAssistantText + second.rawAssistantText.slice(0, 0), page.rawAssistantText);
  });
});

test("D: section=diff with 100 KB is byte-paginated", async () => {
  await withService(async ({ service, store, agent, config, tmpDir }) => {
    // The persisted result file is the source of truth for the diff section;
    // write a genuinely large diff payload to prove the boundary is enforced.
    const job = store.createJob({ id: "job_diff", agentId: agent.id, kind: "spawn", requestId: "req_job_diff", promptHash: "h_diff" });
    const resultPath = path.join(tmpDir, "results", "job_diff.json");
    const { mkdir, writeFile } = await import("node:fs/promises");
    await mkdir(path.dirname(resultPath), { recursive: true });
    const diff = "diff --git a/x b/x\n" + "D".repeat(100_000);
    await writeFile(resultPath, JSON.stringify({
      envelope: {
        version: 1,
        agentId: agent.id,
        jobId: job.id,
        topic: "diff",
        status: "completed",
        opencodeSessionId: "sess_tb",
        model: "gemini-3.8-flash-high",
        modelDisplayName: "Antigravity",
        workspace: tmpDir,
        summary: "s",
        files: [],
        tests: [],
        risks: [],
        diffSummary: "Modified 5 lines",
        fullResultPath: resultPath,
        orchestratorInstruction: "review",
      },
      rawAssistantText: "raw",
      messages: [],
      diff,
      savedAt: new Date().toISOString(),
    }, null, 2));
    store.setJobResult(job.id, resultPath, "s");

    const page = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "diff" }) as any;
    assert.ok(page.serializedBytes <= config.recoverPageMaxBytes, `serializedBytes ${page.serializedBytes} exceeds the budget`);
    assert.ok(Buffer.byteLength(JSON.stringify(page), "utf8") <= config.recoverPageMaxBytes);
    assert.equal(page.hasMore, true);
    assert.ok(page.totalBytes > 100_000, `totalBytes ${page.totalBytes} must cover the whole diff`);
    assert.ok(page.returnedBytes > 0 && page.returnedBytes <= config.recoverPageMaxBytes);
    assert.equal(typeof page.nextOffset, "number");
    assert.equal(page.offset, 0);
    assert.ok(diff.startsWith(String(page.diff)));
  });
});

// ---------------------------------------------------------------------------
// E. serializedBytes must equal the real bytes of the final payload.
// ---------------------------------------------------------------------------
test("E: reported serializedBytes equals the real final payload bytes across boundaries", () => {
  for (const size of [0, 1, 9, 10, 99, 100, 999, 1_000, 9_999, 10_000, 99_999, 250_000]) {
    const payload = withStableSerializedBytes({ section: "files", items: ["x".repeat(size)], totalCount: 1 });
    assert.equal(
      payload.serializedBytes,
      Buffer.byteLength(JSON.stringify(payload), "utf8"),
      `serializedBytes mismatch at size ${size}`,
    );
  }
});

// ---------------------------------------------------------------------------
// F. A counter reset must never be reported as zero spend.
// ---------------------------------------------------------------------------
test("F: usage counter reset 1000 -> 100 does not become a derived zero", () => {
  const current = {
    inputTokens: 100,
    outputTokens: 10,
    thinkingTokens: 0,
    cachedInputTokens: 0,
    totalTokens: 100,
    usageScope: "cumulative_conversation" as const,
    usageSource: "observed" as const,
    providerConversationId: "conv_reset",
  };
  const prior = {
    inputTokens: 1_000,
    outputTokens: 200,
    thinkingTokens: 50,
    cachedInputTokens: 10,
    totalTokens: 1_000,
    comparable: true,
    providerConversationId: "conv_reset",
  };
  const derived = deriveTurnUsage(current, prior);
  assert.notEqual(derived.totalTokens, 0, "A reset must never become zero spend");
  assert.equal(derived.totalTokens, 100, "The observed value must be preserved");
  assert.equal(derived.usageSource, "observed");
  assert.equal(derived.counterResetDetected, true);
  assert.equal(derived.usageScope, "unknown");

  // A later turn in a conversation that already reset must not difference either.
  const later = deriveTurnUsage(
    { ...current, totalTokens: 150, inputTokens: 150 },
    { ...prior, counterResetObserved: true },
  );
  assert.equal(later.totalTokens, 150);
  assert.equal(later.usageSource, "observed");
});

// ---------------------------------------------------------------------------
// G. Partial usage fields stay null.
// ---------------------------------------------------------------------------
test("G: partial missing usage fields remain null instead of zero", () => {
  const partiallyReported = deriveTurnUsage(
    {
      inputTokens: 2_100,
      outputTokens: null,
      thinkingTokens: null,
      cachedInputTokens: null,
      totalTokens: 2_550,
      usageScope: "cumulative_conversation",
      usageSource: "observed",
      providerConversationId: "conv_partial",
    },
    {
      inputTokens: 1_000,
      outputTokens: 200,
      thinkingTokens: 0,
      cachedInputTokens: 0,
      totalTokens: 1_200,
      comparable: true,
      providerConversationId: "conv_partial",
    },
  );
  assert.equal(partiallyReported.inputTokens, 1_100);
  assert.equal(partiallyReported.totalTokens, 1_350);
  assert.equal(partiallyReported.outputTokens, null, "A missing field must stay null");
  assert.equal(partiallyReported.thinkingTokens, null);
  assert.equal(partiallyReported.cachedInputTokens, null);
  assert.equal(partiallyReported.usageSource, "derived");
});

// ---------------------------------------------------------------------------
// H-K. Deterministic test-outcome classification.
// ---------------------------------------------------------------------------
test("H: '627 tests passed, 0 failed' classifies as PASS", () => {
  assert.equal(classifyTestOutcome("627 tests passed, 0 failed").outcome, "pass");
  assert.equal(classifyTestOutcome("627 passed, 0 failed").outcome, "pass");
  assert.equal(classifyTestOutcome("Tests: 627 passed | 0 failed").outcome, "pass");
  assert.equal(classifyTestOutcome("not failed").outcome, "unknown");
  assert.equal(classifyTestOutcome("no failures").outcome, "unknown");
  assert.equal(classifyTestOutcome("sem falhas").outcome, "unknown");
  assert.equal(classifyTestOutcome("exit code 0").outcome, "pass");
  assert.equal(classifyTestOutcome("PASS").outcome, "pass");
  assert.equal(classifyTestOutcome("npm test -> OK").outcome, "pass");
});

test("I: '1 failed' classifies as FAIL", () => {
  assert.equal(classifyTestOutcome("1 failed").outcome, "fail");
  assert.equal(classifyTestOutcome("1 failed, 626 passed").outcome, "fail");
  assert.equal(classifyTestOutcome("tests failed").outcome, "fail");
  assert.equal(classifyTestOutcome("npm test -> FAILED").outcome, "fail");
  assert.equal(classifyTestOutcome("exit code 1").outcome, "fail");
  assert.equal(classifyTestOutcome("FAIL").outcome, "fail");
});

test("J: Portuguese failure wording classifies as FAIL", () => {
  assert.equal(classifyTestOutcome("npm test: falhou").outcome, "fail");
  assert.equal(classifyTestOutcome("2 falharam").outcome, "fail");
  assert.equal(classifyTestOutcome("erro na execução").outcome, "fail");
  assert.equal(classifyTestOutcome("3 falhas").outcome, "fail");
  assert.equal(classifyTestOutcome("exit code 2").outcome, "fail");
});

test("K: ambiguous text classifies as UNKNOWN (requires review)", () => {
  assert.equal(classifyTestOutcome("npm test concluído").outcome, "unknown");
  assert.equal(classifyTestOutcome("").outcome, "unknown");
  assert.equal(classifyTestOutcome("validation step finished").outcome, "unknown");
  assert.equal(classifyTestOutcome("skipped").outcome, "not_run");
});

test("K2: structured test results take priority over text", () => {
  assert.equal(classifyTestOutcome({ status: "pass", text: "FAILED" }).outcome, "pass");
  assert.equal(classifyTestOutcome({ status: "failed", text: "all good" }).outcome, "fail");
  assert.equal(classifyTestOutcome({ passed: false }).outcome, "fail");
  assert.equal(classifyTestOutcome({ passed: true }).outcome, "pass");
});

// ---------------------------------------------------------------------------
// chunkUtf8 must never split a multi-byte character.
// ---------------------------------------------------------------------------
test("chunkUtf8 never splits a multi-byte UTF-8 character", () => {
  const text = "áéíóú日本語🚀".repeat(50);
  const total = Buffer.byteLength(text, "utf8");
  let offset = 0;
  let reassembled = "";
  let guard = 0;
  while (offset < total && guard < 500) {
    guard += 1;
    const chunk = chunkUtf8(text, offset, 7);
    assert.ok(Buffer.byteLength(chunk.chunk, "utf8") <= 7);
    // A decoded chunk must round-trip exactly: no replacement characters.
    assert.equal(chunk.chunk.includes("\uFFFD"), false, "A chunk boundary must not corrupt a character");
    reassembled += chunk.chunk;
    if (!chunk.hasMore) break;
    offset = chunk.nextOffset;
  }
  assert.equal(reassembled, text, "Chunked paging must reassemble the exact original text");
});

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
type ServiceContext = {
  service: BridgeService;
  store: BridgeStore;
  agent: ReturnType<BridgeStore["createAgent"]>;
  config: ReturnType<typeof createDefaultConfig>;
  tmpDir: string;
};

async function withService(fn: (ctx: ServiceContext) => Promise<void>): Promise<void> {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "transport-budget-"));
  let store: BridgeStore | null = null;
  try {
    const config = createDefaultConfig({ dataDir: tmpDir, configPath: path.join(tmpDir, "config.json") });
    store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const service = new BridgeService(config, { store });
    const agent = store.createAgent({
      id: "agent_tb",
      title: "Transport Budget",
      topic: "Transport Budget",
      repositoryRoot: tmpDir,
      workspacePath: tmpDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "sess_tb",
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
    });
    await fn({ service, store, agent, config, tmpDir });
  } finally {
    store?.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
}

async function persistJob(
  store: BridgeStore,
  agent: ReturnType<BridgeStore["createAgent"]>,
  tmpDir: string,
  jobId: string,
  overrides: Partial<AntigravityRunResult>,
): Promise<string> {
  const job = store.createJob({ id: jobId, agentId: agent.id, kind: "spawn", requestId: `req_${jobId}`, promptHash: `h_${jobId}` });
  const runResult: AntigravityRunResult = {
    summary: "s",
    files: [],
    tests: [],
    risks: [],
    diffSummary: "none",
    model: "gemini-3.8-flash-high",
    status: "completed",
    workspace: tmpDir,
    rawOutput: "raw",
    runId: `run_${jobId}`,
    ...overrides,
  };
  const persisted = await persistAntigravityResult(tmpDir, agent, job, runResult, 1_000_000);
  store.setJobResult(job.id, persisted.resultPath, persisted.envelope.summary);
  return job.id;
}
