import { createCompactWorkerResult, serializedBytes } from "../dist/result.js";

/**
 * Deterministic payload byte-reduction report. This measures SERIALIZED BYTES,
 * not tokens: it never calls a model and never consumes provider quota.
 *
 * Usage: node scripts/token-economy-bytes.mjs
 */
function makeEnvelope(overrides = {}) {
  return {
    version: 1,
    agentId: "agent_bench",
    jobId: "job_bench",
    topic: "Byte benchmark",
    status: "completed",
    opencodeSessionId: "session_bench",
    model: "gemini-3.8-flash-high",
    modelDisplayName: "Antigravity · gemini-3.8-flash-high",
    workspace: "E:\\work",
    summary: "Resumo do worker. ".repeat(2_800),
    files: Array.from({ length: 100 }, (_, i) => `src/módulo_${i}/arquivo_日本語_${i}.ts`),
    tests: Array.from({ length: 100 }, (_, i) => `npm run suite_${i} -> ${i % 7 === 0 ? "FAILED" : "PASSED"} com acentuação e "aspas"`),
    risks: Array.from({ length: 100 }, (_, i) => (i === 87 ? "CRITICAL: regressão permanece após a correção" : `risco_${i} com escape \\n e tab \\t`)),
    unresolved: Array.from({ length: 20 }, (_, i) => `pendência_${i}`),
    diffSummary: "diff --git a/x b/x\n".repeat(3_000),
    fullResultPath: "E:\\results\\job_bench.json",
    orchestratorInstruction: "Continue this agent only with subagents_continue after reviewing this result.",
    receipt: {
      jobId: "job_bench",
      agentId: "agent_bench",
      provider: "antigravity",
      model: "gemini-3.8-flash-high",
      status: "completed",
      workspace: "E:\\work",
      startedAt: null,
      completedAt: "2026-09-21T00:00:00.000Z",
      durationMs: 1234,
      attempt: "att_1",
      fence: 1,
      outputHash: "deadbeef",
      quiescent: true,
      earlyExit: false,
      filesCount: 100,
      testsCount: 100,
    },
    evidence: {
      items: Array.from({ length: 50 }, (_, i) => ({ type: "code", claim: `claim_${i}`, snippet: "S".repeat(500) })),
      claimsCount: 50,
    },
    ...overrides,
  };
}

const budget = Number(process.env.COMPACT_FOLLOW_MAX_BYTES ?? 8192);
const envelope = makeEnvelope();

// Conceptual OLD-style follow payload: raw envelope + full progress history.
const legacyPayload = {
  agentId: envelope.agentId,
  jobId: envelope.jobId,
  status: envelope.status,
  resultAvailable: true,
  result: { envelope },
  progress: {
    recentActivity: Array.from({ length: 10 }, (_, i) => ({ summary: `step ${i}`, detail: "D".repeat(1_000) })),
    semanticProgress: { stage: "working", summary: "in progress" },
  },
};

const mandatoryEvidence = [
  { kind: "test_failed", detail: "npm run suite_0 -> FAILED" },
  { kind: "blocking_risk", detail: "CRITICAL: regressão permanece após a correção" },
  { kind: "unresolved", detail: "pendência_0" },
];
const compact = createCompactWorkerResult(envelope, { maxBytes: budget, mandatoryEvidence });

const rawBytes = serializedBytes(legacyPayload);
const compactBytes = serializedBytes(compact);
const reductionPercent = ((rawBytes - compactBytes) / rawBytes) * 100;

const rows = [
  ["raw_result_bytes", rawBytes],
  ["compact_follow_bytes", compactBytes],
  ["reduction_percent_bytes", reductionPercent.toFixed(2) + "%"],
  ["budget_bytes", budget],
  ["budget_respected", compactBytes <= budget],
  ["decision_ready", compact.decisionReady],
  ["mandatory_evidence_items", compact.mandatoryEvidence.length],
  ["summary_truncated", compact.detailsRef.summaryTruncated],
  ["files_total", compact.detailsRef.filesTotal],
  ["tests_total", compact.detailsRef.testsTotal],
  ["risks_total", compact.detailsRef.risksTotal],
  ["failing_test_preserved", JSON.stringify(compact).includes("FAILED")],
  ["critical_risk_preserved", JSON.stringify(compact).includes("CRITICAL")],
];
for (const [key, value] of rows) {
  console.log(`${key}: ${value}`);
}
console.log("");
console.log("NOTE: this is PAYLOAD BYTE REDUCTION, not token reduction.");
