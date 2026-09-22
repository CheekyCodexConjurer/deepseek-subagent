import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { buildWorkerPrompt } from "../../src/prompts.js";
import { BridgeStore } from "../../src/store.js";
import {
  classifyValidationEvidence,
  createCompactClaims,
  createCompactWorkerResult,
  createDetailsRef,
  deriveTurnUsage,
  persistAntigravityResult,
  serializedBytes,
} from "../../src/result.js";
import { BridgeService } from "../../src/service.js";
import { createDefaultConfig } from "../../src/config.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createMcpServer } from "../../src/mcp.js";
import type { BridgeHttpClient } from "../../src/http-server.js";
import type { AntigravityRunResult } from "../../src/antigravity/types.js";
import type { ResultEnvelope, ValidationEvidence, WorkOrderEvaluationV1 } from "../../src/types.js";
import { evaluateWorkOrder, parseWorkOrderContract } from "../../src/work-order.js";

const BUDGET = 8_192;

test("work order accepts omitted optional sections without dropping required criteria", () => {
  const order = parseWorkOrderContract({
    schema_version: 1,
    contract_version: 1,
    objective: "Check the narrow change.",
    scope: ["src/example.ts"],
    ownership: ["src/example.ts"],
    acceptance_criteria: [{ id: "AC-01", description: "Requested test passes." }],
  });
  assert.deepEqual(order.contextRefs, []);
  assert.deepEqual(order.designDecisions, []);
  assert.deepEqual(order.invariants, []);
  assert.deepEqual(order.validationCommands, []);
  assert.deepEqual(order.escalationConditions, []);
  assert.equal(order.acceptanceCriteria[0]?.id, "AC-01");
});

test("work order delta repeats only confirmed version and criterion IDs", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "work-order-confirmed-delta-"));
  const workOrder = parseWorkOrderContract({
    schema_version: 1,
    contract_version: 4,
    objective: "FULL_OBJECTIVE_MUST_STAY_IN_PRIOR_CONVERSATION",
    scope: ["FULL_SCOPE_MUST_STAY_IN_PRIOR_CONVERSATION"],
    ownership: ["src/owned.ts"],
    context_refs: ["FULL_CONTEXT_REF_MUST_STAY_IN_PRIOR_CONVERSATION"],
    design_decisions: ["FULL_DESIGN_DECISION_MUST_STAY_IN_PRIOR_CONVERSATION"],
    invariants: ["FULL_INVARIANT_MUST_STAY_IN_PRIOR_CONVERSATION"],
    acceptance_criteria: [
      { id: "AC-01", description: "FULL_CRITERION_TEXT_MUST_STAY_IN_PRIOR_CONVERSATION" },
      { id: "AC-02", description: "FULL_SECOND_CRITERION_TEXT_MUST_STAY_IN_PRIOR_CONVERSATION", requires_git_diff: true },
    ],
    validation_commands: ["FULL_COMMAND_MUST_STAY_IN_PRIOR_CONVERSATION"],
    escalation_conditions: ["FULL_ESCALATION_MUST_STAY_IN_PRIOR_CONVERSATION"],
  });
  try {
    const delta = await buildWorkerPrompt(
      { task: "Continue with this narrow correction.", workOrder, previousWorkOrder: workOrder, confirmedWorkOrderVersion: 4 },
      tmpDir,
      { maxLength: 100_000, isContinuation: true },
    );
    assert.match(delta, /contract version 4/i);
    assert.match(delta, /AC-01.*AC-02/s);
    assert.match(delta, /Do not repeat|do not restate/i);
    assert.match(delta, /Literal Git diff required for AC-02/i);
    assert.doesNotMatch(delta, /FULL_OBJECTIVE|FULL_SCOPE|FULL_CONTEXT_REF|FULL_DESIGN_DECISION|FULL_INVARIANT|FULL_CRITERION_TEXT|FULL_COMMAND|FULL_ESCALATION/);
    assert.ok(delta.length < 1_000, "confirmed continuation must carry a compact contract reference");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("changed confirmed work order sends only its changed fields in the continuation delta", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "work-order-version-delta-"));
  const previous = parseWorkOrderContract({
    schema_version: 1,
    contract_version: 4,
    objective: "UNCHANGED_OBJECTIVE",
    scope: ["UNCHANGED_SCOPE"],
    ownership: ["src/owned.ts"],
    invariants: ["UNCHANGED_INVARIANT"],
    acceptance_criteria: [{ id: "AC-01", description: "UNCHANGED_CRITERION" }],
  });
  const revised = parseWorkOrderContract({
    schema_version: 1,
    contract_version: 5,
    objective: "REVISED_OBJECTIVE_ONLY",
    scope: ["UNCHANGED_SCOPE"],
    ownership: ["src/owned.ts"],
    invariants: ["UNCHANGED_INVARIANT"],
    acceptance_criteria: [{ id: "AC-01", description: "UNCHANGED_CRITERION" }],
  });
  try {
    const delta = await buildWorkerPrompt({
      task: "Apply the revised objective.",
      workOrder: revised,
      previousWorkOrder: previous,
      confirmedWorkOrderVersion: 4,
    }, tmpDir, { maxLength: 100_000, isContinuation: true });
    assert.match(delta, /version 5 replaces confirmed version 4/);
    assert.match(delta, /REVISED_OBJECTIVE_ONLY/);
    assert.doesNotMatch(delta, /UNCHANGED_SCOPE|UNCHANGED_INVARIANT|UNCHANGED_CRITERION/);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("criterion requiring literal Git diff stays blocked when bridge has only a diff summary", () => {
  const workOrder = parseWorkOrderContract({
    schema_version: 1,
    contract_version: 1,
    objective: "Review changed code.",
    scope: ["src/example.ts"],
    ownership: ["src/example.ts"],
    acceptance_criteria: [{ id: "AC-DIFF", description: "Review the exact patch.", requires_git_diff: true }],
  });
  const evaluation = evaluateWorkOrder({
    text: 'WORK_ORDER_OUTCOMES_JSON: {"contract_version":1,"criteria":[{"id":"AC-DIFF","outcome":"satisfied","evidence_refs":["ev_test"]}]}',
    workOrder,
    resultHash: "artifact-hash",
    evidence: { items: [{ id: "ev_test", type: "test", claim: "Tests passed" }] },
    diffAvailability: "summary_only",
  });
  assert.equal(evaluation.criteria[0]?.outcome, "blocked");
  assert.equal(evaluation.gitDiffAvailable, false);
  assert.equal(evaluation.diffAvailability, "summary_only");
  assert.ok(evaluation.issues.includes("git_diff_unavailable:AC-DIFF"));
  assert.equal(evaluation.complete, false);
});

test("criteria stay unverified when persisted result text was truncated", () => {
  const workOrder = parseWorkOrderContract({
    schema_version: 1,
    contract_version: 1,
    objective: "Verify the full final report.",
    scope: ["src/example.ts"],
    ownership: ["src/example.ts"],
    acceptance_criteria: [{ id: "AC-01", description: "The final test report is complete." }],
  });
  const evaluation = evaluateWorkOrder({
    text: 'WORK_ORDER_OUTCOMES_JSON: {"contract_version":1,"criteria":[{"id":"AC-01","outcome":"satisfied","evidence_refs":["ev_test"]}]}',
    workOrder,
    resultHash: "artifact-hash",
    evidence: { items: [{ id: "ev_test", type: "test", claim: "Tests passed" }] },
    diffAvailability: "summary_only",
    resultTextTruncated: true,
  });
  assert.equal(evaluation.criteria[0]?.outcome, "unknown");
  assert.equal(evaluation.resultTextTruncated, true);
  assert.ok(evaluation.issues.includes("result_text_truncated"));
  assert.equal(evaluation.complete, false);
});

function makeEnvelope(overrides: Partial<ResultEnvelope> = {}): ResultEnvelope {
  return {
    version: 1,
    agentId: "agent_1",
    jobId: "job_1",
    topic: "Topic",
    status: "completed",
    opencodeSessionId: "session_1",
    model: "gemini-3.8-flash-high",
    modelDisplayName: "Antigravity · gemini-3.8-flash-high",
    workspace: "/work",
    summary: "Summary",
    files: [],
    tests: [],
    risks: [],
    diffSummary: "none",
    fullResultPath: "/results/job_1.json",
    orchestratorInstruction: "Continue this agent only with subagents_continue after reviewing this result.",
    receipt: {
      jobId: "job_1",
      agentId: "agent_1",
      provider: "antigravity",
      model: "gemini-3.8-flash-high",
      status: "completed",
      workspace: "/work",
      startedAt: null,
      completedAt: new Date().toISOString(),
      durationMs: 120,
      attempt: "att_1",
      fence: 1,
      outputHash: "hash",
      quiescent: true,
      earlyExit: false,
      filesCount: 0,
      testsCount: 0,
    },
    ...overrides,
  };
}

test("token economy: buildWorkerPrompt with isContinuation emits concise delta prompt", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "prompt-continuation-"));
  try {
    const fullPrompt = await buildWorkerPrompt({ task: "Continue fixing the bug." }, tmpDir, { isContinuation: false });
    const deltaPrompt = await buildWorkerPrompt({ task: "Continue fixing the bug." }, tmpDir, { isContinuation: true });

    assert.match(fullPrompt, /At completion, use these exact headings/i);
    assert.match(fullPrompt, /Operating rule/i);
    assert.ok(fullPrompt.length > 500, "Full prompt should be comprehensive");

    assert.doesNotMatch(deltaPrompt, /At completion, use these exact headings/i);
    assert.doesNotMatch(deltaPrompt, /Operating rule/i);
    assert.match(deltaPrompt, /Continuation Task:/);
    assert.match(deltaPrompt, /Continue fixing the bug\./);
    assert.ok(deltaPrompt.length < fullPrompt.length, "Delta prompt must be substantially smaller");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("token economy: delta prompt preserves new requirements, scope changes and visual context", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "prompt-delta-preserve-"));
  try {
    const delta = await buildWorkerPrompt(
      {
        task: "Corrija o bug de encoding e valide.",
        relation: "correction",
        visualContext: "Direct observations: botao vermelho\nInterpretation: erro de estado\nUncertainty: cor exata",
      },
      tmpDir,
      { isContinuation: true },
    );
    assert.match(delta, /Continuation Task:/);
    assert.match(delta, /Corrija o bug de encoding e valide\./);
    assert.match(delta, /Request relation: correction/);
    assert.match(delta, /Visual context from the orchestrator:/);
    assert.match(delta, /erro de estado/);
    // Boilerplate that already lives in the conversation must not be resent.
    assert.doesNotMatch(delta, /You are a local Antigravity sub-agent/);
    assert.doesNotMatch(delta, /Workspace strategy:/);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("token economy: usage deltas are only derived for the same cumulative conversation", () => {
  const current = {
    inputTokens: 2100,
    outputTokens: 450,
    thinkingTokens: 300,
    cachedInputTokens: 1000,
    totalTokens: 2550,
    usageScope: "cumulative_conversation" as const,
    usageSource: "observed" as const,
    providerConversationId: "conv_a",
  };
  const prior = {
    inputTokens: 1000,
    outputTokens: 200,
    thinkingTokens: 150,
    cachedInputTokens: 500,
    totalTokens: 1200,
    comparable: true,
    providerConversationId: "conv_a",
  };

  const sameConversation = deriveTurnUsage(current, prior);
  assert.equal(sameConversation.inputTokens, 1100);
  assert.equal(sameConversation.outputTokens, 250);
  assert.equal(sameConversation.thinkingTokens, 150);
  assert.equal(sameConversation.cachedInputTokens, 500);
  assert.equal(sameConversation.totalTokens, 1350);
  assert.equal(sameConversation.usageScope, "per_turn");
  assert.equal(sameConversation.usageSource, "derived");

  // Session reset: a NEW provider conversation must not subtract the old totals.
  const newConversation = deriveTurnUsage(current, { ...prior, providerConversationId: "conv_b" });
  assert.equal(newConversation.inputTokens, 2100, "New conversation keeps the observed values");
  assert.equal(newConversation.totalTokens, 2550);
  assert.equal(newConversation.usageSource, "observed");
  assert.notEqual(newConversation.totalTokens, 1350, "A reset must never produce a false delta");

  // Unknown scope: never combined arithmetically.
  const unknownScope = deriveTurnUsage({ ...current, usageScope: "unknown" }, prior);
  assert.equal(unknownScope.totalTokens, 2550);
  assert.equal(unknownScope.usageSource, "observed");

  // Missing baseline: keep observed values rather than subtracting from zero.
  const noBaseline = deriveTurnUsage(current, { ...prior, comparable: false });
  assert.equal(noBaseline.totalTokens, 2550);
});

test("token economy: missing usage fields stay null instead of becoming zero", () => {
  const current = {
    inputTokens: null,
    outputTokens: null,
    thinkingTokens: null,
    cachedInputTokens: null,
    totalTokens: null,
    usageScope: "cumulative_conversation" as const,
    usageSource: "observed" as const,
    providerConversationId: "conv_a",
  };
  const derived = deriveTurnUsage(current, {
    inputTokens: 100,
    outputTokens: 10,
    thinkingTokens: 5,
    cachedInputTokens: 1,
    totalTokens: 115,
    comparable: true,
    providerConversationId: "conv_a",
  });
  assert.equal(derived.inputTokens, null, "An unreported field must remain null, not 0");
  assert.equal(derived.totalTokens, null);
});

test("token economy: thinking tokens are never added to a total the provider did not report", () => {
  const envelope = makeEnvelope({
    summary: "ok",
    usage: {
      inputTokens: 100,
      outputTokens: 20,
      thinkingTokens: 30,
      cachedInputTokens: 5,
      totalTokens: null,
      usageScope: "cumulative_conversation",
      usageSource: "observed",
    },
  });
  const compact = createCompactWorkerResult(envelope, { maxBytes: BUDGET });
  assert.equal(compact.tokens?.totalTokens, null, "Total must stay null when the provider did not report one");
  assert.equal(compact.tokens?.thinkingTokens, 30, "Thinking is reported separately");
  assert.equal(compact.tokens?.cachedInputTokens, 5, "Cached input stays separate from input total");
});

test("token economy: compact payload respects the serialized byte budget with adversarial fixtures", () => {
  const envelope = makeEnvelope({
    summary: "S".repeat(50_000),
    files: Array.from({ length: 100 }, (_, i) => `src/very/deep/path/file_${i}_ünïcödé_日本語.ts`),
    tests: Array.from({ length: 100 }, (_, i) => `npm run test:suite_${i} -> PASSED`),
    risks: Array.from({ length: 100 }, (_, i) => `risk ${i} with "quotes" and \u0000 escapes \n`),
    diffSummary: "diff".repeat(2_000),
  });

  const compact = createCompactWorkerResult(envelope, { maxBytes: BUDGET });
  const bytes = serializedBytes(compact);
  assert.ok(bytes <= BUDGET, `compact payload must respect the budget (got ${bytes})`);
  assert.equal(compact.version, 1);
  assert.equal(compact.decisionReady, true);
  assert.ok(compact.claims.summary.length <= 1_000);
});

test("token economy: payload byte reduction is measured in bytes, not tokens", () => {
  const envelope = makeEnvelope({
    summary: "S".repeat(50_000),
    files: Array.from({ length: 100 }, (_, i) => `file_${i}.ts`),
    tests: Array.from({ length: 100 }, (_, i) => `test_${i} -> PASSED`),
    risks: Array.from({ length: 100 }, (_, i) => `risk_${i}`),
    diffSummary: "diff".repeat(5_000),
  });

  const legacyPayload = {
    result: { envelope },
    progress: { recentActivity: Array.from({ length: 10 }, (_, i) => ({ summary: `step ${i}` })) },
  };
  const compact = createCompactWorkerResult(envelope, { maxBytes: BUDGET });
  const rawBytes = serializedBytes(legacyPayload);
  const compactBytes = serializedBytes(compact);
  const reductionPercent = Math.round(((rawBytes - compactBytes) / rawBytes) * 100);

  assert.ok(rawBytes > compactBytes);
  assert.ok(compactBytes <= BUDGET);
  assert.ok(reductionPercent > 90, `expected >90% payload byte reduction, got ${reductionPercent}%`);
});

test("token economy: mandatory evidence is never dropped and overflow fails closed", () => {
  const envelope = makeEnvelope({
    summary: "implementei a correcao",
    tests: [
      "npm test -> PASSED",
      "npm run lint -> PASSED",
      "npm run typecheck -> PASSED",
      "npm run e2e -> PASSED",
      "npm run smoke -> PASSED",
      "npm test -> FAILED",
    ],
    risks: [
      "risk 1",
      "risk 2",
      "risk 3",
      "risk 4",
      "risk 5",
      "CRITICAL: regressão permanece no parser",
    ],
  });

  const evidence = classifyValidationEvidence({
    claimedStatus: "completed",
    providerExecutionStatus: "success",
    status: "completed",
    tests: envelope.tests,
    risks: envelope.risks,
    unresolved: [],
    files: [],
  });

  assert.ok(evidence.testsFailed.some((t) => /FAILED/.test(t)), "A failing test must be classified");
  assert.ok(evidence.blockingRisks.some((r) => /CRITICAL/.test(r)), "A critical risk must be classified");
  assert.equal(evidence.claimEvidenceConflict, true, "A completion claim with a failing test is a conflict");

  const kinds = evidence.mandatory.map((item) => item.kind);
  assert.ok(kinds.includes("test_failed"));
  assert.ok(kinds.includes("blocking_risk"));
  assert.ok(kinds.includes("claim_evidence_conflict"));

  const compact = createCompactWorkerResult(envelope, { maxBytes: BUDGET, mandatoryEvidence: evidence.mandatory });
  assert.ok(serializedBytes(compact) <= BUDGET);
  const serialized = JSON.stringify(compact);
  assert.match(serialized, /FAILED/, "The failing test must survive compaction");
  assert.match(serialized, /CRITICAL/, "The critical risk must survive compaction");
  assert.equal(compact.claims.risks.length <= 5, true);

  // Adversarial: mandatory evidence alone exceeds the budget -> fail closed.
  const overflowing = createCompactWorkerResult(envelope, {
    maxBytes: 256,
    mandatoryEvidence: Array.from({ length: 20 }, (_, i) => ({
      kind: "test_failed" as const,
      detail: `mandatory failure ${i} `.repeat(20),
    })),
  });
  assert.equal(overflowing.decisionReady, false);
  assert.equal(overflowing.decisionReason, "mandatory_evidence_overflow");
  assert.ok(serializedBytes(overflowing) > 0);
});

test("token economy: provider SUCCESS with a failing test never presents as all green", () => {
  const evidence = classifyValidationEvidence({
    claimedStatus: "completed",
    providerExecutionStatus: "success",
    status: "completed",
    tests: ["npm test -> FAILED"],
    risks: ["regressão permanece"],
    unresolved: [],
    files: [],
  });
  const envelope = makeEnvelope({
    summary: "implementei a correção",
    tests: ["npm test -> FAILED"],
    risks: ["regressão permanece"],
    validationEvidence: evidence,
  });
  const compact = createCompactWorkerResult(envelope, { maxBytes: BUDGET, mandatoryEvidence: evidence.mandatory });
  const serialized = JSON.stringify(compact);
  assert.match(serialized, /test_failed/);
  assert.match(serialized, /FAILED/);
  assert.equal(compact.decisionReady, true, "The evidence is present, so a decision can still be made");
  assert.ok(compact.mandatoryEvidence.some((item) => item.kind === "test_failed"), "Mandatory evidence must be present");
  assert.ok(serializedBytes(compact) <= BUDGET);
});

test("token economy: work-order evaluations stay complete or point to the full byte-paginated result", () => {
  const criteria = Array.from({ length: 12 }, (_, index) => ({
    id: `AC-${String(index + 1).padStart(2, "0")}`,
    description: `Criterion ${index + 1}`,
  }));
  const evaluation: WorkOrderEvaluationV1 = {
    schemaVersion: 1,
    contractVersion: 1,
    resultHash: "hash",
    source: "worker_report",
    complete: true,
    criteria: criteria.map((criterion, index) => ({
      id: criterion.id,
      outcome: "satisfied",
      evidenceRefs: [`ev_${index}_` + "x".repeat(56)],
      evidenceRefsResolved: true,
    })),
    issues: [],
  };
  const envelope = makeEnvelope({
    workOrder: {
      schemaVersion: 1,
      contractVersion: 1,
      objective: "Evaluate all criteria without silent truncation.",
      scope: ["src/example.ts"],
      ownership: ["src/example.ts"],
      contextRefs: [],
      designDecisions: [],
      invariants: [],
      acceptanceCriteria: criteria,
      validationCommands: [],
      escalationConditions: [],
    },
    workOrderEvaluation: evaluation,
  });
  const standard = createCompactWorkerResult(envelope, { maxBytes: BUDGET });
  assert.deepEqual(standard.workOrderEvaluation?.criteria, evaluation.criteria);
  assert.equal(standard.workOrderEvaluation?.resultHash, standard.receipt?.outputHash);

  const constrained = createCompactWorkerResult(envelope, { maxBytes: 1_024 });
  assert.ok(serializedBytes(constrained) <= 1_024);
  if (constrained.workOrderEvaluation) {
    assert.deepEqual(constrained.workOrderEvaluation.criteria, evaluation.criteria);
  } else {
    assert.equal(constrained.decisionReady, false);
    assert.equal(constrained.decisionReason, "work_order_transport_overflow");
    assert.equal(constrained.detailsRef.exactSection, "work_order");
    assert.ok(constrained.detailsRef.availableSections.includes("work_order"));
  }
});

test("token economy: detailsRef detects a truncated summary and reports counts", () => {
  const envelope = makeEnvelope({
    summary: "S".repeat(5_000),
    files: Array.from({ length: 50 }, (_, i) => `file_${i}.ts`),
    tests: Array.from({ length: 30 }, (_, i) => `test_${i}`),
    risks: Array.from({ length: 20 }, (_, i) => `risk_${i}`),
    unresolved: ["pending item"],
    evidence: { items: [{ type: "code", claim: "c1" }, { type: "log", claim: "c2" }], claimsCount: 2 },
  });

  const compact = createCompactWorkerResult(envelope, { maxBytes: BUDGET });
  const ref = compact.detailsRef;
  assert.equal(ref.summaryTruncated, true, "A truncated summary must flag hasMoreDetails");
  assert.equal(ref.hasMoreDetails, true);
  assert.equal(ref.filesTotal, 50);
  assert.equal(ref.testsTotal, 30);
  assert.equal(ref.risksTotal, 20);
  assert.equal(ref.evidenceTotal, 2);
  assert.ok(ref.availableSections.includes("unresolved"));

  // Even a summary-only overflow must flag truncation.
  const summaryOnly = createDetailsRef(makeEnvelope({ summary: "S".repeat(5_000) }), { compactSummaryChars: 1_000 });
  assert.equal(summaryOnly.summaryTruncated, true);
  assert.equal(summaryOnly.hasMoreDetails, true);
});

test("token economy: createCompactClaims bounds size and detailsRef points to available sections", () => {
  const envelope = makeEnvelope({
    summary: "A".repeat(5_000),
    files: Array.from({ length: 50 }, (_, i) => `file_${i}.ts`),
    tests: Array.from({ length: 30 }, (_, i) => `test_${i}`),
    risks: Array.from({ length: 20 }, (_, i) => `risk_${i}`),
    diffSummary: "Diff text",
  });

  const claims = createCompactClaims(envelope);
  assert.equal(claims.summary.length, 1_000, "Summary must be bounded to 1000 characters");
  assert.equal(claims.files.length, 10);
  assert.equal(claims.tests.length, 10);
  assert.equal(claims.risks.length, 5);

  const detailsRef = createDetailsRef(envelope);
  assert.equal(detailsRef.hasMoreDetails, true);
  assert.equal(detailsRef.resultPath, "/results/job_1.json");
  assert.deepEqual(detailsRef.availableSections, ["summary", "files", "tests", "risks", "diff", "evidence", "unresolved", "full"]);
});

test("token economy: recoverResult section semantics and byte-budgeted pagination", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "service-recover-"));
  let store: BridgeStore | null = null;
  try {
    const config = createDefaultConfig({ dataDir: tmpDir, configPath: path.join(tmpDir, "config.json") });
    store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const service = new BridgeService(config, { store });

    const agent = store.createAgent({
      id: "agent_rec_1",
      title: "Recover Test",
      topic: "Recover Test",
      repositoryRoot: tmpDir,
      workspacePath: tmpDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "sess_rec_1",
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_rec_1", agentId: agent.id, kind: "spawn", requestId: "req_rec_1", promptHash: "hash_rec_1" });

    const runResult: AntigravityRunResult = {
      summary: "Detailed summary of execution",
      files: ["a.ts", "b.ts", "c.ts", "d.ts", "e.ts"],
      tests: ["test 1", "test 2", "test 3"],
      risks: ["risk alpha", "risk beta"],
      unresolved: ["still pending"],
      diffSummary: "Modified 5 lines",
      model: "gemini-3.8-flash-high",
      status: "completed",
      workspace: tmpDir,
      rawOutput: "raw output text",
      fullText: "STATUS: completed\nSUMMARY: Detailed summary of execution\nTESTS:\n- test 1 -> PASSED",
      runId: "run_1",
      evidence: {
        summary: "Evidence summary",
        items: [
          { type: "code", description: "snippet 1" },
          { type: "log", description: "log 1" },
          { type: "test", description: "test log 1" },
        ],
      },
    };

    const persisted = await persistAntigravityResult(tmpDir, agent, job, runResult, 100_000);
    store.setJobResult(job.id, persisted.resultPath, persisted.envelope.summary);

    // 1. Full recover still returns the complete result.
    const fullRes = await service.recoverResult({ jobId: job.id, agentId: agent.id }) as any;
    assert.ok(fullRes.envelope, "Full recover should contain envelope");
    assert.equal(fullRes.envelope.files.length, 5);

    // 2. section=summary must NOT attach the raw worker text.
    const summarySec = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "summary" }) as any;
    assert.equal(summarySec.section, "summary");
    assert.equal(summarySec.summary, "Detailed summary of execution");
    assert.equal("rawAssistantText" in summarySec, false, "summary must never leak rawAssistantText");
    assert.ok(summarySec.serializedBytes > 0);

    // 3. section=raw is the explicitly named full-text mode.
    const rawSec = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "raw" }) as any;
    assert.equal(rawSec.section, "raw");
    assert.match(rawSec.rawAssistantText, /STATUS: completed/);

    // 4. files pagination with byte accounting.
    const filesPage1 = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "files", offset: 0, limit: 2 }) as any;
    assert.deepEqual(filesPage1.items, ["a.ts", "b.ts"]);
    assert.equal(filesPage1.offset, 0);
    assert.equal(filesPage1.returnedCount, 2);
    assert.equal(filesPage1.totalCount, 5);
    assert.equal(filesPage1.hasMore, true);
    assert.equal(filesPage1.nextOffset, 2);
    assert.ok(filesPage1.serializedBytes > 0);

    const filesPage2 = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "files", offset: 2, limit: 3 }) as any;
    assert.deepEqual(filesPage2.items, ["c.ts", "d.ts", "e.ts"]);
    assert.equal(filesPage2.hasMore, false);
    assert.equal(filesPage2.nextOffset, null);

    // 5. tests / risks / unresolved / diff / evidence sections.
    const testsSec = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "tests" }) as any;
    assert.deepEqual(testsSec.items, ["test 1", "test 2", "test 3"]);
    const risksSec = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "risks" }) as any;
    assert.deepEqual(risksSec.items, ["risk alpha", "risk beta"]);
    const unresolvedSec = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "unresolved" }) as any;
    assert.deepEqual(unresolvedSec.items, ["still pending"]);
    const diffSec = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "diff" }) as any;
    assert.equal(diffSec.diffSummary, "Modified 5 lines");
    const evidenceSec = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "evidence", offset: 0, limit: 2 }) as any;
    assert.equal(evidenceSec.items.length, 2);
    assert.equal(evidenceSec.totalCount, 3);
    assert.equal(evidenceSec.hasMore, true);

    // 6. Byte budget: a page never exceeds recoverPageMaxBytes even with huge items.
    const bigJob = store.createJob({ id: "job_rec_big", agentId: agent.id, kind: "spawn", requestId: "req_rec_big", promptHash: "hash_rec_big" });
    const bigResult: AntigravityRunResult = {
      ...runResult,
      files: Array.from({ length: 40 }, (_, i) => `huge_${i}_` + "X".repeat(3_000)),
      tests: [],
      risks: [],
      unresolved: [],
      evidence: undefined,
    };
    const bigPersisted = await persistAntigravityResult(tmpDir, agent, bigJob, bigResult, 100_000);
    store.setJobResult(bigJob.id, bigPersisted.resultPath, bigPersisted.envelope.summary);
    const bigPage = await service.recoverResult({ jobId: bigJob.id, agentId: agent.id, section: "files", offset: 0, limit: 10 }) as any;
    assert.ok(bigPage.serializedBytes <= config.recoverPageMaxBytes, `page must respect the byte budget (got ${bigPage.serializedBytes})`);
    assert.ok(bigPage.returnedCount < 10, "The byte budget must cut the page before the item limit");
    assert.equal(bigPage.hasMore, true);
    assert.equal(bigPage.nextOffset, bigPage.returnedCount);
  } finally {
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("token economy: service.follow delivers a bounded compact payload and keeps the full result persisted", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "service-follow-"));
  let store: BridgeStore | null = null;
  try {
    const config = createDefaultConfig({ dataDir: tmpDir, configPath: path.join(tmpDir, "config.json") });
    store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const service = new BridgeService(config, { store });
    await service.start();

    const agent = store.createAgent({
      id: "agent_follow_1",
      title: "Follow Test",
      topic: "Follow Test",
      repositoryRoot: tmpDir,
      workspacePath: tmpDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "sess_follow_1",
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
    });
    const job = store.createJob({ id: "job_follow_1", agentId: agent.id, kind: "spawn", requestId: "req_follow_1", promptHash: "hash_follow_1" });

    const runResult: AntigravityRunResult = {
      summary: "S".repeat(20_000),
      files: Array.from({ length: 60 }, (_, i) => `file_${i}.ts`),
      tests: ["npm test -> FAILED", ...Array.from({ length: 30 }, (_, i) => `test_${i} -> PASSED`)],
      risks: ["CRITICAL: regressão permanece", ...Array.from({ length: 10 }, (_, i) => `risk_${i}`)],
      unresolved: ["pending follow-up"],
      diffSummary: "diff".repeat(2_000),
      model: "gemini-3.8-flash-high",
      status: "completed",
      workspace: tmpDir,
      rawOutput: "raw",
      fullText: "STATUS: completed\nSUMMARY: done\nTESTS:\n- npm test -> FAILED\nRISKS:\n- CRITICAL: regressão permanece",
      runId: "run_follow_1",
      conversationId: "conv_follow_1",
      providerExecutionStatus: "success",
      workerClaimedStatus: "completed",
      validationEvidence: classifyValidationEvidence({
        claimedStatus: "completed",
        providerExecutionStatus: "success",
        status: "completed",
        tests: ["npm test -> FAILED"],
        risks: ["CRITICAL: regressão permanece"],
        unresolved: ["pending follow-up"],
        files: [],
      }),
      usage: {
        inputTokens: 19264,
        outputTokens: 1,
        thinkingTokens: 0,
        cachedInputTokens: 0,
        totalTokens: 19265,
        usageScope: "cumulative_conversation",
        usageSource: "observed",
        providerConversationId: "conv_follow_1",
      },
    };

    const persisted = await persistAntigravityResult(tmpDir, agent, job, runResult, 100_000);
    store.updateJobStatus(job.id, "dispatching");
    store.updateJobStatus(job.id, "running");
    store.setJobResult(job.id, persisted.resultPath, persisted.envelope.summary);
    store.updateJobWorkerUsage(job.id, {
      inputTokens: 19264,
      outputTokens: 1,
      thinkingTokens: 0,
      cachedInputTokens: 0,
      totalTokens: 19265,
      usageScope: "cumulative_conversation",
      usageSource: "observed",
      providerConversationId: "conv_follow_1",
    });
    store.updateJobStatus(job.id, "completed");

    const follow = await service.follow({ agentId: agent.id, jobId: job.id }) as any;
    assert.equal(follow.status, "completed");
    assert.ok(follow.compact, "The compact projection must be present");
    assert.ok(serializedBytes(follow.compact) <= config.compactFollowMaxBytes, "Compact payload must respect the byte budget");
    assert.equal("rawAssistantText" in follow, false, "follow must not leak rawAssistantText");
    assert.equal("fullText" in follow, false, "follow must not leak the worker full text");

    const serialized = JSON.stringify(follow.compact);
    assert.match(serialized, /FAILED/, "The failing test must survive the compact projection");
    assert.match(serialized, /CRITICAL/, "The critical risk must survive the compact projection");
    assert.equal(follow.compact.detailsRef.hasMoreDetails, true);
    assert.equal(follow.compact.detailsRef.summaryTruncated, true);
    assert.equal(follow.compact.tokens?.totalTokens, 19265);

    // The complete result is still persisted and recoverable.
    const recovered = await service.recoverResult({ jobId: job.id, agentId: agent.id, section: "raw" }) as any;
    assert.match(recovered.rawAssistantText, /npm test -> FAILED/);
  } finally {
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("token economy: MCP subagents_follow strips envelope and progress and stays within the byte budget", async () => {
  const mockFollowResponse = {
    agentId: "agent_mcp_1",
    jobId: "job_mcp_1",
    status: "completed",
    resultAvailable: true,
    claims: { summary: "Short summary", files: ["f1.ts"], tests: ["t1 -> FAILED"], risks: ["CRITICAL: regression"] },
    mandatoryEvidence: [{ kind: "test_failed", detail: "t1 -> FAILED" }],
    decisionReady: true,
    tokens: {
      inputTokens: 1500,
      outputTokens: 120,
      thinkingTokens: 50,
      cachedInputTokens: 800,
      totalTokens: 1620,
      usageScope: "cumulative_conversation",
      usageSource: "observed",
    },
    detailsRef: {
      resultPath: "/path/to/results/job_mcp_1.json",
      hasMoreDetails: true,
      availableSections: ["summary", "files", "tests", "risks", "diff", "evidence", "unresolved", "full"],
      summaryTruncated: true,
      filesTotal: 60,
      testsTotal: 31,
      risksTotal: 11,
    },
    result: { envelope: { veryLargePayload: "X".repeat(200_000) } },
    progress: { recentActivity: Array.from({ length: 500 }, (_, i) => ({ summary: `did step ${i}`, raw: "Y".repeat(200) })) },
  };

  const bridgeClient = {
    call: async (url: string) => {
      if (url === "/v1/jobs/follow") return mockFollowResponse;
      throw new Error("Unexpected endpoint: " + url);
    },
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient, { compactFollowMaxBytes: BUDGET });
  const client = new Client({ name: "test-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);

  try {
    const res = await client.callTool({ name: "subagents_follow", arguments: { agent_id: "agent_mcp_1" } });
    assert.equal(res.isError, undefined);
    const text = (res.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(text, /tokens: 1620/);
    assert.match(text, /mandatoryEvidence: 1/);

    const structured = res.structuredContent as Record<string, unknown>;
    assert.equal(structured.obligationState, "closed");
    // Single canonical source: claims/tokens live inside the compact projection.
    const compactResult = structured.compact as Record<string, unknown>;
    assert.deepEqual(compactResult.claims, mockFollowResponse.claims);
    assert.deepEqual(compactResult.tokens, mockFollowResponse.tokens);
    assert.equal(compactResult.decisionReady, true);
    assert.equal(structured.claims, undefined, "claims must not be duplicated at the top level");
    assert.equal(structured.tokens, undefined, "tokens must not be duplicated at the top level");
    assert.equal(structured.detailsRef, undefined, "detailsRef must not be duplicated at the top level");

    // Bulky envelope/progress must be stripped; the complete result stays persisted.
    assert.equal(structured.result, undefined, "Bulky result envelope must be stripped");
    assert.equal(structured.progress, undefined, "Bulky progress history must be stripped");
    assert.equal(JSON.stringify(structured).includes("veryLargePayload"), false);
    assert.equal(JSON.stringify(structured).includes("did step"), false);

    // The invariant is over what the model actually sees: content + structuredContent.
    const visible = Buffer.byteLength(JSON.stringify({ content: res.content, structuredContent: structured }), "utf8");
    assert.ok(visible <= BUDGET, `MCP model-visible follow payload must respect the byte budget (got ${visible})`);
    assert.equal(JSON.stringify(structured).includes("FAILED"), true, "Mandatory evidence must survive");
  } finally {
    await client.close();
    await server.close();
  }
});

test("token economy: MCP follow fails closed when even the evidence cannot fit the transport budget", async () => {
  const mockFollowResponse = {
    agentId: "agent_mcp_2",
    jobId: "job_mcp_2",
    status: "completed",
    resultAvailable: true,
    compact: {
      version: 1,
      status: "completed",
      claims: { summary: "S".repeat(50_000), files: [], tests: [], risks: [] },
      mandatoryEvidence: [],
      receipt: null,
      tokens: null,
      decisionReady: true,
      decisionReason: null,
      detailsRef: { resultPath: "/p", hasMoreDetails: true, availableSections: ["full"] },
    },
  };
  const bridgeClient = {
    call: async () => mockFollowResponse,
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient, { compactFollowMaxBytes: 512 });
  const client = new Client({ name: "test-client-2", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const res = await client.callTool({ name: "subagents_follow", arguments: { agent_id: "agent_mcp_2" } });
    const structured = res.structuredContent as Record<string, unknown>;
    const visible = Buffer.byteLength(JSON.stringify({ content: res.content, structuredContent: structured }), "utf8");
    // Requirement: decisionReady=false must never coexist with an over-budget payload.
    assert.ok(visible <= 512, `model-visible payload must respect the budget (got ${visible})`);
    const serialized = JSON.stringify(structured);
    assert.equal(serialized.includes("S".repeat(2_000)), false, "The oversized summary must never be transported");
  } finally {
    await client.close();
    await server.close();
  }
});

test("token economy: mandatory evidence overflow stays inside the budget and points at the exact section", async () => {
  const hugeEvidence = Array.from({ length: 20 }, (_, i) => ({
    kind: "test_failed",
    detail: `case_${i}: ` + "D".repeat(900),
  }));
  const mockFollowResponse = {
    agentId: "agent_mcp_3",
    jobId: "job_mcp_3",
    status: "completed",
    resultAvailable: true,
    compact: {
      version: 1,
      status: "completed",
      claims: { summary: "summary", files: [], tests: [], risks: [] },
      mandatoryEvidence: hugeEvidence,
      receipt: null,
      tokens: null,
      decisionReady: false,
      decisionReason: "mandatory_evidence_overflow",
      detailsRef: {
        resultPath: "/p",
        hasMoreDetails: true,
        availableSections: ["evidence", "full"],
        mandatoryDetailCount: hugeEvidence.length,
        mandatorySections: ["test_failed"],
      },
    },
  };
  const bridgeClient = { call: async () => mockFollowResponse } as unknown as BridgeHttpClient;
  const server = createMcpServer(bridgeClient, { compactFollowMaxBytes: BUDGET });
  const client = new Client({ name: "test-client-3", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const res = await client.callTool({ name: "subagents_follow", arguments: { agent_id: "agent_mcp_3" } });
    const structured = res.structuredContent as Record<string, unknown>;
    const visible = Buffer.byteLength(JSON.stringify({ content: res.content, structuredContent: structured }), "utf8");
    assert.ok(visible <= BUDGET, `overflow response must respect the budget (got ${visible})`);
    const serialized = JSON.stringify(structured);
    assert.equal(serialized.includes("D".repeat(900)), false, "Evidence bulk must not be dumped");
    // The FACT of the evidence must survive: a digest plus the exact section pointer.
    assert.match(serialized, /mandatoryEvidenceSummary|mandatory_evidence_overflow|test_failed/);
    assert.match(serialized, /"exactSection":"evidence"/);
  } finally {
    await client.close();
    await server.close();
  }
});

test("token economy: validation evidence never reports an empty risk list as green", () => {
  const evidence: ValidationEvidence = classifyValidationEvidence({
    claimedStatus: "needs_approval",
    providerExecutionStatus: "success",
    status: "completed",
    tests: [],
    risks: [],
    unresolved: ["aguardando aprovação do usuário"],
    files: [],
    validationAbsent: true,
  });
  assert.equal(evidence.permissionRequired, true);
  assert.equal(evidence.validationAbsent, true);
  assert.ok(evidence.mandatory.some((item) => item.kind === "permission_required"));
  assert.ok(evidence.mandatory.some((item) => item.kind === "unresolved"));
  assert.ok(evidence.mandatory.some((item) => item.kind === "validation_absent"));
});
