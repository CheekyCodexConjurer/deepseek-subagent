import test from "node:test";
import assert from "node:assert/strict";
import { parseAgyOutput } from "../../src/antigravity/parser.js";
import { hasBlockingEvidence } from "../../src/result.js";

/**
 * Regression for the REAL Antigravity CLI JSON envelope captured live from the
 * installed build (agy 1.2.0):
 *
 * {"conversation_id":"...","status":"SUCCESS","response":"...",
 *  "duration_seconds":2.58,"num_turns":1,
 *  "usage":{"input_tokens":19264,"output_tokens":1,"thinking_tokens":0,
 *           "cache_read_tokens":0,"total_tokens":19265}}
 */
const REAL_SUCCESS_ENVELOPE = JSON.stringify({
  conversation_id: "cd3e125d-f2a9-49f5-9848-d5a0e07dbd9c",
  status: "SUCCESS",
  response: "OK\n",
  duration_seconds: 2.5848796,
  num_turns: 1,
  usage: {
    input_tokens: 19264,
    output_tokens: 1,
    thinking_tokens: 0,
    cache_read_tokens: 0,
    total_tokens: 19265,
  },
});

test("agy real JSON: provider SUCCESS maps to execution success, not to a semantic approval", () => {
  const parsed = parseAgyOutput(REAL_SUCCESS_ENVELOPE, "");
  assert.equal(parsed.hasJson, true);
  assert.equal(parsed.status, "completed");
  assert.equal(parsed.providerExecutionStatus, "success");
  assert.equal(parsed.conversationId, "cd3e125d-f2a9-49f5-9848-d5a0e07dbd9c");
  assert.equal(parsed.summary, "OK");
  assert.equal(parsed.durationSeconds, 2.5848796);
  assert.equal(parsed.numTurns, 1);
  assert.equal(parsed.usage?.inputTokens, 19264);
  assert.equal(parsed.usage?.outputTokens, 1);
  assert.equal(parsed.usage?.thinkingTokens, 0);
  assert.equal(parsed.usage?.cachedInputTokens, 0);
  assert.equal(parsed.usage?.totalTokens, 19265, "The observed provider total must be preserved verbatim");
  assert.equal(parsed.usage?.usageScope, "cumulative_conversation");
  assert.equal(parsed.usage?.usageSource, "observed");
  assert.equal(parsed.usage?.providerConversationId, "cd3e125d-f2a9-49f5-9848-d5a0e07dbd9c");
});

test("agy real JSON: textual protocol headings inside response are parsed, not ignored", () => {
  const envelope = JSON.stringify({
    conversation_id: "conv_protocol_1",
    status: "SUCCESS",
    response: [
      "STATUS: completed",
      "SUMMARY: implementei a correção do parser",
      "ASSUMPTIONS: none",
      "CHANGES:",
      "- src/parser.ts atualizado",
      "FILES:",
      "- src/parser.ts",
      "- test/parser.test.ts",
      "TESTS:",
      "- npm test -> FAILED",
      "- npm run lint -> PASSED",
      "RISKS:",
      "- regressão permanece",
      "UNRESOLVED:",
      "- falta validar em produção",
    ].join("\n"),
    usage: { input_tokens: 100, output_tokens: 20, thinking_tokens: 0, cache_read_tokens: 0, total_tokens: 120 },
  });

  const parsed = parseAgyOutput(envelope, "");
  assert.equal(parsed.workerClaimedStatus, "completed");
  assert.equal(parsed.providerExecutionStatus, "success");
  assert.equal(parsed.summary, "implementei a correção do parser");
  assert.deepEqual(parsed.files, ["src/parser.ts", "test/parser.test.ts"]);
  assert.deepEqual(parsed.tests, ["npm test -> FAILED", "npm run lint -> PASSED"]);
  assert.deepEqual(parsed.risks, ["regressão permanece"]);
  assert.deepEqual(parsed.unresolved, ["falta validar em produção"]);
  assert.ok(parsed.validationEvidence.testsFailed.some((t) => /FAILED/.test(t)));
  assert.equal(parsed.validationEvidence.claimEvidenceConflict, true);
});

test("agy real JSON: provider SUCCESS + failing test preserves failure evidence", () => {
  const envelope = JSON.stringify({
    conversation_id: "conv_adversarial_1",
    status: "SUCCESS",
    response: [
      "STATUS: completed",
      "SUMMARY: implementei a correção",
      "TESTS:",
      "- npm test -> FAILED",
      "RISKS:",
      "- regressão permanece",
    ].join("\n"),
    usage: { input_tokens: 10, output_tokens: 2, thinking_tokens: 0, cache_read_tokens: 0, total_tokens: 12 },
  });

  const parsed = parseAgyOutput(envelope, "");
  assert.equal(parsed.providerExecutionStatus, "success");
  assert.equal(parsed.status, "completed");
  // The failing evidence MUST survive: never tests=[] risks=[] on a green envelope.
  assert.notEqual(parsed.tests.length, 0);
  assert.notEqual(parsed.risks.length, 0);
  assert.equal(parsed.tests[0], "npm test -> FAILED");
  assert.equal(parsed.risks[0], "regressão permanece");
  const kinds = parsed.validationEvidence.mandatory.map((item) => item.kind);
  assert.ok(kinds.includes("test_failed"));
  assert.ok(kinds.includes("blocking_risk"));
  assert.ok(kinds.includes("claim_evidence_conflict"));
});

test("agy real JSON: structured top-level fields win and merge without duplication", () => {
  const envelope = JSON.stringify({
    conversation_id: "conv_merge_1",
    status: "SUCCESS",
    response: "STATUS: completed\nSUMMARY: resumo textual\nFILES:\n- src/parser.ts\n- src/extra.ts\nTESTS:\n- npm test -> PASSED",
    files: ["src/parser.ts"],
    tests: ["npm test -> PASSED"],
    risks: [],
    usage: { input_tokens: 5, output_tokens: 1, thinking_tokens: 0, cache_read_tokens: 0, total_tokens: 6 },
  });

  const parsed = parseAgyOutput(envelope, "");
  assert.deepEqual(parsed.files, ["src/parser.ts", "src/extra.ts"], "Structured file must not be duplicated");
  assert.deepEqual(parsed.tests, ["npm test -> PASSED"], "Identical test must not be duplicated");
  assert.equal(parsed.summary, "resumo textual");
});

test("agy real JSON: a run identifier is never reused as a conversation id", () => {
  const envelope = JSON.stringify({
    status: "SUCCESS",
    run_id: "5b1f9c6e-1111-2222-3333-444455556666",
    response: "STATUS: completed\nSUMMARY: ok\nTESTS:\n- npm test -> PASSED",
    usage: { input_tokens: 5, output_tokens: 1, thinking_tokens: 0, cache_read_tokens: 0, total_tokens: 6 },
  });

  const parsed = parseAgyOutput(envelope, "");
  assert.equal(parsed.conversationId, null, "run_id must not become conversationId");
  assert.equal(parsed.runId, "5b1f9c6e-1111-2222-3333-444455556666");
  assert.equal(parsed.usage?.usageScope, "unknown", "Without a conversation id the usage scope is unknown");
  assert.equal(parsed.usage?.providerConversationId, null);
});

test("agy real JSON: a missing total is null, never fabricated from thinking tokens", () => {
  const envelope = JSON.stringify({
    conversation_id: "conv_no_total",
    status: "SUCCESS",
    response: "STATUS: completed\nSUMMARY: ok",
    usage: { input_tokens: 100, output_tokens: 20, thinking_tokens: 30, cache_read_tokens: 7 },
  });

  const parsed = parseAgyOutput(envelope, "");
  assert.equal(parsed.usage?.totalTokens, null, "Total must not be invented by summing thinking tokens");
  assert.equal(parsed.usage?.thinkingTokens, 30);
  assert.equal(parsed.usage?.cachedInputTokens, 7);
  assert.equal(parsed.usage?.inputTokens, 100);
});

test("agy real JSON: provider failure maps to a failure execution status", () => {
  const envelope = JSON.stringify({
    conversation_id: "conv_failure",
    status: "ERROR",
    response: "STATUS: failed\nSUMMARY: não foi possível concluir",
    usage: { input_tokens: 5, output_tokens: 1, thinking_tokens: 0, cache_read_tokens: 0, total_tokens: 6 },
  });

  const parsed = parseAgyOutput(envelope, "");
  assert.equal(parsed.providerExecutionStatus, "failure");
  assert.equal(parsed.status, "failed");
  assert.equal(parsed.workerClaimedStatus, "failed");
  assert.ok(parsed.validationEvidence.mandatory.some((item) => item.kind === "worker_failure"));
});

test("agy real JSON: unknown provider status fails closed instead of claiming completion", () => {
  const envelope = JSON.stringify({
    conversation_id: "conv_unknown",
    status: "SOMETHING_NEW",
    response: "STATUS: completed\nSUMMARY: ok",
  });
  const parsed = parseAgyOutput(envelope, "");
  assert.equal(parsed.status, null);
  assert.equal(parsed.providerExecutionStatus, "unknown");
  assert.equal(parsed.hasJson, true);
});

test("agy real JSON: SUCCESS with an auto-denied action and empty response never looks green", () => {
  // Captured live from agy 1.2.7 running headless without command permission.
  const envelope = JSON.stringify({
    conversation_id: "26105946-3757-4ef4-aeaf-b7675be12834",
    status: "SUCCESS",
    response: "",
    duration_seconds: 13.58,
    num_turns: 1,
    usage: { input_tokens: 25731, output_tokens: 904, thinking_tokens: 557, cache_read_tokens: 48895, total_tokens: 26635 },
    denied_actions: [{ action: "command", display_name: "RunCommand" }],
  });
  const parsed = parseAgyOutput(envelope, "");

  assert.equal(parsed.providerExecutionStatus, "success");
  assert.equal(parsed.deniedActions.length, 1);
  assert.equal(parsed.deniedActions[0], "RunCommand");

  const kinds = parsed.validationEvidence.mandatory.map((item) => item.kind);
  assert.ok(kinds.includes("action_denied"), "A denied action must be mandatory evidence");
  assert.ok(kinds.includes("worker_failure"), "An empty response must count as a worker failure");
  assert.equal(parsed.validationEvidence.emptyResult, true);
  assert.equal(parsed.validationEvidence.permissionRequired, true);
  assert.equal(parsed.validationEvidence.workerFailure, true);
  assert.equal(hasBlockingEvidence(parsed.validationEvidence.mandatory), true);
});

test("agy real JSON: denied actions are surfaced for every shape the CLI may emit", () => {
  for (const denied of [
    [{ action: "command", display_name: "RunCommand" }],
    [{ action: "command", displayName: "RunCommand" }],
    [{ action: "command" }],
    ["RunCommand"],
  ]) {
    const envelope = JSON.stringify({
      conversation_id: "conv_denied_shape",
      status: "SUCCESS",
      response: "STATUS: completed\nSUMMARY: done",
      denied_actions: denied,
    });
    const parsed = parseAgyOutput(envelope, "");
    assert.equal(parsed.deniedActions.length, 1, `shape ${JSON.stringify(denied)} must yield one action`);
    assert.ok(parsed.validationEvidence.mandatory.some((item) => item.kind === "action_denied"));
  }
});
