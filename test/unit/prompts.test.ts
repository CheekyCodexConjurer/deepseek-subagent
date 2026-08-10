import test from "node:test";
import assert from "node:assert/strict";
import { buildWorkerPrompt } from "../../src/prompts.js";

const ANALYZE_RULE = "Inspect and reason only. Do not edit files, configuration, package state, or Git history.";
const EDIT_RULE = "Implement the requested change with the smallest safe diff. Preserve unrelated worktree changes.";
const CONTINUE_RULE = "Continue under the operating mode already established in this OpenCode session.";
const FINALIZE_SCOPE_RULE =
  "Any prior GRACEFUL_FINALIZE_PROMPT stop was scoped to the expired job; this accepted continuation authorizes the current task without broadening the session's original permissions.";

function build(input: Record<string, unknown>): Promise<string> {
  return buildWorkerPrompt(input as never, "E:\\work", { maxLength: 100_000 });
}

test("spawn prompt in analyze mode keeps the explicit analyze operating rule", async () => {
  const prompt = await build({
    requestId: "request_analyze",
    topic: "Analyze",
    task: "Inspect the fixture",
    mode: "analyze",
    workspaceStrategy: "shared",
  });
  assert.match(prompt, /Operating rule: Inspect and reason only\. Do not edit files/);
  assert.doesNotMatch(prompt, /Operating rule: Implement the requested change/);
  assert.doesNotMatch(prompt, new RegExp(CONTINUE_RULE));
  assert.doesNotMatch(prompt, /GRACEFUL_FINALIZE_PROMPT stop/);
});

test("spawn prompt in edit mode keeps the explicit edit operating rule", async () => {
  const prompt = await build({
    requestId: "request_edit",
    topic: "Edit",
    task: "Apply the fix",
    mode: "edit",
    workspaceStrategy: "worktree",
  });
  assert.match(prompt, /Operating rule: Implement the requested change with the smallest safe diff/);
  assert.doesNotMatch(prompt, /Inspect and reason only/);
  assert.doesNotMatch(prompt, new RegExp(CONTINUE_RULE));
  assert.doesNotMatch(prompt, /GRACEFUL_FINALIZE_PROMPT stop/);
});

test("continuation prompt does not downgrade to analyze and states the inherited mode", async () => {
  const prompt = await build({
    task: "Fix the continuation bug",
    relation: "correction",
  });
  assert.match(prompt, /Request relation: correction/);
  assert.doesNotMatch(prompt, new RegExp(ANALYZE_RULE));
  assert.doesNotMatch(prompt, new RegExp(EDIT_RULE));
  assert.match(prompt, new RegExp("Operating rule: " + CONTINUE_RULE));
  assert.match(prompt, new RegExp(FINALIZE_SCOPE_RULE));
});

test("continuation prompt keeps the relation label and task", async () => {
  const prompt = await build({
    task: "Answer the review findings",
    relation: "review",
  });
  assert.match(prompt, /Request relation: review/);
  assert.match(prompt, new RegExp("Task:\nAnswer the review findings"));
});
