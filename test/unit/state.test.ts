import test from "node:test";
import assert from "node:assert/strict";
import { assertAgentTransition, assertJobTransition, canTransitionJob } from "../../src/state.js";

test("job state machine permits asynchronous delivery", () => {
  assert.equal(canTransitionJob("running", "completed"), true);
  assert.equal(canTransitionJob("completed", "delivery_pending"), true);
  assert.equal(canTransitionJob("delivery_pending", "delivered"), true);
  assert.throws(() => assertJobTransition("delivered", "running"));
});

test("agent state machine allows continuation after completion", () => {
  assert.doesNotThrow(() => assertAgentTransition("completed", "working"));
  assert.throws(() => assertAgentTransition("closed", "working"));
});
