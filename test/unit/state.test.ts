import test from "node:test";
import assert from "node:assert/strict";
import { assertAgentTransition, assertJobTransition, canTransitionJob } from "../../src/state.js";

test("job state machine permits asynchronous delivery", () => {
  assert.equal(canTransitionJob("running", "completed"), true);
  assert.equal(canTransitionJob("running", "following"), true);
  assert.equal(canTransitionJob("following", "finalizing"), true);
  assert.equal(canTransitionJob("finalizing", "completed_partial"), true);
  assert.equal(canTransitionJob("finalizing", "timed_out"), true);
  assert.equal(canTransitionJob("completed", "delivery_pending"), true);
  assert.equal(canTransitionJob("delivery_pending", "delivered"), true);
  assert.throws(() => assertJobTransition("delivered", "running"));
});

test("agent state machine allows continuation after completion", () => {
  assert.doesNotThrow(() => assertAgentTransition("completed", "working"));
  assert.doesNotThrow(() => assertAgentTransition("working", "timed_out"));
  assert.doesNotThrow(() => assertAgentTransition("completed_partial", "working"));
  assert.throws(() => assertAgentTransition("closed", "working"));
});
