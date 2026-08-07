import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { assertInside, redactSecrets, validateContextFiles } from "../../src/security.js";

test("redacts bearer, basic and key-shaped values", () => {
  const value = redactSecrets("Authorization: Bearer abc123 password=secret");
  assert.match(value, /\[REDACTED\]/);
  assert.doesNotMatch(value, /abc123/);
  assert.doesNotMatch(value, /secret/);
});

test("context files resolve relative to the workspace and reject traversal", () => {
  const root = path.resolve("workspace-fixture");
  assert.equal(validateContextFiles(root, ["notes.txt"])[0], path.join(root, "notes.txt"));
  assert.throws(() => assertInside(root, path.join(root, "..", "outside.txt")));
});
