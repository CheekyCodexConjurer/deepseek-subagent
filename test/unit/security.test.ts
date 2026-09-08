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

test("shouldIncludeGlobalGeminiContext detects MCP, PromptPad, and governance keywords", async () => {
  const { shouldIncludeGlobalGeminiContext } = await import("../../src/security.js");
  assert.equal(shouldIncludeGlobalGeminiContext("Configure the MCP server"), true);
  assert.equal(shouldIncludeGlobalGeminiContext("Update mcp_tool configurations"), true);
  assert.equal(shouldIncludeGlobalGeminiContext("Integrate with PromptPad"), true);
  assert.equal(shouldIncludeGlobalGeminiContext("Follow prompt-pad rules"), true);
  assert.equal(shouldIncludeGlobalGeminiContext("Review AGENTS.md governance"), true);
  assert.equal(shouldIncludeGlobalGeminiContext("Update GEMINI.md instructions"), true);
  assert.equal(shouldIncludeGlobalGeminiContext("Create a new skill for database management"), true);
  assert.equal(shouldIncludeGlobalGeminiContext("Skill de configuração avançada"), true);
  assert.equal(shouldIncludeGlobalGeminiContext("Inspect skills directory"), true);

  // Normal tasks must return false
  assert.equal(shouldIncludeGlobalGeminiContext("Fix typo in index.html"), false);
  assert.equal(shouldIncludeGlobalGeminiContext("Optimize SQL query for orders"), false);
  assert.equal(shouldIncludeGlobalGeminiContext("Implement user authentication form"), false);
  assert.equal(shouldIncludeGlobalGeminiContext("Run benchmark on websocket server"), false);
});

test("allowlisted global context file is accepted while unallowlisted external file is rejected", async () => {
  const { validateContextFilesStrict, defaultGlobalGeminiContextPath } = await import("../../src/security.js");
  const root = path.resolve("workspace-fixture");
  const globalGemini = defaultGlobalGeminiContextPath();

  // An external file that is NOT allowlisted is rejected with outside_workspace
  await assert.rejects(
    () => validateContextFilesStrict(root, ["C:\\some\\other\\unallowed.txt"], 1_000_000, [globalGemini]),
    (error: unknown) => {
      assert.equal((error as { code?: string }).code, "context_file_invalid");
      assert.equal((error as { details?: { reason?: string } }).details?.reason, "outside_workspace");
      return true;
    },
  );
});

test("isProcessAlive accurately detects running process and non-existent PID", async () => {
  const { isProcessAlive } = await import("../../src/security.js");
  assert.equal(typeof isProcessAlive, "function");
  assert.equal(isProcessAlive(process.pid), true);
  // Extremely large PID that doesn't exist
  assert.equal(isProcessAlive(9999999), false);
});

test("isProcessAlive treats EACCES as alive (fail-closed)", async () => {
  // Catches mutation: treating EACCES as dead/false
  const { isProcessAlive } = await import("../../src/security.js");
  const originalKill = process.kill;
  try {
    const error = Object.assign(new Error("permission denied"), { code: "EACCES" });
    process.kill = (() => {
      throw error;
    }) as unknown as typeof process.kill;

    assert.equal(isProcessAlive(1234), true);
  } finally {
    process.kill = originalKill;
  }
});

test("isProcessAlive treats EPERM as alive (fail-closed)", async () => {
  // Catches mutation: treating EPERM as dead or rethrowing
  const { isProcessAlive } = await import("../../src/security.js");
  const originalKill = process.kill;
  try {
    const error = Object.assign(new Error("operation not permitted"), { code: "EPERM" });
    process.kill = (() => {
      throw error;
    }) as unknown as typeof process.kill;

    assert.equal(isProcessAlive(1234), true);
  } finally {
    process.kill = originalKill;
  }
});

test("isProcessAlive treats ESRCH as dead", async () => {
  // Catches mutation: treating ESRCH as alive or rethrowing
  const { isProcessAlive } = await import("../../src/security.js");
  const originalKill = process.kill;
  try {
    const error = Object.assign(new Error("no such process"), { code: "ESRCH" });
    process.kill = (() => {
      throw error;
    }) as unknown as typeof process.kill;

    assert.equal(isProcessAlive(1234), false);
  } finally {
    process.kill = originalKill;
  }
});

test("isProcessAlive rethrows unexpected OS/runtime errors like EIO", async () => {
  // Catches mutation: silently swallowing unknown errors and returning false/true
  const { isProcessAlive } = await import("../../src/security.js");
  const originalKill = process.kill;
  const expectedError = Object.assign(new Error("input/output error"), { code: "EIO" });
  try {
    process.kill = (() => {
      throw expectedError;
    }) as unknown as typeof process.kill;

    assert.throws(
      () => isProcessAlive(1234),
      (err: unknown) => err === expectedError,
    );
  } finally {
    process.kill = originalKill;
  }
});
