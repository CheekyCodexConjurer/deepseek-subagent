import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { buildAgyArgs } from "../../src/antigravity/args.js";
import { AntigravityAdapter } from "../../src/antigravity/adapter.js";
import { extractAgyJson, parseAgyOutput, parseAgyStatus } from "../../src/antigravity/parser.js";
import { AntigravityProcessError, runAgy } from "../../src/antigravity/runner.js";
import { InvalidRequestError } from "../../src/errors.js";

const fixturePath = fileURLToPath(new URL("../fixtures/agy.cjs", import.meta.url));
const fixtureArgs = buildAgyArgs("runner fixture task", {});

function fixtureSpawn(behavior: string, calls: string[] = []) {
  return (command: string, args: string[], options: { cwd: string; env?: NodeJS.ProcessEnv; shell: false; windowsHide: boolean; stdio: ReadonlyArray<"ignore" | "pipe"> }) => {
    calls.push(command);
    return spawn(process.execPath, [fixturePath, ...args], {
      cwd: options.cwd,
      env: { ...process.env, ...(options.env ?? {}), AGY_FIXTURE: behavior },
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
  };
}

test("buildAgyArgs matches the smoke-observed contract with the prompt last", () => {
  assert.deepEqual(buildAgyArgs("do the thing", {}), [
    "--model",
    "gemini-3.7-flash-high",
    "-p",
    "do the thing",
    "--print-timeout",
    "15m",
  ]);
});

test("buildAgyArgs honors model and print-timeout overrides", () => {
  assert.deepEqual(buildAgyArgs("x", { model: "gemini-3.6-pro", printTimeout: "5m" }), [
    "--model",
    "gemini-3.6-pro",
    "-p",
    "x",
    "--print-timeout",
    "5m",
  ]);
});

test("buildAgyArgs adds the lab-only sandbox permission flags before the prompt", () => {
  assert.deepEqual(buildAgyArgs("x", {
    sandbox: true,
    addDirs: ["C:\\lab\\external-a", "C:\\lab\\external-b"],
    dangerouslySkipPermissions: true,
  }), [
    "--model",
    "gemini-3.7-flash-high",
    "--sandbox",
    "--add-dir",
    "C:\\lab\\external-a",
    "--add-dir",
    "C:\\lab\\external-b",
    "--dangerously-skip-permissions",
    "-p",
    "x",
    "--print-timeout",
    "15m",
  ]);
});

test("buildAgyArgs passes auto-approval independently of the sandbox flag", () => {
  const args = buildAgyArgs("x", { dangerouslySkipPermissions: true });
  assert.ok(args.includes("--dangerously-skip-permissions"));
  assert.ok(!args.includes("--sandbox"), "auto-approval must not force the sandbox");
  const sandboxed = buildAgyArgs("x", { sandbox: true });
  assert.ok(sandboxed.includes("--sandbox"));
  assert.ok(!sandboxed.includes("--dangerously-skip-permissions"), "the sandbox must not force auto-approval");
});

const ENVELOPE = {
  status: "success",
  runId: "run_fixture_1",
  summary: "Fixture summary: task completed without quota.",
  files: ["src/example.ts"],
  tests: ["npm test"],
  risks: ["fixture only; no real inference"],
  diffSummary: "1 file changed",
};

test("parseAgyOutput parses a whole-JSON stdout into the bridge result contract", () => {
  const parsed = parseAgyOutput(JSON.stringify(ENVELOPE) + "\n", "");
  assert.equal(parsed.hasJson, true);
  assert.equal(parsed.status, "completed");
  assert.equal(parsed.runId, "run_fixture_1");
  assert.equal(parsed.summary, ENVELOPE.summary);
  assert.deepEqual(parsed.files, ["src/example.ts"]);
  assert.deepEqual(parsed.tests, ["npm test"]);
  assert.deepEqual(parsed.risks, [ENVELOPE.risks[0]]);
  assert.equal(parsed.diffSummary, "1 file changed");
});

test("extractAgyJson finds fenced and marker-wrapped JSON", () => {
  assert.equal(extractAgyJson("```json\n{\"summary\":\"a\"}\n```")?.summary, "a");
  assert.equal(extractAgyJson("AGY_JSON:\n{\"summary\":\"b\"}")?.summary, "b");
  assert.equal(extractAgyJson("plain text only"), null);
});

test("parseAgyOutput falls back to text summaries and tolerates alias keys", () => {
  const text = parseAgyOutput("Plain fixture summary.\n", "");
  assert.equal(text.hasJson, false);
  assert.equal(text.status, null);
  assert.equal(text.summary, "Plain fixture summary.");
  const aliased = parseAgyOutput(JSON.stringify({
    output: "aliased summary",
    changedFiles: ["a.ts", "b.ts"],
    test_results: "unit\nintegration",
    warnings: ["w1"],
    diff: "short diff",
  }), "");
  assert.equal(aliased.hasJson, true);
  assert.equal(aliased.summary, "aliased summary");
  assert.deepEqual(aliased.files, ["a.ts", "b.ts"]);
  assert.deepEqual(aliased.tests, ["unit", "integration"]);
  assert.deepEqual(aliased.risks, ["w1"]);
  assert.equal(aliased.diffSummary, "short diff");
});

test("parseAgyStatus is fail-closed: unknown and missing status values return null", () => {
  assert.equal(parseAgyStatus("error"), "failed");
  assert.equal(parseAgyStatus("timeout"), "timed_out");
  assert.equal(parseAgyStatus("cancelled"), "aborted");
  assert.equal(parseAgyStatus("partial"), "completed_partial");
  assert.equal(parseAgyStatus("weird"), null);
  assert.equal(parseAgyStatus(null), null);
  assert.equal(parseAgyStatus(""), null);
});

test("runAgy captures a successful fixture run", async () => {
  const result = await runAgy(fixtureArgs, { command: "node", cwd: process.cwd(), spawnFn: fixtureSpawn("ok") });
  assert.equal(result.code, 0);
  const parsed = JSON.parse(result.stdout) as { summary?: string };
  assert.equal(parsed.summary, ENVELOPE.summary);
});

test("runAgy rejects a non-zero exit with kind exit and the exit code", async () => {
  await assert.rejects(
    () => runAgy(fixtureArgs, { command: "node", cwd: process.cwd(), spawnFn: fixtureSpawn("fail") }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "exit");
      assert.equal(error.code, 1);
      assert.match(error.message, /quota exceeded/);
      return true;
    },
  );
});

test("runAgy rejects a missing binary with kind spawn", async () => {
  await assert.rejects(
    () => runAgy(fixtureArgs, { command: "definitely-not-a-real-binary-xyz", cwd: process.cwd() }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "spawn");
      return true;
    },
  );
});

test("runAgy rejects a synchronous spawn throw with kind spawn", async () => {
  await assert.rejects(
    () => runAgy(fixtureArgs, {
      command: "node",
      cwd: process.cwd(),
      spawnFn: () => {
        throw new Error("boom");
      },
    }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "spawn");
      assert.match(error.message, /boom/);
      return true;
    },
  );
});

test("runAgy caps captured stdout at the configured byte limit", async () => {
  const result = await runAgy(fixtureArgs, {
    command: "node",
    cwd: process.cwd(),
    maxOutputBytes: 64,
    spawnFn: fixtureSpawn("big"),
  });
  assert.equal(result.code, 0);
  assert.equal(result.stdout.length, 64);
  assert.equal(result.stdout, "x".repeat(64));
});

test("runAgy kills a hanging run on timeout and rejects with kind timeout", async () => {
  await assert.rejects(
    () => runAgy(fixtureArgs, { command: "node", cwd: process.cwd(), timeoutMs: 100, spawnFn: fixtureSpawn("hang") }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "timeout");
      assert.match(error.message, /100ms/);
      return true;
    },
  );
});

test("runAgy rejects with kind aborted when the caller cancels mid-run", async () => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 50);
  try {
    await assert.rejects(
      () => runAgy(fixtureArgs, { command: "node", cwd: process.cwd(), signal: controller.signal, spawnFn: fixtureSpawn("slow") }),
      (error: unknown) => {
        assert.ok(error instanceof AntigravityProcessError);
        assert.equal(error.kind, "aborted");
        return true;
      },
    );
  } finally {
    clearTimeout(timer);
  }
});

test("runAgy rejects a pre-aborted signal without spawning", async () => {
  const calls: string[] = [];
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    () => runAgy(fixtureArgs, { command: "node", cwd: process.cwd(), signal: controller.signal, spawnFn: fixtureSpawn("ok", calls) }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "aborted");
      return true;
    },
  );
  assert.equal(calls.length, 0);
});

test("runAgy ignores an abort signal fired after the run completed", async () => {
  const controller = new AbortController();
  const result = await runAgy(fixtureArgs, { command: "node", cwd: process.cwd(), signal: controller.signal, spawnFn: fixtureSpawn("ok") });
  controller.abort();
  assert.equal(result.code, 0);
  const parsed = JSON.parse(result.stdout) as { summary?: string };
  assert.equal(parsed.summary, ENVELOPE.summary);
});

test("AntigravityAdapter passes auto-approval independently of the sandbox", async () => {
  const recorded: string[][] = [];
  const adapter = new AntigravityAdapter({
    command: "node",
    dangerouslySkipPermissions: true,
    spawnFn: (command, args, options) => {
      recorded.push(args);
      return fixtureSpawn("ok")(command, args, options);
    },
  });
  const result = await adapter.runPrompt({ prompt: "approve task", cwd: process.cwd() });
  const args = recorded[0] ?? [];
  assert.ok(args.includes("--dangerously-skip-permissions"));
  assert.ok(!args.includes("--sandbox"), "an unsandboxed adapter run must not add the sandbox flag");
  assert.equal(result.status, "completed");

  const sandboxed: string[][] = [];
  const sandboxOnly = new AntigravityAdapter({
    command: "node",
    sandbox: true,
    spawnFn: (command, args, options) => {
      sandboxed.push(args);
      return fixtureSpawn("ok")(command, args, options);
    },
  });
  const sandboxResult = await sandboxOnly.runPrompt({ prompt: "sandbox task", cwd: process.cwd() });
  const sandboxArgs = sandboxed[0] ?? [];
  assert.ok(sandboxArgs.includes("--sandbox"));
  assert.ok(!sandboxArgs.includes("--dangerously-skip-permissions"), "the sandbox alone must not add auto-approval");
  assert.equal(sandboxResult.status, "completed");
});

test("AntigravityAdapter maps a fixture run into the bridge result contract", async () => {
  const adapter = new AntigravityAdapter({ command: "node", spawnFn: fixtureSpawn("ok") });
  const result = await adapter.runPrompt({ prompt: "Solve the fixture task", cwd: process.cwd() });
  assert.equal(result.status, "completed");
  assert.equal(result.runId, "run_fixture_1");
  assert.equal(result.summary, ENVELOPE.summary);
  assert.deepEqual(result.files, ["src/example.ts"]);
  assert.deepEqual(result.tests, ["npm test"]);
  assert.equal(result.diffSummary, "1 file changed");
  assert.equal(result.model, "gemini-3.7-flash-high");
  assert.equal(result.modelDisplayName, "Antigravity · gemini-3.7-flash-high");
  assert.equal(result.workspace, process.cwd());
  assert.match(result.rawOutput, /run_fixture_1/);
});

test("AntigravityAdapter treats plain text under the smoke contract as completed", async () => {
  const adapter = new AntigravityAdapter({ command: "node", spawnFn: fixtureSpawn("text") });
  const result = await adapter.runPrompt({ prompt: "text task", cwd: process.cwd() });
  assert.equal(result.status, "completed");
  assert.equal(result.summary, "Fixture text summary: completed.");
  assert.deepEqual(result.files, []);
});

test("AntigravityAdapter fails closed on JSON without a recognized status", async () => {
  const adapter = new AntigravityAdapter({ command: "node", spawnFn: fixtureSpawn("nostatus") });
  await assert.rejects(
    () => adapter.runPrompt({ prompt: "nostatus task", cwd: process.cwd() }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "invalid_output");
      assert.match(error.message, /refusing to claim completion/);
      return true;
    },
  );
});

test("AntigravityAdapter fails closed on empty stdout and stderr", async () => {
  const adapter = new AntigravityAdapter({ command: "node", spawnFn: fixtureSpawn("empty") });
  await assert.rejects(
    () => adapter.runPrompt({ prompt: "empty task", cwd: process.cwd() }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "invalid_output");
      assert.match(error.message, /without producing any output/);
      return true;
    },
  );
});

test("AntigravityAdapter fails closed on an empty prompt with the bridge typed 400 before spawning", async () => {
  const calls: string[] = [];
  const adapter = new AntigravityAdapter({ command: "node", spawnFn: fixtureSpawn("ok", calls) });
  await assert.rejects(
    () => adapter.runPrompt({ prompt: "   ", cwd: process.cwd() }),
    (error: unknown) => {
      assert.ok(error instanceof InvalidRequestError);
      assert.equal(error.code, "invalid_request");
      return true;
    },
  );
  assert.equal(calls.length, 0);
});

test("AntigravityAdapter never falls back: exactly one spawn on error, timeout and cancellation", async () => {
  const failCalls: string[] = [];
  const failAdapter = new AntigravityAdapter({ command: "node", spawnFn: fixtureSpawn("fail", failCalls) });
  await assert.rejects(
    () => failAdapter.runPrompt({ prompt: "fail", cwd: process.cwd() }),
    (error: unknown) => error instanceof AntigravityProcessError && error.kind === "exit",
  );
  assert.equal(failCalls.length, 1);

  const hangCalls: string[] = [];
  const hangAdapter = new AntigravityAdapter({ command: "node", timeoutMs: 100, spawnFn: fixtureSpawn("hang", hangCalls) });
  await assert.rejects(
    () => hangAdapter.runPrompt({ prompt: "hang", cwd: process.cwd() }),
    (error: unknown) => error instanceof AntigravityProcessError && error.kind === "timeout",
  );
  assert.equal(hangCalls.length, 1);

  const slowCalls: string[] = [];
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 50);
  const cancelAdapter = new AntigravityAdapter({ command: "node", spawnFn: fixtureSpawn("slow", slowCalls) });
  try {
    await assert.rejects(
      () => cancelAdapter.runPrompt({ prompt: "cancel", cwd: process.cwd(), signal: controller.signal }),
      (error: unknown) => error instanceof AntigravityProcessError && error.kind === "aborted",
    );
  } finally {
    clearTimeout(timer);
  }
  assert.equal(slowCalls.length, 1);
});
