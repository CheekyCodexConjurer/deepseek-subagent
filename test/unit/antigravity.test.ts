import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { AGY_MAX_PROMPT_LENGTH, AGY_PRINT_TIMEOUT_UNLIMITED, buildAgyArgs, formatPrintTimeout } from "../../src/antigravity/args.js";
import { AntigravityAdapter } from "../../src/antigravity/adapter.js";
import { extractAgyJson, parseAgyOutput, parseAgyStatus } from "../../src/antigravity/parser.js";
import { AntigravityProcessError, runAgy, type AgyProcessResult, type SpawnLike } from "../../src/antigravity/runner.js";
import { AntigravitySpool } from "../../src/antigravity/spool.js";
import { AntigravitySupervisor } from "../../src/antigravity/supervisor.js";
import type { AntigravityAttemptManifest, AntigravityAttemptStatus } from "../../src/antigravity/types.js";
import { writePrivateFile } from "../../src/security.js";
import { InvalidRequestError } from "../../src/errors.js";

const fixturePath = fileURLToPath(new URL("../fixtures/agy.cjs", import.meta.url));
const fixtureArgs = buildAgyArgs("runner fixture task", {});

function fixtureSpawn(behavior: string, calls: string[] = [], spawned: ChildProcess[] = []) {
  return (command: string, args: string[], options: { cwd: string; env?: NodeJS.ProcessEnv; shell: false; windowsHide: boolean; stdio: ReadonlyArray<"ignore" | "pipe"> }) => {
    calls.push(command);
    const child = spawn(process.execPath, [fixturePath, ...args], {
      cwd: options.cwd,
      env: { ...process.env, ...(options.env ?? {}), AGY_FIXTURE: behavior },
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    spawned.push(child);
    return child;
  };
}

async function killProcessTree(pid: number): Promise<void> {
  if (process.platform === "win32") {
    await new Promise<void>((resolve) => {
      try {
        const killer = spawn("taskkill.exe", ["/PID", String(pid), "/T", "/F"], {
          shell: false,
          windowsHide: true,
          stdio: "ignore",
        });
        killer.once("error", () => resolve());
        killer.once("close", () => resolve());
      } catch {
        resolve();
      }
    });
  } else {
    try {
      process.kill(pid, "SIGKILL");
    } catch {}
  }
}

async function cleanupSupervisor(
  controller: AbortController,
  runPromise: Promise<any> | null,
  tempDir?: string,
  processes?: ChildProcess[],
): Promise<void> {
  controller.abort();
  if (runPromise) {
    await runPromise.catch(() => {});
  }
  if (processes) {
    for (const child of processes) {
      if (child.pid) {
        try {
          process.kill(child.pid, 0);
          await killProcessTree(child.pid);
        } catch {}
      }
    }
  }
  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
}

test("buildAgyArgs matches the smoke-observed contract with sentinel 2562047h47m16s by default", () => {
  assert.deepEqual(buildAgyArgs("do the thing", {}), [
    "--model",
    "gemini-3.8-flash-high",
    "-p",
    "do the thing",
    "--print-timeout",
    AGY_PRINT_TIMEOUT_UNLIMITED,
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

test("buildAgyArgs adds the lab-only sandbox permission flags before the prompt with sentinel print-timeout", () => {
  assert.deepEqual(buildAgyArgs("x", {
    sandbox: true,
    addDirs: ["C:\\lab\\external-a", "C:\\lab\\external-b"],
    dangerouslySkipPermissions: true,
  }), [
    "--model",
    "gemini-3.8-flash-high",
    "--sandbox",
    "--add-dir",
    "C:\\lab\\external-a",
    "--add-dir",
    "C:\\lab\\external-b",
    "--dangerously-skip-permissions",
    "-p",
    "x",
    "--print-timeout",
    AGY_PRINT_TIMEOUT_UNLIMITED,
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
  const controller = new AbortController();
  const spawnedProcesses: ChildProcess[] = [];
  try {
    await assert.rejects(
      () => runAgy(fixtureArgs, { command: "node", cwd: process.cwd(), timeoutMs: 100, signal: controller.signal, spawnFn: fixtureSpawn("hang", [], spawnedProcesses) }),
      (error: unknown) => {
        assert.ok(error instanceof AntigravityProcessError);
        assert.equal(error.kind, "timeout");
        assert.match(error.message, /100ms/);
        return true;
      },
    );
  } finally {
    await cleanupSupervisor(controller, null, undefined, spawnedProcesses);
  }
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
  assert.equal(result.model, "gemini-3.8-flash-high");
  assert.equal(result.modelDisplayName, "Antigravity · gemini-3.8-flash-high");
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
  const hangProcesses: ChildProcess[] = [];
  const hangController = new AbortController();
  const hangAdapter = new AntigravityAdapter({ command: "node", timeoutMs: 100, spawnFn: fixtureSpawn("hang", hangCalls, hangProcesses) });
  try {
    await assert.rejects(
      () => hangAdapter.runPrompt({ prompt: "hang", cwd: process.cwd(), signal: hangController.signal }),
      (error: unknown) => error instanceof AntigravityProcessError && error.kind === "timeout",
    );
  } finally {
    await cleanupSupervisor(hangController, null, undefined, hangProcesses);
  }
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

test("formatPrintTimeout formats milliseconds to minutes or seconds correctly", () => {
  assert.equal(formatPrintTimeout(900_000), "15m");
  assert.equal(formatPrintTimeout(300_000), "5m");
  assert.equal(formatPrintTimeout(60_000), "1m");
  assert.equal(formatPrintTimeout(30_000), "30s");
  assert.equal(formatPrintTimeout(1_000), "1s");
  assert.equal(formatPrintTimeout(500), "1s");
});

test("buildAgyArgs derives --print-timeout from timeoutMs when printTimeout is not specified", () => {
  const args = buildAgyArgs("test task", { timeoutMs: 300_000 });
  assert.deepEqual(args, [
    "--model",
    "gemini-3.8-flash-high",
    "-p",
    "test task",
    "--print-timeout",
    "5m",
  ]);
});

test("extractAgyJson does not confuse embedded markdown json code blocks with protocol envelopes", () => {
  const markdownText = `Here is the configuration you requested:
\`\`\`json
{
  "key": "value",
  "port": 8080
}
\`\`\`
All tests have passed.`;
  assert.equal(extractAgyJson(markdownText), null);
  const parsed = parseAgyOutput(markdownText, "");
  assert.equal(parsed.hasJson, false);
  assert.equal(parsed.status, null);
  assert.equal(parsed.summary, markdownText);
});

test("extractAgyJson extracts whole-stdout and whole-fenced envelopes", () => {
  const wholeJson = JSON.stringify({ status: "completed", summary: "done" });
  assert.deepEqual(extractAgyJson(wholeJson), { status: "completed", summary: "done" });

  const wholeFenced = `\`\`\`json\n{"status":"completed","summary":"done"}\n\`\`\``;
  assert.deepEqual(extractAgyJson(wholeFenced), { status: "completed", summary: "done" });

  const markerJson = `AGY_JSON:\n{"status":"completed","summary":"done"}`;
  assert.deepEqual(extractAgyJson(markerJson), { status: "completed", summary: "done" });
});

test("AntigravityAdapter succeeds on legitimate text response with embedded markdown code blocks", async () => {
  const markdownOutput = `Task completed successfully.
\`\`\`json
{
  "api": "v1",
  "active": true
}
\`\`\`
No further action needed.`;
  // Use a custom spawn mock returning text with embedded json
  const customAdapter = new AntigravityAdapter({
    command: "node",
    spawnFn: () => {
      const child = spawn(process.execPath, ["-e", `process.stdout.write(${JSON.stringify(markdownOutput)}); process.exit(0);`], {
        cwd: process.cwd(),
        shell: false,
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      });
      return child;
    },
  });
  const result = await customAdapter.runPrompt({ prompt: "give config", cwd: process.cwd() });
  assert.equal(result.status, "completed");
  assert.equal(result.summary, markdownOutput);
});

test("AntigravityAdapter fails closed on prompt exceeding safe command line length before spawning", async () => {
  const calls: string[] = [];
  const adapter = new AntigravityAdapter({ command: "node", spawnFn: fixtureSpawn("ok", calls) });
  const oversizedPrompt = "a".repeat(AGY_MAX_PROMPT_LENGTH + 1);
  await assert.rejects(
    () => adapter.runPrompt({ prompt: oversizedPrompt, cwd: process.cwd() }),
    (error: unknown) => {
      assert.ok(error instanceof InvalidRequestError);
      assert.equal(error.code, "invalid_request");
      assert.match(error.message, /exceeds the maximum safe argument length/);
      return true;
    },
  );
  assert.equal(calls.length, 0);
});

test("buildAgyArgs refutes omission and represents null/undefined timeout as sentinel 2562047h47m16s", () => {
  const argsDefault = buildAgyArgs("task default", {});
  assert.equal(argsDefault.includes("--print-timeout"), true);
  assert.equal(argsDefault[argsDefault.indexOf("--print-timeout") + 1], AGY_PRINT_TIMEOUT_UNLIMITED);

  const argsNull = buildAgyArgs("task null", { timeoutMs: null });
  assert.equal(argsNull.includes("--print-timeout"), true);
  assert.equal(argsNull[argsNull.indexOf("--print-timeout") + 1], AGY_PRINT_TIMEOUT_UNLIMITED);

  const argsUndefined = buildAgyArgs("task undefined", { timeoutMs: undefined });
  assert.equal(argsUndefined.includes("--print-timeout"), true);
  assert.equal(argsUndefined[argsUndefined.indexOf("--print-timeout") + 1], AGY_PRINT_TIMEOUT_UNLIMITED);

  const argsPositive = buildAgyArgs("task positive", { timeoutMs: 120_000 });
  assert.equal(argsPositive.includes("--print-timeout"), true);
  assert.equal(argsPositive[argsPositive.indexOf("--print-timeout") + 1], "2m");
});

test("runAgy in unlimited mode stays alive beyond short deadline until explicit abort", async () => {
  const controller = new AbortController();
  const spawnedProcesses: ChildProcess[] = [];
  const start = Date.now();
  let runPromise: Promise<AgyProcessResult> | null = null;
  try {
    runPromise = runAgy(fixtureArgs, {
      command: "node",
      cwd: process.cwd(),
      // No timeoutMs specified (unlimited mode default)
      signal: controller.signal,
      spawnFn: fixtureSpawn("hang", [], spawnedProcesses),
    });

    // Wait 150ms — which is beyond a typical short deadline (e.g. 50ms-100ms)
    await new Promise((r) => setTimeout(r, 150));

    // The runner must still be alive!
    controller.abort();

    await assert.rejects(
      runPromise,
      (error: unknown) => {
        assert.ok(error instanceof AntigravityProcessError);
        assert.equal(error.kind, "aborted");
        return true;
      },
    );

    const elapsed = Date.now() - start;
    assert.ok(elapsed >= 140, "Runner must have stayed alive until aborted, elapsed: " + elapsed + "ms");
  } finally {
    await cleanupSupervisor(controller, runPromise, undefined, spawnedProcesses);
  }
});

test("runAgy preserves opt-in positive timeout", async () => {
  const controller = new AbortController();
  const spawnedProcesses: ChildProcess[] = [];
  try {
    await assert.rejects(
      () => runAgy(fixtureArgs, {
        command: "node",
        cwd: process.cwd(),
        timeoutMs: 80,
        signal: controller.signal,
        spawnFn: fixtureSpawn("hang", [], spawnedProcesses),
      }),
      (error: unknown) => {
        assert.ok(error instanceof AntigravityProcessError);
        assert.equal(error.kind, "timeout");
        assert.match(error.message, /80ms/);
        return true;
      },
    );
  } finally {
    await cleanupSupervisor(controller, null, undefined, spawnedProcesses);
  }
});

test("AntigravitySupervisor runs unlimited by default and stays alive beyond short deadline until cancel signal", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-supervisor-unlimited-"));
  const controller = new AbortController();
  const spawnedProcesses: ChildProcess[] = [];
  let runPromise: Promise<AntigravityAttemptStatus> | null = null;
  try {
    const spool = new AntigravitySpool(tempDir);
    const attempt = await spool.createAttempt({
      agentId: "agent_unlimited",
      jobId: "job_unlimited",
      requestId: "req_unlimited",
      prompt: "Execute unlimited task",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      // Notice: timeoutMs omitted -> unlimited
    });

    assert.equal(attempt.timeoutMs, null, "Attempt manifest should represent unlimited as null");

    const supervisor = new AntigravitySupervisor({
      spoolDir: attempt.attemptDir,
      manifest: attempt,
      signal: controller.signal,
      spawnFn: fixtureSpawn("hang", [], spawnedProcesses),
    });

    runPromise = supervisor.run();

    // Wait 150ms beyond a short deadline
    await new Promise((r) => setTimeout(r, 150));

    // Cancel via spool signal file
    await spool.writeCancelSignal(attempt.attemptId, "Explicit cancellation");

    const status = await runPromise;
    assert.equal(status.status, "aborted");
    assert.match(status.error ?? "", /signal file|cancelled/);
  } finally {
    await cleanupSupervisor(controller, runPromise, tempDir, spawnedProcesses);
  }
});

test("AntigravitySupervisor preserves opt-in positive timeout", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-supervisor-optin-"));
  const controller = new AbortController();
  const spawnedProcesses: ChildProcess[] = [];
  let runPromise: Promise<AntigravityAttemptStatus> | null = null;
  try {
    const spool = new AntigravitySpool(tempDir);
    const attempt = await spool.createAttempt({
      agentId: "agent_optin",
      jobId: "job_optin",
      requestId: "req_optin",
      prompt: "Execute opt-in task",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      timeoutMs: 80,
    });

    assert.equal(attempt.timeoutMs, 80);

    const supervisor = new AntigravitySupervisor({
      spoolDir: attempt.attemptDir,
      manifest: attempt,
      signal: controller.signal,
      spawnFn: fixtureSpawn("hang", [], spawnedProcesses),
    });

    runPromise = supervisor.run();
    const status = await runPromise;
    assert.equal(status.status, "timed_out");
    assert.match(status.error ?? "", /80ms.*terminated/);
  } finally {
    await cleanupSupervisor(controller, runPromise, tempDir, spawnedProcesses);
  }
});

test("AntigravitySpool accepts both old numeric timeout manifests and new unlimited null manifests", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-spool-migration-"));
  try {
    const spool = new AntigravitySpool(tempDir);

    // 1. Create a new attempt without timeoutMs -> new manifest with timeoutMs: null
    const newAttempt = await spool.createAttempt({
      agentId: "agent_new",
      jobId: "job_new",
      requestId: "req_new",
      prompt: "New attempt prompt",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });
    assert.equal(newAttempt.timeoutMs, null);

    const readNew = await spool.readManifest(newAttempt.attemptDir);
    assert.ok(readNew);
    assert.equal(readNew.timeoutMs, null, "New manifest should have timeoutMs: null");
    assert.equal(readNew.args.includes("--print-timeout"), true, "New args must include --print-timeout");
    assert.equal(readNew.args[readNew.args.indexOf("--print-timeout") + 1], AGY_PRINT_TIMEOUT_UNLIMITED, "New args must use sentinel");

    // 2. Simulate an old manifest on disk with numeric timeoutMs: 30000
    const oldAttemptDir = path.join(tempDir, "spool", "antigravity", "job_old", "attempt_old");
    const oldManifestContent = JSON.stringify({
      schemaVersion: 1,
      agentId: "agent_old",
      jobId: "job_old",
      requestId: "req_old",
      attemptId: "attempt_old",
      parentAttemptId: null,
      promptHash: "hash_old",
      promptPath: path.join(oldAttemptDir, "prompt.txt"),
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      command: "node",
      args: ["--model", "gemini-3.8-flash-high", "-p", "old prompt", "--print-timeout", "30s"],
      timeoutMs: 30000,
      sandbox: false,
      addDirs: [],
      dangerouslySkipPermissions: false,
      attemptDir: oldAttemptDir,
      stdoutPath: path.join(oldAttemptDir, "stdout.log"),
      stderrPath: path.join(oldAttemptDir, "stderr.log"),
      statusPath: path.join(oldAttemptDir, "status.json"),
      heartbeatPath: path.join(oldAttemptDir, "heartbeat.json"),
      cancelPath: path.join(oldAttemptDir, "cancel.signal"),
      createdAt: new Date().toISOString(),
      maxOutputBytes: 1048576,
      fence: 1,
    }, null, 2);

    await writePrivateFile(path.join(oldAttemptDir, "manifest.json"), oldManifestContent);

    const readOld = await spool.readManifest(oldAttemptDir);
    assert.ok(readOld);
    assert.equal(readOld.timeoutMs, 30000, "Old manifest with numeric timeoutMs must be read successfully");
    assert.equal(readOld.attemptId, "attempt_old");
  } finally {
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("AntigravityAdapter runs unlimited by default through durable spool with no pending handles", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-adapter-unlimited-"));
  try {
    const adapter = new AntigravityAdapter({
      command: "node",
      dataDir: tempDir,
      spawnFn: fixtureSpawn("ok"),
    });

    assert.equal(adapter.timeoutMs, null, "Adapter timeoutMs must be null (unlimited) by default");

    const result = await adapter.runPrompt({
      prompt: "End-to-end unlimited task",
      cwd: tempDir,
      dataDir: tempDir,
      agentId: "agent_e2e",
      jobId: "job_e2e",
      requestId: "req_e2e",
      // No timeoutMs -> unlimited
    });

    assert.equal(result.status, "completed");
    assert.equal(result.runId, "run_fixture_1");

    const spool = new AntigravitySpool(tempDir);
    const attempts = await spool.listAttempts("job_e2e");
    assert.equal(attempts.length, 1);
    const attempt = attempts[0]!;
    assert.equal(attempt.timeoutMs, null, "Spool manifest must record timeoutMs as null");
    assert.equal(attempt.args.includes("--print-timeout"), true, "Args must include --print-timeout");
    assert.equal(attempt.args[attempt.args.indexOf("--print-timeout") + 1], AGY_PRINT_TIMEOUT_UNLIMITED, "Args must use sentinel");

    const status = await spool.readStatus(attempt.attemptId, "job_e2e");
    assert.ok(status);
    assert.equal(status!.status, "completed");
  } finally {
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("Heartbeat updates and lease reconciliation function normally during unlimited execution without clock kills", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-heartbeat-liveness-"));
  const controller = new AbortController();
  const spawnedProcesses: ChildProcess[] = [];
  let runPromise: Promise<AntigravityAttemptStatus> | null = null;
  try {
    const spool = new AntigravitySpool(tempDir);
    const attempt = await spool.createAttempt({
      agentId: "agent_hb",
      jobId: "job_hb",
      requestId: "req_hb",
      prompt: "Heartbeat check",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      fence: 42,
    });

    const heartbeats: number[] = [];
    let notifyHeartbeat: (() => void) | null = null;
    const supervisor = new AntigravitySupervisor({
      spoolDir: attempt.attemptDir,
      manifest: attempt,
      heartbeatIntervalMs: 50,
      signal: controller.signal,
      spawnFn: fixtureSpawn("hang", [], spawnedProcesses),
      onHeartbeat: (hb) => {
        heartbeats.push(hb.updatedAt);
        notifyHeartbeat?.();
      },
    });

    runPromise = supervisor.run();

    // Event/condition driven wait with test-only load limit instead of fragile sleep
    await new Promise<void>((resolve, reject) => {
      if (heartbeats.length >= 2) return resolve();
      const timer = setTimeout(() => {
        notifyHeartbeat = null;
        reject(new Error(`Timed out waiting for heartbeats under load (got ${heartbeats.length})`));
      }, 5000);
      timer.unref?.();
      notifyHeartbeat = () => {
        if (heartbeats.length >= 2) {
          clearTimeout(timer);
          notifyHeartbeat = null;
          resolve();
        }
      };
    });

    assert.ok(heartbeats.length >= 2, "Heartbeats must continue firing periodically without clock kill");

    const activeHb = await spool.readHeartbeat(attempt.attemptId, "job_hb");
    assert.ok(activeHb);
    assert.equal(activeHb!.fence, 42);
    assert.ok(spool.isHeartbeatLive(activeHb, 5000));

    // Cancel cleanly
    await spool.writeCancelSignal(attempt.attemptId, "Done testing heartbeat");
    const status = await runPromise;
    assert.equal(status.status, "aborted");
  } finally {
    await cleanupSupervisor(controller, runPromise, tempDir, spawnedProcesses);
  }
});

test("authoritative binary contract refutes omitting --print-timeout with exit error", async () => {
  const customArgs = ["--model", "gemini-3.8-flash-high", "-p", "task without print-timeout"];
  await assert.rejects(
    () => runAgy(customArgs, { command: "node", cwd: process.cwd(), spawnFn: fixtureSpawn("ok") }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "exit");
      assert.equal(error.code, 2);
      assert.match(error.message, /--print-timeout must not be omitted/);
      return true;
    },
  );
});

test("authoritative binary smoke refutes --print-timeout 0s with immediate timeout error", async () => {
  const zeroArgs = ["--model", "gemini-3.8-flash-high", "-p", "task zero", "--print-timeout", "0s"];
  await assert.rejects(
    () => runAgy(zeroArgs, { command: "node", cwd: process.cwd(), spawnFn: fixtureSpawn("ok") }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "exit");
      assert.equal(error.code, 1);
      assert.match(error.message, /timeout waiting for response/);
      return true;
    },
  );

  const zeroBareArgs = ["--model", "gemini-3.8-flash-high", "-p", "task zero bare", "--print-timeout", "0"];
  await assert.rejects(
    () => runAgy(zeroBareArgs, { command: "node", cwd: process.cwd(), spawnFn: fixtureSpawn("ok") }),
    (error: unknown) => {
      assert.ok(error instanceof AntigravityProcessError);
      assert.equal(error.kind, "exit");
      assert.equal(error.code, 1);
      assert.match(error.message, /timeout waiting for response/);
      return true;
    },
  );
});

test("authoritative binary smoke confirms --print-timeout 2562047h47m16s responds OK", async () => {
  const sentinelArgs = ["--model", "gemini-3.8-flash-high", "-p", "sentinel task", "--print-timeout", AGY_PRINT_TIMEOUT_UNLIMITED];
  const result = await runAgy(sentinelArgs, { command: "node", cwd: process.cwd(), spawnFn: fixtureSpawn("ok") });
  assert.equal(result.code, 0);
  const parsed = JSON.parse(result.stdout) as { status: string; summary: string };
  assert.equal(parsed.status, "success");
  assert.equal(parsed.summary, ENVELOPE.summary);
});

test("unlimited run leaves no lingering handles or timers in runner or supervisor", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-handles-"));
  const supervisorController = new AbortController();
  try {
    // 1. Runner in unlimited mode
    const runnerController = new AbortController();
    const runnerResult = await runAgy(fixtureArgs, {
      command: "node",
      cwd: process.cwd(),
      signal: runnerController.signal,
      spawnFn: fixtureSpawn("ok"),
    });
    assert.equal(runnerResult.code, 0);

    // 2. Supervisor in unlimited mode
    const spool = new AntigravitySpool(tempDir);
    const attempt = await spool.createAttempt({
      agentId: "agent_handles",
      jobId: "job_handles",
      requestId: "req_handles",
      prompt: "Handle check task",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
    });
    assert.equal(attempt.timeoutMs, null);

    const supervisor = new AntigravitySupervisor({
      spoolDir: attempt.attemptDir,
      manifest: attempt,
      signal: supervisorController.signal,
      spawnFn: fixtureSpawn("ok"),
    });

    const status = await supervisor.run();
    assert.equal(status.status, "completed");

    // Internal timer handles must be completely stopped/cleared
    assert.equal((supervisor as any).heartbeatTimer, null);
    assert.equal((supervisor as any).cancelWatcherTimer, null);
    assert.equal((supervisor as any).timeoutTimer, null);
    assert.equal((supervisor as any).abortHandler, null);
  } finally {
    supervisorController.abort();
    await rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

test("TDD: unconditional cleanup in finally kills hanging worker and clears handles on forced failure before cancel", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agy-tdd-leak-proof-"));
  const controller = new AbortController();
  const spawnedProcesses: ChildProcess[] = [];
  let runPromise: Promise<AntigravityAttemptStatus> | null = null;
  let supervisorInstance: AntigravitySupervisor | null = null;
  let forcedErrorCaught = false;

  try {
    const spool = new AntigravitySpool(tempDir);
    const attempt = await spool.createAttempt({
      agentId: "agent_tdd",
      jobId: "job_tdd",
      requestId: "req_tdd",
      prompt: "Forced failure before cancel",
      cwd: tempDir,
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
      modelRoute: "antigravity-flash-high",
      fence: 100,
    });

    let onHeartbeatNotify: (() => void) | null = null;
    supervisorInstance = new AntigravitySupervisor({
      spoolDir: attempt.attemptDir,
      manifest: attempt,
      heartbeatIntervalMs: 50,
      signal: controller.signal,
      spawnFn: fixtureSpawn("hang", [], spawnedProcesses),
      onHeartbeat: () => {
        onHeartbeatNotify?.();
      },
    });

    runPromise = supervisorInstance.run();

    // Wait until child process has actually spawned and is running
    await new Promise<void>((resolve, reject) => {
      if (spawnedProcesses.length >= 1 && spawnedProcesses[0]?.pid) return resolve();
      const timer = setTimeout(() => reject(new Error("Worker failed to spawn within 5000ms")), 5000);
      timer.unref?.();
      onHeartbeatNotify = () => {
        if (spawnedProcesses.length >= 1 && spawnedProcesses[0]?.pid) {
          clearTimeout(timer);
          resolve();
        }
      };
    });

    const child = spawnedProcesses[0]!;
    const childPid = child.pid!;
    assert.ok(childPid > 0, "Spawned child must have a valid PID");

    // Verify child is alive initially in OS
    assert.doesNotThrow(() => process.kill(childPid, 0), "Child worker must be running in OS before forced failure");

    // Force an assertion failure simulating test failure / load timeout BEFORE cancel signal is written
    try {
      assert.fail("Forced assertion failure before cancel signal");
      // Any lines below are unreachable
      await spool.writeCancelSignal(attempt.attemptId, "unreachable cancel");
    } catch (err) {
      forcedErrorCaught = true;
      throw err; // rethrow so finally block is exercised on failure path
    }
  } catch (err: any) {
    assert.equal(err.message, "Forced assertion failure before cancel signal");
  } finally {
    // Unconditional cleanup in finally ANTES rm
    await cleanupSupervisor(controller, runPromise, tempDir, spawnedProcesses);
  }

  // 1. Proved that the error was caught on the failure path
  assert.equal(forcedErrorCaught, true, "Forced failure must occur before cancel");

  // 2. Proved zero lingering PIDs (child process is dead in OS)
  const child = spawnedProcesses[0]!;
  const childPid = child.pid!;
  const deadline = Date.now() + 3000;
  let pidAlive = true;
  while (Date.now() < deadline) {
    try {
      process.kill(childPid, 0);
      await new Promise((r) => setTimeout(r, 50));
    } catch {
      pidAlive = false;
      break;
    }
  }
  assert.equal(pidAlive, false, `Worker PID ${childPid} must be terminated in OS despite failure before cancel`);

  // 3. Proved zero lingering supervisor handles/timers
  assert.ok(supervisorInstance);
  assert.equal((supervisorInstance as any).heartbeatTimer, null, "heartbeatTimer must be cleared");
  assert.equal((supervisorInstance as any).cancelWatcherTimer, null, "cancelWatcherTimer must be cleared");
  assert.equal((supervisorInstance as any).timeoutTimer, null, "timeoutTimer must be cleared");
  assert.equal((supervisorInstance as any).abortHandler, null, "abortHandler must be removed");
  assert.equal((supervisorInstance as any).settled, true, "supervisor must be settled");

  // 4. Proved tempDir was successfully removed without file lock / EPERM issues
  assert.equal(existsSync(tempDir), false, "tempDir must be completely removed without handle locks");
});
