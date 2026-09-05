#!/usr/bin/env node
"use strict";

// Fake `agy` executable for tests without quota. Behavior selected by the
// AGY_FIXTURE environment variable. Always validates the argument contract
// observed in the smoke: `--model gemini-3.8-flash-high -p <prompt>
// --print-timeout <timeout>`.

const args = process.argv.slice(2);
const promptIndex = args.indexOf("-p");
const prompt = promptIndex >= 0 ? (args[promptIndex + 1] ?? "") : "";

const printTimeoutIndex = args.indexOf("--print-timeout");
const printTimeout = printTimeoutIndex >= 0 ? args[printTimeoutIndex + 1] : null;

// Authoritative binary evidence:
// 1. agy.exe --help declares --print-timeout default 5m0s; omitting the flag is NOT unlimited.
// 2. Real smoke with --print-timeout 0s fails immediately ("timeout waiting for response").
// 3. Real smoke with --print-timeout 2562047h47m16s responds OK.
if (printTimeoutIndex === -1) {
  process.stderr.write("agy: invalid argument contract: --print-timeout must not be omitted (binary default is 5m0s, not unlimited)\n");
  process.exit(2);
}

if (printTimeout === "0s" || printTimeout === "0" || printTimeout === "0m" || printTimeout === "0h") {
  process.stderr.write("timeout waiting for response\n");
  process.exit(1);
}

const validPrintTimeout =
  typeof printTimeout === "string" &&
  /^(?:\d+[hmsd])+$/.test(printTimeout);

const valid =
  args[0] === "--model" &&
  (typeof args[1] === "string" && args[1].startsWith("gemini-")) &&
  promptIndex >= 2 &&
  prompt.trim().length > 0 &&
  validPrintTimeout;

if (!valid) {
  process.stderr.write("agy: invalid argument contract: " + JSON.stringify(args) + "\n");
  process.exit(2);
}

const envelope = {
  status: "success",
  runId: "run_fixture_1",
  summary: "Fixture summary: task completed without quota.",
  files: ["src/example.ts"],
  tests: ["npm test"],
  risks: ["fixture only; no real inference"],
  diffSummary: "1 file changed",
};

const behavior = process.env.AGY_FIXTURE ?? "ok";
switch (behavior) {
  case "text":
    process.stdout.write("Fixture text summary: completed.\n");
    process.exit(0);
    break;
  case "fenced":
    process.stdout.write("```json\n" + JSON.stringify(envelope) + "\n```\n");
    process.exit(0);
    break;
  case "marker":
    process.stdout.write("AGY_JSON:\n" + JSON.stringify(envelope) + "\n");
    process.exit(0);
    break;
  case "empty":
    process.exit(0);
    break;
  case "nostatus":
    process.stdout.write(JSON.stringify({ summary: "no status declared", runId: "run_nostatus" }) + "\n");
    process.exit(0);
    break;
  case "big":
    process.stdout.write("x".repeat(4096) + "\n");
    process.exit(0);
    break;
  case "fail":
    process.stderr.write("agy: quota exceeded for the requested model\n");
    process.exit(1);
    break;
  case "slow":
    setTimeout(() => {
      process.stdout.write(JSON.stringify(envelope) + "\n");
      process.exit(0);
    }, 400);
    break;
  case "hang":
    setInterval(() => undefined, 1000);
    break;
  case "ok":
  default:
    process.stdout.write(JSON.stringify(envelope) + "\n");
    process.exit(0);
    break;
}
