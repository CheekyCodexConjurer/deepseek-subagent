#!/usr/bin/env node
"use strict";

// Fake `agy` executable for tests without quota. Behavior selected by the
// AGY_FIXTURE environment variable. Always validates the argument contract
// observed in the smoke: `--model gemini-3.7-flash-high -p <prompt>
// --print-timeout 15m`.

const args = process.argv.slice(2);
const promptIndex = args.indexOf("-p");
const prompt = promptIndex >= 0 ? (args[promptIndex + 1] ?? "") : "";

const valid =
  args[0] === "--model" &&
  args[1] === "gemini-3.7-flash-high" &&
  promptIndex >= 2 &&
  prompt.trim().length > 0 &&
  args[args.length - 2] === "--print-timeout" &&
  args[args.length - 1] === "15m";

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
