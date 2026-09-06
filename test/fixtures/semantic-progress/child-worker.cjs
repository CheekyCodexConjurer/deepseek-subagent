#!/usr/bin/env node
"use strict";

// Dedicated fixture for semantic progress child testing with real process execution.
// Supports timed chunked output that crosses capturecap limits, followed by a controlled
// silence window, and then clean finite self-exit.

const args = process.argv.slice(2);
const promptIndex = args.indexOf("-p");
const prompt = promptIndex >= 0 ? (args[promptIndex + 1] ?? "") : "";

const chunkBytes = parseInt(process.env.TEST_CHUNK_BYTES || "262144", 10);
const chunkCount = parseInt(process.env.TEST_CHUNK_COUNT || "6", 10);
const chunkIntervalMs = parseInt(process.env.TEST_CHUNK_INTERVAL_MS || "25", 10);
const silenceMs = parseInt(process.env.TEST_SILENCE_MS || "2000", 10);
const exitCode = parseInt(process.env.TEST_EXIT_CODE || "0", 10);

// Signal handling to guarantee clean self-termination if killed early
process.on("SIGTERM", () => process.exit(0));
process.on("SIGINT", () => process.exit(0));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  // Phase 1: Timed chunked streaming output
  for (let i = 0; i < chunkCount; i++) {
    const header = `[CHUNK ${i + 1}/${chunkCount} len=${chunkBytes} time=${Date.now()}]\n`;
    const paddingLength = Math.max(0, chunkBytes - header.length);
    const chunk = header + ".".repeat(paddingLength);
    process.stdout.write(chunk);
    if (i < chunkCount - 1 && chunkIntervalMs > 0) {
      await sleep(chunkIntervalMs);
    }
  }

  // Phase 2: Silence window (no output emitted while remaining alive)
  if (silenceMs > 0) {
    await sleep(silenceMs);
  }

  // Phase 3: Finite self-exit with completion payload
  process.stdout.write("\nFinished task successfully\n");
  process.exit(exitCode);
}

main().catch((err) => {
  process.stderr.write("child-worker error: " + String(err) + "\n");
  process.exit(1);
});
