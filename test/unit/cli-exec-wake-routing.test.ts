import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile, mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  DefaultCodexCliTransport,
  type ProcessRunner,
  type ProcessRunOptions,
  type ProcessRunResult,
} from "../../src/codex/cli-resolver.js";
import {
  detectTaskSessionOrigin,
  readSessionMetaHeader,
  resolveSessionsDir,
} from "../../src/codex/session-origin.js";

interface RecordedCall {
  executable: string;
  args: string[];
  options?: ProcessRunOptions;
}

function createMockRunner(handlers?: {
  onQueue?: (args: string[], options?: ProcessRunOptions) => ProcessRunResult | Promise<ProcessRunResult>;
  onResume?: (args: string[], options?: ProcessRunOptions) => ProcessRunResult | Promise<ProcessRunResult>;
  onHelp?: (args: string[], options?: ProcessRunOptions) => ProcessRunResult | Promise<ProcessRunResult>;
  onVersion?: (args: string[], options?: ProcessRunOptions) => ProcessRunResult | Promise<ProcessRunResult>;
}) {
  const recordedCalls: RecordedCall[] = [];
  const runner: ProcessRunner = async (executable, args, options) => {
    recordedCalls.push({ executable, args, options });
    if (args[0] === "--version") {
      return (
        handlers?.onVersion?.(args, options) ?? {
          code: 0,
          stdout: "codex 0.153.1\n",
          stderr: "",
        }
      );
    }
    if (args[0] === "exec" && args[1] === "resume" && args[2] === "--help") {
      return (
        handlers?.onHelp?.(args, options) ?? {
          code: 0,
          stdout: "codex exec resume session_id\n",
          stderr: "",
        }
      );
    }
    if (args[0] === "queue" && args[1] === "--help") {
      return {
        code: 0,
        stdout: "codex queue --thread <thread_id> --message <message>\n",
        stderr: "",
      };
    }
    if (args[0] === "queue") {
      return (
        handlers?.onQueue?.(args, options) ?? {
          code: 0,
          stdout: JSON.stringify({ accepted: true, message_id: "msg_queued_1" }),
          stderr: "",
        }
      );
    }
    if (args[0] === "exec" && args[1] === "resume") {
      return (
        handlers?.onResume?.(args, options) ?? {
          code: 0,
          stdout: JSON.stringify({
            type: "turn.started",
            thread_id: args[4] ?? "unknown",
          }),
          stderr: "",
        }
      );
    }
    return { code: 0, stdout: "", stderr: "" };
  };
  return { runner, recordedCalls };
}

async function createRolloutFixture(options: {
  baseDir: string;
  year?: string;
  month?: string;
  day?: string;
  threadId: string;
  headerId?: string;
  source?: string;
  originator?: string;
  includeBaseInstructions?: boolean;
}) {
  const year = options.year ?? "2026";
  const month = options.month ?? "09";
  const day = options.day ?? "05";
  const dayDir = path.join(options.baseDir, year, month, day);
  await mkdir(dayDir, { recursive: true });

  const fileName = `rollout-${year}-${month}-${day}T12-00-00-${options.threadId}.jsonl`;
  const filePath = path.join(dayDir, fileName);

  const payload: Record<string, unknown> = {
    session_id: options.headerId ?? options.threadId,
    id: options.headerId ?? options.threadId,
    timestamp: new Date().toISOString(),
    cwd: "E:\\test-workspace",
    originator: options.originator ?? "Codex Desktop",
    cli_version: "0.153.1",
  };
  if (options.source !== undefined) {
    payload.source = options.source;
  }
  if (options.includeBaseInstructions) {
    payload.base_instructions = {
      text: "X".repeat(50 * 1024),
    };
  }

  const line0 = JSON.stringify({
    timestamp: new Date().toISOString(),
    ordinal: 0,
    type: "session_meta",
    payload,
  });

  const line1 = JSON.stringify({
    timestamp: new Date().toISOString(),
    ordinal: 1,
    type: "event_msg",
    payload: { type: "task_started" },
  });

  await writeFile(filePath, `${line0}\n${line1}\n`, "utf8");
  return { filePath, fileName };
}

test("RED/GREEN: proven exec session bypasses queue and directly invokes exec resume", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
    await createRolloutFixture({
      baseDir: tmpDir,
      threadId,
      source: "exec",
      originator: "Codex Desktop",
    });

    const { runner, recordedCalls } = createMockRunner();
    const transport = new DefaultCodexCliTransport({
      sessionsDir: tmpDir,
      candidates: ["codex"],
      runner,
    });

    const result = await transport.deliverWake(threadId, "WAKE_MARKER_TEST");

    assert.equal(result.success, true);
    assert.equal(result.deliveryMode, "cli_resume");
    assert.equal(result.accepted, true);

    const queueCalls = recordedCalls.filter((c) => c.args[0] === "queue" && c.args[1] === "--thread");
    assert.equal(queueCalls.length, 0, "Proven exec session must bypass queue entirely");

    const resumeCalls = recordedCalls.filter((c) => c.args[0] === "exec" && c.args[1] === "resume" && c.args[2] === "--json");
    assert.equal(resumeCalls.length, 1, "Must call exec resume directly");
    assert.equal(resumeCalls[0].args[4], threadId);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: proven exec session treats activeWriter as durable defer", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
    await createRolloutFixture({
      baseDir: tmpDir,
      threadId,
      source: "exec",
    });

    const { runner, recordedCalls } = createMockRunner({
      onResume: () => ({
        code: 1,
        stdout: "",
        stderr: "Error: active writer on thread 01a0729c-83f3-7653-97ba-9ad6a19efa2e",
      }),
    });

    const transport = new DefaultCodexCliTransport({
      sessionsDir: tmpDir,
      candidates: ["codex"],
      runner,
    });

    const result = await transport.deliverWake(threadId, "WAKE_MARKER_TEST");

    assert.equal(result.success, false);
    assert.equal(result.activeWriter, true, "Must flag activeWriter for durable defer");
    assert.equal(result.deliveryMode, "cli_resume");

    const queueCalls = recordedCalls.filter((c) => c.args[0] === "queue" && c.args[1] === "--thread");
    assert.equal(queueCalls.length, 0, "Must not have attempted queue");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: Desktop session preserves queue-first routing", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
    await createRolloutFixture({
      baseDir: tmpDir,
      threadId,
      source: "desktop",
      originator: "Codex Desktop",
    });

    const { runner, recordedCalls } = createMockRunner();
    const transport = new DefaultCodexCliTransport({
      sessionsDir: tmpDir,
      candidates: ["codex"],
      runner,
    });

    const result = await transport.deliverWake(threadId, "WAKE_MARKER_TEST");

    assert.equal(result.success, true);
    assert.equal(result.deliveryMode, "queued");
    assert.equal(result.messageId, "msg_queued_1");

    const queueCalls = recordedCalls.filter((c) => c.args[0] === "queue" && c.args[1] === "--thread");
    assert.equal(queueCalls.length, 1, "Desktop session must use queue-first");
    const resumeCalls = recordedCalls.filter((c) => c.args[0] === "exec" && c.args[1] === "resume" && c.args[2] === "--json");
    assert.equal(resumeCalls.length, 0, "Should not call exec resume when queue succeeds");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: header mismatch (untrusted filename) preserves queue-first", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
    // Filename has threadId, but header has different id!
    await createRolloutFixture({
      baseDir: tmpDir,
      threadId,
      headerId: "019dfa2b-d026-7740-ad06-e898c537b443",
      source: "exec",
    });

    const { runner, recordedCalls } = createMockRunner();
    const transport = new DefaultCodexCliTransport({
      sessionsDir: tmpDir,
      candidates: ["codex"],
      runner,
    });

    const result = await transport.deliverWake(threadId, "WAKE_MARKER_TEST");

    assert.equal(result.success, true);
    assert.equal(result.deliveryMode, "queued", "Must not trust filename when header id mismatches; fall back to queue-first");

    const queueCalls = recordedCalls.filter((c) => c.args[0] === "queue" && c.args[1] === "--thread");
    assert.equal(queueCalls.length, 1);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: unknown source or missing rollout preserves queue-first and unknownOutcome", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";

    const { runner, recordedCalls } = createMockRunner({
      onQueue: () => ({
        code: 1,
        stdout: "unrecognized error",
        stderr: "network timeout",
        timedOut: true,
      }),
    });

    const transport = new DefaultCodexCliTransport({
      sessionsDir: tmpDir,
      candidates: ["codex"],
      runner,
    });

    const result = await transport.deliverWake(threadId, "WAKE_MARKER_TEST");

    assert.equal(result.success, false);
    assert.equal(result.unknownOutcome, true, "unknownOutcome must be preserved on timeout");
    assert.equal(result.deliveryMode, "queued");

    const queueCalls = recordedCalls.filter((c) => c.args[0] === "queue" && c.args[1] === "--thread");
    assert.equal(queueCalls.length, 1);
    const resumeCalls = recordedCalls.filter((c) => c.args[0] === "exec" && c.args[1] === "resume" && c.args[2] === "--json");
    assert.equal(resumeCalls.length, 0, "Must not blindly resume on unknown outcome");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: bounded header read does not load base_instructions", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
    const { filePath } = await createRolloutFixture({
      baseDir: tmpDir,
      threadId,
      source: "exec",
      includeBaseInstructions: true,
    });

    const header = await readSessionMetaHeader(filePath, threadId);
    assert.ok(header);
    assert.equal(header.id, threadId);
    assert.equal(header.source, "exec");
    assert.equal((header as any).base_instructions, undefined, "base_instructions must not be loaded or present");

    const origin = await detectTaskSessionOrigin(threadId, { sessionsDir: tmpDir });
    assert.ok(origin);
    assert.equal(origin.isExec, true);
    assert.equal(origin.source, "exec");
    assert.equal((origin as any).base_instructions, undefined);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: malformed first line with second session_meta fails closed to null (no multiline regex leak)", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
    const dayDir = path.join(tmpDir, "2026", "09", "05");
    await mkdir(dayDir, { recursive: true });
    const filePath = path.join(dayDir, `rollout-2026-09-05T12-00-00-${threadId}.jsonl`);

    const line0 = '{"type": "session_meta", "broken": true';
    const line1 = JSON.stringify({
      timestamp: new Date().toISOString(),
      ordinal: 1,
      type: "session_meta",
      payload: {
        id: threadId,
        source: "exec",
      },
    });
    await writeFile(filePath, `${line0}\n${line1}\n`, "utf8");

    const header = await readSessionMetaHeader(filePath, threadId);
    assert.equal(header, null, "Must not recover session meta from line 1 when line 0 is malformed");

    const origin = await detectTaskSessionOrigin(threadId, { sessionsDir: tmpDir });
    assert.equal(origin, null);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: nested source exec in payload.extra with absent payload.source yields source: null and isExec: false", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
    const dayDir = path.join(tmpDir, "2026", "09", "05");
    await mkdir(dayDir, { recursive: true });
    const filePath = path.join(dayDir, `rollout-2026-09-05T12-00-00-${threadId}.jsonl`);

    const line0 = JSON.stringify({
      timestamp: new Date().toISOString(),
      ordinal: 0,
      type: "session_meta",
      payload: {
        id: threadId,
        extra: { source: "exec" },
      },
    });
    await writeFile(filePath, `${line0}\n`, "utf8");

    const header = await readSessionMetaHeader(filePath, threadId);
    assert.ok(header);
    assert.equal(header.id, threadId);
    assert.equal(header.source, null, "source must be null when not directly on payload");

    const origin = await detectTaskSessionOrigin(threadId, { sessionsDir: tmpDir });
    assert.ok(origin);
    assert.equal(origin.threadId, threadId);
    assert.equal(origin.source, null);
    assert.equal(origin.isExec, false, "Must not be exec session when source is nested in extra");

    const { runner, recordedCalls } = createMockRunner();
    const transport = new DefaultCodexCliTransport({
      sessionsDir: tmpDir,
      candidates: ["codex"],
      runner,
    });

    const result = await transport.deliverWake(threadId, "WAKE_MARKER_TEST");
    assert.equal(result.success, true);
    assert.equal(result.deliveryMode, "queued", "Must remain queue-first when source is only nested in extra");
    const queueCalls = recordedCalls.filter((c) => c.args[0] === "queue" && c.args[1] === "--thread");
    assert.equal(queueCalls.length, 1);
    const resumeCalls = recordedCalls.filter((c) => c.args[0] === "exec" && c.args[1] === "resume" && c.args[2] === "--json");
    assert.equal(resumeCalls.length, 0);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: missing or invalid type fails closed to null (no generic obj or session_meta bypass)", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
    const dayDir = path.join(tmpDir, "2026", "09", "05");
    await mkdir(dayDir, { recursive: true });

    // Case A: Missing type
    const fileA = path.join(dayDir, `rollout-2026-09-05T12-00-00-${threadId}.jsonl`);
    const lineA = JSON.stringify({
      payload: { id: threadId, source: "exec" },
    });
    await writeFile(fileA, `${lineA}\n`, "utf8");
    const headerA = await readSessionMetaHeader(fileA, threadId);
    assert.equal(headerA, null, "Must fail closed when type is missing");

    // Case B: type is other_event, but obj has session_meta property (bypass attempt)
    const lineB = JSON.stringify({
      type: "other_event",
      session_meta: { id: threadId, source: "exec" },
      payload: { id: threadId, source: "exec" },
    });
    await writeFile(fileA, `${lineB}\n`, "utf8");
    const headerB = await readSessionMetaHeader(fileA, threadId);
    assert.equal(headerB, null, "Must fail closed when type is not session_meta even if session_meta key exists");

    // Case C: payload missing, top-level keys only
    const lineC = JSON.stringify({
      type: "session_meta",
      id: threadId,
      source: "exec",
    });
    await writeFile(fileA, `${lineC}\n`, "utf8");
    const headerC = await readSessionMetaHeader(fileA, threadId);
    assert.equal(headerC, null, "Must fail closed when payload is missing");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: header exceeding 64k fails closed to null without regex fallback", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-exec-wake-"));
  try {
    const threadId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
    const dayDir = path.join(tmpDir, "2026", "09", "05");
    await mkdir(dayDir, { recursive: true });
    const filePath = path.join(dayDir, `rollout-2026-09-05T12-00-00-${threadId}.jsonl`);

    const line0 = JSON.stringify({
      type: "session_meta",
      payload: {
        id: threadId,
        source: "exec",
        padding: "X".repeat(70 * 1024),
      },
    });
    await writeFile(filePath, `${line0}\n`, "utf8");

    const header = await readSessionMetaHeader(filePath, threadId);
    assert.equal(header, null, "Must return null when header line exceeds 64k without regex fallback");

    const origin = await detectTaskSessionOrigin(threadId, { sessionsDir: tmpDir });
    assert.equal(origin, null);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("RED/GREEN: real default session origin detection works on real canary task", async (t) => {
  const canaryId = "01a0729c-83f3-7653-97ba-9ad6a19efa2e";
  const origin = await detectTaskSessionOrigin(canaryId);
  if (!origin) {
    t.skip("Host does not have canary rollout file at default location");
    return;
  }
  assert.equal(origin.threadId, canaryId);
  assert.equal(origin.source, "exec");
  assert.equal(origin.isExec, true);
});
