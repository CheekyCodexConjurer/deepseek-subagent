import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService } from "../../src/service.js";
import {
  DEFAULT_CODEX_CAPABILITIES,
  type CodexCorrelation,
  type CodexDeliveryAdapter,
} from "../../src/codex/adapter.js";
import {
  DefaultCodexCliTransport,
  reconcileQueuedWake,
  type CodexCliTransport,
  type CodexCliExecutionResult,
  type ProcessRunner,
  type ProcessRunOptions,
  type ProcessRunResult,
} from "../../src/codex/cli-resolver.js";
import { DatabaseSync } from "node:sqlite";
import type {
  CodexBinding,
  JobRecord,
  OpenCodeClientLike,
  OpenCodeMessage,
  WakeEnvelope,
} from "../../src/types.js";

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
          stdout: "codex 0.150.0\n",
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
          stdout: JSON.stringify({ accepted: true, message_id: "msg_default_queue" }),
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

class FakeCodexDelivery implements CodexDeliveryAdapter {
  available = true;
  reason: string | null = null;
  capabilities = { ...DEFAULT_CODEX_CAPABILITIES, authoritativeAttachment: false };
  deliveredWakes: Array<{ envelope: WakeEnvelope; binding: CodexBinding }> = [];

  async start(): Promise<void> {}
  async close(): Promise<void> {}
  async deliver(job: JobRecord, binding: CodexBinding, text: string): Promise<"codex-steer" | "codex-start"> {
    return "codex-start";
  }
  async deliverWake(envelope: WakeEnvelope, binding: CodexBinding): Promise<"codex-steer" | "codex-start"> {
    this.deliveredWakes.push({ envelope, binding });
    return "codex-start";
  }
  async reconcileSend(threadId: string, marker: string): Promise<boolean> {
    return false;
  }
  onCorrelation(_listener: (correlation: CodexCorrelation) => void): () => void {
    return () => undefined;
  }
}

class FakeOpenCodeClient implements OpenCodeClientLike {
  messages: OpenCodeMessage[] = [];
  async health() {
    return { healthy: true };
  }
  async createSession() {
    return { id: "session_fake" };
  }
  async promptAsync() {}
  async listMessages() {
    return this.messages;
  }
  async getDiff() {
    return "";
  }
  async abort() {}
  async replyPermission() {}
  async subscribe() {}
}

async function createTestEnv() {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "ds-queue-v3-"));
  const config = createDefaultConfig({
    dataDir: tmp,
    configPath: path.join(tmp, "config.json"),
    experimentalSameChatDelivery: true,
  });
  const store = new BridgeStore(path.join(tmp, "bridge.sqlite"));
  return { tmp, config, store };
}

function setupAgent(store: BridgeStore, tmp: string, id: string = "agent_queue_test") {
  return store.createAgent({
    id,
    title: "Queue Test Agent",
    topic: "Testing v3 codex queue delivery",
    repositoryRoot: tmp,
    workspacePath: tmp,
    workspaceStrategy: "shared",
    opencodeServerId: "srv",
    opencodeSessionId: `session_${id}`,
    modelProviderId: "deepseek",
    modelId: "deepseek-chat",
    modelVariant: null,
  });
}

function makeJobCompleted(store: BridgeStore, jobId: string, resultPath?: string, resultSummary?: string) {
  store.updateJobStatus(jobId, "dispatching");
  store.updateJobStatus(jobId, "running");
  if (resultPath) {
    store.setJobResult(jobId, resultPath, resultSummary ?? "completed result");
  }
  store.updateJobStatus(jobId, "completed");
}

// ---------------------------------------------------------------------------
// 1. Loaded / active-writer wake uses bundled Codex CLI queue --thread <id> --message <metadata-only marker>
// ---------------------------------------------------------------------------
test("QUEUE-V3-RED-1: loaded/active-writer wake uses bundled Codex CLI queue --thread <id> --message <metadata-only marker>", async () => {
  const { tmp, config, store } = await createTestEnv();
  const threadId = "11111111-1111-1111-1111-111111111111";

  const { runner, recordedCalls } = createMockRunner({
    onResume: () => ({
      code: 1,
      stdout: "",
      stderr: "active writer conflict: thread 11111111-1111-1111-1111-111111111111 is loaded and active",
    }),
    onQueue: () => ({
      code: 0,
      stdout: JSON.stringify({ accepted: true, message_id: "msg-queue-1" }),
      stderr: "",
    }),
  });

  const cliTransport = new DefaultCodexCliTransport({
    candidates: ["C:\\Codex\\codex.exe"],
    runner,
  });

  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_q1");
    const job = store.createJob({
      id: "job_q1",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_q1",
      promptHash: "hash_q1",
    });
    store.bindJob({
      jobId: job.id,
      threadId,
      originatingTurnId: "turn_q1",
      originatingItemId: "item_q1",
    });

    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);

    const resPath = path.join(tmp, "res_q1.json");
    await writeFile(resPath, JSON.stringify({ envelope: { summary: "job q1 done" } }));
    makeJobCompleted(store, job.id, resPath, "job q1 done");

    // Evaluate wakes: session is loaded / active writer, so wake must invoke queue command
    await service.evaluateParkWakes(job.id);

    // Verify bundled Codex CLI was invoked with `queue --thread <id> --message <marker>`
    const queueCall = recordedCalls.find((c) => c.args[0] === "queue" && c.args[1] !== "--help");
    assert.ok(
      queueCall,
      "Expected bundled Codex CLI 'queue' command to be executed for loaded/active-writer wake",
    );

    const threadFlagIdx = queueCall.args.indexOf("--thread");
    assert.ok(threadFlagIdx !== -1, "queue command must include --thread flag");
    assert.equal(
      queueCall.args[threadFlagIdx + 1],
      threadId,
      "queue --thread argument must match target thread ID",
    );

    const messageFlagIdx = queueCall.args.indexOf("--message");
    assert.ok(messageFlagIdx !== -1, "queue command must include --message flag");
    const messageArg = queueCall.args[messageFlagIdx + 1];
    assert.ok(messageArg, "--message argument must not be empty");
    assert.match(
      messageArg,
      /<!--\s*\[SUBAGENT_BRIDGE_WAKE:park=.*:gen=.*\]\s*-->/,
      "--message argument must contain the metadata-only wake marker",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Successful queue acceptance is terminal delivery and is never retried
// ---------------------------------------------------------------------------
test("QUEUE-V3-RED-2: successful queue acceptance is terminal delivery and is never retried", async () => {
  const { tmp, config, store } = await createTestEnv();
  const threadId = "22222222-2222-2222-2222-222222222222";

  const { runner, recordedCalls } = createMockRunner({
    onResume: () => ({
      code: 1,
      stdout: "",
      stderr: "active writer conflict: thread 22222222-2222-2222-2222-222222222222 is active",
    }),
    onQueue: () => ({
      code: 0,
      stdout: JSON.stringify({ accepted: true, message_id: "msg-term-002" }),
      stderr: "",
    }),
  });

  const cliTransport = new DefaultCodexCliTransport({
    candidates: ["C:\\Codex\\codex.exe"],
    runner,
  });

  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_q2");
    const job = store.createJob({
      id: "job_q2",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_q2",
      promptHash: "hash_q2",
    });
    store.bindJob({
      jobId: job.id,
      threadId,
      originatingTurnId: "turn_q2",
      originatingItemId: "item_q2",
    });

    const receipt = await service.park({ job_ids: [job.id] });
    assert.equal(receipt.armed, true);

    const resPath = path.join(tmp, "res_q2.json");
    await writeFile(resPath, JSON.stringify({ envelope: { summary: "job q2 done" } }));
    makeJobCompleted(store, job.id, resPath, "job q2 done");

    await service.evaluateParkWakes(job.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox, "Wake outbox record must exist");

    // Queue acceptance MUST be terminal delivery:
    // 1. status and wakeState must be 'delivered', NOT 'deferred_active_writer'
    assert.equal(
      outbox.status,
      "delivered",
      "Successful queue acceptance must set terminal 'delivered' status, not deferred_active_writer",
    );
    assert.equal(outbox.wakeState, "delivered", "wakeState must be 'delivered'");

    // 2. Barrier must be woken and disarmed
    const barrier = store.getParkBarrier(receipt.parkId)!;
    assert.equal(barrier.state, "woken", "Barrier state must be 'woken'");
    assert.equal(barrier.armed, false, "Barrier must be disarmed");

    // 3. No retry backoff must be scheduled (nextAttemptAt must be null/empty)
    assert.equal(
      outbox.nextAttemptAt,
      null,
      "Successful queue acceptance must never schedule a retry (nextAttemptAt must be null)",
    );

    // 4. No additional calls should be made after terminal delivery
    const initialCallCount = recordedCalls.length;
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(
      recordedCalls.length,
      initialCallCount,
      "No retry executions should occur after terminal queue acceptance",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Unloaded / no-session queue failure routes to existing exec-resume transport
// ---------------------------------------------------------------------------
test("QUEUE-V3-RED-3: unloaded/no-session queue failure can route to existing exec-resume transport", async () => {
  const { tmp, config, store } = await createTestEnv();
  const threadId = "33333333-3333-3333-3333-333333333333";
  const marker = "<!-- [SUBAGENT_BRIDGE_WAKE:park=park_3:gen=1] -->";

  const { runner, recordedCalls } = createMockRunner({
    onQueue: () => ({
      code: 1,
      stdout: "",
      stderr: "Error: No active session for thread 33333333-3333-3333-3333-333333333333 (thread is unloaded)",
    }),
    onResume: (args) => ({
      code: 0,
      stdout: JSON.stringify({
        type: "turn.started",
        thread_id: args[4] ?? threadId,
      }),
      stderr: "",
    }),
  });

  const cliTransport = new DefaultCodexCliTransport({
    candidates: ["C:\\Codex\\codex.exe"],
    runner,
  });

  try {
    // Attempt wake delivery
    const result = await cliTransport.deliverWake(threadId, marker);

    // 1. Queue command should have been attempted
    const queueCallIdx = recordedCalls.findIndex(
      (c) => c.args[0] === "queue" && c.args[1] !== "--help",
    );
    assert.ok(
      queueCallIdx !== -1,
      "Codex CLI queue transport must be attempted for wake delivery",
    );

    // 2. Upon unloaded/no-session failure, it must route/fallback to exec resume
    const resumeCallIdx = recordedCalls.findIndex(
      (c) => c.args[0] === "exec" && c.args[1] === "resume" && c.args[2] !== "--help",
    );
    assert.ok(
      resumeCallIdx !== -1,
      "exec-resume transport must be invoked when queue fails due to unloaded session",
    );
    assert.ok(
      resumeCallIdx > queueCallIdx,
      "exec-resume fallback must be called after the queue attempt failed",
    );

    // 3. Overall result should be successful delivery via the fallback
    assert.equal(
      result.success,
      true,
      "Delivery must succeed via exec-resume fallback after unloaded queue failure",
    );
    assert.equal(
      result.deliveryMode,
      "cli_resume",
      "Delivery mode must be cli_resume when falling through to resume",
    );

    // 3B: Timeout on queue must be classified as unknownOutcome and MUST NOT resume blindly
    const { runner: timeoutRunner, recordedCalls: timeoutCalls } = createMockRunner({
      onQueue: () => ({
        code: null,
        stdout: "",
        stderr: "",
        timedOut: true,
      }),
    });
    const timeoutTransport = new DefaultCodexCliTransport({
      candidates: ["C:\\Codex\\codex.exe"],
      runner: timeoutRunner,
    });
    const timeoutResult = await timeoutTransport.deliverWake(threadId, marker);
    assert.equal(timeoutResult.success, false);
    assert.equal(timeoutResult.unknownOutcome, true, "Timeout must be classified as unknownOutcome");
    const timeoutResumeCall = timeoutCalls.find(
      (c) => c.args[0] === "exec" && c.args[1] === "resume" && c.args[2] !== "--help",
    );
    assert.equal(timeoutResumeCall, undefined, "Timeout must not blindly fall through to exec resume");

    // 3C: Ambiguous output on queue must be classified as unknownOutcome and MUST NOT resume blindly
    const { runner: ambiguousRunner, recordedCalls: ambiguousCalls } = createMockRunner({
      onQueue: () => ({
        code: 1,
        stdout: "fatal error: unexpected internal failure",
        stderr: "",
      }),
    });
    const ambiguousTransport = new DefaultCodexCliTransport({
      candidates: ["C:\\Codex\\codex.exe"],
      runner: ambiguousRunner,
    });
    const ambiguousResult = await ambiguousTransport.deliverWake(threadId, marker);
    assert.equal(ambiguousResult.success, false);
    assert.equal(ambiguousResult.unknownOutcome, true, "Ambiguous error must be classified as unknownOutcome");
    const ambiguousResumeCall = ambiguousCalls.find(
      (c) => c.args[0] === "exec" && c.args[1] === "resume" && c.args[2] !== "--help",
    );
    assert.equal(ambiguousResumeCall, undefined, "Ambiguous error must not blindly fall through to exec resume");

    // 3D: Old CLI without queue support uses existing resume directly
    const { runner: oldCliRunner, recordedCalls: oldCliCalls } = createMockRunner();
    const oldCliRunnerWrapper: ProcessRunner = async (executable, args, options) => {
      if (args[0] === "queue" && args[1] === "--help") {
        return { code: 1, stdout: "", stderr: "unknown command 'queue'" };
      }
      return oldCliRunner(executable, args, options);
    };
    const oldCliTransport = new DefaultCodexCliTransport({
      candidates: ["C:\\Codex\\codex.exe"],
      runner: oldCliRunnerWrapper,
    });
    const oldCliResult = await oldCliTransport.deliverWake(threadId, marker);
    assert.equal(oldCliResult.success, true);
    assert.equal(oldCliResult.deliveryMode, "cli_resume", "Old CLI must deliver via cli_resume");
    const oldCliQueueCall = oldCliCalls.find(
      (c) => c.args[0] === "queue" && c.args[1] !== "--help",
    );
    assert.equal(oldCliQueueCall, undefined, "Old CLI without queue support must not run queue command");
  } finally {
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Queue output message id is captured in receipt if architecture exposes it
// ---------------------------------------------------------------------------
test("QUEUE-V3-RED-4: queue output message id is captured in receipt if architecture exposes it", async () => {
  const { tmp, config, store } = await createTestEnv();
  const threadId = "44444444-4444-4444-4444-444444444444";
  const marker = "<!-- [SUBAGENT_BRIDGE_WAKE:park=park_4:gen=1] -->";
  const expectedMessageId = "msg-queue-output-4444";

  const { runner } = createMockRunner({
    onResume: () => ({
      code: 1,
      stdout: "",
      stderr: "active writer conflict",
    }),
    onQueue: () => ({
      code: 0,
      stdout: JSON.stringify({
        accepted: true,
        message_id: expectedMessageId,
      }),
      stderr: "",
    }),
  });

  const cliTransport = new DefaultCodexCliTransport({
    candidates: ["C:\\Codex\\codex.exe"],
    runner,
  });

  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    // 4A: Check transport level execution result
    const transportResult = await cliTransport.deliverWake(threadId, marker);
    assert.equal(transportResult.success, true, "Transport deliverWake must succeed");
    assert.equal(transportResult.deliveryMode, "queued", "Transport deliveryMode must be 'queued'");
    assert.equal(
      (transportResult as any).messageId,
      expectedMessageId,
      "Transport execution result must capture message ID from queue output",
    );

    // 4B: Check service level outbox / receipt capture
    const agent = setupAgent(store, tmp, "agent_q4");
    const job = store.createJob({
      id: "job_q4",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_q4",
      promptHash: "hash_q4",
    });
    store.bindJob({
      jobId: job.id,
      threadId,
      originatingTurnId: "turn_q4",
      originatingItemId: "item_q4",
    });

    const receipt = await service.park({ job_ids: [job.id] });
    const resPath = path.join(tmp, "res_q4.json");
    await writeFile(resPath, JSON.stringify({ envelope: { summary: "job q4 done" } }));
    makeJobCompleted(store, job.id, resPath, "job q4 done");

    await service.evaluateParkWakes(job.id);

    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox, "Outbox record must exist");
    const capturedMessageId =
      (outbox as any).messageId ??
      (outbox as any).clientUserMessageId ??
      (outbox as any).message_id;
    if (capturedMessageId !== undefined) {
      assert.equal(
        capturedMessageId,
        expectedMessageId,
        "Queue output message ID must be persisted in outbox / receipt",
      );
    }

    // 4C: Check actual CLI text format parsing: "Queued message <uuid> for thread <uuid>."
    const actualTextUuid = "98765432-abcd-ef01-2345-6789abcdef01";
    const { runner: textRunner } = createMockRunner({
      onQueue: () => ({
        code: 0,
        stdout: `Queued message ${actualTextUuid} for thread ${threadId}.\n`,
        stderr: "",
      }),
    });
    const textCliTransport = new DefaultCodexCliTransport({
      candidates: ["C:\\Codex\\codex.exe"],
      runner: textRunner,
    });
    const textTransportResult = await textCliTransport.deliverWake(threadId, marker);
    assert.equal(textTransportResult.success, true, "Text transport deliverWake must succeed");
    assert.equal(textTransportResult.deliveryMode, "queued", "deliveryMode must be 'queued'");
    assert.equal(textTransportResult.messageId, actualTextUuid, "messageId must match parsed text UUID");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. No subagent result text appears in payload
// ---------------------------------------------------------------------------
test("QUEUE-V3-RED-5: no subagent result text appears in payload", async () => {
  const { tmp, config, store } = await createTestEnv();
  const threadId = "55555555-5555-5555-5555-555555555555";
  const secretSubagentResultText = "CONFIDENTIAL_SUBAGENT_DIFF_AND_OUTPUT_55555";

  const { runner, recordedCalls } = createMockRunner({
    onResume: () => ({
      code: 1,
      stdout: "",
      stderr: "active writer conflict",
    }),
    onQueue: () => ({
      code: 0,
      stdout: JSON.stringify({ accepted: true, message_id: "msg-q5" }),
      stderr: "",
    }),
  });

  const cliTransport = new DefaultCodexCliTransport({
    candidates: ["C:\\Codex\\codex.exe"],
    runner,
  });

  const fakeCodex = new FakeCodexDelivery();
  const service = new BridgeService(config, {
    store,
    codex: fakeCodex,
    cliTransport,
    manager: {
      start: async () => ({
        serverId: "srv",
        baseUrl: "http://127.0.0.1:9999",
        client: new FakeOpenCodeClient(),
        processId: null,
        stop: async () => {},
      }),
      stop: async () => {},
    },
  });
  await service.start();

  try {
    const agent = setupAgent(store, tmp, "agent_q5");
    const job = store.createJob({
      id: "job_q5",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_q5",
      promptHash: "hash_q5",
    });
    store.bindJob({
      jobId: job.id,
      threadId,
      originatingTurnId: "turn_q5",
      originatingItemId: "item_q5",
    });

    const receipt = await service.park({ job_ids: [job.id] });

    // Complete job with sensitive output content
    const resPath = path.join(tmp, "res_q5.json");
    await writeFile(
      resPath,
      JSON.stringify({
        envelope: {
          summary: secretSubagentResultText,
          content: "sensitive worker content that must never leak",
        },
      }),
    );
    makeJobCompleted(store, job.id, resPath, secretSubagentResultText);

    // Trigger wake evaluation
    await service.evaluateParkWakes(job.id);

    // 5A: Verify CLI queue invocation payload
    const queueCall = recordedCalls.find((c) => c.args[0] === "queue" && c.args[1] !== "--help");
    assert.ok(
      queueCall,
      "Codex CLI queue must be invoked for wake delivery so that payload can be verified",
    );

    const messageFlagIdx = queueCall.args.indexOf("--message");
    assert.ok(messageFlagIdx !== -1, "queue invocation must include --message");
    const messagePayload = queueCall.args[messageFlagIdx + 1]!;

    assert.equal(
      messagePayload.includes(secretSubagentResultText),
      false,
      "CLI --message payload must not contain subagent result text",
    );
    assert.equal(
      JSON.stringify(queueCall.args).includes(secretSubagentResultText),
      false,
      "No CLI queue arguments may contain subagent result text",
    );

    // 5B: Verify database outbox records
    const outbox = store.getWakeOutbox(receipt.parkId, receipt.generation)!;
    assert.ok(outbox);
    assert.equal(
      outbox.payloadJson.includes(secretSubagentResultText),
      false,
      "outbox.payloadJson must not contain subagent result text",
    );
    assert.equal(
      outbox.wakeMarker.includes(secretSubagentResultText),
      false,
      "outbox.wakeMarker must not contain subagent result text",
    );

    const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
    assert.ok(envelope.jobIds.includes(job.id));
    assert.ok(envelope.resultHashes[job.id], "Result hash must be present in lieu of raw text");
  } finally {
    await service.stop();
    store.close();
    await rm(tmp, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 6. Multi-candidate ranking: old resume-only candidate first does not preempt bundled 0.153.1 queue CLI
// ---------------------------------------------------------------------------
test("QUEUE-V3-RED-6: multi-candidate ranking: old candidate first does not preempt bundled 0.153.1 queue CLI, and aggregate probe returns top queue-capable candidate", async () => {
  const oldExe = "C:\\OldCodex\\bin\\codex.exe";
  const bundledExe = "C:\\AppData\\OpenAI\\Codex\\bin\\0.153.1\\codex.exe";
  const threadId = "66666666-6666-6666-6666-666666666666";
  const marker = "<!-- [SUBAGENT_BRIDGE_WAKE:park=park_6:gen=1] -->";

  const recordedCalls: RecordedCall[] = [];
  const runner: ProcessRunner = async (executable, args, options) => {
    recordedCalls.push({ executable, args, options });

    if (args[0] === "--version") {
      if (executable === oldExe) {
        return { code: 0, stdout: "codex 0.150.0\n", stderr: "" };
      }
      if (executable === bundledExe) {
        return { code: 0, stdout: "codex 0.153.1\n", stderr: "" };
      }
    }

    if (args[0] === "exec" && args[1] === "resume" && args[2] === "--help") {
      return { code: 0, stdout: "codex exec resume session_id\n", stderr: "" };
    }

    if (args[0] === "queue" && args[1] === "--help") {
      if (executable === oldExe) {
        return { code: 1, stdout: "", stderr: "unknown command 'queue'" };
      }
      if (executable === bundledExe) {
        return { code: 0, stdout: "codex queue --thread <thread_id> --message <message>\n", stderr: "" };
      }
    }

    if (args[0] === "queue") {
      if (executable === bundledExe) {
        return {
          code: 0,
          stdout: JSON.stringify({ accepted: true, message_id: "msg-bundled-01531" }),
          stderr: "",
        };
      }
    }

    if (args[0] === "exec" && args[1] === "resume") {
      if (executable === oldExe) {
        // If old candidate were invoked on active session, it would fail with active-writer conflict
        return {
          code: 1,
          stdout: "",
          stderr: "active writer conflict: thread 66666666-6666-6666-6666-666666666666 is loaded and active",
        };
      }
      return {
        code: 0,
        stdout: JSON.stringify({ type: "turn.started", thread_id: threadId }),
        stderr: "",
      };
    }

    return { code: 0, stdout: "", stderr: "" };
  };

  const cliTransport = new DefaultCodexCliTransport({
    candidates: [oldExe, bundledExe], // Old candidate is placed FIRST in raw candidates list
    runner,
  });

  // 1. Individual probe assertions
  const oldProbe = await cliTransport.probeCapabilities(oldExe);
  assert.equal(oldProbe.compatible, true, "Old 0.150.0 CLI with exec resume is compatible for resume");
  assert.equal(oldProbe.version, "0.150.0");
  assert.equal(oldProbe.queueSupported, false, "Old CLI does not support queue command");

  const bundledProbe = await cliTransport.probeCapabilities(bundledExe);
  assert.equal(bundledProbe.compatible, true);
  assert.equal(bundledProbe.version, "0.153.1");
  assert.equal(bundledProbe.queueSupported, true, "Bundled 0.153.1 CLI supports queue command");

  // 2. Aggregate probe assertions: must probe all candidates, rank queue-capable highest, and report anyQueueSupported
  const aggProbe = await cliTransport.probeCapabilities();
  assert.equal(aggProbe.compatible, true);
  assert.equal(
    aggProbe.version,
    "0.153.1",
    "Aggregate probe must return the version of the top queue-capable candidate (0.153.1), not the first raw candidate",
  );
  assert.equal(
    aggProbe.queueSupported,
    true,
    "Aggregate probe must report queueSupported=true because bundled CLI has queue support",
  );

  // 3. Deliver wake: must rank queue-capable bundled CLI first and deliver via queue without active-writer conflict
  const result = await cliTransport.deliverWake(threadId, marker);
  assert.equal(result.success, true, "Wake delivery must succeed");
  assert.equal(result.deliveryMode, "queued", "Must deliver via queued mode");
  assert.equal(result.executablePath, bundledExe, "Must execute bundled queue-capable CLI");
  assert.equal(result.version, "0.153.1");
  assert.equal(result.messageId, "msg-bundled-01531");

  // 4. Preemption prevention assertion: old candidate resume must never be called
  const oldResumeExecution = recordedCalls.find(
    (c) => c.executable === oldExe && c.args[0] === "exec" && c.args[1] === "resume" && c.args[2] !== "--help",
  );
  assert.equal(
    oldResumeExecution,
    undefined,
    "Old resume-only CLI must NOT be executed or preempt the queue-capable bundled CLI",
  );
});

// ---------------------------------------------------------------------------
// 7. Multi-candidate ranking: incompatible legacy candidate (< 0.150.0) is rejected and never preempts bundled 0.153.1
// ---------------------------------------------------------------------------
test("QUEUE-V3-RED-7: multi-candidate ranking: incompatible legacy candidate (< 0.150.0) is rejected and never preempts bundled 0.153.1", async () => {
  const legacyExe = "C:\\Legacy\\codex.exe";
  const bundledExe = "C:\\Bundled\\0.153.1\\codex.exe";
  const threadId = "77777777-7777-7777-7777-777777777777";
  const marker = "<!-- [SUBAGENT_BRIDGE_WAKE:park=park_7:gen=1] -->";

  const recordedCalls: RecordedCall[] = [];
  const runner: ProcessRunner = async (executable, args, options) => {
    recordedCalls.push({ executable, args, options });

    if (args[0] === "--version") {
      if (executable === legacyExe) {
        return { code: 0, stdout: "codex 0.145.0\n", stderr: "" };
      }
      if (executable === bundledExe) {
        return { code: 0, stdout: "codex 0.153.1\n", stderr: "" };
      }
    }

    if (args[0] === "exec" && args[1] === "resume" && args[2] === "--help") {
      return { code: 0, stdout: "codex exec resume session_id\n", stderr: "" };
    }

    if (args[0] === "queue" && args[1] === "--help") {
      if (executable === legacyExe) {
        return { code: 1, stdout: "", stderr: "unknown command" };
      }
      if (executable === bundledExe) {
        return { code: 0, stdout: "codex queue --thread <thread_id> --message <message>\n", stderr: "" };
      }
    }

    if (args[0] === "queue") {
      if (executable === bundledExe) {
        return {
          code: 0,
          stdout: JSON.stringify({ accepted: true, message_id: "msg-bundled-01531-leg" }),
          stderr: "",
        };
      }
    }

    return { code: 0, stdout: "", stderr: "" };
  };

  const cliTransport = new DefaultCodexCliTransport({
    candidates: [legacyExe, bundledExe], // Legacy candidate first
    runner,
  });

  // 1. Semver compatibility is enforced: legacy < 0.150.0 is marked compatible=false
  const legacyProbe = await cliTransport.probeCapabilities(legacyExe);
  assert.equal(
    legacyProbe.compatible,
    false,
    "Legacy CLI (0.145.0 < 0.150.0) must be classified as compatible=false (semver compatibility enforced)",
  );
  assert.equal(legacyProbe.version, "0.145.0");

  // 2. Aggregate probe returns bundled 0.153.1
  const aggProbe = await cliTransport.probeCapabilities();
  assert.equal(aggProbe.compatible, true);
  assert.equal(aggProbe.version, "0.153.1");
  assert.equal(aggProbe.queueSupported, true);

  // 3. Deliver wake succeeds using bundled CLI
  const result = await cliTransport.deliverWake(threadId, marker);
  assert.equal(result.success, true);
  assert.equal(result.deliveryMode, "queued");
  assert.equal(result.executablePath, bundledExe);
  assert.equal(result.version, "0.153.1");
  assert.equal(result.messageId, "msg-bundled-01531-leg");

  // 4. Legacy candidate was never invoked for execution
  const legacyExecCall = recordedCalls.find(
    (c) => c.executable === legacyExe && (c.args[0] === "queue" || (c.args[0] === "exec" && c.args[1] === "resume")) && !c.args.includes("--help"),
  );
  assert.equal(legacyExecCall, undefined, "Legacy candidate must never be executed");
});

// ---------------------------------------------------------------------------
// 8. Read-only queue reconciliation seam against CODEX_HOME/queue_1.sqlite without mutation
// ---------------------------------------------------------------------------
test("QUEUE-V3-RED-8: read-only queue reconciliation seam against CODEX_HOME/queue_1.sqlite without mutation", async () => {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "ds-queue-sqlite-"));
  const dbPath = path.join(tmp, "queue_1.sqlite");

  // Create queue_1.sqlite with schema matching Desktop Codex
  const db = new DatabaseSync(dbPath);
  db.exec(`
    CREATE TABLE queued_items (
      id TEXT PRIMARY KEY NOT NULL,
      thread_id TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      queue_order INTEGER NOT NULL,
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL
    );
  `);

  const targetThread = "88888888-8888-8888-8888-888888888888";
  const targetMarker = "<!-- [SUBAGENT_BRIDGE_WAKE:park=park_8:gen=1] -->";
  const targetMessageId = "msg_sqlite_match_001";

  // Insert matching row
  db.prepare(
    "INSERT INTO queued_items (id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
  ).run(
    targetMessageId,
    targetThread,
    JSON.stringify({
      type: "user_message",
      text: `Please wake up! ${targetMarker}`,
      metadata: { parkId: "park_8" },
    }),
    1,
    1700000000000,
    1700000000000,
  );

  // Insert other thread row
  db.prepare(
    "INSERT INTO queued_items (id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
  ).run(
    "msg_sqlite_other_thread",
    "99999999-9999-9999-9999-999999999999",
    JSON.stringify({ type: "user_message", text: targetMarker }),
    2,
    1700000001000,
    1700000001000,
  );

  // Insert same thread, different marker row
  db.prepare(
    "INSERT INTO queued_items (id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
  ).run(
    "msg_sqlite_other_marker",
    targetThread,
    JSON.stringify({ type: "user_message", text: "<!-- other marker -->" }),
    3,
    1700000002000,
    1700000002000,
  );

  db.close();

  const prevCodexHome = process.env.CODEX_HOME;
  try {
    process.env.CODEX_HOME = tmp;

    const transport = new DefaultCodexCliTransport();

    // 8A. Matching thread + marker using instr(payload_json, marker) -> returns found: true and messageId
    const match = await transport.reconcileQueuedWake(targetThread, targetMarker);
    assert.equal(match.found, true, "reconcileQueuedWake must find item matching thread + marker");
    assert.equal(match.messageId, targetMessageId, "reconcileQueuedWake must return message ID");

    // 8B. Thread mismatch -> returns found: false
    const threadMismatch = await transport.reconcileQueuedWake(
      "00000000-0000-0000-0000-000000000000",
      targetMarker,
    );
    assert.equal(threadMismatch.found, false, "Must return found: false on thread mismatch");
    assert.equal(threadMismatch.messageId, undefined);

    // 8C. Marker mismatch -> returns found: false
    const markerMismatch = await transport.reconcileQueuedWake(
      targetThread,
      "<!-- [SUBAGENT_BRIDGE_WAKE:park=nonexistent:gen=9] -->",
    );
    assert.equal(markerMismatch.found, false, "Must return found: false on marker mismatch");
    assert.equal(markerMismatch.messageId, undefined);

    // 8D. Direct function call with explicit dbPath option
    const directResult = await reconcileQueuedWake(targetThread, targetMarker, { dbPath });
    assert.equal(directResult.found, true);
    assert.equal(directResult.messageId, targetMessageId);

    // 8E. Direct function call with transport queueDbPath option
    const customTransport = new DefaultCodexCliTransport({ queueDbPath: dbPath });
    const customMatch = await customTransport.reconcileQueuedWake(targetThread, targetMarker);
    assert.equal(customMatch.found, true);
    assert.equal(customMatch.messageId, targetMessageId);

    // 8F. Read-only verification: ensure database was not mutated
    const verifyDb = new DatabaseSync(dbPath, { readOnly: true });
    const countRow = verifyDb.prepare("SELECT count(*) as cnt FROM queued_items").get() as { cnt: number };
    assert.equal(countRow.cnt, 3, "Database must not be mutated (row count must remain 3)");
    verifyDb.close();

    // 8G. Non-existent database path returns found: false safely without throwing
    const missingDbResult = await reconcileQueuedWake(targetThread, targetMarker, {
      dbPath: path.join(tmp, "nonexistent_queue.sqlite"),
    });
    assert.equal(missingDbResult.found, false, "Missing database file must return found: false");

    // 8H. Empty parameters return found: false safely
    const emptyThread = await transport.reconcileQueuedWake("", targetMarker);
    assert.equal(emptyThread.found, false);
    const emptyMarker = await transport.reconcileQueuedWake(targetThread, "");
    assert.equal(emptyMarker.found, false);
  } finally {
    if (prevCodexHome !== undefined) {
      process.env.CODEX_HOME = prevCodexHome;
    } else {
      delete process.env.CODEX_HOME;
    }
    await rm(tmp, { recursive: true, force: true });
  }
});
