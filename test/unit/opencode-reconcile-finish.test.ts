import test from "node:test";
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig } from "../../src/config.js";
import { InboxDelivery } from "../../src/delivery/inbox.js";
import { BridgeStore } from "../../src/store.js";
import { BridgeService, type ManagedOpenCodeLike, type OpenCodeManagerLike } from "../../src/service.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage } from "../../src/types.js";

class FakeOpenCodeClient implements OpenCodeClientLike {
  promptCalls: Array<{ sessionId: string; task: string }> = [];
  messages: OpenCodeMessage[] = [];
  diffCalls = 0;
  private onEvent?: (event: OpenCodeEvent) => Promise<void> | void;

  async health() {
    return { healthy: true, version: "fake" };
  }
  async createSession() {
    return { id: "session_fake_1" };
  }
  async promptAsync(sessionId: string, task: string) {
    this.promptCalls.push({ sessionId, task });
  }
  async listMessages() {
    return this.messages;
  }
  async getDiff() {
    this.diffCalls += 1;
    return "";
  }
  async abort() {}
  async replyPermission() {}
  async subscribe(onEvent: (event: OpenCodeEvent) => Promise<void> | void) {
    this.onEvent = onEvent;
  }
  async emit(event: OpenCodeEvent) {
    await this.onEvent?.(event);
  }
}

class FakeInbox extends InboxDelivery {
  delivered: string[] = [];
  constructor(directory: string) {
    super(directory, async () => undefined);
  }
  override async deliver(envelope: { jobId: string }, _humanText: string): Promise<string> {
    this.delivered.push(envelope.jobId);
    return "fake://" + envelope.jobId;
  }
}

function makeManager(client: OpenCodeClientLike): OpenCodeManagerLike {
  return {
    async start(): Promise<ManagedOpenCodeLike> {
      return {
        serverId: "server_fake",
        baseUrl: "http://127.0.0.1:9999",
        client,
        processId: null,
        stop: async () => undefined,
      };
    },
    async stop(): Promise<void> {},
  };
}

test("reconcileJob: assistant text with info.finish absent is streaming and must not be persisted, completed, or delivered", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "opencode-reconcile-absent-"));
  const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
  const client = new FakeOpenCodeClient();
  const inbox = new FakeInbox(tmpDir);
  const config = createDefaultConfig({ dataDir: tmpDir });
  const service = new BridgeService(config, {
    store,
    manager: makeManager(client),
    inbox,
  });
  await service.start();
  try {
    service.recordBackpressure("setup");
    const initialCredits = service.getTargetCredits();

    const accepted = await service.spawn({
      requestId: "req_absent",
      topic: "Partial stream absent finish",
      task: "Stream in progress",
      cwd: tmpDir,
      mode: "analyze",
    });
    const job = service.getJob(accepted.jobId)!;
    assert.equal(job.status, "running");

    // Assistant message is actively streaming: text is present, but info.finish is absent (undefined)
    client.messages = [
      {
        info: { id: "msg_user_1", role: "user" },
        parts: [{ type: "text", text: "Stream in progress" }],
      },
      {
        info: { id: "msg_asst_1", role: "assistant" },
        parts: [{ type: "text", text: "STATUS: completed\nStreaming partial content..." }],
      },
    ];

    await (service as any).reconcileJob(job);

    const refreshedJob = store.getJob(job.id)!;
    assert.equal(refreshedJob.resultPath, null, "Streaming assistant message without finish must not have a persisted resultPath");
    assert.equal(existsSync(path.join(tmpDir, "results", `${job.id}.json`)), false, "Streaming assistant message without finish must not write a result file to disk");
    assert.equal(refreshedJob.status, "running", "Job must remain in running status while assistant message is streaming without finish");
    assert.equal(inbox.delivered.includes(job.id), false, "Streaming assistant message without finish must not be delivered");
    assert.equal(store.getDeliveryByJob(job.id), null, "Streaming assistant message without finish must not create a delivery record");
    assert.equal((service as any).successfulJobIds.has(job.id), false, "Streaming assistant message must not be recorded in successfulJobIds");
    assert.equal(service.getTargetCredits(), initialCredits, "Streaming assistant message must not increment AIMD targetCredits");
  } finally {
    await service.stop();
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("reconcileJob: assistant text with info.finish null is streaming and must not be persisted, completed, or delivered", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "opencode-reconcile-null-"));
  const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
  const client = new FakeOpenCodeClient();
  const inbox = new FakeInbox(tmpDir);
  const config = createDefaultConfig({ dataDir: tmpDir });
  const service = new BridgeService(config, {
    store,
    manager: makeManager(client),
    inbox,
  });
  await service.start();
  try {
    service.recordBackpressure("setup");
    const initialCredits = service.getTargetCredits();

    const accepted = await service.spawn({
      requestId: "req_null",
      topic: "Partial stream null finish",
      task: "Stream with null finish",
      cwd: tmpDir,
      mode: "analyze",
    });
    const job = service.getJob(accepted.jobId)!;

    // Assistant message is actively streaming: info.finish is explicitly null
    client.messages = [
      {
        info: { id: "msg_user_1", role: "user" },
        parts: [{ type: "text", text: "Stream with null finish" }],
      },
      {
        info: { id: "msg_asst_1", role: "assistant", finish: null as unknown as undefined },
        parts: [{ type: "text", text: "STATUS: completed\nStreaming partial content..." }],
      },
    ];

    await (service as any).reconcileJob(job);

    const refreshedJob = store.getJob(job.id)!;
    assert.equal(refreshedJob.resultPath, null, "Streaming assistant message with null finish must not have a persisted resultPath");
    assert.equal(existsSync(path.join(tmpDir, "results", `${job.id}.json`)), false, "Streaming assistant message with null finish must not write a result file to disk");
    assert.equal(refreshedJob.status, "running", "Job must remain in running status while assistant message is streaming with null finish");
    assert.equal(inbox.delivered.includes(job.id), false, "Streaming assistant message with null finish must not be delivered");
    assert.equal(store.getDeliveryByJob(job.id), null, "Streaming assistant message with null finish must not create a delivery record");
    assert.equal((service as any).successfulJobIds.has(job.id), false, "Streaming assistant message must not be recorded in successfulJobIds");
    assert.equal(service.getTargetCredits(), initialCredits, "Streaming assistant message must not increment AIMD targetCredits");
  } finally {
    await service.stop();
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("recovery: startup recoverPendingJobs does not reconcile or deliver in-flight jobs with streaming assistant messages", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "opencode-recover-pending-"));
  const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
  const client = new FakeOpenCodeClient();
  const inbox = new FakeInbox(tmpDir);
  const config = createDefaultConfig({ dataDir: tmpDir });

  // Pre-seed an active agent and a running job in the store before service start
  const agent = store.createAgent({
    id: "agent_recovery_1",
    title: "Recovery Agent",
    topic: "Recovery Topic",
    repositoryRoot: tmpDir,
    workspacePath: tmpDir,
    workspaceStrategy: "shared",
    opencodeServerId: "server_fake",
    opencodeSessionId: "session_fake_1",
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
  });
  store.updateAgentStatus(agent.id, "working");

  const job = store.createJob({
    id: "job_recovery_1",
    agentId: agent.id,
    kind: "spawn",
    requestId: "req_recovery_1",
    promptHash: "hash_recovery_1",
    status: "running",
  });

  // Client messages show streaming output: text is present, finish is absent
  client.messages = [
    {
      info: { id: "msg_user_1", role: "user" },
      parts: [{ type: "text", text: "In-flight task" }],
    },
    {
      info: { id: "msg_asst_1", role: "assistant" },
      parts: [{ type: "text", text: "STATUS: completed\nStreaming in progress during crash..." }],
    },
  ];

  const service = new BridgeService(config, {
    store,
    manager: makeManager(client),
    inbox,
  });
  service.recordBackpressure("setup");
  const initialCredits = service.getTargetCredits();

  try {
    // service.start() executes recoverPendingJobs()
    await service.start();

    const refreshedJob = store.getJob(job.id)!;
    assert.equal(refreshedJob.resultPath, null, "Recovery must not persist result for streaming job");
    assert.equal(existsSync(path.join(tmpDir, "results", `${job.id}.json`)), false, "Recovery must not write a result file for streaming job");
    assert.equal(refreshedJob.status, "running", "Recovery must leave streaming job in running state");
    assert.equal(inbox.delivered.includes(job.id), false, "Recovery must not deliver streaming job");
    assert.equal(store.getDeliveryByJob(job.id), null, "Recovery must not create a delivery record for streaming job");
    assert.equal((service as any).successfulJobIds.has(job.id), false, "Recovery must not record terminal AIMD success for streaming job");
    assert.equal(service.getTargetCredits(), initialCredits, "Recovery must not increment AIMD targetCredits for streaming job");
  } finally {
    await service.stop();
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("reconcileJob / recovery: assistant info.error is authoritative failure", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "opencode-reconcile-error-"));
  const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
  const client = new FakeOpenCodeClient();
  const inbox = new FakeInbox(tmpDir);
  const config = createDefaultConfig({ dataDir: tmpDir });
  const service = new BridgeService(config, {
    store,
    manager: makeManager(client),
    inbox,
  });
  await service.start();
  try {
    service.recordBackpressure("setup");
    const initialCredits = service.getTargetCredits();

    const accepted = await service.spawn({
      requestId: "req_error",
      topic: "Authoritative error test",
      task: "Task encountering error",
      cwd: tmpDir,
      mode: "analyze",
    });
    const job = service.getJob(accepted.jobId)!;

    // Assistant message reports an authoritative error in info.error
    client.messages = [
      {
        info: { id: "msg_user_1", role: "user" },
        parts: [{ type: "text", text: "Task encountering error" }],
      },
      {
        info: {
          id: "msg_asst_1",
          role: "assistant",
          error: { code: "rate_limit_exceeded", message: "Model provider rate limit reached" },
        },
        parts: [{ type: "text", text: "Execution aborted due to rate limit" }],
      },
    ];

    await (service as any).reconcileJob(job);

    const refreshedJob = store.getJob(job.id)!;
    assert.equal(refreshedJob.status, "failed", "Assistant info.error must transition job to failed");
    assert.ok(refreshedJob.error && refreshedJob.error.includes("rate_limit"), "Job error must record authoritative error details");
    const refreshedAgent = store.getAgent(job.agentId)!;
    assert.equal(refreshedAgent.status, "failed", "Assistant info.error must transition agent to failed");
    assert.equal(refreshedJob.resultPath, null, "Failed job must not persist a successful resultPath");
    assert.equal(inbox.delivered.includes(job.id), false, "Failed job must not be delivered as a result");
    assert.equal((service as any).successfulJobIds.has(job.id), false, "Authoritative failure must not be recorded in successfulJobIds");
    assert.equal(service.getTargetCredits(), initialCredits, "Authoritative failure must not increase AIMD targetCredits");
  } finally {
    await service.stop();
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("reconcileJob: assistant text with terminal finish value ('stop') completes exactly once with no duplicate delivery and no duplicate AIMD success", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "opencode-reconcile-stop-"));
  const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
  const client = new FakeOpenCodeClient();
  const inbox = new FakeInbox(tmpDir);
  const config = createDefaultConfig({ dataDir: tmpDir });
  const service = new BridgeService(config, {
    store,
    manager: makeManager(client),
    inbox,
  });
  await service.start();
  try {
    service.recordBackpressure("setup");
    const initialCredits = service.getTargetCredits();

    const accepted = await service.spawn({
      requestId: "req_terminal_stop",
      topic: "Terminal stop finish",
      task: "Task completing normally",
      cwd: tmpDir,
      mode: "analyze",
    });
    const job = service.getJob(accepted.jobId)!;

    // Assistant message has completed with a terminal finish value ('stop')
    client.messages = [
      {
        info: { id: "msg_user_1", role: "user" },
        parts: [{ type: "text", text: "Task completing normally" }],
      },
      {
        info: { id: "msg_asst_1", role: "assistant", finish: "stop" },
        parts: [{
          type: "text",
          text: "STATUS: completed\nSUMMARY: Task completed successfully\nFILES: none\nTESTS: none\nRISKS: none\nUNRESOLVED: none",
        }],
      },
    ];

    // First reconcile: eligible for normal completion exactly once
    await (service as any).reconcileJob(job);

    const completedJob = store.getJob(job.id)!;
    assert.ok(
      ["completed", "delivery_pending", "delivered"].includes(completedJob.status),
      `Job should reach terminal completion state, got: ${completedJob.status}`,
    );
    assert.ok(completedJob.resultPath !== null, "Terminal finish message must have a persisted resultPath");
    assert.equal(existsSync(completedJob.resultPath!), true, "Result file must exist on disk");
    assert.equal(inbox.delivered.filter((id) => id === job.id).length, 1, "Job must be delivered exactly once");
    assert.equal((service as any).successfulJobIds.has(job.id), true, "Terminal success must be recorded in successfulJobIds");
    assert.equal(service.getTargetCredits(), initialCredits + 1, "AIMD credits must increment exactly once on terminal completion");

    const creditsAfterCompletion = service.getTargetCredits();
    const deliveriesAfterCompletion = inbox.delivered.filter((id) => id === job.id).length;

    // Second reconcile: duplicate call / recovery must NOT cause duplicate delivery or AIMD credit increment
    await (service as any).reconcileJob(completedJob);

    assert.equal(
      inbox.delivered.filter((id) => id === job.id).length,
      deliveriesAfterCompletion,
      "Duplicate reconcileJob must not produce duplicate delivery",
    );
    assert.equal(
      service.getTargetCredits(),
      creditsAfterCompletion,
      "Duplicate reconcileJob must not produce duplicate AIMD credit increments",
    );
  } finally {
    await service.stop();
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("reconcileJob: full progression from streaming to terminal finish with no false early completion or duplicate delivery", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "opencode-reconcile-progression-"));
  const store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
  const client = new FakeOpenCodeClient();
  const inbox = new FakeInbox(tmpDir);
  const config = createDefaultConfig({ dataDir: tmpDir });
  const service = new BridgeService(config, {
    store,
    manager: makeManager(client),
    inbox,
  });
  await service.start();
  try {
    service.recordBackpressure("setup");
    const initialCredits = service.getTargetCredits();

    const accepted = await service.spawn({
      requestId: "req_progression",
      topic: "Stream progression test",
      task: "Progression task",
      cwd: tmpDir,
      mode: "analyze",
    });
    const job = service.getJob(accepted.jobId)!;

    // Stage 1: Streaming chunk (no finish value)
    client.messages = [
      {
        info: { id: "msg_user_1", role: "user" },
        parts: [{ type: "text", text: "Progression task" }],
      },
      {
        info: { id: "msg_asst_1", role: "assistant" },
        parts: [{ type: "text", text: "Partial output chunk 1" }],
      },
    ];

    await (service as any).reconcileJob(job);

    let currentJob = store.getJob(job.id)!;
    assert.equal(currentJob.resultPath, null, "Stage 1: Streaming message must not persist result");
    assert.equal(currentJob.status, "running", "Stage 1: Job must remain running while streaming");
    assert.equal(inbox.delivered.length, 0, "Stage 1: No delivery while streaming");
    assert.equal(service.getTargetCredits(), initialCredits, "Stage 1: No AIMD credit increment while streaming");

    // Stage 2: Stream completes with terminal finish: 'stop'
    client.messages = [
      {
        info: { id: "msg_user_1", role: "user" },
        parts: [{ type: "text", text: "Progression task" }],
      },
      {
        info: { id: "msg_asst_1", role: "assistant", finish: "stop" },
        parts: [{
          type: "text",
          text: "STATUS: completed\nSUMMARY: Finished work\nFILES: none\nTESTS: none\nRISKS: none\nUNRESOLVED: none",
        }],
      },
    ];

    await (service as any).reconcileJob(currentJob);

    currentJob = store.getJob(job.id)!;
    assert.ok(currentJob.resultPath !== null, "Stage 2: Result must be persisted once terminal finish received");
    assert.ok(["completed", "delivery_pending", "delivered"].includes(currentJob.status), "Stage 2: Job must complete");
    assert.equal(inbox.delivered.filter((id) => id === job.id).length, 1, "Stage 2: Exactly one delivery");
    assert.equal(service.getTargetCredits(), initialCredits + 1, "Stage 2: Exactly one AIMD credit increment");

    // Stage 3: Re-reconcile does nothing extra
    await (service as any).reconcileJob(currentJob);

    assert.equal(inbox.delivered.filter((id) => id === job.id).length, 1, "Stage 3: No duplicate delivery");
    assert.equal(service.getTargetCredits(), initialCredits + 1, "Stage 3: No duplicate credit increment");
  } finally {
    await service.stop();
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});
