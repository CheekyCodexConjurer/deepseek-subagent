import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { InboxDelivery } from "../../src/delivery/inbox.js";
import type { ResultEnvelope } from "../../src/types.js";

function envelopeFixture(directory: string, jobId: string): ResultEnvelope {
  return {
    version: 1,
    agentId: "agent_inbox",
    jobId,
    topic: "Inbox notifier",
    status: "completed",
    opencodeSessionId: "session_inbox",
    model: "opencode-go/deepseek-v4-flash · max",
    modelDisplayName: "DeepSeek V4 Flash · Max",
    workspace: directory,
    summary: "Persist once",
    files: [],
    tests: [],
    risks: [],
    diffSummary: "none",
    fullResultPath: path.join(directory, "result.json"),
    orchestratorInstruction: "fixture",
  } as ResultEnvelope;
}

test("inbox result delivery is idempotent across recovery", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-inbox-idempotent-"));
  let notifications = 0;
  const inbox = new InboxDelivery(directory, async () => {
    notifications += 1;
  });
  const envelope = {
    version: 1,
    agentId: "agent_inbox",
    jobId: "job_inbox",
    topic: "Inbox idempotency",
    status: "completed",
    opencodeSessionId: "session_inbox",
    model: "opencode-go/deepseek-v4-flash · max",
    modelDisplayName: "DeepSeek V4 Flash · Max",
    workspace: directory,
    summary: "Persist once",
    files: [],
    tests: [],
    risks: [],
    diffSummary: "none",
    fullResultPath: path.join(directory, "result.json"),
    orchestratorInstruction: "fixture",
  } as ResultEnvelope;
  try {
    const first = await inbox.deliver(envelope, "first");
    const second = await inbox.deliver(envelope, "second");
    assert.equal(first, second);
    assert.equal(notifications, 1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("concurrent inbox writers notify only the winner", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-inbox-concurrent-"));
  let notifications = 0;
  const notify = async () => {
    notifications += 1;
  };
  const firstInbox = new InboxDelivery(directory, notify);
  const secondInbox = new InboxDelivery(directory, notify);
  const envelope = {
    version: 1,
    agentId: "agent_concurrent",
    jobId: "job_concurrent",
    topic: "Concurrent inbox",
    status: "completed",
    opencodeSessionId: "session_concurrent",
    model: "opencode-go/deepseek-v4-flash · max",
    modelDisplayName: "DeepSeek V4 Flash · Max",
    workspace: directory,
    summary: "Persist once",
    files: [],
    tests: [],
    risks: [],
    diffSummary: "none",
    fullResultPath: path.join(directory, "result.json"),
    orchestratorInstruction: "fixture",
  } as ResultEnvelope;
  try {
    const paths = await Promise.all([
      firstInbox.deliver(envelope, "first"),
      secondInbox.deliver(envelope, "second"),
    ]);
    assert.equal(paths[0], paths[1]);
    assert.equal(notifications, 1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("default result delivery and notice writing work without a notifier", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-inbox-default-"));
  const inbox = new InboxDelivery(directory);
  try {
    const resultPath = await inbox.deliver(envelopeFixture(directory, "job_default"), "human text");
    assert.equal(resultPath, path.join(directory, "inbox", "job_default.json"));
    const resultPayload = JSON.parse(await readFile(resultPath, "utf8"));
    assert.equal(resultPayload.envelope.jobId, "job_default");
    assert.equal(resultPayload.humanText, "human text");

    const noticePath = await inbox.writeNotice({
      kind: "permission",
      agentId: "agent_inbox",
      jobId: "job_default",
      topic: "Needs permission",
      message: "Approve?",
    });
    assert.ok((await readFile(noticePath, "utf8")).includes("Approve?"));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("explicitly injected notifier receives callbacks for results and notices", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-inbox-notifier-"));
  const calls: string[] = [];
  const inbox = new InboxDelivery(directory, async (title, message) => {
    calls.push(title + ": " + message);
  });
  try {
    await inbox.deliver(envelopeFixture(directory, "job_notify"), "human text");
    await inbox.writeNotice({
      kind: "notice",
      agentId: "agent_inbox",
      jobId: "job_notify",
      topic: "Notice topic",
      message: "Notice message",
    });
    assert.equal(calls.length, 2);
    assert.equal(calls[0], "DeepSeek Sub-Agent: Inbox notifier: Persist once");
    assert.equal(calls[1], "DeepSeek Sub-Agent: Notice topic: Notice message");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
