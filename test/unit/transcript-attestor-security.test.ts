import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile, mkdir, utimes } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  TranscriptAttestor,
  findRecentJsonlFiles,
  readFileTail,
  isValidPath,
  resolveSessionsDir,
  DEFAULT_ACCEPTED_SERVERS,
  DEFAULT_ACCEPTED_TOOLS,
} from "../../src/codex/transcript-attestor.js";
import { BridgeStore } from "../../src/store.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CANONICAL_THREAD_ID = "01a06d2f-6d39-74f3-b122-f4a2194a0706";
const CANONICAL_TURN_ID = "01a06dbe-d2b8-7073-a71f-a36b479546c7";

function makeCanonicalEvent({
  jobId,
  threadId = CANONICAL_THREAD_ID,
  turnId = CANONICAL_TURN_ID,
  itemId = "item_spawn_001",
  server = "deepseek-subagent",
  tool = "subagents_spawn",
  status = "completed",
  accepted = true,
  useMetaTechnical = false,
  timestamp = "2026-09-04T12:00:00.000Z",
}: {
  jobId: string;
  threadId?: string;
  turnId?: string;
  itemId?: string;
  server?: string;
  tool?: string;
  status?: string;
  accepted?: boolean;
  useMetaTechnical?: boolean;
  timestamp?: string | number;
}) {
  const result: Record<string, unknown> = {};
  if (useMetaTechnical) {
    result._meta = { technical: { jobId, accepted } };
  } else {
    result.structuredContent = { jobId, accepted };
  }

  return {
    type: "event_msg",
    timestamp,
    payload: {
      type: "item_completed",
      thread_id: threadId,
      turn_id: turnId,
      item: {
        id: itemId,
        type: "McpToolCall",
        status,
        server,
        tool,
        result,
      },
    },
  };
}

// ---------------------------------------------------------------------------
// 1. Canonical Codex JSONL Attestation Acceptance
// ---------------------------------------------------------------------------

test("canonical acceptance: accepts valid event_msg -> item_completed -> McpToolCall -> deepseek-subagent -> subagents_spawn", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_canonical_001";
    const event = makeCanonicalEvent({
      jobId: targetJobId,
      server: "deepseek-subagent",
      tool: "subagents_spawn",
      status: "completed",
      accepted: true,
    });

    await writeFile(path.join(tmpDir, "session_001.jsonl"), JSON.stringify(event) + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.ok(match, "Canonical subagents_spawn event must be attested");
    assert.equal(match.jobId, targetJobId);
    assert.equal(match.threadId, CANONICAL_THREAD_ID);
    assert.equal(match.turnId, CANONICAL_TURN_ID);
    assert.equal(match.itemId, "item_spawn_001");
    assert.equal(match.server, "deepseek-subagent");
    assert.equal(match.tool, "subagents_spawn");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("canonical acceptance: accepts deepseek_spawn tool under deepseek-subagent server", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_deepseek_spawn_001";
    const event = makeCanonicalEvent({
      jobId: targetJobId,
      server: "deepseek-subagent",
      tool: "deepseek_spawn",
    });

    await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.ok(match, "Canonical deepseek_spawn event must be attested");
    assert.equal(match.jobId, targetJobId);
    assert.equal(match.tool, "deepseek_spawn");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("canonical acceptance: accepts continuation tools and alternative accepted servers", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });

    const cases = [
      { server: "subagents", tool: "subagents_spawn", jobId: "job_sub_spawn" },
      { server: "subagents", tool: "subagents_continue", jobId: "job_sub_cont" },
      { server: "deepseek", tool: "deepseek_continue", jobId: "job_ds_cont" },
    ];

    for (const c of cases) {
      const event = makeCanonicalEvent({
        jobId: c.jobId,
        server: c.server,
        tool: c.tool,
      });
      await writeFile(path.join(tmpDir, `${c.jobId}.jsonl`), JSON.stringify(event) + "\n");
      const match = await attestor.attestJob(c.jobId);
      assert.ok(match, `Expected attestation for server ${c.server} and tool ${c.tool}`);
      assert.equal(match.jobId, c.jobId);
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("canonical acceptance: accepts result._meta.technical.jobId format", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_meta_tech_001";
    const event = makeCanonicalEvent({
      jobId: targetJobId,
      useMetaTechnical: true,
    });

    await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.ok(match, "Attestor must support _meta.technical.jobId");
    assert.equal(match.jobId, targetJobId);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("canonical acceptance: case insensitivity for item.type (mcptoolcall) and item.status (completed)", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_case_insensitive";
    const event = {
      type: "event_msg",
      payload: {
        type: "item_completed",
        thread_id: CANONICAL_THREAD_ID,
        turn_id: CANONICAL_TURN_ID,
        item: {
          id: "item_ci_1",
          type: "mcptoolcall",
          status: "COMPLETED",
          server: "DEEPSEEK-SUBAGENT",
          tool: "SUBAGENTS_SPAWN",
          result: {
            structuredContent: { jobId: targetJobId, accepted: true },
          },
        },
      },
    };

    await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.ok(match, "Attestor must handle case insensitivity for McpToolCall and COMPLETED");
    assert.equal(match.jobId, targetJobId);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Reject User and Assistant Mentions (Prompt / Chat Spoofing)
// ---------------------------------------------------------------------------

test("security rejection: rejects user mentions mentioning jobId in raw message or user payload", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_user_spoof_001";
    const userLines = [
      JSON.stringify({ type: "user_message", content: `Please inspect ${targetJobId}` }),
      JSON.stringify({
        type: "event_msg",
        payload: { type: "user_message", text: `I want to track ${targetJobId}` },
      }),
      JSON.stringify({
        type: "event_msg",
        payload: {
          type: "item_completed",
          thread_id: CANONICAL_THREAD_ID,
          turn_id: CANONICAL_TURN_ID,
          item: {
            id: "item_user_msg",
            type: "user_message",
            content: `Triggering ${targetJobId}`,
          },
        },
      }),
    ];

    await writeFile(path.join(tmpDir, "session.jsonl"), userLines.join("\n") + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.equal(match, null, "User mentions must NEVER attest as valid MCP tool call provenance");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("security rejection: rejects assistant mentions, thoughts, and text output containing jobId", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_assistant_spoof_001";
    const assistantLines = [
      JSON.stringify({ type: "assistant_message", content: `I have started ${targetJobId}` }),
      JSON.stringify({
        type: "event_msg",
        payload: {
          type: "item_completed",
          thread_id: CANONICAL_THREAD_ID,
          turn_id: CANONICAL_TURN_ID,
          item: {
            id: "item_assistant_msg",
            type: "assistant_message",
            text: `Confirmed jobId: ${targetJobId}`,
          },
        },
      }),
      JSON.stringify({
        type: "event_msg",
        payload: {
          type: "item_completed",
          thread_id: CANONICAL_THREAD_ID,
          turn_id: CANONICAL_TURN_ID,
          item: {
            id: "item_thought",
            type: "reasoning",
            content: `Planning subagent with ${targetJobId}`,
          },
        },
      }),
    ];

    await writeFile(path.join(tmpDir, "session.jsonl"), assistantLines.join("\n") + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.equal(match, null, "Assistant text mentions must NEVER attest as valid tool call provenance");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("security rejection: rejects non-item_completed payloads (e.g. item_started, turn_completed)", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_uncompleted_item";
    const lines = [
      JSON.stringify({
        type: "event_msg",
        payload: {
          type: "item_started",
          thread_id: CANONICAL_THREAD_ID,
          turn_id: CANONICAL_TURN_ID,
          item: {
            id: "item_001",
            type: "McpToolCall",
            server: "deepseek-subagent",
            tool: "subagents_spawn",
            arguments: { prompt: `Start ${targetJobId}` },
          },
        },
      }),
      JSON.stringify({
        type: "event_msg",
        payload: {
          type: "turn_completed",
          thread_id: CANONICAL_THREAD_ID,
          turn_id: CANONICAL_TURN_ID,
          summary: `Finished turn for ${targetJobId}`,
        },
      }),
    ];

    await writeFile(path.join(tmpDir, "session.jsonl"), lines.join("\n") + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.equal(match, null, "Non-item_completed payloads must be rejected");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 3. Reject Wrong Server or Tool
// ---------------------------------------------------------------------------

test("security rejection: rejects wrong server or unauthorized MCP server", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_wrong_server";
    const untrustedServers = ["untrusted-server", "bash", "filesystem", "desktop", "subagents-fake"];

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });

    for (const server of untrustedServers) {
      const event = makeCanonicalEvent({
        jobId: targetJobId,
        server,
        tool: "subagents_spawn",
      });
      await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");
      const match = await attestor.attestJob(targetJobId);
      assert.equal(match, null, `Server "${server}" must be rejected`);
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("security rejection: rejects wrong tool under accepted server", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_wrong_tool";
    const unauthorizedTools = ["bash", "exec_command", "subagents_destroy", "subagents_status", "eval"];

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });

    for (const tool of unauthorizedTools) {
      const event = makeCanonicalEvent({
        jobId: targetJobId,
        server: "deepseek-subagent",
        tool,
      });
      await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");
      const match = await attestor.attestJob(targetJobId);
      assert.equal(match, null, `Tool "${tool}" must be rejected`);
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("security rejection: respects explicit custom acceptedServers and acceptedTools options", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_custom_filters";
    const event = makeCanonicalEvent({
      jobId: targetJobId,
      server: "deepseek",
      tool: "deepseek_spawn",
    });
    await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });

    // Restricting acceptedServers to only deepseek-subagent
    const matchServer = await attestor.attestJob(targetJobId, {
      acceptedServers: ["deepseek-subagent"],
    });
    assert.equal(matchServer, null, "Must reject server deepseek when custom acceptedServers excludes it");

    // Restricting acceptedTools to only subagents_spawn
    const matchTool = await attestor.attestJob(targetJobId, {
      acceptedTools: ["subagents_spawn"],
    });
    assert.equal(matchTool, null, "Must reject tool deepseek_spawn when custom acceptedTools excludes it");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 4. Reject Rejected Spawn or Non-Completed Status
// ---------------------------------------------------------------------------

test("security rejection: rejects rejected spawn (accepted=false in structuredContent)", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_rejected_spawn";
    const event = makeCanonicalEvent({
      jobId: targetJobId,
      accepted: false,
    });
    await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.equal(match, null, "Spawn with accepted=false must be rejected");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("security rejection: rejects non-completed item status (failed, in_progress, cancelled)", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_non_completed";
    const nonCompletedStatuses = ["failed", "in_progress", "cancelled", "running", "error"];

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });

    for (const status of nonCompletedStatuses) {
      const event = makeCanonicalEvent({
        jobId: targetJobId,
        status,
      });
      await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");
      const match = await attestor.attestJob(targetJobId);
      assert.equal(match, null, `Item status "${status}" must be rejected`);
    }
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("security rejection: rejects invalid UUID formats in thread_id or turn_id", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });

    // Invalid thread_id
    const eventBadThread = makeCanonicalEvent({
      jobId: "job_bad_thread",
      threadId: "not-a-valid-uuid",
    });
    await writeFile(path.join(tmpDir, "session1.jsonl"), JSON.stringify(eventBadThread) + "\n");
    const match1 = await attestor.attestJob("job_bad_thread");
    assert.equal(match1, null, "Non-UUID thread_id must be rejected");

    // Invalid turn_id
    const eventBadTurn = makeCanonicalEvent({
      jobId: "job_bad_turn",
      turnId: "invalid-turn-id",
    });
    await writeFile(path.join(tmpDir, "session2.jsonl"), JSON.stringify(eventBadTurn) + "\n");
    const match2 = await attestor.attestJob("job_bad_turn");
    assert.equal(match2, null, "Non-UUID turn_id must be rejected");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 5. Ambiguous Matches & Idempotent Duplicates
// ---------------------------------------------------------------------------

test("security rejection: fails closed on ambiguous matches with conflicting provenance", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_ambiguous_001";
    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });

    // Conflict A: Different threadId claiming same jobId
    const eventA1 = makeCanonicalEvent({
      jobId: targetJobId,
      threadId: "01a06d2f-6d39-74f3-b122-f4a2194a0706",
      turnId: "01a06dbe-d2b8-7073-a71f-a36b479546c7",
      itemId: "item_001",
    });
    const eventA2 = makeCanonicalEvent({
      jobId: targetJobId,
      threadId: "01a06d2f-6d39-74f3-b122-f4a2194a0707",
      turnId: "01a06dbe-d2b8-7073-a71f-a36b479546c7",
      itemId: "item_001",
    });
    await writeFile(path.join(tmpDir, "conflict_thread.jsonl"), `${JSON.stringify(eventA1)}\n${JSON.stringify(eventA2)}\n`);
    const matchA = await attestor.attestJob(targetJobId);
    assert.equal(matchA, null, "Conflicting thread IDs must fail closed");

    // Conflict B: Different turnId claiming same jobId
    const eventB1 = makeCanonicalEvent({
      jobId: "job_ambiguous_turn",
      threadId: CANONICAL_THREAD_ID,
      turnId: "01a06dbe-d2b8-7073-a71f-a36b479546c7",
      itemId: "item_001",
    });
    const eventB2 = makeCanonicalEvent({
      jobId: "job_ambiguous_turn",
      threadId: CANONICAL_THREAD_ID,
      turnId: "01a06dbe-d2b8-7073-a71f-a36b479546c8",
      itemId: "item_001",
    });
    await writeFile(path.join(tmpDir, "conflict_turn.jsonl"), `${JSON.stringify(eventB1)}\n${JSON.stringify(eventB2)}\n`);
    const matchB = await attestor.attestJob("job_ambiguous_turn");
    assert.equal(matchB, null, "Conflicting turn IDs must fail closed");

    // Conflict C: Different itemId claiming same jobId
    const eventC1 = makeCanonicalEvent({
      jobId: "job_ambiguous_item",
      threadId: CANONICAL_THREAD_ID,
      turnId: CANONICAL_TURN_ID,
      itemId: "item_001",
    });
    const eventC2 = makeCanonicalEvent({
      jobId: "job_ambiguous_item",
      threadId: CANONICAL_THREAD_ID,
      turnId: CANONICAL_TURN_ID,
      itemId: "item_002",
    });
    await writeFile(path.join(tmpDir, "conflict_item.jsonl"), `${JSON.stringify(eventC1)}\n${JSON.stringify(eventC2)}\n`);
    const matchC = await attestor.attestJob("job_ambiguous_item");
    assert.equal(matchC, null, "Conflicting item IDs must fail closed");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("security idempotence: accepts duplicate identical lines with exact same provenance", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_idempotent_dup";
    const event = makeCanonicalEvent({
      jobId: targetJobId,
      threadId: CANONICAL_THREAD_ID,
      turnId: CANONICAL_TURN_ID,
      itemId: "item_idempotent_1",
    });

    // File contains multiple copies of the exact same event
    const content = `${JSON.stringify(event)}\n${JSON.stringify(event)}\n${JSON.stringify(event)}\n`;
    await writeFile(path.join(tmpDir, "idempotent.jsonl"), content);

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.ok(match, "Exact duplicate records must be accepted idempotently");
    assert.equal(match.jobId, targetJobId);
    assert.equal(match.threadId, CANONICAL_THREAD_ID);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 6. Caller Hint Consistency
// ---------------------------------------------------------------------------

test("caller hints: rejects caller hint mismatch and accepts matching hint", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_caller_hint";
    const event = makeCanonicalEvent({
      jobId: targetJobId,
      threadId: CANONICAL_THREAD_ID,
      turnId: CANONICAL_TURN_ID,
    });
    await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });

    // Matching hints -> succeed
    const matchOk = await attestor.attestJob(targetJobId, {
      callerHint: {
        threadId: CANONICAL_THREAD_ID,
        turnId: CANONICAL_TURN_ID,
      },
    });
    assert.ok(matchOk, "Must succeed when caller hints match transcript");

    // Case-insensitive matching hints -> succeed
    const matchCaseOk = await attestor.attestJob(targetJobId, {
      callerHint: {
        threadId: CANONICAL_THREAD_ID.toUpperCase(),
        turnId: CANONICAL_TURN_ID.toUpperCase(),
      },
    });
    assert.ok(matchCaseOk, "Must succeed with case-insensitive hint comparison");

    // Mismatched thread hint -> fail closed
    const matchBadThread = await attestor.attestJob(targetJobId, {
      callerHint: {
        threadId: "00000000-0000-4000-8000-000000000000",
        turnId: CANONICAL_TURN_ID,
      },
    });
    assert.equal(matchBadThread, null, "Must reject mismatched caller thread hint");

    // Mismatched turn hint -> fail closed
    const matchBadTurn = await attestor.attestJob(targetJobId, {
      callerHint: {
        threadId: CANONICAL_THREAD_ID,
        turnId: "00000000-0000-4000-8000-000000000000",
      },
    });
    assert.equal(matchBadTurn, null, "Must reject mismatched caller turn hint");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 7. Robustness: Partial EOF, Corrupted Lines, and Garbage Tolerance
// ---------------------------------------------------------------------------

test("robustness: tolerates partial EOF line without crashing and attests prior valid line", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_partial_eof";
    const validEvent = makeCanonicalEvent({ jobId: targetJobId });

    // Valid JSON line followed by truncated partial JSON at EOF
    const content = `${JSON.stringify(validEvent)}\n{"type":"event_msg","payload":{"type":"item_up`;
    await writeFile(path.join(tmpDir, "session_eof.jsonl"), content);

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.ok(match, "Must safely attest prior line despite truncated EOF line");
    assert.equal(match.jobId, targetJobId);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("robustness: ignores unparseable partial EOF line that contains target jobId", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_incomplete_json";
    // Only a broken JSON line containing the jobId
    const brokenLine = `{"type":"event_msg","payload":{"type":"item_completed","item":{"result":{"structuredContent":{"jobId":"${targetJobId}"`;
    await writeFile(path.join(tmpDir, "broken.jsonl"), brokenLine);

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.equal(match, null, "Unparseable truncated line must not crash or falsely attest");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("robustness: skips blank lines, non-JSON logs, and malformed lines", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const targetJobId = "job_mixed_logs";
    const validEvent = makeCanonicalEvent({ jobId: targetJobId });

    const mixedContent = [
      "",
      "   ",
      "[INFO] 2026-09-04 12:00:00 Starting Codex session",
      "not json at all",
      "{ invalid json obj",
      JSON.stringify(validEvent),
      "--- end of turn ---",
    ].join("\n") + "\n";

    await writeFile(path.join(tmpDir, "mixed.jsonl"), mixedContent);

    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const match = await attestor.attestJob(targetJobId);

    assert.ok(match, "Must find valid record amidst logs and whitespace");
    assert.equal(match.jobId, targetJobId);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 8. Bounded Recent-File Behavior and Traversal
// ---------------------------------------------------------------------------

test("bounding: enforces maxFiles cutoff and prioritizes newer files by mtime", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    const baseTime = Date.now() / 1000;
    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });

    // Create 4 files with descending mtimes
    const targetJobOld = "job_in_old_file";
    const targetJobNew = "job_in_new_file";

    const fileOld = path.join(tmpDir, "file_old.jsonl");
    const fileMid1 = path.join(tmpDir, "file_mid1.jsonl");
    const fileMid2 = path.join(tmpDir, "file_mid2.jsonl");
    const fileNew = path.join(tmpDir, "file_new.jsonl");

    await writeFile(fileOld, JSON.stringify(makeCanonicalEvent({ jobId: targetJobOld })) + "\n");
    await writeFile(fileMid1, '{"type":"turn.started"}\n');
    await writeFile(fileMid2, '{"type":"turn.started"}\n');
    await writeFile(fileNew, JSON.stringify(makeCanonicalEvent({ jobId: targetJobNew })) + "\n");

    // Adjust mtimes: fileNew > fileMid2 > fileMid1 > fileOld
    await utimes(fileOld, baseTime - 40, baseTime - 40);
    await utimes(fileMid1, baseTime - 30, baseTime - 30);
    await utimes(fileMid2, baseTime - 20, baseTime - 20);
    await utimes(fileNew, baseTime - 10, baseTime - 10);

    // With maxFiles: 2, only fileNew and fileMid2 are inspected.
    // targetJobNew should be found:
    const matchNew = await attestor.attestJob(targetJobNew, { maxFiles: 2 });
    assert.ok(matchNew, "Newest file must be inspected within maxFiles=2");
    assert.equal(matchNew.jobId, targetJobNew);

    // targetJobOld is in fileOld (rank 4), beyond maxFiles=2 cutoff -> must return null:
    const matchOld = await attestor.attestJob(targetJobOld, { maxFiles: 2 });
    assert.equal(matchOld, null, "File beyond maxFiles cutoff must not be inspected");

    // With maxFiles: 4, fileOld is included -> succeeds:
    const matchOldExpanded = await attestor.attestJob(targetJobOld, { maxFiles: 4 });
    assert.ok(matchOldExpanded, "File must be inspected when maxFiles window covers it");
    assert.equal(matchOldExpanded.jobId, targetJobOld);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("bounding: findRecentJsonlFiles enforces maxDepth restriction", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-test-"));
  try {
    // Structure: tmpDir/l1/l2/l3/l4/l5/deep.jsonl
    const deepDir = path.join(tmpDir, "l1", "l2", "l3", "l4", "l5");
    await mkdir(deepDir, { recursive: true });
    await writeFile(path.join(deepDir, "deep.jsonl"), "{}\n");

    const shallowDir = path.join(tmpDir, "l1");
    await writeFile(path.join(shallowDir, "shallow.jsonl"), "{}\n");

    // Default maxDepth = 4: deepDir (depth 6) must not be walked
    const foundDefault = await findRecentJsonlFiles(tmpDir, 4);
    assert.ok(foundDefault.some((f) => f.includes("shallow.jsonl")), "Must find shallow file");
    assert.ok(!foundDefault.some((f) => f.includes("deep.jsonl")), "Must NOT walk directories deeper than maxDepth");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 9. Store Binding Verification from Canonical Attestation
// ---------------------------------------------------------------------------

test("store binding: binds job provenance in SQLite when transcript attestation succeeds", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-store-"));
  const dbPath = path.join(tmpDir, "bridge.sqlite");
  const store = new BridgeStore(dbPath);

  try {
    const agent = store.createAgent({
      id: "agent_store_attest",
      title: "Store Attest Agent",
      topic: "Topic",
      repositoryRoot: tmpDir,
      workspacePath: tmpDir,
      workspaceStrategy: "shared",
      opencodeServerId: "srv",
      opencodeSessionId: "session_1",
      modelProviderId: "deepseek",
      modelId: "deepseek-chat",
      modelVariant: null,
    });

    const job = store.createJob({
      id: "job_store_test_001",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_store_1",
      promptHash: "hash_store_1",
    });

    // 1. Initially unbound
    assert.equal(store.getBinding(job.id), null);

    // 2. Write canonical transcript
    const sessionsDir = path.join(tmpDir, "sessions");
    await mkdir(sessionsDir, { recursive: true });
    const event = makeCanonicalEvent({
      jobId: job.id,
      threadId: CANONICAL_THREAD_ID,
      turnId: CANONICAL_TURN_ID,
      itemId: "item_store_001",
    });
    await writeFile(path.join(sessionsDir, "session_store.jsonl"), JSON.stringify(event) + "\n");

    // 3. Attest and bind
    const attestor = new TranscriptAttestor({ sessionsDir });
    const match = await attestor.attestJob(job.id);
    assert.ok(match, "Canonical event must be attested");

    const binding = store.bindJob({
      jobId: job.id,
      threadId: match.threadId,
      originatingTurnId: match.turnId,
      originatingItemId: match.itemId,
    });

    assert.equal(binding.jobId, job.id);
    assert.equal(binding.threadId, CANONICAL_THREAD_ID);
    assert.equal(binding.originatingTurnId, CANONICAL_TURN_ID);
    assert.equal(binding.originatingItemId, "item_store_001");

    // 4. Stored binding is persistent in SQLite
    const retrieved = store.getBinding(job.id);
    assert.ok(retrieved);
    assert.equal(retrieved.threadId, CANONICAL_THREAD_ID);
  } finally {
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 10. Source API Contract: Short Bounded Retry for Delayed Transcript Flush
// ---------------------------------------------------------------------------

test("source API contract: short bounded retry seam for asynchronous transcript flush", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-retry-"));
  let timer: NodeJS.Timeout | undefined;
  try {
    const attestor = new TranscriptAttestor({ sessionsDir: tmpDir });
    const targetJobId = "job_delayed_flush_001";

    // Simulate asynchronous transcript file flush by Codex 30ms after attestJob is called
    timer = setTimeout(async () => {
      try {
        const validEvent = makeCanonicalEvent({
          jobId: targetJobId,
          threadId: CANONICAL_THREAD_ID,
          turnId: CANONICAL_TURN_ID,
          itemId: "item_delayed_1",
        });
        await writeFile(path.join(tmpDir, "delayed_session.jsonl"), JSON.stringify(validEvent) + "\n");
      } catch {}
    }, 30);

    // Contract expectation: TranscriptAttestor.attestJob should support bounded retry options
    // (e.g. retries and retryDelayMs, or timeoutMs) to handle asynchronous transcript flushing.
    // If the source API is still incomplete (lacks retry options and polling loop), this test
    // will return null on the first immediate poll and fail (keeping the test RED).
    const match = await (attestor as any).attestJob(targetJobId, {
      retries: 3,
      retryDelayMs: 30,
      timeoutMs: 200,
    });

    assert.ok(
      match !== null,
      "MISSING SOURCE CONTRACT: TranscriptAttestor.attestJob lacks short bounded retry support for asynchronous transcript flushing",
    );
    assert.equal(match.jobId, targetJobId);
  } finally {
    if (timer) clearTimeout(timer);
    await rm(tmpDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 11. Deterministic Environment Discovery & Precedence
// ---------------------------------------------------------------------------

async function withIsolatedEnv<T>(fn: () => Promise<T> | T): Promise<T> {
  const savedEnv = { ...process.env };
  const restore = () => {
    for (const key of Object.keys(process.env)) {
      if (!(key in savedEnv)) {
        delete process.env[key];
      }
    }
    for (const [key, value] of Object.entries(savedEnv)) {
      process.env[key] = value;
    }
  };

  try {
    return await fn();
  } finally {
    restore();
  }
}

test("env discovery: explicit options.sessionsDir takes precedence over CODEX_SESSIONS_DIR, CODEX_HOME, and standard user home", async () => {
  await withIsolatedEnv(async () => {
    process.env.CODEX_SESSIONS_DIR = "C:\\env\\sessions";
    process.env.CODEX_HOME = "C:\\env\\home";

    const explicit = "C:\\explicit\\sessions";
    assert.equal(resolveSessionsDir(explicit), explicit);

    const attestor = new TranscriptAttestor({ sessionsDir: explicit });
    assert.equal(attestor.defaultSessionsDir, explicit);
  });
});

test("env discovery: CODEX_SESSIONS_DIR takes precedence over CODEX_HOME and standard user home", async () => {
  await withIsolatedEnv(async () => {
    process.env.CODEX_SESSIONS_DIR = "C:\\env\\codex_sessions_precedence";
    process.env.CODEX_HOME = "C:\\env\\custom_codex_home";

    assert.equal(resolveSessionsDir(), "C:\\env\\codex_sessions_precedence");

    const attestor = new TranscriptAttestor();
    assert.equal(attestor.defaultSessionsDir, "C:\\env\\codex_sessions_precedence");
  });
});

test("env discovery: custom CODEX_HOME on Windows resolves to <CODEX_HOME>/sessions", async () => {
  await withIsolatedEnv(async () => {
    delete process.env.CODEX_SESSIONS_DIR;
    const customHome = "C:\\CustomCodexHome";
    process.env.CODEX_HOME = customHome;

    const expected = path.join(customHome, "sessions");
    assert.equal(resolveSessionsDir(), expected);

    const attestor = new TranscriptAttestor();
    assert.equal(attestor.defaultSessionsDir, expected);
  });
});

test("env discovery: fallback to standard user home .codex/sessions when env vars are unset", async () => {
  await withIsolatedEnv(async () => {
    delete process.env.CODEX_SESSIONS_DIR;
    delete process.env.CODEX_HOME;

    const expected = path.join(os.homedir(), ".codex", "sessions");
    assert.equal(resolveSessionsDir(), expected);

    const attestor = new TranscriptAttestor();
    assert.equal(attestor.defaultSessionsDir, expected);
  });
});

test("env discovery: fallback to Windows USERPROFILE when standard homedir is unavailable", async () => {
  await withIsolatedEnv(async () => {
    delete process.env.CODEX_SESSIONS_DIR;
    delete process.env.CODEX_HOME;
    const fallbackProfile = "C:\\Users\\FallbackUserProfile";
    process.env.USERPROFILE = fallbackProfile;

    const expected = path.join(fallbackProfile, ".codex", "sessions");

    // Homedir returns empty string
    assert.equal(resolveSessionsDir(undefined, process.env, () => ""), expected);

    // Homedir throws error
    assert.equal(
      resolveSessionsDir(undefined, process.env, () => {
        throw new Error("homedir unavailable");
      }),
      expected,
    );
  });
});

test("env discovery: validates non-empty paths and rejects empty or whitespace-only candidates", async () => {
  assert.equal(isValidPath(""), false);
  assert.equal(isValidPath("   "), false);
  assert.equal(isValidPath(null), false);
  assert.equal(isValidPath(undefined), false);
  assert.equal(isValidPath("\0invalid"), false);
  assert.equal(isValidPath("valid/path"), true);

  // findRecentJsonlFiles and readFileTail guard against empty/whitespace paths safely
  assert.deepEqual(await findRecentJsonlFiles(""), []);
  assert.deepEqual(await findRecentJsonlFiles("   "), []);
  assert.equal(await readFileTail(""), "");
  assert.equal(await readFileTail("   "), "");

  await withIsolatedEnv(async () => {
    process.env.CODEX_SESSIONS_DIR = "   ";
    process.env.CODEX_HOME = "";

    // Whitespace options fall back to standard home instead of resolving to empty/cwd
    const resolved = resolveSessionsDir("   ");
    assert.equal(resolved, path.join(os.homedir(), ".codex", "sessions"));

    const attestor = new TranscriptAttestor({ sessionsDir: "   " });
    assert.equal(attestor.defaultSessionsDir, path.join(os.homedir(), ".codex", "sessions"));
  });
});

test("env discovery: attestJob succeeds discovering sessions via CODEX_SESSIONS_DIR", async () => {
  await withIsolatedEnv(async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-env-sess-"));
    try {
      process.env.CODEX_SESSIONS_DIR = tmpDir;
      delete process.env.CODEX_HOME;

      const targetJobId = "job_env_sessions_dir_001";
      const event = makeCanonicalEvent({ jobId: targetJobId });
      await writeFile(path.join(tmpDir, "session.jsonl"), JSON.stringify(event) + "\n");

      // No sessionsDir passed to constructor or attestJob
      const attestor = new TranscriptAttestor();
      const match = await attestor.attestJob(targetJobId);

      assert.ok(match, "Must attest job discovered via CODEX_SESSIONS_DIR");
      assert.equal(match.jobId, targetJobId);
      assert.equal(match.threadId, CANONICAL_THREAD_ID);
    } finally {
      await rm(tmpDir, { recursive: true, force: true });
    }
  });
});

test("env discovery: attestJob succeeds discovering sessions via CODEX_HOME on Windows", async () => {
  await withIsolatedEnv(async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "ds-attest-env-home-"));
    try {
      const sessionsDir = path.join(tmpDir, "sessions");
      await mkdir(sessionsDir, { recursive: true });

      delete process.env.CODEX_SESSIONS_DIR;
      process.env.CODEX_HOME = tmpDir;

      const targetJobId = "job_env_codex_home_001";
      const event = makeCanonicalEvent({ jobId: targetJobId });
      await writeFile(path.join(sessionsDir, "session.jsonl"), JSON.stringify(event) + "\n");

      // No sessionsDir passed to constructor or attestJob
      const attestor = new TranscriptAttestor();
      const match = await attestor.attestJob(targetJobId);

      assert.ok(match, "Must attest job discovered via CODEX_HOME/sessions");
      assert.equal(match.jobId, targetJobId);
      assert.equal(match.threadId, CANONICAL_THREAD_ID);
    } finally {
      await rm(tmpDir, { recursive: true, force: true });
    }
  });
});

test("env discovery: deterministic restoration isolates and completely restores process.env", async () => {
  const beforeSnapshot = { ...process.env };

  await withIsolatedEnv(async () => {
    process.env.CODEX_SESSIONS_DIR = "C:\\mutated\\sessions";
    process.env.CODEX_HOME = "C:\\mutated\\home";
    process.env.USERPROFILE = "C:\\mutated\\profile";
    process.env.__TEST_NEW_KEY__ = "test_value";
  });

  const afterSnapshot = { ...process.env };
  assert.deepEqual(afterSnapshot, beforeSnapshot, "process.env must be restored to its exact original state");
});

