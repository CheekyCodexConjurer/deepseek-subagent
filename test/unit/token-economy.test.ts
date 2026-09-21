import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { buildWorkerPrompt } from "../../src/prompts.js";
import { BridgeStore } from "../../src/store.js";
import { createCompactClaims, createDetailsRef, persistAntigravityResult } from "../../src/result.js";
import { BridgeService } from "../../src/service.js";
import { createDefaultConfig } from "../../src/config.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createMcpServer } from "../../src/mcp.js";
import type { BridgeHttpClient } from "../../src/http-server.js";
import type { AntigravityRunResult } from "../../src/antigravity/types.js";
import type { ResultEnvelope } from "../../src/types.js";

test("token economy: buildWorkerPrompt with isContinuation emits concise delta prompt", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "prompt-continuation-"));
  try {
    const fullPrompt = await buildWorkerPrompt(
      { task: "Continue fixing the bug." },
      tmpDir,
      { isContinuation: false },
    );
    const deltaPrompt = await buildWorkerPrompt(
      { task: "Continue fixing the bug." },
      tmpDir,
      { isContinuation: true },
    );

    // Full prompt contains operational instructions and protocol sections
    assert.match(fullPrompt, /At completion, use these exact headings/i);
    assert.match(fullPrompt, /Operating rule/i);
    assert.ok(fullPrompt.length > 500, "Full prompt should be comprehensive");

    // Delta prompt is lean and does not repeat boilerplate
    assert.doesNotMatch(deltaPrompt, /At completion, use these exact headings/i);
    assert.doesNotMatch(deltaPrompt, /Operating rule/i);
    assert.match(deltaPrompt, /Continuation Task:/);
    assert.match(deltaPrompt, /Continue fixing the bug\./);
    assert.ok(deltaPrompt.length < fullPrompt.length, "Delta prompt must be substantially smaller");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("token economy: store correctly computes per-turn delta usage without double counting", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "store-tokens-"));
  let store: BridgeStore | null = null;
  try {
    store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const agent = store.createAgent({
      id: "agent_token_1",
      title: "Token Test",
      topic: "Token Test Agent",
      repositoryRoot: tmpDir,
      workspacePath: tmpDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "sess_token_1",
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
    });

    // Turn 1
    const job1 = store.createJob({
      id: "job_turn_1",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_1",
      promptHash: "hash_1",
    });

    const turn1Usage = {
      inputTokens: 1000,
      outputTokens: 200,
      thinkingTokens: 150,
      cachedInputTokens: 500,
      totalTokens: 1200,
    };
    store.updateJobWorkerUsage(job1.id, turn1Usage);

    // Turn 2: CLI returns cumulative totals across both turns (e.g. 2500 total)
    const cumulativeAfterTurn2 = {
      inputTokens: 2100,
      outputTokens: 450,
      thinkingTokens: 300,
      cachedInputTokens: 1000,
      totalTokens: 2550,
    };

    const prior = store.getAgentPriorCumulativeWorkerTokens(agent.id, "job_turn_2");
    assert.equal(prior.inputTokens, 1000);
    assert.equal(prior.outputTokens, 200);
    assert.equal(prior.thinkingTokens, 150);
    assert.equal(prior.cachedInputTokens, 500);
    assert.equal(prior.totalTokens, 1200);

    const deltaUsageTurn2 = {
      inputTokens: Math.max(0, cumulativeAfterTurn2.inputTokens - prior.inputTokens),
      outputTokens: Math.max(0, cumulativeAfterTurn2.outputTokens - prior.outputTokens),
      thinkingTokens: Math.max(0, cumulativeAfterTurn2.thinkingTokens - prior.thinkingTokens),
      cachedInputTokens: Math.max(0, cumulativeAfterTurn2.cachedInputTokens - prior.cachedInputTokens),
      totalTokens: Math.max(0, cumulativeAfterTurn2.totalTokens - prior.totalTokens),
    };

    assert.equal(deltaUsageTurn2.inputTokens, 1100);
    assert.equal(deltaUsageTurn2.outputTokens, 250);
    assert.equal(deltaUsageTurn2.thinkingTokens, 150);
    assert.equal(deltaUsageTurn2.cachedInputTokens, 500);
    assert.equal(deltaUsageTurn2.totalTokens, 1350);

    const job2 = store.createJob({
      id: "job_turn_2",
      agentId: agent.id,
      kind: "continue",
      requestId: "req_2",
      promptHash: "hash_2",
    });
    store.updateJobWorkerUsage(job2.id, deltaUsageTurn2);

    const retrievedJob2 = store.getJob(job2.id);
    assert.equal(retrievedJob2?.workerInputTokens, 1100);
    assert.equal(retrievedJob2?.workerOutputTokens, 250);
    assert.equal(retrievedJob2?.workerTotalTokens, 1350);

    // Cumulative sum across both turns
    const cumulativeTotal = store.getAgentPriorCumulativeWorkerTokens(agent.id);
    assert.equal(cumulativeTotal.totalTokens, 2550);
    assert.equal(cumulativeTotal.inputTokens, 2100);
  } finally {
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("token economy: createCompactClaims bounds size and detailsRef points to available sections", () => {
  const envelope: ResultEnvelope = {
    version: 1,
    agentId: "agent_claims_1",
    jobId: "job_claims_1",
    topic: "Claims Test",
    status: "completed",
    opencodeSessionId: "sess_claims_1",
    model: "gemini-3.8-flash-high",
    modelDisplayName: "Gemini",
    workspace: "/path/to/work",
    summary: "A".repeat(5000), // Long summary
    files: Array.from({ length: 50 }, (_, i) => `file_${i}.ts`),
    tests: Array.from({ length: 30 }, (_, i) => `test_${i}`),
    risks: Array.from({ length: 20 }, (_, i) => `risk_${i}`),
    diffSummary: "Diff text",
    fullResultPath: "/path/to/results/job_claims_1.json",
    orchestratorInstruction: "None",
    receipt: {
      jobId: "job_claims_1",
      agentId: "agent_claims_1",
      provider: "antigravity",
      model: "gemini-3.8-flash-high",
      status: "completed",
      workspace: "/path/to/work",
      startedAt: null,
      completedAt: new Date().toISOString(),
      durationMs: 120,
      attempt: "att_1",
      fence: 1,
      outputHash: "hash",
      quiescent: true,
      earlyExit: false,
      filesCount: 50,
      testsCount: 30,
    },
  };

  const claims = createCompactClaims(envelope);
  assert.equal(claims.summary.length, 1000, "Summary must be bounded to 1000 characters");
  assert.equal(claims.files.length, 10, "Files in claims should be capped at 10");
  assert.equal(claims.tests.length, 10, "Tests in claims should be capped at 10");
  assert.equal(claims.risks.length, 5, "Risks in claims should be capped at 5");

  const detailsRef = createDetailsRef(envelope);
  assert.equal(detailsRef.hasMoreDetails, true);
  assert.equal(detailsRef.resultPath, "/path/to/results/job_claims_1.json");
  assert.deepEqual(detailsRef.availableSections, ["summary", "files", "tests", "risks", "diff", "evidence", "full"]);
});

test("token economy: recoverResult supports section filtering and pagination", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "service-recover-"));
  let store: BridgeStore | null = null;
  try {
    const config = createDefaultConfig({
      dataDir: tmpDir,
      configPath: path.join(tmpDir, "config.json"),
    });
    store = new BridgeStore(path.join(tmpDir, "test.sqlite"));
    const service = new BridgeService(config, { store });

    const agent = store.createAgent({
      id: "agent_rec_1",
      title: "Recover Test",
      topic: "Recover Test",
      repositoryRoot: tmpDir,
      workspacePath: tmpDir,
      workspaceStrategy: "shared",
      opencodeServerId: "antigravity",
      opencodeSessionId: "sess_rec_1",
      modelProviderId: "antigravity",
      modelId: "gemini-3.8-flash-high",
      modelVariant: null,
    });

    const job = store.createJob({
      id: "job_rec_1",
      agentId: agent.id,
      kind: "spawn",
      requestId: "req_rec_1",
      promptHash: "hash_rec_1",
    });

    const runResult: AntigravityRunResult = {
      summary: "Detailed summary of execution",
      files: ["a.ts", "b.ts", "c.ts", "d.ts", "e.ts"],
      tests: ["test 1", "test 2", "test 3"],
      risks: ["risk alpha", "risk beta"],
      diffSummary: "Modified 5 lines",
      model: "gemini-3.8-flash-high",
      status: "completed",
      workspace: tmpDir,
      rawOutput: "raw output text",
      fullText: "full text",
      runId: "run_1",
      evidence: {
        summary: "Evidence summary",
        items: [
          { type: "code", description: "snippet 1" },
          { type: "log", description: "log 1" },
          { type: "test", description: "test log 1" },
        ],
      },
    };

    const persisted = await persistAntigravityResult(tmpDir, agent, job, runResult, 100_000);
    store.setJobResult(job.id, persisted.resultPath, persisted.envelope.summary);

    // 1. Recover full result (default)
    const fullRes = await service.recoverResult({ jobId: job.id, agentId: agent.id }) as any;
    assert.ok(fullRes.envelope, "Full recover should contain envelope");
    assert.equal(fullRes.envelope.files.length, 5);

    // 2. Recover section: "files" with pagination
    const filesPage1 = await service.recoverResult({
      jobId: job.id,
      agentId: agent.id,
      section: "files",
      offset: 0,
      limit: 2,
    }) as any;
    assert.equal(filesPage1.section, "files");
    assert.deepEqual(filesPage1.items, ["a.ts", "b.ts"]);
    assert.equal(filesPage1.offset, 0);
    assert.equal(filesPage1.limit, 2);
    assert.equal(filesPage1.totalCount, 5);
    assert.equal(filesPage1.hasMore, true);

    const filesPage2 = await service.recoverResult({
      jobId: job.id,
      agentId: agent.id,
      section: "files",
      offset: 2,
      limit: 3,
    }) as any;
    assert.deepEqual(filesPage2.items, ["c.ts", "d.ts", "e.ts"]);
    assert.equal(filesPage2.hasMore, false);

    // 3. Recover section: "tests"
    const testsSec = await service.recoverResult({
      jobId: job.id,
      agentId: agent.id,
      section: "tests",
    }) as any;
    assert.equal(testsSec.section, "tests");
    assert.deepEqual(testsSec.items, ["test 1", "test 2", "test 3"]);
    assert.equal(testsSec.totalCount, 3);
    assert.equal(testsSec.hasMore, false);

    // 4. Recover section: "diff"
    const diffSec = await service.recoverResult({
      jobId: job.id,
      agentId: agent.id,
      section: "diff",
    }) as any;
    assert.equal(diffSec.section, "diff");
    assert.equal(diffSec.diffSummary, "Modified 5 lines");

    // 5. Recover section: "evidence" with pagination
    const evidenceSec = await service.recoverResult({
      jobId: job.id,
      agentId: agent.id,
      section: "evidence",
      offset: 0,
      limit: 2,
    }) as any;
    assert.equal(evidenceSec.section, "evidence");
    assert.equal(evidenceSec.summary, "Evidence summary");
    assert.equal(evidenceSec.items.length, 2);
    assert.equal(evidenceSec.totalCount, 3);
    assert.equal(evidenceSec.hasMore, true);
  } finally {
    store.close();
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("token economy: MCP subagents_follow projects claims, detailsRef, and tokens without envelope bloat", async () => {
  const mockFollowResponse = {
    agentId: "agent_mcp_1",
    jobId: "job_mcp_1",
    status: "completed",
    resultAvailable: true,
    claims: {
      summary: "Short summary",
      files: ["f1.ts"],
      tests: ["t1"],
      risks: [],
    },
    tokens: {
      inputTokens: 1500,
      outputTokens: 120,
      thinkingTokens: 50,
      cachedInputTokens: 800,
      totalTokens: 1620,
    },
    detailsRef: {
      resultPath: "/path/to/results/job_mcp_1.json",
      hasMoreDetails: true,
      availableSections: ["summary", "files", "tests", "risks", "diff", "evidence", "full"],
    },
    result: {
      envelope: {
        veryLargePayload: "X".repeat(50_000),
      },
    },
    progress: {
      recentActivity: [{ summary: "did step 1" }, { summary: "did step 2" }],
    },
  };

  const bridgeClient = {
    call: async (url: string) => {
      if (url === "/v1/jobs/follow") {
        return mockFollowResponse;
      }
      throw new Error("Unexpected endpoint: " + url);
    },
  } as unknown as BridgeHttpClient;

  const server = createMcpServer(bridgeClient);
  const client = new Client({ name: "test-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);

  try {
    const res = await client.callTool({ name: "subagents_follow", arguments: { agent_id: "agent_mcp_1" } });
    assert.equal(res.isError, undefined);
    const text = (res.content as Array<{ type: string; text?: string }>).find((item) => item.type === "text")?.text ?? "";
    assert.match(text, /tokens: 1620/);

    const structured = res.structuredContent as Record<string, unknown>;
    assert.equal(structured.obligationState, "closed");
    assert.deepEqual(structured.claims, mockFollowResponse.claims);
    assert.deepEqual(structured.tokens, mockFollowResponse.tokens);
    assert.deepEqual(structured.detailsRef, mockFollowResponse.detailsRef);

    // CRITICAL: Ensure bloated progress and raw result envelope are stripped from follow payload
    assert.equal(structured.result, undefined, "Bulky result envelope must be stripped when claims are present");
    assert.equal(structured.progress, undefined, "Bulky progress history must be stripped when claims are present");
  } finally {
    await client.close();
    await server.close();
  }
});

