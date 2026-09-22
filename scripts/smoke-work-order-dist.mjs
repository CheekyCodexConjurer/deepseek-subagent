#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createDefaultConfig } from "../dist/config.js";
import { BridgeHttpClient, BridgeHttpServer } from "../dist/http-server.js";
import { createMcpServer } from "../dist/mcp.js";
import { computeWorkOrderArtifactHash } from "../dist/result.js";
import { BridgeService } from "../dist/service.js";
import { BridgeStore } from "../dist/store.js";
import { AntigravityAdapter } from "../dist/antigravity/adapter.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const distRoot = path.join(repoRoot, "dist");
const fixturePath = path.join(repoRoot, "test", "fixtures", "agy.cjs");

async function chooseLoopbackPort() {
  const probe = net.createServer();
  probe.listen(0, "127.0.0.1");
  await new Promise((resolve, reject) => {
    probe.once("listening", resolve);
    probe.once("error", reject);
  });
  const address = probe.address();
  assert.ok(address && typeof address === "object");
  const port = address.port;
  await new Promise((resolve, reject) => probe.close((error) => error ? reject(error) : resolve()));
  return port;
}

async function hashDistManifest(directory) {
  const files = [];
  const visit = async (current) => {
    for (const entry of await readdir(current, { withFileTypes: true })) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) await visit(fullPath);
      else if (entry.isFile()) {
        const relative = path.relative(directory, fullPath).replaceAll(path.sep, "/");
        const bytes = await readFile(fullPath);
        files.push({ path: relative, sha256: createHash("sha256").update(bytes).digest("hex") });
      }
    }
  };
  await visit(directory);
  files.sort((left, right) => left.path.localeCompare(right.path));
  const manifest = files.map((file) => `${file.path}\0${file.sha256}`).join("\n");
  return {
    fileCount: files.length,
    sha256: createHash("sha256").update(manifest).digest("hex"),
    selected: Object.fromEntries(files
      .filter(({ path: name }) => ["service.js", "result.js", "prompts.js", "mcp.js", "work-order.js", "http-server.js"].includes(path.basename(name)))
      .map(({ path: name, sha256 }) => [name, sha256])),
  };
}

function fixtureSpawn(calls, prompts) {
  const cleanEnv = {};
  for (const key of ["PATH", "SystemRoot", "WINDIR", "TEMP", "TMP"]) {
    if (process.env[key]) cleanEnv[key] = process.env[key];
  }
  return (_command, args, options) => {
    calls.push([...args]);
    const promptIndex = args.indexOf("-p");
    if (promptIndex >= 0) prompts.push(args[promptIndex + 1] ?? "");
    return spawn(process.execPath, [fixturePath, ...args], {
      cwd: options.cwd,
      env: { ...cleanEnv, AGY_FIXTURE: "work-order" },
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
  };
}

function structured(result, label) {
  assert.equal(result.isError, undefined, `${label} MCP tool returned an error`);
  assert.ok(result.structuredContent, `${label} MCP tool returned no structured result`);
  return result.structuredContent;
}

async function assertPersistedArtifact(store, jobId, expectedContractVersion, compactEvaluation) {
  const job = store.getJob(jobId);
  assert.ok(job?.resultPath, `job ${jobId} has no persisted result path`);
  const document = JSON.parse(await readFile(job.resultPath, "utf8"));
  const evaluation = document.envelope?.workOrderEvaluation;
  assert.equal(evaluation?.resultHashVersion, 1);
  assert.equal(evaluation?.contractVersion, expectedContractVersion);
  assert.equal(evaluation?.resultHash, computeWorkOrderArtifactHash(document), "persisted artifact hash must revalidate");
  assert.equal(compactEvaluation?.resultHash, evaluation.resultHash, "compact follow must carry persisted artifact hash");
  assert.equal(compactEvaluation?.criteria?.[0]?.outcome, "satisfied");
  assert.equal(evaluation.diffAvailability, "summary_only");
  assert.equal(evaluation.gitDiffAvailable, false);
  return evaluation;
}

const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-work-order-dist-smoke-"));
const port = await chooseLoopbackPort();
const calls = [];
const prompts = [];
const config = createDefaultConfig({
  dataDir: directory,
  configPath: path.join(directory, "config.json"),
  daemonHost: "127.0.0.1",
  daemonPort: port,
  daemonToken: "isolated-smoke-only",
  followDefaultWaitMinutes: 1,
  followDefaultGraceMinutes: 1,
  globalGeminiContextPath: path.join(directory, "no-global-instructions.md"),
  inactivityThresholdSeconds: 60,
});
const store = await BridgeStore.open(directory);
const service = new BridgeService(config, {
  store,
  antigravity: new AntigravityAdapter({
    command: "local-smoke-fixture",
    timeoutMs: 30_000,
    dataDir: directory,
    spawnFn: fixtureSpawn(calls, prompts),
  }),
});
const httpServer = new BridgeHttpServer(config, service);
const httpClient = new BridgeHttpClient(config);
const mcpServer = createMcpServer(httpClient, { env: {}, compactFollowMaxBytes: config.compactFollowMaxBytes });
const mcpClient = new Client({ name: "work-order-dist-smoke", version: "1.0.0" }, { capabilities: {} });
const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
let serviceStarted = false;
let httpStarted = false;
let mcpServerConnected = false;
let mcpClientConnected = false;

try {
  const dist = await hashDistManifest(distRoot);
  await service.start();
  serviceStarted = true;
  assert.equal(service.isReady(), true, "BridgeService must report ready before MCP use");
  const readyAt = Date.now();
  await httpServer.start();
  httpStarted = true;
  const health = await httpClient.health();
  assert.equal(health.ready, true, "loopback HTTP readiness must be healthy");
  const readinessMs = Date.now() - readyAt;

  await mcpServer.connect(serverTransport);
  mcpServerConnected = true;
  await mcpClient.connect(clientTransport);
  mcpClientConnected = true;
  const listed = await mcpClient.listTools();
  const tools = new Map(listed.tools.map((tool) => [tool.name, tool]));
  for (const name of ["subagents_spawn", "subagents_continue", "subagents_follow", "subagents_recover_result"]) {
    assert.ok(tools.has(name), `built MCP schema is missing ${name}`);
  }
  const spawnSchema = tools.get("subagents_spawn")?.inputSchema;
  assert.ok(spawnSchema?.properties?.work_order, "spawn schema must expose optional work_order");
  assert.equal(spawnSchema.properties.work_order.properties.schema_version.const, 1);

  const workOrder = {
    schema_version: 1,
    contract_version: 1,
    objective: "Verify the built bridge work-order lifecycle end to end.",
    scope: ["local fixture only"],
    ownership: ["isolated temporary workspace"],
    context_refs: ["smoke fixture"],
    design_decisions: ["use exact built dist modules"],
    invariants: ["no external provider calls"],
    acceptance_criteria: [{ id: "AC-01", description: "Persist and return evidence-bound work-order outcome." }],
    validation_commands: ["node scripts/smoke-work-order-dist.mjs"],
    escalation_conditions: ["missing persisted artifact or result hash mismatch"],
  };

  const spawnStarted = Date.now();
  const spawned = structured(await mcpClient.callTool({
    name: "subagents_spawn",
    arguments: {
      request_id: "dist-smoke-spawn-v1",
      topic: "Built work-order smoke",
      task: "Run the local fixture and emit the required AC-01 outcome with evidence.",
      cwd: directory,
      mode: "analyze",
      work_order: workOrder,
    },
  }), "spawn");
  assert.equal(spawned.accepted, true);
  const spawnedAgentId = spawned.agentId;
  const spawnedJobId = spawned.jobId;
  assert.ok(typeof spawnedAgentId === "string" && typeof spawnedJobId === "string");

  const followed = structured(await mcpClient.callTool({
    name: "subagents_follow",
    arguments: { agent_id: spawnedAgentId, job_id: spawnedJobId, wait_minutes: 1, grace_minutes: 1 },
  }), "spawn follow");
  assert.equal(followed.resultAvailable, true);
  assert.equal(followed.status, "completed");
  const spawnEvaluation = followed.compact?.workOrderEvaluation;
  assert.equal(spawnEvaluation?.criteria?.[0]?.evidenceRefs?.[0], "ev_work_order_test");
  assert.equal(spawnEvaluation?.criteria?.[0]?.evidenceRefsResolved, true);
  const persistedSpawnEvaluation = await assertPersistedArtifact(store, spawnedJobId, 1, spawnEvaluation);
  const spawnFollowMs = Date.now() - spawnStarted;

  const recoveredSpawn = structured(await mcpClient.callTool({
    name: "subagents_recover_result",
    arguments: { agent_id: spawnedAgentId, job_id: spawnedJobId, section: "work_order" },
  }), "spawn recovery");
  assert.equal(recoveredSpawn.result?.section, "work_order");
  assert.match(recoveredSpawn.result?.text ?? "", /AC-01/);
  assert.match(recoveredSpawn.result?.text ?? "", new RegExp(persistedSpawnEvaluation.resultHash));

  const continueStarted = Date.now();
  const continued = structured(await mcpClient.callTool({
    name: "subagents_continue",
    arguments: {
      request_id: "dist-smoke-continue-v1",
      agent_id: spawnedAgentId,
      relation: "continuation",
      task: "Continue from confirmed contract version one and verify its delta.",
      confirmed_contract_version: 1,
    },
  }), "continue");
  assert.equal(continued.accepted, true);
  assert.equal(continued.agentId, spawnedAgentId);
  assert.ok(typeof continued.jobId === "string");

  const continuedFollow = structured(await mcpClient.callTool({
    name: "subagents_follow",
    arguments: { agent_id: spawnedAgentId, job_id: continued.jobId, wait_minutes: 1, grace_minutes: 1 },
  }), "continued follow");
  assert.equal(continuedFollow.resultAvailable, true);
  assert.equal(continuedFollow.status, "completed");
  assert.match(prompts[1] ?? "", /confirmed baseline contract version 1/i, "continued prompt must reference the confirmed contract version");
  assert.ok(!prompts[1]?.includes("WORK ORDER JSON"), "confirmed continuation must stay a prompt delta");
  assert.ok(calls[1]?.includes("--conversation"), "confirmed continuation should retain its verified provider conversation");
  const continueEvaluation = continuedFollow.compact?.workOrderEvaluation;
  assert.equal(continueEvaluation?.confirmedPreviousContractVersion, 1);
  assert.equal(continueEvaluation?.criteria?.[0]?.outcome, "satisfied");
  const persistedContinueEvaluation = await assertPersistedArtifact(store, continued.jobId, 1, continueEvaluation);
  const continueFollowMs = Date.now() - continueStarted;

  const recoveredContinue = structured(await mcpClient.callTool({
    name: "subagents_recover_result",
    arguments: { agent_id: spawnedAgentId, job_id: continued.jobId, section: "work_order" },
  }), "continued recovery");
  assert.equal(recoveredContinue.result?.section, "work_order");
  assert.match(recoveredContinue.result?.text ?? "", /confirmedPreviousContractVersion/);
  assert.match(recoveredContinue.result?.text ?? "", new RegExp(persistedContinueEvaluation.resultHash));
  assert.equal(calls.length, 2, "only the two local fixture processes may run");

  console.log(JSON.stringify({
    status: "PASS",
    target: "fresh dist/ JavaScript imports",
    distManifest: dist,
    flow: ["MCP listTools/schema", "subagents_spawn", "subagents_follow", "subagents_recover_result", "subagents_continue(confirmed_contract_version=1)", "subagents_follow", "subagents_recover_result"],
    transport: "MCP SDK Client -> MCP server -> authenticated loopback HTTP -> BridgeService -> temporary SQLite",
    provider: "local test fixture; external provider calls: 0",
    readinessMs,
    spawnFollowMs,
    continueFollowMs,
    persistedArtifacts: 2,
    resultHashVersion: 1,
    resultHashes: [persistedSpawnEvaluation.resultHash, persistedContinueEvaluation.resultHash],
  }, null, 2));
} finally {
  if (mcpClientConnected) await mcpClient.close().catch(() => undefined);
  if (mcpServerConnected) await mcpServer.close().catch(() => undefined);
  if (httpStarted) await httpServer.stop().catch(() => undefined);
  if (serviceStarted) await service.stop().catch(() => undefined);
  await httpClient.close().catch(() => undefined);
  store.close();
  await rm(directory, { recursive: true, force: true });
}
