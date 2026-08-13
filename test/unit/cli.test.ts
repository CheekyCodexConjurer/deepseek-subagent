import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { collectObligationDiagnostics, doctorDatabaseCheck, doctorObligationChecks, readCodexMcpToolTimeout, runCapture, terminateProcessTree, withStore } from "../../src/cli.js";
import { BridgeStore } from "../../src/store.js";
import type { DoctorCheck } from "../../src/types.js";

test("doctor timeout parser accepts hyphen and underscore Codex MCP section names", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-cli-timeout-"));
  try {
    for (const section of ["deepseek-subagent", "deepseek_subagent", '"deepseek-subagent"', '"deepseek_subagent"']) {
      const configPath = path.join(directory, section.replace(/[^a-z0-9]/gi, "_") + ".toml");
      await writeFile(configPath, `[mcp_servers.${section}]\ntool_timeout_sec = 4500\n[mcp_servers.other]\ntool_timeout_sec = 30\n`, "utf8");
      assert.equal(await readCodexMcpToolTimeout(configPath), 4500);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("runCapture captures a quick probe", async () => {
  const result = await runCapture(process.execPath, ["-e", "console.log('doctor-ok')"]);
  assert.equal(result.ok, true);
  assert.equal(result.output.trim(), "doctor-ok");
});

test("runCapture bounds a hung probe and reports a partial result", async () => {
  const start = Date.now();
  const result = await runCapture(process.execPath, ["-e", "setInterval(() => {}, 1000)"], 300);
  assert.equal(result.ok, false);
  assert.match(result.error, /timed out/);
  assert.ok(Date.now() - start < 3_000, "hung probe must return well below the default 10s cap");
});

test("terminateProcessTree absorbs best-effort cleanup failures", () => {
  const calls: string[] = [];
  const child = {
    pid: 0,
    kill: () => { calls.push("kill"); return false; },
  } as unknown as import("node:child_process").ChildProcess;
  assert.doesNotThrow(() => terminateProcessTree(child));
  if (process.platform === "win32") {
    // On Windows a real taskkill.exe is spawned against the inert PID 0; its
    // "not found" exit and any spawn failure are absorbed by the error listener.
  } else {
    assert.deepEqual(calls, ["kill"]);
  }
});

test("withStore closes the database store through failures", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-store-close-"));
  try {
    const dbPath = path.join(directory, "bridge.sqlite");
    let captured: BridgeStore | undefined;
    assert.throws(() => withStore(dbPath, (store) => {
      captured = store;
      throw new Error("boom");
    }), /boom/);
    assert.ok(captured, "callback must observe the opened store");
    assert.throws(() => captured!.db.prepare("SELECT 1").get(), /not open/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("doctorDatabaseCheck reports a healthy database with correlation counts", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-db-check-"));
  try {
    const dbPath = path.join(directory, "bridge.sqlite");
    new BridgeStore(dbPath).close();
    const seen: DoctorCheck[] = [];
    await doctorDatabaseCheck(dbPath, (check) => seen.push(check));
    assert.equal(seen.length, 2);
    assert.equal(seen[0].name, "database");
    assert.equal(seen[0].status, "ok");
    assert.equal(seen[1].name, "correlation");
    assert.equal(seen[1].status, "ok");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("doctorDatabaseCheck reports an error check for an unreadable database file", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-db-check-"));
  try {
    const dbPath = path.join(directory, "bridge.sqlite");
    await writeFile(dbPath, "this is not a sqlite database file", "utf8");
    const seen: DoctorCheck[] = [];
    await doctorDatabaseCheck(dbPath, (check) => seen.push(check));
    assert.equal(seen.length, 1);
    assert.equal(seen[0].name, "database");
    assert.equal(seen[0].status, "error");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("doctorDatabaseCheck warns when the database does not exist yet", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-db-check-"));
  try {
    const dbPath = path.join(directory, "missing.sqlite");
    const seen: DoctorCheck[] = [];
    await doctorDatabaseCheck(dbPath, (check) => seen.push(check));
    assert.equal(seen.length, 1);
    assert.equal(seen[0].name, "database");
    assert.equal(seen[0].status, "warning");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

function seedAgent(store: BridgeStore, id: string, sessionId: string): void {
  store.createAgent({
    id,
    title: id,
    topic: id,
    repositoryRoot: "C:\\deepseek-cli-obligations",
    workspacePath: "C:\\deepseek-cli-obligations",
    workspaceStrategy: "shared",
    opencodeServerId: "server_" + id,
    opencodeSessionId: sessionId,
    modelProviderId: "opencode-go",
    modelId: "deepseek-v4-flash",
    modelVariant: "max",
    modelRoute: "flash-max",
  });
}

test("obligation diagnostics flag unconsumed results, open terminal agents and open obligations", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-cli-obligations-"));
  try {
    const dbPath = path.join(directory, "bridge.sqlite");
    const store = new BridgeStore(dbPath);
    seedAgent(store, "agent_unconsumed", "session_unconsumed");
    const unconsumed = store.createJob({ id: "job_unconsumed", agentId: "agent_unconsumed", kind: "spawn", requestId: "request_unconsumed", promptHash: "h" });
    store.updateJobStatus(unconsumed.id, "dispatching");
    store.updateJobStatus(unconsumed.id, "running");
    store.updateJobStatus(unconsumed.id, "completed");
    store.updateJobStatus(unconsumed.id, "delivery_pending");
    store.updateJobStatus(unconsumed.id, "delivered");
    store.setJobResult(unconsumed.id, "C:\\deepseek-cli-obligations\\results\\job_unconsumed.json", "summary");

    seedAgent(store, "agent_open", "session_open");
    const openJob = store.createJob({ id: "job_open", agentId: "agent_open", kind: "spawn", requestId: "request_open", promptHash: "h" });
    store.updateJobStatus(openJob.id, "dispatching");
    store.updateAgentStatus("agent_open", "working");
    store.updateAgentStatus("agent_open", "completed");

    seedAgent(store, "agent_failed_open", "session_failed_open");
    const failedJob = store.createJob({ id: "job_failed_open", agentId: "agent_failed_open", kind: "spawn", requestId: "request_failed_open", promptHash: "h" });
    store.updateJobStatus(failedJob.id, "dispatching");
    store.updateJobStatus(failedJob.id, "failed");
    store.updateAgentStatus("agent_failed_open", "working");
    store.updateAgentStatus("agent_failed_open", "failed");

    const diagnostics = collectObligationDiagnostics(dbPath);
    assert.ok(diagnostics);
    assert.deepEqual(diagnostics.unconsumedTerminalResults.map((item) => item.jobId), ["job_unconsumed"]);
    assert.deepEqual(
      diagnostics.openTerminalAgents.map((item) => item.agentId).sort(),
      ["agent_failed_open", "agent_open"],
      "open terminal agents must include failed agents",
    );
    assert.deepEqual(diagnostics.openObligations.map((item) => item.jobId), ["job_open"]);

    const seen: DoctorCheck[] = [];
    await doctorObligationChecks(dbPath, (check) => seen.push(check));
    const byName = Object.fromEntries(seen.map((check) => [check.name, check.status]));
    assert.equal(byName.obligation_consumption, "warning");
    assert.equal(byName.open_terminal_agents, "warning");
    assert.equal(byName.open_obligations, "warning");
    assert.equal(byName.stale_follow_windows, "ok");
    store.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("stale follow windows flag only expired non-auto-armed windows, never fresh ones", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-cli-stale-"));
  try {
    const dbPath = path.join(directory, "bridge.sqlite");
    const store = new BridgeStore(dbPath);
    const past = new Date(Date.now() - 60 * 60_000).toISOString();
    const future = new Date(Date.now() + 60 * 60_000).toISOString();

    seedAgent(store, "agent_stale", "session_stale");
    const stale = store.createJob({ id: "job_stale", agentId: "agent_stale", kind: "spawn", requestId: "request_stale", promptHash: "h" });
    store.updateJobStatus(stale.id, "dispatching");
    store.updateJobStatus(stale.id, "following");
    store.setFollowWindow(stale.id, { startedAt: past, deadlineAt: past, graceMinutes: 5, graceDeadlineAt: past });

    seedAgent(store, "agent_fresh", "session_fresh");
    const fresh = store.createJob({ id: "job_fresh", agentId: "agent_fresh", kind: "spawn", requestId: "request_fresh", promptHash: "h" });
    store.updateJobStatus(fresh.id, "dispatching");
    store.updateJobStatus(fresh.id, "following");
    store.setFollowWindow(fresh.id, { startedAt: past, deadlineAt: past, graceMinutes: 5, graceDeadlineAt: future });

    seedAgent(store, "agent_auto", "session_auto");
    const auto = store.createJob({ id: "job_auto", agentId: "agent_auto", kind: "spawn", requestId: "request_auto", promptHash: "h" });
    store.updateJobStatus(auto.id, "dispatching");
    store.updateJobStatus(auto.id, "following");
    store.setFollowWindow(auto.id, { startedAt: past, deadlineAt: past, graceMinutes: 5, graceDeadlineAt: past });
    store.markDispatchUnknown(auto.id);

    const diagnostics = collectObligationDiagnostics(dbPath);
    assert.ok(diagnostics);
    assert.deepEqual(diagnostics.staleFollowWindows.map((item) => item.jobId), ["job_stale"], "only the genuinely expired non-auto-armed window is flagged");

    const seen: DoctorCheck[] = [];
    await doctorObligationChecks(dbPath, (check) => seen.push(check));
    const staleCheck = seen.find((check) => check.name === "stale_follow_windows");
    assert.equal(staleCheck?.status, "warning");
    store.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
