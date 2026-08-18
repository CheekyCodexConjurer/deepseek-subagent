import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { acquireDaemonLock, collectObligationDiagnostics, doctorDatabaseCheck, doctorObligationChecks, isProcessAlive, readCodexMcpToolTimeout, runCapture, runRouteCommand, terminateProcessTree, withStore } from "../../src/cli.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeHttpError, BridgeTransportError } from "../../src/http-server.js";
import { BridgeStore } from "../../src/store.js";
import type { DoctorCheck } from "../../src/types.js";

const routeStatusFixture = {
  activeRoute: { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", display: "DeepSeek V4 Flash · Max" },
  activeRouteError: null,
  defaultModelRoute: "flash-max",
  source: "configured-default",
  routes: [
    { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
    { name: "antigravity-flash-high", providerId: "antigravity", modelId: "gemini-3.7-flash-high", variant: null, enabled: true, default: false, display: "Antigravity · Gemini 3.7 Flash High" },
  ],
};

test("route list and route status map to the daemon status endpoint", async () => {
  const calls: string[] = [];
  const client = {
    get: async (pathname: string) => {
      calls.push("GET " + pathname);
      return routeStatusFixture;
    },
    call: async () => {
      throw new Error("must not be reached");
    },
  } as unknown as import("../../src/http-server.js").BridgeHttpClient;
  const config = createDefaultConfig({
    dataDir: "C:\\deepseek-cli-route-list",
    configPath: "C:\\deepseek-cli-route-list\\config.json",
  });
  await runRouteCommand(config, "list", undefined, true, client);
  await runRouteCommand(config, "status", undefined, true, client);
  assert.deepEqual(calls, ["GET /v1/routes/status", "GET /v1/routes/status"]);
});

test("route set maps to the daemon switch endpoint with the route name", async () => {
  const calls: Array<{ pathname: string; body: unknown }> = [];
  const client = {
    get: async () => {
      throw new Error("must not be reached");
    },
    call: async (pathname: string, body: unknown) => {
      calls.push({ pathname, body });
      return { ...routeStatusFixture, activeRoute: routeStatusFixture.routes[1], source: "operator-set" };
    },
  } as unknown as import("../../src/http-server.js").BridgeHttpClient;
  const config = createDefaultConfig({
    dataDir: "C:\\deepseek-cli-route-set",
    configPath: "C:\\deepseek-cli-route-set\\config.json",
  });
  await runRouteCommand(config, "set", "antigravity-flash-high", true, client);
  assert.deepEqual(calls, [{ pathname: "/v1/routes/active", body: { route: "antigravity-flash-high" } }]);
});

test("route requires a known subcommand and route set requires a route name", async () => {
  const config = createDefaultConfig({
    dataDir: "C:\\deepseek-cli-route-invalid",
    configPath: "C:\\deepseek-cli-route-invalid\\config.json",
  });
  await assert.rejects(() => runRouteCommand(config, "toggle", undefined, true), /route requires one of: list, status, set/);
  await assert.rejects(() => runRouteCommand(config, "set", undefined, true), /route set requires a route name/);
});

test("route commands fail closed when the daemon is not running and never write the store", async () => {
  const client = {
    get: async () => {
      throw new BridgeTransportError("Bridge HTTP request failed: connect ECONNREFUSED 127.0.0.1:42653 (cause: ECONNREFUSED)");
    },
    call: async () => {
      throw new BridgeTransportError("Bridge HTTP request failed: connect ECONNREFUSED 127.0.0.1:42653 (cause: ECONNREFUSED)");
    },
  } as unknown as import("../../src/http-server.js").BridgeHttpClient;
  const config = createDefaultConfig({
    dataDir: "C:\\deepseek-cli-route-offline",
    configPath: "C:\\deepseek-cli-route-offline\\config.json",
  });
  await assert.rejects(
    () => runRouteCommand(config, "set", "pro-max", true, client),
    (error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      assert.match(message, /requires the running daemon/);
      assert.match(message, /No route state was written while the daemon was stopped/);
      return true;
    },
  );
});

test("route commands surface malformed live responses accurately, never as offline guidance", async () => {
  const malformed = {
    get: async () => "not a route status object",
    call: async () => "not a route status object",
  } as unknown as import("../../src/http-server.js").BridgeHttpClient;
  const config = createDefaultConfig({
    dataDir: "C:\\deepseek-cli-route-malformed",
    configPath: "C:\\deepseek-cli-route-malformed\\config.json",
  });
  for (const subcommand of ["list", "status"] as const) {
    await assert.rejects(
      () => runRouteCommand(config, subcommand, undefined, true, malformed),
      (error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        assert.doesNotMatch(message, /requires the running daemon/, "a live but malformed response must not look like a stopped daemon");
        assert.doesNotMatch(message, /No route state was written/, "malformed responses must never produce the offline guidance");
        return true;
      },
    );
  }
});

test("route commands pass typed daemon errors through untouched", async () => {
  const client = {
    get: async () => {
      throw new Error("must not be reached");
    },
    call: async () => {
      throw new BridgeHttpError(400, "unknown_route", "Unknown model route: nonsense", { route: "nonsense" });
    },
  } as unknown as import("../../src/http-server.js").BridgeHttpClient;
  const config = createDefaultConfig({
    dataDir: "C:\\deepseek-cli-route-typed",
    configPath: "C:\\deepseek-cli-route-typed\\config.json",
  });
  await assert.rejects(() => runRouteCommand(config, "set", "nonsense", true, client), (error: unknown) => {
    assert.ok(error instanceof BridgeHttpError);
    assert.equal(error.code, "unknown_route");
    return true;
  });
});

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
    assert.equal(seen[0].detail, "ok");
    assert.equal(seen[1].name, "correlation");
    assert.equal(seen[1].status, "ok");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("doctorDatabaseCheck with full: true performs PRAGMA quick_check", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-db-check-full-"));
  try {
    const dbPath = path.join(directory, "bridge.sqlite");
    new BridgeStore(dbPath).close();
    const seen: DoctorCheck[] = [];
    await doctorDatabaseCheck(dbPath, (check) => seen.push(check), { full: true });
    assert.equal(seen.length, 2);
    assert.equal(seen[0].name, "database");
    assert.equal(seen[0].status, "ok");
    assert.equal(seen[0].detail, "ok");
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

test("isProcessAlive accurately detects running process and non-existent PID", () => {
  assert.equal(isProcessAlive(process.pid), true);
  // Extremely large PID that doesn't exist
  assert.equal(isProcessAlive(9999999), false);
});

test("acquireDaemonLock exclusive lock prevents second acquisition and cleans up on release", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-cli-lock-"));
  try {
    const releaseFirst = await acquireDaemonLock(directory, process.pid);
    await assert.rejects(
      () => acquireDaemonLock(directory, process.pid),
      (error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        assert.match(message, /already running/);
        assert.match(message, /Duplicate daemon instance prevented/);
        return true;
      },
    );
    await releaseFirst();

    // After release, acquisition succeeds again
    const releaseSecond = await acquireDaemonLock(directory, process.pid);
    await releaseSecond();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("acquireDaemonLock safely recovers from stale dead PID", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-cli-stale-lock-"));
  try {
    const pidPath = path.join(directory, "daemon.pid");
    await writeFile(pidPath, "9999999\n", "utf8");

    const release = await acquireDaemonLock(directory, process.pid);
    assert.ok(typeof release === "function");
    await release();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("acquireDaemonLock concurrent contenders result in exactly one winner", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-cli-race-lock-"));
  try {
    const contenders = [1, 2, 3, 4, 5];
    const results = await Promise.allSettled(
      contenders.map(() => acquireDaemonLock(directory, process.pid)),
    );
    const fulfilled = results.filter((r): r is PromiseFulfilledResult<() => Promise<void>> => r.status === "fulfilled");
    const rejected = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");

    assert.equal(fulfilled.length, 1, "exactly one contender acquires the lock");
    assert.equal(rejected.length, 4, "all other contenders are rejected");
    for (const rej of rejected) {
      assert.match(rej.reason?.message ?? "", /Duplicate daemon instance prevented|already running/);
    }
    await fulfilled[0].value();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("acquireDaemonLock concurrent contenders recovering from stale dead PID result in exactly one winner", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-cli-stale-race-lock-"));
  try {
    const pidPath = path.join(directory, "daemon.pid");
    await writeFile(pidPath, "9999999\n", "utf8");

    const contenders = [1, 2, 3, 4, 5, 6];
    const results = await Promise.allSettled(
      contenders.map(() => acquireDaemonLock(directory, process.pid)),
    );
    const fulfilled = results.filter((r): r is PromiseFulfilledResult<() => Promise<void>> => r.status === "fulfilled");
    const rejected = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");

    assert.equal(fulfilled.length, 1, "exactly one contender acquires the lock after stale recovery");
    assert.equal(rejected.length, 5, "all other contenders are rejected");
    for (const rej of rejected) {
      assert.match(rej.reason?.message ?? "", /Duplicate daemon instance prevented|already running|lock file is held by another process/);
    }

    // Verify winner has valid PID written
    const content = await readFile(pidPath, "utf8");
    assert.equal(Number.parseInt(content.trim(), 10), process.pid);

    // Release winner's lock
    await fulfilled[0].value();

    // After release, acquisition succeeds again cleanly
    const releaseSubsequent = await acquireDaemonLock(directory, process.pid);
    await releaseSubsequent();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("acquireDaemonLock fails closed when lock file content is empty or unreadable during creation", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-cli-empty-lock-"));
  try {
    const pidPath = path.join(directory, "daemon.pid");
    await writeFile(pidPath, "", "utf8");

    await assert.rejects(
      () => acquireDaemonLock(directory, process.pid),
      (error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        assert.match(message, /lock file is held by another process/);
        return true;
      },
    );

    // Verify file was NOT unlinked / assumed stale
    const content = await readFile(pidPath, "utf8");
    assert.equal(content, "");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
