import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { doctorDatabaseCheck, readCodexMcpToolTimeout, runCapture, terminateProcessTree, withStore } from "../../src/cli.js";
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
