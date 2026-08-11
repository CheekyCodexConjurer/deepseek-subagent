import { access, open, readFile, unlink } from "node:fs/promises";
import { spawn } from "node:child_process";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { canRead, defaultUserDataRoot, ensurePrivateDir, redactSecrets, writePrivateFile } from "./security.js";
import { createDefaultConfig, DEFAULT_CODEX_MCP_TOOL_TIMEOUT_SEC, defaultConfigPath, FOLLOW_MAX_TOTAL_MINUTES, isValidFollowDefaults, loadConfig, saveConfig } from "./config.js";
import { BridgeHttpClient, BridgeHttpServer, BRIDGE_CONNECT_TIMEOUT_MS, createDoctorHealthDispatcher, DOCTOR_HEALTH_TIMEOUT_MS } from "./http-server.js";
import { runMcp } from "./mcp.js";
import { BridgeService } from "./service.js";
import { BridgeStore } from "./store.js";
import { OpenCodeClient } from "./opencode/client.js";
import type { BridgeConfig, DoctorCheck, DoctorReport } from "./types.js";

export const DOCTOR_PROBE_TIMEOUT_MS = 10_000;

interface CliArgs {
  command: string;
  rest: string[];
  json: boolean;
  verbose: boolean;
  configPath: string;
  removeCodex: boolean;
  purgeData: boolean;
  confirmPurge: boolean;
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const parsed = parseArgs(argv);
  if (parsed.command === "mcp") {
    await runMcp(parsed.configPath);
    return;
  }
  if (parsed.command === "uninstall") {
    await runUninstall(parsed);
    return;
  }
  const config = await ensureConfig(parsed.configPath);
  switch (parsed.command) {
    case "help":
      printHelp();
      return;
    case "install":
      await saveConfig(config);
      await installInstructions(config);
      output(parsed.json, {
        installed: true,
        displayName: "DeepSeek Sub-Agent",
        configPath: config.configPath,
        dataDir: config.dataDir,
      }, "DeepSeek Sub-Agent configuration is ready.");
      return;
    case "daemon":
      await runDaemon(config);
      return;
    case "doctor":
      await outputDoctor(config, parsed.json);
      return;
    case "start":
      await startDaemon(config, parsed.json);
      return;
    case "stop":
      await stopDaemon(config, parsed.json);
      return;
    case "restart":
      await stopDaemon(config, true);
      await startDaemon(config, parsed.json);
      return;
    case "agents":
      await listResource(config, "agents", parsed.json, parsed.verbose);
      return;
    case "jobs":
      await listResource(config, "jobs", parsed.json, parsed.verbose);
      return;
    case "logs":
      await showLogs(config, parsed.json);
      return;
    case "agent":
      await showAgent(config, parsed.rest[0] === "show" ? parsed.rest[1] : parsed.rest[0], parsed.json);
      return;
    case "inbox":
      await listInbox(config, parsed.json);
      return;
    case "recover":
      await recover(config, parsed.rest[0], parsed.json);
      return;
    case "deliver":
      await deliver(config, parsed.rest[0], parsed.json);
      return;
    case "config":
      await showConfig(config, parsed.json);
      return;
    default:
      throw new Error("Unknown command: " + parsed.command);
  }
}

async function runDaemon(config: BridgeConfig): Promise<void> {
  await ensurePrivateDir(config.dataDir);
  await saveConfig(config);
  const service = new BridgeService(config);
  const http = new BridgeHttpServer(config, service);
  await service.start();
  await http.start();
  await writePrivateFile(path.join(config.dataDir, "daemon.pid"), String(process.pid) + "\n");
  console.error("DeepSeek Sub-Agent daemon listening on " + config.daemonHost + ":" + config.daemonPort);
  const shutdown = async () => {
    await http.stop();
    await service.stop();
    await unlink(path.join(config.dataDir, "daemon.pid")).catch(() => undefined);
  };
  const onSignal = () => {
    void shutdown().then(() => process.exit(0));
  };
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);
  await new Promise<void>(() => undefined);
}

async function startDaemon(config: BridgeConfig, json: boolean): Promise<void> {
  const client = new BridgeHttpClient(config);
  try {
    await client.health();
    output(json, { running: true, alreadyRunning: true }, "DeepSeek Sub-Agent daemon is already running.");
    return;
  } catch {
    // Start below.
  }
  const script = process.argv[1];
  if (!script) throw new Error("Unable to resolve CLI script");
  const logHandle = await open(path.join(config.dataDir, "daemon.log"), "a");
  const child = spawn(process.execPath, [script, "daemon", "--config", config.configPath], {
    detached: true,
    stdio: ["ignore", logHandle.fd, logHandle.fd],
    windowsHide: true,
    shell: false,
  });
  await logHandle.close();
  child.unref();
  output(json, { running: "starting", pid: child.pid ?? null }, "DeepSeek Sub-Agent daemon is starting.");
}

async function stopDaemon(config: BridgeConfig, json: boolean): Promise<void> {
  const pidPath = path.join(config.dataDir, "daemon.pid");
  let pid: number | null = null;
  try {
    const value = Number.parseInt(await readFile(pidPath, "utf8"), 10);
    if (Number.isInteger(value) && value > 0) pid = value;
  } catch {
    // No pid file.
  }
  if (!pid) {
    output(json, { stopped: false, reason: "no_pid_file" }, "No DeepSeek Sub-Agent daemon pid file was found.");
    return;
  }
  if (process.platform === "win32") {
    await runProcess("taskkill.exe", ["/PID", String(pid), "/T", "/F"]);
  } else {
    process.kill(pid, "SIGTERM");
  }
  await unlink(pidPath).catch(() => undefined);
  output(json, { stopped: true }, "DeepSeek Sub-Agent daemon stopped.");
}

async function outputDoctor(config: BridgeConfig, json: boolean): Promise<void> {
  const checks: DoctorCheck[] = [];
  const push = (check: DoctorCheck): void => {
    checks.push(check);
    console.error("[doctor] " + check.name + "=" + check.status);
  };
  push({
    name: "node",
    status: Number.parseInt(process.versions.node.split(".")[0] ?? "0", 10) >= 24 ? "ok" : "error",
    detail: process.version,
  });
  push({
    name: "data_directory",
    status: await canRead(config.dataDir) ? "ok" : "warning",
    detail: config.dataDir,
  });
  console.error(`[doctor] probing codex and opencode CLIs (each probe bounded to ${DOCTOR_PROBE_TIMEOUT_MS / 1000}s, probes run in parallel)`);
  const [codexVersion, openCodeCommand] = await Promise.all([
    runCodex(["--version"]),
    resolveOpenCodeCommand(),
  ]);
  push({
    name: "codex_installed",
    status: codexVersion.ok ? "ok" : "warning",
    detail: codexVersion.ok ? codexVersion.output.trim() : codexVersion.error,
  });
  const [openCodeVersion, authList, models, mcpList] = await Promise.all([
    runCapture(openCodeCommand, ["--version"]),
    runCapture(openCodeCommand, ["auth", "list"]),
    runCapture(openCodeCommand, ["models"]),
    runCodex(["mcp", "list"]),
  ]);
  push({
    name: "opencode_installed",
    status: openCodeVersion.ok ? "ok" : "warning",
    detail: openCodeVersion.ok ? openCodeVersion.output.trim() : openCodeVersion.error,
  });
  push({
    name: "opencode_go_auth",
    status: authList.ok && /opencode-go|opencode go/i.test(authList.output) ? "ok" : "warning",
    detail: authList.ok ? "provider names inspected; secret values were not read" : authList.error,
  });
  const targetModel = /opencode-go[\/\\]deepseek-v4-flash/i.test(models.output);
  push({
    name: "deepseek_v4_flash",
    status: targetModel ? "ok" : "warning",
    detail: targetModel ? "configured model is listed by OpenCode" : "target model was not found in OpenCode model listing",
  });
  push({
    name: "max_variant",
    status: config.opencodeVariant === "max" ? "warning" : "unknown",
    detail: config.opencodeVariant === "max"
      ? "configured as max; live runtime smoke is the proof, not a static listing"
      : "no max variant configured",
  });
  push({
    name: "async_execution",
    status: "ok",
    detail: "spawn dispatches asynchronously and follow waits on internal events",
  });
  push({
    name: "sse_completion_events",
    status: "ok",
    detail: "OpenCode SSE session.idle events are subscribed without job-status polling",
  });
  push({
    name: "progress_snapshots",
    status: "ok",
    detail: "observable activity is persisted in SQLite without private reasoning",
  });
  push({
    name: "follow_mode",
    status: "ok",
    detail: "event-driven waiter with one deadline timer and persisted restart state",
  });
  push({
    name: "follow_default_timeout",
    status: isValidFollowDefaults(config) ? "ok" : "error",
    detail: isValidFollowDefaults(config)
      ? `${config.followDefaultWaitMinutes} min wait + ${config.followDefaultGraceMinutes} min graceful finalize`
      : "MISCONFIGURED: follow defaults must be whole minutes within 1..60 wait and 1..10 grace",
  });
  const codexToolTimeout = await readCodexMcpToolTimeout();
  const minimumToolTimeout = DEFAULT_CODEX_MCP_TOOL_TIMEOUT_SEC;
  push({
    name: "mcp_tool_timeout",
    status: codexToolTimeout !== null && codexToolTimeout >= minimumToolTimeout ? "ok" : "warning",
    detail: codexToolTimeout === null
      ? `MISCONFIGURED: Codex MCP tool_timeout_sec was not found; required > ${FOLLOW_MAX_TOTAL_MINUTES} min, recommended ${DEFAULT_CODEX_MCP_TOOL_TIMEOUT_SEC / 60} min`
      : `DeepSeek follow max wait: ${FOLLOW_MAX_TOTAL_MINUTES} min; Codex MCP tool timeout: ${Math.floor(codexToolTimeout / 60)} min; Status: ${codexToolTimeout >= minimumToolTimeout ? "OK" : "MISCONFIGURED"}`,
  });
  push({
    name: "same_chat_push",
    status: config.experimentalSameChatDelivery ? "warning" : "ok",
    detail: config.experimentalSameChatDelivery
      ? "Experimental / Enabled; requires an explicit live App Server correlation"
      : "Experimental / Disabled; normal operation uses persistence and follow",
  });
  push({
    name: "fallback_persistence",
    status: "ok",
    detail: "private inbox and result files remain the durable fallback",
  });
  push({
    name: "mcp_registered",
    status: /deepseek-subagent/i.test(mcpList.output) ? "ok" : "warning",
    detail: mcpList.ok ? "deepseek-subagent registration was " + (/deepseek-subagent/i.test(mcpList.output) ? "found" : "not found") : mcpList.error,
  });
  push({
    name: "codex_delivery",
    status: config.experimentalSameChatDelivery && (config.codexAppServerCommand || config.codexAppServerSocket) ? "warning" : "ok",
    detail: config.experimentalSameChatDelivery && (config.codexAppServerCommand || config.codexAppServerSocket)
      ? "Configured adapter is separate from Codex Desktop and requires a live correlation probe."
      : "Same-chat push is experimental and disabled; inbox fallback is expected.",
  });
  if (config.opencodeMode === "attach" && config.opencodeUrl) {
    try {
      const client = new OpenCodeClient({
        baseUrl: config.opencodeUrl,
        username: config.opencodeUsername,
        password: config.opencodePassword,
      });
      console.error("[doctor] probing opencode health (bounded to 5s)");
      const health = await client.health();
      push({ name: "opencode_health", status: health.healthy ? "ok" : "error", detail: health.version ?? "healthy" });
    } catch (error) {
      push({ name: "opencode_health", status: "error", detail: redactSecrets(String(error)) });
    }
  } else {
    push({ name: "opencode_mode", status: "ok", detail: "managed loopback server; health is checked at daemon start" });
  }
  console.error(`[doctor] probing bridge daemon health (headers/body bounded to ${DOCTOR_HEALTH_TIMEOUT_MS / 1000}s; TCP connect separately bounded to ${BRIDGE_CONNECT_TIMEOUT_MS / 1000}s)`);
  try {
    const health = await new BridgeHttpClient(config, createDoctorHealthDispatcher()).health();
    const status = health && typeof health === "object" ? (health as Record<string, unknown>).status : null;
    const running = status && typeof status === "object" ? (status as Record<string, unknown>).running === true : false;
    push({ name: "bridge_daemon", status: running ? "ok" : "warning", detail: JSON.stringify(health) });
  } catch (error) {
    push({ name: "bridge_daemon", status: "warning", detail: "not reachable: " + redactSecrets(String(error)) });
  }
  const databasePath = path.join(config.dataDir, "bridge.sqlite");
  console.error("[doctor] checking database (synchronous SQLite quick_check; it can take a moment under IO/lock contention)");
  await doctorDatabaseCheck(databasePath, push);
  const report: DoctorReport = {
    generatedAt: new Date().toISOString(),
    displayName: "DeepSeek Sub-Agent",
    checks,
    completeDeliverySupported: false,
  };
  output(json, report, checks.map((check) => check.name + "=" + check.status + " (" + check.detail + ")").join("\n"));
}

// Runs the synchronous SQLite doctor checks and guarantees the store handle is
// closed even when a check throws. The synchronous quick_check cannot be
// interrupted from the caller, so the honest contract is: run it, report its
// result, and never leak the handle.
export function withStore<T>(databasePath: string, fn: (store: BridgeStore) => T): T {
  const store = new BridgeStore(databasePath);
  try {
    return fn(store);
  } finally {
    store.close();
  }
}

export async function doctorDatabaseCheck(databasePath: string, push: (check: DoctorCheck) => void): Promise<void> {
  if (!(await canRead(databasePath))) {
    push({ name: "database", status: "warning", detail: "database has not been created yet" });
    return;
  }
  try {
    withStore(databasePath, (store) => {
      const integrity = store.integrityCheck();
      push({ name: "database", status: integrity === "ok" ? "ok" : "error", detail: integrity });
      const hintCount = store.countJobsWithCorrelationHints();
      const bindingCount = store.countCodexBindings();
      push({
        name: "correlation",
        status: "ok",
        detail: `${hintCount} persisted MCP hint(s) vs ${bindingCount} authoritative App Server binding(s); delivery still requires an authoritative binding`,
      });
    });
  } catch (error) {
    push({ name: "database", status: "error", detail: redactSecrets(String(error)) });
  }
}

async function listResource(config: BridgeConfig, resource: "agents" | "jobs", json: boolean, verbose: boolean): Promise<void> {
  const value = await (resource === "agents"
    ? new BridgeHttpClient(config).get<{ agents: unknown[] }>("/v1/agents")
    : new BridgeHttpClient(config).get<{ jobs: unknown[] }>("/v1/jobs"));
  if (json) {
    console.log(JSON.stringify(value, null, 2));
    return;
  }
  const records = resource === "agents"
    ? (value as { agents: Array<Record<string, unknown>> }).agents
    : (value as { jobs: Array<Record<string, unknown>> }).jobs;
  if (records.length === 0) {
    console.log("No " + resource + ".");
    return;
  }
  for (const record of records) {
    if (resource === "agents") {
      const line = [
        typeof record.title === "string" ? record.title : "DeepSeek task",
        humanResourceState(record.status),
        durationLabel(record.createdAt, record.updatedAt),
        displayModelLabel(record.modelId, record.modelVariant),
        typeof record.topic === "string" ? record.topic : "",
      ].filter(Boolean).join(" · ");
      console.log(line);
      if (verbose) {
        console.log("  agent_id=" + String(record.id ?? ""));
        console.log("  opencode_session_id=" + String(record.opencodeSessionId ?? ""));
        console.log("  workspace=" + String(record.workspacePath ?? ""));
      }
    } else {
      const line = [
        "DeepSeek task",
        humanResourceState(record.status),
        durationLabel(record.createdAt, record.completedAt),
      ].filter(Boolean).join(" · ");
      console.log(line);
      if (verbose) {
        console.log("  job_id=" + String(record.id ?? ""));
        console.log("  agent_id=" + String(record.agentId ?? ""));
      }
    }
  }
}

async function showAgent(config: BridgeConfig, id: string | undefined, json: boolean): Promise<void> {
  if (!id) throw new Error("agent show requires an agent id; use --json with agents to obtain technical ids");
  const value = await new BridgeHttpClient(config).get<Record<string, unknown>>("/v1/agents/" + encodeURIComponent(id));
  const agent = (value.agent && typeof value.agent === "object") ? value.agent as Record<string, unknown> : {};
  const human = [
    "DeepSeek Sub-Agent · " + String(agent.topic ?? "unknown"),
    "state=" + humanResourceState(agent.status),
    "duration=" + (durationLabel(agent.createdAt, agent.updatedAt) || "unknown"),
    "model=" + displayModelLabel(agent.modelId, agent.modelVariant),
    "agent_id=" + String(agent.id ?? id),
    "opencode_session_id=" + String(agent.opencodeSessionId ?? "unknown"),
    "workspace=" + String(agent.workspacePath ?? "unknown"),
  ].join("\n");
  output(json, value, human);
}

async function listInbox(config: BridgeConfig, json: boolean): Promise<void> {
  const entries = await import("node:fs/promises").then(({ readdir }) => readdir(path.join(config.dataDir, "inbox"), { withFileTypes: true }).catch(() => []));
  const files = entries.filter((entry) => entry.isFile() && entry.name.endsWith(".json")).map((entry) => entry.name);
  output(json, { files }, files.length + " persisted inbox result(s).");
}

async function showLogs(config: BridgeConfig, json: boolean): Promise<void> {
  const logPath = path.join(config.dataDir, "daemon.log");
  let text = "";
  try {
    text = await readFile(logPath, "utf8");
  } catch {
    text = "No detached daemon log exists. Foreground daemon logs are written to stderr.";
  }
  const lines = text.split(/\r?\n/).filter(Boolean).slice(-200);
  output(json, { logPath, lines }, lines.join("\n"));
}

async function recover(config: BridgeConfig, jobId: string | undefined, json: boolean): Promise<void> {
  if (!jobId) throw new Error("recover requires a job id");
  const result = await new BridgeHttpClient(config).call<unknown>("/v1/jobs/recover", { jobId });
  output(json, result, "Persisted DeepSeek result recovered.");
}

async function deliver(config: BridgeConfig, jobId: string | undefined, json: boolean): Promise<void> {
  if (!jobId) throw new Error("deliver requires a job id");
  const result = await new BridgeHttpClient(config).call<unknown>("/v1/jobs/deliver", { jobId });
  output(json, result, "Persisted DeepSeek result delivered.");
}

async function showConfig(config: BridgeConfig, json: boolean): Promise<void> {
  const safe = {
    ...config,
    daemonToken: "[REDACTED]",
    opencodePassword: config.opencodePassword ? "[REDACTED]" : null,
    opencodeUrl: safeUrl(config.opencodeUrl),
    codexAppServerArgs: config.codexAppServerArgs.map((value) => redactSecrets(value)),
  };
  output(json, safe, JSON.stringify(safe, null, 2));
}

async function ensureConfig(configPath: string): Promise<BridgeConfig> {
  const config = await loadConfig(configPath);
  await ensurePrivateDir(config.dataDir);
  if (!(await canRead(configPath))) await saveConfig(config);
  return config;
}

async function installInstructions(config: BridgeConfig): Promise<void> {
  const target = path.join(config.dataDir, "orchestrator-instructions.md");
  if (await canRead(target)) return;
  const scriptPath = process.argv[1] ? path.resolve(process.argv[1]) : process.cwd();
  const source = path.resolve(path.dirname(scriptPath), "..", "docs", "orchestrator-instructions.md");
  try {
    await writePrivateFile(target, await readFile(source, "utf8"));
  } catch {
    // Package installations may omit repository docs; the checked-in copy remains available.
  }
}

function parseArgs(argv: string[]): CliArgs {
  const command = argv[0] ?? "help";
  let configPath = defaultConfigPath();
  let json = false;
  let verbose = false;
  let removeCodex = false;
  let purgeData = false;
  let confirmPurge = false;
  const rest: string[] = [];
  for (let index = 1; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--json") json = true;
    else if (value === "--verbose") verbose = true;
    else if (value === "--remove-codex") removeCodex = true;
    else if (value === "--purge-data") purgeData = true;
    else if (value === "--confirm-purge") confirmPurge = true;
    else if (value === "--config") {
      const next = argv[++index];
      if (!next) throw new Error("--config requires a path");
      configPath = path.resolve(next);
    } else rest.push(value as string);
  }
  return { command, rest, json, verbose, configPath, removeCodex, purgeData, confirmPurge };
}

function output(json: boolean, value: unknown, human: string): void {
  console.log(json ? JSON.stringify(value, null, 2) : human);
}

async function runProcess(command: string, args: string[]): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, { shell: false, windowsHide: true, stdio: "ignore" });
    child.once("error", reject);
    child.once("close", (code) => code === 0 ? resolve() : reject(new Error(command + " exited with " + code)));
  });
}

export async function runCapture(command: string, args: string[], timeoutMs = DOCTOR_PROBE_TIMEOUT_MS): Promise<{ ok: boolean; output: string; error: string }> {
  return await new Promise((resolve) => {
    const chunks: Buffer[] = [];
    const errors: Buffer[] = [];
    let settled = false;
    let child: import("node:child_process").ChildProcess;
    try {
      child = spawn(command, args, { shell: false, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
    } catch (error) {
      resolve({ ok: false, output: "", error: redactSecrets(String(error)) });
      return;
    }
    child.stdout?.on("data", (chunk: Buffer) => chunks.push(chunk));
    child.stderr?.on("data", (chunk: Buffer) => errors.push(chunk));
    const finish = (ok: boolean, error = "") => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({
        ok,
        output: Buffer.concat(chunks).toString("utf8"),
        error: error || Buffer.concat(errors).toString("utf8").trim() || "command failed",
      });
    };
    const timer = setTimeout(() => {
      terminateProcessTree(child);
      finish(false, "command timed out");
    }, timeoutMs);
    child.once("error", (error) => finish(false, redactSecrets(String(error))));
    child.once("close", (code) => finish(code === 0, code === 0 ? "" : command + " exited with " + code));
  });
}

// Killing the shell wrapper alone on Windows leaves grandchildren behind.
// taskkill /T /F terminates the whole subtree so a timed-out doctor probe
// cannot orphan a codex/opencode child. If taskkill cannot be spawned (for
// example it is missing from PATH), the failure is absorbed: cleanup is
// best-effort and must not crash the doctor process.
export function terminateProcessTree(child: import("node:child_process").ChildProcess): void {
  if (child.pid === undefined) return;
  if (process.platform === "win32") {
    const killer = spawn("taskkill.exe", ["/PID", String(child.pid), "/T", "/F"], { shell: false, windowsHide: true, stdio: "ignore" });
    killer.once("error", () => undefined);
  } else {
    child.kill();
  }
}

async function runUninstall(parsed: CliArgs): Promise<void> {
  const entry = process.argv[1];
  if (!entry) throw new Error("Unable to resolve the CLI path for uninstall");
  const script = path.resolve(path.dirname(entry), "..", "scripts", "uninstall.ps1");
  const shell = process.platform === "win32" ? "powershell.exe" : "pwsh";
  const args = ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script];
  if (parsed.removeCodex) args.push("-RemoveCodex");
  if (parsed.purgeData) args.push("-PurgeData");
  if (parsed.confirmPurge) args.push("-ConfirmPurge");
  const result = await runCapture(shell, args);
  if (!result.ok) throw new Error(redactSecrets(result.error || result.output));
  output(parsed.json, { removed: true, removeCodex: parsed.removeCodex, purgeData: parsed.purgeData }, result.output.trim() || "DeepSeek Sub-Agent uninstalled.");
}

function humanResourceState(value: unknown): string {
  switch (value) {
    case "created": return "Preparing";
    case "dispatching": return "Starting";
    case "running":
    case "working": return "Working";
    case "needs_approval": return "Needs Approval";
    case "completed": return "Completed";
    case "delivery_pending": return "Delivering";
    case "delivered": return "Delivered";
    case "failed": return "Failed";
    case "aborted":
    case "closed": return "Stopped";
    default: return typeof value === "string" && value ? value : "Unknown";
  }
}

function durationLabel(startValue: unknown, endValue: unknown): string {
  if (typeof startValue !== "string") return "";
  const start = Date.parse(startValue);
  if (!Number.isFinite(start)) return "";
  const end = typeof endValue === "string" ? Date.parse(endValue) : Date.now();
  if (!Number.isFinite(end) || end < start) return "";
  const seconds = Math.max(0, Math.round((end - start) / 1_000));
  return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function displayModelLabel(modelId: unknown, variant: unknown): string {
  if (modelId !== "deepseek-v4-flash") return typeof modelId === "string" ? modelId : "";
  return "DeepSeek V4 Flash" + (variant === "max" ? " · Max" : "");
}

function safeUrl(value: string | null): string | null {
  if (!value) return null;
  try {
    const url = new URL(value);
    url.username = "";
    url.password = "";
    return url.toString();
  } catch {
    return redactSecrets(value);
  }
}

async function resolveOpenCodeCommand(): Promise<string> {
  const candidates = [
    process.env.APPDATA ? path.join(process.env.APPDATA, "npm", "node_modules", "opencode-ai", "bin", "opencode.exe") : "",
    path.join(process.env.USERPROFILE ?? "", "AppData", "Roaming", "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"),
  ].filter(Boolean);
  for (const candidate of candidates) {
    if (await canRead(candidate)) return candidate;
  }
  return "opencode";
}

export async function readCodexMcpToolTimeout(explicitConfigPath?: string): Promise<number | null> {
  const codexHome = process.env.CODEX_HOME || path.join(process.env.USERPROFILE ?? "", ".codex");
  const configPath = explicitConfigPath ?? path.join(codexHome, "config.toml");
  let text = "";
  try {
    text = await readFile(configPath, "utf8");
  } catch {
    return null;
  }
  const header = /^\[mcp_servers\.(?:deepseek-subagent|deepseek_subagent|"deepseek-subagent"|"deepseek_subagent")\][ \t]*(?:\r?\n|$)/m.exec(text);
  if (!header || header.index === undefined) return null;
  const remainder = text.slice(header.index + header[0].length);
  const nextHeader = /^\[/m.exec(remainder);
  const section = nextHeader?.index === undefined ? remainder : remainder.slice(0, nextHeader.index);
  const value = section.match(/^\s*tool_timeout_sec\s*=\s*(\d+)\s*$/m)?.[1];
  const parsed = value ? Number.parseInt(value, 10) : Number.NaN;
  return Number.isFinite(parsed) ? parsed : null;
}

async function runCodex(args: string[]): Promise<{ ok: boolean; output: string; error: string }> {
  if (process.platform === "win32") {
    return runCapture("cmd.exe", ["/d", "/s", "/c", ["codex", ...args].join(" ")]);
  }
  return runCapture("codex", args);
}

function printHelp(): void {
  console.log([
    "DeepSeek Sub-Agent local bridge",
    "",
    "Commands: daemon, mcp, install, start, stop, restart, doctor, logs, agents, jobs, agent show <id>, inbox, deliver <jobId>, recover <jobId>, config show",
    "Use --json for machine-readable output or --verbose for technical IDs in human listings. Secrets are always redacted.",
  ].join("\n"));
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().catch((error: unknown) => {
    console.error(redactSecrets(String(error)));
    process.exitCode = 1;
  });
}
