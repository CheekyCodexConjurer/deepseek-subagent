import { spawn, type ChildProcess } from "node:child_process";
import { access } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createServer } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { newId, redactSecrets } from "../security.js";
import type { BridgeConfig, OpenCodeClientLike } from "../types.js";
import { OpenCodeClient } from "./client.js";

export interface ManagedOpenCode {
  serverId: string;
  baseUrl: string;
  client: OpenCodeClientLike;
  processId: number | null;
  stop(): Promise<void>;
}

export class OpenCodeManager {
  private active: ManagedOpenCode | null = null;
  private child: ChildProcess | null = null;
  private restartTask: Promise<void> | null = null;
  private restartAbort: AbortController | null = null;
  private stopping = false;

  constructor(private readonly config: BridgeConfig) {}

  async start(workspaceRoot: string): Promise<ManagedOpenCode> {
    if (this.active) return this.active;
    this.stopping = false;
    if (this.config.opencodeMode === "attach") {
      if (!this.config.opencodeUrl) throw new Error("Attach mode requires opencodeUrl");
      const client = new OpenCodeClient({
        baseUrl: this.config.opencodeUrl,
        username: this.config.opencodeUsername,
        password: this.config.opencodePassword,
        reconnectMaxMs: this.config.opencodeEventReconnectMaxMs,
      });
      await this.waitForHealth(client);
      const serverId = newId("server");
      this.active = {
        serverId,
        baseUrl: client.baseUrl,
        client,
        processId: null,
        stop: async () => {
          if (this.active?.serverId === serverId) this.active = null;
        },
      };
      return this.active;
    }

    const command = await this.resolveBinary();
    const port = await findFreePort();
    const password = this.config.opencodePassword ?? newId("local");
    const baseUrl = "http://127.0.0.1:" + port;
    const client = new OpenCodeClient({
      baseUrl,
      username: this.config.opencodeUsername,
      password,
      reconnectMaxMs: this.config.opencodeEventReconnectMaxMs,
    });
    const child = await this.spawnManagedChild(command, workspaceRoot, port, password, client);
    this.child = child;
    let processId = child.pid ?? null;
    const serverId = newId("server");
    const managed: ManagedOpenCode = {
      serverId,
      baseUrl,
      client,
      get processId() {
        return processId;
      },
      stop: async () => {
        this.stopping = true;
        this.restartAbort?.abort();
        await this.restartTask?.catch(() => undefined);
        this.restartTask = null;
        this.restartAbort = null;
        const currentChild = this.child;
        if (currentChild) await this.stopChild(currentChild);
        if (this.active?.serverId === serverId) this.active = null;
        if (this.child === currentChild) this.child = null;
      },
    };
    this.active = managed;
    this.attachExitHandler(managed, child, command, workspaceRoot, port, password, client, (pid) => {
      processId = pid;
    });
    if (child.exitCode !== null || child.signalCode !== null) {
      this.scheduleRestart(managed, command, workspaceRoot, port, password, client, (pid) => {
        processId = pid;
      });
    }
    return managed;
  }

  async stop(): Promise<void> {
    this.stopping = true;
    this.restartAbort?.abort();
    if (this.active) {
      await this.active.stop();
      return;
    }
    await this.restartTask?.catch(() => undefined);
    this.restartTask = null;
    this.restartAbort = null;
    if (this.child) await this.stopChild(this.child);
    this.child = null;
  }

  private async spawnManagedChild(
    command: string,
    workspaceRoot: string,
    port: number,
    password: string,
    client: OpenCodeClient,
    signal?: AbortSignal,
  ): Promise<ChildProcess> {
    const args = ["serve", "--hostname", "127.0.0.1", "--port", String(port)];
    const child = spawn(command, args, {
      cwd: workspaceRoot,
      env: {
        ...process.env,
        OPENCODE_SERVER_USERNAME: this.config.opencodeUsername,
        OPENCODE_SERVER_PASSWORD: password,
        OPENCODE_PERMISSION: JSON.stringify({ "*": "allow" }),
      },
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    this.child = child;
    let spawnError: Error | null = null;
    child.once("error", (error) => {
      spawnError = error;
    });
    child.stdout?.resume();
    child.stderr?.resume();
    try {
      await this.waitForHealth(client, child, () => spawnError, signal);
    } catch (error) {
      await this.stopChild(child);
      if (this.child === child) this.child = null;
      throw new Error("Unable to start OpenCode: " + redactSecrets(String(error)));
    }
    return child;
  }

  private async resolveBinary(): Promise<string> {
    if (this.config.opencodeBinary) {
      const configured = path.resolve(this.config.opencodeBinary);
      await access(configured);
      if (isShellShim(configured)) throw new Error("opencodeBinary must point to an executable, not a shell shim");
      return configured;
    }
    const candidates: string[] = [];
    if (process.env.APPDATA) {
      candidates.push(path.join(process.env.APPDATA, "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"));
    }
    candidates.push(path.join(os.homedir(), "AppData", "Roaming", "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"));
    for (const candidate of candidates) {
      try {
        await access(candidate);
        return candidate;
      } catch {
        // Try the next explicit executable path.
      }
    }
    if (process.platform === "win32") {
      const where = spawn("where.exe", ["opencode"], { shell: false, windowsHide: true, stdio: ["ignore", "pipe", "ignore"] });
      const output = await collectOutput(where);
      const first = output.split(/\r?\n/).map((line) => line.trim()).find((line) => line.length > 0 && !isShellShim(line));
      if (first) return first;
    }
    return "opencode";
  }

  private async waitForHealth(
    client: OpenCodeClientLike,
    child?: ChildProcess,
    getSpawnError?: () => Error | null,
    signal?: AbortSignal,
  ): Promise<void> {
    const deadline = Date.now() + this.config.opencodeStartupTimeoutMs;
    let lastError = "health check has not run";
    while (Date.now() < deadline && !signal?.aborted) {
      if (child?.exitCode !== null && child?.exitCode !== undefined) {
        throw new Error("OpenCode exited with code " + child.exitCode);
      }
      const spawnError = getSpawnError?.();
      if (spawnError) throw spawnError;
      try {
        const health = await client.health();
        if (health.healthy) return;
        lastError = "OpenCode reported unhealthy";
      } catch (error) {
        lastError = String(error);
      }
      await delay(250, undefined, { signal }).catch(() => undefined);
    }
    if (signal?.aborted) throw new Error("OpenCode startup was cancelled");
    throw new Error("OpenCode health timeout: " + lastError);
  }

  private attachExitHandler(
    managed: ManagedOpenCode,
    child: ChildProcess,
    command: string,
    workspaceRoot: string,
    port: number,
    password: string,
    client: OpenCodeClient,
    setProcessId: (pid: number | null) => void,
  ): void {
    child.once("exit", () => {
      if (this.child === child) this.child = null;
      setProcessId(null);
      if (!this.stopping && this.active?.serverId === managed.serverId) {
        this.scheduleRestart(managed, command, workspaceRoot, port, password, client, setProcessId);
      }
    });
  }

  private scheduleRestart(
    managed: ManagedOpenCode,
    command: string,
    workspaceRoot: string,
    port: number,
    password: string,
    client: OpenCodeClient,
    setProcessId: (pid: number | null) => void,
  ): void {
    if (this.restartTask || this.stopping || this.active?.serverId !== managed.serverId) return;
    const controller = new AbortController();
    this.restartAbort = controller;
    this.restartTask = (async () => {
      for (let attempt = 0; !this.stopping && this.active?.serverId === managed.serverId; attempt += 1) {
        const waitMs = Math.min(this.config.opencodeEventReconnectMaxMs, 500 * 2 ** Math.min(attempt, 8)) + Math.floor(Math.random() * 250);
        await delay(waitMs, undefined, { signal: controller.signal }).catch(() => undefined);
        if (this.stopping || controller.signal.aborted || this.active?.serverId !== managed.serverId) return;
        try {
          const child = await this.spawnManagedChild(command, workspaceRoot, port, password, client, controller.signal);
          if (this.stopping || controller.signal.aborted || this.active?.serverId !== managed.serverId) {
            await this.stopChild(child);
            return;
          }
          if (child.exitCode !== null || child.signalCode !== null) {
            if (this.child === child) this.child = null;
            setProcessId(null);
            continue;
          }
          setProcessId(child.pid ?? null);
          this.attachExitHandler(managed, child, command, workspaceRoot, port, password, client, setProcessId);
          return;
        } catch {
          // Keep retrying while the bridge is running; the SSE client uses the same loopback URL.
        }
      }
    })().finally(() => {
      if (this.restartAbort === controller) this.restartAbort = null;
      this.restartTask = null;
    });
  }

  private async stopChild(child: ChildProcess): Promise<void> {
    if (child.exitCode !== null || child.signalCode !== null) return;
    child.kill();
    await Promise.race([
      new Promise<void>((resolve) => child.once("exit", () => resolve())),
      delay(5_000),
    ]);
  }
}

function isShellShim(filePath: string): boolean {
  return [".cmd", ".bat", ".ps1", ".com"].includes(path.extname(filePath).toLowerCase());
}

async function findFreePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  if (!port) throw new Error("Unable to allocate a local port");
  return port;
}

async function collectOutput(child: ChildProcess): Promise<string> {
  const chunks: Buffer[] = [];
  child.stdout?.on("data", (chunk: Buffer) => chunks.push(chunk));
  await new Promise<void>((resolve) => child.once("close", () => resolve()));
  return Buffer.concat(chunks).toString("utf8");
}
