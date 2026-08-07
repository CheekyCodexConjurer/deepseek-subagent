import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import { redactSecrets } from "../security.js";

export interface JsonRpcNotification {
  method: string;
  params?: unknown;
}

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

export class JsonRpcStdioClient {
  private readonly pending = new Map<number, PendingRequest>();
  private nextId = 1;
  private process: ChildProcess | null = null;
  private closed = false;
  private readonly notificationListeners = new Set<(notification: JsonRpcNotification) => void>();

  async start(command: string, args: string[]): Promise<void> {
    if (this.process) throw new Error("JSON-RPC client already started");
    const windowsShim = process.platform === "win32" && (command === "codex" || path.extname(command).toLowerCase() === ".cmd");
    const spawnCommand = windowsShim ? "cmd.exe" : command;
    const spawnArgs = windowsShim
      ? ["/d", "/s", "/c", [command, ...args].map(quoteWindowsArgument).join(" ")]
      : args;
    const child = spawn(spawnCommand, spawnArgs, {
      shell: false,
      windowsHide: true,
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.process = child;
    child.stderr?.resume();
    child.once("error", (error) => this.failPending(error));
    child.once("exit", (code, signal) => {
      if (!this.closed) this.failPending(new Error("Codex app-server exited: code=" + code + " signal=" + signal));
    });
    if (!child.stdin || !child.stdout) throw new Error("Codex app-server stdio was not available");
    const lines = createInterface({ input: child.stdout });
    lines.on("line", (line) => this.handleLine(line));
    await this.call("initialize", {
      clientInfo: { name: "codex-opencode-bridge", title: "DeepSeek Sub-Agent", version: "0.1.0" },
      capabilities: {},
    });
    this.notify("initialized", {});
  }

  onNotification(listener: (notification: JsonRpcNotification) => void): () => void {
    this.notificationListeners.add(listener);
    return () => {
      this.notificationListeners.delete(listener);
    };
  }

  async call(method: string, params: unknown): Promise<unknown> {
    if (!this.process?.stdin || this.closed) throw new Error("Codex app-server is not connected");
    const id = this.nextId++;
    const request = JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n";
    const pending = new Promise<unknown>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
    this.process.stdin.write(request);
    return pending;
  }

  notify(method: string, params: unknown): void {
    if (!this.process?.stdin || this.closed) return;
    this.process.stdin.write(JSON.stringify({ jsonrpc: "2.0", method, params }) + "\n");
  }

  async close(): Promise<void> {
    this.closed = true;
    for (const pending of this.pending.values()) pending.reject(new Error("Codex app-server closed"));
    this.pending.clear();
    const child = this.process;
    this.process = null;
    if (!child || child.exitCode !== null) return;
    child.kill();
    await new Promise<void>((resolve) => {
      child.once("exit", () => resolve());
      setTimeout(resolve, 2_000);
    });
  }

  private handleLine(line: string): void {
    if (!line.trim()) return;
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      this.failPending(new Error("Invalid Codex app-server JSON: " + redactSecrets(line.slice(0, 400))));
      return;
    }
    if (!value || typeof value !== "object") return;
    const record = value as Record<string, unknown>;
    if (typeof record.id === "number") {
      const pending = this.pending.get(record.id);
      if (!pending) return;
      this.pending.delete(record.id);
      if (record.error && typeof record.error === "object") {
        const error = record.error as Record<string, unknown>;
        pending.reject(new Error("Codex " + String(error.message ?? "JSON-RPC error")));
      } else {
        pending.resolve(record.result);
      }
      return;
    }
    if (typeof record.method === "string") {
      for (const listener of this.notificationListeners) {
        listener({
          method: record.method,
          ...(record.params === undefined ? {} : { params: record.params }),
        });
      }
    }
  }

  private failPending(error: Error): void {
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
  }
}

function quoteWindowsArgument(value: string): string {
  if (/^[A-Za-z0-9_./:=+-]+$/.test(value)) return value;
  return "\"" + value.replace(/(["\\])/g, "\\$1") + "\"";
}
