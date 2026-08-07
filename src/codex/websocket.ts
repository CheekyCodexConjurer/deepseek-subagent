import { redactSecrets } from "../security.js";
import type { JsonRpcNotification } from "./jsonrpc.js";

interface WebSocketEvent {
  data?: unknown;
}

interface WebSocketLike {
  readonly readyState: number;
  addEventListener(type: string, listener: (event: WebSocketEvent) => void): void;
  removeEventListener(type: string, listener: (event: WebSocketEvent) => void): void;
  send(data: string): void;
  close(): void;
}

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

type WebSocketConstructor = new (url: string) => WebSocketLike;

const OPEN = 1;

/**
 * JSON-RPC transport for an explicitly configured local Codex WebSocket.
 *
 * This is intentionally opt-in. The Windows Desktop App Server currently
 * uses an internal stdio child and does not expose this endpoint by itself.
 */
export class JsonRpcWebSocketClient {
  private readonly pending = new Map<number, PendingRequest>();
  private readonly notificationListeners = new Set<(notification: JsonRpcNotification) => void>();
  private nextId = 1;
  private socket: WebSocketLike | null = null;
  private closed = false;

  async start(endpoint: string, _args: string[]): Promise<void> {
    if (this.socket) throw new Error("JSON-RPC WebSocket client already started");
    validateEndpoint(endpoint);
    const WebSocketImpl = getWebSocketConstructor();
    const socket = new WebSocketImpl(endpoint);
    this.socket = socket;
    this.closed = false;
    socket.addEventListener("message", (event) => {
      void this.handleMessage(event.data).catch((error: unknown) => {
        this.failConnection(new Error(redactSecrets(String(error))));
      });
    });
    socket.addEventListener("error", () => {
      this.failConnection(new Error("Codex app-server WebSocket reported an error"));
    });
    socket.addEventListener("close", () => {
      if (!this.closed) this.failConnection(new Error("Codex app-server WebSocket closed"));
    });
    try {
      await waitForOpen(socket);
      await this.call("initialize", {
        clientInfo: { name: "codex-opencode-bridge", title: "DeepSeek Sub-Agent", version: "0.1.0" },
        capabilities: {},
      });
      this.notify("initialized", {});
    } catch (error) {
      this.closed = true;
      this.failPending(new Error(redactSecrets(String(error))));
      this.socket = null;
      if (socket.readyState !== 3) socket.close();
      throw error;
    }
  }

  onNotification(listener: (notification: JsonRpcNotification) => void): () => void {
    this.notificationListeners.add(listener);
    return () => {
      this.notificationListeners.delete(listener);
    };
  }

  async call(method: string, params: unknown): Promise<unknown> {
    const socket = this.socket;
    if (!socket || this.closed || socket.readyState !== OPEN) {
      throw new Error("Codex app-server WebSocket is not connected");
    }
    const id = this.nextId++;
    const request = JSON.stringify({ jsonrpc: "2.0", id, method, params });
    const pending = new Promise<unknown>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
    try {
      socket.send(request);
    } catch (error) {
      this.pending.delete(id);
      throw new Error(redactSecrets(String(error)));
    }
    return pending;
  }

  notify(method: string, params: unknown): void {
    const socket = this.socket;
    if (!socket || this.closed || socket.readyState !== OPEN) return;
    socket.send(JSON.stringify({ jsonrpc: "2.0", method, params }));
  }

  async close(): Promise<void> {
    this.closed = true;
    this.failPending(new Error("Codex app-server WebSocket closed"));
    const socket = this.socket;
    this.socket = null;
    if (!socket || socket.readyState === 3) return;
    socket.close();
  }

  private async handleMessage(data: unknown): Promise<void> {
    const line = await messageText(data);
    if (!line.trim()) return;
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      throw new Error("Invalid Codex app-server WebSocket JSON: " + redactSecrets(line.slice(0, 400)));
    }
    if (!value || typeof value !== "object") {
      throw new Error("Invalid Codex app-server WebSocket JSON-RPC message envelope");
    }
    const record = value as Record<string, unknown>;
    if (Object.prototype.hasOwnProperty.call(record, "jsonrpc") && record.jsonrpc !== "2.0") {
      throw new Error("Invalid Codex app-server WebSocket JSON-RPC version");
    }
    const hasId = Object.prototype.hasOwnProperty.call(record, "id");
    if (hasId) {
      if (typeof record.id !== "number" || typeof record.method === "string") {
        throw new Error("Invalid Codex app-server WebSocket JSON-RPC response envelope");
      }
      const hasResult = Object.prototype.hasOwnProperty.call(record, "result");
      const hasError = Object.prototype.hasOwnProperty.call(record, "error");
      if (hasResult === hasError || (hasError && !isJsonRpcError(record.error))) {
        throw new Error("Codex app-server WebSocket response must contain exactly one valid result or error");
      }
      const pending = this.pending.get(record.id);
      if (!pending) throw new Error("Codex app-server WebSocket returned an unknown request id");
      this.pending.delete(record.id);
      if (hasError) {
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
      return;
    }
    throw new Error("Invalid Codex app-server WebSocket JSON-RPC message envelope");
  }

  private failPending(error: Error): void {
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
  }

  private failConnection(error: Error): void {
    this.closed = true;
    this.failPending(error);
    const socket = this.socket;
    if (socket && socket.readyState !== 3) socket.close();
  }
}

function getWebSocketConstructor(): WebSocketConstructor {
  const candidate = (globalThis as { WebSocket?: unknown }).WebSocket;
  if (typeof candidate !== "function") {
    throw new Error("This Node.js runtime does not provide the WebSocket API required for Codex app-server delivery");
  }
  return candidate as WebSocketConstructor;
}

function validateEndpoint(endpoint: string): void {
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    throw new Error("Codex app-server WebSocket endpoint is not a valid URL");
  }
  if (url.protocol !== "ws:") {
    throw new Error("Codex app-server delivery accepts only a local ws:// endpoint");
  }
  if (!isLoopbackHostname(url.hostname)) {
    throw new Error("Codex app-server WebSocket endpoint must use a loopback host");
  }
}

function isLoopbackHostname(hostname: string): boolean {
  const normalized = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  return normalized === "localhost" || normalized === "127.0.0.1" || normalized === "::1";
}

async function waitForOpen(socket: WebSocketLike): Promise<void> {
  if (socket.readyState === OPEN) return;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("Codex app-server WebSocket did not open before the connection timeout"));
    }, 10_000);
    timer.unref?.();
    const onOpen = () => {
      cleanup();
      resolve();
    };
    const onError = () => {
      cleanup();
      reject(new Error("Unable to open the Codex app-server WebSocket"));
    };
    const cleanup = () => {
      clearTimeout(timer);
      socket.removeEventListener("open", onOpen);
      socket.removeEventListener("error", onError);
      socket.removeEventListener("close", onError);
    };
    socket.addEventListener("open", onOpen);
    socket.addEventListener("error", onError);
    socket.addEventListener("close", onError);
  });
}

async function messageText(data: unknown): Promise<string> {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (typeof Blob !== "undefined" && data instanceof Blob) return await data.text();
  return String(data ?? "");
}

function isJsonRpcError(value: unknown): value is { code: number; message: string } {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  return typeof record.code === "number" && typeof record.message === "string";
}
