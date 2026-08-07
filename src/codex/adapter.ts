import { JsonRpcStdioClient, type JsonRpcNotification } from "./jsonrpc.js";
import { JsonRpcWebSocketClient } from "./websocket.js";
import { redactSecrets } from "../security.js";
import type { BridgeConfig, CodexBinding, JobRecord } from "../types.js";

export interface CodexRpcTransport {
  start(command: string, args: string[]): Promise<void>;
  onNotification(listener: (notification: JsonRpcNotification) => void): () => void;
  call(method: string, params: unknown): Promise<unknown>;
  close(): Promise<void>;
}

export interface CodexCorrelation {
  jobId: string;
  threadId: string;
  turnId: string;
  itemId: string;
}

export interface CodexDeliveryAdapter {
  readonly available: boolean;
  readonly reason: string | null;
  start(): Promise<void>;
  close(): Promise<void>;
  deliver(job: JobRecord, binding: CodexBinding, text: string): Promise<"codex-steer" | "codex-start">;
  onCorrelation(listener: (correlation: CodexCorrelation) => void): () => void;
}

export class UnavailableCodexDeliveryAdapter implements CodexDeliveryAdapter {
  readonly available = false;
  constructor(readonly reason = "No compatible Codex App Server connection is configured") {}
  async start(): Promise<void> {}
  async close(): Promise<void> {}
  async deliver(_job: JobRecord, _binding: CodexBinding, _text: string): Promise<"codex-steer" | "codex-start"> {
    throw new Error(this.reason);
  }
  onCorrelation(_listener: (correlation: CodexCorrelation) => void): () => void {
    return () => undefined;
  }
}

export class CodexAppServerDeliveryAdapter implements CodexDeliveryAdapter {
  readonly available = true;
  readonly reason = null;
  private readonly rpc: CodexRpcTransport;
  private readonly correlationListeners = new Set<(correlation: CodexCorrelation) => void>();
  private readonly correlations = new Map<string, CodexCorrelation>();
  private readonly itemCorrelations = new Map<string, CodexCorrelation>();
  private readonly turnWaiters = new Set<TurnWaiter>();
  private unsubscribe: (() => void) | null = null;
  private started = false;

  constructor(private readonly config: BridgeConfig, rpc?: CodexRpcTransport) {
    this.rpc = rpc ?? (isWebSocketEndpoint(config.codexAppServerSocket)
      ? new JsonRpcWebSocketClient()
      : new JsonRpcStdioClient());
  }

  async start(): Promise<void> {
    if (this.started) return;
    if (this.config.codexAppServerSocket) {
      if (!isWebSocketEndpoint(this.config.codexAppServerSocket)) {
        throw new Error("Configured Codex app-server socket is not a supported local ws:// endpoint");
      }
      await this.rpc.start(this.config.codexAppServerSocket, []);
    } else {
      const command = this.config.codexAppServerCommand ?? "codex";
      const args = this.config.codexAppServerArgs.length > 0 ? this.config.codexAppServerArgs : ["app-server"];
      await this.rpc.start(command, args);
    }
    this.unsubscribe = this.rpc.onNotification((notification) => this.handleNotification(notification));
    this.started = true;
  }

  async close(): Promise<void> {
    this.unsubscribe?.();
    this.unsubscribe = null;
    await this.rpc.close();
    this.correlations.clear();
    this.itemCorrelations.clear();
    this.started = false;
  }

  async deliver(job: JobRecord, binding: CodexBinding, text: string): Promise<"codex-steer" | "codex-start"> {
    if (!this.started) throw new Error("Codex App Server adapter is not started");
    const input = [{ type: "text", text }];
    if (binding.originatingTurnId) {
      try {
        await this.rpc.call("turn/steer", {
          threadId: binding.threadId,
          input,
          expectedTurnId: binding.originatingTurnId,
        });
        return "codex-steer";
      } catch (error) {
        if (isRecoverableTurnError(error)) {
          // The originating turn is already gone; starting a new turn is safe.
        } else if (isNonSteerableTurnError(error)) {
          await this.waitForTurnCompletion(binding.threadId, binding.originatingTurnId);
        } else {
          throw error;
        }
      }
    }
    await this.rpc.call("turn/start", { threadId: binding.threadId, input });
    return "codex-start";
  }

  onCorrelation(listener: (correlation: CodexCorrelation) => void): () => void {
    this.correlationListeners.add(listener);
    return () => {
      this.correlationListeners.delete(listener);
    };
  }

  private handleNotification(notification: JsonRpcNotification): void {
    this.resolveTurnWaiters(notification);
    if (notification.method !== "item/completed") return;
    const params = asRecord(notification.params);
    const item = asRecord(params.item);
    if (item.type !== "mcpToolCall") return;
    if (item.status !== "completed") return;
    const tool = typeof item.tool === "string" ? item.tool : "";
    if (!["deepseek_spawn", "deepseek_continue"].includes(tool)) return;
    if (item.server !== "deepseek-subagent") return;
    const result = asRecord(item.result);
    const structuredContent = asRecord(result.structuredContent);
    if (structuredContent.accepted !== true || structuredContent.status !== "accepted") return;
    const jobId = asString(structuredContent.jobId);
    if (!jobId) return;
    const threadId = asString(params.threadId);
    if (!threadId) return;
    const turnId = asString(params.turnId);
    if (!turnId) return;
    const itemId = asString(item.id);
    if (!itemId) return;
    const correlation = { jobId, threadId, turnId, itemId };
    const previousItem = this.itemCorrelations.get(itemId);
    if (previousItem) {
      if (!sameCorrelation(previousItem, correlation)) return;
      return;
    }
    const previous = this.correlations.get(jobId);
    if (previous) {
      if (!sameCorrelation(previous, correlation)) return;
      return;
    }
    this.correlations.set(jobId, correlation);
    this.itemCorrelations.set(itemId, correlation);
    for (const listener of this.correlationListeners) listener(correlation);
  }

  private async waitForTurnCompletion(threadId: string, turnId: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const waiter: TurnWaiter = {
        threadId,
        turnId,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.turnWaiters.delete(waiter);
          reject(new Error("Codex turn did not complete before delivery timeout"));
        }, 30_000),
      };
      waiter.timer.unref?.();
      this.turnWaiters.add(waiter);
    });
  }

  private resolveTurnWaiters(notification: JsonRpcNotification): void {
    if (!isTurnCompletionNotification(notification.method)) return;
    const params = asRecord(notification.params);
    const nestedTurn = asRecord(params.turn);
    const threadId = asString(params.threadId) ?? asString(nestedTurn.threadId);
    const turnId = asString(params.turnId) ?? asString(nestedTurn.id);
    if (!threadId || !turnId) return;
    for (const waiter of [...this.turnWaiters]) {
      if (waiter.threadId !== threadId || waiter.turnId !== turnId) continue;
      clearTimeout(waiter.timer);
      this.turnWaiters.delete(waiter);
      waiter.resolve();
    }
  }
}

interface TurnWaiter {
  threadId: string;
  turnId: string;
  resolve: () => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? value as Record<string, unknown> : {};
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function isWebSocketEndpoint(value: string | null): value is string {
  return typeof value === "string" && value.startsWith("ws://");
}

function sameCorrelation(left: CodexCorrelation, right: CodexCorrelation): boolean {
  return left.jobId === right.jobId &&
    left.threadId === right.threadId &&
    left.turnId === right.turnId &&
    left.itemId === right.itemId;
}

function isRecoverableTurnError(error: unknown): boolean {
  const message = redactSecrets(String(error)).toLowerCase();
  return message.includes("no active turn") ||
    message.includes("turn") && (message.includes("not found") || message.includes("already completed"));
}

function isNonSteerableTurnError(error: unknown): boolean {
  const message = redactSecrets(String(error)).toLowerCase();
  return message.includes("non-steerable") ||
    message.includes("not steerable") ||
    message.includes("cannot steer") ||
    message.includes("can't steer");
}

function isTurnCompletionNotification(method: string): boolean {
  const normalized = method.toLowerCase();
  return normalized.startsWith("turn/") &&
    ["completed", "ended", "finished", "aborted", "failed"].some((suffix) => normalized.endsWith("/" + suffix));
}
