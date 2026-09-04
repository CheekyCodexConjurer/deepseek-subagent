import { JsonRpcStdioClient, type JsonRpcNotification } from "./jsonrpc.js";
import { JsonRpcWebSocketClient } from "./websocket.js";
import { redactSecrets } from "../security.js";
import { ConflictError } from "../errors.js";
import type { BridgeConfig, CodexBinding, CodexCapabilities, JobRecord, WakeEnvelope } from "../types.js";

export interface CodexRpcTransport {
  start(command: string, args: string[]): Promise<void>;
  onNotification(listener: (notification: JsonRpcNotification) => void): () => void;
  call(method: string, params: unknown): Promise<unknown>;
  close(): Promise<void>;
  readonly serverCapabilities?: Record<string, unknown>;
}

export interface CodexCorrelation {
  jobId: string;
  threadId: string;
  turnId: string;
  itemId: string;
}

export const DEFAULT_CODEX_CAPABILITIES: CodexCapabilities = {
  supportsSteer: true,
  supportsStartTurn: true,
  supportsToolOutput: false,
  authoritativeAttachment: false,
  supportsGoalPauseResume: false,
};

export interface CodexDeliveryAdapter {
  readonly available: boolean;
  readonly reason: string | null;
  readonly capabilities: CodexCapabilities;
  start(): Promise<void>;
  close(): Promise<void>;
  deliver(job: JobRecord, binding: CodexBinding, text: string): Promise<"codex-steer" | "codex-start">;
  deliverWake(envelope: WakeEnvelope, binding: CodexBinding): Promise<"codex-steer" | "codex-start">;
  reconcileSend(threadId: string, marker: string, clientUserMessageId?: string | null): Promise<boolean>;
  onCorrelation(listener: (correlation: CodexCorrelation) => void): () => void;
}

export class UnavailableCodexDeliveryAdapter implements CodexDeliveryAdapter {
  readonly available = false;
  readonly capabilities: CodexCapabilities = {
    supportsSteer: false,
    supportsStartTurn: false,
    supportsToolOutput: false,
    authoritativeAttachment: false,
    supportsGoalPauseResume: false,
  };
  constructor(readonly reason = "No compatible Codex App Server connection is configured") {}
  async start(): Promise<void> {}
  async close(): Promise<void> {}
  async deliver(_job: JobRecord, _binding: CodexBinding, _text: string): Promise<"codex-steer" | "codex-start"> {
    throw new Error(this.reason);
  }
  async deliverWake(_envelope: WakeEnvelope, _binding: CodexBinding): Promise<"codex-steer" | "codex-start"> {
    throw new Error(this.reason);
  }
  async reconcileSend(_threadId: string, _marker: string): Promise<boolean> {
    return false;
  }
  onCorrelation(_listener: (correlation: CodexCorrelation) => void): () => void {
    return () => undefined;
  }
}

export const CANONICAL_SERVER_NAMES = ["subagents", "subagents-mcp"] as const;
export const TRANSITION_SERVER_NAMES = ["deepseek-subagent", "deepseek_subagent"] as const;
export const ACCEPTED_SERVER_NAMES = new Set<string>([...CANONICAL_SERVER_NAMES, ...TRANSITION_SERVER_NAMES]);

export const CANONICAL_SPAWN_TOOLS = ["subagents_spawn", "subagents_continue"] as const;
export const TRANSITION_SPAWN_TOOLS = ["deepseek_spawn", "deepseek_continue"] as const;
export const ACCEPTED_SPAWN_TOOLS = new Set<string>([...CANONICAL_SPAWN_TOOLS, ...TRANSITION_SPAWN_TOOLS]);

export class CodexAppServerDeliveryAdapter implements CodexDeliveryAdapter {
  readonly available = true;
  readonly reason = null;
  readonly capabilities: CodexCapabilities = { ...DEFAULT_CODEX_CAPABILITIES };
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
    if (isWebSocketEndpoint(config.codexAppServerSocket)) {
      this.capabilities.authoritativeAttachment = true;
    }
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
    const caps = this.rpc.serverCapabilities;
    if (caps && typeof caps === "object") {
      if (caps.toolOutput === true) {
        this.capabilities.supportsToolOutput = true;
      }
      if (caps.authoritativeAttachment === true || caps.originatingThreadAuthority === true) {
        this.capabilities.authoritativeAttachment = true;
      }
      if (caps.goalPauseResume === true) {
        this.capabilities.supportsGoalPauseResume = true;
      }
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

  async deliverWake(envelope: WakeEnvelope, binding: CodexBinding): Promise<"codex-steer" | "codex-start"> {
    if (!this.started) throw new Error("Codex App Server adapter is not started");

    const textLines = [
      envelope.marker,
      `[SubAgent Bridge Wake Notice] Park ID: ${envelope.parkId} (generation ${envelope.generation})`,
      `Reason: ${envelope.reason}`,
      `Ready jobs: ${envelope.readyJobIds.join(", ")}`,
      `Statuses: ${JSON.stringify(envelope.statuses)}`,
      `Result hashes: ${JSON.stringify(envelope.resultHashes)}`,
      `Pending count: ${envelope.pendingCount}`,
      `Instruction: ${envelope.instruction}`,
    ];
    const text = textLines.join("\n");

    const input: Array<Record<string, unknown>> = [{ type: "text", text }];
    const turnStartPayload: Record<string, unknown> = {
      threadId: binding.threadId,
      input,
    };

    if (this.capabilities.supportsToolOutput) {
      turnStartPayload.toolOutput = {
        parkId: envelope.parkId,
        generation: envelope.generation,
        readyJobIds: envelope.readyJobIds,
        statuses: envelope.statuses,
      };
    }

    let currentActiveTurnId: string | null = null;
    try {
      const threadInfo = await this.rpc.call("thread/read", { threadId: binding.threadId }).catch(() => null) as { activeTurnId?: string | null } | null;
      if (threadInfo && typeof threadInfo === "object" && typeof threadInfo.activeTurnId === "string") {
        currentActiveTurnId = threadInfo.activeTurnId;
      }
    } catch {
      // continue
    }

    if (currentActiveTurnId) {
      if (binding.originatingTurnId && currentActiveTurnId === binding.originatingTurnId) {
        await this.rpc.call("turn/steer", {
          threadId: binding.threadId,
          input,
          expectedTurnId: binding.originatingTurnId,
        });
        return "codex-steer";
      } else {
        throw new ConflictError(
          `Cannot deliver wake into thread ${binding.threadId}: a different active turn (${currentActiveTurnId}) is in progress. Deferring to remain durably pending.`,
          "active_turn_conflict",
        );
      }
    }

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
          // The originating turn is already completed; thread is idle, starting new turn is safe.
        } else if (isNonSteerableTurnError(error)) {
          await this.waitForTurnCompletion(binding.threadId, binding.originatingTurnId);
        } else {
          throw error;
        }
      }
    }

    await this.rpc.call("turn/start", turnStartPayload);
    return "codex-start";
  }

  async reconcileSend(threadId: string, marker: string, clientUserMessageId?: string | null): Promise<boolean> {
    try {
      const res = await this.rpc.call("thread/read", { threadId }) as { items?: Array<{ id?: string; type?: string; text?: string; content?: unknown }> } | null;
      if (!res || !Array.isArray(res.items)) {
        return false;
      }
      for (const item of res.items) {
        if (clientUserMessageId && item.id === clientUserMessageId) {
          return true;
        }
        if (typeof item.text === "string" && item.text.includes(marker)) {
          return true;
        }
        if (Array.isArray(item.content)) {
          for (const c of item.content) {
            if (c && typeof c === "object" && typeof (c as { text?: unknown }).text === "string" && (c as { text: string }).text.includes(marker)) {
              return true;
            }
          }
        }
      }
      return false;
    } catch {
      return false;
    }
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
    if (!ACCEPTED_SPAWN_TOOLS.has(tool)) return;
    const server = typeof item.server === "string" ? item.server : "";
    if (!ACCEPTED_SERVER_NAMES.has(server)) return;
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
