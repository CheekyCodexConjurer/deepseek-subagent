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

export const CANONICAL_BATCH_SPAWN_TOOLS = ["subagents_spawn_batch"] as const;
export const TRANSITION_BATCH_SPAWN_TOOLS = ["deepseek_spawn_batch"] as const;
export const ACCEPTED_BATCH_SPAWN_TOOLS = new Set<string>([...CANONICAL_BATCH_SPAWN_TOOLS, ...TRANSITION_BATCH_SPAWN_TOOLS]);

export class CodexAppServerDeliveryAdapter implements CodexDeliveryAdapter {
  readonly available = true;
  readonly reason = null;
  readonly capabilities: CodexCapabilities = { ...DEFAULT_CODEX_CAPABILITIES };
  private readonly rpc: CodexRpcTransport;
  private readonly correlationListeners = new Set<(correlation: CodexCorrelation) => void>();
  private readonly correlations = new Map<string, CodexCorrelation>();
  private readonly itemCorrelations = new Map<string, CodexCorrelation>();
  private readonly itemRecords = new Map<string, { threadId: string; turnId: string; isBatch: boolean; batchId: string | null; jobIds: Set<string> }>();
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
    this.itemRecords.clear();
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
    const itemType = typeof item.type === "string" ? item.type.toLowerCase() : "";
    if (itemType !== "mcptoolcall") return;
    const itemStatus = typeof item.status === "string" ? item.status.toLowerCase() : "";
    if (itemStatus !== "completed") return;
    const tool = typeof item.tool === "string" ? item.tool : "";
    const isUnary = ACCEPTED_SPAWN_TOOLS.has(tool);
    const isBatch = ACCEPTED_BATCH_SPAWN_TOOLS.has(tool);
    if (!isUnary && !isBatch) return;
    const server = typeof item.server === "string" ? item.server.toLowerCase() : "";
    if (!ACCEPTED_SERVER_NAMES.has(server)) return;

    const threadId = asString(params.threadId);
    if (!threadId) return;
    const turnId = asString(params.turnId);
    if (!turnId) return;
    const itemId = asString(item.id);
    if (!itemId) return;

    const result = asRecord(item.result);
    const structuredContent = asRecord(result.structuredContent);
    const metaTechnical = asRecord(asRecord(result._meta).technical);

    const hasStructured = Object.keys(structuredContent).length > 0;
    const hasMeta = Object.keys(metaTechnical).length > 0;
    if (!hasStructured && !hasMeta) return;

    // Fail-closed: reject if either source explicitly signals rejection
    if (
      isExplicitlyRejected(structuredContent.status, structuredContent.accepted) ||
      isExplicitlyRejected(metaTechnical.status, metaTechnical.accepted)
    ) {
      return;
    }

    // Fail-closed: reject contradictory accepted booleans between sources
    if (
      typeof structuredContent.accepted === "boolean" &&
      typeof metaTechnical.accepted === "boolean" &&
      structuredContent.accepted !== metaTechnical.accepted
    ) {
      return;
    }

    const jobIds: string[] = [];
    let batchId: string | null = null;

    if (isUnary) {
      const scAccepted = structuredContent.accepted === true &&
        (typeof structuredContent.status === "string" ? structuredContent.status.toLowerCase() === "accepted" : false);
      const metaAccepted = metaTechnical.accepted === true &&
        (typeof metaTechnical.status === "string" ? metaTechnical.status.toLowerCase() === "accepted" : metaTechnical.status === undefined);

      if (!scAccepted && !metaAccepted) return;

      const scJobId = asString(structuredContent.jobId);
      const metaJobId = asString(metaTechnical.jobId);

      // Contradictory identity between sources
      if (scJobId && metaJobId && scJobId !== metaJobId) return;

      // Identity must come only from an accepted source, never mixing identity from a rejected/unaccepted source
      const jobId = scAccepted ? (scJobId ?? (metaAccepted ? metaJobId : null)) : (metaAccepted ? metaJobId : null);
      if (!jobId) return;
      jobIds.push(jobId);
    } else if (isBatch) {
      const scBatchAccepted = structuredContent.accepted === true;
      const metaBatchAccepted = metaTechnical.accepted === true;
      if (!scBatchAccepted && !metaBatchAccepted) return;

      const scBatchId = asString(structuredContent.batchId) ?? asString(structuredContent.batch_id);
      const metaBatchId = asString(metaTechnical.batchId) ?? asString(metaTechnical.batch_id);
      if (scBatchId && metaBatchId && scBatchId !== metaBatchId) return;
      batchId = scBatchId ?? metaBatchId ?? null;

      const hasScItems = Array.isArray(structuredContent.items);
      const hasMetaItems = Array.isArray(metaTechnical.items);
      const hasItems = hasScItems || hasMetaItems;

      if (hasItems) {
        const scMap = parseBatchItemsMap(structuredContent.items);
        const metaMap = parseBatchItemsMap(metaTechnical.items);

        // Check cross-source consistency when both provide items
        if (scMap && metaMap) {
          if (scMap.size !== metaMap.size) return;
          for (const [id, scItem] of scMap) {
            const metaItem = metaMap.get(id);
            if (!metaItem) return;
            const scRej = isExplicitlyRejected(scItem.status, scItem.accepted);
            const metaRej = isExplicitlyRejected(metaItem.status, metaItem.accepted);
            if (scRej !== metaRej) return;
            if (scItem.accepted !== undefined && metaItem.accepted !== undefined && scItem.accepted !== metaItem.accepted) {
              return;
            }
            if (scItem.status !== undefined && metaItem.status !== undefined && scItem.status.toLowerCase() !== metaItem.status.toLowerCase()) {
              return;
            }
          }
        }

        const combinedMap = scMap ?? metaMap ?? new Map<string, RawBatchItem>();
        if (scMap && metaMap) {
          for (const [id, item] of metaMap) {
            if (!combinedMap.has(id)) {
              combinedMap.set(id, item);
            }
          }
        }

        for (const [id, itemRec] of combinedMap) {
          if (isExplicitlyRejected(itemRec.status, itemRec.accepted)) {
            continue;
          }
          const normStatus = typeof itemRec.status === "string" ? itemRec.status.toLowerCase() : undefined;
          const isAccepted = itemRec.accepted === true || normStatus === "accepted" || normStatus === "queued" ||
            (itemRec.accepted === undefined && normStatus === undefined);
          if (isAccepted && !jobIds.includes(id)) {
            jobIds.push(id);
          }
        }

        // When items are present, do NOT fall back to raw jobIds arrays!
        if (jobIds.length === 0) return;
      } else {
        // Fallback to jobIds arrays ONLY when no items array was provided
        const scJobIdArr = extractStringArray(structuredContent.jobIds) ?? extractStringArray(structuredContent.job_ids);
        const metaJobIdArr = extractStringArray(metaTechnical.jobIds) ?? extractStringArray(metaTechnical.job_ids);

        if (scJobIdArr && metaJobIdArr) {
          if (scJobIdArr.length !== metaJobIdArr.length || !scJobIdArr.every((id, idx) => id === metaJobIdArr[idx])) {
            return;
          }
        }

        const sourceArr = scJobIdArr ?? metaJobIdArr ?? [];
        for (const id of sourceArr) {
          if (!jobIds.includes(id)) {
            jobIds.push(id);
          }
        }
        if (jobIds.length === 0) return;
      }
    }

    const previousItem = this.itemRecords.get(itemId);
    if (previousItem) {
      if (previousItem.threadId !== threadId || previousItem.turnId !== turnId) {
        return;
      }
      if (previousItem.isBatch !== isBatch) {
        return;
      }
      if (previousItem.batchId !== batchId) {
        return;
      }
      if (previousItem.jobIds.size !== jobIds.length) {
        return;
      }
      for (const id of jobIds) {
        if (!previousItem.jobIds.has(id)) {
          return;
        }
      }
      // Exact replay of already accepted immutable set: reject duplicate emission
      return;
    }

    for (const jobId of jobIds) {
      const candidate: CodexCorrelation = { jobId, threadId, turnId, itemId };
      const previousJob = this.correlations.get(jobId);
      if (previousJob && !sameCorrelation(previousJob, candidate)) {
        return;
      }
    }

    const itemRec = {
      threadId,
      turnId,
      isBatch,
      batchId,
      jobIds: new Set<string>(jobIds),
    };
    this.itemRecords.set(itemId, itemRec);

    const toEmit: CodexCorrelation[] = [];
    for (const jobId of jobIds) {
      const correlation: CodexCorrelation = { jobId, threadId, turnId, itemId };
      this.correlations.set(jobId, correlation);
      this.itemCorrelations.set(itemId, correlation);
      toEmit.push(correlation);
    }

    for (const correlation of toEmit) {
      for (const listener of this.correlationListeners) {
        listener(correlation);
      }
    }
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

interface RawBatchItem {
  jobId: string;
  accepted?: boolean | undefined;
  status?: string | undefined;
}

function parseBatchItemsMap(arr: unknown): Map<string, RawBatchItem> | null {
  if (!Array.isArray(arr)) return null;
  const map = new Map<string, RawBatchItem>();
  for (const it of arr) {
    if (it && typeof it === "object") {
      const rec = it as Record<string, unknown>;
      const id = asString(rec.jobId) ?? asString(rec.job_id);
      if (!id) continue;
      map.set(id, {
        jobId: id,
        accepted: typeof rec.accepted === "boolean" ? rec.accepted : undefined,
        status: typeof rec.status === "string" ? rec.status : undefined,
      });
    } else if (typeof it === "string") {
      const id = asString(it);
      if (id) {
        map.set(id, { jobId: id });
      }
    }
  }
  return map;
}

function extractStringArray(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null;
  const result: string[] = [];
  for (const raw of value) {
    const id = asString(raw);
    if (id && !result.includes(id)) {
      result.push(id);
    }
  }
  return result;
}

function isExplicitlyRejected(status: unknown, accepted: unknown): boolean {
  if (accepted === false) return true;
  if (typeof status === "string") {
    const s = status.toLowerCase();
    if (s === "rejected" || s === "failed" || s === "error") return true;
  }
  return false;
}
