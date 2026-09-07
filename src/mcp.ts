import { open } from "node:fs/promises";
import { spawn } from "node:child_process";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { defaultConfigPath, loadConfig, saveConfig } from "./config.js";
import { BridgeHttpClient, BridgeHttpError, BridgeTransportError } from "./http-server.js";
import { ConflictError, InvalidRequestError } from "./errors.js";
import { resolveCodexTaskProvenance, type TaskProvenance } from "./codex/cli-resolver.js";
import { canRead, ensurePrivateDir, newId, redactSecrets } from "./security.js";
import type { BridgeConfig } from "./types.js";

const DISPLAY_NAME = "SubAgents MCP";
const LEGACY_DISPLAY_NAME = "DeepSeek Sub-Agent";
const MODEL_DISPLAY = "Antigravity · gemini-3.8-flash-high";
const CANONICAL_SERVER_NAME = "subagents";

export async function runMcp(configPath = defaultConfigPath()): Promise<void> {
  const config = await loadConfig(configPath);
  await ensureMcpConfig(config);
  const client = new BridgeHttpClient(config);
  // The MCP handshake and tool listing must never wait for daemon or worker
  // startup. Daemon readiness is bootstrapped lazily on the first tool call,
  // memoized, and shared by concurrent first operations.
  const server = createMcpServer(client, {
    ensureReady: createLazyDaemonBootstrap(config, client),
  });
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error(DISPLAY_NAME + " server connected; daemon readiness is bootstrapped on the first tool call.");
}

export interface LazyDaemonBootstrap {
  (): Promise<void>;
  invalidate(): void;
  recover(): Promise<void>;
}

/**
 * Returns a memoized single-flight daemon readiness bootstrap with in-band
 * recovery. The first tool operation triggers the daemon health check and,
 * if offline, the one-time recovery start. Concurrent first operations share
 * the same bootstrap. If a post-bootstrap transport loss occurs, recovery
 * invalidates stale readiness, shares a single start attempt across concurrent
 * failures, and retries the original HTTP call once.
 */
export function createLazyDaemonBootstrap(
  config: BridgeConfig,
  client: DaemonHealthClient,
  options: DaemonBootstrapOptions = {},
): LazyDaemonBootstrap {
  let pendingPromise: Promise<void> | null = null;
  let isReady = false;

  const run = (): Promise<void> => {
    if (isReady) return Promise.resolve();
    if (pendingPromise) return pendingPromise;
    pendingPromise = (async () => {
      try {
        await ensureDaemonRunning(config, client, options);
        isReady = true;
      } catch (error) {
        isReady = false;
        throw new Error("SubAgents MCP daemon is not ready: " + redactSecrets(String(error)));
      } finally {
        pendingPromise = null;
      }
    })();
    return pendingPromise;
  };

  const invalidate = (): void => {
    isReady = false;
  };

  const recover = (): Promise<void> => {
    invalidate();
    return run();
  };

  const bootstrap: LazyDaemonBootstrap = Object.assign(() => run(), {
    invalidate,
    recover,
  });

  return bootstrap;
}

class LazyReadyClient {
  private readonly recoverFn: (() => Promise<void>) | undefined;

  constructor(
    private readonly client: Pick<BridgeHttpClient, "call">,
    private readonly ensureReady: () => Promise<void>,
    recover?: () => Promise<void>,
  ) {
    this.recoverFn = recover
      ?? (typeof (ensureReady as { recover?: unknown }).recover === "function"
        ? () => (ensureReady as LazyDaemonBootstrap).recover()
        : undefined);
  }

  async call<T>(pathname: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    await this.ensureReady();
    try {
      return await this.client.call<T>(pathname, body, signal);
    } catch (error) {
      if (error instanceof BridgeTransportError && this.recoverFn) {
        await this.recoverFn();
        return await this.client.call<T>(pathname, body, signal);
      }
      throw error;
    }
  }
}

export interface DaemonHealthClient {
  health(): Promise<unknown>;
}

export interface DaemonBootstrapOptions {
  start?: (config: BridgeConfig) => Promise<void>;
  timeoutMs?: number;
  retryMs?: number;
}

export function isDaemonHealthReady(health: unknown): boolean {
  if (!health || typeof health !== "object") return false;
  const h = health as Record<string, unknown>;
  if (h.ready === true) return true;
  if (h.state === "ready") return true;
  const status = h.status;
  if (status && typeof status === "object") {
    const s = status as Record<string, unknown>;
    if (s.ready === true) return true;
    if (s.state === "ready") return true;
    if (s.running === true && s.ready === undefined && s.state === undefined) return true;
  }
  return false;
}

export function isDaemonHealthDegraded(health: unknown): boolean {
  if (!health || typeof health !== "object") return false;
  const h = health as Record<string, unknown>;
  if (h.state === "degraded") return true;
  const status = h.status;
  if (status && typeof status === "object") {
    const s = status as Record<string, unknown>;
    if (s.state === "degraded") return true;
  }
  return false;
}

export function getDaemonHealthState(health: unknown): string | null {
  if (!health || typeof health !== "object") return null;
  const h = health as Record<string, unknown>;
  if (typeof h.state === "string") return h.state;
  const status = h.status;
  if (status && typeof status === "object") {
    const s = status as Record<string, unknown>;
    if (typeof s.state === "string") return s.state;
  }
  return null;
}

export function getDaemonHealthError(health: unknown): string {
  if (!health || typeof health !== "object") return "unknown error";
  const h = health as Record<string, unknown>;
  if (typeof h.error === "string") return h.error;
  const status = h.status;
  if (status && typeof status === "object") {
    const s = status as Record<string, unknown>;
    if (typeof s.error === "string") return s.error;
  }
  return "degraded state";
}

/**
 * MCP startup is allowed to recover the local daemon once. This is readiness
 * handling, not a job-status polling loop: the MCP process only waits for the
 * bridge HTTP endpoint before exposing tools.
 */
export async function ensureDaemonRunning(
  config: BridgeConfig,
  client: DaemonHealthClient,
  options: DaemonBootstrapOptions = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? Math.max(10_000, Math.min(45_000, config.opencodeStartupTimeoutMs + 5_000));
  const retryMs = options.retryMs ?? 100;
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = null;

  let initialReachable = false;
  try {
    const health = await client.health();
    if (isDaemonHealthReady(health)) {
      return;
    }
    initialReachable = true;
    if (isDaemonHealthDegraded(health)) {
      throw new Error("SubAgents MCP daemon is degraded: " + getDaemonHealthError(health));
    }
    lastError = new Error("Daemon is " + (getDaemonHealthState(health) || "not ready"));
  } catch (error) {
    lastError = error;
    if (initialReachable) throw error;
  }

  if (!initialReachable) {
    await (options.start ?? startDetachedDaemon)(config);
  }

  while (Date.now() < deadline) {
    try {
      const health = await client.health();
      if (isDaemonHealthReady(health)) {
        return;
      }
      if (isDaemonHealthDegraded(health)) {
        throw new Error("SubAgents MCP daemon is degraded: " + getDaemonHealthError(health));
      }
      lastError = new Error("Daemon is " + (getDaemonHealthState(health) || "not ready"));
    } catch (error) {
      if (error instanceof Error && error.message.includes("daemon is degraded")) {
        throw error;
      }
      lastError = error;
    }
    await delay(Math.min(retryMs, Math.max(1, deadline - Date.now())));
  }
  throw new Error("SubAgents MCP daemon did not become ready: " + redactSecrets(String(lastError)));
}

async function ensureMcpConfig(config: BridgeConfig): Promise<void> {
  await ensurePrivateDir(config.dataDir);
  if (!(await canRead(config.configPath))) await saveConfig(config);
}

async function startDetachedDaemon(config: BridgeConfig): Promise<void> {
  const script = process.argv[1];
  if (!script) throw new Error("Unable to resolve CLI script for daemon startup");
  await ensurePrivateDir(config.dataDir);
  const logHandle = await open(path.join(config.dataDir, "daemon.log"), "a");
  try {
    const child = spawn(process.execPath, [script, "daemon", "--config", config.configPath], {
      detached: true,
      stdio: ["ignore", logHandle.fd, logHandle.fd],
      windowsHide: true,
      shell: false,
    });
    child.once("error", () => undefined);
    child.unref();
  } finally {
    await logHandle.close();
  }
}

export interface McpServerOptions {
  ensureReady?: LazyDaemonBootstrap | (() => Promise<void>);
  name?: string;
  env?: Record<string, string | undefined>;
  provenanceResolver?: () => TaskProvenance;
}

export function createMcpServer(
  client: BridgeHttpClient,
  options: McpServerOptions = {},
): McpServer {
  const mcpProcessSessionId = `mcp-proc-${process.pid}-${newId("sess")}`;
  const taskProvenance = options.provenanceResolver
    ? options.provenanceResolver()
    : resolveCodexTaskProvenance(options.env ?? process.env);
  const server = new McpServer({
    name: options.name ?? CANONICAL_SERVER_NAME,
    title: DISPLAY_NAME,
    version: "0.1.0",
  });
  const readyClient = options.ensureReady ? new LazyReadyClient(client, options.ensureReady) : client;

  const spawnInputSchema = {
    request_id: z.string().min(1).optional(),
    topic: z.string().min(1).max(240),
    task: z.string().min(1),
    cwd: z.string().optional(),
    mode: z.enum(["analyze", "edit", "test"]).optional(),
    workspace_strategy: z.enum(["shared", "worktree"]).optional(),
    context_files: z.array(z.string()).optional(),
    visual_context: z.string().optional(),
    priority: z.number().int().min(1).max(100).optional(),
    exclusive_resources: z.array(z.string().min(1)).optional(),
    thread_id: z.string().optional(),
    turn_id: z.string().optional(),
  };

  const spawnBatchItemSchema = z.object({
    request_id: z.string().min(1).optional(),
    topic: z.string().min(1).max(240).optional(),
    task: z.string().min(1),
    cwd: z.string().optional(),
    mode: z.enum(["analyze", "edit", "test"]).optional(),
    workspace_strategy: z.enum(["shared", "worktree"]).optional(),
    context_files: z.array(z.string()).optional(),
    visual_context: z.string().optional(),
    priority: z.number().int().min(1).max(100).optional(),
    exclusive_resources: z.array(z.string().min(1)).optional(),
    thread_id: z.string().optional(),
    turn_id: z.string().optional(),
  });

  const spawnBatchInputSchema = {
    batch_request_id: z.string().min(1).optional(),
    items: z.array(spawnBatchItemSchema).min(1),
  };

  const batchItemReceiptSchema = z.object({
    jobId: z.string(),
    agentId: z.string(),
    requestId: z.string().optional(),
    status: z.string(),
  });

  const spawnBatchOutputSchema = {
    accepted: z.boolean(),
    batchId: z.string(),
    batchRequestId: z.string(),
    items: z.array(batchItemReceiptSchema),
    jobIds: z.array(z.string()),
    obligationState: z.literal("pending"),
    nextRequiredAction: z.string(),
    capabilities: z.record(z.string(), z.boolean()).optional(),
  };

  const continueInputSchema = {
    request_id: z.string().min(1).optional(),
    agent_id: z.string().min(1),
    relation: z.enum(["clarification", "correction", "review", "continuation"]).default("continuation"),
    task: z.string().min(1),
    visual_context: z.string().optional(),
    thread_id: z.string().optional(),
    turn_id: z.string().optional(),
    permission_id: z.string().optional(),
    permission_reply: z.enum(["once", "always", "reject"]).optional(),
    permission_message: z.string().max(2_000).optional(),
    allow_respawn: z.boolean().optional(),
  };

  const consultInputSchema = {
    agent_id: z.string().min(1),
    job_id: z.string().min(1).optional(),
    activity_limit: z.number().int().min(1).max(20).default(10),
  };

  const followInputSchema = {
    agent_id: z.string().min(1),
    job_id: z.string().min(1).optional(),
    wait_minutes: z.number().int().min(1).max(60).optional(),
    grace_minutes: z.number().int().min(1).max(10).optional(),
  };

  const abortInputSchema = {
    agent_id: z.string().min(1),
    reason: z.string().max(500).optional(),
  };

  const closeInputSchema = {
    agent_id: z.string().min(1),
  };

  const recoverInputSchema = {
    agent_id: z.string().min(1),
    job_id: z.string().min(1),
  };

  const parkInputSchema = {
    job_ids: z.array(z.string().min(1)).optional(),
    jobIds: z.array(z.string().min(1)).optional(),
    job_id: z.string().min(1).optional(),
    jobId: z.string().min(1).optional(),
    park_id: z.string().min(1).optional(),
    parkId: z.string().min(1).optional(),
    thread_id: z.string().optional(),
    threadId: z.string().optional(),
    turn_id: z.string().optional(),
    turnId: z.string().optional(),
    goal_id: z.string().optional(),
    goalId: z.string().optional(),
    reason: z.string().max(500).optional(),
    wait: z.boolean().optional(),
    delivery_mode: z.enum(["queued", "cli_resume", "in_turn", "none"]).optional(),
    deliveryMode: z.enum(["queued", "cli_resume", "in_turn", "none"]).optional(),
    predicate: z.enum(["ALL", "ANY", "QUORUM", "REQUIRED"]).optional(),
    predicate_type: z.enum(["ALL", "ANY", "QUORUM", "REQUIRED"]).optional(),
    predicateType: z.enum(["ALL", "ANY", "QUORUM", "REQUIRED"]).optional(),
    quorum_count: z.number().int().min(1).optional(),
    quorumCount: z.number().int().min(1).optional(),
    required_job_ids: z.array(z.string().min(1)).optional(),
    requiredJobIds: z.array(z.string().min(1)).optional(),
    wake_on_exception: z.boolean().optional(),
    wakeOnException: z.boolean().optional(),
    queue_message_id: z.string().min(1).optional(),
    queueMessageId: z.string().min(1).optional(),
    message_id: z.string().min(1).optional(),
    messageId: z.string().min(1).optional(),
  };

  function normalizeParkArgs(args: Record<string, unknown>): Record<string, unknown> {
    const result: Record<string, unknown> = { ...args };
    const rawJobIds = args.job_ids ?? args.jobIds;
    if (rawJobIds !== undefined) {
      result.job_ids = rawJobIds;
      result.jobIds = rawJobIds;
    }
    const rawJobId = args.job_id ?? args.jobId;
    if (rawJobId !== undefined) {
      result.job_id = rawJobId;
      result.jobId = rawJobId;
    }
    const rawParkId = args.park_id ?? args.parkId;
    if (rawParkId !== undefined) {
      result.park_id = rawParkId;
      result.parkId = rawParkId;
    }
    const rawThreadId = args.thread_id ?? args.threadId;
    if (rawThreadId !== undefined) {
      result.thread_id = rawThreadId;
      result.threadId = rawThreadId;
    }
    const rawTurnId = args.turn_id ?? args.turnId;
    if (rawTurnId !== undefined) {
      result.turn_id = rawTurnId;
      result.turnId = rawTurnId;
    }
    const rawGoalId = args.goal_id ?? args.goalId;
    if (rawGoalId !== undefined) {
      result.goal_id = rawGoalId;
      result.goalId = rawGoalId;
    }
    const rawPred = args.predicate ?? args.predicate_type ?? args.predicateType;
    if (rawPred !== undefined) {
      result.predicate = rawPred;
      result.predicate_type = rawPred;
      result.predicateType = rawPred;
    }
    const rawQuorum = args.quorum_count ?? args.quorumCount;
    if (rawQuorum !== undefined) {
      result.quorum_count = rawQuorum;
      result.quorumCount = rawQuorum;
    }
    const rawReq = args.required_job_ids ?? args.requiredJobIds;
    if (rawReq !== undefined) {
      result.required_job_ids = rawReq;
      result.requiredJobIds = rawReq;
    }
    const rawWake = args.wake_on_exception ?? args.wakeOnException;
    if (rawWake !== undefined) {
      result.wake_on_exception = rawWake;
      result.wakeOnException = rawWake;
    }
    const rawDeliveryMode = args.delivery_mode ?? args.deliveryMode;
    if (rawDeliveryMode !== undefined) {
      result.delivery_mode = rawDeliveryMode;
      result.deliveryMode = rawDeliveryMode;
    }
    const rawQueueMessageId = args.queue_message_id ?? args.queueMessageId ?? args.message_id ?? args.messageId;
    if (rawQueueMessageId !== undefined) {
      result.queue_message_id = rawQueueMessageId;
      result.queueMessageId = rawQueueMessageId;
    }
    return result;
  }

  function validateCallerThread(callerThreadId?: string): void {
    if (callerThreadId) {
      if (taskProvenance.error) {
        throw new ConflictError(
          `Caller thread_id "${callerThreadId}" cannot be verified: invalid Codex task provenance (${taskProvenance.error})`,
          "identity_mismatch",
        );
      }
      if (taskProvenance.threadId && callerThreadId !== taskProvenance.threadId) {
        throw new ConflictError(
          `Caller thread_id "${callerThreadId}" does not match trusted Codex task identity "${taskProvenance.threadId}"`,
          "identity_mismatch",
        );
      }
    }
  }

  const trustedThreadId = taskProvenance.threadId ?? undefined;

  // --- Canonical SubAgents MCP Surface ---

  server.registerTool("subagents_spawn", {
    title: DISPLAY_NAME + " · Spawn",
    description: "Start one asynchronous task in a new managed session on the bridge's active model route. Return immediately after acceptance; do not poll. Accepted is not a result: acceptance creates a pending obligation — consume the job with subagents_follow before a dependent gate or a final response, or explicitly end it with subagents_abort or subagents_close. Do not duplicate this delegated front locally; you may orchestrate other fronts in parallel while it is pending. The bridge, not the caller, selects and pins the active model route at spawn. Changing routes is an operator-only control-plane action; ordinary MCP callers must never send a remembered/default route name. When the task depends on visual material, inspect the visuals yourself first and send a compact textual visual_context (string, optional, no default) with three labeled parts, 'Direct observations:', 'Interpretation:' and 'Uncertainty:'. Send only your textual interpretation; the worker never receives pixels. Treat direct observations as evidence, interpretation as a hypothesis, and never invent visual details absent from the context.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: spawnInputSchema,
    outputSchema: {
      accepted: z.boolean(),
      status: z.string(),
      topic: z.string(),
      modelDisplayName: z.string(),
      agentId: z.string(),
      jobId: z.string(),
      state: z.string(),
      obligationState: z.literal("pending"),
      nextRequiredAction: z.literal("subagents_follow"),
      priority: z.number().optional(),
      exclusiveResources: z.array(z.string()).optional(),
    },
  }, async (args, extra) => {
    try {
      validateCallerThread(args.thread_id);
      const payload = {
        ...args,
        request_id: args.request_id ?? newId("request"),
        mcp_session_id: mcpProcessSessionId,
        ...(trustedThreadId ? { trusted_thread_id: trustedThreadId } : {}),
      };
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/spawn", payload, extra?.signal);
      return acceptedResult(result, false);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("subagents_spawn_batch", {
    title: DISPLAY_NAME + " · Spawn Batch",
    description: "Start a batch of asynchronous tasks with atomic admission into the swarm queue. Returns immediately after acceptance; do not poll. Accepted is not a result: acceptance creates pending obligations — consume the jobs with subagents_follow before a dependent gate or a final response, or use subagents_park to wait across the batch. Items specify stable request_id, bounded priority (1-100), and generic exclusive_resources. The bridge pins the active model route at admission; no provider fallback.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: spawnBatchInputSchema,
    outputSchema: spawnBatchOutputSchema,
  }, async (args, extra) => {
    try {
      for (const item of args.items) {
        validateCallerThread(item.thread_id);
      }
      const payload = {
        batch_request_id: args.batch_request_id ?? newId("batch_req"),
        items: args.items.map((item) => ({
          ...item,
          ...(item.request_id ? { request_id: item.request_id } : {}),
          mcp_session_id: mcpProcessSessionId,
          ...(trustedThreadId ? { trusted_thread_id: trustedThreadId } : {}),
        })),
      };
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/spawn-batch", payload, extra?.signal);
      return acceptedBatchResult(result, false);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("subagents_continue", {
    title: DISPLAY_NAME + " · Continue",
    description: "Continue an existing agent after reviewing its delivered result. Asynchronous; returns immediately; do not poll. Accepted is not a result: acceptance creates a pending obligation — consume the job with subagents_follow before a dependent gate or a final response, or explicitly end it with subagents_abort or subagents_close. An open agent continues in its same managed session. Reject or wait if the agent is busy; use subagents_abort to stop it. A closed agent is not continuable: set allow_respawn=true only as an explicit recovery when the agent was closed AFTER a terminal job with a persisted result and was NOT explicitly aborted — the bridge then accepts automatically by spawning a NEW agent and a NEW managed session in the same persisted workspace, topic, workspace strategy and pinned model route, records the lineage (parent_agent_id and auditable activity on both agents), preserves or derives the correlation thread/turn hints, and returns the NEW agentId/jobId to follow; it never claims the closed session is the same session and never reopens the closed agent. allow_respawn is rejected (typed 409/400) for aborted agents, closed agents without a persisted result, busy agents, permission-field answers and any scope change: the child inherits only the parent's persisted identity and workspace, with no provider fallback and no live-config route. For an explicit permission response on an open agent, also provide permission_id and permission_reply (once, always, or reject). When the continuation depends on visual material, inspect the visuals yourself first and send a compact textual visual_context (string, optional, no default) with three labeled parts, 'Direct observations:', 'Interpretation:' and 'Uncertainty:'. Send only your textual interpretation; the worker never receives pixels. Treat direct observations as evidence, interpretation as a hypothesis, and never invent visual details absent from the context.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: continueInputSchema,
    outputSchema: {
      accepted: z.boolean(),
      status: z.string(),
      topic: z.string(),
      modelDisplayName: z.string(),
      agentId: z.string(),
      jobId: z.string(),
      state: z.string(),
      obligationState: z.literal("pending"),
      nextRequiredAction: z.literal("subagents_follow"),
    },
  }, async (args, extra) => {
    try {
      validateCallerThread(args.thread_id);
      const payload = {
        ...args,
        request_id: args.request_id ?? newId("request"),
        mcp_session_id: mcpProcessSessionId,
        ...(trustedThreadId ? { trusted_thread_id: trustedThreadId } : {}),
      };
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/continue", payload, extra?.signal);
      return acceptedResult(result, false);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("subagents_status", {
    title: DISPLAY_NAME + " · Status",
    description: "Get one immediate observable progress snapshot for an existing agent. Use only when the user asks for progress, a task is taking unusually long, or the snapshot materially changes the orchestrator's next decision. Do not use repeatedly to wait for completion. Never exposes private reasoning.",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: consultInputSchema,
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/consult", args, extra?.signal);
      return {
        content: [{ type: "text", text: "Observable SubAgents MCP status snapshot returned." }],
        structuredContent: {
          ...result,
          capabilities: { batch_scheduler: true },
        },
      };
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  const receiptOutputSchema = z.object({
    jobId: z.string(),
    agentId: z.string(),
    provider: z.string(),
    model: z.string(),
    status: z.string(),
    workspace: z.string(),
    startedAt: z.string().nullable(),
    completedAt: z.string(),
    durationMs: z.number().nullable(),
    attempt: z.string().nullable(),
    fence: z.number().nullable(),
    outputHash: z.string(),
    quiescent: z.boolean(),
    earlyExit: z.boolean(),
    filesCount: z.number(),
    testsCount: z.number(),
  }).optional();

  const earlyExitOutputSchema = z.object({
    triggered: z.boolean(),
    reason: z.string(),
    confidence: z.union([z.string(), z.number()]).optional(),
    evidenceSnippet: z.string().optional(),
    signaledAt: z.string().optional(),
  }).optional();

  const escalationOutputSchema = z.object({
    targetRole: z.string().optional(),
    recommendedRoute: z.string().optional(),
    reason: z.string(),
    advisoryOnly: z.boolean(),
    suggestedAction: z.string().optional(),
  }).optional();

  const semanticProgressOutputSchema = z.object({
    stage: z.string(),
    percent: z.number().nullable().optional(),
    summary: z.string(),
    milestones: z.array(z.string()).optional(),
    lastActiveAt: z.string().nullable().optional(),
    isStalled: z.boolean().optional(),
    earlyExitTriggered: z.boolean().optional(),
  }).optional();

  server.registerTool("subagents_follow", {
    title: DISPLAY_NAME + " · Follow",
    description: "Wait for a job until it reaches a terminal result, using internal events and one deadline timer without polling. Use it for every job your next decision depends on: before a dependent gate or a final response, and before synthesizing from that front. You may orchestrate other fronts in parallel while a job is pending, but you must consume its result before depending on it; a pending job is not a result, and an unconsumed job leaves an open obligation. Returning a usable terminal result consumes the job obligation explicitly and persistently; a needs_approval follow keeps the obligation pending and requires subagents_continue with permission_id and permission_reply. A terminal follow result closes the job obligation only: the agent stays open and continuable until you close it with subagents_close after reviewing — closing the agent is separate from consuming the obligation. Completed, failed and timed-out agents remain continuable with subagents_continue. The daemon-configured defaults are the worker's minimum window: wait_minutes and grace_minutes below the defaults are raised, and only larger values extend the window; once active, subsequent followers share the existing persisted window. Omit wait_minutes and grace_minutes to use the defaults. When the follow window expires, the worker is gracefully finalized and may be aborted after the grace period.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: followInputSchema,
    outputSchema: {
      agentId: z.string(),
      jobId: z.string(),
      status: z.string(),
      resultAvailable: z.boolean(),
      permissionId: z.string().nullable().optional(),
      message: z.string().optional(),
      obligationState: z.union([z.literal("pending"), z.literal("closed")]),
      nextRequiredAction: z.literal("subagents_continue").optional(),
      receipt: receiptOutputSchema,
      earlyExit: earlyExitOutputSchema,
      escalation: escalationOutputSchema,
      semanticProgress: semanticProgressOutputSchema,
    },
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/follow", args, extra?.signal);
      return followResult(result, false);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("subagents_park", {
    title: DISPLAY_NAME + " · Park",
    description: "Park the current orchestration turn and wait for one or more running jobs to wake it when ready. Returns immediately with an armed receipt if bridge-observed authoritative correlation confirms the target thread, or armed: false if authoritative attachment is not supported or correlated. Parking never consumes a job: the obligation remains pending and you must follow the ready jobs with subagents_follow upon waking.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: parkInputSchema,
    outputSchema: {
      parkId: z.string(),
      generation: z.number(),
      armed: z.boolean(),
      targetIdentity: z.string(),
      obligationState: z.literal("pending"),
      nextRequiredAction: z.literal("subagents_follow"),
      jobIds: z.array(z.string()),
      reason: z.string().nullable().optional(),
      pendingCount: z.number(),
      readyCount: z.number(),
      deliveryMode: z.enum(["queued", "cli_resume", "none"]).optional(),
      delivery_mode: z.enum(["queued", "cli_resume", "none"]).optional(),
      wakeState: z.enum(["waiting", "deferred_active_writer", "delivered", "failed"]).optional(),
      readyJobIds: z.array(z.string()).optional(),
      predicateType: z.enum(["ALL", "ANY", "QUORUM", "REQUIRED"]).optional(),
      quorumCount: z.number().nullable().optional(),
      requiredJobIds: z.array(z.string()).nullable().optional(),
      queueMessageId: z.string().nullable().optional(),
      queue_message_id: z.string().nullable().optional(),
    },
  }, async (args, extra) => {
    try {
      if (args.wait === true) {
        throw new InvalidRequestError(
          "In-turn waiting (wait=true) is no longer supported on park. Use subagents_follow for in-turn waiting, or omit wait for external background parking.",
        );
      }
      validateCallerThread(args.thread_id ?? args.threadId);
      const payload = {
        ...normalizeParkArgs(args),
        wait: false,
        mcp_session_id: mcpProcessSessionId,
        ...(trustedThreadId ? { trusted_thread_id: trustedThreadId } : {}),
      };
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/park", payload, extra?.signal);
      return parkResult(result, false);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("subagents_abort", {
    title: DISPLAY_NAME + " · Abort",
    description: "Stop the active task for an agent and end its pending obligation. This is a control action, not a polling operation.",
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    inputSchema: abortInputSchema,
    outputSchema: {
      agentId: z.string(),
      jobId: z.string().nullable().optional(),
      status: z.string(),
      state: z.string(),
      obligationState: z.literal("closed"),
    },
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/abort", args, extra?.signal);
      return technicalResult(result, "SubAgents MCP task stopped.");
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("subagents_close", {
    title: DISPLAY_NAME + " · Close",
    description: "Close an agent after its work is complete or stopped, ending any pending obligation. It does not delete result history.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: closeInputSchema,
    outputSchema: {
      agentId: z.string(),
      status: z.string(),
      state: z.string(),
      obligationState: z.literal("closed"),
    },
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/close", args, extra?.signal);
      return technicalResult(result, "SubAgents MCP agent closed.");
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("subagents_recover_result", {
    title: DISPLAY_NAME + " · Recover result",
    description: "Recover a persisted asynchronous result after automatic delivery failed or the user explicitly requested recovery. A successful recover returns the usable final result and explicitly consumes the job obligation (persisted), separate from closing the agent. Do not use this as a status poll and never call it repeatedly to check progress.",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: recoverInputSchema,
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<unknown>("/v1/jobs/recover", args, extra?.signal);
      return {
        content: [{ type: "text", text: "Persisted SubAgents MCP result recovered." }],
        structuredContent: { result },
      };
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  // --- Migration Aliases (deepseek_*) ---

  server.registerTool("deepseek_spawn", {
    title: LEGACY_DISPLAY_NAME + " · Spawn",
    description: "Start one asynchronous task in a new managed session on the bridge's active model route. Return immediately after acceptance; do not poll. Accepted is not a result: acceptance creates a pending obligation — consume the job with deepseek_follow before a dependent gate or a final response, or explicitly end it with deepseek_abort or deepseek_close. Do not duplicate this delegated front locally; you may orchestrate other fronts in parallel while it is pending. The bridge, not the caller, selects and pins the active model route at spawn. Changing routes is an operator-only control-plane action; ordinary MCP callers must never send a remembered/default route name. When the task depends on visual material, inspect the visuals yourself first and send a compact textual visual_context (string, optional, no default) with three labeled parts, 'Direct observations:', 'Interpretation:' and 'Uncertainty:'. Send only your textual interpretation; DeepSeek never receives pixels. Treat direct observations as evidence, interpretation as a hypothesis, and never invent visual details absent from the context.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: spawnInputSchema,
    outputSchema: {
      accepted: z.boolean(),
      status: z.string(),
      topic: z.string(),
      modelDisplayName: z.string(),
      agentId: z.string(),
      jobId: z.string(),
      state: z.string(),
      obligationState: z.literal("pending"),
      nextRequiredAction: z.literal("deepseek_follow"),
      priority: z.number().optional(),
      exclusiveResources: z.array(z.string()).optional(),
    },
  }, async (args, extra) => {
    try {
      validateCallerThread(args.thread_id);
      const payload = {
        ...args,
        request_id: args.request_id ?? newId("request"),
        mcp_session_id: mcpProcessSessionId,
        ...(trustedThreadId ? { trusted_thread_id: trustedThreadId } : {}),
      };
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/spawn", payload, extra?.signal);
      return acceptedResult(result, true);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_spawn_batch", {
    title: LEGACY_DISPLAY_NAME + " · Spawn Batch",
    description: "Start a batch of asynchronous DeepSeek tasks with atomic admission into the swarm queue. Returns immediately after acceptance; do not poll. Accepted is not a result: acceptance creates pending obligations — consume the jobs with deepseek_follow before a dependent gate or a final response, or use deepseek_park to wait across the batch. Items specify stable request_id, bounded priority (1-100), and generic exclusive_resources.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: spawnBatchInputSchema,
    outputSchema: spawnBatchOutputSchema,
  }, async (args, extra) => {
    try {
      for (const item of args.items) {
        validateCallerThread(item.thread_id);
      }
      const payload = {
        batch_request_id: args.batch_request_id ?? newId("batch_req"),
        items: args.items.map((item) => ({
          ...item,
          ...(item.request_id ? { request_id: item.request_id } : {}),
          mcp_session_id: mcpProcessSessionId,
          ...(trustedThreadId ? { trusted_thread_id: trustedThreadId } : {}),
        })),
      };
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/spawn-batch", payload, extra?.signal);
      return acceptedBatchResult(result, true);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_continue", {
    title: LEGACY_DISPLAY_NAME + " · Continue",
    description: "Continue an existing DeepSeek agent after reviewing its delivered result. Asynchronous; returns immediately; do not poll. Accepted is not a result: acceptance creates a pending obligation — consume the job with deepseek_follow before a dependent gate or a final response, or explicitly end it with deepseek_abort or deepseek_close. An open agent continues in its same OpenCode session. Reject or wait if the agent is busy; use deepseek_abort to stop it. A closed agent is not continuable: set allow_respawn=true only as an explicit recovery when the agent was closed AFTER a terminal job with a persisted result and was NOT explicitly aborted — the bridge then accepts automatically by spawning a NEW agent and a NEW OpenCode session in the same persisted workspace, topic, workspace strategy and pinned model route, records the lineage (parent_agent_id and auditable activity on both agents), preserves or derives the correlation thread/turn hints, and returns the NEW agentId/jobId to follow; it never claims the closed session is the same session and never reopens the closed agent. allow_respawn is rejected (typed 409/400) for aborted agents, closed agents without a persisted result, busy agents, permission-field answers and any scope change: the child inherits only the parent's persisted identity and workspace, with no provider fallback and no live-config route. For an explicit OpenCode permission response on an open agent, also provide permission_id and permission_reply (once, always, or reject). When the continuation depends on visual material, inspect the visuals yourself first and send a compact textual visual_context (string, optional, no default) with three labeled parts, 'Direct observations:', 'Interpretation:' and 'Uncertainty:'. Send only your textual interpretation; DeepSeek never receives pixels. Treat direct observations as evidence, interpretation as a hypothesis, and never invent visual details absent from the context.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: continueInputSchema,
    outputSchema: {
      accepted: z.boolean(),
      status: z.string(),
      topic: z.string(),
      modelDisplayName: z.string(),
      agentId: z.string(),
      jobId: z.string(),
      state: z.string(),
      obligationState: z.literal("pending"),
      nextRequiredAction: z.literal("deepseek_follow"),
    },
  }, async (args, extra) => {
    try {
      validateCallerThread(args.thread_id);
      const payload = {
        ...args,
        request_id: args.request_id ?? newId("request"),
        mcp_session_id: mcpProcessSessionId,
        ...(trustedThreadId ? { trusted_thread_id: trustedThreadId } : {}),
      };
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/continue", payload, extra?.signal);
      return acceptedResult(result, true);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_consult", {
    title: LEGACY_DISPLAY_NAME + " · Consult",
    description: "Get one immediate observable progress snapshot for an existing DeepSeek agent. Use only when the user asks for progress, a task is taking unusually long, or the snapshot materially changes the orchestrator's next decision. Do not use repeatedly to wait for completion. Never exposes private reasoning.",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: consultInputSchema,
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/consult", args, extra?.signal);
      return {
        content: [{ type: "text", text: "Observable DeepSeek progress snapshot returned." }],
        structuredContent: {
          ...result,
          capabilities: { batch_scheduler: true },
        },
      };
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_follow", {
    title: LEGACY_DISPLAY_NAME + " · Follow",
    description: "Wait for a DeepSeek job until it reaches a terminal result, using internal events and one deadline timer without polling. Use it for every job your next decision depends on: before a dependent gate or a final response, and before synthesizing from that front. You may orchestrate other fronts in parallel while a job is pending, but you must consume its result before depending on it; a pending job is not a result, and an unconsumed job leaves an open obligation. Returning a usable terminal result consumes the job obligation explicitly and persistently; a needs_approval follow keeps the obligation pending and requires deepseek_continue with permission_id and permission_reply. A terminal follow result closes the job obligation only: the DeepSeek agent stays open and continuable until you close it with deepseek_close after reviewing — closing the agent is separate from consuming the obligation. Completed, failed and timed-out agents remain continuable with deepseek_continue. The daemon-configured defaults are the worker's minimum window: wait_minutes and grace_minutes below the defaults are raised, and only larger values extend the window; once active, subsequent followers share the existing persisted window. Omit wait_minutes and grace_minutes to use the defaults. When the follow window expires, the worker is gracefully finalized and may be aborted after the grace period.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: followInputSchema,
    outputSchema: {
      agentId: z.string(),
      jobId: z.string(),
      status: z.string(),
      resultAvailable: z.boolean(),
      permissionId: z.string().nullable().optional(),
      message: z.string().optional(),
      obligationState: z.union([z.literal("pending"), z.literal("closed")]),
      nextRequiredAction: z.literal("deepseek_continue").optional(),
      receipt: receiptOutputSchema,
      earlyExit: earlyExitOutputSchema,
      escalation: escalationOutputSchema,
      semanticProgress: semanticProgressOutputSchema,
    },
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/follow", args, extra?.signal);
      return followResult(result, true);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_park", {
    title: LEGACY_DISPLAY_NAME + " · Park",
    description: "Park the current orchestration turn and wait for one or more running DeepSeek jobs to wake it when ready. Returns immediately with an armed receipt if bridge-observed authoritative correlation confirms the target thread, or armed: false if authoritative attachment is not supported or correlated. Parking never consumes a job: the obligation remains pending and you must follow the ready jobs with deepseek_follow upon waking.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: parkInputSchema,
    outputSchema: {
      parkId: z.string(),
      generation: z.number(),
      armed: z.boolean(),
      targetIdentity: z.string(),
      obligationState: z.literal("pending"),
      nextRequiredAction: z.literal("deepseek_follow"),
      jobIds: z.array(z.string()),
      reason: z.string().nullable().optional(),
      pendingCount: z.number(),
      readyCount: z.number(),
      deliveryMode: z.enum(["queued", "cli_resume", "none"]).optional(),
      delivery_mode: z.enum(["queued", "cli_resume", "none"]).optional(),
      wakeState: z.enum(["waiting", "deferred_active_writer", "delivered", "failed"]).optional(),
      readyJobIds: z.array(z.string()).optional(),
      predicateType: z.enum(["ALL", "ANY", "QUORUM", "REQUIRED"]).optional(),
      quorumCount: z.number().nullable().optional(),
      requiredJobIds: z.array(z.string()).nullable().optional(),
      queueMessageId: z.string().nullable().optional(),
      queue_message_id: z.string().nullable().optional(),
    },
  }, async (args, extra) => {
    try {
      if (args.wait === true) {
        throw new InvalidRequestError(
          "In-turn waiting (wait=true) is no longer supported on park. Use deepseek_follow for in-turn waiting, or omit wait for external background parking.",
        );
      }
      validateCallerThread(args.thread_id ?? args.threadId);
      const payload = {
        ...normalizeParkArgs(args),
        is_alias: true,
        wait: false,
        mcp_session_id: mcpProcessSessionId,
        ...(trustedThreadId ? { trusted_thread_id: trustedThreadId } : {}),
      };
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/park", payload, extra?.signal);
      return parkResult(result, true);
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_abort", {
    title: LEGACY_DISPLAY_NAME + " · Abort",
    description: "Stop the active DeepSeek task for an agent and end its pending obligation. This is a control action, not a polling operation.",
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    inputSchema: abortInputSchema,
    outputSchema: {
      agentId: z.string(),
      jobId: z.string().nullable().optional(),
      status: z.string(),
      state: z.string(),
      obligationState: z.literal("closed"),
    },
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/abort", args, extra?.signal);
      return technicalResult(result, "DeepSeek task stopped.");
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_close", {
    title: LEGACY_DISPLAY_NAME + " · Close",
    description: "Close a DeepSeek agent after its work is complete or stopped, ending any pending obligation. It does not delete result history.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: closeInputSchema,
    outputSchema: {
      agentId: z.string(),
      status: z.string(),
      state: z.string(),
      obligationState: z.literal("closed"),
    },
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/close", args, extra?.signal);
      return technicalResult(result, "DeepSeek agent closed.");
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_recover_result", {
    title: LEGACY_DISPLAY_NAME + " · Recover result",
    description: "Recover a persisted asynchronous result after automatic delivery failed or the user explicitly requested recovery. A successful recover returns the usable final result and explicitly consumes the job obligation (persisted), separate from closing the agent. Do not use this as a status poll and never call it repeatedly to check progress.",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: recoverInputSchema,
  }, async (args, extra) => {
    try {
      const result = await readyClient.call<unknown>("/v1/jobs/recover", args, extra?.signal);
      return {
        content: [{ type: "text", text: "Persisted DeepSeek result recovered." }],
        structuredContent: { result },
      };
    } catch (error) {
      if (extra?.signal?.aborted) throw error;
      return errorResult(error);
    }
  });

  return server;
}

function acceptedBatchResult(result: Record<string, unknown>, isAlias = false): {
  content: [{ type: "text"; text: string }];
  structuredContent: {
    accepted: true;
    batchId: string;
    batchRequestId: string;
    items: unknown;
    jobIds: string[];
    obligationState: "pending";
    nextRequiredAction: "deepseek_follow" | "subagents_follow";
    capabilities?: Record<string, boolean>;
  };
  _meta: Record<string, unknown>;
} {
  const batchId = String(result.batchId ?? "");
  const batchRequestId = String(result.batchRequestId ?? "");
  const rawItems = (result.items as Array<Record<string, unknown>>) ?? [];
  const nextRequiredAction = isAlias ? "deepseek_follow" : "subagents_follow";
  const displayName = isAlias ? LEGACY_DISPLAY_NAME : DISPLAY_NAME;
  const jobIds = rawItems.map((it) => String(it.jobId ?? ""));
  const text = isAlias
    ? `DeepSeek Sub-Agent accepted batch ${batchId} (${rawItems.length} items). Jobs: ${jobIds.join(", ")}. Pending obligations created. Follow each job or park across the batch before dependent gates.`
    : `${DISPLAY_NAME} accepted batch ${batchId} (${rawItems.length} items). Jobs: ${jobIds.join(", ")}. Pending obligations created. Follow each job or park across the batch before dependent gates.`;

  return {
    content: [{ type: "text", text }],
    structuredContent: {
      accepted: true,
      batchId,
      batchRequestId,
      items: rawItems,
      jobIds,
      obligationState: "pending",
      nextRequiredAction,
      capabilities: { batch_scheduler: true },
    },
    _meta: {
      technical: {
        batchId,
        batchRequestId,
        items: rawItems,
        jobIds,
      },
    },
  };
}

function acceptedResult(result: Record<string, unknown>, isAlias = false): {
  content: [{ type: "text"; text: string }];
  structuredContent: {
    accepted: true;
    status: "accepted";
    topic: unknown;
    modelDisplayName: unknown;
    agentId: unknown;
    jobId: unknown;
    state: "Starting";
    obligationState: "pending";
    nextRequiredAction: "deepseek_follow" | "subagents_follow";
  };
  _meta: Record<string, unknown>;
} {
  const jobId = String(result.jobId ?? "");
  const uncertain = result.outcome === "dispatch_unknown";
  const modelDisplayName = result.modelDisplayName ?? MODEL_DISPLAY;
  const nextRequiredAction = isAlias ? "deepseek_follow" : "subagents_follow";
  const followTool = nextRequiredAction;
  const abortTool = isAlias ? "deepseek_abort" : "subagents_abort";
  const closeTool = isAlias ? "deepseek_close" : "subagents_close";

  const text = isAlias
    ? (uncertain
      ? "DeepSeek Sub-Agent accepted the task; provider dispatch acceptance is uncertain after a transport failure. Pending DeepSeek job: " + jobId
        + ". Accepted is not a result. Do not duplicate this delegated front locally. Consume this exact job with deepseek_follow, or explicitly abort/close it, before a dependent gate or a final response."
      : "DeepSeek Sub-Agent accepted the task. Pending DeepSeek job created: " + jobId
        + ". Accepted is not a result. Do not duplicate this delegated front locally. Before a dependent gate or final response, consume the job with deepseek_follow, or explicitly abort/close it.")
    : (uncertain
      ? `${DISPLAY_NAME} accepted the task (${modelDisplayName}); provider dispatch acceptance is uncertain after a transport failure. Pending job: ` + jobId
        + `. Accepted is not a result. Do not duplicate this delegated front locally. Consume this exact job with ${followTool}, or explicitly abort/close it with ${abortTool} or ${closeTool}, before a dependent gate or a final response.`
      : `${DISPLAY_NAME} accepted the task (${modelDisplayName}). Pending job created: ` + jobId
        + `. Accepted is not a result. Do not duplicate this delegated front locally. Before a dependent gate or final response, consume the job with ${followTool}, or explicitly abort/close it with ${abortTool} or ${closeTool}.`);

  return {
    content: [{ type: "text", text }],
    structuredContent: {
      accepted: true,
      status: "accepted",
      topic: result.topic,
      modelDisplayName,
      agentId: result.agentId,
      jobId: result.jobId,
      state: "Starting",
      obligationState: "pending",
      nextRequiredAction,
      ...(result.priority !== undefined ? { priority: result.priority } : {}),
      ...(result.exclusiveResources !== undefined ? { exclusiveResources: result.exclusiveResources } : {}),
    },
    _meta: {
      technical: {
        agentId: result.agentId,
        jobId: result.jobId,
        state: "Starting",
        provider: result.modelProviderId ?? "antigravity",
        model: modelDisplayName,
        ...(result.priority !== undefined ? { priority: result.priority } : {}),
        ...(result.exclusiveResources !== undefined ? { exclusiveResources: result.exclusiveResources } : {}),
      },
    },
  };
}

function followResult(result: Record<string, unknown>, isAlias = false): {
  content: [{ type: "text"; text: string }];
  structuredContent: Record<string, unknown>;
} {
  const nextRequiredAction = isAlias ? "deepseek_continue" : "subagents_continue";
  const abortTool = isAlias ? "deepseek_abort" : "subagents_abort";
  const closeTool = isAlias ? "deepseek_close" : "subagents_close";
  const displayName = isAlias ? LEGACY_DISPLAY_NAME : DISPLAY_NAME;

  const receipt = result.receipt as Record<string, unknown> | undefined;
  const earlyExit = result.earlyExit as Record<string, unknown> | undefined;
  const escalation = result.escalation as Record<string, unknown> | undefined;
  const semanticProgress = (result.semanticProgress ?? (result.progress as Record<string, unknown> | undefined)?.semanticProgress) as Record<string, unknown> | undefined;

  const compactParts: string[] = [];
  if (receipt) {
    const duration = receipt.durationMs !== null && receipt.durationMs !== undefined ? `${receipt.durationMs}ms` : "n/a";
    compactParts.push(`receipt: ${receipt.status} (${duration})`);
  }
  if (earlyExit?.triggered) {
    compactParts.push(`earlyExit: ${earlyExit.reason || "triggered"}`);
  }
  if (escalation) {
    const route = escalation.recommendedRoute ? ` -> ${escalation.recommendedRoute}` : "";
    compactParts.push(`escalation: ${escalation.reason}${route}`);
  }
  if (semanticProgress) {
    compactParts.push(`stage: ${semanticProgress.stage}`);
  }

  const compactSuffix = compactParts.length > 0 ? " [" + compactParts.join(" | ") + "]" : "";

  if (result.status === "needs_approval") {
    return {
      content: [{
        type: "text",
        text: `${displayName} follow requires explicit approval before continuing. Answer with ${nextRequiredAction}, providing permission_id and permission_reply, or end the obligation with ${abortTool} or ${closeTool}.${compactSuffix}`,
      }],
      structuredContent: {
        ...result,
        ...(receipt ? { receipt } : {}),
        ...(earlyExit ? { earlyExit } : {}),
        ...(escalation ? { escalation } : {}),
        ...(semanticProgress ? { semanticProgress } : {}),
        obligationState: "pending",
        nextRequiredAction,
      },
    };
  }
  return {
    content: [{
      type: "text",
      text: `${displayName} follow returned a terminal result. The job obligation is closed; the ${isAlias ? "DeepSeek " : ""}agent itself remains open and continuable. Close it with ${closeTool} after reviewing the result.${compactSuffix}`,
    }],
    structuredContent: {
      ...result,
      ...(receipt ? { receipt } : {}),
      ...(earlyExit ? { earlyExit } : {}),
      ...(escalation ? { escalation } : {}),
      ...(semanticProgress ? { semanticProgress } : {}),
      obligationState: "closed",
    },
  };
}

function parkResult(result: Record<string, unknown>, isAlias = false): {
  content: [{ type: "text"; text: string }];
  structuredContent: {
    parkId: string;
    generation: number;
    armed: boolean;
    targetIdentity: string;
    obligationState: "pending";
    nextRequiredAction: "deepseek_follow" | "subagents_follow";
    jobIds: string[];
    reason: string | null;
    pendingCount: number;
    readyCount: number;
    deliveryMode?: "queued" | "cli_resume" | "none";
    delivery_mode?: "queued" | "cli_resume" | "none";
    wakeState?: "waiting" | "deferred_active_writer" | "delivered" | "failed";
    readyJobIds?: string[];
    predicateType?: "ALL" | "ANY" | "QUORUM" | "REQUIRED";
    quorumCount?: number | null;
    requiredJobIds?: string[] | null;
    queueMessageId?: string | null;
    queue_message_id?: string | null;
  };
} {
  const parkId = String(result.parkId ?? "");
  const armed = Boolean(result.armed);
  const targetIdentity = String(result.targetIdentity ?? "");
  const nextRequiredAction = isAlias ? ("deepseek_follow" as const) : ("subagents_follow" as const);
  const displayName = isAlias ? LEGACY_DISPLAY_NAME : DISPLAY_NAME;
  const rawDeliveryMode = result.deliveryMode ?? result.delivery_mode;
  let deliveryMode: "queued" | "cli_resume" | "none" | undefined;
  if (rawDeliveryMode === "queued" || rawDeliveryMode === "cli_resume" || rawDeliveryMode === "none") {
    deliveryMode = rawDeliveryMode;
  } else if (rawDeliveryMode === "in_turn") {
    deliveryMode = armed ? "cli_resume" : "none";
  }
  const wakeState = result.wakeState as ("waiting" | "deferred_active_writer" | "delivered" | "failed") | undefined;
  const readyJobIds = Array.isArray(result.readyJobIds) ? (result.readyJobIds as string[]) : undefined;
  const predicateType = result.predicateType as ("ALL" | "ANY" | "QUORUM" | "REQUIRED") | undefined;
  const quorumCount = result.quorumCount !== undefined ? (result.quorumCount as number | null) : undefined;
  const requiredJobIds = Array.isArray(result.requiredJobIds) ? (result.requiredJobIds as string[]) : (result.requiredJobIds === null ? null : undefined);

  const outboxObj = (result.outbox && typeof result.outbox === "object") ? (result.outbox as Record<string, unknown>) : undefined;
  const outboxStatusObj = (result.outboxStatus && typeof result.outboxStatus === "object") ? (result.outboxStatus as Record<string, unknown>) : undefined;
  const rawQueueMsgId = result.queueMessageId ??
    result.queue_message_id ??
    result.messageId ??
    result.message_id ??
    outboxObj?.queueMessageId ??
    outboxObj?.queue_message_id ??
    outboxObj?.messageId ??
    outboxObj?.message_id ??
    outboxStatusObj?.queueMessageId ??
    outboxStatusObj?.queue_message_id ??
    outboxStatusObj?.messageId ??
    outboxStatusObj?.message_id;
  const queueMessageId = typeof rawQueueMsgId === "string" && rawQueueMsgId.length > 0
    ? rawQueueMsgId
    : (rawQueueMsgId === null ? null : undefined);

  let text: string;
  if (armed) {
    text = `${displayName} parked turn on barrier ${parkId} (target: ${targetIdentity}). The turn will wake when ready jobs finish. Jobs remain pending; consume them with ${nextRequiredAction} upon wake.`;
  } else {
    text = `${displayName} park registered barrier ${parkId} (unarmed; authoritative attachment or bridge correlation not active). Consume pending jobs with ${nextRequiredAction}.`;
  }

  return {
    content: [{ type: "text" as const, text }],
    structuredContent: {
      parkId,
      generation: Number(result.generation ?? 1),
      armed,
      targetIdentity,
      obligationState: "pending" as const,
      nextRequiredAction,
      jobIds: Array.isArray(result.jobIds) ? (result.jobIds as string[]) : [],
      reason: result.reason !== undefined && result.reason !== null ? String(result.reason) : null,
      pendingCount: Number(result.pendingCount ?? 0),
      readyCount: Number(result.readyCount ?? 0),
      ...(deliveryMode ? { deliveryMode, delivery_mode: deliveryMode } : {}),
      ...(wakeState ? { wakeState } : {}),
      ...(readyJobIds ? { readyJobIds } : {}),
      ...(predicateType ? { predicateType } : {}),
      ...(quorumCount !== undefined ? { quorumCount } : {}),
      ...(requiredJobIds !== undefined ? { requiredJobIds } : {}),
      ...(queueMessageId !== undefined ? { queueMessageId, queue_message_id: queueMessageId } : {}),
    },
  };
}

function technicalResult(result: Record<string, unknown>, text: string): {
  content: [{ type: "text"; text: string }];
  structuredContent: Record<string, unknown>;
} {
  return {
    content: [{ type: "text", text }],
    structuredContent: {
      ...result,
      state: humanState(typeof result.status === "string" ? result.status : ""),
      obligationState: "closed",
    },
  };
}

function errorResult(error: unknown): {
  isError: true;
  content: [{ type: "text"; text: string }];
  structuredContent?: Record<string, unknown>;
} {
  const text = redactSecrets(String(error));
  // Structured typed propagation: the HTTP error carries a stable code and
  // status; never sniff message text to classify errors.
  if (error instanceof BridgeHttpError) {
    return {
      isError: true,
      content: [{ type: "text", text }],
      structuredContent: {
        code: error.code,
        status: error.status,
        message: text,
        ...(error.details !== undefined ? { details: error.details } : {}),
        retry: error.status === 409 || error.status === 403 ? false : undefined,
      },
    };
  }
  return {
    isError: true,
    content: [{ type: "text", text }],
    structuredContent: { code: "internal", status: 500, message: text },
  };
}

function humanState(status: string): string {
  switch (status) {
    case "created": return "Preparing";
    case "dispatching": return "Starting";
    case "running": return "Working";
    case "needs_approval": return "Needs Approval";
    case "completed": return "Completed";
    case "delivery_pending": return "Delivering";
    case "delivered": return "Delivered";
    case "failed": return "Failed";
    case "aborted": return "Stopped";
    case "closed": return "Stopped";
    default: return status || "Working";
  }
}
