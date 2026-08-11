import { open } from "node:fs/promises";
import { spawn } from "node:child_process";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { defaultConfigPath, loadConfig, saveConfig } from "./config.js";
import { BridgeHttpClient } from "./http-server.js";
import { canRead, ensurePrivateDir, redactSecrets } from "./security.js";
import type { BridgeConfig } from "./types.js";

const DISPLAY_NAME = "DeepSeek Sub-Agent";
const MODEL_DISPLAY = "DeepSeek V4 Flash · Max";

export async function runMcp(configPath = defaultConfigPath()): Promise<void> {
  const config = await loadConfig(configPath);
  await ensureMcpConfig(config);
  const client = new BridgeHttpClient(config);
  // The MCP handshake and tool listing must never wait for daemon or OpenCode
  // startup. Daemon readiness is bootstrapped lazily on the first tool call,
  // memoized, and shared by concurrent first operations.
  const server = createMcpServer(client, {
    ensureReady: createLazyDaemonBootstrap(config, client),
  });
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error(DISPLAY_NAME + " MCP server connected; daemon readiness is bootstrapped on the first tool call.");
}

/**
 * Returns a memoized single-flight daemon readiness bootstrap. The first tool
 * operation triggers the daemon health check and, if offline, the one-time
 * recovery start. Concurrent first operations share the same bootstrap.
 * A failure resets the memo so a later operation can retry, and each tool call
 * surfaces a clear readiness error.
 */
export function createLazyDaemonBootstrap(
  config: BridgeConfig,
  client: DaemonHealthClient,
  options: DaemonBootstrapOptions = {},
): () => Promise<void> {
  let readyPromise: Promise<void> | null = null;
  return () => {
    if (!readyPromise) {
      readyPromise = ensureDaemonRunning(config, client, options).catch((error) => {
        readyPromise = null;
        throw new Error("DeepSeek Sub-Agent daemon is not ready: " + redactSecrets(String(error)));
      });
    }
    return readyPromise;
  };
}

class LazyReadyClient {
  constructor(
    private readonly client: Pick<BridgeHttpClient, "call">,
    private readonly ensureReady: () => Promise<void>,
  ) {}

  async call<T>(pathname: string, body?: unknown): Promise<T> {
    await this.ensureReady();
    return this.client.call<T>(pathname, body);
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
  try {
    await client.health();
    return;
  } catch (error) {
    // Start below and retain the first failure for a useful timeout message.
    var lastError: unknown = error;
  }

  await (options.start ?? startDetachedDaemon)(config);
  const timeoutMs = options.timeoutMs ?? Math.max(10_000, Math.min(45_000, config.opencodeStartupTimeoutMs + 5_000));
  const retryMs = options.retryMs ?? 100;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await client.health();
      return;
    } catch (error) {
      lastError = error;
    }
    await delay(Math.min(retryMs, Math.max(1, deadline - Date.now())));
  }
  throw new Error("DeepSeek Sub-Agent daemon did not become ready: " + redactSecrets(String(lastError)));
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

export function createMcpServer(
  client: BridgeHttpClient,
  options: { ensureReady?: () => Promise<void> } = {},
): McpServer {
  const server = new McpServer({
    name: "deepseek-subagent",
    title: DISPLAY_NAME,
    version: "0.1.0",
  });
  const readyClient = options.ensureReady ? new LazyReadyClient(client, options.ensureReady) : client;

  server.registerTool("deepseek_spawn", {
    title: DISPLAY_NAME + " · Spawn",
    description: "Start one asynchronous DeepSeek V4 Flash task in a new OpenCode session. Return immediately after acceptance; do not poll. Accepted is not a result: acceptance creates a pending obligation — consume the job with deepseek_follow before a dependent gate or a final response, or explicitly end it with deepseek_abort or deepseek_close. Do not duplicate this delegated front locally; you may orchestrate other fronts in parallel while it is pending. When the task depends on visual material, inspect the visuals yourself first and send a compact textual visual_context (string, optional, no default) with three labeled parts, 'Direct observations:', 'Interpretation:' and 'Uncertainty:'. Send only your textual interpretation; DeepSeek never receives pixels. Treat direct observations as evidence, interpretation as a hypothesis, and never invent visual details absent from the context.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: {
      request_id: z.string().min(1).optional(),
      topic: z.string().min(1).max(240),
      task: z.string().min(1),
      cwd: z.string().optional(),
      mode: z.enum(["analyze", "edit", "test"]).optional(),
      workspace_strategy: z.enum(["shared", "worktree"]).optional(),
      context_files: z.array(z.string()).optional(),
      visual_context: z.string().optional(),
      thread_id: z.string().optional(),
      turn_id: z.string().optional(),
    },
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
  }, async (args) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/spawn", args);
      return acceptedResult(result);
    } catch (error) {
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_continue", {
    title: DISPLAY_NAME + " · Continue",
    description: "Continue an existing DeepSeek agent in the same OpenCode session after reviewing its delivered result. Asynchronous; returns immediately; do not poll. Accepted is not a result: acceptance creates a pending obligation — consume the job with deepseek_follow before a dependent gate or a final response, or explicitly end it with deepseek_abort or deepseek_close. Reject or wait if the agent is busy; use deepseek_abort to stop it. For an explicit OpenCode permission response, also provide permission_id and permission_reply (once, always, or reject). When the continuation depends on visual material, inspect the visuals yourself first and send a compact textual visual_context (string, optional, no default) with three labeled parts, 'Direct observations:', 'Interpretation:' and 'Uncertainty:'. Send only your textual interpretation; DeepSeek never receives pixels. Treat direct observations as evidence, interpretation as a hypothesis, and never invent visual details absent from the context.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: {
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
    },
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
  }, async (args) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/continue", args);
      return acceptedResult(result);
    } catch (error) {
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_consult", {
    title: DISPLAY_NAME + " · Consult",
    description: "Get one immediate observable progress snapshot for an existing DeepSeek agent. Use only when the user asks for progress, a task is taking unusually long, or the snapshot materially changes the orchestrator's next decision. Do not use repeatedly to wait for completion. Never exposes private reasoning.",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: {
      agent_id: z.string().min(1),
      job_id: z.string().min(1).optional(),
      activity_limit: z.number().int().min(1).max(20).default(10),
    },
  }, async (args) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/consult", args);
      return {
        content: [{ type: "text", text: "Observable DeepSeek progress snapshot returned." }],
        structuredContent: result,
      };
    } catch (error) {
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_follow", {
    title: DISPLAY_NAME + " · Follow",
    description: "Wait for a DeepSeek job until it reaches a terminal result, using internal events and one deadline timer without polling. Use it for every job your next decision depends on: before a dependent gate or a final response, and before synthesizing from that front. You may orchestrate other fronts in parallel while a job is pending, but you must consume its result before depending on it; a pending job is not a result, and an unconsumed job leaves an open obligation. A terminal follow result closes the job obligation only: the DeepSeek agent stays open and continuable until you close it with deepseek_close after reviewing. Completed, failed and timed-out agents remain continuable with deepseek_continue. The daemon-configured defaults are the worker's minimum window: wait_minutes and grace_minutes below the defaults are raised, and only larger values extend the window; once active, subsequent followers share the existing persisted window. Omit wait_minutes and grace_minutes to use the defaults. When the follow window expires, the worker is gracefully finalized and may be aborted after the grace period. Terminal results close the obligation; needs_approval keeps it pending and requires deepseek_continue with permission_id and permission_reply.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: {
      agent_id: z.string().min(1),
      job_id: z.string().min(1).optional(),
      wait_minutes: z.number().int().min(1).max(60).optional(),
      grace_minutes: z.number().int().min(1).max(10).optional(),
    },
    outputSchema: {
      agentId: z.string(),
      jobId: z.string(),
      status: z.string(),
      resultAvailable: z.boolean(),
      permissionId: z.string().nullable().optional(),
      message: z.string().optional(),
      obligationState: z.union([z.literal("pending"), z.literal("closed")]),
      nextRequiredAction: z.literal("deepseek_continue").optional(),
    },
  }, async (args) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/follow", args);
      return followResult(result);
    } catch (error) {
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_abort", {
    title: DISPLAY_NAME + " · Abort",
    description: "Stop the active DeepSeek task for an agent and end its pending obligation. This is a control action, not a polling operation.",
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    inputSchema: {
      agent_id: z.string().min(1),
      reason: z.string().max(500).optional(),
    },
    outputSchema: {
      agentId: z.string(),
      jobId: z.string().nullable().optional(),
      status: z.string(),
      state: z.string(),
      obligationState: z.literal("closed"),
    },
  }, async (args) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/abort", args);
      return technicalResult(result, "DeepSeek task stopped.");
    } catch (error) {
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_close", {
    title: DISPLAY_NAME + " · Close",
    description: "Close a DeepSeek agent after its work is complete or stopped, ending any pending obligation. It does not delete result history.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: {
      agent_id: z.string().min(1),
    },
    outputSchema: {
      agentId: z.string(),
      status: z.string(),
      state: z.string(),
      obligationState: z.literal("closed"),
    },
  }, async (args) => {
    try {
      const result = await readyClient.call<Record<string, unknown>>("/v1/jobs/close", args);
      return technicalResult(result, "DeepSeek agent closed.");
    } catch (error) {
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_recover_result", {
    title: DISPLAY_NAME + " · Recover result",
    description: "Recover a persisted asynchronous result after automatic delivery failed or the user explicitly requested recovery. Do not use this as a status poll and never call it repeatedly to check progress.",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    inputSchema: {
      agent_id: z.string().min(1),
      job_id: z.string().min(1),
    },
  }, async (args) => {
    try {
      const result = await readyClient.call<unknown>("/v1/jobs/recover", args);
      return {
        content: [{ type: "text", text: "Persisted DeepSeek result recovered." }],
        structuredContent: { result },
      };
    } catch (error) {
      return errorResult(error);
    }
  });
  return server;
}

function acceptedResult(result: Record<string, unknown>): {
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
    nextRequiredAction: "deepseek_follow";
  };
  _meta: Record<string, unknown>;
} {
  const jobId = String(result.jobId ?? "");
  const uncertain = result.outcome === "dispatch_unknown";
  const text = uncertain
    ? "DeepSeek Sub-Agent accepted the task; OpenCode dispatch acceptance is uncertain after a transport failure. Pending DeepSeek job: " + jobId
      + ". Accepted is not a result. Do not duplicate this delegated front locally. Consume this exact job with deepseek_follow, or explicitly abort/close it, before a dependent gate or a final response."
    : "DeepSeek Sub-Agent accepted the task. Pending DeepSeek job created: " + jobId
      + ". Accepted is not a result. Do not duplicate this delegated front locally. Before a dependent gate or final response, consume the job with deepseek_follow, or explicitly abort/close it.";
  return {
    content: [{ type: "text", text }],
    structuredContent: {
      accepted: true,
      status: "accepted",
      topic: result.topic,
      modelDisplayName: result.modelDisplayName ?? MODEL_DISPLAY,
      agentId: result.agentId,
      jobId: result.jobId,
      state: "Starting",
      obligationState: "pending",
      nextRequiredAction: "deepseek_follow",
    },
    _meta: {
      technical: {
        agentId: result.agentId,
        jobId: result.jobId,
        state: "Starting",
      },
    },
  };
}

function followResult(result: Record<string, unknown>): {
  content: [{ type: "text"; text: string }];
  structuredContent: Record<string, unknown>;
} {
  if (result.status === "needs_approval") {
    return {
      content: [{
        type: "text",
        text: "DeepSeek Sub-Agent follow requires explicit approval before continuing. Answer with deepseek_continue, providing permission_id and permission_reply, or end the obligation with deepseek_abort or deepseek_close.",
      }],
      structuredContent: {
        ...result,
        obligationState: "pending",
        nextRequiredAction: "deepseek_continue",
      },
    };
  }
  return {
    content: [{ type: "text", text: "DeepSeek Sub-Agent follow returned a terminal result. The job obligation is closed; the DeepSeek agent itself remains open and continuable. Close it with deepseek_close after reviewing the result." }],
    structuredContent: { ...result, obligationState: "closed" },
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
  const busy = text.toLowerCase().includes("busy");
  return {
    isError: true,
    content: [{ type: "text", text }],
    ...(busy ? { structuredContent: { code: "busy", retry: false, message: text } } : {}),
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
