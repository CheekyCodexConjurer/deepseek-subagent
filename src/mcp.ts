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
  await ensureDaemonRunning(config, client);
  const server = createMcpServer(client);
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error(DISPLAY_NAME + " MCP server connected to the local daemon.");
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

export function createMcpServer(client: BridgeHttpClient): McpServer {
  const server = new McpServer({
    name: "deepseek-subagent",
    title: DISPLAY_NAME,
    version: "0.1.0",
  });

  server.registerTool("deepseek_spawn", {
    title: DISPLAY_NAME + " · Spawn",
    description: "Start one asynchronous DeepSeek V4 Flash task in a new OpenCode session. Return immediately after acceptance; do not poll for completion. The daemon delivers the result to the originating Codex thread when a configured App Server correlation exists, otherwise it writes a private inbox result.",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: {
      request_id: z.string().min(1).optional(),
      topic: z.string().min(1).max(240),
      task: z.string().min(1),
      cwd: z.string().optional(),
      mode: z.enum(["analyze", "edit", "test"]).optional(),
      workspace_strategy: z.enum(["shared", "worktree"]).optional(),
      context_files: z.array(z.string()).optional(),
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
    },
  }, async (args) => {
    try {
      const result = await client.call<Record<string, unknown>>("/v1/jobs/spawn", args);
      return acceptedResult(result);
    } catch (error) {
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_continue", {
    title: DISPLAY_NAME + " · Continue",
    description: "Continue an existing DeepSeek agent in the same OpenCode session after reviewing its delivered result. This is asynchronous and returns immediately. Do not poll. Reject or wait if the agent is busy; use deepseek_abort to stop it. For an explicit OpenCode permission response, also provide permission_id and permission_reply (once, always, or reject).",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    inputSchema: {
      request_id: z.string().min(1).optional(),
      agent_id: z.string().min(1),
      relation: z.enum(["clarification", "correction", "review", "continuation"]).default("continuation"),
      task: z.string().min(1),
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
    },
  }, async (args) => {
    try {
      const result = await client.call<Record<string, unknown>>("/v1/jobs/continue", args);
      return acceptedResult(result);
    } catch (error) {
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_abort", {
    title: DISPLAY_NAME + " · Abort",
    description: "Stop the active DeepSeek task for an agent. This is a control action, not a polling operation.",
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    inputSchema: {
      agent_id: z.string().min(1),
      reason: z.string().max(500).optional(),
    },
  }, async (args) => {
    try {
      const result = await client.call<Record<string, unknown>>("/v1/jobs/abort", args);
      return technicalResult(result, "DeepSeek task stopped.");
    } catch (error) {
      return errorResult(error);
    }
  });

  server.registerTool("deepseek_close", {
    title: DISPLAY_NAME + " · Close",
    description: "Close a DeepSeek agent after its work is complete or stopped. It does not delete result history.",
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    inputSchema: {
      agent_id: z.string().min(1),
    },
  }, async (args) => {
    try {
      const result = await client.call<Record<string, unknown>>("/v1/jobs/close", args);
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
      const result = await client.call<unknown>("/v1/jobs/recover", args);
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
  structuredContent: Record<string, unknown>;
  _meta: Record<string, unknown>;
} {
  return {
    content: [{ type: "text", text: "DeepSeek Sub-Agent accepted the task. Result delivery is asynchronous." }],
    structuredContent: {
      accepted: true,
      status: "accepted",
      topic: result.topic,
      modelDisplayName: result.modelDisplayName ?? MODEL_DISPLAY,
      agentId: result.agentId,
      jobId: result.jobId,
      state: "Starting",
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

function technicalResult(result: Record<string, unknown>, text: string): {
  content: [{ type: "text"; text: string }];
  structuredContent: Record<string, unknown>;
} {
  return {
    content: [{ type: "text", text }],
    structuredContent: {
      ...result,
      state: humanState(typeof result.status === "string" ? result.status : ""),
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
