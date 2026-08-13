import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { URL } from "node:url";
import { Agent, fetch, type Dispatcher } from "undici";
import { isLoopbackHost, newId, redactSecrets, truncate } from "./security.js";
import { BridgeError, InvalidRequestError } from "./errors.js";
import { BridgeBusyError, FollowCancelledError, BridgeService } from "./service.js";
import type { AgentMode, ConsultInput, ContinueInput, FollowInput, SpawnInput, WorkspaceStrategy } from "./types.js";
import type { BridgeConfig } from "./types.js";

// The follow endpoint holds the HTTP response open until the worker finishes.
// A valid follow window can be 60 minutes of work plus 10 minutes of graceful
// finalization. Undici's default 300-second headers timeout would kill such
// calls, so the bridge uses a reusable dispatcher with headers and body
// timeouts safely above the maximum follow window plus margin.
export const BRIDGE_HEADERS_TIMEOUT_MS = 90 * 60_000;
export const BRIDGE_BODY_TIMEOUT_MS = 90 * 60_000;
export const BRIDGE_CONNECT_TIMEOUT_MS = 10_000;

export function createBridgeDispatcher(): Dispatcher {
  return new Agent({
    keepAliveTimeout: 5 * 60_000,
    headersTimeout: BRIDGE_HEADERS_TIMEOUT_MS,
    bodyTimeout: BRIDGE_BODY_TIMEOUT_MS,
    connect: { timeout: BRIDGE_CONNECT_TIMEOUT_MS },
  });
}

// The doctor command probes the daemon health endpoint with its own caller-side
// cap. The bridge's reusable dispatcher must keep the 90-minute follow timeouts
// for real calls, so doctor uses a dedicated short-timeout dispatcher instead of
// lowering the shared one.
export const DOCTOR_HEALTH_TIMEOUT_MS = 5_000;

export function createDoctorHealthDispatcher(timeoutMs = DOCTOR_HEALTH_TIMEOUT_MS): Dispatcher {
  return new Agent({
    keepAliveTimeout: 5 * 60_000,
    headersTimeout: timeoutMs,
    bodyTimeout: timeoutMs,
    connect: { timeout: BRIDGE_CONNECT_TIMEOUT_MS },
  });
}

export class BridgeHttpServer {
  private server: Server | null = null;

  constructor(
    private readonly config: BridgeConfig,
    private readonly service: BridgeService,
  ) {}

  async start(): Promise<void> {
    if (this.server) return;
    if (!isLoopbackHost(this.config.daemonHost)) throw new Error("Bridge daemon must bind to loopback");
    const server = createServer((request, response) => {
      void this.handle(request, response).catch((error: unknown) => {
        if (error instanceof FollowCancelledError || response.destroyed || response.writableEnded) return;
        writeError(response, error);
      });
    });
    this.server = server;
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(this.config.daemonPort, this.config.daemonHost, () => resolve());
    });
  }

  async stop(): Promise<void> {
    const server = this.server;
    this.server = null;
    if (!server) return;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }

  private async handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const method = request.method ?? "GET";
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    if (url.pathname === "/health" && method === "GET") {
      writeJson(response, 200, {
        displayName: "DeepSeek Sub-Agent",
        status: this.service.status(),
      });
      return;
    }
    if (!this.authorized(request)) {
      writeJson(response, 401, { error: "Unauthorized", code: "unauthorized", status: 401 });
      return;
    }
    if (method === "GET" && url.pathname === "/v1/agents") {
      writeJson(response, 200, { agents: this.service.listAgents() });
      return;
    }
    if (method === "GET" && url.pathname === "/v1/jobs") {
      writeJson(response, 200, { jobs: this.service.listJobs() });
      return;
    }
    const body = method === "POST" ? await readJson(request).catch((error: unknown) => {
      throw invalidRequestError(error);
    }) : null;
    if (method === "POST" && url.pathname === "/v1/jobs/spawn") {
      const result = await this.service.spawn(toSpawnInput(body));
      writeJson(response, 202, result);
      return;
    }
    if (method === "POST" && url.pathname === "/v1/jobs/continue") {
      const result = await this.service.continueJob(toContinueInput(body));
      writeJson(response, 202, result);
      return;
    }
    if (method === "POST" && url.pathname === "/v1/jobs/consult") {
      const result = await this.service.consult(toConsultInput(body));
      writeJson(response, 200, result);
      return;
    }
    if (method === "POST" && url.pathname === "/v1/jobs/follow") {
      const cancellation = new AbortController();
      const onClose = () => {
        if (!response.writableEnded) cancellation.abort();
      };
      response.once("close", onClose);
      try {
        const result = await this.service.follow(toFollowInput(body), cancellation.signal);
        if (!response.writableEnded) writeJson(response, 200, result);
      } catch (error) {
        if (!(error instanceof FollowCancelledError)) throw error;
      } finally {
        response.off("close", onClose);
      }
      return;
    }
    if (method === "POST" && url.pathname === "/v1/jobs/abort") {
      const value = asRecord(body);
      const result = await this.service.abort(requiredString(value.agentId ?? value.agent_id, "agentId"), optionalString(value.reason));
      writeJson(response, 202, result);
      return;
    }
    if (method === "POST" && url.pathname === "/v1/jobs/close") {
      const value = asRecord(body);
      const result = await this.service.close(requiredString(value.agentId ?? value.agent_id, "agentId"));
      writeJson(response, 200, result);
      return;
    }
    if (method === "POST" && url.pathname === "/v1/jobs/recover") {
      const value = asRecord(body);
      const result = await this.service.recoverResult(
        requiredString(value.jobId ?? value.job_id, "jobId"),
        optionalString(value.agentId ?? value.agent_id),
      );
      writeJson(response, 200, result);
      return;
    }
    if (method === "POST" && url.pathname === "/v1/jobs/deliver") {
      const value = asRecord(body);
      await this.service.deliverJob(requiredString(value.jobId ?? value.job_id, "jobId"));
      writeJson(response, 200, { delivered: true });
      return;
    }
    const jobMatch = url.pathname.match(/^\/v1\/jobs\/([^/]+)$/);
    if (method === "GET" && jobMatch) {
      const job = this.service.getJob(decodeURIComponent(jobMatch[1] as string));
      if (!job) {
        writeJson(response, 404, { error: "Job not found", code: "unknown_job", status: 404 });
        return;
      }
      writeJson(response, 200, { job });
      return;
    }
    const agentMatch = url.pathname.match(/^\/v1\/agents\/([^/]+)$/);
    if (method === "GET" && agentMatch) {
      const agent = this.service.getAgent(decodeURIComponent(agentMatch[1] as string));
      if (!agent) {
        writeJson(response, 404, { error: "Agent not found", code: "unknown_agent", status: 404 });
        return;
      }
      writeJson(response, 200, { agent });
      return;
    }
    writeJson(response, 404, { error: "Not found", code: "not_found", status: 404 });
  }

  private authorized(request: IncomingMessage): boolean {
    const header = request.headers.authorization;
    return header === "Bearer " + this.config.daemonToken;
  }
}

export class BridgeHttpClient {
  private readonly baseUrl: string;
  private readonly dispatcher: Dispatcher;

  constructor(private readonly config: BridgeConfig, dispatcher?: Dispatcher) {
    this.baseUrl = "http://" + (config.daemonHost === "::1" ? "[::1]" : config.daemonHost) + ":" + config.daemonPort;
    this.dispatcher = dispatcher ?? createBridgeDispatcher();
  }

  async health(): Promise<unknown> {
    const response = await bridgeFetch(this.baseUrl + "/health", {}, this.dispatcher);
    return parseResponse(response);
  }

  async call<T>(pathname: string, body?: unknown): Promise<T> {
    const response = await bridgeFetch(this.baseUrl + pathname, {
      method: "POST",
      headers: {
        authorization: "Bearer " + this.config.daemonToken,
        "content-type": "application/json",
      },
      body: JSON.stringify(body ?? {}),
    }, this.dispatcher);
    return parseResponse(response) as Promise<T>;
  }

  async get<T>(pathname: string): Promise<T> {
    const response = await bridgeFetch(this.baseUrl + pathname, {
      method: "GET",
      headers: { authorization: "Bearer " + this.config.daemonToken },
    }, this.dispatcher);
    return parseResponse(response) as Promise<T>;
  }
}

type BridgeFetchInit = Parameters<typeof fetch>[1];
type BridgeFetchResponse = Awaited<ReturnType<typeof fetch>>;

async function bridgeFetch(url: string, init: BridgeFetchInit, dispatcher: Dispatcher): Promise<BridgeFetchResponse> {
  try {
    return await fetch(url, { ...init, dispatcher });
  } catch (error) {
    // Surface nested fetch/Undici cause codes (for example ECONNREFUSED or
    // UND_ERR_HEADERS_TIMEOUT) so transport failures are distinguishable from
    // definite HTTP rejections.
    throw new Error("Bridge HTTP request failed: " + describeFetchError(error), { cause: error });
  }
}

function describeFetchError(error: unknown): string {
  const message = redactSecrets(String(error));
  const cause = error && typeof error === "object" && "cause" in error ? (error as { cause?: unknown }).cause : undefined;
  if (!cause) return message;
  const causeText = redactSecrets(String(cause));
  const code = cause && typeof cause === "object" && "code" in cause ? (cause as { code?: unknown }).code : undefined;
  return code !== undefined ? message + " (cause: " + String(code) + " - " + causeText + ")" : message + " (cause: " + causeText + ")";
}

/**
 * Stable typed HTTP error contract: { error, code, status } plus optional
 * details and retry semantics. Text is never sniffed by consumers; the code
 * is the machine-readable discriminator.
 */
function writeError(response: ServerResponse, error: unknown): void {
  if (error instanceof BridgeError) {
    const body: Record<string, unknown> = {
      error: redactSecrets(error.message),
      code: error.code,
      status: error.status,
    };
    if (error.details !== undefined) body.details = error.details;
    if (error instanceof BridgeBusyError) {
      body.retry = false;
      body.jobId = error.jobId;
    }
    writeJson(response, error.status, body);
    return;
  }
  writeJson(response, 500, { error: redactSecrets(String(error)), code: "internal", status: 500 });
}

export class BridgeHttpError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details: unknown;

  constructor(status: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = "BridgeHttpError";
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

async function parseResponse(response: BridgeFetchResponse): Promise<unknown> {
  const text = await response.text();
  let value: unknown;
  try {
    value = text ? JSON.parse(text) : null;
  } catch {
    value = text;
  }
  if (!response.ok) {
    const record = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
    const code = typeof record?.code === "string" && record.code.length > 0 ? record.code : defaultCodeForStatus(response.status);
    const detail = typeof value === "string" ? value : typeof record?.error === "string" ? record.error : JSON.stringify(value);
    throw new BridgeHttpError(response.status, code, truncate(redactSecrets(detail), 600), record?.details);
  }
  return value;
}

function defaultCodeForStatus(status: number): string {
  if (status === 400) return "invalid_request";
  if (status === 401) return "unauthorized";
  if (status === 404) return "not_found";
  if (status === 409) return "conflict";
  return "internal";
}

function invalidRequestError(error: unknown): BridgeError {
  if (error instanceof BridgeError) return error;
  const message = error && typeof error === "object" && "message" in error
    ? String((error as { message: unknown }).message)
    : String(error);
  return new BridgeError(400, "invalid_request", message);
}

async function readJson(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > 2_000_000) throw new Error("Request body is too large");
    chunks.push(buffer);
  }
  const text = Buffer.concat(chunks).toString("utf8");
  if (!text) return {};
  return JSON.parse(text) as unknown;
}

function writeJson(response: ServerResponse, status: number, value: unknown): void {
  response.statusCode = status;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.end(JSON.stringify(value));
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) throw new InvalidRequestError(name + " is required");
  return value.trim();
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function optionalArray(value: unknown): string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) throw new InvalidRequestError("contextFiles must be an array of strings");
  return value as string[];
}

function toSpawnInput(body: unknown): SpawnInput {
  const value = asRecord(body);
  const cwd = optionalString(value.cwd);
  const mode = value.mode === undefined ? undefined : enumValue(value.mode, ["analyze", "edit", "test"], "mode") as AgentMode;
  const workspaceStrategyValue = value.workspaceStrategy ?? value.workspace_strategy;
  const workspaceStrategy = workspaceStrategyValue === undefined
    ? undefined
    : enumValue(workspaceStrategyValue, ["shared", "worktree"], "workspaceStrategy") as WorkspaceStrategy;
  const contextFiles = optionalArray(value.contextFiles ?? value.context_files);
  const visualContext = optionalString(value.visualContext ?? value.visual_context);
  const threadId = optionalString(value.threadId ?? value.thread_id);
  const turnId = optionalString(value.turnId ?? value.turn_id);
  const modelRoute = optionalString(value.modelRoute ?? value.model_route);
  return {
    requestId: optionalString(value.requestId ?? value.request_id) ?? newId("request"),
    topic: requiredString(value.topic, "topic"),
    task: requiredString(value.task, "task"),
    ...(cwd ? { cwd } : {}),
    ...(mode ? { mode } : {}),
    ...(workspaceStrategy ? { workspaceStrategy } : {}),
    ...(contextFiles ? { contextFiles } : {}),
    ...(visualContext ? { visualContext } : {}),
    ...(threadId ? { threadId } : {}),
    ...(turnId ? { turnId } : {}),
    ...(modelRoute ? { modelRoute } : {}),
  };
}

function toContinueInput(body: unknown): ContinueInput {
  const value = asRecord(body);
  const threadId = optionalString(value.threadId ?? value.thread_id);
  const turnId = optionalString(value.turnId ?? value.turn_id);
  const permissionId = optionalString(value.permissionId ?? value.permission_id);
  const permissionReplyValue = value.permissionReply ?? value.permission_reply;
  const permissionReply = permissionReplyValue === undefined
    ? undefined
    : enumValue(permissionReplyValue, ["once", "always", "reject"], "permissionReply") as ContinueInput["permissionReply"];
  const permissionMessage = optionalString(value.permissionMessage ?? value.permission_message);
  const visualContext = optionalString(value.visualContext ?? value.visual_context);
  return {
    requestId: optionalString(value.requestId ?? value.request_id) ?? newId("request"),
    agentId: requiredString(value.agentId ?? value.agent_id, "agentId"),
    relation: enumValue(value.relation ?? "continuation", ["clarification", "correction", "review", "continuation"], "relation") as ContinueInput["relation"],
    task: requiredString(value.task, "task"),
    ...(visualContext ? { visualContext } : {}),
    ...(threadId ? { threadId } : {}),
    ...(turnId ? { turnId } : {}),
    ...(permissionId ? { permissionId } : {}),
    ...(permissionReply ? { permissionReply } : {}),
    ...(permissionMessage ? { permissionMessage } : {}),
  };
}

function toConsultInput(body: unknown): ConsultInput {
  const value = asRecord(body);
  const activityLimitValue = value.activityLimit ?? value.activity_limit;
  const jobId = optionalString(value.jobId ?? value.job_id);
  return {
    agentId: requiredString(value.agentId ?? value.agent_id, "agentId"),
    ...(jobId ? { jobId } : {}),
    ...(activityLimitValue === undefined ? {} : { activityLimit: integerInRange(activityLimitValue, 1, 20, "activityLimit") }),
  };
}

function toFollowInput(body: unknown): FollowInput {
  const value = asRecord(body);
  const waitValue = value.waitMinutes ?? value.wait_minutes;
  const graceValue = value.graceMinutes ?? value.grace_minutes;
  const jobId = optionalString(value.jobId ?? value.job_id);
  return {
    agentId: requiredString(value.agentId ?? value.agent_id, "agentId"),
    ...(jobId ? { jobId } : {}),
    ...(waitValue === undefined ? {} : { waitMinutes: integerInRange(waitValue, 1, 60, "waitMinutes") }),
    ...(graceValue === undefined ? {} : { graceMinutes: integerInRange(graceValue, 1, 10, "graceMinutes") }),
  };
}

function enumValue(value: unknown, allowed: string[], name: string): string {
  if (typeof value === "string" && allowed.includes(value)) return value;
  throw new InvalidRequestError(name + " must be one of: " + allowed.join(", "));
}

function integerInRange(value: unknown, minimum: number, maximum: number, name: string): number {
  if (typeof value === "number" && Number.isInteger(value) && value >= minimum && value <= maximum) return value;
  throw new InvalidRequestError(name + " must be an integer between " + minimum + " and " + maximum);
}
