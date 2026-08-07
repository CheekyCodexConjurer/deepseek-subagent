import { setTimeout as delay } from "node:timers/promises";
import { assertLoopbackUrl, redactSecrets } from "../security.js";
import { SseParser, parseJsonSseEvent } from "../sse.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage, OpenCodeSession } from "../types.js";

export interface OpenCodeClientOptions {
  baseUrl: string;
  username?: string;
  password?: string | null;
  reconnectMaxMs?: number;
  requestTimeoutMs?: number;
}

function encodePath(value: string): string {
  return encodeURIComponent(value);
}

export class OpenCodeHttpError extends Error {
  constructor(
    readonly status: number,
    readonly method: string,
    readonly url: string,
    readonly responseBody: string,
  ) {
    super(method + " " + url + " failed with HTTP " + status + ": " + redactSecrets(responseBody.slice(0, 600)));
    this.name = "OpenCodeHttpError";
  }
}

export class OpenCodeClient implements OpenCodeClientLike {
  readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly reconnectMaxMs: number;
  private readonly requestTimeoutMs: number;

  constructor(options: OpenCodeClientOptions) {
    const url = assertLoopbackUrl(options.baseUrl);
    this.baseUrl = url.toString().replace(/\/$/, "");
    this.headers = { accept: "application/json" };
    if (options.username && options.password) {
      this.headers.authorization = "Basic " + Buffer.from(options.username + ":" + options.password).toString("base64");
    }
    this.reconnectMaxMs = options.reconnectMaxMs ?? 30_000;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
  }

  async health(): Promise<{ healthy: boolean; version?: string }> {
    const value = await this.request<Record<string, unknown>>("GET", "/global/health", { timeoutMs: 5_000 });
    const result: { healthy: boolean; version?: string } = { healthy: value.healthy === true };
    if (typeof value.version === "string") result.version = value.version;
    return result;
  }

  async createSession(directory: string, title: string): Promise<OpenCodeSession> {
    return this.request<OpenCodeSession>("POST", "/session", {
      body: { directory, title },
    });
  }

  async promptAsync(
    sessionId: string,
    task: string,
    options: { providerId: string; modelId: string; variant?: string; agent?: string },
  ): Promise<void> {
    const body: Record<string, unknown> = {
      parts: [{ type: "text", text: task }],
      model: { providerID: options.providerId, modelID: options.modelId },
    };
    if (options.variant) body.variant = options.variant;
    if (options.agent) body.agent = options.agent;
    await this.request<void>("POST", "/session/" + encodePath(sessionId) + "/prompt_async", {
      body,
      acceptedStatus: [200, 201, 202, 204],
      timeoutMs: 30_000,
    });
  }

  async listMessages(sessionId: string): Promise<OpenCodeMessage[]> {
    return this.request<OpenCodeMessage[]>("GET", "/session/" + encodePath(sessionId) + "/message");
  }

  async getDiff(sessionId: string): Promise<unknown> {
    return this.request<unknown>("GET", "/session/" + encodePath(sessionId) + "/diff");
  }

  async abort(sessionId: string): Promise<void> {
    await this.request<void>("POST", "/session/" + encodePath(sessionId) + "/abort", {
      acceptedStatus: [200, 202, 204],
    });
  }

  async replyPermission(
    sessionId: string,
    permissionId: string,
    reply: "once" | "always" | "reject",
    message?: string,
  ): Promise<void> {
    await this.request<void>("POST", "/api/session/" + encodePath(sessionId) + "/permission/" + encodePath(permissionId) + "/reply", {
      body: { reply, ...(message ? { message } : {}) },
      acceptedStatus: [200, 202, 204],
    });
  }

  async subscribe(onEvent: (event: OpenCodeEvent) => Promise<void> | void, signal?: AbortSignal): Promise<void> {
    let attempt = 0;
    while (!signal?.aborted) {
      try {
        await this.readEventStream(onEvent, signal);
        attempt = 0;
      } catch (error) {
        if (signal?.aborted) return;
        const waitMs = Math.min(this.reconnectMaxMs, 500 * 2 ** Math.min(attempt, 8)) + Math.floor(Math.random() * 250);
        attempt += 1;
        await delay(waitMs, undefined, { signal }).catch(() => undefined);
        if (error instanceof OpenCodeHttpError && error.status === 401) {
          throw error;
        }
      }
    }
  }

  async getOpenApi(): Promise<unknown> {
    return this.request<unknown>("GET", "/doc");
  }

  async listModels(): Promise<unknown> {
    return this.request<unknown>("GET", "/api/model");
  }

  async listProviders(): Promise<unknown> {
    return this.request<unknown>("GET", "/api/provider");
  }

  private async readEventStream(
    onEvent: (event: OpenCodeEvent) => Promise<void> | void,
    signal?: AbortSignal,
  ): Promise<void> {
    const response = await fetch(this.baseUrl + "/event", {
      method: "GET",
      headers: { ...this.headers, accept: "text/event-stream" },
      ...(signal ? { signal } : {}),
    });
    if (!response.ok) {
      throw new OpenCodeHttpError(response.status, "GET", this.baseUrl + "/event", await response.text());
    }
    if (!response.body) throw new Error("OpenCode event stream returned no body");
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const parser = new SseParser();
    try {
      while (true) {
        const next = await reader.read();
        if (next.done) break;
        const frames = parser.feed(decoder.decode(next.value, { stream: true }));
        for (const frame of frames) {
          const value = parseJsonSseEvent(frame);
          if (!value) continue;
          const type = typeof value.type === "string" ? value.type : frame.event ?? "unknown";
          const properties = isRecord(value.properties) ? value.properties : value;
          await onEvent({
            type,
            properties,
            ...(frame.id ? { id: frame.id } : {}),
          });
        }
      }
      for (const frame of parser.flush()) {
        const value = parseJsonSseEvent(frame);
        if (!value) continue;
        const type = typeof value.type === "string" ? value.type : frame.event ?? "unknown";
        const properties = isRecord(value.properties) ? value.properties : value;
        await onEvent({
          type,
          properties,
          ...(frame.id ? { id: frame.id } : {}),
        });
      }
      throw new Error("OpenCode event stream ended");
    } finally {
      reader.releaseLock();
    }
  }

  private async request<T>(
    method: string,
    pathname: string,
    options: { body?: unknown; acceptedStatus?: number[]; timeoutMs?: number } = {},
  ): Promise<T> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), options.timeoutMs ?? this.requestTimeoutMs);
    let response: Response;
    try {
      response = await fetch(this.baseUrl + pathname, {
        method,
        headers: {
          ...this.headers,
          ...(options.body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
        signal: controller.signal,
      });
    } catch (error) {
      if (controller.signal.aborted) throw new Error(method + " " + pathname + " timed out");
      throw error;
    } finally {
      clearTimeout(timeout);
    }
    const acceptedStatus = options.acceptedStatus ?? [200];
    const text = await response.text();
    if (!acceptedStatus.includes(response.status)) {
      throw new OpenCodeHttpError(response.status, method, this.baseUrl + pathname, text);
    }
    if (text.length === 0) return undefined as T;
    try {
      return JSON.parse(text) as T;
    } catch {
      return text as T;
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
