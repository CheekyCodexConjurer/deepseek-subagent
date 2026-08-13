import test from "node:test";
import assert from "node:assert/strict";
import { createServer, type AddressInfo, type Server as NetServer } from "node:net";
import { createServer as createHttpServer } from "node:http";
import { Agent } from "undici";
import { FOLLOW_MAX_TOTAL_MINUTES } from "../../src/config.js";
import { createDefaultConfig } from "../../src/config.js";
import { BridgeError, ConflictError, InvalidRequestError, NotFoundError, UnknownAgentError } from "../../src/errors.js";
import { BridgeBusyError } from "../../src/service.js";
import { BridgeHttpClient, BridgeHttpError, BridgeHttpServer, BRIDGE_HEADERS_TIMEOUT_MS, BRIDGE_BODY_TIMEOUT_MS, DOCTOR_HEALTH_TIMEOUT_MS, createDoctorHealthDispatcher } from "../../src/http-server.js";
import type { BridgeService } from "../../src/service.js";

async function freePort(): Promise<number> {
  const probe = createServer();
  return new Promise<number>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      const port = typeof address === "object" && address ? (address as AddressInfo).port : 0;
      probe.close((error) => error ? reject(error) : resolve(port));
    });
  });
}

test("HTTP maps visual_context to spawn and continue inputs", async () => {
  const calls: Array<{ kind: string; input: unknown }> = [];
  const service = {
    spawn: async (input: unknown) => {
      calls.push({ kind: "spawn", input });
      return { accepted: true };
    },
    continueJob: async (input: unknown) => {
      calls.push({ kind: "continue", input });
      return { accepted: true };
    },
  } as unknown as BridgeService;
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: await freePort(),
    daemonToken: "http-test-token",
    dataDir: "C:\\deepseek-http-test-data",
    configPath: "C:\\deepseek-http-test-data\\config.json",
  });
  const server = new BridgeHttpServer(config, service);
  await server.start();
  try {
    const headers = {
      authorization: "Bearer " + config.daemonToken,
      "content-type": "application/json",
    };
    const spawnResponse = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/spawn`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        topic: "visual spawn",
        task: "inspect the screenshot",
        visual_context: "Direct observations: a red banner is visible",
      }),
    });
    assert.equal(spawnResponse.status, 202);

    const continueResponse = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/continue`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        agent_id: "agent_visual",
        task: "continue from the screenshot",
        visual_context: "Interpretation: the banner indicates a failed build",
        allow_respawn: true,
      }),
    });
    assert.equal(continueResponse.status, 202);

    assert.equal(calls.length, 2);
    assert.equal((calls[0]?.input as { visualContext?: string }).visualContext, "Direct observations: a red banner is visible");
    assert.equal((calls[1]?.input as { visualContext?: string }).visualContext, "Interpretation: the banner indicates a failed build");
    assert.equal((calls[1]?.input as { allowRespawn?: boolean }).allowRespawn, true, "allow_respawn maps to the continue input");
  } finally {
    await server.stop();
  }
});

test("HTTP rejects a non-boolean allow_respawn with a typed 400", async () => {
  const calls: Array<{ kind: string; input: unknown }> = [];
  const service = {
    continueJob: async (input: unknown) => {
      calls.push({ kind: "continue", input });
      return { accepted: true };
    },
  } as unknown as BridgeService;
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: await freePort(),
    daemonToken: "http-test-token",
    dataDir: "C:\\deepseek-http-test-data",
    configPath: "C:\\deepseek-http-test-data\\config.json",
  });
  const server = new BridgeHttpServer(config, service);
  await server.start();
  try {
    const headers = {
      authorization: "Bearer " + config.daemonToken,
      "content-type": "application/json",
    };
    const response = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/continue`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        agent_id: "agent_1",
        task: "continue",
        allow_respawn: "yes",
      }),
    });
    assert.equal(response.status, 400);
    const body = await response.json() as Record<string, unknown>;
    assert.equal(body.code, "invalid_request");
    assert.equal(calls.length, 0, "the service is never reached with an invalid flag");
  } finally {
    await server.stop();
  }
});

test("bridge dispatcher timeouts exceed the maximum follow window plus margin", () => {
  const maxFollowMs = FOLLOW_MAX_TOTAL_MINUTES * 60_000;
  assert.equal(FOLLOW_MAX_TOTAL_MINUTES, 70);
  assert.ok(BRIDGE_HEADERS_TIMEOUT_MS >= maxFollowMs + 10 * 60_000, "headers timeout must sit safely above 60 min wait + 10 min grace");
  assert.ok(BRIDGE_BODY_TIMEOUT_MS >= maxFollowMs + 10 * 60_000, "body timeout must sit safely above the maximum follow window");
});

test("BridgeHttpClient uses the injected dispatcher and surfaces nested cause codes", async () => {
  const http = createHttpServer((_request, response) => {
    setTimeout(() => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end("{}");
    }, 1_500);
  });
  await listenHttp(http);
  const address = http.address();
  assert.ok(address && typeof address === "object");
  try {
    const config = createDefaultConfig({
      daemonHost: "127.0.0.1",
      daemonPort: address.port,
      daemonToken: "http-test-token",
      dataDir: "C:\\deepseek-http-test-data",
      configPath: "C:\\deepseek-http-test-data\\config.json",
    });
    const fastTimeout = new Agent({ headersTimeout: 250, bodyTimeout: 250 });
    const client = new BridgeHttpClient(config, fastTimeout);
    await assert.rejects(() => client.call("/v1/jobs/anything"), /UND_ERR_HEADERS_TIMEOUT/);
    const normalClient = new BridgeHttpClient(config);
    const value = await normalClient.get("/health");
    assert.deepEqual(value, {});
  } finally {
    await closeHttp(http);
  }
});

test("BridgeHttpClient surfaces fetch connection causes like ECONNREFUSED", async () => {
  const probe = createServer();
  await new Promise<void>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => resolve());
  });
  const address = probe.address();
  assert.ok(address && typeof address === "object");
  await new Promise<void>((resolve) => probe.close(() => resolve()));
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: (address as AddressInfo).port,
    daemonToken: "http-test-token",
    dataDir: "C:\\deepseek-http-test-data",
    configPath: "C:\\deepseek-http-test-data\\config.json",
  });
  const client = new BridgeHttpClient(config);
  await assert.rejects(() => client.call("/v1/jobs/anything"), /ECONNREFUSED/);
});

test("doctor health dispatcher stays far below the bridge follow timeouts", () => {
  assert.ok(DOCTOR_HEALTH_TIMEOUT_MS < BRIDGE_HEADERS_TIMEOUT_MS, "doctor probe must not inherit the 90-minute follow timeouts");
  assert.ok(DOCTOR_HEALTH_TIMEOUT_MS <= 10_000, "doctor probe must be bounded to a few seconds");
});

test("doctor health dispatcher bounds an unresponsive daemon on the caller side", async () => {
  const http = createHttpServer(() => {
    // Stall forever: an unresponsive daemon must not hold doctor for 90 minutes.
  });
  await listenHttp(http);
  const address = http.address();
  assert.ok(address && typeof address === "object");
  try {
    const dispatcher = createDoctorHealthDispatcher(250);
    const start = Date.now();
    await assert.rejects(
      () => fetch(`http://127.0.0.1:${address.port}/health`, { dispatcher }),
      (error: unknown) => {
        const message = error instanceof Error ? error.message + (error.cause instanceof Error ? ": " + error.cause.message : "") : String(error);
        return /timeout/i.test(message);
      },
    );
    assert.ok(Date.now() - start < 3_000, "doctor health probe must abort well below the follow timeouts");
    await dispatcher.close();
  } finally {
    await closeHttp(http);
  }
});

async function listenHttp(server: ReturnType<typeof createHttpServer>): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
}

async function closeHttp(server: NetServer): Promise<void> {
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

test("HTTP surfaces stable typed 400/404/409/500 error codes in the JSON body", async () => {
  const service = {
    spawn: async () => {
      throw new InvalidRequestError("Unknown model route: nonsense", "unknown_route", { route: "nonsense" });
    },
    consult: async () => {
      throw new UnknownAgentError("agent_missing");
    },
    follow: async () => {
      throw new ConflictError("Agent is busy with job job_9", "busy");
    },
    abort: async () => {
      throw new NotFoundError("No persisted result is available for job job_9");
    },
  } as unknown as BridgeService;
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: await freePort(),
    daemonToken: "http-error-token",
    dataDir: "C:\\deepseek-http-error-data",
    configPath: "C:\\deepseek-http-error-data\\config.json",
  });
  const server = new BridgeHttpServer(config, service);
  await server.start();
  try {
    const headers = {
      authorization: "Bearer " + config.daemonToken,
      "content-type": "application/json",
    };
    const post = async (pathname: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> => {
      const response = await fetch(`http://${config.daemonHost}:${config.daemonPort}${pathname}`, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
      });
      return { status: response.status, body: await response.json() as Record<string, unknown> };
    };

    const route = await post("/v1/jobs/spawn", { topic: "t", task: "t", model_route: "nonsense" });
    assert.equal(route.status, 400);
    assert.equal(route.body.code, "unknown_route");
    assert.equal(route.body.status, 400);
    assert.deepEqual(route.body.details, { route: "nonsense" });

    const agent = await post("/v1/jobs/consult", { agent_id: "agent_missing" });
    assert.equal(agent.status, 404);
    assert.equal(agent.body.code, "unknown_agent");

    const busy = await post("/v1/jobs/follow", { agent_id: "agent_busy" });
    assert.equal(busy.status, 409);
    assert.equal(busy.body.code, "busy");

    const missing = await post("/v1/jobs/abort", { agent_id: "agent_x" });
    assert.equal(missing.status, 404);
    assert.equal(missing.body.code, "not_found");

    const badBody = await post("/v1/jobs/spawn", "not json");
    assert.equal(badBody.status, 400);
    assert.equal(badBody.body.code, "invalid_request");
  } finally {
    await server.stop();
  }
});

test("HTTP maps BridgeBusyError to 409 with retry and jobId without text sniffing", async () => {
  const service = {
    spawn: async () => {
      throw new BridgeBusyError("job_busy_1");
    },
  } as unknown as BridgeService;
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: await freePort(),
    daemonToken: "http-busy-token",
    dataDir: "C:\\deepseek-http-busy-data",
    configPath: "C:\\deepseek-http-busy-data\\config.json",
  });
  const server = new BridgeHttpServer(config, service);
  await server.start();
  try {
    const response = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/spawn`, {
      method: "POST",
      headers: {
        authorization: "Bearer " + config.daemonToken,
        "content-type": "application/json",
      },
      body: JSON.stringify({ topic: "t", task: "t" }),
    });
    assert.equal(response.status, 409);
    const body = await response.json() as Record<string, unknown>;
    assert.equal(body.code, "busy");
    assert.equal(body.retry, false);
    assert.equal(body.jobId, "job_busy_1");
    assert.ok(body.error && typeof body.error === "string" && body.error.length > 0);
  } finally {
    await server.stop();
  }
});

test("unknown errors map to a stable 500 internal code", async () => {
  const service = {
    spawn: async () => {
      throw new Error("unexpected internal failure");
    },
  } as unknown as BridgeService;
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: await freePort(),
    daemonToken: "http-500-token",
    dataDir: "C:\\deepseek-http-500-data",
    configPath: "C:\\deepseek-http-500-data\\config.json",
  });
  const server = new BridgeHttpServer(config, service);
  await server.start();
  try {
    const response = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/spawn`, {
      method: "POST",
      headers: {
        authorization: "Bearer " + config.daemonToken,
        "content-type": "application/json",
      },
      body: JSON.stringify({ topic: "t", task: "t" }),
    });
    assert.equal(response.status, 500);
    const body = await response.json() as Record<string, unknown>;
    assert.equal(body.code, "internal");
    assert.equal(body.status, 500);
  } finally {
    await server.stop();
  }
});

test("unauthorized requests return a structured typed 401 body", async () => {
  const service = {
    spawn: async () => {
      throw new Error("must never be reached without a valid token");
    },
  } as unknown as BridgeService;
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: await freePort(),
    daemonToken: "http-401-token",
    dataDir: "C:\\deepseek-http-401-data",
    configPath: "C:\\deepseek-http-401-data\\config.json",
  });
  const server = new BridgeHttpServer(config, service);
  await server.start();
  try {
    const response = await fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/spawn`, {
      method: "POST",
      headers: {
        authorization: "Bearer wrong-token",
        "content-type": "application/json",
      },
      body: JSON.stringify({ topic: "t", task: "t" }),
    });
    assert.equal(response.status, 401);
    const body = await response.json() as Record<string, unknown>;
    assert.equal(body.error, "Unauthorized");
    assert.equal(body.code, "unauthorized");
    assert.equal(body.status, 401);
  } finally {
    await server.stop();
  }
});

test("BridgeHttpClient propagates the daemon's structured code as a typed BridgeHttpError", async () => {
  const http = createHttpServer((_request, response) => {
    response.writeHead(400, { "content-type": "application/json" });
    response.end(JSON.stringify({ error: "Model route is disabled: pro-max", code: "route_disabled", status: 400, details: { route: "pro-max" } }));
  });
  await listenHttp(http);
  const address = http.address();
  assert.ok(address && typeof address === "object");
  try {
    const config = createDefaultConfig({
      daemonHost: "127.0.0.1",
      daemonPort: address.port,
      daemonToken: "http-client-token",
      dataDir: "C:\\deepseek-http-client-data",
      configPath: "C:\\deepseek-http-client-data\\config.json",
    });
    const client = new BridgeHttpClient(config);
    await assert.rejects(
      () => client.call("/v1/jobs/spawn", { topic: "t", task: "t", model_route: "pro-max" }),
      (error: unknown) => {
        assert.ok(error instanceof BridgeHttpError);
        assert.equal(error.status, 400);
        assert.equal(error.code, "route_disabled");
        assert.deepEqual(error.details, { route: "pro-max" });
        assert.ok(error instanceof BridgeError === false);
        return true;
      },
    );
  } finally {
    await closeHttp(http);
  }
});
