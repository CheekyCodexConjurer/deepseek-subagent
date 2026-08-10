import test from "node:test";
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { OpenCodeClient } from "../../src/opencode/client.js";
import type { OpenCodeEvent } from "../../src/types.js";

const GLOBAL_EVENT_PATH = "/global/event";

test(
  "OpenCode SSE client subscribes through /global/event, normalizes GlobalEvent wrappers, and reconnects after a stream ends",
  { timeout: 10_000 },
  async () => {
    let connections = 0;
    const server = createServer((request, response) => {
      if (request.url !== GLOBAL_EVENT_PATH) {
        response.writeHead(404);
        response.end();
        return;
      }
      connections += 1;
      response.writeHead(200, {
        "content-type": "text/event-stream",
        connection: "keep-alive",
      });
      response.write("data: " + JSON.stringify({
        directory: "C:\\work",
        payload: {
          id: "evt_wrapped",
          type: "session.status",
          properties: { sessionID: "session_wrapped", status: "idle" },
        },
      }) + "\n\n");
      if (connections === 1) response.end();
    });
    await listen(server);
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const client = new OpenCodeClient({
      baseUrl: "http://127.0.0.1:" + address.port,
      reconnectMaxMs: 5,
    });
    const controller = new AbortController();
    const events: OpenCodeEvent[] = [];
    try {
      await client.subscribe((event) => {
        events.push(event);
        if (events.length >= 2) controller.abort();
      }, controller.signal);
      assert.equal(connections, 2);
      assert.equal(events.length, 2);
      for (const event of events) {
        assert.equal(event.type, "session.status");
        assert.deepEqual(event.properties, { sessionID: "session_wrapped", status: "idle" });
        assert.equal(event.id, "evt_wrapped");
        const raw = event.raw as { directory?: string; payload?: { type?: string } };
        assert.equal(raw.directory, "C:\\work");
        assert.equal(raw.payload?.type, "session.status");
      }
    } finally {
      await close(server);
    }
  },
);

test(
  "OpenCode SSE client keeps unwrapped event compatibility",
  { timeout: 10_000 },
  async () => {
    let connections = 0;
    const server = createServer((request, response) => {
      if (request.url !== GLOBAL_EVENT_PATH) {
        response.writeHead(404);
        response.end();
        return;
      }
      connections += 1;
      response.writeHead(200, {
        "content-type": "text/event-stream",
        connection: "keep-alive",
      });
      response.write("data: " + JSON.stringify({
        type: "session.status",
        properties: { sessionID: "session_unwrapped", status: "idle" },
      }) + "\n\n");
      response.end();
    });
    await listen(server);
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const client = new OpenCodeClient({
      baseUrl: "http://127.0.0.1:" + address.port,
      reconnectMaxMs: 5,
    });
    const controller = new AbortController();
    const events: OpenCodeEvent[] = [];
    try {
      await client.subscribe((event) => {
        events.push(event);
        controller.abort();
      }, controller.signal);
      assert.equal(events.length, 1);
      assert.equal(events[0].type, "session.status");
      assert.deepEqual(events[0].properties, { sessionID: "session_unwrapped", status: "idle" });
      assert.deepEqual(events[0].raw, {
        type: "session.status",
        properties: { sessionID: "session_unwrapped", status: "idle" },
      });
    } finally {
      await close(server);
    }
  },
);

test(
  "OpenCode SSE client reconnects after stream inactivity on a half-open connection",
  { timeout: 10_000 },
  async () => {
    let connections = 0;
    const server = createServer((request, response) => {
      if (request.url !== GLOBAL_EVENT_PATH) {
        response.writeHead(404);
        response.end();
        return;
      }
      connections += 1;
      response.writeHead(200, {
        "content-type": "text/event-stream",
        connection: "keep-alive",
      });
      const sessionID = connections === 1 ? "session_stalled" : "session_recovered";
      response.write("data: " + JSON.stringify({
        directory: "C:\\work",
        payload: {
          type: "session.status",
          properties: { sessionID, status: "idle" },
        },
      }) + "\n\n");
      if (connections === 1) return;
    });
    await listen(server);
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const client = new OpenCodeClient({
      baseUrl: "http://127.0.0.1:" + address.port,
      reconnectMaxMs: 5,
      streamInactivityMs: 60,
    });
    const controller = new AbortController();
    const events: OpenCodeEvent[] = [];
    try {
      await client.subscribe((event) => {
        events.push(event);
        if (events.length >= 2) controller.abort();
      }, controller.signal);
      assert.equal(connections, 2);
      assert.deepEqual(events.map((event) => event.properties.sessionID), [
        "session_stalled",
        "session_recovered",
      ]);
    } finally {
      await close(server);
    }
  },
);

test(
  "OpenCode SSE client treats keepalive comment bytes as liveness without emitting events",
  { timeout: 10_000 },
  async () => {
    let connections = 0;
    const server = createServer((request, response) => {
      if (request.url !== GLOBAL_EVENT_PATH) {
        response.writeHead(404);
        response.end();
        return;
      }
      connections += 1;
      response.writeHead(200, {
        "content-type": "text/event-stream",
        connection: "keep-alive",
      });
      response.write("data: " + JSON.stringify({
        directory: "C:\\work",
        payload: {
          type: "session.status",
          properties: { sessionID: "session_keepalive", status: "idle" },
        },
      }) + "\n\n");
      const keepalive = setInterval(() => {
        response.write(": keepalive\n\n");
      }, 30);
      response.on("close", () => clearInterval(keepalive));
    });
    await listen(server);
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const client = new OpenCodeClient({
      baseUrl: "http://127.0.0.1:" + address.port,
      reconnectMaxMs: 5,
      streamInactivityMs: 80,
    });
    const controller = new AbortController();
    const events: OpenCodeEvent[] = [];
    try {
      const subscribed = client.subscribe((event) => {
        events.push(event);
      }, controller.signal);
      await delay(200);
      assert.equal(connections, 1, "keepalive bytes must reset the inactivity watchdog");
      assert.deepEqual(events.map((event) => event.type), ["session.status"]);
      controller.abort();
      await subscribed;
    } finally {
      await close(server);
    }
  },
);

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function listen(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
}

async function close(server: Server): Promise<void> {
  await new Promise<void>((resolve) => server.close(() => resolve()));
}
