import test from "node:test";
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { OpenCodeClient } from "../../src/opencode/client.js";

test("OpenCode SSE client reconnects after a stream ends", async () => {
  let connections = 0;
  const server = createServer((request, response) => {
    if (request.url !== "/event") {
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
      properties: { sessionID: "session_reconnect", status: "idle" },
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
  const events: string[] = [];
  try {
    await client.subscribe((event) => {
      events.push(event.type);
      if (events.length >= 2) controller.abort();
    }, controller.signal);
    assert.deepEqual(events, ["session.status", "session.status"]);
    assert.equal(connections, 2);
  } finally {
    await close(server);
  }
});

async function listen(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
}

async function close(server: Server): Promise<void> {
  await new Promise<void>((resolve) => server.close(() => resolve()));
}
