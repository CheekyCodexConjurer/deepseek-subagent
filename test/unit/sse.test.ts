import test from "node:test";
import assert from "node:assert/strict";
import { SseParser, parseJsonSseEvent } from "../../src/sse.js";

test("parses complete SSE frames without splitting ordinary lines", () => {
  const parser = new SseParser();
  const events = parser.feed("id: one\n" + "data: {\"type\":\"message\"}\n" + "data: second\n\n");
  assert.equal(events.length, 1);
  assert.equal(events[0]?.id, "one");
  assert.equal(events[0]?.data, "{\"type\":\"message\"}\nsecond");
});

test("handles CRLF frames split across chunks", () => {
  const parser = new SseParser();
  assert.deepEqual(parser.feed("data: {\"ok\":"), []);
  const events = parser.feed("true}\r\n\r\n");
  assert.deepEqual(parseJsonSseEvent(events[0]!), { ok: true });
});
