import { redactSecrets } from "./security.js";

export interface ParsedSseEvent {
  id?: string;
  event?: string;
  data: string;
}

export class SseParser {
  private buffer = "";
  private eventName: string | undefined;
  private eventId: string | undefined;
  private dataLines: string[] = [];

  constructor(private readonly maxEventBytes = 2_000_000) {}

  feed(chunk: string): ParsedSseEvent[] {
    this.buffer += chunk;
    const events: ParsedSseEvent[] = [];
    let separator = this.findSeparator(this.buffer);
    while (separator) {
      const frame = this.buffer.slice(0, separator.index);
      this.buffer = this.buffer.slice(separator.index + separator.length);
      const event = this.consumeFrame(frame);
      if (event) events.push(event);
      separator = this.findSeparator(this.buffer);
    }
    if (Buffer.byteLength(this.buffer, "utf8") > this.maxEventBytes) {
      throw new Error("SSE frame exceeds " + this.maxEventBytes + " bytes");
    }
    return events;
  }

  flush(): ParsedSseEvent[] {
    if (!this.buffer && this.dataLines.length === 0) return [];
    const event = this.consumeFrame(this.buffer);
    this.buffer = "";
    return event ? [event] : [];
  }

  private findSeparator(value: string): { index: number; length: number } | null {
    const lf = value.indexOf("\n\n");
    const cr = value.indexOf("\r\r");
    const crlf = value.indexOf("\r\n\r\n");
    const candidates = [
      lf === -1 ? null : { index: lf, length: 2 },
      cr === -1 ? null : { index: cr, length: 2 },
      crlf === -1 ? null : { index: crlf, length: 4 },
    ].filter((candidate): candidate is { index: number; length: number } => candidate !== null);
    if (candidates.length === 0) return null;
    return candidates.sort((left, right) => left.index - right.index)[0] ?? null;
  }

  private consumeFrame(frame: string): ParsedSseEvent | null {
    const lines = frame.split(/\r?\n|\r/);
    for (const line of lines) {
      if (line === "" || line.startsWith(":")) continue;
      const colon = line.indexOf(":");
      const field = colon === -1 ? line : line.slice(0, colon);
      const value = colon === -1 ? "" : line.slice(colon + (line[colon + 1] === " " ? 2 : 1));
      if (field === "event") this.eventName = value;
      else if (field === "id") this.eventId = value;
      else if (field === "data") this.dataLines.push(value);
    }
    if (this.dataLines.length === 0) {
      this.eventName = undefined;
      this.eventId = undefined;
      return null;
    }
    const result: ParsedSseEvent = { data: this.dataLines.join("\n") };
    if (this.eventName !== undefined) result.event = this.eventName;
    if (this.eventId !== undefined) result.id = this.eventId;
    this.eventName = undefined;
    this.eventId = undefined;
    this.dataLines = [];
    return result;
  }
}

export function parseJsonSseEvent(event: ParsedSseEvent): Record<string, unknown> | null {
  try {
    const value: unknown = JSON.parse(event.data);
    return value && typeof value === "object" ? (value as Record<string, unknown>) : null;
  } catch {
    throw new Error("Invalid OpenCode SSE JSON: " + redactSecrets(event.data.slice(0, 400)));
  }
}
