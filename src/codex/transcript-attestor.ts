import { open, readdir, readFile, stat } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { isValidUuid } from "./cli-resolver.js";

export interface TranscriptAttestationMatch {
  jobId: string;
  threadId: string;
  turnId: string;
  itemId: string;
  timestamp?: string | number | null | undefined;
  server?: string | undefined;
  tool?: string | undefined;
}

export interface TranscriptAttestationOptions {
  sessionsDir?: string | null | undefined;
  callerHint?: {
    threadId?: string | null | undefined;
    turnId?: string | null | undefined;
  } | undefined;
  jobCreatedAt?: string | null | undefined;
  maxFiles?: number | undefined;
  acceptedServers?: string[] | undefined;
  acceptedTools?: string[] | undefined;
  retries?: number | undefined;
  retryDelayMs?: number | undefined;
  maxBytesPerFile?: number | undefined;
}

export const DEFAULT_ACCEPTED_SERVERS = ["subagents", "deepseek-subagent", "deepseek"];
export const DEFAULT_ACCEPTED_TOOLS = [
  "subagents_spawn",
  "subagents_continue",
  "deepseek_spawn",
  "deepseek_continue",
];

export function isValidPath(val: unknown): val is string {
  return typeof val === "string" && val.trim().length > 0 && !val.includes("\0");
}

export function resolveSessionsDir(
  explicitDir?: string | null | undefined,
  env: NodeJS.ProcessEnv = process.env,
  homedirFn: () => string = os.homedir,
): string | null {
  // 1. Explicit options.sessionsDir
  if (isValidPath(explicitDir)) {
    return explicitDir.trim();
  }

  // 2. CODEX_SESSIONS_DIR
  if (isValidPath(env.CODEX_SESSIONS_DIR)) {
    return env.CODEX_SESSIONS_DIR.trim();
  }

  // 3. CODEX_HOME/sessions
  if (isValidPath(env.CODEX_HOME)) {
    return path.join(env.CODEX_HOME.trim(), "sessions");
  }

  // 4. Standard user home .codex/sessions with Windows USERPROFILE fallback when needed
  let userHome: string | null = null;
  try {
    const home = homedirFn();
    if (isValidPath(home)) {
      userHome = home.trim();
    }
  } catch {
    userHome = null;
  }

  if (!userHome && isValidPath(env.USERPROFILE)) {
    userHome = env.USERPROFILE.trim();
  }

  if (!userHome && isValidPath(env.HOME)) {
    userHome = env.HOME.trim();
  }

  if (userHome) {
    return path.join(userHome, ".codex", "sessions");
  }

  return null;
}

export async function readFileTail(filePath: string, maxBytes = 5 * 1024 * 1024): Promise<string> {
  if (!isValidPath(filePath)) {
    return "";
  }
  let st;
  try {
    st = await stat(filePath);
  } catch {
    return "";
  }
  if (st.size <= maxBytes) {
    return await readFile(filePath, "utf8");
  }
  const fd = await open(filePath, "r");
  try {
    const buffer = Buffer.alloc(maxBytes);
    const startPos = st.size - maxBytes;
    await fd.read(buffer, 0, maxBytes, startPos);
    const text = buffer.toString("utf8");
    const firstNewline = text.indexOf("\n");
    return firstNewline >= 0 ? text.slice(firstNewline + 1) : text;
  } finally {
    await fd.close();
  }
}

export async function findRecentJsonlFiles(dir: string, maxDepth = 4): Promise<string[]> {
  if (!isValidPath(dir)) {
    return [];
  }
  const results: Array<{ filePath: string; mtimeMs: number }> = [];

  async function walk(currentDir: string, depth: number) {
    if (depth > maxDepth) return;
    let entries;
    try {
      entries = await readdir(currentDir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath, depth + 1);
      } else if (entry.isFile() && entry.name.endsWith(".jsonl")) {
        try {
          const st = await stat(fullPath);
          results.push({ filePath: fullPath, mtimeMs: st.mtimeMs });
        } catch {}
      }
    }
  }

  await walk(dir, 1);
  results.sort((a, b) => b.mtimeMs - a.mtimeMs);
  return results.map((r) => r.filePath);
}

export class TranscriptAttestor {
  private readonly configuredSessionsDir: string | null;
  private readonly env: NodeJS.ProcessEnv;

  constructor(options?: {
    sessionsDir?: string | null | undefined;
    env?: NodeJS.ProcessEnv | undefined;
  }) {
    this.configuredSessionsDir = isValidPath(options?.sessionsDir) ? options.sessionsDir.trim() : null;
    this.env = options?.env ?? process.env;
  }

  get defaultSessionsDir(): string | null {
    return resolveSessionsDir(this.configuredSessionsDir, this.env);
  }

  async attestJob(
    jobId: string,
    options?: TranscriptAttestationOptions,
  ): Promise<TranscriptAttestationMatch | null> {
    const retries = Math.max(1, options?.retries ?? 1);
    const retryDelay = options?.retryDelayMs ?? 50;

    for (let attempt = 0; attempt < retries; attempt++) {
      const match = await this.attestJobOnce(jobId, options);
      if (match) return match;
      if (attempt < retries - 1) {
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
      }
    }
    return null;
  }

  private async attestJobOnce(
    jobId: string,
    options?: TranscriptAttestationOptions,
  ): Promise<TranscriptAttestationMatch | null> {
    const explicitDir = isValidPath(options?.sessionsDir)
      ? options.sessionsDir.trim()
      : this.configuredSessionsDir;
    const sessionsDir = resolveSessionsDir(explicitDir, this.env);

    if (!sessionsDir) {
      return null;
    }
    const maxFiles = options?.maxFiles ?? 50;
    const acceptedServers = (options?.acceptedServers ?? DEFAULT_ACCEPTED_SERVERS).map((s) => s.toLowerCase());
    const acceptedTools = (options?.acceptedTools ?? DEFAULT_ACCEPTED_TOOLS).map((t) => t.toLowerCase());

    const jsonlFiles = await findRecentJsonlFiles(sessionsDir);
    const candidateFiles = jsonlFiles.slice(0, maxFiles);

    if (candidateFiles.length === 0) {
      return null;
    }

    const matches: TranscriptAttestationMatch[] = [];

    for (const file of candidateFiles) {
      let content: string;
      try {
        content = await readFileTail(file, options?.maxBytesPerFile ?? 5 * 1024 * 1024);
      } catch {
        continue;
      }

      if (!content.includes(jobId)) {
        continue;
      }

      const lines = content.split(/\r?\n/);
      for (const line of lines) {
        if (!line.includes(jobId)) continue;
        const trimmed = line.trim();
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) continue;

        let obj: Record<string, unknown>;
        try {
          obj = JSON.parse(trimmed) as Record<string, unknown>;
        } catch {
          continue;
        }

        if (obj.type !== "event_msg") continue;
        const payload = obj.payload as Record<string, unknown> | undefined;
        if (!payload || typeof payload !== "object") continue;
        if (payload.type !== "item_completed") continue;

        const threadId = typeof payload.thread_id === "string" ? payload.thread_id.trim() : null;
        const turnId = typeof payload.turn_id === "string" ? payload.turn_id.trim() : null;
        if (!threadId || !isValidUuid(threadId)) continue;
        if (!turnId || !isValidUuid(turnId)) continue;

        const item = payload.item as Record<string, unknown> | undefined;
        if (!item || typeof item !== "object") continue;
        const itemId = typeof item.id === "string" ? item.id.trim() : "";
        if (!itemId) continue;

        const itemType = String(item.type || "").toLowerCase();
        if (itemType !== "mcptoolcall") continue;

        const itemStatus = String(item.status || "").toLowerCase();
        if (itemStatus !== "completed") continue;

        const itemServer = String(item.server || "").toLowerCase();
        if (!acceptedServers.includes(itemServer)) continue;

        const itemTool = String(item.tool || "").toLowerCase();
        if (!acceptedTools.includes(itemTool)) continue;

        const result = item.result as Record<string, unknown> | undefined;
        const structured = result?.structuredContent as Record<string, unknown> | undefined;
        const metaTech = (result?._meta as Record<string, unknown> | undefined)?.technical as Record<string, unknown> | undefined;
        const reportedJobId = (typeof structured?.jobId === "string" && structured.jobId) ||
          (typeof metaTech?.jobId === "string" && metaTech.jobId) ||
          null;

        if (reportedJobId !== jobId) continue;
        const isAccepted = structured?.accepted === true || metaTech?.accepted === true;
        if (!isAccepted) continue;

        // Check staleness if job creation time is provided (e.g. events older than 24 hours)
        if (options?.jobCreatedAt) {
          const jobTime = new Date(options.jobCreatedAt).getTime();
          const eventTime = obj.timestamp
            ? new Date(String(obj.timestamp)).getTime()
            : (typeof payload.completed_at_ms === "number" ? payload.completed_at_ms : null);
          if (eventTime && (jobTime - eventTime) > 24 * 3600_000) {
            continue;
          }
        }

        matches.push({
          jobId,
          threadId,
          turnId,
          itemId,
          timestamp: (obj.timestamp as string | undefined) ?? (payload.completed_at_ms as number | undefined),
          server: itemServer,
          tool: itemTool,
        });
      }
    }

    if (matches.length === 0) {
      return null;
    }

    // Check unique (threadId, turnId, itemId).
    // Duplicate lines with the exact same (threadId, turnId, itemId) are allowed idempotently.
    // Conflicting duplicates (differing threadId, turnId, or itemId for the same jobId) MUST be rejected.
    const first = matches[0]!;
    for (let i = 1; i < matches.length; i++) {
      const m = matches[i]!;
      if (m.threadId !== first.threadId || m.turnId !== first.turnId || m.itemId !== first.itemId) {
        // Conflicting duplicate matches found! Fail closed.
        return null;
      }
    }

    // Check caller hint consistency: if caller provided hints, they must match the attested binding.
    if (options?.callerHint) {
      const hintThread = options.callerHint.threadId?.trim();
      const hintTurn = options.callerHint.turnId?.trim();
      if (hintThread && hintThread.toLowerCase() !== first.threadId.toLowerCase()) {
        return null;
      }
      if (hintTurn && hintTurn.toLowerCase() !== first.turnId.toLowerCase()) {
        return null;
      }
    }

    return first;
  }
}
