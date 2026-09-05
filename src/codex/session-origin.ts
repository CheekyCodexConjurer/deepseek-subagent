import { open, readdir, stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import os from "node:os";

export interface TaskSessionOrigin {
  threadId: string;
  source: string | null;
  isExec: boolean;
  filePath?: string | undefined;
}

export interface SessionOriginOptions {
  sessionsDir?: string | null | undefined;
  codexHome?: string | null | undefined;
  env?: Record<string, string | undefined> | undefined;
  homedirFn?: (() => string) | undefined;
  maxHeaderBytes?: number | undefined;
  maxDepth?: number | undefined;
  maxDirs?: number | undefined;
}

export function isValidUuid(id: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);
}

export function isValidPath(val: unknown): val is string {
  return typeof val === "string" && val.trim().length > 0 && !val.includes("\0");
}

export function resolveSessionsDir(options?: {
  sessionsDir?: string | null | undefined;
  codexHome?: string | null | undefined;
  env?: Record<string, string | undefined> | undefined;
  homedirFn?: (() => string) | undefined;
}): string | null {
  // 1. Explicit options.sessionsDir
  if (isValidPath(options?.sessionsDir)) {
    return path.resolve(options.sessionsDir.trim());
  }

  const env = options?.env ?? process.env;

  // 2. CODEX_SESSIONS_DIR env
  if (isValidPath(env.CODEX_SESSIONS_DIR)) {
    return path.resolve(env.CODEX_SESSIONS_DIR.trim());
  }

  // 3. CODEX_HOME/sessions
  const explicitHome = options?.codexHome ?? env.CODEX_HOME;
  if (isValidPath(explicitHome)) {
    return path.join(path.resolve(explicitHome.trim()), "sessions");
  }

  // 4. Standard user home .codex/sessions with Windows USERPROFILE fallback
  let userHome: string | null = null;
  try {
    const homedir = options?.homedirFn ? options.homedirFn() : os.homedir();
    if (isValidPath(homedir)) {
      userHome = homedir.trim();
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
    return path.join(path.resolve(userHome), ".codex", "sessions");
  }

  return null;
}

export async function findRolloutFilePath(
  threadId: string,
  sessionsDir: string,
  maxDepth = 4,
  maxDirs = 100,
): Promise<string | null> {
  if (!isValidUuid(threadId) || !isValidPath(sessionsDir) || !existsSync(sessionsDir)) {
    return null;
  }

  const normalizedTarget = threadId.toLowerCase();
  let dirsVisited = 0;

  async function walk(currentDir: string, currentDepth: number): Promise<string | null> {
    if (currentDepth > maxDepth || dirsVisited >= maxDirs) {
      return null;
    }
    dirsVisited++;

    let entries;
    try {
      entries = await readdir(currentDir, { withFileTypes: true });
    } catch {
      return null;
    }

    // 1. Check files in current directory first (bounded by basename, no file content read)
    for (const entry of entries) {
      if (entry.isFile() && entry.name.toLowerCase().endsWith(".jsonl")) {
        const lowerName = entry.name.toLowerCase();
        if (lowerName.includes(normalizedTarget)) {
          return path.join(currentDir, entry.name);
        }
      }
    }

    // 2. Sort subdirectories descending (e.g. recent dates first)
    const subdirs = entries
      .filter((e) => e.isDirectory())
      .map((e) => e.name)
      .sort((a, b) => b.localeCompare(a));

    for (const sub of subdirs) {
      const fullSubPath = path.join(currentDir, sub);
      const found = await walk(fullSubPath, currentDepth + 1);
      if (found) {
        return found;
      }
    }

    return null;
  }

  return walk(sessionsDir, 1);
}

export interface SessionMetaHeaderResult {
  id: string;
  source: string | null;
  originator?: string | undefined;
}

export async function readSessionMetaHeader(
  filePath: string,
  expectedThreadId?: string,
  maxHeaderBytes = 64 * 1024,
): Promise<SessionMetaHeaderResult | null> {
  if (!isValidPath(filePath)) {
    return null;
  }

  let fd;
  try {
    fd = await open(filePath, "r");
  } catch {
    return null;
  }

  let text = "";
  let bytesRead = 0;
  try {
    const buffer = Buffer.alloc(maxHeaderBytes);
    const readResult = await fd.read(buffer, 0, maxHeaderBytes, 0);
    bytesRead = readResult.bytesRead;
    if (bytesRead <= 0) {
      return null;
    }
    text = buffer.toString("utf8", 0, bytesRead);
  } catch {
    return null;
  } finally {
    try {
      await fd.close();
    } catch {}
  }

  const newlineIdx = text.indexOf("\n");
  // If no newline was encountered and buffer filled maxHeaderBytes,
  // line 0 exceeds the bounded header limit (>64k). Fail closed to null.
  if (newlineIdx === -1 && bytesRead >= maxHeaderBytes) {
    return null;
  }

  const headerText = newlineIdx !== -1 ? text.slice(0, newlineIdx).trim() : text.trim();
  if (!headerText.startsWith("{") || !headerText.endsWith("}")) {
    return null;
  }

  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(headerText) as Record<string, unknown>;
  } catch {
    return null;
  }

  if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
    return null;
  }

  // Strictly require top-level type 'session_meta'
  if (typeof obj.type !== "string" || obj.type.trim().toLowerCase() !== "session_meta") {
    return null;
  }

  // Strictly require obj.payload object directly (no fallback to obj or obj.session_meta)
  if (!obj.payload || typeof obj.payload !== "object" || Array.isArray(obj.payload)) {
    return null;
  }
  const payload = obj.payload as Record<string, unknown>;

  // Strictly extract raw ID directly from payload.id or payload.session_id as exact UUID
  const rawId =
    typeof payload.id === "string" && payload.id.trim()
      ? payload.id.trim()
      : typeof payload.session_id === "string" && payload.session_id.trim()
        ? payload.session_id.trim()
        : null;

  if (!rawId || !isValidUuid(rawId)) {
    return null;
  }

  // Authoritative header ID validation: never trust filename alone without validating header ID
  if (expectedThreadId && rawId.toLowerCase() !== expectedThreadId.trim().toLowerCase()) {
    return null;
  }

  // Strictly extract source directly from payload.source string only (no regex, no nested extra)
  const source =
    typeof payload.source === "string" && payload.source.trim().length > 0
      ? payload.source.trim()
      : null;

  // Extract originator directly from payload.originator string if present
  const originator =
    typeof payload.originator === "string" && payload.originator.trim().length > 0
      ? payload.originator.trim()
      : undefined;

  return {
    id: rawId,
    source,
    originator,
  };
}

export async function detectTaskSessionOrigin(
  threadId: string,
  options?: SessionOriginOptions,
): Promise<TaskSessionOrigin | null> {
  if (!isValidUuid(threadId)) {
    return null;
  }

  const sessionsDir = resolveSessionsDir(options);
  if (!sessionsDir) {
    return null;
  }

  const filePath = await findRolloutFilePath(
    threadId,
    sessionsDir,
    options?.maxDepth ?? 4,
    options?.maxDirs ?? 100,
  );
  if (!filePath) {
    return null;
  }

  const header = await readSessionMetaHeader(
    filePath,
    threadId,
    options?.maxHeaderBytes ?? 64 * 1024,
  );
  if (!header) {
    return null;
  }

  const isExec = typeof header.source === "string" && header.source.trim().toLowerCase() === "exec";

  return {
    threadId: header.id,
    source: header.source,
    isExec,
    filePath,
  };
}
