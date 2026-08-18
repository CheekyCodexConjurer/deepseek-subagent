import { createHash, randomBytes } from "node:crypto";
import { access, chmod, mkdir, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { InvalidRequestError } from "./errors.js";

const SECRET_PATTERNS = [
  /(authorization\s*:\s*bearer\s+)[^\s]+/gi,
  /(authorization\s*:\s*basic\s+)[^\s]+/gi,
  /(bearer\s+)[A-Za-z0-9._~+/=-]+/gi,
  /((?:api[_-]?key|token|password|secret)\s*[=:]\s*)[^\s,;]+/gi,
  /(-----BEGIN [^-]+-----)[\s\S]*?(-----END [^-]+-----)/gi,
];

export function redactSecrets(value: string): string {
  let output = value;
  for (const pattern of SECRET_PATTERNS) {
    output = output.replace(pattern, (_match, prefix: string, suffix?: string) => {
      return suffix ? prefix + "[REDACTED]" + suffix : prefix + "[REDACTED]";
    });
  }
  return output;
}

export function redactUnknown(value: unknown): unknown {
  const seen = new WeakSet<object>();
  const visit = (current: unknown): unknown => {
    if (typeof current === "string") return redactSecrets(current);
    if (current === null || typeof current !== "object") return current;
    if (seen.has(current)) return "[REDACTED_CIRCULAR]";
    seen.add(current);
    if (Array.isArray(current)) return current.map((item) => visit(item));
    const output: Record<string, unknown> = {};
    for (const [key, child] of Object.entries(current as Record<string, unknown>)) {
      output[key] = /authorization|api[_-]?key|token|password|secret|credential/i.test(key)
        ? "[REDACTED]"
        : visit(child);
    }
    return output;
  };
  return visit(value);
}

export function hashPrompt(prompt: string): string {
  return createHash("sha256").update(prompt, "utf8").digest("hex");
}

export function newId(prefix: string): string {
  return prefix + "_" + Date.now().toString(36) + "_" + randomBytes(8).toString("hex");
}

export function normalizeTitle(topic: string): string {
  const words = topic
    .replace(/[^a-zA-Z0-9À-ÿ]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 5);
  if (words.length === 0) return "DeepSeek Task";
  return words
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

export function truncate(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;
  return value.slice(0, Math.max(0, maxLength - 1)) + "…";
}

export function isLoopbackHost(host: string): boolean {
  const normalized = host.toLowerCase();
  return normalized === "localhost" || normalized === "127.0.0.1" || normalized === "::1" || normalized === "[::1]";
}

export function assertLoopbackUrl(rawUrl: string): URL {
  const url = new URL(rawUrl);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("OpenCode URL must use http or https");
  }
  if (!isLoopbackHost(url.hostname)) {
    throw new Error("OpenCode URL must resolve to loopback");
  }
  if (url.username || url.password) {
    throw new Error("OpenCode URL must not embed credentials; configure them separately");
  }
  return url;
}

export function isInside(root: string, candidate: string): boolean {
  const absoluteRoot = path.resolve(root);
  const absoluteCandidate = path.resolve(candidate);
  const relative = path.relative(absoluteRoot, absoluteCandidate);
  return relative === "" || (!relative.startsWith(".." + path.sep) && relative !== ".." && !path.isAbsolute(relative));
}

export function isSamePath(pathA: string, pathB: string): boolean {
  const resolvedA = path.resolve(pathA);
  const resolvedB = path.resolve(pathB);
  if (process.platform === "win32") {
    return resolvedA.toLowerCase() === resolvedB.toLowerCase();
  }
  return resolvedA === resolvedB;
}

export function defaultGlobalGeminiContextPath(): string {
  return path.join(os.homedir(), ".gemini", "config", "GEMINI.md");
}

export function shouldIncludeGlobalGeminiContext(task: string, topic = ""): boolean {
  const content = `${topic}\n${task}`;
  const patterns = [
    /\bmcps?\b/i,
    /\bmcp[-_]/i,
    /\bprompt[-_]?pad\b/i,
    /\bagents\.md\b/i,
    /\bgemini\.md\b/i,
    /\bskills?\b/i,
  ];
  return patterns.some((pattern) => pattern.test(content));
}

export function assertInside(root: string, candidate: string): string {
  const absoluteRoot = path.resolve(root);
  const absoluteCandidate = path.resolve(candidate);
  const relative = path.relative(absoluteRoot, absoluteCandidate);
  if (relative === "" || (!relative.startsWith(".." + path.sep) && relative !== ".." && !path.isAbsolute(relative))) {
    return absoluteCandidate;
  }
  throw new Error("Path escapes allowed root: " + candidate);
}

export function validateContextFiles(
  root: string,
  files: string[],
  allowedExternalFiles: string[] = [defaultGlobalGeminiContextPath()],
): string[] {
  return files.map((file) => {
    const candidate = path.isAbsolute(file) ? path.resolve(file) : path.resolve(root, file);
    if (isInside(root, candidate)) {
      return candidate;
    }
    if (allowedExternalFiles.some((allowed) => isSamePath(allowed, candidate))) {
      return candidate;
    }
    throw new Error("Path escapes allowed root: " + candidate);
  });
}

export const DEFAULT_MAX_CONTEXT_FILE_BYTES = 1_000_000;

export type ContextFileValidationReason = "outside_workspace" | "missing" | "not_a_regular_file" | "unreadable" | "too_large" | "inside_worktrees_directory";

export interface ContextFileValidationErrorDetails {
  file: string;
  reason: ContextFileValidationReason;
  sizeBytes?: number;
  maxBytes?: number;
}

/**
 * Validates every context file before any workspace/worktree/session/job side
 * effect: containment inside the workspace root (or an explicitly allowlisted
 * external global context file), existence, regular readable file, and
 * bounded size. Oversized input is rejected with a stable typed 400, never
 * silently truncated.
 */
export async function validateContextFilesStrict(
  root: string,
  files: string[],
  maxBytes = DEFAULT_MAX_CONTEXT_FILE_BYTES,
  allowedExternalFiles: string[] = [defaultGlobalGeminiContextPath()],
): Promise<string[]> {
  const resolved: string[] = [];
  for (const file of files) {
    const candidate = path.isAbsolute(file) ? path.resolve(file) : path.resolve(root, file);
    const inside = isInside(root, candidate);
    const isAllowedExternal = !inside && allowedExternalFiles.some((allowed) => isSamePath(allowed, candidate));
    if (!inside && !isAllowedExternal) {
      throw new InvalidRequestError(
        "context file is outside the requested workspace: " + file,
        "context_file_invalid",
        { file, reason: "outside_workspace" } satisfies ContextFileValidationErrorDetails,
      );
    }
    const info = await stat(candidate).catch((error: unknown) => {
      const code = error && typeof error === "object" && "code" in error
        ? (error as { code?: unknown }).code
        : undefined;
      if (code === "ENOENT") {
        throw new InvalidRequestError(
          "context file does not exist: " + file,
          "context_file_invalid",
          { file, reason: "missing" } satisfies ContextFileValidationErrorDetails,
        );
      }
      throw error;
    });
    if (!info.isFile()) {
      throw new InvalidRequestError(
        "context file is not a regular file: " + file,
        "context_file_invalid",
        { file, reason: "not_a_regular_file" } satisfies ContextFileValidationErrorDetails,
      );
    }
    const readable = await access(candidate).then(() => true).catch(() => false);
    if (!readable) {
      throw new InvalidRequestError(
        "context file is not readable: " + file,
        "context_file_invalid",
        { file, reason: "unreadable" } satisfies ContextFileValidationErrorDetails,
      );
    }
    if (info.size > maxBytes) {
      throw new InvalidRequestError(
        "context file exceeds the configured size limit (" + maxBytes + " bytes): " + file,
        "context_file_invalid",
        {
          file,
          reason: "too_large",
          sizeBytes: info.size,
          maxBytes,
        } satisfies ContextFileValidationErrorDetails,
      );
    }
    resolved.push(candidate);
  }
  return resolved;
}

export async function ensurePrivateDir(directory: string): Promise<void> {
  await mkdir(directory, { recursive: true });
  try {
    await chmod(directory, 0o700);
  } catch {
    // Windows ACLs are managed by the installer; chmod is best effort.
  }
}

export async function writePrivateFile(filePath: string, contents: string): Promise<void> {
  await ensurePrivateDir(path.dirname(filePath));
  await writeFile(filePath, contents, { encoding: "utf8", mode: 0o600 });
  try {
    await chmod(filePath, 0o600);
  } catch {
    // Windows ACLs are managed by the installer; chmod is best effort.
  }
}

export async function writePrivateFileExclusive(filePath: string, contents: string): Promise<boolean> {
  await ensurePrivateDir(path.dirname(filePath));
  try {
    await writeFile(filePath, contents, { encoding: "utf8", mode: 0o600, flag: "wx" });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
    throw error;
  }
  try {
    await chmod(filePath, 0o600);
  } catch {
    // Windows ACLs are managed by the installer; chmod is best effort.
  }
  return true;
}

export async function canRead(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

export function defaultWorkspace(): string {
  return process.cwd();
}

export function defaultUserDataRoot(): string {
  if (process.platform === "win32") {
    return path.join(process.env.LOCALAPPDATA ?? path.join(os.homedir(), "AppData", "Local"), "DeepSeek Sub-Agent");
  }
  if (process.platform === "darwin") {
    return path.join(os.homedir(), "Library", "Application Support", "DeepSeek Sub-Agent");
  }
  return path.join(process.env.XDG_DATA_HOME ?? path.join(os.homedir(), ".local", "share"), "deepseek-subagent");
}
