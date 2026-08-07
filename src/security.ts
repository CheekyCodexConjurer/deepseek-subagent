import { createHash, randomBytes } from "node:crypto";
import { access, chmod, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

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

export function assertInside(root: string, candidate: string): string {
  const absoluteRoot = path.resolve(root);
  const absoluteCandidate = path.resolve(candidate);
  const relative = path.relative(absoluteRoot, absoluteCandidate);
  if (relative === "" || (!relative.startsWith(".." + path.sep) && relative !== ".." && !path.isAbsolute(relative))) {
    return absoluteCandidate;
  }
  throw new Error("Path escapes allowed root: " + candidate);
}

export function validateContextFiles(root: string, files: string[]): string[] {
  return files.map((file) => assertInside(root, path.isAbsolute(file) ? file : path.join(root, file)));
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
