import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { readdir, stat } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { DatabaseSync } from "node:sqlite";
import { detectTaskSessionOrigin, type TaskSessionOrigin } from "./session-origin.js";

export interface CodexCliCandidate {
  executablePath: string;
  version: string;
}

export interface TaskProvenance {
  threadId: string | null;
  error?: string;
}

export function isValidUuid(id: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);
}

export function resolveCodexTaskProvenance(env: Record<string, string | undefined> = process.env): TaskProvenance {
  const threadId = env.CODEX_THREAD_ID?.trim();
  const sessionId = env.CODEX_SESSION_ID?.trim();

  if (!threadId && !sessionId) {
    return { threadId: null };
  }

  if (threadId && !isValidUuid(threadId)) {
    return { threadId: null, error: `Invalid CODEX_THREAD_ID format: "${threadId}" is not a valid UUID` };
  }

  if (sessionId && !isValidUuid(sessionId)) {
    return { threadId: null, error: `Invalid CODEX_SESSION_ID format: "${sessionId}" is not a valid UUID` };
  }

  if (threadId && sessionId && threadId !== sessionId) {
    return {
      threadId: null,
      error: `Provenance mismatch: CODEX_THREAD_ID "${threadId}" does not match CODEX_SESSION_ID "${sessionId}"`,
    };
  }

  return { threadId: threadId ?? sessionId ?? null };
}

export function parseSemver(versionStr: string): { major: number; minor: number; patch: number } | null {
  const match = versionStr.match(/(\d+)\.(\d+)\.(\d+)/);
  if (!match) return null;
  return {
    major: parseInt(match[1]!, 10),
    minor: parseInt(match[2]!, 10),
    patch: parseInt(match[3]!, 10),
  };
}

export function isCompatibleCodexCli(versionStr: string): boolean {
  const parsed = parseSemver(versionStr);
  if (!parsed) return false;
  // Versions < 0.150.0 (like 0.145.0, 0.130.0) cannot deserialize Desktop schemas (unknown variant functionCallOutput)
  if (parsed.major > 0) return true;
  if (parsed.major === 0 && parsed.minor >= 150) return true;
  return false;
}

export async function probeCodexVersion(executablePath: string): Promise<string | null> {
  return new Promise<string | null>((resolve) => {
    try {
      const child = spawn(executablePath, ["--version"], {
        shell: false,
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      });

      let stdout = "";
      let stderr = "";
      child.stdout?.on("data", (chunk) => {
        stdout += String(chunk);
      });
      child.stderr?.on("data", (chunk) => {
        stderr += String(chunk);
      });

      const timer = setTimeout(() => {
        try {
          child.kill();
        } catch {}
        resolve(null);
      }, 5000);

      child.on("error", () => {
        clearTimeout(timer);
        resolve(null);
      });

      child.on("close", (code) => {
        clearTimeout(timer);
        if (code !== 0 && !stdout) {
          resolve(null);
          return;
        }
        const output = (stdout + " " + stderr).trim();
        const semver = parseSemver(output);
        if (semver) {
          resolve(`${semver.major}.${semver.minor}.${semver.patch}`);
        } else {
          resolve(null);
        }
      });
    } catch {
      resolve(null);
    }
  });
}

export interface CodexCliResolverConfig {
  codexCliPath?: string | null | undefined;
  codexAppServerCommand?: string | null | undefined;
}

export async function discoverCodexCandidates(config?: CodexCliResolverConfig): Promise<string[]> {
  const candidates: string[] = [];

  const explicit = process.env.CODEX_CLI_PATH ?? config?.codexCliPath;
  if (explicit && explicit.trim().length > 0) {
    candidates.push(path.resolve(explicit.trim()));
  }

  const appServerCmd = config?.codexAppServerCommand;
  if (appServerCmd && appServerCmd.trim().length > 0 && appServerCmd !== "codex") {
    candidates.push(path.resolve(appServerCmd.trim()));
  }

  if (process.platform === "win32") {
    const localAppData = process.env.LOCALAPPDATA ?? path.join(os.homedir(), "AppData", "Local");
    const codexBinDir = path.join(localAppData, "OpenAI", "Codex", "bin");
    try {
      const entries = await readdir(codexBinDir).catch(() => []);
      for (const entry of entries) {
        const full = path.join(codexBinDir, entry);
        try {
          const st = await stat(full);
          if (st.isDirectory()) {
            const nestedExe = path.join(full, "codex.exe");
            const nestedSt = await stat(nestedExe).catch(() => null);
            if (nestedSt && nestedSt.isFile()) {
              candidates.push(path.resolve(nestedExe));
            }
          } else if (entry.toLowerCase() === "codex.exe") {
            candidates.push(path.resolve(full));
          }
        } catch {}
      }
    } catch {}
  }

  // Also include PATH candidate
  candidates.push("codex");

  // Deduplicate
  const seen = new Set<string>();
  const deduped: string[] = [];
  for (const c of candidates) {
    const normalized = c.toLowerCase();
    if (!seen.has(normalized)) {
      seen.add(normalized);
      deduped.push(c);
    }
  }

  return deduped;
}

export async function resolveCodexCli(options?: {
  config?: CodexCliResolverConfig;
  candidates?: string[];
}): Promise<CodexCliCandidate | null> {
  const candidatePaths = options?.candidates ?? (await discoverCodexCandidates(options?.config));
  const probed: CodexCliCandidate[] = [];

  for (const p of candidatePaths) {
    const version = await probeCodexVersion(p);
    if (version && isCompatibleCodexCli(version)) {
      probed.push({ executablePath: p, version });
    }
  }

  if (probed.length === 0) return null;

  // Sort descending by semver to pick highest compatible version
  probed.sort((a, b) => {
    const sa = parseSemver(a.version)!;
    const sb = parseSemver(b.version)!;
    if (sa.major !== sb.major) return sb.major - sa.major;
    if (sa.minor !== sb.minor) return sb.minor - sa.minor;
    return sb.patch - sa.patch;
  });

  return probed[0] ?? null;
}

export interface ProcessRunOptions {
  stdin?: string;
  timeoutMs?: number;
  expectedThreadId?: string;
  expectedMarker?: string;
  onAccepted?: () => void;
  onSpawn?: (child: import("node:child_process").ChildProcess) => void;
  isAccepted?: (obj: Record<string, unknown>) => boolean;
}

export interface ProcessRunResult {
  code: number | null;
  stdout: string;
  stderr: string;
  timedOut?: boolean;
  accepted?: boolean;
}

export type ProcessRunner = (
  executable: string,
  args: string[],
  options?: ProcessRunOptions,
) => Promise<ProcessRunResult>;

export type ProvenanceState = "none" | "matched" | "mismatched";

export class JsonlAcceptanceDetector {
  private provenance: ProvenanceState = "none";
  private _accepted = false;
  private _correlatedTurn = false;

  constructor(readonly expectedThreadId?: string) {}

  get isAccepted(): boolean {
    return this._accepted;
  }

  get hasCorrelatedTurn(): boolean {
    return this._correlatedTurn;
  }

  get provenanceState(): ProvenanceState {
    return this.provenance;
  }

  processEvent(obj: Record<string, unknown>): boolean {
    if (this._accepted) return true;
    if (!obj || typeof obj !== "object") return false;

    const rawType =
      (typeof obj.type === "string" && obj.type) ||
      (typeof obj.event === "string" && obj.event) ||
      (typeof obj.kind === "string" && obj.kind) ||
      "";
    const type = rawType.toLowerCase().trim();
    if (!type) return false;

    // Explicit failure or error events must never be treated as accepted
    if (
      type.includes("failed") ||
      type.includes("error") ||
      type.includes("rejected") ||
      type.includes("cancelled") ||
      type.includes("canceled")
    ) {
      return false;
    }

    const eventThreadId =
      (typeof obj.thread_id === "string" && obj.thread_id.trim()) ||
      (typeof obj.threadId === "string" && obj.threadId.trim()) ||
      (typeof obj.session_id === "string" && obj.session_id.trim()) ||
      (typeof obj.sessionId === "string" && obj.sessionId.trim()) ||
      (typeof (obj.thread as any)?.id === "string" && (obj.thread as any).id.trim()) ||
      (typeof (obj.turn as any)?.thread_id === "string" && (obj.turn as any).thread_id.trim()) ||
      (typeof (obj.turn as any)?.threadId === "string" && (obj.turn as any).threadId.trim()) ||
      null;

    const normalizedExpected = this.expectedThreadId?.trim().toLowerCase();

    // 1. Thread provenance events: establishes provenance ONLY and MUST NOT accept.
    if (type === "thread.started" || type === "thread_started") {
      if (normalizedExpected) {
        if (eventThreadId && eventThreadId.toLowerCase() === normalizedExpected) {
          this.provenance = "matched";
        } else {
          this.provenance = "mismatched";
        }
      } else {
        this.provenance = "matched";
      }
      return false;
    }

    // 2. Turn-scoped events
    const isTurnStarted =
      type === "turn.started" ||
      type === "turn_started" ||
      type === "turn_created" ||
      type === "turn_resumed" ||
      type === "turn/start" ||
      type === "turn/started";

    const isTurnScoped =
      isTurnStarted ||
      type === "turn.completed" ||
      type === "turn_completed" ||
      type === "turn/completed" ||
      type === "turn/finished" ||
      type === "turn/ended";

    if (isTurnScoped) {
      if (normalizedExpected) {
        // If the turn event itself carries a thread id:
        if (eventThreadId) {
          if (eventThreadId.toLowerCase() === normalizedExpected) {
            this.provenance = "matched";
            this._correlatedTurn = true;
            this._accepted = true;
            return true;
          } else {
            this.provenance = "mismatched";
            return false;
          }
        }

        // Unscoped turn event: accept only after matching-thread provenance has been established.
        // A mismatched thread.started followed by unscoped turn.started must remain rejected.
        if (this.provenance === "matched") {
          this._correlatedTurn = true;
          this._accepted = true;
          return true;
        }
        return false;
      } else {
        // No expectedThreadId specified: accept turn event
        this._correlatedTurn = true;
        this._accepted = true;
        return true;
      }
    }

    // 3. Item events: "Do not accept item.* alone before a correlated turn."
    const isItemEvent =
      type === "item.started" ||
      type === "item.updated" ||
      type === "item.completed" ||
      type.startsWith("item.") ||
      type === "item";

    if (isItemEvent) {
      if (this._correlatedTurn) {
        this._accepted = true;
        return true;
      }
      return false;
    }

    // 4. Generic accepted event
    if (type === "accepted") {
      if (normalizedExpected) {
        if (eventThreadId && eventThreadId.toLowerCase() === normalizedExpected) {
          this.provenance = "matched";
          this._accepted = true;
          return true;
        }
        if (this.provenance === "matched") {
          this._accepted = true;
          return true;
        }
        return false;
      }
      this._accepted = true;
      return true;
    }

    return false;
  }

  processLine(line: string): boolean {
    const trimmed = line.trim();
    if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return false;
    try {
      const obj = JSON.parse(trimmed) as Record<string, unknown>;
      return this.processEvent(obj);
    } catch {
      return false;
    }
  }

  processText(text: string): boolean {
    const lines = text.split("\n");
    for (const line of lines) {
      if (this.processLine(line)) {
        return true;
      }
    }
    return this._accepted;
  }
}

export function isAcceptedTurnEvent(
  obj: Record<string, unknown>,
  expectedThreadId?: string,
): boolean {
  const detector = new JsonlAcceptanceDetector(expectedThreadId);
  return detector.processEvent(obj);
}

export const defaultProcessRunner: ProcessRunner = async (executable, args, options) => {
  return new Promise((resolve) => {
    try {
      const child = spawn(executable, args, {
        shell: false,
        windowsHide: true,
        stdio: ["pipe", "pipe", "pipe"],
      });

      options?.onSpawn?.(child);

      let stdout = "";
      let stderr = "";
      let lineBuffer = "";
      let settled = false;
      const detector = new JsonlAcceptanceDetector(options?.expectedThreadId);

      const timeoutMs = options?.timeoutMs ?? 30_000;
      let timer: NodeJS.Timeout | null = setTimeout(() => {
        if (settled) return;
        settled = true;
        try {
          child.kill();
        } catch {}
        resolve({ code: null, stdout, stderr, timedOut: true, accepted: false });
      }, timeoutMs);

      const checkLine = (line: string): boolean => {
        const trimmed = line.trim();
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return false;
        try {
          const obj = JSON.parse(trimmed) as Record<string, unknown>;
          if (options?.isAccepted) {
            return options.isAccepted(obj);
          }
          return detector.processEvent(obj);
        } catch {
          return false;
        }
      };

      const processIncomingText = (chunkText: string): boolean => {
        lineBuffer += chunkText;
        let newlineIdx: number;
        while ((newlineIdx = lineBuffer.indexOf("\n")) !== -1) {
          const line = lineBuffer.slice(0, newlineIdx);
          lineBuffer = lineBuffer.slice(newlineIdx + 1);
          if (checkLine(line)) {
            return true;
          }
        }
        return false;
      };

      const handleAccepted = () => {
        if (settled) return;
        settled = true;
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        options?.onAccepted?.();
        // Promptly resolve the transport promise upon turn acceptance.
        // Invariant: KEEP the CLI child and its stdio drain/error/close lifecycle alive
        // until natural completion; do NOT kill, detach, or unref it in a way that permits
        // the owner process to exit and interrupt/drop the turn.
        // Child stdout and stderr listeners continue draining chunks until natural close.
        resolve({ code: 0, stdout, stderr, accepted: true, timedOut: false });
      };

      child.stdout?.on("data", (chunk) => {
        const text = String(chunk);
        stdout += text;
        if (!settled && processIncomingText(text)) {
          handleAccepted();
        }
      });

      child.stderr?.on("data", (chunk) => {
        stderr += String(chunk);
      });

      if (options?.stdin !== undefined && child.stdin) {
        child.stdin.write(options.stdin);
        child.stdin.end();
      }

      child.on("error", (err) => {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        if (!settled) {
          settled = true;
          resolve({ code: null, stdout, stderr: stderr + " " + String(err), timedOut: false, accepted: false });
        }
      });

      child.on("close", (code) => {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        if (!settled) {
          settled = true;
          let accepted = false;
          if (lineBuffer.trim() && checkLine(lineBuffer)) {
            accepted = true;
          }
          resolve({ code, stdout, stderr, timedOut: false, accepted });
        }
      });
    } catch (err) {
      resolve({ code: null, stdout: "", stderr: String(err), timedOut: false, accepted: false });
    }
  });
};

export interface CodexCliExecutionResult {
  success: boolean;
  activeWriter?: boolean | undefined;
  candidateIncompatible?: boolean | undefined;
  unknownOutcome?: boolean | undefined;
  accepted?: boolean | undefined;
  error?: string | null | undefined;
  executablePath?: string | undefined;
  version?: string | null | undefined;
  deliveryMode?: "queued" | "cli_resume" | undefined;
  messageId?: string | undefined;
}

export interface QueuedWakeReconciliationResult {
  found: boolean;
  messageId?: string | undefined;
}

export interface CodexCliCapabilities {
  compatible: boolean;
  version: string | null;
  queueSupported?: boolean | undefined;
}

export interface CodexCliTransport {
  deliverWake(
    threadId: string,
    marker: string,
  ): Promise<CodexCliExecutionResult>;
  probeCapabilities(executable?: string): Promise<CodexCliCapabilities>;
  reconcileQueuedWake?(
    threadId: string,
    marker: string,
    customDbPath?: string,
  ): Promise<QueuedWakeReconciliationResult>;
}

export function isStoredSchemaIncompatible(output: string): boolean {
  const lower = output.toLowerCase();
  return (
    lower.includes("unknown variant") ||
    lower.includes("functioncalloutput") ||
    lower.includes("schema incompatibility") ||
    lower.includes("deserializ") ||
    lower.includes("unsupported schema version") ||
    lower.includes("failed to deserialize") ||
    lower.includes("invalid schema")
  );
}

export function isActiveWriterConflict(output: string): boolean {
  const lower = output.toLowerCase();
  return lower.includes("active writer") || lower.includes("thread-store conflict");
}

export function isDeterministicQueueFallthroughError(output: string): boolean {
  const lower = output.toLowerCase();
  return (
    lower.includes("unloaded") ||
    lower.includes("no active session") ||
    lower.includes("no session") ||
    lower.includes("no_session") ||
    lower.includes("session not found") ||
    lower.includes("thread not found") ||
    lower.includes("session does not exist") ||
    lower.includes("thread does not exist") ||
    lower.includes("no rollout") ||
    lower.includes("not rolled out") ||
    lower.includes("feature not enabled") ||
    lower.includes("not enabled") ||
    lower.includes("not available") ||
    lower.includes("not supported") ||
    lower.includes("unknown subcommand") ||
    lower.includes("unrecognized subcommand") ||
    lower.includes("unknown command")
  );
}

export function parseQueueMessageId(stdout: string, stderr: string): string | undefined {
  try {
    const fullObj = JSON.parse(stdout.trim());
    if (fullObj && typeof fullObj === "object") {
      const id =
        (typeof fullObj.message_id === "string" && fullObj.message_id) ||
        (typeof fullObj.messageId === "string" && fullObj.messageId) ||
        (typeof fullObj.id === "string" && fullObj.id) ||
        undefined;
      if (id) return id;
    }
  } catch {}

  const lines = (stdout + "\n" + stderr).split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
      try {
        const obj = JSON.parse(trimmed);
        if (obj && typeof obj === "object") {
          const id =
            (typeof obj.message_id === "string" && obj.message_id) ||
            (typeof obj.messageId === "string" && obj.messageId) ||
            (typeof obj.id === "string" && obj.id) ||
            undefined;
          if (id) return id;
        }
      } catch {}
    }
  }

  const combined = (stdout + " " + stderr).trim();
  const textMatch = combined.match(/Queued message\s+(\S+)\s+for thread/i);
  if (textMatch && textMatch[1]) {
    return textMatch[1].replace(/^['"`]+|['"`.,]+$/g, "");
  }

  return undefined;
}

export function inspectJsonlTurnAcceptance(stdout: string, expectedThreadId?: string): boolean {
  const detector = new JsonlAcceptanceDetector(expectedThreadId);
  return detector.processText(stdout);
}

export function ensureLineTerminatedMarker(marker: string): string {
  if (marker.endsWith("\n") || marker.endsWith("\r")) {
    return marker;
  }
  return marker + "\n";
}

export interface CandidateCapabilities {
  executablePath: string;
  version: string | null;
  compatible: boolean;
  queueSupported: boolean;
  hasExecResume: boolean;
  semver: { major: number; minor: number; patch: number } | null;
}

export function rankCodexCliCandidates(candidates: CandidateCapabilities[]): CandidateCapabilities[] {
  return [...candidates].sort((a, b) => {
    // 1. Queue-capable AND compatible candidates rank highest
    const aQueueCapable = a.compatible && a.queueSupported;
    const bQueueCapable = b.compatible && b.queueSupported;
    if (aQueueCapable && !bQueueCapable) return -1;
    if (!aQueueCapable && bQueueCapable) return 1;

    // 2. Compatible candidates rank above incompatible candidates
    if (a.compatible && !b.compatible) return -1;
    if (!a.compatible && b.compatible) return 1;

    // 3. Among candidates in the same compatibility & queue tier, rank by compatible semver descending
    if (a.semver && b.semver) {
      if (b.semver.major !== a.semver.major) return b.semver.major - a.semver.major;
      if (b.semver.minor !== a.semver.minor) return b.semver.minor - a.semver.minor;
      if (b.semver.patch !== a.semver.patch) return b.semver.patch - a.semver.patch;
    } else if (a.semver && !b.semver) {
      return -1;
    } else if (!a.semver && b.semver) {
      return 1;
    }

    return 0;
  });
}

export async function probeCandidateCapabilities(
  executable: string,
  runner: ProcessRunner = defaultProcessRunner,
): Promise<CandidateCapabilities> {
  const versionRes = await runner(executable, ["--version"], { timeoutMs: 5000 });
  let version: string | null = null;
  let semver: { major: number; minor: number; patch: number } | null = null;
  if (versionRes.code === 0 || versionRes.stdout) {
    semver = parseSemver(versionRes.stdout + " " + versionRes.stderr);
    if (semver) {
      version = `${semver.major}.${semver.minor}.${semver.patch}`;
    }
  }

  const helpRes = await runner(executable, ["exec", "resume", "--help"], { timeoutMs: 5000 });
  const helpOutput = (helpRes.stdout + " " + helpRes.stderr).toLowerCase();
  const hasExecResume =
    helpRes.code === 0 &&
    (helpOutput.includes("exec resume") || helpOutput.includes("resume") || helpOutput.includes("session_id"));

  const queueHelpRes = await runner(executable, ["queue", "--help"], { timeoutMs: 5000 });
  const queueHelpOutput = (queueHelpRes.stdout + " " + queueHelpRes.stderr).toLowerCase();
  const hasQueue =
    queueHelpRes.code === 0 &&
    (queueHelpOutput.includes("queue") || queueHelpOutput.includes("--thread") || queueHelpOutput.includes("--message")) &&
    !queueHelpOutput.includes("exec resume");

  const semverCompatible = version !== null && isCompatibleCodexCli(version);
  const compatible = hasExecResume && semverCompatible;

  return {
    executablePath: executable,
    version,
    compatible,
    queueSupported: hasQueue,
    hasExecResume,
    semver,
  };
}

export function resolveQueueDbPath(options?: {
  dbPath?: string | undefined;
  codexHome?: string | undefined;
  env?: Record<string, string | undefined> | undefined;
}): string {
  if (options?.dbPath && options.dbPath.trim().length > 0) {
    return path.resolve(options.dbPath.trim());
  }

  const env = options?.env ?? process.env;
  const explicitHome = options?.codexHome ?? env.CODEX_HOME;
  if (explicitHome && explicitHome.trim().length > 0) {
    return path.join(path.resolve(explicitHome.trim()), "queue_1.sqlite");
  }

  const userHome = os.homedir() || env.USERPROFILE || env.HOME || ".";
  return path.join(path.resolve(userHome), ".codex", "queue_1.sqlite");
}

export async function reconcileQueuedWake(
  threadId: string,
  marker: string,
  options?: {
    dbPath?: string | undefined;
    codexHome?: string | undefined;
    env?: Record<string, string | undefined> | undefined;
  },
): Promise<QueuedWakeReconciliationResult> {
  if (!threadId || !marker) {
    return { found: false };
  }

  const dbPath = resolveQueueDbPath(options);
  if (!existsSync(dbPath)) {
    return { found: false };
  }

  let db: DatabaseSync | null = null;
  try {
    db = new DatabaseSync(dbPath, { readOnly: true });

    const tableCheck = db
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'queued_items'")
      .get() as { name?: string } | undefined;
    if (!tableCheck) {
      return { found: false };
    }

    const trimmedThread = threadId.trim();
    const trimmedMarker = marker.trim();

    const row = db
      .prepare(
        "SELECT id FROM queued_items WHERE thread_id = ? AND (instr(payload_json, ?) > 0 OR instr(payload_json, ?) > 0) ORDER BY rowid DESC LIMIT 1",
      )
      .get(trimmedThread, marker, trimmedMarker) as { id?: unknown } | undefined;

    if (row && typeof row.id === "string") {
      return { found: true, messageId: row.id };
    }
    return { found: false };
  } catch {
    return { found: false };
  } finally {
    if (db) {
      try {
        db.close();
      } catch {}
    }
  }
}

export interface DefaultCodexCliTransportOptions {
  config?: CodexCliResolverConfig | undefined;
  candidates?: string[] | undefined;
  runner?: ProcessRunner | undefined;
  codexHome?: string | undefined;
  queueDbPath?: string | undefined;
  sessionsDir?: string | undefined;
  env?: Record<string, string | undefined> | undefined;
  sessionOriginResolver?: ((threadId: string) => Promise<TaskSessionOrigin | null>) | undefined;
}

export class DefaultCodexCliTransport implements CodexCliTransport {
  private readonly config: CodexCliResolverConfig | undefined;
  private readonly candidateOverride: string[] | undefined;
  private readonly runner: ProcessRunner;
  private readonly codexHome: string | undefined;
  private readonly queueDbPath: string | undefined;
  private readonly sessionsDir: string | undefined;
  private readonly env: Record<string, string | undefined> | undefined;
  private readonly sessionOriginResolver:
    | ((threadId: string) => Promise<TaskSessionOrigin | null>)
    | undefined;

  constructor(options: DefaultCodexCliTransportOptions = {}) {
    this.config = options.config;
    this.candidateOverride = options.candidates;
    this.runner = options.runner ?? defaultProcessRunner;
    this.codexHome = options.codexHome;
    this.queueDbPath = options.queueDbPath;
    this.sessionsDir = options.sessionsDir;
    this.env = options.env;
    this.sessionOriginResolver = options.sessionOriginResolver;
  }

  async reconcileQueuedWake(
    threadId: string,
    marker: string,
    customDbPath?: string,
  ): Promise<QueuedWakeReconciliationResult> {
    return reconcileQueuedWake(threadId, marker, {
      dbPath: customDbPath ?? this.queueDbPath,
      codexHome: this.codexHome,
    });
  }

  async probeCapabilities(executable?: string): Promise<CodexCliCapabilities> {
    if (executable) {
      const single = await probeCandidateCapabilities(executable, this.runner);
      return {
        compatible: single.compatible,
        version: single.version,
        queueSupported: single.queueSupported,
      };
    }

    const candidatePaths = this.candidateOverride ?? (await discoverCodexCandidates(this.config));
    if (candidatePaths.length === 0) {
      return { compatible: false, version: null, queueSupported: false };
    }

    const probed: CandidateCapabilities[] = [];
    for (const candidate of candidatePaths) {
      probed.push(await probeCandidateCapabilities(candidate, this.runner));
    }

    const ranked = rankCodexCliCandidates(probed);
    const anyQueueSupported = ranked.some((c) => c.compatible && c.queueSupported);
    const top = ranked[0]!;

    return {
      compatible: top.compatible,
      version: top.version,
      queueSupported: anyQueueSupported,
    };
  }

  async deliverWake(threadId: string, marker: string): Promise<CodexCliExecutionResult> {
    const candidatePaths = this.candidateOverride ?? (await discoverCodexCandidates(this.config));
    if (candidatePaths.length === 0) {
      return { success: false, error: "No Codex CLI candidate executables found" };
    }

    let isExecSession = false;
    try {
      const origin = this.sessionOriginResolver
        ? await this.sessionOriginResolver(threadId)
        : await detectTaskSessionOrigin(threadId, {
            sessionsDir: this.sessionsDir,
            codexHome: this.codexHome,
            env: this.env,
          });
      if (origin?.isExec === true) {
        isExecSession = true;
      }
    } catch {
      isExecSession = false;
    }

    const markerPayload = ensureLineTerminatedMarker(marker);

    const probed: CandidateCapabilities[] = [];
    for (const candidate of candidatePaths) {
      probed.push(await probeCandidateCapabilities(candidate, this.runner));
    }

    const rankedCandidates = rankCodexCliCandidates(probed);
    const hasCompatibleCandidate = rankedCandidates.some((c) => c.compatible);

    let lastError: string | null = null;
    let lastExe: string | undefined = undefined;
    let lastVer: string | null = null;

    for (const probe of rankedCandidates) {
      const candidate = probe.executablePath;
      const compatible = probe.compatible;
      const version = probe.version;
      const queueSupported = probe.queueSupported === true;
      lastExe = candidate;
      lastVer = version;

      if (!compatible) {
        lastError = `Incompatible Codex CLI candidate ${candidate} (version: ${version ?? "unknown"})`;
        continue;
      }

      // For a queue-capable candidate, run queue first EXCEPT for proven exec sessions
      // Proven exec sessions must bypass queue and directly invoke existing exec resume
      // Desktop or unknown origin preserves queue-first
      if (queueSupported && !isExecSession) {
        const queueResult = await this.runner(
          candidate,
          ["queue", "--thread", threadId, "--message", marker],
          { timeoutMs: 30_000, expectedThreadId: threadId, expectedMarker: marker },
        );

        // Exit 0 is accepted terminal queued delivery
        if (queueResult.code === 0) {
          const messageId = parseQueueMessageId(queueResult.stdout, queueResult.stderr);
          return {
            success: true,
            accepted: true,
            deliveryMode: "queued",
            messageId,
            executablePath: candidate,
            version,
          };
        }

        const queueCombined = (queueResult.stdout + " " + queueResult.stderr).trim();

        // Timeout is unknownOutcome and must not resume blindly
        if (queueResult.timedOut) {
          return {
            success: false,
            unknownOutcome: true,
            deliveryMode: "queued",
            error: queueCombined || "CLI queue execution timed out",
            executablePath: candidate,
            version,
          };
        }

        // Schema incompatibility on queue command -> continue to next candidate
        if (isStoredSchemaIncompatible(queueCombined)) {
          lastError = `Schema incompatibility on ${candidate}: ${queueCombined}`;
          continue;
        }

        // Only deterministic unloaded/no-session/no-rollout errors fall through immediately to existing exec resume on same candidate
        if (!isDeterministicQueueFallthroughError(queueCombined)) {
          // Ambiguous output or non-deterministic error: unknownOutcome and must not resume blindly
          return {
            success: false,
            unknownOutcome: true,
            deliveryMode: "queued",
            error: queueCombined || `CLI queue failed with exit code ${queueResult.code}`,
            executablePath: candidate,
            version,
          };
        }
      }

      // Execute resume with stdin prompt ("-") (used for old CLI or after deterministic queue fallthrough)
      const result = await this.runner(
        candidate,
        ["exec", "resume", "--json", "--skip-git-repo-check", threadId, "-"],
        { stdin: markerPayload, timeoutMs: 30_000, expectedThreadId: threadId, expectedMarker: markerPayload },
      );

      const combined = (result.stdout + " " + result.stderr).trim();

      // Check stored-schema incompatibility
      if (isStoredSchemaIncompatible(combined)) {
        lastError = `Schema incompatibility on ${candidate}: ${combined}`;
        // Classify as candidate-incompatible and CONTINUE to next candidate
        continue;
      }

      // Check active writer
      if (isActiveWriterConflict(combined)) {
        return {
          success: false,
          activeWriter: true,
          deliveryMode: "cli_resume",
          error: (result.stderr || result.stdout).trim() || "Active writer conflict",
          executablePath: candidate,
          version,
        };
      }

      // Check turn acceptance (from runner early acceptance or by inspecting stdout)
      const accepted = result.accepted === true || inspectJsonlTurnAcceptance(result.stdout, threadId);
      if (accepted) {
        return {
          success: true,
          accepted: true,
          deliveryMode: "cli_resume",
          executablePath: candidate,
          version,
        };
      }

      // Check success
      if (result.code === 0) {
        return {
          success: true,
          accepted: true,
          deliveryMode: "cli_resume",
          executablePath: candidate,
          version,
        };
      }

      // Check unknown outcome (timeout or crash with output)
      if (result.timedOut || (result.code !== 0 && result.stdout.length > 0)) {
        // If unknown outcome after potential send, do NOT blindly try next candidate
        return {
          success: false,
          unknownOutcome: true,
          deliveryMode: "cli_resume",
          error: (result.stderr || result.stdout).trim() || (result.timedOut ? "CLI execution timed out" : `Exit code ${result.code}`),
          executablePath: candidate,
          version,
        };
      }

      lastError = (result.stderr || result.stdout).trim() || `Exit code ${result.code}`;
    }

    return {
      success: false,
      candidateIncompatible: !hasCompatibleCandidate ? true : undefined,
      error: lastError || "All Codex CLI candidates failed",
      executablePath: lastExe,
      version: lastVer,
    };
  }
}

export async function executeCodexCliResume(
  executablePath: string,
  threadId: string,
  markerPayload: string,
  options?: { timeoutMs?: number },
): Promise<{ success: boolean; activeWriter: boolean; error?: string }> {
  const runner = defaultProcessRunner;
  const terminatedMarker = ensureLineTerminatedMarker(markerPayload);
  const result = await runner(
    executablePath,
    ["exec", "resume", "--json", "--skip-git-repo-check", threadId, "-"],
    { stdin: terminatedMarker, timeoutMs: options?.timeoutMs ?? 30_000 },
  );

  const combined = (result.stdout + " " + result.stderr).toLowerCase();
  if (combined.includes("active writer") || combined.includes("thread-store conflict")) {
    return { success: false, activeWriter: true, error: (result.stderr || result.stdout).trim() };
  }
  if (result.code === 0) {
    return { success: true, activeWriter: false };
  }
  return {
    success: false,
    activeWriter: false,
    error: (result.stderr || result.stdout).trim() || (result.timedOut ? "codex exec resume timed out" : `Exit code ${result.code}`),
  };
}
