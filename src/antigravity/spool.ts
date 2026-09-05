import { readdir, readFile, stat, unlink } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { AGY_COMMAND, buildAgyArgs } from "./args.js";
import {
  ensurePrivateDir,
  isProcessAlive,
  newId,
  redactSecrets,
  writePrivateFile,
  writePrivateFileExclusive,
} from "../security.js";
import type {
  AntigravityAttemptManifest,
  AntigravityAttemptStatus,
  AntigravityHeartbeat,
  AntigravityRecoveryClaim,
} from "./types.js";

export const ANTIGRAVITY_HEARTBEAT_TTL_MS = 10_000;
export const ANTIGRAVITY_DEFAULT_MAX_OUTPUT_BYTES = 1_048_576;

export interface CreateAttemptInput {
  agentId: string;
  jobId: string;
  requestId: string;
  prompt: string;
  cwd: string;
  modelProviderId: string;
  modelId: string;
  modelVariant: string | null;
  modelRoute: string | null;
  command?: string | undefined;
  args?: string[] | undefined;
  timeoutMs?: number | null | undefined;
  sandbox?: boolean | undefined;
  addDirs?: string[] | undefined;
  dangerouslySkipPermissions?: boolean | undefined;
  parentAttemptId?: string | null | undefined;
  maxOutputBytes?: number | undefined;
  fence?: number | null | undefined;
}

export class AntigravitySpool {
  constructor(readonly dataDir: string) {}

  jobSpoolDir(jobId: string): string {
    return path.join(this.dataDir, "spool", "antigravity", jobId);
  }

  attemptDir(jobId: string, attemptId: string): string {
    return path.join(this.jobSpoolDir(jobId), attemptId);
  }

  async createAttempt(input: CreateAttemptInput): Promise<AntigravityAttemptManifest> {
    const attemptId = newId("attempt");
    const dir = this.attemptDir(input.jobId, attemptId);
    await ensurePrivateDir(dir);

    const command = input.command ?? AGY_COMMAND;
    const timeoutMs = typeof input.timeoutMs === "number" && input.timeoutMs > 0 ? input.timeoutMs : null;
    const sandbox = input.sandbox === true;
    const addDirs = [...new Set(input.addDirs ?? [])];
    const dangerouslySkipPermissions = input.dangerouslySkipPermissions === true;
    const maxOutputBytes = input.maxOutputBytes ?? ANTIGRAVITY_DEFAULT_MAX_OUTPUT_BYTES;

    const args = input.args ?? buildAgyArgs(input.prompt, {
      model: input.modelId,
      timeoutMs,
      sandbox,
      addDirs,
      dangerouslySkipPermissions,
    });

    const promptPath = path.join(dir, "prompt.txt");
    const stdoutPath = path.join(dir, "stdout.log");
    const stderrPath = path.join(dir, "stderr.log");
    const statusPath = path.join(dir, "status.json");
    const heartbeatPath = path.join(dir, "heartbeat.json");
    const cancelPath = path.join(dir, "cancel.signal");
    const manifestPath = path.join(dir, "manifest.json");

    // Write transient prompt file with mode 0600
    await writePrivateFile(promptPath, input.prompt);

    const manifest: AntigravityAttemptManifest = {
      schemaVersion: 1,
      agentId: input.agentId,
      jobId: input.jobId,
      requestId: input.requestId,
      attemptId,
      parentAttemptId: input.parentAttemptId ?? null,
      promptHash: "",
      promptPath,
      cwd: input.cwd,
      modelProviderId: input.modelProviderId,
      modelId: input.modelId,
      modelVariant: input.modelVariant,
      modelRoute: input.modelRoute,
      command,
      args,
      timeoutMs,
      sandbox,
      addDirs,
      dangerouslySkipPermissions,
      attemptDir: dir,
      stdoutPath,
      stderrPath,
      statusPath,
      heartbeatPath,
      cancelPath,
      createdAt: new Date().toISOString(),
      maxOutputBytes,
      fence: input.fence ?? 1,
    };

    await writePrivateFile(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
    return manifest;
  }

  async listAttempts(jobId: string): Promise<AntigravityAttemptManifest[]> {
    const root = this.jobSpoolDir(jobId);
    if (!existsSync(root)) return [];
    const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
    const manifests: AntigravityAttemptManifest[] = [];

    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const manifestPath = path.join(root, entry.name, "manifest.json");
      const manifest = await this.readManifest(manifestPath);
      if (manifest) manifests.push(manifest);
    }

    manifests.sort((a, b) => Date.parse(a.createdAt) - Date.parse(b.createdAt));
    return manifests;
  }

  async getLatestAttempt(jobId: string): Promise<AntigravityAttemptManifest | null> {
    const attempts = await this.listAttempts(jobId);
    return attempts.at(-1) ?? null;
  }

  async readManifest(manifestOrAttemptPath: string): Promise<AntigravityAttemptManifest | null> {
    const target = manifestOrAttemptPath.endsWith("manifest.json")
      ? manifestOrAttemptPath
      : path.join(manifestOrAttemptPath, "manifest.json");
    try {
      const raw = await readFile(target, "utf8");
      const parsed = JSON.parse(raw) as any;
      if (
        parsed?.schemaVersion === 1 &&
        typeof parsed.attemptId === "string" &&
        typeof parsed.jobId === "string" &&
        (typeof parsed.timeoutMs === "number" || parsed.timeoutMs === null || parsed.timeoutMs === undefined)
      ) {
        return {
          ...parsed,
          timeoutMs: typeof parsed.timeoutMs === "number" ? parsed.timeoutMs : null,
        } as AntigravityAttemptManifest;
      }
      return null;
    } catch {
      return null;
    }
  }

  private async resolveAttemptDir(attemptIdOrPath: string, jobId?: string): Promise<string | null> {
    if (path.isAbsolute(attemptIdOrPath)) {
      if (attemptIdOrPath.endsWith(".json") || attemptIdOrPath.endsWith(".signal") || attemptIdOrPath.endsWith(".txt") || attemptIdOrPath.endsWith(".log")) {
        return path.dirname(attemptIdOrPath);
      }
      return attemptIdOrPath;
    }
    if (jobId) {
      return this.attemptDir(jobId, attemptIdOrPath);
    }
    return await this.findAttemptDir(attemptIdOrPath);
  }

  async readStatus(attemptIdOrPath: string, jobId?: string): Promise<AntigravityAttemptStatus | null> {
    const dir = await this.resolveAttemptDir(attemptIdOrPath, jobId);
    if (!dir) return null;
    const target = path.join(dir, "status.json");
    try {
      const raw = await readFile(target, "utf8");
      const parsed = JSON.parse(raw) as AntigravityAttemptStatus;
      if (parsed?.schemaVersion === 1 && typeof parsed.attemptId === "string" && typeof parsed.status === "string") {
        return parsed;
      }
      return null;
    } catch {
      return null;
    }
  }

  async writeStatus(attemptIdOrDir: string, statusPayload: AntigravityAttemptStatus, jobId?: string): Promise<void> {
    const dir = await this.resolveAttemptDir(attemptIdOrDir, jobId);
    if (!dir) throw new Error("Attempt directory not found for: " + attemptIdOrDir);
    const statusPath = path.join(dir, "status.json");
    await writePrivateFile(statusPath, JSON.stringify(statusPayload, null, 2) + "\n");
    // Prompt cleanup rule: remove transient prompt.txt immediately after terminal status is safely written
    await this.cleanupPrompt(dir);
  }

  async readHeartbeat(attemptIdOrDir: string, jobId?: string): Promise<AntigravityHeartbeat | null> {
    const dir = await this.resolveAttemptDir(attemptIdOrDir, jobId);
    if (!dir) return null;
    const heartbeatPath = path.join(dir, "heartbeat.json");
    try {
      const raw = await readFile(heartbeatPath, "utf8");
      const parsed = JSON.parse(raw) as AntigravityHeartbeat;
      if (typeof parsed?.nonce === "string" && typeof parsed?.supervisorPid === "number" && typeof parsed?.updatedAt === "number") {
        return parsed;
      }
      return null;
    } catch {
      return null;
    }
  }

  async writeHeartbeat(attemptIdOrDir: string, heartbeat: AntigravityHeartbeat, jobId?: string): Promise<void> {
    const dir = await this.resolveAttemptDir(attemptIdOrDir, jobId);
    if (!dir) throw new Error("Attempt directory not found for: " + attemptIdOrDir);
    const heartbeatPath = path.join(dir, "heartbeat.json");
    await writePrivateFile(heartbeatPath, JSON.stringify(heartbeat, null, 2) + "\n");
  }

  isHeartbeatLive(heartbeat: AntigravityHeartbeat | null | undefined, ttlMs = ANTIGRAVITY_HEARTBEAT_TTL_MS): boolean {
    if (!heartbeat) return false;
    if (!isProcessAlive(heartbeat.supervisorPid)) return false;
    const now = Date.now();
    return (now - heartbeat.updatedAt) <= ttlMs;
  }

  async writeCancelSignal(attemptIdOrJobIdOrDir: string, reason = "Cancelled", jobId?: string): Promise<void> {
    const jobDir = this.jobSpoolDir(attemptIdOrJobIdOrDir);
    const payload = JSON.stringify({ requestedAt: new Date().toISOString(), reason: redactSecrets(reason) }, null, 2) + "\n";
    if (existsSync(jobDir)) {
      const cancelPath = path.join(jobDir, "cancel.signal");
      await writePrivateFile(cancelPath, payload);
      const attempts = await this.listAttempts(attemptIdOrJobIdOrDir).catch(() => []);
      for (const attempt of attempts) {
        await writePrivateFile(attempt.cancelPath, payload).catch(() => undefined);
      }
      return;
    }
    const dir = await this.resolveAttemptDir(attemptIdOrJobIdOrDir, jobId);
    if (!dir) return;
    const cancelPath = path.join(dir, "cancel.signal");
    await writePrivateFile(cancelPath, payload);
  }

  async writeDeadlineExtension(attemptIdOrJobIdOrDir: string, timeoutMs: number | null, jobId?: string): Promise<void> {
    const jobDir = this.jobSpoolDir(attemptIdOrJobIdOrDir);
    const payload = JSON.stringify({ timeoutMs, requestedAt: new Date().toISOString() }, null, 2) + "\n";
    if (existsSync(jobDir)) {
      const deadlinePath = path.join(jobDir, "deadline.json");
      await writePrivateFile(deadlinePath, payload);
      const attempts = await this.listAttempts(attemptIdOrJobIdOrDir).catch(() => []);
      for (const attempt of attempts) {
        await writePrivateFile(path.join(attempt.attemptDir, "deadline.json"), payload).catch(() => undefined);
      }
      return;
    }
    const dir = await this.resolveAttemptDir(attemptIdOrJobIdOrDir, jobId);
    if (!dir) return;
    const deadlinePath = path.join(dir, "deadline.json");
    await writePrivateFile(deadlinePath, payload);
  }

  async hasCancelSignal(attemptIdOrDir: string, jobId?: string): Promise<boolean> {
    const dir = await this.resolveAttemptDir(attemptIdOrDir, jobId);
    if (!dir) return false;
    const cancelPath = path.join(dir, "cancel.signal");
    if (existsSync(cancelPath)) return true;
    const parentCancel = path.join(path.dirname(dir), "cancel.signal");
    return existsSync(parentCancel);
  }

  async claimRecovery(jobId: string, claimant: string): Promise<boolean> {
    const dir = this.jobSpoolDir(jobId);
    await ensurePrivateDir(dir);
    const claimPath = path.join(dir, "recovery-claim.json");
    const payload: AntigravityRecoveryClaim = {
      claimedBy: claimant,
      claimedAt: new Date().toISOString(),
      jobId,
    };
    return await writePrivateFileExclusive(claimPath, JSON.stringify(payload, null, 2) + "\n");
  }

  async cleanupPrompt(attemptIdOrDir: string, jobId?: string): Promise<void> {
    const dir = await this.resolveAttemptDir(attemptIdOrDir, jobId);
    if (!dir) return;
    const promptPath = path.join(dir, "prompt.txt");
    await unlink(promptPath).catch(() => undefined);
  }

  private async findAttemptDir(attemptId: string): Promise<string | null> {
    const spoolBase = path.join(this.dataDir, "spool", "antigravity");
    if (!existsSync(spoolBase)) return null;
    const jobEntries = await readdir(spoolBase, { withFileTypes: true }).catch(() => []);
    for (const jobEntry of jobEntries) {
      if (!jobEntry.isDirectory()) continue;
      const candidate = path.join(spoolBase, jobEntry.name, attemptId);
      if (existsSync(candidate)) return candidate;
    }
    return null;
  }
}

// Standalone functional exports
export async function createAttemptSpool(dataDir: string, input: CreateAttemptInput): Promise<AntigravityAttemptManifest> {
  return new AntigravitySpool(dataDir).createAttempt(input);
}

export async function readAttemptManifest(manifestOrAttemptPath: string): Promise<AntigravityAttemptManifest | null> {
  return new AntigravitySpool("").readManifest(manifestOrAttemptPath);
}

export async function readAttemptStatus(statusOrAttemptPath: string): Promise<AntigravityAttemptStatus | null> {
  return new AntigravitySpool("").readStatus(statusOrAttemptPath);
}

export async function writeAttemptStatus(dataDir: string, attemptId: string, status: AntigravityAttemptStatus, jobId?: string): Promise<void> {
  return new AntigravitySpool(dataDir).writeStatus(attemptId, status, jobId);
}

export async function writeHeartbeat(dataDir: string, attemptId: string, heartbeat: AntigravityHeartbeat, jobId?: string): Promise<void> {
  return new AntigravitySpool(dataDir).writeHeartbeat(attemptId, heartbeat, jobId);
}

export function isHeartbeatLive(heartbeat: AntigravityHeartbeat | null | undefined, ttlMs = ANTIGRAVITY_HEARTBEAT_TTL_MS): boolean {
  return new AntigravitySpool("").isHeartbeatLive(heartbeat, ttlMs);
}

export async function claimRecovery(dataDir: string, jobId: string, claimant: string): Promise<boolean> {
  return new AntigravitySpool(dataDir).claimRecovery(jobId, claimant);
}

export async function cleanupPrompt(dataDir: string, attemptId: string, jobId?: string): Promise<void> {
  return new AntigravitySpool(dataDir).cleanupPrompt(attemptId, jobId);
}

export async function writeCancelSignal(dataDir: string, attemptId: string, reason?: string, jobId?: string): Promise<void> {
  return new AntigravitySpool(dataDir).writeCancelSignal(attemptId, reason, jobId);
}

export async function hasCancelSignal(dataDir: string, attemptId: string, jobId?: string): Promise<boolean> {
  return new AntigravitySpool(dataDir).hasCancelSignal(attemptId, jobId);
}
