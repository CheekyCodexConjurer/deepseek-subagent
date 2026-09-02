import { appendFile, readFile, unlink } from "node:fs/promises";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { newId, redactSecrets, writePrivateFile } from "../security.js";
import { parseAgyOutput } from "./parser.js";
import { AntigravitySpool } from "./spool.js";
import type {
  AntigravityAttemptManifest,
  AntigravityAttemptStatus,
  AntigravityHeartbeat,
  AntigravityResultStatus,
} from "./types.js";
import type { SpawnLike } from "./runner.js";

export interface SupervisorOptions {
  spoolDir: string;
  manifest: AntigravityAttemptManifest;
  spawnFn?: SpawnLike | undefined;
  killTreeFn?: ((pid: number) => Promise<void>) | undefined;
  heartbeatIntervalMs?: number | undefined;
  signal?: AbortSignal | undefined;
  onHeartbeat?: ((heartbeat: AntigravityHeartbeat) => void | Promise<void>) | undefined;
}

export class AntigravitySupervisor {
  private readonly spoolDir: string;
  private readonly manifest: AntigravityAttemptManifest;
  private readonly spawnFn: SpawnLike;
  private readonly killTreeFn: (pid: number) => Promise<void>;
  private readonly heartbeatIntervalMs: number;
  private readonly signal: AbortSignal | undefined;
  private readonly nonce: string;
  private readonly onHeartbeat?: ((heartbeat: AntigravityHeartbeat) => void | Promise<void>) | undefined;
  private child: ChildProcess | null = null;
  private settled = false;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private cancelWatcherTimer: NodeJS.Timeout | null = null;
  private timeoutTimer: NodeJS.Timeout | null = null;
  private effectiveTimeoutMs: number;
  private startTime = 0;
  private resetTimeoutFn: ((newTimeoutMs: number) => void) | null = null;

  constructor(options: SupervisorOptions) {
    this.spoolDir = options.spoolDir;
    this.manifest = options.manifest;
    this.spawnFn = options.spawnFn ?? ((command, args, opts) => spawn(command, args, opts as any));
    this.killTreeFn = options.killTreeFn ?? defaultKillTree;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 500;
    this.signal = options.signal;
    this.onHeartbeat = options.onHeartbeat;
    this.effectiveTimeoutMs = options.manifest.timeoutMs;
    this.nonce = newId("nonce");
  }

  extendTimeout(newTimeoutMs: number): void {
    if (this.resetTimeoutFn) {
      this.resetTimeoutFn(newTimeoutMs);
    } else if (newTimeoutMs > this.effectiveTimeoutMs) {
      this.effectiveTimeoutMs = newTimeoutMs;
    }
  }

  async run(): Promise<AntigravityAttemptStatus> {
    if (this.settled) throw new Error("Supervisor has already run");
    this.startTime = Date.now();

    // Write initial heartbeat
    await this.updateHeartbeat(null);

    // Start periodic heartbeat timer
    this.heartbeatTimer = setInterval(() => {
      void this.updateHeartbeat(this.child?.pid ?? null);
    }, this.heartbeatIntervalMs);
    this.heartbeatTimer.unref?.();

    // Check pre-aborted signal or cancel.signal file
    const spool = new AntigravitySpool(path.dirname(this.spoolDir));
    if (this.signal?.aborted || await spool.hasCancelSignal(this.spoolDir)) {
      return await this.finalizeTerminalStatus("aborted", null, "agy run was cancelled before start", "", "");
    }

    return await new Promise<AntigravityAttemptStatus>((resolve) => {
      let stdoutLength = 0;
      let stderrLength = 0;
      const stdoutChunks: Buffer[] = [];
      const stderrChunks: Buffer[] = [];
      const maxBytes = this.manifest.maxOutputBytes;

      let child: ChildProcess;
      try {
        child = this.spawnFn(this.manifest.command, this.manifest.args, {
          cwd: this.manifest.cwd,
          shell: false,
          windowsHide: true,
          stdio: ["ignore", "pipe", "pipe"],
        });
        this.child = child;
      } catch (error) {
        const errorMsg = "Unable to spawn " + this.manifest.command + ": " + redactSecrets(String(error));
        void this.finalizeTerminalStatus("failed", null, errorMsg, "", "").then(resolve);
        return;
      }

      if (child.pid) {
        void this.updateHeartbeat(child.pid);
      }

      child.stdout?.on("data", (chunk: Buffer) => {
        if (stdoutLength < maxBytes) {
          const remaining = maxBytes - stdoutLength;
          const slice = chunk.length <= remaining ? chunk : chunk.subarray(0, remaining);
          stdoutChunks.push(slice);
          stdoutLength += slice.length;
          void appendFile(this.manifest.stdoutPath, slice).catch(() => undefined);
        }
      });

      child.stderr?.on("data", (chunk: Buffer) => {
        if (stderrLength < maxBytes) {
          const remaining = maxBytes - stderrLength;
          const slice = chunk.length <= remaining ? chunk : chunk.subarray(0, remaining);
          stderrChunks.push(slice);
          stderrLength += slice.length;
          void appendFile(this.manifest.stderrPath, slice).catch(() => undefined);
        }
      });

      // Cancellation handling
      const cancelTriggered = async (reason: string, statusKind: AntigravityResultStatus = "aborted") => {
        if (this.settled) return;
        this.settled = true;
        this.stopTimers();
        if (child.pid) {
          await this.killVerifiedChild(child.pid);
        }
        const stdoutText = Buffer.concat(stdoutChunks).toString("utf8");
        const stderrText = Buffer.concat(stderrChunks).toString("utf8");
        const status = await this.finalizeTerminalStatus(statusKind, null, reason, stdoutText, stderrText);
        resolve(status);
      };

      // Watch for caller AbortSignal
      if (this.signal) {
        this.signal.addEventListener("abort", () => {
          void cancelTriggered("agy run was cancelled by the caller", "aborted");
        }, { once: true });
      }

      // Execution timeout timer
      const resetTimeoutTimer = (newTimeoutMs: number) => {
        if (newTimeoutMs <= this.effectiveTimeoutMs) return;
        this.effectiveTimeoutMs = newTimeoutMs;
        if (this.timeoutTimer) clearTimeout(this.timeoutTimer);
        const elapsed = Date.now() - this.startTime;
        const remaining = Math.max(0, this.effectiveTimeoutMs - elapsed);
        this.timeoutTimer = setTimeout(() => {
          void cancelTriggered(
            this.manifest.command + " did not finish within " + this.effectiveTimeoutMs + "ms; terminated",
            "timed_out",
          );
        }, remaining);
        this.timeoutTimer.unref?.();
      };
      this.resetTimeoutFn = resetTimeoutTimer;

      if (this.effectiveTimeoutMs > 0) {
        this.timeoutTimer = setTimeout(() => {
          void cancelTriggered(
            this.manifest.command + " did not finish within " + this.effectiveTimeoutMs + "ms; terminated",
            "timed_out",
          );
        }, this.effectiveTimeoutMs);
        this.timeoutTimer.unref?.();
      }

      // Watch for cancel.signal and deadline.json files in spool dir
      this.cancelWatcherTimer = setInterval(() => {
        const parentDir = path.dirname(this.spoolDir);
        const parentCancel = path.join(parentDir, "cancel.signal");
        if (existsSync(this.manifest.cancelPath) || existsSync(parentCancel)) {
          void cancelTriggered("agy run was cancelled by signal file", "aborted");
          return;
        }
        const deadlinePath = path.join(this.spoolDir, "deadline.json");
        const parentDeadlinePath = path.join(parentDir, "deadline.json");
        const activeDeadlinePath = existsSync(deadlinePath)
          ? deadlinePath
          : existsSync(parentDeadlinePath)
            ? parentDeadlinePath
            : null;
        if (activeDeadlinePath) {
          try {
            const raw = readFileSync(activeDeadlinePath, "utf8");
            const data = JSON.parse(raw) as { timeoutMs?: number };
            if (typeof data.timeoutMs === "number" && data.timeoutMs > this.effectiveTimeoutMs) {
              resetTimeoutTimer(data.timeoutMs);
            }
          } catch {}
        }
      }, 200);
      this.cancelWatcherTimer.unref?.();

      child.once("error", (error) => {
        if (this.settled) return;
        this.settled = true;
        this.stopTimers();
        const errorMsg = "Unable to run " + this.manifest.command + ": " + redactSecrets(String(error));
        const stdoutText = Buffer.concat(stdoutChunks).toString("utf8");
        const stderrText = Buffer.concat(stderrChunks).toString("utf8");
        void this.finalizeTerminalStatus("failed", null, errorMsg, stdoutText, stderrText).then(resolve);
      });

      child.once("close", (code) => {
        if (this.settled) return;
        this.settled = true;
        this.stopTimers();
        const stdoutText = Buffer.concat(stdoutChunks).toString("utf8");
        const stderrText = Buffer.concat(stderrChunks).toString("utf8");

        if (code !== 0) {
          const tail = (stderrText.trim() || stdoutText.trim() || "command exited with code " + code).slice(-800);
          const errorMsg = this.manifest.command + " exited with " + code + ": " + redactSecrets(tail);
          void this.finalizeTerminalStatus("failed", code, errorMsg, stdoutText, stderrText).then(resolve);
          return;
        }

        if (!stdoutText.trim() && !stderrText.trim()) {
          const errorMsg = "agy exited 0 without producing any output";
          void this.finalizeTerminalStatus("failed", code, errorMsg, stdoutText, stderrText).then(resolve);
          return;
        }

        const parsed = parseAgyOutput(stdoutText, stderrText);
        if (parsed.hasJson && parsed.status === null) {
          const errorMsg = "agy returned JSON without a recognized status; refusing to claim completion";
          void this.finalizeTerminalStatus("failed", code, errorMsg, stdoutText, stderrText).then(resolve);
          return;
        }

        const finalStatus = parsed.status ?? "completed";
        void this.finalizeTerminalStatus(
          finalStatus,
          code,
          null,
          stdoutText,
          stderrText,
          parsed,
        ).then(resolve);
      });
    });
  }

  private async killVerifiedChild(pid: number): Promise<void> {
    if (this.child && this.child.pid === pid) {
      await this.killTreeFn(pid);
    }
  }

  private async updateHeartbeat(agyPid: number | null): Promise<void> {
    const heartbeat: AntigravityHeartbeat = {
      attemptId: this.manifest.attemptId,
      nonce: this.nonce,
      supervisorPid: process.pid,
      agyPid,
      updatedAt: Date.now(),
      timestamp: new Date().toISOString(),
      fence: this.manifest.fence ?? null,
    };
    await writePrivateFile(this.manifest.heartbeatPath, JSON.stringify(heartbeat, null, 2) + "\n").catch(() => undefined);
    if (this.onHeartbeat) {
      try {
        await this.onHeartbeat(heartbeat);
      } catch {}
    }
  }

  private stopTimers(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.cancelWatcherTimer) clearInterval(this.cancelWatcherTimer);
    if (this.timeoutTimer) clearTimeout(this.timeoutTimer);
    this.heartbeatTimer = null;
    this.cancelWatcherTimer = null;
    this.timeoutTimer = null;
  }

  private async finalizeTerminalStatus(
    status: AntigravityResultStatus,
    exitCode: number | null,
    error: string | null,
    stdout: string,
    stderr: string,
    parsed?: ReturnType<typeof parseAgyOutput>,
  ): Promise<AntigravityAttemptStatus> {
    this.stopTimers();
    const statusPayload: AntigravityAttemptStatus = {
      schemaVersion: 1,
      attemptId: this.manifest.attemptId,
      status,
      exitCode,
      summary: parsed?.summary || stdout.trim() || stderr.trim() || error || "",
      runId: parsed?.runId ?? null,
      files: parsed?.files ?? [],
      tests: parsed?.tests ?? [],
      risks: parsed?.risks ?? [],
      diffSummary: parsed?.diffSummary ?? "none",
      error: error ? redactSecrets(error) : null,
      completedAt: new Date().toISOString(),
      stdout,
      stderr,
    };

    // Write terminal status.json atomically
    await writePrivateFile(this.manifest.statusPath, JSON.stringify(statusPayload, null, 2) + "\n");

    // Security invariant: prompt file must be removed immediately after terminal status is written
    await unlink(this.manifest.promptPath).catch(() => undefined);

    return statusPayload;
  }
}

async function defaultKillTree(pid: number): Promise<void> {
  if (process.platform === "win32") {
    return new Promise((resolve) => {
      let killer: ChildProcess;
      try {
        killer = spawn("taskkill.exe", ["/PID", String(pid), "/T", "/F"], {
          shell: false,
          windowsHide: true,
          stdio: "ignore",
        });
      } catch {
        try { process.kill(pid, "SIGKILL"); } catch {}
        resolve();
        return;
      }
      killer.once("error", () => {
        try { process.kill(pid, "SIGKILL"); } catch {}
        resolve();
      });
      killer.once("close", () => resolve());
    });
  }
  try {
    process.kill(pid, "SIGKILL");
  } catch {}
  return Promise.resolve();
}

export async function runSupervisor(options: SupervisorOptions): Promise<AntigravityAttemptStatus> {
  const supervisor = new AntigravitySupervisor(options);
  return await supervisor.run();
}
