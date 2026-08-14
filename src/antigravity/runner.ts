import { spawn, type ChildProcess } from "node:child_process";
import { AGY_COMMAND } from "./args.js";
import { redactSecrets } from "../security.js";
import type { AntigravityProcessErrorKind } from "./types.js";

export interface AgyProcessResult {
  code: number | null;
  stdout: string;
  stderr: string;
}

export interface SpawnOptions {
  cwd: string;
  env?: NodeJS.ProcessEnv;
  shell: false;
  windowsHide: boolean;
  stdio: ReadonlyArray<"ignore" | "pipe">;
}

export type SpawnLike = (command: string, args: string[], options: SpawnOptions) => ChildProcess;

export interface RunAgyOptions {
  command?: string;
  cwd: string;
  timeoutMs?: number;
  signal?: AbortSignal;
  env?: NodeJS.ProcessEnv;
  spawnFn?: SpawnLike;
  maxOutputBytes?: number;
}

export class AntigravityProcessError extends Error {
  readonly kind: AntigravityProcessErrorKind;
  readonly command: string;
  readonly code: number | null;

  constructor(kind: AntigravityProcessErrorKind, command: string, message: string, code: number | null = null) {
    super(message);
    this.name = "AntigravityProcessError";
    this.kind = kind;
    this.command = command;
    this.code = code;
  }
}

export const AGY_DEFAULT_TIMEOUT_MS = 900_000;
export const AGY_MAX_OUTPUT_BYTES = 1_048_576;
const ERROR_TAIL_LIMIT = 800;

// Captures at most `limit` bytes per stream; the rest of the stream is still
// drained (so the child never blocks on a full pipe) but is not retained.
function boundedSink(limit: number): { buffers: Buffer[]; length: number; append: (chunk: Buffer) => void; text: () => string } {
  const buffers: Buffer[] = [];
  let length = 0;
  return {
    buffers,
    length: 0,
    append(chunk: Buffer) {
      if (length >= limit) return;
      const remaining = limit - length;
      const slice = chunk.length <= remaining ? chunk : chunk.subarray(0, remaining);
      buffers.push(slice);
      length += slice.length;
    },
    text() {
      return Buffer.concat(buffers).toString("utf8");
    },
  };
}

/**
 * Runs the agy executable once with the observed CLI contract, capturing
 * bounded stdout/stderr. Fails closed on spawn failure, non-zero exit, timeout
 * or cancellation; there is never a retry or a second process here, so callers
 * get exactly one attempt per invocation.
 */
export async function runAgy(args: string[], options: RunAgyOptions): Promise<AgyProcessResult> {
  const command = options.command ?? AGY_COMMAND;
  const timeoutMs = options.timeoutMs ?? AGY_DEFAULT_TIMEOUT_MS;
  const maxOutputBytes = options.maxOutputBytes ?? AGY_MAX_OUTPUT_BYTES;
  if (options.signal?.aborted) {
    throw new AntigravityProcessError("aborted", command, "agy run was cancelled before it started");
  }
  return await new Promise<AgyProcessResult>((resolve, reject) => {
    const spawnFn = options.spawnFn ?? spawn;
    let child: ChildProcess;
    try {
      child = spawnFn(command, args, {
        cwd: options.cwd,
        ...(options.env ? { env: options.env } : {}),
        shell: false,
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      });
    } catch (error) {
      reject(new AntigravityProcessError("spawn", command, "Unable to spawn " + command + ": " + redactSecrets(String(error))));
      return;
    }
    const stdoutSink = boundedSink(maxOutputBytes);
    const stderrSink = boundedSink(maxOutputBytes);
    child.stdout?.on("data", (chunk: Buffer) => stdoutSink.append(chunk));
    child.stderr?.on("data", (chunk: Buffer) => stderrSink.append(chunk));
    let settled = false;
    let timer: NodeJS.Timeout | null = null;
    const cleanup = () => {
      if (timer) clearTimeout(timer);
      timer = null;
      options.signal?.removeEventListener("abort", onAbort);
    };
    const onAbort = () => {
      if (settled) return;
      settled = true;
      cleanup();
      void killTree(child).finally(() => {
        reject(new AntigravityProcessError("aborted", command, "agy run was cancelled by the caller"));
      });
    };
    options.signal?.addEventListener("abort", onAbort, { once: true });
    timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      void killTree(child).finally(() => {
        reject(new AntigravityProcessError(
          "timeout",
          command,
          command + " did not finish within " + timeoutMs + "ms; the process tree was terminated",
        ));
      });
    }, timeoutMs);
    timer.unref?.();
    child.once("error", (error) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new AntigravityProcessError("spawn", command, "Unable to run " + command + ": " + redactSecrets(String(error))));
    });
    child.once("close", (code) => {
      if (settled) return;
      settled = true;
      cleanup();
      const stderrText = stderrSink.text();
      const stdoutText = stdoutSink.text();
      if (code !== 0) {
        const tail = (stderrText.trim() || stdoutText.trim() || "command exited with " + code).slice(-ERROR_TAIL_LIMIT);
        reject(new AntigravityProcessError("exit", command, command + " exited with " + code + ": " + redactSecrets(tail), code));
        return;
      }
      resolve({ code, stdout: stdoutText, stderr: stderrText });
    });
  });
}

/**
 * Kills the spawned process tree. On Windows a taskkill /T /F subtree kill is
 * required so timed-out or cancelled runs cannot orphan grandchildren; if
 * taskkill cannot be spawned, best-effort child.kill() is attempted and
 * failures are absorbed (cleanup must never crash the caller).
 */
function killTree(child: ChildProcess): Promise<void> {
  if (child.pid === undefined) return Promise.resolve();
  if (process.platform === "win32") {
    return new Promise((resolve) => {
      let killer: ChildProcess;
      try {
        killer = spawn("taskkill.exe", ["/PID", String(child.pid), "/T", "/F"], {
          shell: false,
          windowsHide: true,
          stdio: "ignore",
        });
      } catch {
        try { child.kill(); } catch {}
        resolve();
        return;
      }
      killer.once("error", () => {
        try { child.kill(); } catch {}
        resolve();
      });
      killer.once("close", () => resolve());
    });
  }
  try { child.kill("SIGKILL"); } catch {}
  return Promise.resolve();
}
