import { AGY_COMMAND, AGY_MAX_PROMPT_LENGTH, AGY_MODEL, buildAgyArgs } from "./args.js";
import { parseAgyOutput } from "./parser.js";
import { AntigravityProcessError, runAgy, type SpawnLike } from "./runner.js";
import { InvalidRequestError } from "../errors.js";
import { AntigravitySupervisor } from "./supervisor.js";
import { AntigravitySpool } from "./spool.js";
import type {
  AntigravityAttemptManifest,
  AntigravityAttemptStatus,
  AntigravityRunResult,
} from "./types.js";

export interface AntigravityAdapterOptions {
  command?: string | undefined;
  model?: string | undefined;
  timeoutMs?: number | undefined;
  sandbox?: boolean | undefined;
  addDirs?: string[] | undefined;
  dangerouslySkipPermissions?: boolean | undefined;
  spawnFn?: SpawnLike | undefined;
  killTreeFn?: ((pid: number) => Promise<void>) | undefined;
  dataDir?: string | undefined;
}

export interface AntigravityRunOptions {
  prompt: string;
  cwd: string;
  model?: string | undefined;
  signal?: AbortSignal | undefined;
  timeoutMs?: number | undefined;
  attemptManifest?: AntigravityAttemptManifest | undefined;
  dataDir?: string | undefined;
  agentId?: string | undefined;
  jobId?: string | undefined;
  requestId?: string | undefined;
}

export interface AntigravityProviderLike {
  runPrompt(options: AntigravityRunOptions): Promise<AntigravityRunResult>;
  runAttempt?(manifest: AntigravityAttemptManifest, signal?: AbortSignal): Promise<AntigravityRunResult>;
}

/**
 * Antigravity provider wired into the bridge dispatch: takes the worker prompt
 * and executes under durable supervision or direct runner. The parsed output is
 * shaped after the bridge's result contract.
 *
 * Contract guarantees:
 * - exactly one process spawn per invocation; any failure rejects and there
 *   is NO fallback to another provider or model;
 * - explicit model id is always passed on argv (never inherited from config);
 * - cancellation (AbortSignal) and timeout kill only the verified agy process tree;
 * - an empty prompt fails closed with the bridge's typed 400 before spawning;
 * - prompts exceeding the safe argument limit fail closed with typed 400 before spawning;
 * - JSON output with an unknown or missing status fails closed (invalid output);
 * - empty stdout AND empty stderr on exit 0 is invalid output, not a result.
 */
export class AntigravityAdapter implements AntigravityProviderLike {
  readonly command: string;
  readonly model: string;
  readonly timeoutMs: number;
  readonly sandbox: boolean;
  readonly addDirs: string[];
  readonly dangerouslySkipPermissions: boolean;
  readonly spawnFn: SpawnLike | undefined;
  readonly killTreeFn: ((pid: number) => Promise<void>) | undefined;
  readonly dataDir: string | undefined;

  constructor(options: AntigravityAdapterOptions = {}) {
    this.command = options.command ?? AGY_COMMAND;
    this.model = options.model ?? AGY_MODEL;
    this.timeoutMs = options.timeoutMs ?? 900_000;
    this.sandbox = options.sandbox === true;
    this.addDirs = [...new Set(options.addDirs ?? [])];
    this.dangerouslySkipPermissions = options.dangerouslySkipPermissions === true;
    this.spawnFn = options.spawnFn;
    this.killTreeFn = options.killTreeFn;
    this.dataDir = options.dataDir;
  }

  async runAttempt(manifest: AntigravityAttemptManifest, signal?: AbortSignal): Promise<AntigravityRunResult> {
    const supervisor = new AntigravitySupervisor({
      spoolDir: manifest.attemptDir,
      manifest,
      ...(this.spawnFn ? { spawnFn: this.spawnFn } : {}),
      ...(this.killTreeFn ? { killTreeFn: this.killTreeFn } : {}),
      ...(signal ? { signal } : {}),
    });
    const status = await supervisor.run();
    return this.mapAttemptStatusToResult(manifest, status);
  }

  mapAttemptStatusToResult(manifest: AntigravityAttemptManifest, status: AntigravityAttemptStatus): AntigravityRunResult {
    if (status.status === "aborted") {
      throw new AntigravityProcessError("aborted", manifest.command, status.error || "agy run was cancelled");
    }
    if (status.status === "timed_out") {
      throw new AntigravityProcessError("timeout", manifest.command, status.error || (manifest.command + " did not finish within " + manifest.timeoutMs + "ms"));
    }
    if (status.status === "failed") {
      if (status.exitCode !== null && status.exitCode !== 0) {
        throw new AntigravityProcessError("exit", manifest.command, status.error || (manifest.command + " exited with " + status.exitCode), status.exitCode);
      }
      if (status.error?.includes("without producing any output") || status.error?.includes("refusing to claim completion")) {
        throw new AntigravityProcessError("invalid_output", manifest.command, status.error);
      }
      throw new AntigravityProcessError("spawn", manifest.command, status.error || "agy run failed");
    }
    return {
      status: status.status,
      runId: status.runId,
      summary: status.summary,
      files: status.files,
      tests: status.tests,
      risks: status.risks,
      diffSummary: status.diffSummary,
      model: manifest.modelId,
      modelDisplayName: "Antigravity · " + manifest.modelId,
      workspace: manifest.cwd,
      rawOutput: status.stdout,
    };
  }

  async runPrompt(options: AntigravityRunOptions): Promise<AntigravityRunResult> {
    if (!options.prompt.trim()) throw new InvalidRequestError("Task must not be empty");
    if (options.prompt.length > AGY_MAX_PROMPT_LENGTH) {
      throw new InvalidRequestError(
        "Task prompt length (" + options.prompt.length + ") exceeds the maximum safe argument length (" +
          AGY_MAX_PROMPT_LENGTH + " characters); reduce prompt size to fit within command-line limits",
      );
    }
    if (options.attemptManifest) {
      return await this.runAttempt(options.attemptManifest, options.signal);
    }
    const dataDir = options.dataDir ?? this.dataDir;
    if (dataDir && options.jobId && options.agentId && options.requestId) {
      const spool = new AntigravitySpool(dataDir);
      const manifest = await spool.createAttempt({
        agentId: options.agentId,
        jobId: options.jobId,
        requestId: options.requestId,
        prompt: options.prompt,
        cwd: options.cwd,
        modelProviderId: "antigravity",
        modelId: options.model ?? this.model,
        modelVariant: null,
        modelRoute: "antigravity-flash-high",
        command: this.command,
        timeoutMs: options.timeoutMs ?? this.timeoutMs,
        sandbox: this.sandbox,
        addDirs: this.addDirs,
        dangerouslySkipPermissions: this.dangerouslySkipPermissions,
      });
      return await this.runAttempt(manifest, options.signal);
    }

    const model = options.model ?? this.model;
    const effectiveTimeoutMs = options.timeoutMs ?? this.timeoutMs;
    const args = buildAgyArgs(options.prompt, {
      model,
      timeoutMs: effectiveTimeoutMs,
      sandbox: this.sandbox,
      addDirs: this.addDirs,
      dangerouslySkipPermissions: this.dangerouslySkipPermissions,
    });
    const captured = await runAgy(args, {
      command: this.command,
      cwd: options.cwd,
      timeoutMs: effectiveTimeoutMs,
      ...(options.signal ? { signal: options.signal } : {}),
      ...(this.spawnFn ? { spawnFn: this.spawnFn } : {}),
    });
    if (!captured.stdout.trim() && !captured.stderr.trim()) {
      throw new AntigravityProcessError("invalid_output", this.command, "agy exited 0 without producing any output");
    }
    const parsed = parseAgyOutput(captured.stdout, captured.stderr);
    if (parsed.hasJson && parsed.status === null) {
      throw new AntigravityProcessError(
        "invalid_output",
        this.command,
        "agy returned JSON without a recognized status; refusing to claim completion",
      );
    }
    const status = parsed.status ?? "completed";
    return {
      status,
      runId: parsed.runId,
      summary: parsed.summary || captured.stdout.trim() || captured.stderr.trim(),
      files: parsed.files,
      tests: parsed.tests,
      risks: parsed.risks,
      diffSummary: parsed.diffSummary,
      model,
      modelDisplayName: "Antigravity · " + model,
      workspace: options.cwd,
      rawOutput: captured.stdout,
    };
  }
}

export { AntigravityProcessError };
