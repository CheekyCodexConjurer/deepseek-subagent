import { AGY_COMMAND, AGY_MAX_PROMPT_LENGTH, buildAgyArgs } from "./args.js";
import { parseAgyOutput } from "./parser.js";
import { AntigravityProcessError, runAgy, type SpawnLike } from "./runner.js";
import { InvalidRequestError } from "../errors.js";
import type { AntigravityRunResult } from "./types.js";

export interface AntigravityAdapterOptions {
  command?: string;
  model?: string;
  timeoutMs?: number;
  sandbox?: boolean;
  addDirs?: string[];
  dangerouslySkipPermissions?: boolean;
  spawnFn?: SpawnLike;
}

export interface AntigravityRunOptions {
  prompt: string;
  cwd: string;
  model?: string;
  signal?: AbortSignal;
  timeoutMs?: number;
}

export interface AntigravityProviderLike {
  runPrompt(options: AntigravityRunOptions): Promise<AntigravityRunResult>;
}

/**
 * Antigravity provider wired into the bridge dispatch: takes the same worker
 * prompt the bridge builds for jobs and runs the authenticated `agy`
 * executable once with the smoke-observed contract
 * (`--model <model> -p <prompt> --print-timeout 15m`). The parsed output is
 * shaped after the bridge's result contract (ResultEnvelope fields:
 * status/summary/files/tests/risks/diffSummary/model/workspace).
 *
 * Contract guarantees:
 * - exactly one process spawn per invocation; any failure rejects and there
 *   is NO fallback to another provider or model;
 * - explicit model id is always passed on argv (never inherited from config);
 * - cancellation (AbortSignal) and timeout kill the process tree and reject
 *   with a typed error;
 * - an empty prompt fails closed with the bridge's typed 400 before spawning;
 * - prompts exceeding the safe argument limit fail closed with typed 400 before spawning;
 * - execution timeout aligns with --print-timeout;
 * - JSON output with an unknown or missing status fails closed (invalid
 *   output): completion is never assumed from a JSON payload that does not
 *   declare a recognized status. Plain-text stdout is treated as a completed
 *   result only under the observed smoke contract (`--print-timeout` prints
 *   the response text and exits 0);
 * - empty stdout AND empty stderr on exit 0 is invalid output, not a result.
 */
export class AntigravityAdapter implements AntigravityProviderLike {
  private readonly command: string;
  private readonly model: string;
  private readonly timeoutMs: number;
  private readonly sandbox: boolean;
  private readonly addDirs: string[];
  private readonly dangerouslySkipPermissions: boolean;
  private readonly spawnFn: SpawnLike | undefined;

  constructor(options: AntigravityAdapterOptions = {}) {
    this.command = options.command ?? AGY_COMMAND;
    this.model = options.model ?? "gemini-3.7-flash-high";
    this.timeoutMs = options.timeoutMs ?? 900_000;
    this.sandbox = options.sandbox === true;
    this.addDirs = [...new Set(options.addDirs ?? [])];
    this.dangerouslySkipPermissions = options.dangerouslySkipPermissions === true;
    this.spawnFn = options.spawnFn;
  }

  async runPrompt(options: AntigravityRunOptions): Promise<AntigravityRunResult> {
    if (!options.prompt.trim()) throw new InvalidRequestError("Task must not be empty");
    if (options.prompt.length > AGY_MAX_PROMPT_LENGTH) {
      throw new InvalidRequestError(
        "Task prompt length (" + options.prompt.length + ") exceeds the maximum safe argument length (" +
          AGY_MAX_PROMPT_LENGTH + " characters); reduce prompt size to fit within command-line limits",
      );
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
