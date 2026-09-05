/**
 * Single source of truth for the `agy` CLI argument contract, matching the
 * smoke observed against the installed authenticated CLI:
 * `agy.exe --model gemini-3.8-flash-high -p <prompt> --print-timeout 15m`.
 * The smoke ran with this exact shape; no invented flags are added here.
 */
export const AGY_COMMAND = process.platform === "win32" ? "agy.exe" : "agy";
export const AGY_MODEL = "gemini-3.8-flash-high";
export const AGY_PRINT_TIMEOUT_UNLIMITED = "2562047h47m16s";
export const AGY_PRINT_TIMEOUT_SENTINEL = AGY_PRINT_TIMEOUT_UNLIMITED;
export const AGY_PRINT_TIMEOUT = AGY_PRINT_TIMEOUT_UNLIMITED;
export const AGY_MAX_PROMPT_LENGTH = 30_000;

export interface AntigravityCliOptions {
  model?: string;
  printTimeout?: string | null;
  timeoutMs?: number | null;
  sandbox?: boolean;
  addDirs?: string[];
  dangerouslySkipPermissions?: boolean;
}

export function formatPrintTimeout(timeoutMs: number): string {
  if (timeoutMs % 60_000 === 0 && timeoutMs >= 60_000) {
    return `${Math.floor(timeoutMs / 60_000)}m`;
  }
  if (timeoutMs % 1_000 === 0 && timeoutMs >= 1_000) {
    return `${Math.floor(timeoutMs / 1_000)}s`;
  }
  return `${Math.max(1, Math.ceil(timeoutMs / 1_000))}s`;
}

export function buildAgyArgs(prompt: string, options: AntigravityCliOptions = {}): string[] {
  const args = [
    "--model",
    options.model ?? AGY_MODEL,
  ];
  if (options.sandbox) args.push("--sandbox");
  for (const directory of options.addDirs ?? []) args.push("--add-dir", directory);
  if (options.dangerouslySkipPermissions) args.push("--dangerously-skip-permissions");

  const printTimeout = (typeof options.printTimeout === "string" && options.printTimeout.length > 0)
    ? options.printTimeout
    : (typeof options.timeoutMs === "number" && options.timeoutMs > 0)
      ? formatPrintTimeout(options.timeoutMs)
      : AGY_PRINT_TIMEOUT_UNLIMITED;

  return [
    ...args,
    "-p",
    prompt,
    "--print-timeout",
    printTimeout,
  ];
}
