import type { AntigravityResultStatus } from "./types.js";

export interface ParsedAgyOutput {
  status: AntigravityResultStatus | null;
  hasJson: boolean;
  runId: string | null;
  summary: string;
  files: string[];
  tests: string[];
  risks: string[];
  diffSummary: string;
}

const STATUS_ALIASES: Record<string, AntigravityResultStatus> = {
  success: "completed",
  completed: "completed",
  done: "completed",
  partial: "completed_partial",
  completed_partial: "completed_partial",
  timeout: "timed_out",
  timed_out: "timed_out",
  error: "failed",
  failed: "failed",
  cancelled: "aborted",
  canceled: "aborted",
  aborted: "aborted",
};

function firstString(record: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0) return value;
  }
  return null;
}

function firstStringList(record: Record<string, unknown>, keys: string[]): string[] {
  for (const key of keys) {
    const value = record[key];
    if (Array.isArray(value)) {
      const items = value.filter((item): item is string => typeof item === "string");
      if (items.length > 0) return items;
    }
    if (typeof value === "string" && value.trim().length > 0) {
      return value.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    }
  }
  return [];
}

/**
 * Extracts a JSON payload from agy output: the whole stdout when it parses as
 * JSON, otherwise a ```json fenced block, otherwise a JSON object after an
 * explicit marker line. Returns null when none is present.
 */
export function extractAgyJson(stdout: string): Record<string, unknown> | null {
  const trimmed = stdout.trim();
  if (!trimmed) return null;
  const candidates: string[] = [];
  candidates.push(trimmed);
  const fenced = /```json[ \t]*\r?\n([\s\S]*?)\r?\n?```/.exec(stdout);
  if (fenced?.[1]) candidates.push(fenced[1]);
  const marker = /^[ \t]*AGY_JSON:[ \t]*\r?\n?([\s\S]*)$/m.exec(stdout);
  if (marker?.[1]) candidates.push(marker[1]);
  for (const candidate of candidates) {
    try {
      const value: unknown = JSON.parse(candidate);
      if (value && typeof value === "object" && !Array.isArray(value)) {
        return value as Record<string, unknown>;
      }
    } catch {
      // Try the next candidate shape.
    }
  }
  return null;
}

export function parseAgyOutput(stdout: string, stderr: string): ParsedAgyOutput {
  const json = extractAgyJson(stdout);
  if (!json) {
    // Plain-text contract observed in the smoke: `--print-timeout` prints the
    // model response text and the CLI exits 0. There is no machine-readable
    // status in this mode.
    return {
      status: null,
      hasJson: false,
      runId: null,
      summary: stdout.trim() || stderr.trim(),
      files: [],
      tests: [],
      risks: [],
      diffSummary: "",
    };
  }
  const statusValue = firstString(json, ["status", "state", "result"]);
  return {
    status: parseAgyStatus(statusValue),
    hasJson: true,
    runId: firstString(json, ["runId", "run_id", "taskId", "task_id", "sessionId", "session_id", "executionId"]),
    summary: firstString(json, ["summary", "result", "output", "description", "message"])
      ?? stdout.trim(),
    files: firstStringList(json, ["files", "changedFiles", "changed_files"]),
    tests: firstStringList(json, ["tests", "testResults", "test_results"]),
    risks: firstStringList(json, ["risks", "warnings"]),
    diffSummary: firstString(json, ["diffSummary", "diff_summary", "diff"]) ?? "",
  };
}

/**
 * Fail-closed status mapping: only recognized status words resolve to a
 * concrete status; unknown or missing status values return null so callers can
 * refuse to claim completion instead of guessing.
 */
export function parseAgyStatus(value: string | null): AntigravityResultStatus | null {
  if (value === null) return null;
  const normalized = value.trim().toLowerCase();
  const status = STATUS_ALIASES[normalized];
  return status ?? null;
}
