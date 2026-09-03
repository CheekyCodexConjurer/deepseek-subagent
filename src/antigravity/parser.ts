import type { AntigravityResultStatus } from "./types.js";
import type { EarlyExitSignal, EscalationProposal, EvidenceBundle, EvidenceItem } from "../types.js";
import { parseEarlyExit, parseEscalation, parseEvidence } from "../result.js";
import { redactSecrets, truncate } from "../security.js";

export interface ParsedAgyOutput {
  status: AntigravityResultStatus | null;
  hasJson: boolean;
  runId: string | null;
  summary: string;
  files: string[];
  tests: string[];
  risks: string[];
  diffSummary: string;
  evidence?: EvidenceBundle;
  earlyExit?: EarlyExitSignal;
  escalation?: EscalationProposal;
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
 * Extracts a protocol JSON payload from agy output: only when the output is
 * an intentional envelope (the entire stdout parses as JSON, the entire stdout
 * is wrapped in a single ```json fence, or an explicit AGY_JSON: marker line is
 * present). Embedded markdown code blocks within a larger text response are
 * NOT treated as protocol envelopes so legitimate text answers with JSON
 * examples parse as valid text results. Returns null when none is present.
 */
export function extractAgyJson(stdout: string): Record<string, unknown> | null {
  const trimmed = stdout.trim();
  if (!trimmed) return null;

  const marker = /^[ \t]*AGY_JSON:[ \t]*\r?\n?([\s\S]*)$/m.exec(stdout);
  if (marker?.[1]) {
    try {
      const value: unknown = JSON.parse(marker[1].trim());
      if (value && typeof value === "object" && !Array.isArray(value)) {
        return value as Record<string, unknown>;
      }
    } catch {
      // Try the next candidate shape.
    }
  }

  const wholeFenced = /^```(?:json)?\s*\r?\n([\s\S]*?)\r?\n?\s*```$/i.exec(trimmed);
  if (wholeFenced?.[1]) {
    try {
      const value: unknown = JSON.parse(wholeFenced[1].trim());
      if (value && typeof value === "object" && !Array.isArray(value)) {
        return value as Record<string, unknown>;
      }
    } catch {
      // Try the next candidate shape.
    }
  }

  if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
    try {
      const value: unknown = JSON.parse(trimmed);
      if (value && typeof value === "object" && !Array.isArray(value)) {
        return value as Record<string, unknown>;
      }
    } catch {
      // Not valid JSON.
    }
  }

  return null;
}

/**
 * Parses early-exit metadata from Antigravity JSON or text output.
 * Early exit is a post-turn result signal emitted upon completing a turn;
 * it does not cancel or abort an in-flight process actively or speculatively.
 */
function parseAgyEarlyExit(json: Record<string, unknown> | null, stdout: string): EarlyExitSignal | undefined {
  if (json) {
    const raw = json.earlyExit ?? json.early_exit;
    if (raw && typeof raw === "object" && !Array.isArray(raw)) {
      const r = raw as Record<string, unknown>;
      return {
        triggered: Boolean(r.triggered ?? true),
        reason: truncate(redactSecrets(String(r.reason ?? "Early exit signaled")), 1_000),
        ...(r.confidence !== undefined ? { confidence: r.confidence as any } : {}),
        ...(typeof r.evidenceSnippet === "string" ? { evidenceSnippet: truncate(redactSecrets(r.evidenceSnippet), 2_000) } : {}),
        signaledAt: typeof r.signaledAt === "string" ? redactSecrets(r.signaledAt) : new Date().toISOString(),
      };
    }
    if (typeof raw === "string" && raw.trim().length > 0) {
      return {
        triggered: true,
        reason: truncate(redactSecrets(raw.trim()), 1_000),
        signaledAt: new Date().toISOString(),
      };
    }
    if (raw === true) {
      return {
        triggered: true,
        reason: "Early exit signaled",
        signaledAt: new Date().toISOString(),
      };
    }
  }
  return parseEarlyExit(stdout);
}

function parseAgyEscalation(json: Record<string, unknown> | null, stdout: string): EscalationProposal | undefined {
  if (json) {
    const raw = json.escalation ?? json.escalationProposal ?? json.escalation_proposal;
    if (raw && typeof raw === "object" && !Array.isArray(raw)) {
      const r = raw as Record<string, unknown>;
      return {
        reason: truncate(redactSecrets(String(r.reason ?? "Escalation proposed")), 2_000),
        advisoryOnly: true,
        ...(typeof r.targetRole === "string" ? { targetRole: truncate(redactSecrets(r.targetRole), 200) } : {}),
        ...(typeof r.recommendedRoute === "string" ? { recommendedRoute: truncate(redactSecrets(r.recommendedRoute), 200) } : {}),
        ...(typeof r.suggestedAction === "string" ? { suggestedAction: truncate(redactSecrets(r.suggestedAction), 1_000) } : {}),
      };
    }
    if (typeof raw === "string" && raw.trim().length > 0) {
      return {
        reason: truncate(redactSecrets(raw.trim()), 2_000),
        advisoryOnly: true,
      };
    }
  }
  return parseEscalation(stdout);
}

function parseAgyEvidence(json: Record<string, unknown> | null, stdout: string): EvidenceBundle | undefined {
  if (json) {
    const raw = json.evidence ?? json.evidenceBundle ?? json.evidence_bundle;
    if (raw && typeof raw === "object") {
      const isArr = Array.isArray(raw);
      const rawItems: unknown[] = isArr ? raw : (Array.isArray((raw as any).items) ? (raw as any).items : []);
      const items: EvidenceItem[] = rawItems
        .filter((item: unknown): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
        .slice(0, 100)
        .map((item: Record<string, unknown>): EvidenceItem => ({
          ...(typeof item.id === "string" ? { id: truncate(redactSecrets(item.id), 100) } : {}),
          ...(typeof item.type === "string" ? { type: truncate(redactSecrets(item.type), 100) } : {}),
          ...(typeof item.claim === "string" ? { claim: truncate(redactSecrets(item.claim), 2_000) } : {}),
          ...(typeof item.source === "string" ? { source: truncate(redactSecrets(item.source), 1_000) } : {}),
          ...(typeof item.snippet === "string" ? { snippet: truncate(redactSecrets(item.snippet), 2_000) } : {}),
          ...(item.confidence !== undefined ? { confidence: item.confidence as any } : {}),
          ...(typeof item.verified === "boolean" ? { verified: item.verified } : {}),
        }));
      if (items.length > 0 || (!isArr && typeof (raw as any).summary === "string")) {
        return {
          items,
          ...(typeof (raw as any).summary === "string" ? { summary: truncate(redactSecrets((raw as any).summary), 2_000) } : {}),
          claimsCount: typeof (raw as any).claimsCount === "number" ? (raw as any).claimsCount : items.length,
          collectedAt: typeof (raw as any).collectedAt === "string" ? redactSecrets((raw as any).collectedAt) : new Date().toISOString(),
        };
      }
    }
  }
  return parseEvidence(stdout);
}

export function parseAgyOutput(stdout: string, stderr: string): ParsedAgyOutput {
  const json = extractAgyJson(stdout);
  const earlyExit = parseAgyEarlyExit(json, stdout);
  const escalation = parseAgyEscalation(json, stdout);
  const evidence = parseAgyEvidence(json, stdout);
  if (!json) {
    // Plain-text contract observed in the smoke: `--print-timeout` prints the
    // model response text and the CLI exits 0. There is no machine-readable
    // status in this mode.
    return {
      status: null,
      hasJson: false,
      runId: null,
      summary: truncate(redactSecrets(stdout.trim() || stderr.trim()), 4_000),
      files: [],
      tests: [],
      risks: [],
      diffSummary: "",
      ...(evidence ? { evidence } : {}),
      ...(earlyExit ? { earlyExit } : {}),
      ...(escalation ? { escalation } : {}),
    };
  }
  const rawStatus = firstString(json, ["status", "state", "result"]);
  const rawRunId = firstString(json, ["runId", "run_id", "taskId", "task_id", "sessionId", "session_id", "executionId"]);
  const rawSummary = firstString(json, ["summary", "result", "output", "description", "message"]) ?? stdout.trim();
  const rawFiles = firstStringList(json, ["files", "changedFiles", "changed_files"]);
  const rawTests = firstStringList(json, ["tests", "testResults", "test_results"]);
  const rawRisks = firstStringList(json, ["risks", "warnings"]);
  const rawDiff = firstString(json, ["diffSummary", "diff_summary", "diff"]) ?? "";

  return {
    status: parseAgyStatus(rawStatus),
    hasJson: true,
    runId: rawRunId ? truncate(redactSecrets(rawRunId), 200) : null,
    summary: truncate(redactSecrets(rawSummary), 4_000),
    files: rawFiles.slice(0, 100).map((f) => truncate(redactSecrets(f), 500)),
    tests: rawTests.slice(0, 100).map((t) => truncate(redactSecrets(t), 1_000)),
    risks: rawRisks.slice(0, 100).map((r) => truncate(redactSecrets(r), 1_000)),
    diffSummary: truncate(redactSecrets(rawDiff), 10_000),
    ...(evidence ? { evidence } : {}),
    ...(earlyExit ? { earlyExit } : {}),
    ...(escalation ? { escalation } : {}),
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
