import type {
  AntigravityResultStatus,
  ProviderExecutionStatus,
  WorkerTokenUsage,
} from "./types.js";
import type {
  EarlyExitSignal,
  EscalationProposal,
  EvidenceBundle,
  EvidenceItem,
  MandatoryEvidenceItem,
  ValidationEvidence,
  WorkerClaimedStatus,
} from "../types.js";
import { classifyValidationEvidence, parseEarlyExit, parseEscalation, parseEvidence, parseWorkerProtocolText } from "../result.js";
import { redactSecrets, truncate } from "../security.js";

export interface ParsedAgyOutput {
  status: AntigravityResultStatus | null;
  hasJson: boolean;
  runId: string | null;
  conversationId?: string | null;
  usage?: WorkerTokenUsage;
  durationSeconds?: number | null;
  numTurns?: number | null;
  summary: string;
  fullText: string;
  files: string[];
  tests: string[];
  risks: string[];
  unresolved: string[];
  /** Provider-reported actions that were auto-denied and never executed. */
  deniedActions: string[];
  diffSummary: string;
  providerExecutionStatus: ProviderExecutionStatus;
  workerClaimedStatus: WorkerClaimedStatus;
  validationEvidence: ValidationEvidence;
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

const CANDIDATE_MACHINE_KEY_REGEX =
  /"(?:status|state|runId|run_id|taskId|task_id|sessionId|session_id|conversationId|conversation_id|executionId|attemptId|attempt_id|reasoning|thought|thoughts|thinking|internal|diffSummary|diff_summary|response|usage)"\s*:/i;

const PROTOCOL_ENVELOPE_KEYS = new Set([
  "status",
  "state",
  "result",
  "runId",
  "run_id",
  "conversationId",
  "conversation_id",
  "taskId",
  "task_id",
  "sessionId",
  "session_id",
  "executionId",
  "attemptId",
  "attempt_id",
  "exitCode",
  "exit_code",
  "summary",
  "output",
  "description",
  "message",
  "response",
  "usage",
  "num_turns",
  "numTurns",
  "duration_seconds",
  "durationSeconds",
  "fullText",
  "full_text",
  "rawAssistantText",
  "raw_assistant_text",
  "files",
  "changedFiles",
  "changed_files",
  "tests",
  "testResults",
  "test_results",
  "risks",
  "warnings",
  "diffSummary",
  "diff_summary",
  "diff",
  "evidence",
  "evidenceBundle",
  "evidence_bundle",
  "earlyExit",
  "early_exit",
  "escalation",
  "escalationProposal",
  "escalation_proposal",
  "reasoning",
  "thought",
  "thoughts",
  "thinking",
  "internal",
  "hidden",
]);

function isProtocolEnvelopeObject(obj: Record<string, unknown>): boolean {
  for (const key of Object.keys(obj)) {
    if (PROTOCOL_ENVELOPE_KEYS.has(key)) return true;
  }
  return false;
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
        if (isProtocolEnvelopeObject(value as Record<string, unknown>)) {
          return value as Record<string, unknown>;
        }
      }
    } catch {
      // Try the next candidate shape.
    }
  }

  if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
    try {
      const value: unknown = JSON.parse(trimmed);
      if (value && typeof value === "object" && !Array.isArray(value)) {
        if (isProtocolEnvelopeObject(value as Record<string, unknown>)) {
          return value as Record<string, unknown>;
        }
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

function isAmbiguousMachineEnvelope(text: string): boolean {
  const trimmed = text.trim();
  if (!trimmed) return false;
  if (/^[ \t]*AGY_JSON:[ \t]*/m.test(trimmed)) return true;
  const isCandidateShape = trimmed.startsWith("{") || /^```(?:json)?\s*\r?\n\s*\{/i.test(trimmed);
  if (isCandidateShape && CANDIDATE_MACHINE_KEY_REGEX.test(trimmed)) {
    return true;
  }
  return false;
}

function extractRecognizedVisibleText(json: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = json[key];
    if (typeof value === "string" && value.trim().length > 0) {
      if (key === "result" && parseAgyStatus(value) !== null) {
        continue;
      }
      return value;
    }
  }
  return null;
}

/**
 * Parses the provider usage block without inventing precision.
 *
 * Observed contract for the installed Antigravity CLI (v1.2.0, captured live):
 *   {"input_tokens":N,"output_tokens":N,"thinking_tokens":N,"cache_read_tokens":N,"total_tokens":N}
 * `total_tokens` is reported by the provider and is preserved verbatim. When it
 * is absent it is left null: thinking tokens are reported separately and there
 * is no proven rule that they are disjoint from output tokens, so summing them
 * could double count. Cached input is reported separately from input total and
 * is never added into it. `cache_read_tokens` was observed to grow across turns
 * of the same conversation while not being included in `total_tokens`.
 *
 * The observed numbers are running totals for one provider conversation
 * (`cumulative_conversation`); that scope is only asserted when a conversation
 * id is present.
 */
function parseAgyUsage(raw: unknown, providerConversationId: string | null): WorkerTokenUsage | undefined {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const obj = raw as Record<string, unknown>;
  const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) && v >= 0 ? Math.floor(v) : null);
  const inputTokens = num(obj.input_tokens ?? obj.inputTokens);
  const outputTokens = num(obj.output_tokens ?? obj.outputTokens);
  const thinkingTokens = num(obj.thinking_tokens ?? obj.thinkingTokens);
  const cachedInputTokens = num(obj.cache_read_tokens ?? obj.cache_read_tokens_total ?? obj.cached_input_tokens ?? obj.cachedInputTokens);
  const reportedTotal = num(obj.total_tokens ?? obj.totalTokens);
  const anyReported = inputTokens !== null || outputTokens !== null || thinkingTokens !== null ||
    cachedInputTokens !== null || reportedTotal !== null;
  if (!anyReported) return undefined;
  return {
    inputTokens,
    outputTokens,
    thinkingTokens,
    cachedInputTokens,
    totalTokens: reportedTotal,
    usageScope: providerConversationId ? "cumulative_conversation" : "unknown",
    usageSource: "observed",
    providerConversationId,
  };
}

/** Merges a structured list with a textual-protocol list without duplicates. */
function mergeUnique(structured: string[], textual: string[]): string[] {
  const seen = new Set<string>();
  const merged: string[] = [];
  for (const item of [...structured, ...textual]) {
    const key = item.trim().toLowerCase();
    if (key.length === 0 || seen.has(key)) continue;
    seen.add(key);
    merged.push(item);
  }
  return merged.slice(0, 100);
}

function mergeSummary(structured: string, textual: string): string {
  const structuredTrimmed = structured.trim();
  const textualTrimmed = textual.trim();
  if (structuredTrimmed.length === 0) return textualTrimmed;
  if (textualTrimmed.length === 0) return structuredTrimmed;
  if (structuredTrimmed === textualTrimmed) return structuredTrimmed;
  // The structured field is authoritative; append the protocol summary only
  // when it carries additional information.
  if (structuredTrimmed.includes(textualTrimmed)) return structuredTrimmed;
  if (textualTrimmed.includes(structuredTrimmed)) return textualTrimmed;
  return structuredTrimmed + "\n\n" + textualTrimmed;
}

/**
 * Reads the provider's `denied_actions` list. The installed CLI reports actions
 * it auto-denied because headless mode cannot prompt (observed live on agy
 * 1.2.7: `[{"action":"command","display_name":"RunCommand"}]`). A denied action
 * means the worker was BLOCKED even though `status` says SUCCESS, so it must be
 * surfaced as evidence instead of being silently ignored.
 */
function parseDeniedActions(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const actions: string[] = [];
  for (const item of raw) {
    if (typeof item === "string" && item.trim().length > 0) {
      actions.push(truncate(redactSecrets(item.trim()), 200));
      continue;
    }
    if (item && typeof item === "object" && !Array.isArray(item)) {
      const record = item as Record<string, unknown>;
      const label = [record.display_name, record.displayName, record.action, record.name, record.tool]
        .find((value) => typeof value === "string" && value.trim().length > 0);
      if (typeof label === "string") actions.push(truncate(redactSecrets(label.trim()), 200));
    }
  }
  return actions.slice(0, 20);
}

function providerExecutionStatusOf(rawStatus: string | null): ProviderExecutionStatus {
  if (rawStatus === null) return "unknown";
  const normalized = rawStatus.trim().toLowerCase();
  if (["success", "succeeded", "ok", "completed", "complete", "done"].includes(normalized)) return "success";
  if (["error", "failed", "failure", "cancelled", "canceled", "aborted", "timeout", "timed_out"].includes(normalized)) return "failure";
  return "unknown";
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
    const rawText = stdout.trim() || stderr.trim();
    if (isAmbiguousMachineEnvelope(rawText)) {
      // Malformed or ambiguous machine data: fail closed rather than exposing raw contents
      const validationEvidence = classifyValidationEvidence({
        claimedStatus: "unknown",
        status: "failed",
        tests: [],
        risks: [],
        unresolved: [],
        files: [],
        validationAbsent: true,
        error: "Ambiguous machine envelope rejected",
      });
      return {
        status: null,
        hasJson: false,
        runId: null,
        summary: "",
        fullText: "",
        files: [],
        tests: [],
        risks: [],
        unresolved: [],
        deniedActions: [],
        diffSummary: "",
        providerExecutionStatus: "unknown",
        workerClaimedStatus: "unknown",
        validationEvidence,
        ...(evidence ? { evidence } : {}),
        ...(earlyExit ? { earlyExit } : {}),
        ...(escalation ? { escalation } : {}),
      };
    }
    const redacted = redactSecrets(rawText);
    const protocol = parseWorkerProtocolText(redacted);
    const validationEvidence = classifyValidationEvidence({
      claimedStatus: protocol.claimedStatus,
      status: protocol.claimedStatus === "failed" ? "failed" : "completed",
      tests: protocol.tests,
      risks: protocol.risks,
      unresolved: protocol.unresolved,
      files: protocol.files,
    });
    return {
      status: null,
      hasJson: false,
      runId: null,
      summary: protocol.summary ? truncate(protocol.summary, 4_000) : truncate(redacted, 4_000),
      fullText: truncate(redacted, 2_000_000),
      files: protocol.files,
      tests: protocol.tests,
      risks: protocol.risks,
      unresolved: protocol.unresolved,
      deniedActions: [],
      diffSummary: "",
      providerExecutionStatus: "unknown",
      workerClaimedStatus: protocol.claimedStatus,
      validationEvidence,
      ...(evidence ? { evidence } : {}),
      ...(earlyExit ? { earlyExit } : {}),
      ...(escalation ? { escalation } : {}),
    };
  }
  const rawStatus = firstString(json, ["status", "state", "result"]);
  // Conversation identity is ONLY accepted from an explicitly documented
  // conversation field. runId/taskId/sessionId/executionId are execution
  // identifiers and are never reused as a provider conversation id.
  const rawConversationId = firstString(json, ["conversation_id", "conversationId"]);
  const rawRunId = firstString(json, ["runId", "run_id", "taskId", "task_id", "sessionId", "session_id", "executionId"]);
  // Known machine envelope: extract ONLY recognized visible response fields.
  // Never fall back to raw JSON/stdout when recognized response is absent.
  const recognizedSummary = extractRecognizedVisibleText(json, ["summary", "output", "description", "message", "response", "result"]) ?? "";
  const recognizedFullText = extractRecognizedVisibleText(json, [
    "fullText",
    "full_text",
    "rawAssistantText",
    "raw_assistant_text",
    "response",
    "output",
    "description",
    "message",
    "summary",
    "result",
  ]) ?? recognizedSummary;
  // The worker answers with textual protocol headings inside the visible
  // response; parse them so a provider SUCCESS envelope can never hide a
  // failing test or an unresolved risk reported only in the response body.
  const protocol = parseWorkerProtocolText(recognizedFullText);
  const rawFiles = firstStringList(json, ["files", "changedFiles", "changed_files"]);
  const rawTests = firstStringList(json, ["tests", "testResults", "test_results"]);
  const rawRisks = firstStringList(json, ["risks", "warnings"]);
  const rawUnresolved = firstStringList(json, ["unresolved", "pending", "openItems", "open_items"]);
  const rawDiff = firstString(json, ["diffSummary", "diff_summary", "diff"]) ?? "";
  const providerExecutionStatus = providerExecutionStatusOf(rawStatus);
  const usage = parseAgyUsage(json.usage, rawConversationId ? truncate(redactSecrets(rawConversationId), 200) : null);
  const durationSeconds = typeof json.duration_seconds === "number" ? json.duration_seconds : (typeof json.durationSeconds === "number" ? json.durationSeconds : null);
  const numTurns = typeof json.num_turns === "number" ? json.num_turns : (typeof json.numTurns === "number" ? json.numTurns : null);

  const redactedSummary = redactSecrets(recognizedSummary);
  const redactedFullText = redactSecrets(recognizedFullText);
  // An explicit top-level `summary` field is authoritative; otherwise the
  // worker's own SUMMARY heading is the intended summary and the recognized
  // response text is only the fallback.
  const explicitSummary = firstString(json, ["summary"]);
  const summarySource = explicitSummary
    ? redactSecrets(explicitSummary)
    : (protocol.summary || redactedSummary);
  const summary = truncate(redactSecrets(mergeSummary(summarySource, explicitSummary ? protocol.summary : "")), 4_000);
  const files = mergeUnique(rawFiles, protocol.files);
  const tests = mergeUnique(rawTests, protocol.tests);
  const risks = mergeUnique(rawRisks, protocol.risks);
  const unresolved = mergeUnique(rawUnresolved, protocol.unresolved);
  const mappedStatus = parseAgyStatus(rawStatus);
  const claimedStatus: WorkerClaimedStatus = protocol.claimedStatus !== "unknown"
    ? protocol.claimedStatus
    : mappedStatus === "failed" || mappedStatus === "aborted"
      ? "failed"
      : mappedStatus === "completed" || mappedStatus === "completed_partial"
        ? "completed"
        : "unknown";
  const validationEvidence = classifyValidationEvidence({
    claimedStatus,
    providerExecutionStatus,
    ...(mappedStatus ? { status: mappedStatus } : {}),
    tests,
    risks,
    unresolved,
    files,
    diffSummary: rawDiff,
    deniedActions: parseDeniedActions(json.denied_actions ?? json.deniedActions),
    emptyResult: recognizedFullText.trim().length === 0,
  });

  return {
    status: mappedStatus,
    hasJson: true,
    runId: rawRunId ? truncate(redactSecrets(rawRunId), 200) : null,
    // No fallback: without an explicit conversation field the provider
    // conversation is unknown and continuation must not be assumed.
    conversationId: rawConversationId ? truncate(redactSecrets(rawConversationId), 200) : null,
    ...(usage ? { usage } : {}),
    ...(durationSeconds !== null ? { durationSeconds } : {}),
    ...(numTurns !== null ? { numTurns } : {}),
    summary,
    fullText: truncate(redactedFullText, 2_000_000),
    files: files.map((f) => truncate(redactSecrets(f), 500)),
    tests: tests.map((t) => truncate(redactSecrets(t), 1_000)),
    risks: risks.map((r) => truncate(redactSecrets(r), 1_000)),
    unresolved: unresolved.map((u) => truncate(redactSecrets(u), 1_000)),
    deniedActions: parseDeniedActions(json.denied_actions ?? json.deniedActions),
    diffSummary: truncate(redactSecrets(rawDiff), 10_000),
    providerExecutionStatus,
    workerClaimedStatus: claimedStatus,
    validationEvidence,
    ...(evidence ? { evidence } : {}),
    ...(earlyExit ? { earlyExit } : {}),
    ...(escalation ? { escalation } : {}),
  };
}

function firstParagraphOf(text: string): string {
  return text
    .split(/\r?\n\s*\r?\n/)
    .map((part) => part.replace(/^\s*#+\s*/, "").trim())
    .find((part) => part.length > 0 && !/^(STATUS|SUMMARY|ASSUMPTIONS|CHANGES|FILES|TESTS|RISKS|UNRESOLVED|EARLY_EXIT|ESCALATION|ESCALATION_PROPOSAL|EVIDENCE)\s*:/i.test(part)) ?? "";
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
