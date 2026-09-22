import { createHash } from "node:crypto";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { writePrivateFile, redactSecrets, redactUnknown, truncate } from "./security.js";
import type { AntigravityRunResult } from "./antigravity/types.js";
import type {
  AgentRecord,
  CompactWorkerResultV1,
  EarlyExitSignal,
  EscalationProposal,
  EvidenceBundle,
  EvidenceItem,
  ExecutionReceipt,
  JobRecord,
  MandatoryEvidenceItem,
  OpenCodeMessage,
  ResultDetailsRef,
  ResultEnvelope,
  ValidationEvidence,
  WorkerClaimedStatus,
  WorkerClaims,
  WorkerUsageScope,
  WorkerUsageSource,
} from "./types.js";

const PROTOCOL_HEADINGS = [
  "STATUS",
  "SUMMARY",
  "ASSUMPTIONS",
  "CHANGES",
  "FILES",
  "TESTS",
  "RISKS",
  "UNRESOLVED",
  "EARLY_EXIT",
  "ESCALATION_PROPOSAL",
  "ESCALATION",
  "EVIDENCE",
];

// A heading value runs until the next known protocol heading or the end of the
// text. Arbitrary uppercase lines such as "NOTE:" inside a value must not
// terminate it.
const PROTOCOL_HEADING_TERMINATOR = "(?=\\n\\s*(?:" + PROTOCOL_HEADINGS.join("|") + ")\\s*:|$)";

export interface ParsedSubagentResult {
  status: "completed" | "failed" | "aborted";
  summary: string;
  files: string[];
  tests: string[];
  risks: string[];
  unresolved: string[];
  fullText: string;
  hasText: boolean;
  assistantMessageId: string | null;
  userMessageId: string | null;
  earlyExit?: EarlyExitSignal;
  escalation?: EscalationProposal;
  evidence?: EvidenceBundle;
}

/**
 * Aggregates the non-empty assistant text written after the job baseline.
 * Empty tails (tool-only or reasoning-only assistant messages) are skipped so
 * a truncated tail never shadows earlier text, and output from a prior
 * continuation (before the baseline) never leaks into the current job. When a
 * non-null baseline is not found in the session, no text is considered
 * relevant (fail closed) instead of falling back to all history.
 */
export function assistantTextAfterBaseline(messages: OpenCodeMessage[], baselineAssistantId: string | null): { text: string; hasText: boolean } {
  const assistants = messages.filter((message) => message.info?.role === "assistant");
  const baselineIndex = baselineAssistantId
    ? assistants.findIndex((message) => message.info?.id === baselineAssistantId)
    : -1;
  const relevant = baselineAssistantId === null
    ? assistants
    : baselineIndex < 0
      ? []
      : assistants.slice(baselineIndex + 1);
  const blocks: string[] = [];
  for (const message of relevant) {
    const text = extractText(message);
    if (text) blocks.push(text);
  }
  const fullText = blocks.join("\n\n");
  return { text: fullText, hasText: fullText.trim().length > 0 };
}

export async function persistResult(
  dataDir: string,
  agent: AgentRecord,
  job: JobRecord,
  messages: OpenCodeMessage[],
  diff: unknown,
  maxLength: number,
  options: {
    statusOverride?: ResultEnvelope["status"];
    deadlineReached?: boolean;
    gracefulFinalize?: boolean;
    partial?: boolean;
    workerAborted?: boolean;
    fallback?: ResultEnvelope["fallback"];
  } = {},
): Promise<{ envelope: ResultEnvelope; resultPath: string; parsed: ParsedSubagentResult }> {
  const parsed = parseMessages(messages, job.lastAssistantMessageId);
  const resultPath = path.join(dataDir, "results", job.id + ".json");
  const diffSummary = redactSecrets(summarizeDiff(diff));
  const fallback = options.fallback ?? (job.fallbackTo ? {
    from: job.fallbackFrom ?? "antigravity",
    to: job.fallbackTo,
    reason: job.fallbackReason ?? "timeout",
    status: job.fallbackStatus ?? "succeeded",
  } : undefined);
  const outputHash = computeOutputHash(parsed.summary, diffSummary);
  const receipt = createExecutionReceipt({
    job,
    agent,
    provider: "opencode",
    model: agent.modelProviderId + "/" + agent.modelId + (agent.modelVariant ? " · " + agent.modelVariant : ""),
    status: options.statusOverride ?? parsed.status,
    workspace: agent.workspacePath,
    earlyExit: Boolean(parsed.earlyExit?.triggered),
    filesCount: parsed.files.length,
    testsCount: parsed.tests.length,
    outputHash,
  });

  const envelope: ResultEnvelope = {
    version: 1,
    agentId: redactSecrets(agent.id),
    jobId: redactSecrets(job.id),
    topic: redactSecrets(agent.topic),
    status: options.statusOverride ?? parsed.status,
    opencodeSessionId: redactSecrets(agent.opencodeSessionId),
    model: redactSecrets(agent.modelProviderId + "/" + agent.modelId + (agent.modelVariant ? " · " + agent.modelVariant : "")),
    modelDisplayName: redactSecrets(displayModel(agent.modelId, agent.modelVariant)),
    workspace: redactSecrets(agent.workspacePath),
    summary: truncate(redactSecrets(parsed.summary), 4_000),
    files: parsed.files.slice(0, 100).map((value) => redactSecrets(value)),
    tests: parsed.tests.slice(0, 100).map((value) => redactSecrets(value)),
    risks: parsed.risks.concat(parsed.unresolved).slice(0, 100).map((value) => redactSecrets(value)),
    diffSummary: truncate(diffSummary, 10_000),
    fullResultPath: redactSecrets(resultPath),
    orchestratorInstruction: redactSecrets("Continue this agent only with deepseek_continue after reviewing this result."),
    ...(options.deadlineReached === undefined ? {} : { deadlineReached: options.deadlineReached }),
    ...(options.gracefulFinalize === undefined ? {} : { gracefulFinalize: options.gracefulFinalize }),
    ...(options.partial === undefined ? {} : { partial: options.partial }),
    ...(options.workerAborted === undefined ? {} : { workerAborted: options.workerAborted }),
    ...(fallback ? { fallback } : {}),
    receipt,
    ...(parsed.evidence ? { evidence: parsed.evidence } : {}),
    ...(parsed.earlyExit ? { earlyExit: parsed.earlyExit } : {}),
    ...(parsed.escalation ? { escalation: parsed.escalation } : {}),
  };

  await mkdir(path.dirname(resultPath), { recursive: true });
  await writePrivateFile(
    resultPath,
    JSON.stringify({
      envelope,
      rawAssistantText: truncate(redactSecrets(parsed.fullText), maxLength),
      // Keep only user-visible text and stable message identifiers. OpenCode
      // also returns reasoning/tool parts that must never cross recover_result.
      messages: projectSafeMessages(messages),
      diff: redactUnknown(diff),
      savedAt: new Date().toISOString(),
    }, null, 2) + "\n",
  );
  return { envelope, resultPath, parsed };
}

/**
 * Persists a result produced by the Antigravity provider as a bridge result
 * file (same layout as persistResult: { envelope, rawAssistantText, messages,
 * diff, savedAt }) so follow/recover/deliver work unchanged. There are no
 * OpenCode messages or diffs; rawAssistantText carries the summary and diff
 * carries the run provenance only.
 */
export async function persistAntigravityResult(
  dataDir: string,
  agent: AgentRecord,
  job: JobRecord,
  result: AntigravityRunResult,
  maxLength: number,
): Promise<{ envelope: ResultEnvelope; resultPath: string }> {
  const resultPath = path.join(dataDir, "results", job.id + ".json");
  const outputHash = computeOutputHash(result.summary, result.diffSummary);
  const receipt = createExecutionReceipt({
    job,
    agent,
    provider: "antigravity",
    model: result.model,
    status: result.status,
    workspace: result.workspace,
    earlyExit: Boolean(result.earlyExit?.triggered),
    filesCount: result.files.length,
    testsCount: result.tests.length,
    outputHash,
  });
  const envelope: ResultEnvelope = {
    version: 1,
    agentId: redactSecrets(agent.id),
    jobId: redactSecrets(job.id),
    topic: redactSecrets(agent.topic),
    status: result.status,
    opencodeSessionId: redactSecrets(agent.opencodeSessionId),
    model: redactSecrets(result.model),
    modelDisplayName: redactSecrets(result.modelDisplayName || result.model || "Antigravity"),
    workspace: redactSecrets(result.workspace),
    summary: truncate(redactSecrets(result.summary), 4_000),
    files: result.files.slice(0, 100).map((value) => redactSecrets(value)),
    tests: result.tests.slice(0, 100).map((value) => redactSecrets(value)),
    risks: result.risks.slice(0, 100).map((value) => redactSecrets(value)),
    unresolved: (result.unresolved ?? []).slice(0, 100).map((value) => redactSecrets(value)),
    diffSummary: truncate(redactSecrets(result.diffSummary), 10_000),
    fullResultPath: redactSecrets(resultPath),
    orchestratorInstruction: redactSecrets("Continue this agent only with subagents_continue after reviewing this result."),
    receipt,
    ...(result.providerExecutionStatus ? { providerExecutionStatus: result.providerExecutionStatus } : {}),
    ...(result.workerClaimedStatus ? { workerClaimedStatus: result.workerClaimedStatus } : {}),
    ...(result.validationEvidence ? (() => {
      const safe = projectSafeValidationEvidence(result.validationEvidence);
      return safe ? { validationEvidence: safe } : {};
    })() : {}),
    ...(result.usage ? (() => {
      const safe = projectSafeUsage(result.usage);
      return safe ? { usage: safe } : {};
    })() : {}),
    ...(result.evidence ? (() => {
      const safe = projectSafeEvidence(result.evidence);
      return safe ? { evidence: safe } : {};
    })() : {}),
    ...(result.earlyExit ? (() => {
      const safe = projectSafeEarlyExit(result.earlyExit);
      return safe ? { earlyExit: safe } : {};
    })() : {}),
    ...(result.escalation ? (() => {
      const safe = projectSafeEscalation(result.escalation);
      return safe ? { escalation: safe } : {};
    })() : {}),
  };
  const rawAssistantText = truncate(redactSecrets(extractAntigravityText(result)), maxLength);
  await mkdir(path.dirname(resultPath), { recursive: true });
  await writePrivateFile(
    resultPath,
    JSON.stringify({
      envelope,
      rawAssistantText,
      messages: [],
      // Full local audit record: the compact transport projection is never a
      // substitute for this file. Provider identity, usage semantics and the
      // worker's own claim all stay persisted here.
      diff: {
        source: "antigravity",
        runId: result.runId === null ? null : truncate(redactSecrets(result.runId), 200),
        providerConversationId: result.conversationId ? truncate(redactSecrets(result.conversationId), 200) : null,
        providerExecutionStatus: result.providerExecutionStatus ?? "unknown",
        workerClaimedStatus: result.workerClaimedStatus ?? "unknown",
        usage: result.usage ?? null,
      },
      savedAt: new Date().toISOString(),
    }, null, 2) + "\n",
  );
  return { envelope, resultPath };
}

const CANDIDATE_MACHINE_KEY_REGEX =
  /"(?:status|state|runId|run_id|taskId|task_id|sessionId|session_id|executionId|attemptId|attempt_id|reasoning|thought|thoughts|thinking|internal|diffSummary|diff_summary)"\s*:/i;

function extractSafeVisibleTextFromEnvelope(text: string): string | null {
  const trimmed = text.trim();
  if (!trimmed) return null;

  const hasMarker = /^[ \t]*AGY_JSON:[ \t]*/m.test(trimmed);
  const isCandidateShape = trimmed.startsWith("{") || /^```(?:json)?\s*\r?\n\s*\{/i.test(trimmed);
  const isCandidate = hasMarker || (isCandidateShape && CANDIDATE_MACHINE_KEY_REGEX.test(trimmed));

  if (!isCandidate) {
    // Legitimate plaintext, Markdown link, bracket tag [STATUS], list [1, 2, 3], or non-protocol code example
    return trimmed;
  }

  // Candidate machine envelope: attempt to safely parse and extract recognized response fields
  try {
    const marker = /^[ \t]*AGY_JSON:[ \t]*\r?\n?([\s\S]*)$/m.exec(trimmed);
    const wholeFenced = /^```(?:json)?\s*\r?\n([\s\S]*?)\r?\n?\s*```$/i.exec(trimmed);
    const candidate = marker?.[1] ? marker[1].trim() : wholeFenced?.[1] ? wholeFenced[1].trim() : trimmed;
    const parsed = JSON.parse(candidate);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      for (const key of [
        "fullText",
        "full_text",
        "rawAssistantText",
        "raw_assistant_text",
        "output",
        "description",
        "message",
        "summary",
        "result",
      ]) {
        const val = (parsed as Record<string, unknown>)[key];
        if (typeof val === "string" && val.trim().length > 0) {
          if (
            key === "result" &&
            /^(?:completed|completed_partial|timed_out|failed|aborted|success|partial|timeout|error|cancelled|canceled)$/i.test(val.trim())
          ) {
            continue;
          }
          return val;
        }
      }
    }
  } catch {}

  // Known machine envelope with absent recognized response or malformed ambiguous data: fail closed
  return "";
}

function extractAntigravityText(result: AntigravityRunResult): string {
  if (typeof result.fullText === "string" && result.fullText.trim().length > 0) {
    const extracted = extractSafeVisibleTextFromEnvelope(result.fullText);
    if (extracted !== null && extracted.trim().length > 0) return extracted;
  }
  if (typeof result.rawOutput === "string" && result.rawOutput.trim().length > 0) {
    const extracted = extractSafeVisibleTextFromEnvelope(result.rawOutput);
    if (extracted !== null && extracted.trim().length > 0) return extracted;
  }
  if (typeof result.summary === "string" && result.summary.trim().length > 0) {
    const extracted = extractSafeVisibleTextFromEnvelope(result.summary);
    if (extracted !== null && extracted.trim().length > 0) return extracted;
  }
  return "";
}

function isAntigravityPersistedResult(value: Record<string, unknown>): boolean {
  if (isRecord(value.diff) && value.diff.source === "antigravity") return true;
  if (isRecord(value.envelope)) {
    const env = value.envelope as Record<string, unknown>;
    if (isRecord(env.receipt) && env.receipt.provider === "antigravity") return true;
  }
  return false;
}

export function sanitizePersistedResult(value: unknown, maxLength = 100_000): unknown {
  if (!isRecord(value)) return {};
  const output: Record<string, unknown> = {};
  const envelope = projectSafeEnvelope(value.envelope);
  if (envelope) output.envelope = envelope;
  const messages = Array.isArray(value.messages) ? value.messages : null;
  if (messages) output.messages = projectSafeMessages(messages);
  const isAntigravity = isAntigravityPersistedResult(value);
  if (typeof value.rawAssistantText === "string") {
    // Invariant: never bypass explicit private message parts.
    // If messages are present, every message must contain exclusively safe text parts.
    const messagesAllowRaw = messages !== null && messages.length > 0 && messages.every((message) => hasOnlyTextParts(message));
    const antigravityAllowRaw = isAntigravity && (!messages || messages.length === 0);
    if (messagesAllowRaw || antigravityAllowRaw) {
      const sanitized = extractSafeVisibleTextFromEnvelope(value.rawAssistantText) ?? value.rawAssistantText;
      if (sanitized.length > 0) {
        output.rawAssistantText = truncate(redactSecrets(sanitized), maxLength);
      }
    }
  }
  if ("diff" in value) output.diff = projectSafeDiff(value.diff);
  if (typeof value.savedAt === "string") output.savedAt = redactSecrets(value.savedAt);
  return output;
}

export function sanitizePersistedEnvelope(value: unknown): ResultEnvelope | null {
  const envelope = projectSafeEnvelope(value);
  if (!envelope || envelope.version !== 1) return null;
  const requiredStrings = [
    "agentId", "jobId", "topic", "status", "opencodeSessionId", "model", "modelDisplayName", "workspace",
    "summary", "diffSummary", "fullResultPath", "orchestratorInstruction",
  ];
  if (requiredStrings.some((key) => typeof envelope[key] !== "string")) return null;
  if (!(["completed", "completed_partial", "timed_out", "failed", "aborted"] as string[]).includes(envelope.status as string)) return null;
  if (!["files", "tests", "risks"].every((key) => Array.isArray(envelope[key]))) return null;
  return envelope as unknown as ResultEnvelope;
}

function projectSafeEnvelope(value: unknown): Record<string, unknown> | null {
  if (!isRecord(value)) return null;
  const output: Record<string, unknown> = {};
  for (const key of [
    "agentId", "jobId", "topic", "status", "opencodeSessionId", "model", "modelDisplayName", "workspace",
    "summary", "diffSummary", "fullResultPath", "orchestratorInstruction",
  ]) {
    if (typeof value[key] === "string") output[key] = truncate(redactSecrets(value[key]), 100_000);
  }
  for (const key of ["files", "tests", "risks", "unresolved"]) {
    if (Array.isArray(value[key])) {
      output[key] = value[key].filter((item): item is string => typeof item === "string").slice(0, 100).map((item) => redactSecrets(item));
    }
  }
  if (value.version === 1) output.version = 1;
  for (const key of ["deadlineReached", "gracefulFinalize", "partial", "workerAborted"]) {
    if (typeof value[key] === "boolean") output[key] = value[key];
  }
  if (typeof value.providerExecutionStatus === "string") {
    output.providerExecutionStatus = value.providerExecutionStatus;
  }
  if (typeof value.workerClaimedStatus === "string") {
    output.workerClaimedStatus = value.workerClaimedStatus;
  }
  const validation = projectSafeValidationEvidence(value.validationEvidence);
  if (validation) output.validationEvidence = validation;
  const usage = projectSafeUsage(value.usage);
  if (usage) output.usage = usage;
  if (value.fallback && typeof value.fallback === "object") {
    const fb = value.fallback as Record<string, unknown>;
    output.fallback = {
      from: truncate(redactSecrets(String(fb.from ?? "")), 1_000),
      to: truncate(redactSecrets(String(fb.to ?? "")), 1_000),
      reason: truncate(redactSecrets(String(fb.reason ?? "")), 2_000),
      status: truncate(redactSecrets(String(fb.status ?? "")), 100),
    };
  }
  if (value.receipt && typeof value.receipt === "object") {
    const r = projectSafeReceipt(value.receipt);
    if (r) output.receipt = r;
  }
  if (value.evidence && typeof value.evidence === "object") {
    const ev = projectSafeEvidence(value.evidence);
    if (ev) output.evidence = ev;
  }
  if (value.earlyExit && typeof value.earlyExit === "object") {
    const ee = projectSafeEarlyExit(value.earlyExit);
    if (ee) output.earlyExit = ee;
  }
  if (value.escalation && typeof value.escalation === "object") {
    const esc = projectSafeEscalation(value.escalation);
    if (esc) output.escalation = esc;
  }
  return output;
}

/**
 * Computes an advisory SHA-256 output hash over summary and diffSummary.
 *
 * NOTE: Per legacy receipt contract, this hashes summary-only (compact preview <= 4,000 chars)
 * and diffSummary, not fullText. Callers must treat outputHash as advisory metadata for
 * receipt provenance, correlation, and idempotency checks, rather than as a cryptographic digest
 * of the full response text. The schema is preserved for backward compatibility.
 */
export function computeOutputHash(summary: string, diffSummary: string): string {
  return createHash("sha256")
    .update((summary || "").trim() + "\n---\n" + (diffSummary || "").trim())
    .digest("hex");
}

export function createExecutionReceipt(input: {
  job: JobRecord;
  agent: AgentRecord;
  provider: string;
  model: string;
  status: ResultEnvelope["status"];
  workspace: string;
  earlyExit?: boolean;
  filesCount?: number;
  testsCount?: number;
  outputHash: string;
  now?: string;
}): ExecutionReceipt {
  const completedAt = input.now ?? new Date().toISOString();
  const startedAtMs = input.job.startedAt ? Date.parse(input.job.startedAt) : null;
  const completedAtMs = Date.parse(completedAt);
  const durationMs = startedAtMs !== null && !isNaN(startedAtMs) ? Math.max(0, completedAtMs - startedAtMs) : null;

  return {
    jobId: input.job.id,
    agentId: input.agent.id,
    provider: input.provider,
    model: input.model,
    status: input.status,
    workspace: input.workspace,
    startedAt: input.job.startedAt ?? null,
    completedAt,
    durationMs,
    attempt: input.job.attempt ?? null,
    fence: input.job.fence ?? null,
    outputHash: input.outputHash,
    quiescent: true,
    earlyExit: Boolean(input.earlyExit),
    filesCount: input.filesCount ?? 0,
    testsCount: input.testsCount ?? 0,
  };
}

export interface ParsedWorkerProtocolText {
  claimedStatus: WorkerClaimedStatus;
  summary: string;
  files: string[];
  tests: string[];
  risks: string[];
  unresolved: string[];
  changes: string[];
}

/**
 * Parses the worker's own textual protocol headings out of the visible response
 * text. The Antigravity worker is instructed to answer with STATUS/SUMMARY/
 * ASSUMPTIONS/CHANGES/FILES/TESTS/RISKS/UNRESOLVED headings inside `response`;
 * without this step a provider-level SUCCESS envelope with an empty top-level
 * `tests` array would be reported to the parent as "no tests, no risks".
 */
export function parseWorkerProtocolText(text: string): ParsedWorkerProtocolText {
  const statusValue = statusFirstToken(headingValue(text, "STATUS"));
  const claimedStatus: WorkerClaimedStatus = statusValue === "failed" || statusValue === "failure" || statusValue === "error"
    ? "failed"
    : statusValue === "needs_approval" || statusValue === "approval_required" || statusValue === "permission_required"
      ? "needs_approval"
      : statusValue === "completed" || statusValue === "complete" || statusValue === "success" || statusValue === "done"
        ? "completed"
        : "unknown";
  return {
    claimedStatus,
    summary: headingValue(text, "SUMMARY"),
    files: headingList(text, "FILES"),
    tests: headingList(text, "TESTS"),
    risks: headingList(text, "RISKS"),
    unresolved: headingList(text, "UNRESOLVED"),
    changes: headingList(text, "CHANGES"),
  };
}

const TEST_FAILURE_PATTERN = /\b(?:fail(?:ed|ure|ures|ing)?|error|errored|exception|broken|not\s+passing|did\s+not\s+pass|falhou|falharam|falha|erro|erros)\b/i;
const TEST_NOT_RUN_PATTERN = /\b(?:not\s+(?:run|executed|ran)|skipped|todo|unavailable|blocked|n\/a|no\s+tests?\s+(?:run|executed)|could\s+not\s+run|unable\s+to\s+run|não\s+executado|nao\s+executado|não\s+rodou|ignorado|pendente)\b/i;
const TEST_PASS_PATTERN = /\b(?:pass(?:ed|es|ing)?|ok|success(?:ful)?|green|passou|passaram|aprovado|sucesso)\b/i;

export type TestOutcome = "pass" | "fail" | "not_run" | "unknown";
export interface TestClassification {
  outcome: TestOutcome;
  /** Short human-readable justification, deterministic and bounded. */
  reason: string;
}

/**
 * Deterministic test-outcome classifier.
 *
 * Priority: (1) structured result fields, (2) explicit counts and exit codes,
 * (3) explicit pass/fail tokens with negation awareness, (4) unknown. Ambiguous
 * text resolves to `unknown` (requires review) instead of guessing: a false pass
 * hides a real failure, and a false fail makes the parent chase a green build.
 *
 * Notably `"627 tests passed, 0 failed"` is a PASS (a na├»ve `includes("failed")`
 * check would report a failure), and Portuguese failure/success wording is
 * recognised (`falhou`, `passaram`).
 */
export function classifyTestOutcome(entry: unknown): TestClassification {
  // 1. Structured result when available.
  if (isRecord(entry)) {
    const structured = firstOutcomeToken(entry);
    if (structured !== null) return structured;
    const text = typeof entry.text === "string" ? entry.text : typeof entry.message === "string" ? entry.message : "";
    const nested = classifyTestOutcome(text);
    return nested;
  }
  if (typeof entry !== "string") return { outcome: "unknown", reason: "not a test result" };
  const raw = entry.trim();
  if (raw.length === 0) return { outcome: "unknown", reason: "empty" };

  // 2. Exit codes are unambiguous.
  const exitMatch = /\bexit(?:\s+code)?\s*[:=]?\s*(-?\d+)\b/i.exec(raw);
  if (exitMatch?.[1] !== undefined) {
    const code = Number(exitMatch[1]);
    return code === 0
      ? { outcome: "pass", reason: "exit code 0" }
      : { outcome: "fail", reason: `exit code ${exitMatch[1]}` };
  }

  // 3. Explicit counts. "N passed" / "M failed" in English and Portuguese.
  const passedCount = firstCount(raw, [
    /(\d+)\s+(?:tests?\s+|checks?\s+|specs?\s+|testes?\s+)?(?:passed|passing|passaram|aprovados)\b/i,
    /(?:passed|passing|passaram|aprovados)\s*[:=]?\s*(\d+)\b/i,
  ]);
  const failedCount = firstCount(raw, [
    /(\d+)\s+(?:tests?\s+|checks?\s+|specs?\s+|testes?\s+)?(?:failed|failing|failures?|falhas?|falharam|com\s+falha)\b/i,
    /(?:failed|failing|failures?|falhas?|falharam)\s*[:=]?\s*(\d+)\b/i,
  ]);
  if (failedCount !== null) {
    if (failedCount === 0) return { outcome: "pass", reason: "0 failures reported" };
    return { outcome: "fail", reason: `${failedCount} failure(s) reported` };
  }
  if (passedCount !== null) return { outcome: "pass", reason: `${passedCount} passed` };

  // 4. Negation-aware token scan. Remove negated failure phrases first so
  //    "not failed", "no failures" and "sem falhas" cannot be read as failures.
  const negated = raw
    .replace(/\b(?:no|zero|0|not?|never)\s+(?:failures?|errors?|failed|failing)\b/gi, " ")
    .replace(/\b(?:sem|nenhuma|nenhum)\s+(?:falhas?|erros?|falha)\b/gi, " ")
    .replace(/\bnão\s+(?:falhou|falharam|houve\s+falha)\b/gi, " ");
  if (TEST_FAILURE_PATTERN.test(negated)) return { outcome: "fail", reason: "explicit failure token" };
  if (TEST_NOT_RUN_PATTERN.test(raw)) return { outcome: "not_run", reason: "explicit not-run/skipped token" };
  if (TEST_PASS_PATTERN.test(raw)) return { outcome: "pass", reason: "explicit pass token" };
  return { outcome: "unknown", reason: "no deterministic outcome signal" };
}

/** Extracts an explicit outcome from a structured test-result object. */
function firstOutcomeToken(entry: Record<string, unknown>): TestClassification | null {
  for (const key of ["outcome", "status", "result", "state", "passed", "success", "ok"]) {
    const value = entry[key];
    if (value === undefined || value === null) continue;
    if (typeof value === "boolean") {
      return value ? { outcome: "pass", reason: `${key}=true` } : { outcome: "fail", reason: `${key}=false` };
    }
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      if (["pass", "passed", "passing", "ok", "success", "succeeded", "green", "aprovado", "sucesso"].includes(normalized)) {
        return { outcome: "pass", reason: `${key}=${normalized}` };
      }
      if (["fail", "failed", "failure", "error", "errored", "red", "falhou", "falha", "erro"].includes(normalized)) {
        return { outcome: "fail", reason: `${key}=${normalized}` };
      }
      if (["skip", "skipped", "not_run", "pending", "todo", "ignorado"].includes(normalized)) {
        return { outcome: "not_run", reason: `${key}=${normalized}` };
      }
    }
  }
  return null;
}

function firstCount(text: string, patterns: RegExp[]): number | null {
  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match?.[1] !== undefined) {
      const value = Number(match[1]);
      if (Number.isFinite(value)) return value;
    }
  }
  return null;
}
const BLOCKING_RISK_PATTERN = /\b(?:blocker|blocking|critical|severe|fatal|regression|regress(?:ão|ao|ões|oes)|cr[íi]tic[oa]|bloqueador|bloqueante|grave|perda\s+de\s+dados|data\s+loss|security|seguran[çc]a)\b/i;
const SCOPE_VIOLATION_PATTERN = /\b(?:out\s+of\s+scope|outside\s+(?:the\s+)?scope|unrelated\s+(?:file|change)|scope\s+(?:creep|violation)|beyond\s+(?:the\s+)?(?:requested|authorized)|fora\s+do\s+escopo|fora\s+de\s+escopo|escopo\s+indevido)\b/i;

function nonNone(values: string[]): string[] {
  return values.filter((value) => value.trim().length > 0 && !/^(?:none|n\/a|nothing|-)$/i.test(value.trim()));
}

/**
 * Deterministically classifies the evidence that must never be dropped from the
 * compact transport projection. Classification is textual and conservative: a
 * false positive costs one extra detail fetch, a false negative would let the
 * parent believe a failing run was green.
 */
export function classifyValidationEvidence(input: {
  claimedStatus: WorkerClaimedStatus;
  providerExecutionStatus?: string;
  status?: string;
  tests: string[];
  risks: string[];
  unresolved: string[];
  files: string[];
  diffSummary?: string;
  validationAbsent?: boolean;
  permissionRequired?: boolean;
  /**
   * Actions the provider auto-denied (headless permission was never granted).
   * An auto-denied action means the worker was BLOCKED: the run cannot be
   * treated as complete just because the provider exited SUCCESS.
   */
  deniedActions?: string[];
  /** True when the provider returned no visible response text at all. */
  emptyResult?: boolean;
  error?: string | null;
}): ValidationEvidence {
  const tests = nonNone(input.tests);
  const risks = nonNone(input.risks);
  const unresolved = nonNone(input.unresolved);
  const deniedActions = (input.deniedActions ?? [])
    .map((action) => String(action).trim())
    .filter(Boolean)
    .slice(0, 20);
  // Outcome classification replaces a naive keyword scan: "627 passed, 0 failed"
  // must not be read as a failure, and ambiguous text must stay unknown.
  const classified = tests.map((test) => ({ test, classification: classifyTestOutcome(test) }));
  const testsFailed = classified.filter((item) => item.classification.outcome === "fail").map((item) => item.test);
  const testsPassed = classified.filter((item) => item.classification.outcome === "pass").map((item) => item.test);
  const testsNotRun = classified.filter((item) => item.classification.outcome === "not_run").map((item) => item.test);
  const testsUnknown = classified.filter((item) => item.classification.outcome === "unknown").map((item) => item.test);
  const blockingRisks = risks.filter((risk) => BLOCKING_RISK_PATTERN.test(risk));
  const scopeViolations = unresolved.filter((item) => SCOPE_VIOLATION_PATTERN.test(item));

  const providerFailure = input.providerExecutionStatus === "failure";
  const envelopeFailure = input.status === "failed" || input.status === "aborted" || input.status === "timed_out";
  const partial = input.status === "completed_partial";
  // A denied action or an entirely empty response means the provider claimed a
  // normal exit while the work did not actually happen.
  const emptyResult = Boolean(input.emptyResult) && tests.length === 0 && nonNone(unresolved).length === 0;
  const workerFailure = input.claimedStatus === "failed" || providerFailure || envelopeFailure || deniedActions.length > 0 || emptyResult;
  const permissionRequired = Boolean(input.permissionRequired) || input.claimedStatus === "needs_approval" || deniedActions.length > 0;
  const validationAbsent = Boolean(input.validationAbsent) && tests.length === 0;
  const claimEvidenceConflict = input.claimedStatus === "completed"
    && (testsFailed.length > 0 || providerFailure || envelopeFailure || partial);

  const mandatory: MandatoryEvidenceItem[] = [];
  const push = (kind: MandatoryEvidenceItem["kind"], detail: string): void => {
    const safe = truncate(redactSecrets(detail), 600);
    if (safe.length === 0) return;
    mandatory.push({ kind, detail: safe });
  };
  for (const test of testsFailed) push("test_failed", test);
  for (const test of testsNotRun) push("test_not_run", test);
  for (const test of testsUnknown) push("test_unclassified", test);
  for (const action of deniedActions) push("action_denied", `The provider auto-denied '${action}' and never executed it.`);
  if (permissionRequired && deniedActions.length === 0) push("permission_required", "The worker requested an explicit permission decision.");
  for (const item of unresolved) push("unresolved", item);
  if (workerFailure) push("worker_failure", input.error && input.error.trim().length > 0 ? input.error : "The worker did not complete successfully.");
  if (partial) push("partial_completion", "The worker reported a partial completion.");
  for (const item of scopeViolations) push("scope_violation", item);
  if (validationAbsent) push("validation_absent", "No test evidence was reported for a task that required validation.");
  if (emptyResult) push("validation_absent", "The provider reported success but returned no visible result; nothing was accomplished or reported.");
  for (const risk of blockingRisks) push("blocking_risk", risk);
  if (claimEvidenceConflict) push("claim_evidence_conflict", "The worker claimed completion while the evidence shows a failure or partial result.");
  if (input.error && input.error.trim().length > 0 && !workerFailure) push("operational_error", input.error);

  return {
    testsFailed,
    testsNotRun,
    testsPassed,
    testsUnknown,
    blockingRisks,
    unresolved,
    scopeViolations,
    validationAbsent,
    permissionRequired,
    deniedActions,
    partial,
    workerFailure,
    emptyResult,
    claimEvidenceConflict,
    mandatory,
  };
}

/**
 * Kinds that make a result unusable for an autonomous decision: the parent must
 * intervene (grant permission, supply validation, or fetch details) instead of
 * treating the run as finished work.
 */
const BLOCKING_EVIDENCE_KINDS: ReadonlySet<MandatoryEvidenceItem["kind"]> = new Set([
  "permission_required",
  "action_denied",
  "worker_failure",
  "partial_completion",
  "validation_absent",
]);

export function hasBlockingEvidence(mandatory: MandatoryEvidenceItem[]): boolean {
  return mandatory.some((item) => BLOCKING_EVIDENCE_KINDS.has(item.kind));
}

export const COMPACT_SUMMARY_MAX_CHARS = 1_000;
export const COMPACT_FILES_MAX = 10;
export const COMPACT_TESTS_MAX = 10;
export const COMPACT_RISKS_MAX = 5;
const COMPACT_SUMMARY_MIN_CHARS = 160;
const COMPACT_MIN_ITEM_CAP = 1;

/** Real UTF-8 byte size of the serialized compact payload. */
export function serializedBytes(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value) ?? "", "utf8");
}

export interface PriorUsage {
  inputTokens: number;
  outputTokens: number;
  thinkingTokens: number;
  cachedInputTokens: number;
  totalTokens: number;
  comparable: boolean;
  providerConversationId: string | null;
  /** A previous turn in this conversation already observed a counter reset. */
  counterResetObserved?: boolean;
}

export interface DerivedTurnUsage {
  inputTokens: number | null;
  outputTokens: number | null;
  thinkingTokens: number | null;
  cachedInputTokens: number | null;
  totalTokens: number | null;
  usageScope: WorkerUsageScope;
  usageSource: WorkerUsageSource;
  providerConversationId: string | null;
  /** A cumulative counter went backwards: the provider conversation restarted. */
  counterResetDetected: boolean;
}

/**
 * Derives the usage attributable to a single turn without inventing precision.
 *
 * Only a provider block explicitly scoped `cumulative_conversation` for the
 * SAME provider conversation as the prior jobs may be differenced, and only
 * while every cumulative counter is monotonically non-decreasing. If ANY
 * observed cumulative counter went backwards the provider session restarted:
 * the observed values are kept verbatim, `counterResetDetected` is set, and no
 * delta is produced (a reset must never be reported as zero spend).
 *
 * Differencing is per field: a field that is missing on either side stays null
 * instead of becoming zero, so partial provider blocks never fabricate
 * precision for the fields they do report.
 */
export function deriveTurnUsage(current: {
  inputTokens: number | null;
  outputTokens: number | null;
  thinkingTokens: number | null;
  cachedInputTokens: number | null;
  totalTokens: number | null;
  usageScope: WorkerUsageScope;
  usageSource: WorkerUsageSource;
  providerConversationId?: string | null;
}, prior: PriorUsage): DerivedTurnUsage {
  const sameConversation = Boolean(current.providerConversationId) &&
    current.providerConversationId === prior.providerConversationId;
  const canDifference = current.usageScope === "cumulative_conversation" &&
    prior.comparable &&
    sameConversation &&
    !prior.counterResetObserved;

  if (!canDifference) {
    return {
      inputTokens: current.inputTokens,
      outputTokens: current.outputTokens,
      thinkingTokens: current.thinkingTokens,
      cachedInputTokens: current.cachedInputTokens,
      totalTokens: current.totalTokens,
      // Without a comparable baseline the block cannot be re-scoped to this
      // turn: keep the provider's own scope rather than asserting per_turn.
      usageScope: current.usageScope,
      usageSource: "observed",
      providerConversationId: current.providerConversationId ?? null,
      counterResetDetected: false,
    };
  }

  const pairs: Array<[number | null, number]> = [
    [current.inputTokens, prior.inputTokens],
    [current.outputTokens, prior.outputTokens],
    [current.thinkingTokens, prior.thinkingTokens],
    [current.cachedInputTokens, prior.cachedInputTokens],
    [current.totalTokens, prior.totalTokens],
  ];
  const resetDetected = pairs.some(([value, baseline]) => value !== null && value < baseline);
  if (resetDetected) {
    return {
      inputTokens: current.inputTokens,
      outputTokens: current.outputTokens,
      thinkingTokens: current.thinkingTokens,
      cachedInputTokens: current.cachedInputTokens,
      totalTokens: current.totalTokens,
      usageScope: "unknown",
      usageSource: "observed",
      providerConversationId: current.providerConversationId ?? null,
      counterResetDetected: true,
    };
  }

  // Per-field differencing: unknown stays unknown on either side.
  const delta = (value: number | null, baseline: number): number | null =>
    value === null ? null : Math.max(0, value - baseline);
  return {
    inputTokens: delta(current.inputTokens, prior.inputTokens),
    outputTokens: delta(current.outputTokens, prior.outputTokens),
    thinkingTokens: delta(current.thinkingTokens, prior.thinkingTokens),
    cachedInputTokens: delta(current.cachedInputTokens, prior.cachedInputTokens),
    totalTokens: delta(current.totalTokens, prior.totalTokens),
    usageScope: "per_turn",
    usageSource: "derived",
    providerConversationId: current.providerConversationId ?? null,
    counterResetDetected: false,
  };
}

/**
 * Splits a string into a byte-bounded UTF-8 safe chunk.
 *
 * Never splits a multi-byte character: the end offset is walked back off any
 * UTF-8 continuation byte. `offset` and `nextOffset` are BYTE offsets, so a
 * caller can page through a multi-megabyte field deterministically.
 */
export function chunkUtf8(text: string, offset: number, maxBytes: number): {
  chunk: string;
  offset: number;
  returnedBytes: number;
  totalBytes: number;
  hasMore: boolean;
  nextOffset: number;
} {
  const buffer = Buffer.from(text, "utf8");
  const totalBytes = buffer.length;
  const start = Math.max(0, Math.min(offset, totalBytes));
  const budget = Math.max(0, maxBytes);
  let end = Math.min(start + budget, totalBytes);
  if (end < totalBytes) {
    // Back off any UTF-8 continuation byte so the cut lands on a lead byte.
    while (end > start && (buffer[end]! & 0xc0) === 0x80) end -= 1;
  }
  const chunk = buffer.subarray(start, end).toString("utf8");
  const hasMore = end < totalBytes;
  return {
    chunk,
    offset: start,
    returnedBytes: end - start,
    totalBytes,
    hasMore,
    nextOffset: hasMore ? end : totalBytes,
  };
}

/**
 * Adds `serializedBytes` to a payload and iterates until the reported value
 * equals the real byte length of the final serialization. A single pre-insert
 * measurement is wrong by the width of the field itself, which drifts over
 * 10k/100k boundaries.
 */
export function withStableSerializedBytes<T extends Record<string, unknown>>(payload: T, key = "serializedBytes"): T & { serializedBytes: number } {
  let size = serializedBytes({ ...payload, [key]: 0 });
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const candidate = { ...payload, [key]: size } as T & { serializedBytes: number };
    const measured = serializedBytes(candidate);
    if (measured === size) return candidate;
    size = measured;
  }
  return { ...payload, [key]: size } as T & { serializedBytes: number };
}

export interface CompactWorkerResultOptions {
  maxBytes: number;
  tokens?: CompactWorkerResultV1["tokens"];
  mandatoryEvidence?: MandatoryEvidenceItem[];
  statusOverride?: string;
  /** Worker claim vs evidence conflict already detected by the caller. */
  decisionReady?: boolean;
  decisionReason?: string | null;
}

/**
 * Builds the versioned compact transport projection and enforces the serialized
 * byte budget as a hard invariant. Optional content is shrunk before any
 * mandatory evidence, and if the mandatory evidence alone cannot fit the
 * payload fails closed with `decisionReady = false` and
 * `decisionReason = "mandatory_evidence_overflow"` so the parent knows it must
 * fetch the exact sections instead of assuming a green result.
 */
export function createCompactWorkerResult(
  envelope: ResultEnvelope,
  options: CompactWorkerResultOptions,
): CompactWorkerResultV1 {
  const mandatoryEvidence = (options.mandatoryEvidence ?? []).map((item) => ({
    kind: item.kind,
    detail: truncate(redactSecrets(item.detail), 600),
  }));
  const status = options.statusOverride ?? envelope.status;
  const fullSummary = envelope.summary || "";
  const allFiles = envelope.files ?? [];
  const allTests = envelope.tests ?? [];
  const allRisks = envelope.risks ?? [];

  const build = (caps: { summary: number; files: number; tests: number; risks: number }): CompactWorkerResultV1 => {
    const summary = truncate(fullSummary, caps.summary);
    const claims: WorkerClaims = {
      summary,
      files: allFiles.slice(0, caps.files),
      tests: allTests.slice(0, caps.tests),
      risks: allRisks.slice(0, caps.risks),
    };
    const detailsRef = createDetailsRef(envelope, {
      compactSummaryChars: summary.length,
      mandatoryDetailCount: mandatoryEvidence.length,
      mandatorySections: [...new Set(mandatoryEvidence.map((item) => item.kind))],
    });
    return {
      version: 1,
      status,
      claims,
      mandatoryEvidence,
      receipt: envelope.receipt ? {
        jobId: envelope.receipt.jobId,
        agentId: envelope.receipt.agentId,
        provider: envelope.receipt.provider,
        model: envelope.receipt.model,
        status: envelope.receipt.status,
        completedAt: envelope.receipt.completedAt,
        durationMs: envelope.receipt.durationMs,
        filesCount: envelope.receipt.filesCount,
        testsCount: envelope.receipt.testsCount,
        outputHash: envelope.receipt.outputHash,
      } : null,
      tokens: options.tokens ?? (envelope.usage ? {
        inputTokens: envelope.usage.inputTokens,
        outputTokens: envelope.usage.outputTokens,
        thinkingTokens: envelope.usage.thinkingTokens,
        cachedInputTokens: envelope.usage.cachedInputTokens,
        totalTokens: envelope.usage.totalTokens,
        usageScope: envelope.usage.usageScope,
        usageSource: envelope.usage.usageSource,
        ...(envelope.usage.counterResetDetected ? { counterResetDetected: true } : {}),
      } : null),
      decisionReady: options.decisionReady ?? (!hasBlockingEvidence(mandatoryEvidence)),
      decisionReason: options.decisionReason ?? (hasBlockingEvidence(mandatoryEvidence) ? "blocking_evidence_requires_action" : null),
      detailsRef,
    };
  };

  const caps = {
    summary: COMPACT_SUMMARY_MAX_CHARS,
    files: COMPACT_FILES_MAX,
    tests: COMPACT_TESTS_MAX,
    risks: COMPACT_RISKS_MAX,
  };
  let compact = build(caps);
  if (serializedBytes(compact) <= options.maxBytes) return compact;

  // Shrink optional content only. Mandatory evidence is never trimmed.
  const shrinkOrder: Array<() => boolean> = [
    () => (caps.risks > COMPACT_MIN_ITEM_CAP ? (caps.risks -= 1, true) : false),
    () => (caps.tests > COMPACT_MIN_ITEM_CAP ? (caps.tests -= 1, true) : false),
    () => (caps.files > COMPACT_MIN_ITEM_CAP ? (caps.files -= 1, true) : false),
    () => (caps.summary > COMPACT_SUMMARY_MIN_CHARS ? (caps.summary = Math.max(COMPACT_SUMMARY_MIN_CHARS, Math.floor(caps.summary / 2)), true) : false),
    () => (caps.risks > 0 ? (caps.risks = 0, true) : false),
    () => (caps.tests > 0 ? (caps.tests = 0, true) : false),
    () => (caps.files > 0 ? (caps.files = 0, true) : false),
    () => (caps.summary > 0 ? (caps.summary = 0, true) : false),
  ];
  for (const step of shrinkOrder) {
    while (step()) {
      compact = build(caps);
      if (serializedBytes(compact) <= options.maxBytes) return compact;
    }
  }

  // Even the mandatory floor does not fit. Never silently drop it and never
  // dump the bulk: replace the items with a budget-safe digest and fail closed
  // so the parent fetches the exact section on demand.
  const countsByKind: Record<string, number> = {};
  for (const item of mandatoryEvidence) {
    countsByKind[item.kind] = (countsByKind[item.kind] ?? 0) + 1;
  }
  const overflowDetailsRef = createDetailsRef(envelope, {
    compactSummaryChars: 0,
    mandatoryDetailCount: mandatoryEvidence.length,
    mandatorySections: Object.keys(countsByKind),
  });
  const minimal = (firstRefs: string[], dropReceipt: boolean, dropTokens: boolean): CompactWorkerResultV1 => {
    const base = build({ summary: 0, files: 0, tests: 0, risks: 0 });
    return {
      ...base,
      claims: { summary: "", files: [], tests: [], risks: [] },
      mandatoryEvidence: [],
      mandatoryEvidenceSummary: {
        total: mandatoryEvidence.length,
        countsByKind,
        firstRefs,
      },
      receipt: dropReceipt ? null : base.receipt,
      tokens: dropTokens ? null : base.tokens,
      decisionReady: false,
      decisionReason: "mandatory_evidence_overflow",
      detailsRef: {
        ...overflowDetailsRef,
        hasMoreDetails: true,
        exactSection: "evidence",
        cursor: { offset: 0, limitBytes: options.maxBytes },
        totalCount: mandatoryEvidence.length,
      },
    };
  };

  // firstRefs are identifiers, never the detail body; shrink them (and then the
  // decorations) until the minimal response itself respects the budget.
  const buildRefs = (limit: number): string[] => {
    const refs: string[] = [];
    for (let index = 0; index < mandatoryEvidence.length && refs.length < limit; index += 1) {
      const item = mandatoryEvidence[index]!;
      refs.push(`${item.kind}#${index}`);
    }
    return refs;
  };
  const ladder: Array<[number, boolean, boolean]> = [
    [Math.min(mandatoryEvidence.length, 5), false, false],
    [Math.min(mandatoryEvidence.length, 2), false, false],
    [1, false, false],
    [0, false, false],
    [0, true, false],
    [0, true, true],
  ];
  for (const [limit, dropReceipt, dropTokens] of ladder) {
    const candidate = minimal(buildRefs(limit), dropReceipt, dropTokens);
    if (serializedBytes(candidate) <= options.maxBytes) return candidate;
  }
  return minimal([], true, true);
}

export function createCompactClaims(envelope: ResultEnvelope): WorkerClaims {
  return {
    summary: truncate(envelope.summary || "", COMPACT_SUMMARY_MAX_CHARS),
    files: (envelope.files ?? []).slice(0, COMPACT_FILES_MAX),
    tests: (envelope.tests ?? []).slice(0, COMPACT_TESTS_MAX),
    risks: (envelope.risks ?? []).slice(0, COMPACT_RISKS_MAX),
  };
}

export interface DetailsRefOptions {
  compactSummaryChars?: number;
  mandatoryDetailCount?: number;
  mandatorySections?: string[];
}

/**
 * Truncation-aware pointer to the persisted full result. `hasMoreDetails` is
 * true whenever ANY section was shortened for transport, including a truncated
 * summary, so the parent can never be left believing the compact view is the
 * whole story.
 */
export function createDetailsRef(envelope: ResultEnvelope, options: DetailsRefOptions = {}): ResultDetailsRef {
  const filesTotal = envelope.files?.length ?? 0;
  const testsTotal = envelope.tests?.length ?? 0;
  const risksTotal = envelope.risks?.length ?? 0;
  const unresolvedTotal = envelope.unresolved?.length ?? 0;
  const evidenceTotal = envelope.evidence?.items?.length ?? 0;
  const summaryLimit = options.compactSummaryChars ?? COMPACT_SUMMARY_MAX_CHARS;
  const summaryTruncated = (envelope.summary?.length ?? 0) > summaryLimit;
  const filesTruncated = filesTotal > COMPACT_FILES_MAX;
  const testsTruncated = testsTotal > COMPACT_TESTS_MAX;
  const risksTruncated = risksTotal > COMPACT_RISKS_MAX;
  const diffAvailable = Boolean(envelope.diffSummary && envelope.diffSummary.length > 0 && envelope.diffSummary !== "none");
  const evidenceAvailable = evidenceTotal > 0;
  const unresolvedAvailable = unresolvedTotal > 0;
  const mandatoryDetailCount = options.mandatoryDetailCount ?? (envelope.validationEvidence?.mandatory?.length ?? 0);
  const mandatoryOverflow = mandatoryDetailCount > 0 && Boolean(options.mandatorySections && options.mandatorySections.length > 0);

  const hasMore = summaryTruncated || filesTruncated || testsTruncated || risksTruncated ||
    diffAvailable || evidenceAvailable || unresolvedAvailable;

  return {
    resultPath: envelope.fullResultPath,
    hasMoreDetails: Boolean(hasMore),
    availableSections: ["summary", "files", "tests", "risks", "diff", "evidence", "unresolved", "full"],
    summaryTruncated,
    filesTotal,
    testsTotal,
    risksTotal,
    evidenceTotal,
    ...(unresolvedTotal > 0 ? { unresolvedTotal } : {}),
    mandatoryDetailCount,
    ...(mandatoryOverflow ? { mandatorySections: options.mandatorySections } : {}),
  };
}

function projectSafeReceipt(value: unknown): ExecutionReceipt | null {
  if (!isRecord(value)) return null;
  const requiredStrings = ["jobId", "agentId", "provider", "model", "status", "workspace", "completedAt", "outputHash"];
  if (requiredStrings.some((key) => typeof value[key] !== "string")) return null;
  return {
    jobId: truncate(redactSecrets(String(value.jobId)), 100),
    agentId: truncate(redactSecrets(String(value.agentId)), 100),
    provider: truncate(redactSecrets(String(value.provider)), 100),
    model: truncate(redactSecrets(String(value.model)), 200),
    status: value.status as ResultEnvelope["status"],
    workspace: truncate(redactSecrets(String(value.workspace)), 1_000),
    startedAt: typeof value.startedAt === "string" ? redactSecrets(value.startedAt) : null,
    completedAt: redactSecrets(String(value.completedAt)),
    durationMs: typeof value.durationMs === "number" ? value.durationMs : null,
    attempt: typeof value.attempt === "string" ? truncate(redactSecrets(value.attempt), 50) : null,
    fence: typeof value.fence === "number" ? value.fence : null,
    outputHash: truncate(redactSecrets(String(value.outputHash)), 100),
    quiescent: Boolean(value.quiescent ?? true),
    earlyExit: Boolean(value.earlyExit),
    filesCount: typeof value.filesCount === "number" ? value.filesCount : 0,
    testsCount: typeof value.testsCount === "number" ? value.testsCount : 0,
  };
}

function projectSafeEarlyExit(value: unknown): EarlyExitSignal | null {
  if (!isRecord(value)) return null;
  if (typeof value.triggered !== "boolean" || typeof value.reason !== "string") return null;
  return {
    triggered: value.triggered,
    reason: truncate(redactSecrets(value.reason), 1_000),
    ...(value.confidence !== undefined ? { confidence: value.confidence as any } : {}),
    ...(typeof value.evidenceSnippet === "string" ? { evidenceSnippet: truncate(redactSecrets(value.evidenceSnippet), 2_000) } : {}),
    ...(typeof value.signaledAt === "string" ? { signaledAt: redactSecrets(value.signaledAt) } : {}),
  };
}

function projectSafeEscalation(value: unknown): EscalationProposal | null {
  if (!isRecord(value)) return null;
  if (typeof value.reason !== "string") return null;
  return {
    reason: truncate(redactSecrets(value.reason), 2_000),
    advisoryOnly: true,
    ...(typeof value.targetRole === "string" ? { targetRole: truncate(redactSecrets(value.targetRole), 200) } : {}),
    ...(typeof value.recommendedRoute === "string" ? { recommendedRoute: truncate(redactSecrets(value.recommendedRoute), 200) } : {}),
    ...(typeof value.suggestedAction === "string" ? { suggestedAction: truncate(redactSecrets(value.suggestedAction), 1_000) } : {}),
  };
}

function projectSafeEvidence(value: unknown): EvidenceBundle | null {
  if (!isRecord(value)) return null;
  const rawItems = Array.isArray(value.items) ? value.items : [];
  const items: EvidenceItem[] = rawItems
    .filter((item: unknown): item is Record<string, unknown> => isRecord(item))
    .slice(0, 100)
    .map((item) => ({
      ...(typeof item.id === "string" ? { id: truncate(redactSecrets(item.id), 100) } : {}),
      ...(typeof item.type === "string" ? { type: truncate(redactSecrets(item.type), 100) } : {}),
      ...(typeof item.claim === "string" ? { claim: truncate(redactSecrets(item.claim), 2_000) } : {}),
      ...(typeof item.source === "string" ? { source: truncate(redactSecrets(item.source), 1_000) } : {}),
      ...(typeof item.snippet === "string" ? { snippet: truncate(redactSecrets(item.snippet), 2_000) } : {}),
      ...(item.confidence !== undefined ? { confidence: item.confidence as any } : {}),
      ...(typeof item.verified === "boolean" ? { verified: item.verified } : {}),
    }));
  return {
    items,
    ...(typeof value.summary === "string" ? { summary: truncate(redactSecrets(value.summary), 2_000) } : {}),
    claimsCount: typeof value.claimsCount === "number" ? value.claimsCount : items.length,
    ...(typeof value.collectedAt === "string" ? { collectedAt: redactSecrets(value.collectedAt) } : {}),
  };
}

const VALIDATION_LIST_KEYS = [
  "testsFailed",
  "testsNotRun",
  "testsPassed",
  "testsUnknown",
  "blockingRisks",
  "unresolved",
  "scopeViolations",
  "deniedActions",
] as const;

function projectSafeValidationEvidence(value: unknown): ValidationEvidence | null {
  if (!isRecord(value)) return null;
  const output: Record<string, unknown> = {};
  for (const key of VALIDATION_LIST_KEYS) {
    if (Array.isArray(value[key])) {
      output[key] = value[key]
        .filter((item): item is string => typeof item === "string")
        .slice(0, 100)
        .map((item) => truncate(redactSecrets(item), 1_000));
    }
  }
  for (const key of ["validationAbsent", "permissionRequired", "partial", "workerFailure", "emptyResult", "claimEvidenceConflict"]) {
    if (typeof value[key] === "boolean") output[key] = value[key];
  }
  if (Array.isArray(value.mandatory)) {
    output.mandatory = value.mandatory
      .filter((item): item is Record<string, unknown> => isRecord(item))
      .slice(0, 100)
      .map((item) => ({
        kind: truncate(redactSecrets(String(item.kind ?? "operational_error")), 60),
        detail: truncate(redactSecrets(String(item.detail ?? "")), 600),
      }));
  }
  return output as unknown as ValidationEvidence;
}

function projectSafeUsage(value: unknown): ResultEnvelope["usage"] | null {
  if (!isRecord(value)) return null;
  const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? Math.max(0, Math.floor(v)) : null);
  const scope = value.usageScope === "cumulative_conversation" || value.usageScope === "per_turn" || value.usageScope === "unknown"
    ? value.usageScope
    : "unknown";
  const source = value.usageSource === "observed" || value.usageSource === "derived" || value.usageSource === "unavailable"
    ? value.usageSource
    : "unavailable";
  const usage: NonNullable<ResultEnvelope["usage"]> = {
    inputTokens: num(value.inputTokens),
    outputTokens: num(value.outputTokens),
    thinkingTokens: num(value.thinkingTokens),
    cachedInputTokens: num(value.cachedInputTokens),
    totalTokens: num(value.totalTokens),
    usageScope: scope,
    usageSource: source,
    ...(typeof value.providerConversationId === "string" ? { providerConversationId: redactSecrets(value.providerConversationId) } : {}),
    ...(value.counterResetDetected === true ? { counterResetDetected: true } : {}),
  };
  const allNull = usage.inputTokens === null && usage.outputTokens === null && usage.thinkingTokens === null &&
    usage.cachedInputTokens === null && usage.totalTokens === null;
  return allNull ? null : usage;
}

function projectSafeMessages(messages: unknown[]): unknown[] {
  return messages
    .slice(-200)
    .map((message) => {
      if (!isRecord(message)) return null;
      const info = isRecord(message.info) ? message.info : null;
      const safeInfo: Record<string, string> = {};
      for (const key of ["id", "role", "sessionID", "parentID", "modelID", "providerID", "variant", "finish"]) {
        const value = info?.[key];
        if (typeof value === "string") safeInfo[key] = redactSecrets(value);
      }
      const parts = (Array.isArray(message.parts) ? message.parts : [])
        .filter((part): part is Record<string, unknown> => isRecord(part) && part.type === "text" && typeof part.text === "string")
        .slice(0, 100)
        .map((part) => ({
          type: "text",
          ...(typeof part.id === "string" ? { id: redactSecrets(part.id) } : {}),
          ...(typeof part.messageID === "string" ? { messageID: redactSecrets(part.messageID) } : {}),
          text: truncate(redactSecrets(part.text as string), 20_000),
        }));
      if (parts.length === 0) return null;
      return {
        ...(Object.keys(safeInfo).length > 0 ? { info: safeInfo } : {}),
        parts,
      };
    })
    .filter((message) => message !== null);
}

function hasOnlyTextParts(message: unknown): boolean {
  if (!isRecord(message) || !Array.isArray(message.parts)) return false;
  return message.parts.length > 0 && message.parts.every((part) => isRecord(part) && part.type === "text" && typeof part.text === "string");
}

function projectSafeDiff(value: unknown): unknown {
  if (typeof value === "string") return truncate(redactSecrets(value), 100_000);
  if (Array.isArray(value)) return value.slice(0, 200).map((item) => projectSafeDiff(item)).filter((item) => item !== null);
  if (!isRecord(value)) return value === null ? null : undefined;
  const output: Record<string, unknown> = {};
  for (const key of ["file", "path", "oldPath", "newPath", "status", "additions", "deletions", "patch", "content"]) {
    if (!(key in value)) continue;
    const child = value[key];
    if (typeof child === "string") output[key] = truncate(redactSecrets(child), 100_000);
    else if (typeof child === "number" || typeof child === "boolean" || child === null) output[key] = child;
  }
  return output;
}

function isRecord(value: unknown): value is Record<string, any> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function formatHumanResult(envelope: ResultEnvelope): string {
  const isAntigravity = envelope.fallback
    ? envelope.fallback.to === "antigravity"
    : (
        envelope.receipt?.provider === "antigravity" ||
        envelope.modelDisplayName.startsWith("Antigravity · ") ||
        envelope.opencodeSessionId.startsWith("antigravity:") ||
        envelope.model.includes("gemini")
      );
  const displayName = isAntigravity ? "Antigravity Sub-Agent" : "DeepSeek Sub-Agent";
  const recoveryTool = isAntigravity ? "subagents_recover_result" : "deepseek_recover_result";
  const sessionLabel = isAntigravity ? "session_id" : "opencode_session_id";
  const lines = [
    displayName + " · " + truncate(envelope.topic.replace(/[\r\n]+/g, " "), 240) + " · " + humanState(envelope.status),
    envelope.modelDisplayName,
    "",
    isAntigravity ? "[ANTIGRAVITY_SUBAGENT_RESULT v1]" : "[OPENCODE_SUBAGENT_RESULT v1]",
    "SUMMARY",
    envelope.summary || "No summary was returned.",
  ];
  lines.push("", "FILES", ...(envelope.files.length > 0 ? envelope.files.map((file) => "• " + file) : ["• none"]));
  lines.push("", "TESTS", ...(envelope.tests.length > 0 ? envelope.tests.map((test) => "• " + test) : ["• none"]));
  lines.push("", "RISKS", ...(envelope.risks.length > 0 ? envelope.risks.map((risk) => "• " + risk) : ["• none"]));
  lines.push("", "DIFF SUMMARY", envelope.diffSummary || "none");
  lines.push("", "FULL RESULT", envelope.fullResultPath, "Use " + recoveryTool + " only for explicit recovery.");
  lines.push(
    "",
    "TECHNICAL METADATA",
    "agent_id: " + envelope.agentId,
    "job_id: " + envelope.jobId,
    sessionLabel + ": " + envelope.opencodeSessionId,
    "model: " + envelope.model,
    "workspace: " + envelope.workspace,
  );
  if (envelope.fallback) {
    lines.push("fallback: from " + envelope.fallback.from + " to " + envelope.fallback.to + " (" + envelope.fallback.reason + ")");
    lines.push("fallback_status: " + envelope.fallback.status);
  }
  if (envelope.earlyExit?.triggered) {
    lines.push("", "EARLY EXIT", envelope.earlyExit.reason || "Triggered");
  }
  if (envelope.evidence && envelope.evidence.items.length > 0) {
    lines.push("", "EVIDENCE", ...envelope.evidence.items.slice(0, 10).map((item) => "• " + (item.claim || item.type || "evidence")));
  }
  if (envelope.escalation) {
    lines.push("", "ESCALATION PROPOSAL (ADVISORY ONLY)", envelope.escalation.reason);
    if (envelope.escalation.recommendedRoute) {
      lines.push("recommended_route: " + envelope.escalation.recommendedRoute);
    }
  }
  lines.push("", "ORCHESTRATOR INSTRUCTION", envelope.orchestratorInstruction);
  return lines.join("\n");
}


export function parseEarlyExit(text: string): EarlyExitSignal | undefined {
  const marker = /\[EARLY_EXIT\]([\s\S]*?)\[\/EARLY_EXIT\]/i.exec(text);
  if (marker?.[1]) {
    try {
      const parsed = JSON.parse(marker[1].trim());
      if (parsed && typeof parsed === "object") {
        return {
          triggered: Boolean(parsed.triggered ?? true),
          reason: truncate(redactSecrets(String(parsed.reason ?? "Early exit triggered")), 1_000),
          ...(parsed.confidence !== undefined ? { confidence: parsed.confidence } : {}),
          ...(typeof parsed.evidenceSnippet === "string" ? { evidenceSnippet: truncate(redactSecrets(parsed.evidenceSnippet), 2_000) } : {}),
          signaledAt: typeof parsed.signaledAt === "string" ? parsed.signaledAt : new Date().toISOString(),
        };
      }
    } catch {}
  }
  const val = headingValue(text, "EARLY_EXIT");
  if (val && !/^(?:none|false|no)$/i.test(val.trim())) {
    return {
      triggered: true,
      reason: truncate(redactSecrets(val.trim()), 1_000),
      signaledAt: new Date().toISOString(),
    };
  }
  return undefined;
}

export function parseEscalation(text: string): EscalationProposal | undefined {
  const marker = /\[ESCALATION(?:_PROPOSAL)?\]([\s\S]*?)\[\/ESCALATION(?:_PROPOSAL)?\]/i.exec(text);
  if (marker?.[1]) {
    try {
      const parsed = JSON.parse(marker[1].trim());
      if (parsed && typeof parsed === "object") {
        return {
          reason: truncate(redactSecrets(String(parsed.reason ?? "Escalation proposed")), 2_000),
          advisoryOnly: true,
          ...(typeof parsed.targetRole === "string" ? { targetRole: truncate(redactSecrets(parsed.targetRole), 200) } : {}),
          ...(typeof parsed.recommendedRoute === "string" ? { recommendedRoute: truncate(redactSecrets(parsed.recommendedRoute), 200) } : {}),
          ...(typeof parsed.suggestedAction === "string" ? { suggestedAction: truncate(redactSecrets(parsed.suggestedAction), 1_000) } : {}),
        };
      }
    } catch {}
  }
  const val = headingValue(text, "ESCALATION") || headingValue(text, "ESCALATION_PROPOSAL");
  if (val && !/^(?:none|false|no)$/i.test(val.trim())) {
    const trimmed = val.trim();
    const routeMatch = /\b(?:pro-max|flash-max|gemini-3\.[78]-flash-high|antigravity-flash-high)\b/i.exec(trimmed);
    return {
      reason: truncate(redactSecrets(trimmed), 2_000),
      advisoryOnly: true,
      ...(routeMatch ? { recommendedRoute: routeMatch[0].toLowerCase() } : {}),
    };
  }
  return undefined;
}

export function parseEvidence(text: string): EvidenceBundle | undefined {
  const marker = /\[EVIDENCE(?:_BUNDLE)?\]([\s\S]*?)\[\/EVIDENCE(?:_BUNDLE)?\]/i.exec(text);
  if (marker?.[1]) {
    try {
      const parsed = JSON.parse(marker[1].trim());
      if (parsed && typeof parsed === "object") {
        const rawItems: unknown[] = Array.isArray(parsed.items) ? parsed.items : [];
        const items: EvidenceItem[] = rawItems
          .filter((item: unknown): item is Record<string, unknown> => isRecord(item))
          .map((item: Record<string, unknown>): EvidenceItem => ({
            ...(typeof item.id === "string" ? { id: truncate(redactSecrets(item.id), 100) } : {}),
            ...(typeof item.type === "string" ? { type: truncate(redactSecrets(item.type), 100) } : {}),
            ...(typeof item.claim === "string" ? { claim: truncate(redactSecrets(item.claim), 2_000) } : {}),
            ...(typeof item.source === "string" ? { source: truncate(redactSecrets(item.source), 1_000) } : {}),
            ...(typeof item.snippet === "string" ? { snippet: truncate(redactSecrets(item.snippet), 2_000) } : {}),
            ...(item.confidence !== undefined ? { confidence: item.confidence as any } : {}),
            ...(typeof item.verified === "boolean" ? { verified: item.verified } : {}),
          }));
        return {
          items,
          ...(typeof parsed.summary === "string" ? { summary: truncate(redactSecrets(parsed.summary), 2_000) } : {}),
          claimsCount: items.length,
          collectedAt: typeof parsed.collectedAt === "string" ? parsed.collectedAt : new Date().toISOString(),
        };
      }
    } catch {}
  }
  const itemsList = headingList(text, "EVIDENCE");
  if (itemsList.length > 0) {
    const items: EvidenceItem[] = itemsList.map((line, idx) => {
      const typeMatch = /^\[([a-zA-Z0-9_-]+)\]\s*(.*)$/.exec(line);
      if (typeMatch && typeMatch[1] && typeMatch[2]) {
        return {
          id: `ev_${idx + 1}`,
          type: truncate(redactSecrets(typeMatch[1]), 50),
          claim: truncate(redactSecrets(typeMatch[2]), 2_000),
          verified: true,
        };
      }
      return {
        id: `ev_${idx + 1}`,
        claim: truncate(redactSecrets(line), 2_000),
      };
    });
    return {
      items,
      claimsCount: items.length,
      collectedAt: new Date().toISOString(),
    };
  }
  return undefined;
}

function parseMessages(messages: OpenCodeMessage[], baselineAssistantId: string | null = null): ParsedSubagentResult {
  const users = messages.filter((message) => message.info?.role === "user");
  const assistants = messages.filter((message) => message.info?.role === "assistant");
  const latest = assistants.at(-1);
  const output = assistantTextAfterBaseline(messages, baselineAssistantId);
  const fullText = output.hasText ? redactSecrets(output.text) : "";
  const statusValue = statusFirstToken(headingValue(fullText, "STATUS"));
  const status = statusValue === "failed"
    ? "failed"
    : statusValue === "aborted"
      ? "aborted"
      : "completed";
  const earlyExit = parseEarlyExit(fullText);
  const escalation = parseEscalation(fullText);
  const evidence = parseEvidence(fullText);
  return {
    status,
    summary: headingValue(fullText, "SUMMARY") || firstParagraph(fullText) || "DeepSeek completed without a structured summary.",
    files: headingList(fullText, "FILES"),
    tests: headingList(fullText, "TESTS"),
    risks: headingList(fullText, "RISKS"),
    unresolved: headingList(fullText, "UNRESOLVED"),
    fullText,
    hasText: output.hasText,
    assistantMessageId: latest?.info?.id ?? null,
    userMessageId: users.at(-1)?.info?.id ?? null,
    ...(earlyExit ? { earlyExit } : {}),
    ...(escalation ? { escalation } : {}),
    ...(evidence ? { evidence } : {}),
  };
}

function extractText(message: OpenCodeMessage): string {
  return (message.parts ?? [])
    .filter((part) => part.type === "text" && typeof part.text === "string")
    .map((part) => part.text as string)
    .join("\n")
    .trim();
}

// The aggregate text can contain intermediate milestone reports followed by a
// final report. Each protocol heading is extracted from its latest occurrence
// so an intermediate report never shadows the final one; earlier fields are
// preserved only when no later occurrence exists.
function headingCaptures(text: string, heading: string): string[] {
  const pattern = new RegExp("(^|\\n)\\s*" + heading + "\\s*:\\s*([\\s\\S]*?)" + PROTOCOL_HEADING_TERMINATOR, "gi");
  const captures: string[] = [];
  for (const match of text.matchAll(pattern)) {
    if (match[2] !== undefined) captures.push(match[2]);
  }
  return captures;
}

function headingValue(text: string, heading: string): string {
  const value = headingCaptures(text, heading).at(-1);
  if (value === undefined) return "";
  return value
    .split(/\r?\n/)
    .map((line) => line.replace(/^\s*(?:[-*•]|\d+[.)])\s*/, "").trim())
    .filter((line) => line.length > 0)
    .join(" ");
}

function headingList(text: string, heading: string): string[] {
  const value = headingCaptures(text, heading).at(-1);
  if (value === undefined) return [];
  return value
    .split(/\r?\n/)
    .map((line) => line.replace(/^\s*(?:[-*•]|\d+[.)])\s*/, "").trim())
    .filter((line) => line.length > 0 && !/^none$/i.test(line))
    .slice(0, 100);
}

// STATUS is compared on the first trimmed token/line only: a multiline
// "STATUS: failed" followed by an explanation line still resolves to failed.
function statusFirstToken(value: string): string {
  const firstLine = value.split(/\r?\n/, 1)[0] ?? "";
  return firstLine.trim().split(/\s+/, 1)[0]?.toLowerCase() ?? "";
}

function firstParagraph(text: string): string {
  return text
    .split(/\r?\n\s*\r?\n/)
    .map((part) => part.replace(/^\s*#+\s*/, "").trim())
    .find((part) => part.length > 0 && !/^(STATUS|SUMMARY|ASSUMPTIONS|CHANGES|FILES|TESTS|RISKS|UNRESOLVED|EARLY_EXIT|ESCALATION|ESCALATION_PROPOSAL|EVIDENCE)\s*:/i.test(part)) ?? "";
}

function summarizeDiff(diff: unknown): string {
  if (Array.isArray(diff)) {
    return diff.map((item) => {
      if (!item || typeof item !== "object") return String(item);
      const record = item as Record<string, unknown>;
      return [record.file, record.path, record.status, record.additions, record.deletions]
        .filter((value) => value !== undefined)
        .join(" ");
    }).filter(Boolean).join("\n");
  }
  if (typeof diff === "string") return diff;
  return diff && typeof diff === "object" ? JSON.stringify(redactUnknown(diff)) : "";
}

function displayModel(modelId: string, variant: string | null): string {
  const base = modelId === "deepseek-v4-flash"
    ? "DeepSeek V4 Flash"
    : modelId === "gemini-3.8-flash-high"
      ? "Gemini 3.8 Flash High"
      : modelId === "gemini-3.7-flash-high"
        ? "Gemini 3.7 Flash High"
        : modelId;
  return variant === "max" ? base + " · Max" : base;
}

function humanState(status: ResultEnvelope["status"]): string {
  if (status === "completed") return "Completed";
  if (status === "completed_partial") return "Completed Partial";
  if (status === "timed_out") return "Timed Out";
  if (status === "failed") return "Failed";
  return "Stopped";
}
