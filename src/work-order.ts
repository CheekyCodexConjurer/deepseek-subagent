import { z } from "zod";
import { InvalidRequestError } from "./errors.js";
import { redactSecrets, truncate } from "./security.js";
import type { EvidenceBundle, WorkOrderContractV1, WorkOrderEvaluationV1, WorkOrderOutcome } from "./types.js";

export const MAX_WORK_ORDER_BYTES = 16_384;
export const MAX_WORK_ORDER_CRITERIA = 12;
export const MAX_WORK_ORDER_EVIDENCE_REFS = 4;

const text = (max: number) => z.string().trim().min(1).max(max);
const textList = (maxCount: number, maxText: number) => z.array(text(maxText)).max(maxCount);

export const workOrderV1McpSchema = z.object({
  schema_version: z.literal(1),
  contract_version: z.number().int().min(1).max(1_000_000),
  objective: text(2_000),
  scope: textList(32, 500).min(1),
  ownership: textList(32, 500).min(1),
  context_refs: textList(48, 500).default([]),
  design_decisions: textList(24, 500).default([]),
  invariants: textList(32, 500).default([]),
  acceptance_criteria: z.array(z.object({
    id: z.string().trim().min(1).max(32).regex(/^[A-Za-z][A-Za-z0-9_.-]*$/),
    description: text(800),
    requires_git_diff: z.boolean().optional(),
  }).strict()).min(1).max(MAX_WORK_ORDER_CRITERIA),
  validation_commands: textList(24, 500).default([]),
  escalation_conditions: textList(24, 500).default([]),
}).strict().superRefine((value, ctx) => {
  const seen = new Set<string>();
  value.acceptance_criteria.forEach((criterion, index) => {
    if (seen.has(criterion.id)) {
      ctx.addIssue({ code: "custom", path: ["acceptance_criteria", index, "id"], message: "criterion IDs must be unique" });
    }
    seen.add(criterion.id);
  });
});

const workOrderOutcomeSchema = z.object({
  id: z.string().trim().min(1).max(32),
  outcome: z.enum(["satisfied", "not_satisfied", "blocked", "not_run", "unknown"]),
  evidence_refs: z.array(z.string().trim().min(1).max(64)).max(MAX_WORK_ORDER_EVIDENCE_REFS),
  note: z.string().trim().max(1_000).optional(),
}).strict();

const workOrderOutcomesSchema = z.object({
  contract_version: z.number().int().min(1),
  criteria: z.array(workOrderOutcomeSchema).max(MAX_WORK_ORDER_CRITERIA),
}).strict();

/** Accepts the public snake_case shape and the internal camelCase type. */
export function parseWorkOrderContract(value: unknown): WorkOrderContractV1 {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new InvalidRequestError("work_order must be a versioned object");
  }
  const raw = value as Record<string, unknown>;
  const wire = {
    schema_version: raw.schema_version ?? raw.schemaVersion,
    contract_version: raw.contract_version ?? raw.contractVersion,
    objective: raw.objective,
    scope: raw.scope,
    ownership: raw.ownership,
    context_refs: raw.context_refs ?? raw.contextRefs,
    design_decisions: raw.design_decisions ?? raw.designDecisions,
    invariants: raw.invariants,
    acceptance_criteria: raw.acceptance_criteria ?? raw.acceptanceCriteria,
    validation_commands: raw.validation_commands ?? raw.validationCommands,
    escalation_conditions: raw.escalation_conditions ?? raw.escalationConditions,
  };
  const parsed = workOrderV1McpSchema.safeParse(wire);
  if (!parsed.success) {
    const first = parsed.error.issues[0];
    const field = first?.path.length ? first.path.join(".") + ": " : "";
    throw new InvalidRequestError("Invalid work_order " + field + (first?.message ?? "shape"));
  }
  if (Buffer.byteLength(JSON.stringify(parsed.data), "utf8") > MAX_WORK_ORDER_BYTES) {
    throw new InvalidRequestError("work_order exceeds the configured 16 KB contract limit");
  }
  const clean = (value: string): string => redactSecrets(value);
  return {
    schemaVersion: 1,
    contractVersion: parsed.data.contract_version,
    objective: clean(parsed.data.objective),
    scope: parsed.data.scope.map(clean),
    ownership: parsed.data.ownership.map(clean),
    contextRefs: parsed.data.context_refs.map(clean),
    designDecisions: parsed.data.design_decisions.map(clean),
    invariants: parsed.data.invariants.map(clean),
    acceptanceCriteria: parsed.data.acceptance_criteria.map((criterion) => ({
      id: clean(criterion.id),
      description: clean(criterion.description),
      ...(criterion.requires_git_diff ? { requiresGitDiff: true } : {}),
    })),
    validationCommands: parsed.data.validation_commands.map(clean),
    escalationConditions: parsed.data.escalation_conditions.map(clean),
  };
}

export function workOrderToWire(value: WorkOrderContractV1): Record<string, unknown> {
  return {
    schema_version: value.schemaVersion,
    contract_version: value.contractVersion,
    objective: value.objective,
    scope: value.scope,
    ownership: value.ownership,
    context_refs: value.contextRefs,
    design_decisions: value.designDecisions,
    invariants: value.invariants,
    acceptance_criteria: value.acceptanceCriteria.map((criterion) => ({
      id: criterion.id,
      description: criterion.description,
      ...(criterion.requiresGitDiff ? { requires_git_diff: true } : {}),
    })),
    validation_commands: value.validationCommands,
    escalation_conditions: value.escalationConditions,
  };
}

export function sameWorkOrder(left: WorkOrderContractV1, right: WorkOrderContractV1): boolean {
  return JSON.stringify(workOrderToWire(left)) === JSON.stringify(workOrderToWire(right));
}

/**
 * Parse one explicit worker outcome block. Missing, malformed, mismatched, or
 * unreferenced outcomes become visible unknown records instead of a green gap.
 */
export function evaluateWorkOrder(input: {
  text: string;
  workOrder: WorkOrderContractV1;
  resultHash: string;
  evidence?: EvidenceBundle;
  confirmedPreviousContractVersion?: number;
  diffAvailability?: "unavailable" | "summary_only" | "literal_git_diff";
  resultTextTruncated?: boolean;
  diffSummaryTruncated?: boolean;
}): WorkOrderEvaluationV1 {
  const marker = /^[ \t]*WORK_ORDER_OUTCOMES_JSON[ \t]*:[ \t]*/gmi;
  const matches = Array.from(input.text.matchAll(marker));
  const issues: string[] = [];
  const diffAvailability = input.diffAvailability ?? "unavailable";
  const resultTextTruncated = input.resultTextTruncated ?? false;
  const diffSummaryTruncated = input.diffSummaryTruncated ?? false;
  if (resultTextTruncated) issues.push("result_text_truncated");
  let parsed: z.infer<typeof workOrderOutcomesSchema> | null = null;

  if (matches.length !== 1) {
    issues.push(matches.length === 0 ? "missing_outcome_block" : "duplicate_outcome_blocks");
  } else {
    const match = matches[0]!;
    const start = (match.index ?? 0) + match[0].length;
    const objectText = extractJsonObject(input.text.slice(start));
    if (!objectText) {
      issues.push("malformed_outcome_json");
    } else {
      try {
        const outcomeParse = workOrderOutcomesSchema.safeParse(JSON.parse(objectText));
        if (outcomeParse.success) parsed = outcomeParse.data;
        else issues.push("invalid_outcome_shape");
      } catch {
        issues.push("malformed_outcome_json");
      }
    }
  }

  if (parsed && parsed.contract_version !== input.workOrder.contractVersion) {
    issues.push("contract_version_mismatch");
    parsed = null;
  }

  const ids = new Map<string, number>();
  for (const item of input.evidence?.items ?? []) {
    if (typeof item.id === "string" && item.id.trim()) ids.set(item.id, (ids.get(item.id) ?? 0) + 1);
  }
  const records = new Map<string, z.infer<typeof workOrderOutcomeSchema>[]>();
  for (const outcome of parsed?.criteria ?? []) {
    const list = records.get(outcome.id) ?? [];
    list.push(outcome);
    records.set(outcome.id, list);
  }
  for (const id of records.keys()) {
    if (!input.workOrder.acceptanceCriteria.some((criterion) => criterion.id === id)) issues.push("unexpected_criterion_id");
  }

  const criteria = input.workOrder.acceptanceCriteria.map((criterion) => {
    const candidates = records.get(criterion.id) ?? [];
    const record = candidates.length === 1 ? candidates[0] : undefined;
    if (candidates.length > 1) issues.push("duplicate_criterion_outcome");
    if (!record) {
      if (resultTextTruncated) {
        return {
          id: criterion.id,
          outcome: "unknown" as const,
          evidenceRefs: [],
          evidenceRefsResolved: false,
          note: "Persisted result text was truncated; this criterion is unverified.",
        };
      }
      if (criterion.requiresGitDiff && diffAvailability !== "literal_git_diff") {
        issues.push("git_diff_unavailable:" + criterion.id);
        return {
          id: criterion.id,
          outcome: "blocked" as const,
          evidenceRefs: [],
          evidenceRefsResolved: false,
          note: "Literal Git diff is unavailable; the bridge captured only a diff summary.",
        };
      }
      issues.push("missing_criterion_outcome");
      return {
        id: criterion.id,
        outcome: "unknown" as const,
        evidenceRefs: [],
        evidenceRefsResolved: false,
      };
    }
    const evidenceRefs = record.evidence_refs.map((ref) => truncate(redactSecrets(ref), 64));
    const evidenceRefsResolved = evidenceRefs.length > 0 && evidenceRefs.every((ref) => ids.get(ref) === 1);
    if (!evidenceRefsResolved) issues.push("unresolved_evidence_ref");
    let outcome: WorkOrderOutcome = record.outcome;
    let note = record.note ? truncate(redactSecrets(record.note), 1_000) : undefined;
    if (resultTextTruncated) {
      outcome = "unknown";
      note = "Persisted result text was truncated; this criterion is unverified.";
    } else if (criterion.requiresGitDiff && diffAvailability !== "literal_git_diff") {
      outcome = "blocked";
      note = "Literal Git diff is unavailable; the bridge captured only a diff summary.";
      issues.push("git_diff_unavailable:" + criterion.id);
    } else if (criterion.requiresGitDiff && diffSummaryTruncated) {
      outcome = "blocked";
      note = "Diff content was truncated; this criterion is unverified.";
      issues.push("diff_content_truncated:" + criterion.id);
    }
    return {
      id: criterion.id,
      outcome,
      evidenceRefs,
      evidenceRefsResolved,
      ...(note ? { note } : {}),
    };
  });

  return {
    schemaVersion: 1,
    contractVersion: input.workOrder.contractVersion,
    resultHash: input.resultHash,
    diffAvailability,
    gitDiffAvailable: diffAvailability === "literal_git_diff",
    resultTextTruncated,
    diffSummaryTruncated,
    source: "worker_report",
    complete: issues.length === 0 && criteria.length === input.workOrder.acceptanceCriteria.length,
    criteria,
    issues: Array.from(new Set(issues)).slice(0, 12),
    ...(input.confirmedPreviousContractVersion === undefined
      ? {}
      : { confirmedPreviousContractVersion: input.confirmedPreviousContractVersion }),
  };
}

function extractJsonObject(text: string): string | null {
  const start = text.indexOf("{");
  if (start < 0) return null;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = start; index < text.length; index += 1) {
    const char = text[index];
    if (inString) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') inString = false;
      continue;
    }
    if (char === '"') inString = true;
    else if (char === "{") depth += 1;
    else if (char === "}" && --depth === 0) return text.slice(start, index + 1);
  }
  return null;
}
