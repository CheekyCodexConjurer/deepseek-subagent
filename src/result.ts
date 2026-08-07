import { mkdir } from "node:fs/promises";
import path from "node:path";
import { writePrivateFile, redactSecrets, redactUnknown, truncate } from "./security.js";
import type { AgentRecord, JobRecord, OpenCodeMessage, ResultEnvelope } from "./types.js";

export interface ParsedSubagentResult {
  status: "completed" | "failed" | "aborted";
  summary: string;
  files: string[];
  tests: string[];
  risks: string[];
  unresolved: string[];
  fullText: string;
  assistantMessageId: string | null;
  userMessageId: string | null;
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
  } = {},
): Promise<{ envelope: ResultEnvelope; resultPath: string; parsed: ParsedSubagentResult }> {
  const parsed = parseMessages(messages);
  const resultPath = path.join(dataDir, "results", job.id + ".json");
  const diffSummary = redactSecrets(summarizeDiff(diff));
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

export function sanitizePersistedResult(value: unknown, maxLength = 100_000): unknown {
  if (!isRecord(value)) return {};
  const output: Record<string, unknown> = {};
  const envelope = projectSafeEnvelope(value.envelope);
  if (envelope) output.envelope = envelope;
  const messages = Array.isArray(value.messages) ? value.messages : null;
  if (messages) output.messages = projectSafeMessages(messages);
  if (messages && messages.length > 0 && messages.every((message) => hasOnlyTextParts(message)) && typeof value.rawAssistantText === "string") {
    output.rawAssistantText = truncate(redactSecrets(value.rawAssistantText), maxLength);
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
  for (const key of ["files", "tests", "risks"]) {
    if (Array.isArray(value[key])) {
      output[key] = value[key].filter((item): item is string => typeof item === "string").slice(0, 100).map((item) => redactSecrets(item));
    }
  }
  if (value.version === 1) output.version = 1;
  for (const key of ["deadlineReached", "gracefulFinalize", "partial", "workerAborted"]) {
    if (typeof value[key] === "boolean") output[key] = value[key];
  }
  return output;
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
  const lines = [
    "DeepSeek Sub-Agent · " + truncate(envelope.topic.replace(/[\r\n]+/g, " "), 240) + " · " + humanState(envelope.status),
    envelope.modelDisplayName,
    "",
    "[OPENCODE_SUBAGENT_RESULT v1]",
    "SUMMARY",
    envelope.summary || "No summary was returned.",
  ];
  lines.push("", "FILES", ...(envelope.files.length > 0 ? envelope.files.map((file) => "• " + file) : ["• none"]));
  lines.push("", "TESTS", ...(envelope.tests.length > 0 ? envelope.tests.map((test) => "• " + test) : ["• none"]));
  lines.push("", "RISKS", ...(envelope.risks.length > 0 ? envelope.risks.map((risk) => "• " + risk) : ["• none"]));
  lines.push("", "DIFF SUMMARY", envelope.diffSummary || "none");
  lines.push("", "FULL RESULT", envelope.fullResultPath, "Use deepseek_recover_result only for explicit recovery.");
  lines.push(
    "",
    "TECHNICAL METADATA",
    "agent_id: " + envelope.agentId,
    "job_id: " + envelope.jobId,
    "opencode_session_id: " + envelope.opencodeSessionId,
    "model: " + envelope.model,
    "workspace: " + envelope.workspace,
  );
  lines.push("", "ORCHESTRATOR INSTRUCTION", envelope.orchestratorInstruction);
  return lines.join("\n");
}

function parseMessages(messages: OpenCodeMessage[]): ParsedSubagentResult {
  const users = messages.filter((message) => message.info?.role === "user");
  const assistants = messages.filter((message) => message.info?.role === "assistant");
  const latest = assistants.at(-1);
  const fullText = latest ? redactSecrets(extractText(latest)) : "";
  const statusValue = headingValue(fullText, "STATUS").toLowerCase();
  const status = statusValue === "failed"
    ? "failed"
    : statusValue === "aborted"
      ? "aborted"
      : "completed";
  return {
    status,
    summary: headingValue(fullText, "SUMMARY") || firstParagraph(fullText) || "DeepSeek completed without a structured summary.",
    files: headingList(fullText, "FILES"),
    tests: headingList(fullText, "TESTS"),
    risks: headingList(fullText, "RISKS"),
    unresolved: headingList(fullText, "UNRESOLVED"),
    fullText,
    assistantMessageId: latest?.info?.id ?? null,
    userMessageId: users.at(-1)?.info?.id ?? null,
  };
}

function extractText(message: OpenCodeMessage): string {
  return (message.parts ?? [])
    .filter((part) => part.type === "text" && typeof part.text === "string")
    .map((part) => part.text as string)
    .join("\n")
    .trim();
}

function headingValue(text: string, heading: string): string {
  const match = text.match(new RegExp("(^|\\n)\\s*" + heading + "\\s*:\\s*([^\\n]+)", "i"));
  return match?.[2]?.trim() ?? "";
}

function headingList(text: string, heading: string): string[] {
  const match = text.match(new RegExp("(^|\\n)\\s*" + heading + "\\s*:\\s*([\\s\\S]*?)(?=\\n\\s*[A-Z][A-Z_ ]+\\s*:|$)", "i"));
  if (!match?.[2]) return [];
  return match[2]
    .split(/\r?\n/)
    .map((line) => line.replace(/^\s*(?:[-*•]|\d+[.)])\s*/, "").trim())
    .filter((line) => line.length > 0 && !/^none$/i.test(line))
    .slice(0, 100);
}

function firstParagraph(text: string): string {
  return text
    .split(/\r?\n\s*\r?\n/)
    .map((part) => part.replace(/^\s*#+\s*/, "").trim())
    .find((part) => part.length > 0 && !/^(STATUS|SUMMARY|ASSUMPTIONS|CHANGES|FILES|TESTS|RISKS|UNRESOLVED)\s*:/i.test(part)) ?? "";
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
  const base = modelId === "deepseek-v4-flash" ? "DeepSeek V4 Flash" : modelId;
  return variant === "max" ? base + " · Max" : base;
}

function humanState(status: ResultEnvelope["status"]): string {
  if (status === "completed") return "Completed";
  if (status === "completed_partial") return "Completed Partial";
  if (status === "timed_out") return "Timed Out";
  if (status === "failed") return "Failed";
  return "Stopped";
}
