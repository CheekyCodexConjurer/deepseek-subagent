import { mkdir } from "node:fs/promises";
import path from "node:path";
import { writePrivateFile, redactSecrets, truncate } from "./security.js";
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
): Promise<{ envelope: ResultEnvelope; resultPath: string; parsed: ParsedSubagentResult }> {
  const parsed = parseMessages(messages);
  const resultPath = path.join(dataDir, "results", job.id + ".json");
  const diffSummary = summarizeDiff(diff);
  const envelope: ResultEnvelope = {
    version: 1,
    agentId: agent.id,
    jobId: job.id,
    topic: agent.topic,
    status: parsed.status,
    opencodeSessionId: agent.opencodeSessionId,
    model: agent.modelProviderId + "/" + agent.modelId + (agent.modelVariant ? " · " + agent.modelVariant : ""),
    modelDisplayName: displayModel(agent.modelId, agent.modelVariant),
    workspace: agent.workspacePath,
    summary: truncate(parsed.summary, 4_000),
    files: parsed.files.slice(0, 100),
    tests: parsed.tests.slice(0, 100),
    risks: parsed.risks.concat(parsed.unresolved).slice(0, 100),
    diffSummary: truncate(diffSummary, 10_000),
    fullResultPath: resultPath,
    orchestratorInstruction: "Continue this agent only with deepseek_continue after reviewing this result.",
  };
  await mkdir(path.dirname(resultPath), { recursive: true });
  await writePrivateFile(
    resultPath,
    JSON.stringify({
      envelope,
      rawAssistantText: truncate(redactSecrets(parsed.fullText), maxLength),
      messages,
      diff,
      savedAt: new Date().toISOString(),
    }, null, 2) + "\n",
  );
  return { envelope, resultPath, parsed };
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
  const fullText = latest ? extractText(latest) : "";
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
  return diff && typeof diff === "object" ? JSON.stringify(diff) : "";
}

function displayModel(modelId: string, variant: string | null): string {
  const base = modelId === "deepseek-v4-flash" ? "DeepSeek V4 Flash" : modelId;
  return variant === "max" ? base + " · Max" : base;
}

function humanState(status: ResultEnvelope["status"]): string {
  if (status === "completed") return "Completed";
  if (status === "failed") return "Failed";
  return "Stopped";
}
