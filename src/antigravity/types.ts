import type { EarlyExitSignal, EscalationProposal, EvidenceBundle } from "../types.js";

export type AntigravityResultStatus = "completed" | "completed_partial" | "timed_out" | "failed" | "aborted";

export interface WorkerTokenUsage {
  inputTokens: number;
  outputTokens: number;
  thinkingTokens: number;
  cachedInputTokens: number;
  totalTokens: number;
}

/**
 * Result of a single Antigravity `agy` run, shaped after the bridge's
 * persisted ResultEnvelope contract (summary/files/tests/risks/diffSummary/
 * model/workspace/status) so a future integration can map it directly into a
 * persisted envelope. runId maps to the session id slot.
 */
export interface AntigravityRunResult {
  status: AntigravityResultStatus;
  runId: string | null;
  conversationId?: string | null;
  summary: string;
  fullText?: string;
  files: string[];
  tests: string[];
  risks: string[];
  diffSummary: string;
  model: string;
  modelDisplayName: string;
  workspace: string;
  rawOutput: string;
  usage?: WorkerTokenUsage;
  durationSeconds?: number | null;
  numTurns?: number | null;
  evidence?: EvidenceBundle;
  earlyExit?: EarlyExitSignal;
  escalation?: EscalationProposal;
}

export type AntigravityProcessErrorKind = "spawn" | "timeout" | "aborted" | "exit" | "invalid_output";

export interface AntigravityAttemptManifest {
  schemaVersion: 1;
  agentId: string;
  jobId: string;
  requestId: string;
  attemptId: string;
  parentAttemptId: string | null;
  promptHash: string;
  promptPath: string;
  cwd: string;
  modelProviderId: string;
  modelId: string;
  modelVariant: string | null;
  modelRoute: string | null;
  command: string;
  args: string[];
  timeoutMs: number | null;
  sandbox: boolean;
  addDirs: string[];
  dangerouslySkipPermissions: boolean;
  conversationId?: string | null;
  outputFormat?: "text" | "json" | "stream-json" | null;
  attemptDir: string;
  stdoutPath: string;
  stderrPath: string;
  statusPath: string;
  heartbeatPath: string;
  cancelPath: string;
  progressPath?: string;
  createdAt: string;
  maxOutputBytes: number;
  fence?: number | null;
}

export interface AntigravityHeartbeat {
  attemptId?: string;
  nonce: string;
  supervisorPid: number;
  agyPid: number | null;
  updatedAt: number;
  timestamp: string;
  fence?: number | null;
  lastProgressAt?: string | null;
  progressRevision?: number | null;
}

export interface AntigravityStreamProgress {
  attemptId: string;
  lastProgressAt: string;
  progressRevision: number;
  fence?: number | null;
  totalBytes: number;
}

export interface AntigravityAttemptStatus {
  schemaVersion: 1;
  attemptId: string;
  status: AntigravityResultStatus;
  exitCode: number | null;
  summary: string;
  fullText?: string;
  runId: string | null;
  conversationId?: string | null;
  usage?: WorkerTokenUsage;
  durationSeconds?: number | null;
  numTurns?: number | null;
  files: string[];
  tests: string[];
  risks: string[];
  diffSummary: string;
  error: string | null;
  completedAt: string;
  stdout: string;
  stderr: string;
}

export interface AntigravityRecoveryClaim {
  claimedBy: string;
  claimedAt: string;
  jobId: string;
}

export interface SupervisorMetrics {
  chunksReceived: number;
  progressWritesAttempted: number;
  progressWritesCompleted: number;
  coalescedChunks: number;
}
