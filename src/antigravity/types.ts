export type AntigravityResultStatus = "completed" | "completed_partial" | "timed_out" | "failed" | "aborted";

/**
 * Result of a single Antigravity `agy` run, shaped after the bridge's
 * persisted ResultEnvelope contract (summary/files/tests/risks/diffSummary/
 * model/workspace/status) so a future integration can map it directly into a
 * persisted envelope. runId maps to the session id slot.
 */
export interface AntigravityRunResult {
  status: AntigravityResultStatus;
  runId: string | null;
  summary: string;
  files: string[];
  tests: string[];
  risks: string[];
  diffSummary: string;
  model: string;
  modelDisplayName: string;
  workspace: string;
  rawOutput: string;
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
  timeoutMs: number;
  sandbox: boolean;
  addDirs: string[];
  dangerouslySkipPermissions: boolean;
  attemptDir: string;
  stdoutPath: string;
  stderrPath: string;
  statusPath: string;
  heartbeatPath: string;
  cancelPath: string;
  createdAt: string;
  maxOutputBytes: number;
}

export interface AntigravityHeartbeat {
  nonce: string;
  supervisorPid: number;
  agyPid: number | null;
  updatedAt: number;
  timestamp: string;
}

export interface AntigravityAttemptStatus {
  schemaVersion: 1;
  attemptId: string;
  status: AntigravityResultStatus;
  exitCode: number | null;
  summary: string;
  runId: string | null;
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
