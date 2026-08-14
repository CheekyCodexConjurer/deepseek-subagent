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
