export type WorkspaceStrategy = "shared" | "worktree";
export type AgentMode = "analyze" | "edit" | "test";
export type JobKind = "spawn" | "continue";
export type RetentionMode = "auto" | "disabled" | "dry-run" | "enabled";

export interface ModelRoute {
  name: string;
  providerId: string;
  modelId: string;
  variant: string | null;
  enabled: boolean;
  default: boolean;
  display: string;
}

export interface ResolvedRoute {
  name: string;
  providerId: string;
  modelId: string;
  variant: string | null;
  display: string;
}

/** How the currently effective active route was determined. */
export type ActiveRouteSource = "configured-default" | "operator-set";

/**
 * Operator-visible active-route state. The effective route is the operator-set
 * pointer persisted in the bridge store when one exists, otherwise the
 * configured default route. `activeRoute` is null (with `activeRouteError`)
 * only when the effective route cannot resolve from the registry.
 */
export interface RouteStatusInfo {
  activeRoute: ResolvedRoute | null;
  activeRouteError: string | null;
  defaultModelRoute: string;
  source: ActiveRouteSource;
  routes: ModelRoute[];
}

export type JobStatus =
  | "created"
  | "dispatching"
  | "running"
  | "following"
  | "finalizing"
  | "needs_approval"
  | "completed"
  | "completed_partial"
  | "timed_out"
  | "delivery_pending"
  | "delivered"
  | "failed"
  | "aborted";

export type AgentStatus =
  | "created"
  | "working"
  | "needs_approval"
  | "completed"
  | "completed_partial"
  | "timed_out"
  | "failed"
  | "aborted"
  | "closed";

export type DeliveryMethod = "codex-steer" | "codex-start" | "inbox";
export type DeliveryStatus = "pending" | "delivered" | "failed";

export interface SpawnInput {
  requestId: string;
  topic: string;
  task: string;
  cwd?: string;
  mode?: AgentMode;
  workspaceStrategy?: WorkspaceStrategy;
  contextFiles?: string[];
  visualContext?: string;
  threadId?: string;
  turnId?: string;
  modelRoute?: string;
}

export interface ContinueInput {
  requestId: string;
  agentId: string;
  relation: "clarification" | "correction" | "review" | "continuation";
  task: string;
  visualContext?: string;
  threadId?: string;
  turnId?: string;
  permissionId?: string;
  permissionReply?: "once" | "always" | "reject";
  permissionMessage?: string;
  /**
   * Opt-in recovery for a closed agent: when true and the agent is closed
   * after a terminal job with a persisted result (and was NOT explicitly
   * aborted), the continuation is accepted automatically by spawning a NEW
   * agent/session with auditable lineage in the persisted workspace, topic,
   * strategy and pinned model route. Never applicable to aborted agents,
   * closed agents without a persisted result, busy agents or scope changes;
   * a closed agent is never itself reopened.
   */
  allowRespawn?: boolean;
}

export interface ConsultInput {
  agentId: string;
  jobId?: string;
  activityLimit?: number;
}

export interface FollowInput {
  agentId: string;
  jobId?: string;
  waitMinutes?: number;
  graceMinutes?: number;
}

export interface AbortInput {
  agentId: string;
  reason?: string;
}

export interface AgentRecord {
  id: string;
  title: string;
  topic: string;
  repositoryRoot: string;
  workspacePath: string;
  workspaceStrategy: WorkspaceStrategy;
  mode?: AgentMode;
  opencodeServerId: string;
  opencodeSessionId: string;
  modelProviderId: string;
  modelId: string;
  modelVariant: string | null;
  modelRoute: string | null;
  /** Lineage link to the agent this one was spawned from (closed-agent resume). */
  parentAgentId: string | null;
  status: AgentStatus;
  createdAt: string;
  updatedAt: string;
  closedAt: string | null;
  lastError: string | null;
}

export interface JobRecord {
  id: string;
  agentId: string;
  sequence: number;
  kind: JobKind;
  requestId: string;
  promptHash: string;
  status: JobStatus;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
  lastUserMessageId: string | null;
  lastAssistantMessageId: string | null;
  permissionId: string | null;
  resultPath: string | null;
  resultSummary: string | null;
  error: string | null;
  followStartedAt: string | null;
  followDeadlineAt: string | null;
  followGraceMinutes: number | null;
  graceDeadlineAt: string | null;
  gracefulFinalizeAttempted: boolean;
  approvalDeadlineAt: string | null;
  hintThreadId: string | null;
  hintTurnId: string | null;
  hintSource: string | null;
  dispatchUnknown: boolean;
  resultConsumedAt: string | null;
  fallbackFrom?: string | null;
  fallbackTo?: string | null;
  fallbackReason?: string | null;
  fallbackStatus?: string | null;
  fallbackCount?: number;
}


export type ActivityType =
  | "dispatch"
  | "event"
  | "approval"
  | "result"
  | "deadline"
  | "finalize"
  | "abort"
  | "error";

export interface AgentActivity {
  id: string;
  agentId: string;
  jobId: string | null;
  sessionId: string | null;
  activityType: ActivityType;
  summary: string;
  createdAt: string;
}

export interface ProgressActivity {
  type: ActivityType;
  summary: string;
  timestamp: string;
}

export interface ProgressSnapshot {
  agentId: string;
  jobId: string | null;
  topic: string;
  status: string;
  elapsedSeconds: number;
  lastActivityAgoSeconds: number | null;
  currentActivity: string;
  recentActivity: ProgressActivity[];
  filesTouched: string[];
  testSummary: string;
  resultAvailable: boolean;
}

export interface FollowResult {
  agentId: string;
  jobId: string;
  status: "completed" | "completed_partial" | "timed_out" | "failed" | "aborted" | "needs_approval";
  deadlineReached: boolean;
  gracefulFinalize: boolean;
  partial: boolean;
  workerAborted: boolean;
  resultAvailable: boolean;
  result?: { envelope: ResultEnvelope };
  progress: ProgressSnapshot;
  error?: string;
  permissionId?: string | null;
  message?: string;
}

export interface CodexBinding {
  jobId: string;
  threadId: string;
  originatingTurnId: string | null;
  originatingItemId: string | null;
  boundAt: string;
}

export interface DeliveryRecord {
  id: string;
  jobId: string;
  threadId: string;
  expectedTurnId: string | null;
  deliveryMethod: DeliveryMethod;
  status: DeliveryStatus;
  attempts: number;
  createdAt: string;
  deliveredAt: string | null;
  lastError: string | null;
}

export interface OpenCodeSession {
  id: string;
  directory?: string;
  title?: string;
  model?: {
    id?: string;
    providerID?: string;
    variant?: string;
  };
}

export interface OpenCodeMessage {
  info?: {
    id?: string;
    role?: string;
    sessionID?: string;
    parentID?: string;
    modelID?: string;
    providerID?: string;
    variant?: string;
    finish?: string;
    error?: unknown;
  };
  parts?: Array<{
    type?: string;
    text?: string;
    id?: string;
    messageID?: string;
  }>;
}

export interface OpenCodeEvent {
  id?: string;
  type: string;
  properties: Record<string, unknown>;
  raw?: unknown;
}

export interface OpenCodeClientLike {
  health(): Promise<{ healthy: boolean; version?: string }>;
  createSession(directory: string, title: string): Promise<OpenCodeSession>;
  promptAsync(
    sessionId: string,
    task: string,
    options: { providerId: string; modelId: string; variant?: string; agent?: string },
  ): Promise<void>;
  listMessages(sessionId: string): Promise<OpenCodeMessage[]>;
  getDiff(sessionId: string): Promise<unknown>;
  abort(sessionId: string): Promise<void>;
  replyPermission(sessionId: string, permissionId: string, reply: "once" | "always" | "reject", message?: string): Promise<void>;
  subscribe(onEvent: (event: OpenCodeEvent) => Promise<void> | void, signal?: AbortSignal): Promise<void>;
}

export interface ResultEnvelope {
  version: 1;
  agentId: string;
  jobId: string;
  topic: string;
  status: "completed" | "completed_partial" | "timed_out" | "failed" | "aborted";
  opencodeSessionId: string;
  model: string;
  modelDisplayName: string;
  workspace: string;
  summary: string;
  files: string[];
  tests: string[];
  risks: string[];
  diffSummary: string;
  fullResultPath: string;
  orchestratorInstruction: string;
  deadlineReached?: boolean;
  gracefulFinalize?: boolean;
  partial?: boolean;
  workerAborted?: boolean;
  fallback?: {
    from: string;
    to: string;
    reason: string;
    status: string;
  };
}

export interface BridgeConfig {
  dataDir: string;
  configPath: string;
  daemonHost: string;
  daemonPort: number;
  daemonToken: string;
  opencodeMode: "managed" | "attach";
  opencodeUrl: string | null;
  opencodeUsername: string;
  opencodePassword: string | null;
  opencodeBinary: string | null;
  opencodeProviderId: string;
  opencodeModelId: string;
  opencodeVariant: string | null;
  opencodeAgent: string;
  opencodeStartupTimeoutMs: number;
  opencodeEventReconnectMaxMs: number;
  approvalTimeoutMs: number;
  codexCorrelationWindowMs: number;
  experimentalSameChatDelivery: boolean;
  followDefaultWaitMinutes: number;
  followDefaultGraceMinutes: number;
  codexAppServerSocket: string | null;
  codexAppServerCommand: string | null;
  codexAppServerArgs: string[];
  maxTaskLength: number;
  maxResultLength: number;
  antigravitySandbox: boolean;
  antigravityAddDirs: string[];
  antigravityAutoApprovePermissions: boolean;
  antigravityCommand: string | null;
  antigravityTimeoutFallbackRoute: string | null;
  modelRoutes: ModelRoute[];
  defaultModelRoute: string;
  retentionMode: RetentionMode;
  maxContextFileBytes: number;
  globalGeminiContextPath: string;
  backupDir: string;
}


export interface BackupManifest {
  version: 1;
  timestamp: string;
  databaseFile: string;
  databaseSizeBytes: number;
  databaseSha256: string;
  sourceDatabasePath: string;
}

export interface BackupResult {
  snapshotPath: string;
  manifestPath: string;
  manifest: BackupManifest;
}

export interface BackupOptions {
  sourceDbPath: string;
  destinationDir: string;
  now?: string | undefined;
}

export interface DoctorCheck {
  name: string;
  status: "ok" | "warning" | "error" | "unknown";
  detail: string;
}

export interface DoctorReport {
  generatedAt: string;
  displayName: string;
  checks: DoctorCheck[];
  completeDeliverySupported: boolean;
}
