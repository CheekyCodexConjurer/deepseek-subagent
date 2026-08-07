export type WorkspaceStrategy = "shared" | "worktree";
export type AgentMode = "analyze" | "edit" | "test";
export type JobKind = "spawn" | "continue";
export type JobStatus =
  | "created"
  | "dispatching"
  | "running"
  | "needs_approval"
  | "completed"
  | "delivery_pending"
  | "delivered"
  | "failed"
  | "aborted";

export type AgentStatus =
  | "created"
  | "working"
  | "needs_approval"
  | "completed"
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
  threadId?: string;
  turnId?: string;
}

export interface ContinueInput {
  requestId: string;
  agentId: string;
  relation: "clarification" | "correction" | "review" | "continuation";
  task: string;
  threadId?: string;
  turnId?: string;
  permissionId?: string;
  permissionReply?: "once" | "always" | "reject";
  permissionMessage?: string;
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
  opencodeServerId: string;
  opencodeSessionId: string;
  modelProviderId: string;
  modelId: string;
  modelVariant: string | null;
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
  status: "completed" | "failed" | "aborted";
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
  codexAppServerSocket: string | null;
  codexAppServerCommand: string | null;
  codexAppServerArgs: string[];
  maxTaskLength: number;
  maxResultLength: number;
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
