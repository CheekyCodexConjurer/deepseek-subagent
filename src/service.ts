import { createHash } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { defaultWorkspace } from "./security.js";
import { buildWorkerPrompt } from "./prompts.js";
import { BridgeStore } from "./store.js";
import { newId, normalizeTitle, redactSecrets, truncate, validateContextFiles } from "./security.js";
import { InboxDelivery } from "./delivery/inbox.js";
import {
  CodexAppServerDeliveryAdapter,
  type CodexCorrelation,
  type CodexDeliveryAdapter,
  UnavailableCodexDeliveryAdapter,
} from "./codex/adapter.js";
import { OpenCodeManager, type ManagedOpenCode } from "./opencode/manager.js";
import { formatHumanResult, persistResult } from "./result.js";
import type {
  AgentRecord,
  BridgeConfig,
  ContinueInput,
  JobRecord,
  OpenCodeClientLike,
  OpenCodeEvent,
  ResultEnvelope,
  SpawnInput,
} from "./types.js";

export interface ServiceDependencies {
  store?: BridgeStore;
  manager?: OpenCodeManagerLike;
  codex?: CodexDeliveryAdapter;
  inbox?: InboxDelivery;
}

export interface OpenCodeManagerLike {
  start(workspaceRoot: string): Promise<ManagedOpenCodeLike>;
  stop(): Promise<void>;
}

export interface ManagedOpenCodeLike {
  serverId: string;
  baseUrl: string;
  client: OpenCodeClientLike;
  processId: number | null;
  stop(): Promise<void>;
}

export interface AcceptedOperation {
  accepted: true;
  status: "accepted";
  agentId: string;
  jobId: string;
  topic: string;
  modelDisplayName: string;
  state: "Starting";
  message: string;
}

export class BridgeBusyError extends Error {
  readonly code = "busy";

  constructor(readonly jobId: string) {
    super("Agent is busy with job " + jobId + ". Do not retry in a loop; wait for its asynchronous result or call deepseek_abort.");
    this.name = "BridgeBusyError";
  }
}

export interface ServiceStatus {
  running: boolean;
  opencodeUrl: string | null;
  provider: string;
  model: string;
  variant: string | null;
  codexDelivery: { available: boolean; reason: string | null };
  lastStreamError: string | null;
}

const ACTIVE_JOB_STATUSES = new Set(["dispatching", "running", "needs_approval"]);

export class BridgeService {
  private readonly store: BridgeStore;
  private readonly manager: OpenCodeManagerLike;
  private readonly inbox: InboxDelivery;
  private codex: CodexDeliveryAdapter;
  private managed: ManagedOpenCodeLike | null = null;
  private client: OpenCodeClientLike | null = null;
  private streamAbort: AbortController | null = null;
  private streamTask: Promise<void> | null = null;
  private correlationUnsubscribe: (() => void) | null = null;
  private running = false;
  private ownedStore = false;
  private lastStreamError: string | null = null;
  private readonly deliveryLocks = new Map<string, Promise<void>>();
  private readonly approvalTimers = new Map<string, NodeJS.Timeout>();

  constructor(private readonly config: BridgeConfig, dependencies: ServiceDependencies = {}) {
    this.store = dependencies.store ?? new BridgeStore(path.join(config.dataDir, "bridge.sqlite"));
    this.ownedStore = !dependencies.store;
    this.manager = dependencies.manager ?? new OpenCodeManager(config);
    this.codex = dependencies.codex ?? (
      config.codexAppServerCommand || config.codexAppServerSocket
        ? new CodexAppServerDeliveryAdapter(config)
        : new UnavailableCodexDeliveryAdapter()
    );
    this.inbox = dependencies.inbox ?? new InboxDelivery(config.dataDir);
  }

  async start(): Promise<void> {
    if (this.running) return;
    const managed = await this.manager.start(defaultWorkspace());
    this.managed = managed;
    this.client = managed.client;
    this.store.registerServer({
      id: managed.serverId,
      workspaceRoot: defaultWorkspace(),
      baseUrl: managed.baseUrl,
      processId: managed.processId,
    });
    try {
      await this.codex.start();
    } catch (error) {
      this.codex = new UnavailableCodexDeliveryAdapter(redactSecrets(String(error)));
    }
    this.correlationUnsubscribe = this.codex.onCorrelation((correlation) => {
      void this.handleCorrelation(correlation).catch((error) => {
        this.lastStreamError = redactSecrets(String(error));
      });
    });
    this.running = true;
    this.streamAbort = new AbortController();
    this.streamTask = managed.client.subscribe(
      (event) => this.handleEvent(event),
      this.streamAbort.signal,
    ).catch((error: unknown) => {
      if (!this.streamAbort?.signal.aborted) this.lastStreamError = redactSecrets(String(error));
    });
    await this.recoverPendingJobs();
  }

  async stop(): Promise<void> {
    this.running = false;
    for (const timer of this.approvalTimers.values()) clearTimeout(timer);
    this.approvalTimers.clear();
    this.streamAbort?.abort();
    this.streamAbort = null;
    if (this.streamTask) {
      await Promise.race([
        this.streamTask,
        new Promise<void>((resolve) => setTimeout(resolve, 2_000)),
      ]);
    }
    this.streamTask = null;
    this.correlationUnsubscribe?.();
    this.correlationUnsubscribe = null;
    await this.codex.close().catch(() => undefined);
    await this.manager.stop().catch(() => undefined);
    this.store.stopServers();
    if (this.ownedStore) this.store.close();
    this.client = null;
    this.managed = null;
  }

  status(): ServiceStatus {
    return {
      running: this.running,
      opencodeUrl: this.managed?.baseUrl ?? null,
      provider: this.config.opencodeProviderId,
      model: this.config.opencodeModelId,
      variant: this.config.opencodeVariant,
      codexDelivery: { available: this.codex.available, reason: this.codex.reason },
      lastStreamError: this.lastStreamError,
    };
  }

  async spawn(input: SpawnInput): Promise<AcceptedOperation> {
    this.requireRunning();
    if (!input.task.trim()) throw new Error("Task must not be empty");
    if (input.task.length > this.config.maxTaskLength) throw new Error("Task exceeds configured length limit");
    const existing = this.store.getJobByRequestId(input.requestId);
    if (existing) return this.accepted(existing);
    const repositoryRoot = path.resolve(input.cwd ?? defaultWorkspace());
    await ensureDirectory(repositoryRoot);
    const mode = input.mode ?? "analyze";
    const strategy = input.workspaceStrategy ?? (mode === "edit" ? "worktree" : "shared");
    const agentId = newId("agent");
    const workspacePath = await prepareWorkspace(repositoryRoot, strategy, agentId);
    validateContextFiles(workspacePath, input.contextFiles ?? []);
    const prompt = await buildWorkerPrompt({ ...input, mode, workspaceStrategy: strategy }, workspacePath, {
      maxLength: this.config.maxTaskLength,
    });
    const title = normalizeTitle(input.topic);
    const session = await this.clientOrThrow().createSession(workspacePath, title);
    const agent = this.store.createAgent({
      id: agentId,
      title,
      topic: input.topic.trim() || title,
      repositoryRoot,
      workspacePath,
      workspaceStrategy: strategy,
      opencodeServerId: this.managed?.serverId ?? "unknown",
      opencodeSessionId: session.id,
      modelProviderId: this.config.opencodeProviderId,
      modelId: this.config.opencodeModelId,
      modelVariant: this.config.opencodeVariant,
    });
    const job = this.store.createJob({
      id: newId("job"),
      agentId: agent.id,
      kind: "spawn",
      requestId: input.requestId,
      promptHash: hashPrompt(prompt),
    });
    if (input.threadId) {
      this.store.bindJob({
        jobId: job.id,
        threadId: input.threadId,
        originatingTurnId: input.turnId ?? null,
        originatingItemId: null,
      });
    }
    return this.dispatch(agent, job, prompt);
  }

  async continueJob(input: ContinueInput): Promise<AcceptedOperation> {
    this.requireRunning();
    if (!input.task.trim()) throw new Error("Task must not be empty");
    if (input.task.length > this.config.maxTaskLength) throw new Error("Task exceeds configured length limit");
    const existing = this.store.getJobByRequestId(input.requestId);
    if (existing) return this.accepted(existing);
    const agent = this.store.getAgent(input.agentId);
    if (!agent) throw new Error("Unknown agent: " + input.agentId);
    if (agent.status === "closed" || agent.status === "aborted") throw new Error("Agent is not continuable");
    const active = this.activeJob(agent.id);
    if (active && active.status !== "needs_approval") throw new BridgeBusyError(active.id);
    const prompt = await buildWorkerPrompt({
      task: input.task,
      relation: input.relation,
    }, agent.workspacePath, { maxLength: this.config.maxTaskLength });
    if (active?.status === "needs_approval") {
      this.clearApprovalTimer(agent.id);
      if (input.permissionId || input.permissionReply) {
        if (!input.permissionId || !input.permissionReply) {
          throw new Error("permissionId and permissionReply are both required to answer an approval request");
        }
        return this.replyApproval(agent, active, input.permissionId, input.permissionReply, input.permissionMessage);
      }
      return this.resumeApproval(agent, active, prompt);
    }
    const job = this.store.createJob({
      id: newId("job"),
      agentId: agent.id,
      kind: "continue",
      requestId: input.requestId,
      promptHash: hashPrompt(prompt),
    });
    const priorBinding = this.store.getLatestBindingForAgent(agent.id);
    const threadId = input.threadId ?? priorBinding?.threadId;
    if (threadId) {
      this.store.bindJob({
        jobId: job.id,
        threadId,
        originatingTurnId: input.turnId ?? priorBinding?.originatingTurnId ?? null,
        originatingItemId: null,
      });
    }
    if (agent.status !== "working") this.store.updateAgentStatus(agent.id, "working");
    return this.dispatch(agent, job, prompt);
  }

  async abort(agentId: string, reason?: string): Promise<{ agentId: string; jobId: string | null; status: string }> {
    this.requireRunning();
    const agent = this.store.getAgent(agentId);
    if (!agent) throw new Error("Unknown agent: " + agentId);
    const active = this.activeJob(agentId);
    if (!active) {
      if (agent.status !== "closed" && agent.status !== "aborted") this.store.updateAgentStatus(agentId, "aborted", reason ?? null);
      return { agentId, jobId: null, status: "aborted" };
    }
    this.clearApprovalTimer(agentId);
    await this.clientOrThrow().abort(agent.opencodeSessionId);
    if (active.status !== "aborted") this.store.updateJobStatus(active.id, "aborted", reason ?? "Aborted by orchestrator");
    if (agent.status !== "aborted" && agent.status !== "closed") this.store.updateAgentStatus(agent.id, "aborted", reason ?? null);
    return { agentId, jobId: active.id, status: "aborted" };
  }

  async close(agentId: string): Promise<{ agentId: string; status: string }> {
    const agent = this.store.getAgent(agentId);
    if (!agent) throw new Error("Unknown agent: " + agentId);
    const active = this.activeJob(agentId);
    if (active) await this.abort(agentId, "Closed by orchestrator");
    const refreshed = this.store.getAgent(agentId);
    if (refreshed && refreshed.status !== "closed") this.store.updateAgentStatus(agentId, "closed");
    return { agentId, status: "closed" };
  }

  async recoverResult(jobId: string, agentId?: string): Promise<unknown> {
    let job = this.store.getJob(jobId);
    if (!job) throw new Error("Unknown job: " + jobId);
    if (agentId && job.agentId !== agentId) throw new Error("Job does not belong to the requested agent");
    if (!job.resultPath && this.client && ["dispatching", "running", "completed", "delivery_pending"].includes(job.status)) {
      await this.reconcileJob(job);
      job = this.store.getJob(jobId);
    }
    if (!job?.resultPath) throw new Error("No persisted result is available for job " + jobId);
    return JSON.parse(await readFile(job.resultPath, "utf8"));
  }

  getAgent(agentId: string): AgentRecord | null {
    return this.store.getAgent(agentId);
  }

  getJob(jobId: string): JobRecord | null {
    return this.store.getJob(jobId);
  }

  listAgents(): AgentRecord[] {
    return this.store.listAgents();
  }

  listJobs(): JobRecord[] {
    return this.store.listJobs();
  }

  async deliverPending(): Promise<void> {
    for (const job of this.store.listJobs("delivery_pending")) {
      await this.deliverPersistedJob(job);
    }
  }

  async deliverJob(jobId: string): Promise<void> {
    const job = this.store.getJob(jobId);
    if (!job) throw new Error("Unknown job: " + jobId);
    if (!job.resultPath) throw new Error("Job has no persisted result");
    if (job.status === "completed") this.store.updateJobStatus(job.id, "delivery_pending");
    const pending = this.store.getJob(job.id);
    if (!pending) throw new Error("Job disappeared: " + job.id);
    await this.deliverPersistedJob(pending);
  }

  private async dispatch(agent: AgentRecord, job: JobRecord, prompt: string): Promise<AcceptedOperation> {
    this.store.updateAgentStatus(agent.id, "working");
    this.store.updateJobStatus(job.id, "dispatching");
    try {
      await this.clientOrThrow().promptAsync(jobAgentSession(agent), prompt, {
        providerId: this.config.opencodeProviderId,
        modelId: this.config.opencodeModelId,
        ...(this.config.opencodeVariant ? { variant: this.config.opencodeVariant } : {}),
        ...(this.config.opencodeAgent ? { agent: this.config.opencodeAgent } : {}),
      });
      this.store.updateJobStatus(job.id, "running");
      return this.accepted(this.store.getJob(job.id) ?? job);
    } catch (error) {
      const message = redactSecrets(String(error));
      const current = this.store.getJob(job.id);
      if (current && current.status !== "failed") this.store.updateJobStatus(job.id, "failed", message);
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, "failed", message);
      throw new Error(message);
    }
  }

  private async resumeApproval(agent: AgentRecord, job: JobRecord, prompt: string): Promise<AcceptedOperation> {
    this.store.setJobPermission(job.id, null);
    this.store.updateJobStatus(job.id, "running");
    if (agent.status === "needs_approval") this.store.updateAgentStatus(agent.id, "working");
    try {
      await this.clientOrThrow().promptAsync(agent.opencodeSessionId, prompt, {
        providerId: this.config.opencodeProviderId,
        modelId: this.config.opencodeModelId,
        ...(this.config.opencodeVariant ? { variant: this.config.opencodeVariant } : {}),
        ...(this.config.opencodeAgent ? { agent: this.config.opencodeAgent } : {}),
      });
      return this.accepted(this.store.getJob(job.id) ?? job);
    } catch (error) {
      const message = redactSecrets(String(error));
      const current = this.store.getJob(job.id);
      if (current && current.status !== "failed") this.store.updateJobStatus(job.id, "failed", message);
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, "failed", message);
      throw new Error(message);
    }
  }

  private async replyApproval(
    agent: AgentRecord,
    job: JobRecord,
    permissionId: string,
    reply: "once" | "always" | "reject",
    message?: string,
  ): Promise<AcceptedOperation> {
    this.store.updateJobStatus(job.id, "running");
    if (agent.status === "needs_approval") this.store.updateAgentStatus(agent.id, "working");
    try {
      await this.clientOrThrow().replyPermission(agent.opencodeSessionId, permissionId, reply, message);
      this.store.setJobPermission(job.id, null);
      return this.accepted(this.store.getJob(job.id) ?? job);
    } catch (error) {
      const errorText = redactSecrets(String(error));
      const current = this.store.getJob(job.id);
      if (current && current.status !== "failed") this.store.updateJobStatus(job.id, "failed", errorText);
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, "failed", errorText);
      throw new Error(errorText);
    }
  }

  private async handleEvent(event: OpenCodeEvent): Promise<void> {
    const sessionId = findSessionId(event.properties);
    if (!sessionId) return;
    const agent = this.store.getAgentBySession(sessionId);
    if (!agent) return;
    const sourceEventId = event.id ?? createHash("sha256").update(event.type + ":" + JSON.stringify(event.properties)).digest("hex");
    if (!this.store.insertEvent({
      source: "opencode",
      sourceEventId,
      eventType: event.type,
      sessionId,
      jobId: this.activeJob(agent.id)?.id ?? null,
    })) return;
    try {
      const status = findStatus(event.properties);
      if (event.type === "session.error" || event.type.includes(".error")) {
        await this.failActive(agent, redactSecrets(JSON.stringify(event.properties)));
      } else if (event.type.includes("permission") || event.type.includes("approval")) {
        await this.markNeedsApproval(agent, event.properties);
      } else if (event.type === "session.idle" || status === "idle") {
        await this.completeActive(agent);
      }
      this.store.markEventProcessed("opencode", sourceEventId);
    } catch (error) {
      this.lastStreamError = redactSecrets(String(error));
      this.store.markEventProcessed("opencode", sourceEventId);
    }
  }

  private async completeActive(agent: AgentRecord): Promise<void> {
    const job = this.activeJob(agent.id);
    if (!job || job.status === "needs_approval") return;
    const client = this.clientOrThrow();
    const messages = await client.listMessages(agent.opencodeSessionId);
    const diff = await client.getDiff(agent.opencodeSessionId);
    const currentAgent = this.store.getAgent(agent.id) ?? agent;
    const currentJob = this.store.getJob(job.id) ?? job;
    if (currentJob.status === "dispatching") this.store.updateJobStatus(job.id, "running");
    const stored = await persistResult(this.config.dataDir, currentAgent, currentJob, messages, diff, this.config.maxResultLength);
    this.store.setJobMessages(job.id, stored.parsed.userMessageId, stored.parsed.assistantMessageId);
    this.store.setJobResult(job.id, stored.resultPath, stored.envelope.summary);
    const completedJob = this.store.getJob(job.id) ?? job;
    if (completedJob.status === "running") this.store.updateJobStatus(job.id, "completed");
    const completedAgent = this.store.getAgent(agent.id) ?? agent;
    if (completedAgent.status === "working") this.store.updateAgentStatus(agent.id, "completed");
    const deliveryJob = this.store.getJob(job.id) ?? job;
    if (deliveryJob.status === "completed") this.store.updateJobStatus(job.id, "delivery_pending");
    await this.deliverEnvelope(stored.envelope, this.store.getJob(job.id) ?? job);
  }

  private async deliverPersistedJob(job: JobRecord): Promise<void> {
    if (!job.resultPath) return;
    const parsed = JSON.parse(await readFile(job.resultPath, "utf8")) as { envelope?: ResultEnvelope };
    if (!parsed.envelope) throw new Error("Persisted result has no envelope");
    await this.deliverEnvelope(parsed.envelope, job);
  }

  private async deliverEnvelope(envelope: ResultEnvelope, job: JobRecord): Promise<void> {
    const binding = this.store.getBinding(job.id);
    const lockKey = binding?.threadId ?? "inbox";
    const previous = this.deliveryLocks.get(lockKey) ?? Promise.resolve();
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const chain = previous.then(() => gate);
    this.deliveryLocks.set(lockKey, chain);
    await previous;
    try {
      await this.deliverEnvelopeNow(envelope, job);
    } finally {
      release?.();
      if (this.deliveryLocks.get(lockKey) === chain) this.deliveryLocks.delete(lockKey);
    }
  }

  private async deliverEnvelopeNow(envelope: ResultEnvelope, job: JobRecord): Promise<void> {
    const binding = this.store.getBinding(job.id);
    const humanText = formatHumanResult(envelope);
    const method = binding && this.codex.available ? "codex-start" : "inbox";
    const delivery = this.store.createDelivery({
      jobId: job.id,
      threadId: binding?.threadId ?? "inbox",
      expectedTurnId: binding?.originatingTurnId ?? null,
      deliveryMethod: method,
    });
    if (delivery.status === "delivered") {
      if (job.status === "delivery_pending") this.store.updateJobStatus(job.id, "delivered");
      return;
    }
    try {
      if (binding && this.codex.available) {
        const actualMethod = await this.codex.deliver(job, binding, humanText);
        this.store.setDeliveryMethod(delivery.id, actualMethod);
      } else {
        await this.inbox.deliver(envelope, humanText);
        this.store.setDeliveryMethod(delivery.id, "inbox");
      }
      this.store.updateDelivery(delivery.id, "delivered");
      const current = this.store.getJob(job.id);
      if (current?.status === "delivery_pending") this.store.updateJobStatus(job.id, "delivered");
    } catch (error) {
      const message = redactSecrets(String(error));
      try {
        await this.inbox.deliver(envelope, humanText);
        this.store.setDeliveryMethod(delivery.id, "inbox", message);
        this.store.updateDelivery(delivery.id, "delivered");
        const current = this.store.getJob(job.id);
        if (current?.status === "delivery_pending") this.store.updateJobStatus(job.id, "delivered");
      } catch (fallbackError) {
        this.store.updateDelivery(delivery.id, "failed", message + "; inbox: " + redactSecrets(String(fallbackError)));
        const current = this.store.getJob(job.id);
        if (current?.status === "delivery_pending") this.store.updateJobStatus(job.id, "failed", message);
      }
    }
  }

  private async reconcileJob(job: JobRecord): Promise<void> {
    const agent = this.store.getAgent(job.agentId);
    if (!agent || !this.client) return;
    const messages = await this.client.listMessages(agent.opencodeSessionId);
    const assistants = messages.filter((message) => message.info?.role === "assistant");
    if (assistants.length === 0 || messages.at(-1)?.info?.role !== "assistant") return;
    const diff = await this.client.getDiff(agent.opencodeSessionId);
    const currentAgent = this.store.getAgent(agent.id) ?? agent;
    const stored = await persistResult(this.config.dataDir, currentAgent, job, messages, diff, this.config.maxResultLength);
    this.store.setJobMessages(job.id, stored.parsed.userMessageId, stored.parsed.assistantMessageId);
    this.store.setJobResult(job.id, stored.resultPath, stored.envelope.summary);
    const current = this.store.getJob(job.id);
    if (current?.status === "dispatching") this.store.updateJobStatus(job.id, "running");
    const running = this.store.getJob(job.id);
    if (running?.status === "running") this.store.updateJobStatus(job.id, "completed");
    const currentAgentStatus = this.store.getAgent(agent.id);
    if (currentAgentStatus?.status === "working") this.store.updateAgentStatus(agent.id, "completed");
    const completed = this.store.getJob(job.id);
    if (completed?.status === "completed") this.store.updateJobStatus(job.id, "delivery_pending");
    const pending = this.store.getJob(job.id);
    if (pending) await this.deliverEnvelope(stored.envelope, pending);
  }

  private async recoverPendingJobs(): Promise<void> {
    for (const job of this.store.recoverPendingJobs()) {
      if (["dispatching", "running"].includes(job.status)) {
        await this.reconcileJob(job).catch((error) => {
          this.lastStreamError = redactSecrets(String(error));
        });
      } else if (job.status === "completed") {
        this.store.updateJobStatus(job.id, "delivery_pending");
        const pending = this.store.getJob(job.id);
        if (pending) await this.deliverPersistedJob(pending).catch((error) => {
          this.lastStreamError = redactSecrets(String(error));
        });
      } else if (job.status === "delivery_pending") {
        await this.deliverPersistedJob(job).catch((error) => {
          this.lastStreamError = redactSecrets(String(error));
        });
      } else if (job.status === "needs_approval") {
        const agent = this.store.getAgent(job.agentId);
        if (agent) await this.markNeedsApproval(agent, { permissionID: job.permissionId }).catch((error) => {
          this.lastStreamError = redactSecrets(String(error));
        });
      }
    }
  }

  private async handleCorrelation(correlation: CodexCorrelation): Promise<void> {
    const job = this.store.getJob(correlation.jobId);
    if (!job) return;
    this.store.bindJob({
      jobId: job.id,
      threadId: correlation.threadId,
      originatingTurnId: correlation.turnId,
      originatingItemId: correlation.itemId,
    });
    if (job.status === "completed") {
      this.store.updateJobStatus(job.id, "delivery_pending");
      const refreshed = this.store.getJob(job.id);
      if (refreshed) await this.deliverPersistedJob(refreshed);
    }
  }

  private async failActive(agent: AgentRecord, error: string): Promise<void> {
    const job = this.activeJob(agent.id);
    if (!job) return;
    if (job.status !== "failed") this.store.updateJobStatus(job.id, "failed", error);
    const currentAgent = this.store.getAgent(agent.id);
    if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, "failed", error);
  }

  private async markNeedsApproval(agent: AgentRecord, properties: Record<string, unknown> = {}): Promise<void> {
    const job = this.activeJob(agent.id);
    if (!job) return;
    const permissionId = findPermissionId(properties) ?? job.permissionId;
    if (job.status === "running") this.store.updateJobStatus(job.id, "needs_approval");
    if (permissionId) this.store.setJobPermission(job.id, permissionId);
    const current = this.store.getAgent(agent.id);
    if (current?.status === "working") this.store.updateAgentStatus(agent.id, "needs_approval");
    await this.inbox.writeNotice({
      kind: "needs_approval",
      agentId: agent.id,
      jobId: job.id,
      topic: agent.topic,
      message: "OpenCode requested approval. Review the task and use deepseek_continue for an explicit response.",
      permissionId,
    });
    this.clearApprovalTimer(agent.id);
    const timer = setTimeout(() => {
      void this.expireApproval(agent.id, job.id);
    }, this.config.approvalTimeoutMs);
    timer.unref?.();
    this.approvalTimers.set(agent.id, timer);
  }

  private clearApprovalTimer(agentId: string): void {
    const timer = this.approvalTimers.get(agentId);
    if (timer) clearTimeout(timer);
    this.approvalTimers.delete(agentId);
  }

  private async expireApproval(agentId: string, jobId: string): Promise<void> {
    this.approvalTimers.delete(agentId);
    const job = this.store.getJob(jobId);
    if (!job || job.status !== "needs_approval") return;
    this.store.updateJobStatus(job.id, "failed", "Approval timeout expired");
    const agent = this.store.getAgent(agentId);
    if (agent && agent.status === "needs_approval") this.store.updateAgentStatus(agent.id, "failed", "Approval timeout expired");
    await this.inbox.writeNotice({
      kind: "approval_timeout",
      agentId,
      jobId,
      topic: agent?.topic ?? "DeepSeek task",
      message: "The approval window expired. Start an explicit continuation if the work is still needed.",
      permissionId: job.permissionId,
    });
  }

  private activeJob(agentId: string): JobRecord | null {
    return this.store.listJobs().find((job) => job.agentId === agentId && ACTIVE_JOB_STATUSES.has(job.status)) ?? null;
  }

  private accepted(job: JobRecord): AcceptedOperation {
    const agent = this.store.getAgent(job.agentId);
    if (!agent) throw new Error("Job has no agent: " + job.id);
    return {
      accepted: true,
      status: "accepted",
      agentId: agent.id,
      jobId: job.id,
      topic: agent.topic,
      modelDisplayName: agent.modelId === "deepseek-v4-flash"
        ? "DeepSeek V4 Flash" + (agent.modelVariant === "max" ? " · Max" : "")
        : agent.modelId,
      state: "Starting",
      message: "DeepSeek Sub-Agent accepted the task and will report asynchronously.",
    };
  }

  private clientOrThrow(): OpenCodeClientLike {
    if (!this.client) throw new Error("Bridge daemon is not started");
    return this.client;
  }

  private requireRunning(): void {
    if (!this.running || !this.client) throw new Error("Bridge daemon is not started");
  }
}

function jobAgentSession(agent: AgentRecord): string {
  return agent.opencodeSessionId;
}

function hashPrompt(prompt: string): string {
  return createHash("sha256").update(prompt, "utf8").digest("hex");
}

function findSessionId(properties: Record<string, unknown>): string | null {
  for (const key of ["sessionID", "sessionId", "session_id"]) {
    if (typeof properties[key] === "string") return properties[key] as string;
  }
  return null;
}

function findStatus(properties: Record<string, unknown>): string | null {
  const direct = properties.status;
  if (typeof direct === "string") return direct;
  if (direct && typeof direct === "object") {
    const type = (direct as Record<string, unknown>).type;
    return typeof type === "string" ? type : null;
  }
  return null;
}

function findPermissionId(properties: Record<string, unknown>): string | null {
  for (const key of ["permissionID", "permissionId", "permission_id", "requestID", "requestId"]) {
    if (typeof properties[key] === "string" && properties[key]) return properties[key] as string;
  }
  for (const key of ["permission", "request"]) {
    const nested = properties[key];
    if (nested && typeof nested === "object") {
      const nestedRecord = nested as Record<string, unknown>;
      if (typeof nestedRecord.id === "string" && nestedRecord.id) return nestedRecord.id;
      const found = findPermissionId(nestedRecord);
      if (found) return found;
    }
  }
  return null;
}

async function ensureDirectory(directory: string): Promise<void> {
  const info = await stat(directory);
  if (!info.isDirectory()) throw new Error("Workspace is not a directory: " + directory);
}

async function prepareWorkspace(repositoryRoot: string, strategy: "shared" | "worktree", agentId: string): Promise<string> {
  if (strategy === "shared") return repositoryRoot;
  await assertCleanGitRepository(repositoryRoot);
  const workspacePath = path.join(repositoryRoot, ".deepseek-worktrees", agentId);
  const relative = path.relative(repositoryRoot, workspacePath);
  if (relative.startsWith(".." + path.sep) || path.isAbsolute(relative)) throw new Error("Invalid worktree path");
  const result = await runGit(repositoryRoot, ["worktree", "add", "--detach", workspacePath, "HEAD"]);
  if (result.code !== 0) throw new Error("Unable to create worktree: " + truncate(redactSecrets(result.stderr), 1_000));
  return workspacePath;
}

async function assertCleanGitRepository(repositoryRoot: string): Promise<void> {
  const root = await runGit(repositoryRoot, ["rev-parse", "--show-toplevel"]);
  if (root.code !== 0) {
    throw new Error("worktree strategy requires a Git repository with a committed HEAD");
  }
  const head = await runGit(repositoryRoot, ["rev-parse", "--verify", "HEAD"]);
  if (head.code !== 0) {
    throw new Error("worktree strategy requires a Git repository with a committed HEAD");
  }
  const status = await runGit(repositoryRoot, ["status", "--porcelain=v1", "--untracked-files=all"]);
  if (status.code !== 0) {
    throw new Error("Unable to inspect Git worktree status");
  }
  if (status.stdout.trim().length > 0) {
    throw new Error("Repository has uncommitted changes; worktree from HEAD cannot represent them. Use shared explicitly or preserve the changes outside the bridge first.");
  }
}

async function runGit(cwd: string, args: string[]): Promise<{ code: number | null; stdout: string; stderr: string }> {
  const child = spawn("git", ["-C", cwd, ...args], { cwd, shell: false, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout?.on("data", (chunk: Buffer) => stdout.push(chunk));
  child.stderr?.on("data", (chunk: Buffer) => stderr.push(chunk));
  const code = await new Promise<number | null>((resolve) => child.once("close", resolve));
  return { code, stdout: Buffer.concat(stdout).toString("utf8"), stderr: Buffer.concat(stderr).toString("utf8") };
}
