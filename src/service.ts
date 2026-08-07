import { createHash } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { defaultWorkspace } from "./security.js";
import { buildWorkerPrompt, GRACEFUL_FINALIZE_PROMPT } from "./prompts.js";
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
import { formatHumanResult, persistResult, sanitizePersistedEnvelope, sanitizePersistedResult } from "./result.js";
import type {
  AgentRecord,
  BridgeConfig,
  ConsultInput,
  ContinueInput,
  FollowInput,
  FollowResult,
  JobRecord,
  OpenCodeClientLike,
  OpenCodeEvent,
  OpenCodeMessage,
  ProgressActivity,
  ProgressSnapshot,
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
  experimentalSameChatDelivery: boolean;
  followDefaultWaitMinutes: number;
  followDefaultGraceMinutes: number;
  codexDelivery: { available: boolean; reason: string | null };
  lastStreamError: string | null;
}

const ACTIVE_JOB_STATUSES = new Set(["dispatching", "running", "following", "finalizing", "needs_approval"]);
const TERMINAL_JOB_STATUSES = new Set(["completed", "completed_partial", "timed_out", "failed", "aborted", "delivered"]);

interface FollowLifecycle {
  jobId: string;
  waitMinutes: number;
  graceMinutes: number;
  promise: Promise<FollowResult>;
  resolve: (result: FollowResult) => void;
  reject: (error: unknown) => void;
  deadlineTimer: NodeJS.Timeout | null;
  graceTimer: NodeJS.Timeout | null;
  waiters: Set<symbol>;
  settled: boolean;
}

export class FollowCancelledError extends Error {
  readonly code = "follow_cancelled";

  constructor() {
    super("deepseek_follow waiter was cancelled; the DeepSeek worker continues running");
    this.name = "FollowCancelledError";
  }
}

type DeliveryAdmissionMode = "read" | "write";

interface DeliveryAdmissionWaiter {
  mode: DeliveryAdmissionMode;
  resolve: () => void;
}

class DeliveryAdmission {
  private readers = 0;
  private writer = false;
  private readonly waiters: DeliveryAdmissionWaiter[] = [];

  async withRead<T>(operation: () => Promise<T>): Promise<T> {
    await this.acquire("read");
    try {
      return await operation();
    } finally {
      this.release("read");
    }
  }

  async withWrite<T>(operation: () => Promise<T>): Promise<T> {
    await this.acquire("write");
    try {
      return await operation();
    } finally {
      this.release("write");
    }
  }

  private acquire(mode: DeliveryAdmissionMode): Promise<void> {
    return new Promise<void>((resolve) => {
      this.waiters.push({ mode, resolve });
      this.drain();
    });
  }

  private release(mode: DeliveryAdmissionMode): void {
    if (mode === "write") this.writer = false;
    else this.readers -= 1;
    this.drain();
  }

  private drain(): void {
    if (this.writer) return;
    if (this.readers > 0) {
      while (this.waiters[0]?.mode === "read") this.grantRead();
      return;
    }
    if (this.waiters[0]?.mode === "write") {
      const waiter = this.waiters.shift();
      if (!waiter) return;
      this.writer = true;
      waiter.resolve();
      return;
    }
    while (this.waiters[0]?.mode === "read") this.grantRead();
  }

  private grantRead(): void {
    const waiter = this.waiters.shift();
    if (!waiter) return;
    this.readers += 1;
    waiter.resolve();
  }
}

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
  private readonly agentOperationLocks = new Map<string, Promise<void>>();
  private readonly requestLocks = new Map<string, Promise<void>>();
  private readonly deliveryAdmission = new DeliveryAdmission();
  private readonly correlationFallbackTimers = new Map<string, NodeJS.Timeout>();
  private readonly inboxFallbackJobs = new Set<string>();
  private readonly approvalTimers = new Map<string, NodeJS.Timeout>();
  private readonly eventProcessing = new Set<string>();
  private readonly eventRetryTimers = new Map<string, NodeJS.Timeout>();
  private readonly followLifecycles = new Map<string, FollowLifecycle>();

  constructor(private readonly config: BridgeConfig, dependencies: ServiceDependencies = {}) {
    this.store = dependencies.store ?? new BridgeStore(path.join(config.dataDir, "bridge.sqlite"));
    this.ownedStore = !dependencies.store;
    this.manager = dependencies.manager ?? new OpenCodeManager(config);
    this.codex = config.experimentalSameChatDelivery
      ? dependencies.codex ?? (
        config.codexAppServerCommand || config.codexAppServerSocket
          ? new CodexAppServerDeliveryAdapter(config)
          : new UnavailableCodexDeliveryAdapter("No compatible Codex App Server connection is configured")
      )
      : new UnavailableCodexDeliveryAdapter("Same-chat push is experimental and disabled by default");
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
    if (this.config.experimentalSameChatDelivery) {
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
    }
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
    for (const timer of this.correlationFallbackTimers.values()) clearTimeout(timer);
    this.correlationFallbackTimers.clear();
    for (const timer of this.eventRetryTimers.values()) clearTimeout(timer);
    this.eventRetryTimers.clear();
    this.eventProcessing.clear();
    this.inboxFallbackJobs.clear();
    for (const lifecycle of this.followLifecycles.values()) {
      if (lifecycle.deadlineTimer) clearTimeout(lifecycle.deadlineTimer);
      if (lifecycle.graceTimer) clearTimeout(lifecycle.graceTimer);
      if (!lifecycle.settled) {
        if (lifecycle.waiters.size > 0) lifecycle.reject(new Error("Bridge daemon stopped while following DeepSeek"));
        lifecycle.settled = true;
      }
    }
    this.followLifecycles.clear();
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
    if (this.config.experimentalSameChatDelivery) await this.codex.close().catch(() => undefined);
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
      experimentalSameChatDelivery: this.config.experimentalSameChatDelivery,
      followDefaultWaitMinutes: this.config.followDefaultWaitMinutes,
      followDefaultGraceMinutes: this.config.followDefaultGraceMinutes,
      codexDelivery: { available: this.codex.available, reason: this.codex.reason },
      lastStreamError: this.lastStreamError,
    };
  }

  async spawn(input: SpawnInput): Promise<AcceptedOperation> {
    this.requireRunning();
    if (!input.task.trim()) throw new Error("Task must not be empty");
    if (input.task.length > this.config.maxTaskLength) throw new Error("Task exceeds configured length limit");
    return this.withRequestIdLock(input.requestId, async () => {
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
      this.recordActivity(agent, null, "dispatch", "Created OpenCode session for the DeepSeek task");
      const job = this.store.createJob({
        id: newId("job"),
        agentId: agent.id,
        kind: "spawn",
        requestId: input.requestId,
        promptHash: hashPrompt(prompt),
      });
      // MCP arguments are not proof of origin. A new binding is accepted only
      // after the configured App Server reports this job in item/completed.
      return this.dispatch(agent, job, prompt);
    });
  }

  async continueJob(input: ContinueInput): Promise<AcceptedOperation> {
    this.requireRunning();
    if (!input.task.trim()) throw new Error("Task must not be empty");
    if (input.task.length > this.config.maxTaskLength) throw new Error("Task exceeds configured length limit");
    return this.withAgentOperationLock(input.agentId, async () => {
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
        if (input.permissionId || input.permissionReply || input.permissionMessage) {
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
      if (agent.status !== "working") this.store.updateAgentStatus(agent.id, "working");
      return this.dispatch(agent, job, prompt);
    });
  }

  async consult(input: ConsultInput): Promise<ProgressSnapshot> {
    this.requireRunning();
    const agent = this.store.getAgent(input.agentId);
    if (!agent) throw new Error("Unknown agent: " + input.agentId);
    const job = this.resolveJobForAgent(agent.id, input.jobId);
    if (input.jobId && (!job || job.agentId !== agent.id)) {
      throw new Error("Job does not belong to the requested agent");
    }
    return this.progressSnapshot(agent, job, normalizeActivityLimit(input.activityLimit));
  }

  async follow(input: FollowInput, signal?: AbortSignal): Promise<FollowResult> {
    this.requireRunning();
    const agent = this.store.getAgent(input.agentId);
    if (!agent) throw new Error("Unknown agent: " + input.agentId);
    const job = this.resolveJobForAgent(agent.id, input.jobId);
    if (!job) throw new Error("No DeepSeek job exists for agent " + agent.id);
    if (input.jobId && job.agentId !== agent.id) throw new Error("Job does not belong to the requested agent");

    if (job.status === "needs_approval") {
      return this.followNeedsApproval(agent, job);
    }
    if (TERMINAL_JOB_STATUSES.has(job.status) || ["delivery_pending"].includes(job.status)) {
      return this.followResultForJob(agent, job);
    }
    if (!ACTIVE_JOB_STATUSES.has(job.status)) {
      throw new Error("Job " + job.id + " is not followable in state " + job.status);
    }

    const lifecycle = this.ensureFollowLifecycle(
      job,
      normalizeFollowMinutes(input.waitMinutes, 1, 60, this.config.followDefaultWaitMinutes),
      normalizeFollowMinutes(input.graceMinutes, 1, 10, this.config.followDefaultGraceMinutes),
    );
    const waiter = Symbol("follow-waiter");
    lifecycle.waiters.add(waiter);
    try {
      return await waitWithAbort(lifecycle.promise, signal);
    } finally {
      lifecycle.waiters.delete(waiter);
    }
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
    this.store.setApprovalDeadline(active.id, null);
    let remoteError: string | null = null;
    try {
      await this.clientOrThrow().abort(agent.opencodeSessionId);
    } catch (error) {
      remoteError = redactSecrets(String(error));
    }
    const localReason = reason ?? (remoteError ? "Abort requested; remote abort failed: " + remoteError : "Aborted by orchestrator");
    if (active.status !== "aborted") this.store.updateJobStatus(active.id, "aborted", localReason);
    if (agent.status !== "aborted" && agent.status !== "closed") this.store.updateAgentStatus(agent.id, "aborted", reason ?? null);
    this.recordActivity(agent, active, "abort", remoteError ? "Abort requested but OpenCode returned an error" : "Abort requested for the active DeepSeek task");
    await this.resolveFollow(active.id, {
      status: "aborted",
      error: localReason,
      workerAborted: true,
    });
    if (remoteError) throw new Error(remoteError);
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
    if (!job?.resultPath && job?.status === "timed_out" && this.client) {
      const agent = this.store.getAgent(job.agentId);
      if (agent) await this.captureTimedOutEvidence(agent, job);
      job = this.store.getJob(jobId);
    }
    if (!job?.resultPath) throw new Error("No persisted result is available for job " + jobId);
    return sanitizePersistedResult(JSON.parse(await readFile(job.resultPath, "utf8")), this.config.maxResultLength);
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
    if (["completed", "completed_partial", "timed_out"].includes(job.status)) this.store.updateJobStatus(job.id, "delivery_pending");
    const pending = this.store.getJob(job.id);
    if (!pending) throw new Error("Job disappeared: " + job.id);
    await this.deliverPersistedJob(pending);
  }

  private resolveJobForAgent(agentId: string, jobId?: string): JobRecord | null {
    if (jobId) return this.store.getJob(jobId);
    return this.store.listJobs().find((job) => job.agentId === agentId && ACTIVE_JOB_STATUSES.has(job.status))
      ?? this.store.listJobs().find((job) => job.agentId === agentId)
      ?? null;
  }

  private ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number): FollowLifecycle {
    const existing = this.followLifecycles.get(job.id);
    if (existing) return existing;
    const now = Date.now();
    const current = this.store.getJob(job.id) ?? job;
    const startedAt = parseTimestamp(current.followStartedAt) ?? now;
    const deadlineAt = parseTimestamp(current.followDeadlineAt) ?? startedAt + waitMinutes * 60_000;
    const effectiveGraceMinutes = current.followGraceMinutes ?? graceMinutes;
    const isGracefulFinalization = current.status === "finalizing" || current.gracefulFinalizeAttempted;
    const graceDeadlineAt = parseTimestamp(current.graceDeadlineAt)
      ?? (isGracefulFinalization ? deadlineAt + effectiveGraceMinutes * 60_000 : null);
    if (["dispatching", "running"].includes(current.status)) {
      this.store.updateJobStatus(job.id, "following");
      this.recordActivity(this.store.getAgent(job.agentId), this.store.getJob(job.id), "event", "Follow mode started; waiting for an OpenCode completion event");
    }
    const after = this.store.setFollowWindow(job.id, {
      startedAt: new Date(startedAt).toISOString(),
      deadlineAt: new Date(deadlineAt).toISOString(),
      graceMinutes: effectiveGraceMinutes,
      graceDeadlineAt: graceDeadlineAt === null ? null : new Date(graceDeadlineAt).toISOString(),
      gracefulFinalizeAttempted: current.gracefulFinalizeAttempted,
    });
    let resolve!: (result: FollowResult) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<FollowResult>((resolvePromise, rejectPromise) => {
      resolve = resolvePromise;
      reject = rejectPromise;
    });
    const lifecycle: FollowLifecycle = {
      jobId: job.id,
      waitMinutes,
      graceMinutes: effectiveGraceMinutes,
      promise,
      resolve,
      reject,
      deadlineTimer: null,
      graceTimer: null,
      waiters: new Set(),
      settled: false,
    };
    this.followLifecycles.set(job.id, lifecycle);
    if (after.status === "finalizing" || after.gracefulFinalizeAttempted) {
      this.scheduleGraceTimer(lifecycle, parseTimestamp(after.graceDeadlineAt) ?? deadlineAt + effectiveGraceMinutes * 60_000);
    } else {
      this.scheduleDeadlineTimer(lifecycle, deadlineAt);
    }
    return lifecycle;
  }

  private scheduleDeadlineTimer(lifecycle: FollowLifecycle, deadlineAt: number): void {
    const delayMs = Math.max(0, deadlineAt - Date.now());
    lifecycle.deadlineTimer = setTimeout(() => {
      void this.enterGracefulFinalize(lifecycle.jobId);
    }, delayMs);
    lifecycle.deadlineTimer.unref?.();
  }

  private scheduleGraceTimer(lifecycle: FollowLifecycle, graceDeadlineAt: number): void {
    const delayMs = Math.max(0, graceDeadlineAt - Date.now());
    lifecycle.graceTimer = setTimeout(() => {
      void this.timeoutFollow(lifecycle.jobId);
    }, delayMs);
    lifecycle.graceTimer.unref?.();
  }

  private async enterGracefulFinalize(jobId: string): Promise<void> {
    const lifecycle = this.followLifecycles.get(jobId);
    const job = this.store.getJob(jobId);
    if (!lifecycle || !job || job.status !== "following" || job.gracefulFinalizeAttempted) return;
    const graceDeadlineAt = Date.now() + lifecycle.graceMinutes * 60_000;
    this.store.updateJobStatus(jobId, "finalizing");
    const marked = this.store.markGracefulFinalize(jobId, new Date(graceDeadlineAt).toISOString());
    const agent = this.store.getAgent(marked.agentId);
    if (!agent) return;
    this.recordActivity(agent, marked, "deadline", "Follow deadline reached; starting graceful finalization");
    this.scheduleGraceTimer(lifecycle, graceDeadlineAt);
    await this.requestGracefulFinalize(agent, marked);
  }

  private async requestGracefulFinalize(agent: AgentRecord, job: JobRecord): Promise<void> {
    const client = this.clientOrThrow();
    try {
      await client.promptAsync(agent.opencodeSessionId, GRACEFUL_FINALIZE_PROMPT, {
        providerId: this.config.opencodeProviderId,
        modelId: this.config.opencodeModelId,
        ...(this.config.opencodeVariant ? { variant: this.config.opencodeVariant } : {}),
        ...(this.config.opencodeAgent ? { agent: this.config.opencodeAgent } : {}),
      });
      this.recordActivity(agent, job, "finalize", "Graceful finalization prompt submitted in the same OpenCode session");
    } catch (error) {
      if (!isBusyError(error)) {
        this.recordActivity(agent, job, "error", "Graceful finalization prompt was rejected by OpenCode");
        return;
      }
      this.recordActivity(agent, job, "finalize", "OpenCode was busy; aborting the active turn before finalization");
      try {
        await client.abort(agent.opencodeSessionId);
      } catch {
        this.recordActivity(agent, job, "error", "OpenCode abort failed during graceful finalization");
      }
      try {
        await client.promptAsync(agent.opencodeSessionId, GRACEFUL_FINALIZE_PROMPT, {
          providerId: this.config.opencodeProviderId,
          modelId: this.config.opencodeModelId,
          ...(this.config.opencodeVariant ? { variant: this.config.opencodeVariant } : {}),
          ...(this.config.opencodeAgent ? { agent: this.config.opencodeAgent } : {}),
        });
        this.recordActivity(agent, job, "finalize", "Graceful finalization prompt resubmitted in the same OpenCode session");
      } catch {
        this.recordActivity(agent, job, "error", "Graceful finalization could not be submitted after the busy turn was aborted");
      }
    }
  }

  private async timeoutFollow(jobId: string): Promise<void> {
    const lifecycle = this.followLifecycles.get(jobId);
    const job = this.store.getJob(jobId);
    const agent = job ? this.store.getAgent(job.agentId) : null;
    if (!lifecycle || !job || !agent || job.status !== "finalizing") return;
    const reason = "Follow deadline and graceful-finalize grace period expired";
    this.store.updateJobStatus(job.id, "timed_out", reason);
    if (agent.status === "working") this.store.updateAgentStatus(agent.id, "timed_out", reason);
    this.recordActivity(agent, this.store.getJob(job.id), "deadline", "Grace period expired; the worker will be aborted and partial evidence captured");
    let abortError: string | null = null;
    try {
      await this.clientOrThrow().abort(agent.opencodeSessionId);
    } catch (error) {
      abortError = redactSecrets(String(error));
      this.recordActivity(agent, this.store.getJob(job.id), "error", "Worker abort failed after the follow grace period");
    }
    const timedOut = this.store.getJob(job.id) ?? job;
    const stored = await this.captureTimedOutEvidence(agent, timedOut);
    const envelope = stored?.envelope ?? null;
    await this.resolveFollow(job.id, {
      status: "timed_out",
      deadlineReached: true,
      gracefulFinalize: true,
      partial: true,
      workerAborted: true,
      resultAvailable: envelope !== null,
      error: abortError ? reason + "; abort error: " + abortError : reason,
      ...(envelope ? { envelope } : {}),
    });
    const pending = this.store.getJob(job.id);
    if (pending?.resultPath && pending.status === "timed_out") {
      this.store.updateJobStatus(job.id, "delivery_pending");
      void this.deliverPersistedJob(this.store.getJob(job.id) ?? pending).catch((error: unknown) => {
        this.lastStreamError = redactSecrets(String(error));
      });
    }
  }

  private async captureTimedOutEvidence(agent: AgentRecord, job: JobRecord): Promise<Awaited<ReturnType<typeof persistResult>> | null> {
    try {
      const messages = await this.clientOrThrow().listMessages(agent.opencodeSessionId);
      const diff = await this.clientOrThrow().getDiff(agent.opencodeSessionId);
      const stored = await persistResult(this.config.dataDir, agent, job, messages, diff, this.config.maxResultLength, {
        statusOverride: "timed_out",
        deadlineReached: true,
        gracefulFinalize: true,
        partial: true,
        workerAborted: true,
      });
      this.store.setJobMessages(job.id, stored.parsed.userMessageId, stored.parsed.assistantMessageId);
      this.store.setJobResult(job.id, stored.resultPath, stored.envelope.summary);
      this.recordActivity(agent, this.store.getJob(job.id), "result", "Persisted the last available messages and diff as partial timeout evidence");
      return stored;
    } catch {
      this.recordActivity(agent, this.store.getJob(job.id), "error", "Partial timeout evidence could not be fully captured");
      return null;
    }
  }

  private async followResultForJob(agent: AgentRecord, job: JobRecord): Promise<FollowResult> {
    let envelope: ResultEnvelope | null = null;
    if (job.resultPath) {
      try {
        const persisted = JSON.parse(await readFile(job.resultPath, "utf8")) as { envelope?: ResultEnvelope };
        envelope = persisted.envelope ?? null;
      } catch {
        envelope = null;
      }
    }
    return this.followResultForState(agent, job, {
      ...(envelope ? { envelope } : {}),
      status: envelope?.status ?? mapJobToFollowStatus(job.status),
      resultAvailable: envelope !== null,
      deadlineReached: Boolean(job.gracefulFinalizeAttempted || envelope?.deadlineReached || envelope?.status === "timed_out"),
      gracefulFinalize: Boolean(job.gracefulFinalizeAttempted || envelope?.gracefulFinalize),
      partial: Boolean(envelope?.partial || envelope?.status === "completed_partial" || envelope?.status === "timed_out"),
      workerAborted: Boolean(envelope?.workerAborted || envelope?.status === "timed_out"),
    });
  }

  private followNeedsApproval(agent: AgentRecord, job: JobRecord): Promise<FollowResult> {
    return this.followResultForState(agent, job, {
      status: "needs_approval",
      resultAvailable: false,
      message: "DeepSeek requires explicit approval before continuing.",
      permissionId: job.permissionId,
    });
  }

  private async followResultForState(
    agent: AgentRecord,
    job: JobRecord,
    overrides: {
      status?: FollowResult["status"];
      deadlineReached?: boolean;
      gracefulFinalize?: boolean;
      partial?: boolean;
      workerAborted?: boolean;
      resultAvailable?: boolean;
      envelope?: ResultEnvelope;
      error?: string;
      permissionId?: string | null;
      message?: string;
    } = {},
  ): Promise<FollowResult> {
    const envelope = overrides.envelope;
    const status = overrides.status ?? envelope?.status ?? mapJobToFollowStatus(job.status);
    const progress = await this.progressSnapshot(agent, job, 10);
    const failure = overrides.error ?? job.error;
    return {
      agentId: agent.id,
      jobId: job.id,
      status,
      deadlineReached: overrides.deadlineReached ?? Boolean(job.gracefulFinalizeAttempted || envelope?.deadlineReached),
      gracefulFinalize: overrides.gracefulFinalize ?? Boolean(job.gracefulFinalizeAttempted || envelope?.gracefulFinalize),
      partial: overrides.partial ?? Boolean(envelope?.partial || status === "completed_partial" || status === "timed_out"),
      workerAborted: overrides.workerAborted ?? Boolean(envelope?.workerAborted || status === "timed_out"),
      resultAvailable: overrides.resultAvailable ?? Boolean(job.resultPath || envelope),
      ...(envelope ? { result: { envelope } } : {}),
      progress,
      ...(failure ? { error: failure } : {}),
      ...(status === "needs_approval" ? { permissionId: overrides.permissionId ?? job.permissionId } : {}),
      ...(overrides.message ? { message: overrides.message } : {}),
    };
  }

  private async resolveFollow(jobId: string, overrides: {
    status?: FollowResult["status"];
    deadlineReached?: boolean;
    gracefulFinalize?: boolean;
    partial?: boolean;
    workerAborted?: boolean;
    resultAvailable?: boolean;
    envelope?: ResultEnvelope;
    error?: string;
    permissionId?: string | null;
    message?: string;
  } = {}): Promise<void> {
    const lifecycle = this.followLifecycles.get(jobId);
    if (!lifecycle || lifecycle.settled) return;
    const job = this.store.getJob(jobId);
    const agent = job ? this.store.getAgent(job.agentId) : null;
    if (!job || !agent) {
      lifecycle.settled = true;
      this.followLifecycles.delete(jobId);
      lifecycle.reject(new Error("Follow target disappeared: " + jobId));
      return;
    }
    const result = await this.followResultForState(agent, job, overrides);
    lifecycle.settled = true;
    if (lifecycle.deadlineTimer) clearTimeout(lifecycle.deadlineTimer);
    if (lifecycle.graceTimer) clearTimeout(lifecycle.graceTimer);
    this.followLifecycles.delete(jobId);
    lifecycle.resolve(result);
  }

  private async progressSnapshot(agent: AgentRecord, job: JobRecord | null, limit: number): Promise<ProgressSnapshot> {
    const activities = this.store.listActivity(agent.id, limit);
    let envelope: ResultEnvelope | null = null;
    if (job?.resultPath) {
      try {
        const persisted = JSON.parse(await readFile(job.resultPath, "utf8")) as { envelope?: ResultEnvelope };
        envelope = persisted.envelope ?? null;
      } catch {
        envelope = null;
      }
    }
    const start = parseTimestamp(job?.startedAt) ?? parseTimestamp(agent.createdAt) ?? Date.now();
    const end = parseTimestamp(job?.completedAt) ?? Date.now();
    const latest = activities[0];
    return {
      agentId: agent.id,
      jobId: job?.id ?? null,
      topic: agent.topic,
      status: job?.status ?? agent.status,
      elapsedSeconds: Math.max(0, Math.floor((end - start) / 1_000)),
      lastActivityAgoSeconds: latest ? Math.max(0, Math.floor((Date.now() - (parseTimestamp(latest.createdAt) ?? Date.now())) / 1_000)) : null,
      currentActivity: latest?.summary ?? "No observable activity recorded.",
      recentActivity: activities.map((activity): ProgressActivity => ({
        type: activity.activityType,
        summary: activity.summary,
        timestamp: activity.createdAt,
      })),
      filesTouched: envelope?.files ?? [],
      testSummary: envelope?.tests?.join("; ") || "No test result observed yet.",
      resultAvailable: Boolean(job?.resultPath),
    };
  }

  private recordActivity(agent: AgentRecord | null, job: JobRecord | null, activityType: Parameters<BridgeStore["recordActivity"]>[0]["activityType"], summary: string): void {
    if (!agent) return;
    try {
      this.store.recordActivity({
        agentId: agent.id,
        jobId: job?.id ?? null,
        sessionId: agent.opencodeSessionId,
        activityType,
        summary,
      });
    } catch (error) {
      if (this.running) this.lastStreamError = redactSecrets(String(error));
    }
  }

  private async dispatch(agent: AgentRecord, job: JobRecord, prompt: string): Promise<AcceptedOperation> {
    this.store.updateAgentStatus(agent.id, "working");
    this.store.updateJobStatus(job.id, "dispatching");
    if (job.kind === "continue" && !job.lastAssistantMessageId) {
      const baselineAssistantMessageId = this.previousAssistantMessageId(agent.id, job.id);
      if (baselineAssistantMessageId) this.store.setJobMessages(job.id, null, baselineAssistantMessageId);
    }
    try {
      await this.clientOrThrow().promptAsync(jobAgentSession(agent), prompt, {
        providerId: this.config.opencodeProviderId,
        modelId: this.config.opencodeModelId,
        ...(this.config.opencodeVariant ? { variant: this.config.opencodeVariant } : {}),
        ...(this.config.opencodeAgent ? { agent: this.config.opencodeAgent } : {}),
      });
      const current = this.store.getJob(job.id);
      if (current?.status === "dispatching") this.store.updateJobStatus(job.id, "running");
      this.recordActivity(agent, job, "dispatch", "Dispatched task to the OpenCode session");
      return this.accepted(this.store.getJob(job.id) ?? job);
    } catch (error) {
      const message = redactSecrets(String(error));
      const current = this.store.getJob(job.id);
      const preservesApproval = current?.status === "needs_approval";
      if (current && current.status !== "failed" && !preservesApproval) this.store.updateJobStatus(job.id, "failed", message);
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status !== "closed" && !preservesApproval) this.store.updateAgentStatus(agent.id, "failed", message);
      this.recordActivity(agent, job, "error", "OpenCode rejected the task dispatch");
      if (this.followLifecycles.has(job.id)) {
        await this.resolveFollow(job.id, { status: "failed", error: message });
      }
      throw new Error(message);
    }
  }

  private async resumeApproval(agent: AgentRecord, job: JobRecord, prompt: string): Promise<AcceptedOperation> {
    this.clearApprovalTimer(agent.id);
    this.store.setApprovalDeadline(job.id, null);
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
      if (current?.status !== "needs_approval") {
        this.clearApprovalTimer(agent.id);
        this.store.setApprovalDeadline(job.id, null);
        if (current && ["dispatching", "running", "following", "finalizing"].includes(current.status)) {
          this.store.updateJobStatus(job.id, "failed", message);
        }
      }
      const currentAgent = this.store.getAgent(agent.id);
      if (current?.status !== "needs_approval" && currentAgent && currentAgent.status === "working") {
        this.store.updateAgentStatus(agent.id, "failed", message);
      }
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
    const current = this.store.getJob(job.id);
    if (!current || current.status !== "needs_approval" || current.permissionId !== permissionId) {
      throw new Error("permissionId does not match the active approval request");
    }
    this.clearApprovalTimer(agent.id);
    this.store.setApprovalDeadline(job.id, null);
    this.store.updateJobStatus(job.id, "running");
    if (agent.status === "needs_approval") this.store.updateAgentStatus(agent.id, "working");
    try {
      await this.clientOrThrow().replyPermission(agent.opencodeSessionId, permissionId, reply, message);
      const afterReply = this.store.getJob(job.id);
      if (afterReply?.status === "running" && afterReply.permissionId === permissionId) {
        this.store.setJobPermission(job.id, null);
      }
      return this.accepted(this.store.getJob(job.id) ?? job);
    } catch (error) {
      const errorText = redactSecrets(String(error));
      const current = this.store.getJob(job.id);
      if (current?.status !== "needs_approval") {
        this.clearApprovalTimer(agent.id);
        this.store.setApprovalDeadline(job.id, null);
        if (current && current.status !== "failed") this.store.updateJobStatus(job.id, "failed", errorText);
      }
      const currentAgent = this.store.getAgent(agent.id);
      if (current?.status !== "needs_approval" && currentAgent && currentAgent.status !== "closed") {
        this.store.updateAgentStatus(agent.id, "failed", errorText);
      }
      throw new Error(errorText);
    }
  }

  private async handleEvent(event: OpenCodeEvent, retry: { sourceEventId?: string; attempt?: number } = {}): Promise<void> {
    const sessionId = findSessionId(event.properties);
    if (!sessionId) return;
    const agent = this.store.getAgentBySession(sessionId);
    if (!agent) return;
    const attempt = retry.attempt ?? 0;
    const observedJob = this.activeJob(agent.id);
    const eventJob = observedJob ?? this.store.listJobs().find((job) => job.agentId === agent.id) ?? null;
    const eventScope = eventJob?.id ?? "session";
    const sourceEventId = retry.sourceEventId ?? (event.id
      ? sessionId + ":" + event.id
      : createHash("sha256").update(sessionId + ":" + eventScope + ":" + event.type + ":" + JSON.stringify(event.properties)).digest("hex"));
    if (this.eventProcessing.has(sourceEventId)) return;
    this.eventProcessing.add(sourceEventId);
    const retryTimer = this.eventRetryTimers.get(sourceEventId);
    if (retryTimer) clearTimeout(retryTimer);
    this.eventRetryTimers.delete(sourceEventId);
    try {
      const inserted = this.store.insertEvent({
        source: "opencode",
        sourceEventId,
        eventType: event.type,
        sessionId,
        jobId: eventJob?.id ?? null,
      });
      if (!inserted && this.store.isEventProcessed("opencode", sourceEventId)) return;
      this.recordActivity(agent, observedJob, activityTypeForEvent(event), observableEventSummary(event));
      const status = findStatus(event.properties);
      if (event.type === "session.error" || event.type.includes(".error")) {
        await this.failActive(agent, redactSecrets(JSON.stringify(event.properties)));
      } else if (isApprovalRequestEvent(event.type, event.properties)) {
        await this.markNeedsApproval(agent, event.properties);
      } else if (event.type === "session.idle" || status === "idle") {
        await this.completeActive(agent);
      }
      this.store.markEventProcessed("opencode", sourceEventId);
    } catch (error) {
      this.lastStreamError = redactSecrets(String(error));
      if (this.running && attempt < 3) this.scheduleEventRetry(event, sourceEventId, attempt + 1);
    } finally {
      this.eventProcessing.delete(sourceEventId);
    }
  }

  private scheduleEventRetry(event: OpenCodeEvent, sourceEventId: string, attempt: number): void {
    const previous = this.eventRetryTimers.get(sourceEventId);
    if (previous) clearTimeout(previous);
    const delayMs = attempt === 1 ? 100 : attempt === 2 ? 500 : 2_000;
    const timer = setTimeout(() => {
      this.eventRetryTimers.delete(sourceEventId);
      void this.handleEvent(event, { sourceEventId, attempt }).catch((error) => {
        this.lastStreamError = redactSecrets(String(error));
      });
    }, delayMs);
    timer.unref?.();
    this.eventRetryTimers.set(sourceEventId, timer);
  }

  private async completeActive(agent: AgentRecord): Promise<void> {
    const job = this.activeJob(agent.id);
    if (!job || job.status === "needs_approval") return;
    const client = this.clientOrThrow();
    const messages = await client.listMessages(agent.opencodeSessionId);
    const diff = await client.getDiff(agent.opencodeSessionId);
    const currentAgent = this.store.getAgent(agent.id) ?? agent;
    const currentJob = this.store.getJob(job.id) ?? job;
    const latestAssistantId = latestAssistantMessageId(messages);
    if (currentJob.lastAssistantMessageId && (!latestAssistantId || latestAssistantId === currentJob.lastAssistantMessageId)) return;
    const partial = currentJob.status === "finalizing" || currentJob.gracefulFinalizeAttempted;
    if (currentJob.status === "dispatching") this.store.updateJobStatus(job.id, "running");
    const stored = await persistResult(this.config.dataDir, currentAgent, currentJob, messages, diff, this.config.maxResultLength, {
      ...(partial ? {
        statusOverride: "completed_partial",
        deadlineReached: true,
        gracefulFinalize: true,
        partial: true,
      } : {}),
    });
    this.store.setJobMessages(job.id, stored.parsed.userMessageId, stored.parsed.assistantMessageId);
    this.store.setJobResult(job.id, stored.resultPath, stored.envelope.summary);
    const completedJob = this.store.getJob(job.id) ?? job;
    if (["running", "following", "finalizing"].includes(completedJob.status)) {
      this.store.updateJobStatus(job.id, partial ? "completed_partial" : "completed");
    }
    const completedAgent = this.store.getAgent(agent.id) ?? agent;
    if (completedAgent.status === "working") this.store.updateAgentStatus(agent.id, partial ? "completed_partial" : "completed");
    this.recordActivity(agent, this.store.getJob(job.id), "result", partial ? "Graceful finalization produced a partial result" : "OpenCode session became idle and the result was persisted");
    if (this.followLifecycles.has(job.id)) {
      await this.resolveFollow(job.id, {
        status: partial ? "completed_partial" : "completed",
        deadlineReached: partial,
        gracefulFinalize: partial,
        partial,
        resultAvailable: true,
        envelope: stored.envelope,
      });
    }
    const deliveryJob = this.store.getJob(job.id) ?? job;
    if (["completed", "completed_partial"].includes(deliveryJob.status)) this.store.updateJobStatus(job.id, "delivery_pending");
    await this.deliverEnvelope(stored.envelope, this.store.getJob(job.id) ?? job);
  }

  private async deliverPersistedJob(job: JobRecord): Promise<void> {
    if (!job.resultPath) return;
    const parsed = JSON.parse(await readFile(job.resultPath, "utf8")) as { envelope?: unknown };
    const envelope = sanitizePersistedEnvelope(parsed.envelope);
    if (!envelope) throw new Error("Persisted result envelope is invalid or unsafe");
    await this.deliverEnvelope(envelope, job);
  }

  private async deliverEnvelope(envelope: ResultEnvelope, job: JobRecord): Promise<void> {
    await this.withDeliveryLock("job:" + job.id, async () => {
      const initialBinding = this.store.getBinding(job.id);
      const deliverWithCurrentBinding = async (): Promise<void> => {
        const binding = this.store.getBinding(job.id);
        if (binding) {
          await this.withDeliveryLock("thread:" + binding.threadId, () => this.deliverEnvelopeNow(envelope, job));
        } else {
          await this.deliverEnvelopeNow(envelope, job);
        }
      };
      if (initialBinding) {
        await this.deliveryAdmission.withRead(deliverWithCurrentBinding);
      } else {
        // An unbound result could later correlate to any Codex thread. Keep
        // it exclusive so a late binding cannot overlap another delivery.
        await this.deliveryAdmission.withWrite(deliverWithCurrentBinding);
      }
    });
  }

  private async withDeliveryLock<T>(key: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.deliveryLocks.get(key) ?? Promise.resolve();
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const chain = previous.then(() => gate);
    this.deliveryLocks.set(key, chain);
    await previous;
    try {
      return await operation();
    } finally {
      release?.();
      if (this.deliveryLocks.get(key) === chain) this.deliveryLocks.delete(key);
    }
  }

  private async withAgentOperationLock<T>(key: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.agentOperationLocks.get(key) ?? Promise.resolve();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const chain = previous.then(() => gate);
    this.agentOperationLocks.set(key, chain);
    await previous;
    try {
      return await operation();
    } finally {
      release();
      if (this.agentOperationLocks.get(key) === chain) this.agentOperationLocks.delete(key);
    }
  }

  private async withRequestIdLock<T>(key: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.requestLocks.get(key) ?? Promise.resolve();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const chain = previous.then(() => gate);
    this.requestLocks.set(key, chain);
    await previous;
    try {
      return await operation();
    } finally {
      release();
      if (this.requestLocks.get(key) === chain) this.requestLocks.delete(key);
    }
  }

  private async deliverEnvelopeNow(envelope: ResultEnvelope, job: JobRecord): Promise<void> {
    const binding = this.store.getBinding(job.id);
    if (this.codex.available && !binding && !this.inboxFallbackJobs.has(job.id)) {
      this.scheduleInboxFallback(job.id);
      return;
    }
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
    const latestAssistantId = latestAssistantMessageId(messages);
    if (job.lastAssistantMessageId && (!latestAssistantId || latestAssistantId === job.lastAssistantMessageId)) return;
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
      } else if (["following", "finalizing"].includes(job.status)) {
        this.ensureFollowLifecycle(
          job,
          normalizeFollowMinutes(undefined, 1, 60, this.config.followDefaultWaitMinutes),
          normalizeFollowMinutes(undefined, 1, 10, this.config.followDefaultGraceMinutes),
        );
      } else if (["completed", "completed_partial", "timed_out"].includes(job.status) && job.resultPath) {
        this.store.updateJobStatus(job.id, "delivery_pending");
        const pending = this.store.getJob(job.id);
        if (pending) await this.deliverPersistedJob(pending).catch((error) => {
          this.lastStreamError = redactSecrets(String(error));
        });
      } else if (job.status === "timed_out" && !job.resultPath) {
        const agent = this.store.getAgent(job.agentId);
        if (agent) {
          const stored = await this.captureTimedOutEvidence(agent, job);
          const current = this.store.getJob(job.id);
          if (stored && current?.status === "timed_out" && current.resultPath) {
            this.store.updateJobStatus(job.id, "delivery_pending");
            const pending = this.store.getJob(job.id);
            if (pending) await this.deliverPersistedJob(pending).catch((error) => {
              this.lastStreamError = redactSecrets(String(error));
            });
          }
        }
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
    const fallbackTimer = this.correlationFallbackTimers.get(job.id);
    if (fallbackTimer) clearTimeout(fallbackTimer);
    this.correlationFallbackTimers.delete(job.id);
    this.inboxFallbackJobs.delete(job.id);
    const delivery = this.store.getDeliveryByJob(job.id);
    if (delivery?.status === "delivered") return;
    if (["completed", "completed_partial", "timed_out"].includes(job.status)) {
      this.store.updateJobStatus(job.id, "delivery_pending");
    }
    const refreshed = this.store.getJob(job.id);
    if (refreshed?.status === "delivery_pending") await this.deliverPersistedJob(refreshed);
  }

  private scheduleInboxFallback(jobId: string): void {
    if (this.correlationFallbackTimers.has(jobId)) return;
    const timer = setTimeout(() => {
      this.correlationFallbackTimers.delete(jobId);
      this.inboxFallbackJobs.add(jobId);
      const job = this.store.getJob(jobId);
      if (job) {
        void this.deliverPersistedJob(job)
          .catch((error: unknown) => {
            this.lastStreamError = redactSecrets(String(error));
          })
          .finally(() => {
            this.inboxFallbackJobs.delete(jobId);
          });
      }
    }, this.config.codexCorrelationWindowMs);
    timer.unref?.();
    this.correlationFallbackTimers.set(jobId, timer);
  }

  private async failActive(agent: AgentRecord, error: string): Promise<void> {
    const job = this.activeJob(agent.id);
    if (!job) return;
    this.store.setApprovalDeadline(job.id, null);
    if (job.status !== "failed") this.store.updateJobStatus(job.id, "failed", error);
    const currentAgent = this.store.getAgent(agent.id);
    if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, "failed", error);
    this.recordActivity(agent, job, "error", "OpenCode reported a terminal error");
    if (this.followLifecycles.has(job.id)) {
      await this.resolveFollow(job.id, { status: "failed", error });
    }
  }

  private async markNeedsApproval(agent: AgentRecord, properties: Record<string, unknown> = {}): Promise<void> {
    const job = this.activeJob(agent.id);
    if (!job) return;
    const currentAgent = this.store.getAgent(agent.id);
    const alreadyNeedsApproval = job.status === "needs_approval";
    const currentJob = this.store.getJob(job.id) ?? job;
    const requestedPermissionId = findPermissionId(properties);
    const permissionId = requestedPermissionId ?? currentJob.permissionId;
    const permissionChanged = requestedPermissionId !== null && requestedPermissionId !== currentJob.permissionId;
    const approvalDeadline = permissionChanged
      ? Date.now() + this.config.approvalTimeoutMs
      : parseTimestamp(currentJob.approvalDeadlineAt) ?? Date.now() + this.config.approvalTimeoutMs;
    this.store.setApprovalDeadline(job.id, new Date(approvalDeadline).toISOString());
    if (["dispatching", "running", "following", "finalizing"].includes(job.status)) this.store.updateJobStatus(job.id, "needs_approval");
    if (permissionId) this.store.setJobPermission(job.id, permissionId);
    if (currentAgent?.status === "working") this.store.updateAgentStatus(agent.id, "needs_approval");
    const approvalNoticeExists = await this.inbox.noticeExists(job.id, "needs_approval", permissionId);
    if (!approvalNoticeExists) {
      this.recordActivity(agent, this.store.getJob(job.id), "approval", "OpenCode requested explicit approval before continuing");
    }
    if (this.followLifecycles.has(job.id) && !alreadyNeedsApproval) {
      await this.resolveFollow(job.id, {
        status: "needs_approval",
        permissionId,
        message: "DeepSeek requires explicit approval before continuing.",
      });
    }
    this.store.clearFollowWindow(job.id);
    if (!approvalNoticeExists) {
      await this.inbox.writeNotice({
        kind: "needs_approval",
        agentId: agent.id,
        jobId: job.id,
        topic: agent.topic,
        message: "OpenCode requested approval. Review the task and use deepseek_continue for an explicit response.",
        permissionId,
      });
    }
    this.scheduleApprovalTimer(agent.id, job.id, approvalDeadline);
  }

  private clearApprovalTimer(agentId: string): void {
    const timer = this.approvalTimers.get(agentId);
    if (timer) clearTimeout(timer);
    this.approvalTimers.delete(agentId);
  }

  private scheduleApprovalTimer(agentId: string, jobId: string, deadlineAt: number): void {
    this.clearApprovalTimer(agentId);
    const timer = setTimeout(() => {
      void this.expireApproval(agentId, jobId);
    }, Math.max(0, deadlineAt - Date.now()));
    timer.unref?.();
    this.approvalTimers.set(agentId, timer);
  }

  private async expireApproval(agentId: string, jobId: string): Promise<void> {
    this.approvalTimers.delete(agentId);
    const job = this.store.getJob(jobId);
    if (!job || job.status !== "needs_approval") return;
    const expiringPermissionId = job.permissionId;
    const deadlineAt = parseTimestamp(job.approvalDeadlineAt);
    if (deadlineAt !== null && deadlineAt > Date.now()) {
      this.scheduleApprovalTimer(agentId, jobId, deadlineAt);
      return;
    }
    const agentBeforeAbort = this.store.getAgent(agentId);
    let abortError: string | null = null;
    if (agentBeforeAbort) {
      try {
        await this.clientOrThrow().abort(agentBeforeAbort.opencodeSessionId);
      } catch (error) {
        abortError = redactSecrets(String(error));
      }
    }
    const current = this.store.getJob(jobId);
    if (!current || current.status !== "needs_approval") return;
    if (current.permissionId !== expiringPermissionId) {
      const persistedDeadlineAt = parseTimestamp(current.approvalDeadlineAt);
      const effectiveDeadlineAt = persistedDeadlineAt !== null && persistedDeadlineAt > Date.now()
        ? persistedDeadlineAt
        : Date.now() + this.config.approvalTimeoutMs;
      if (persistedDeadlineAt === null || persistedDeadlineAt <= Date.now()) {
        this.store.setApprovalDeadline(current.id, new Date(effectiveDeadlineAt).toISOString());
      }
      this.scheduleApprovalTimer(agentId, jobId, effectiveDeadlineAt);
      return;
    }
    const currentDeadlineAt = parseTimestamp(current.approvalDeadlineAt);
    if (currentDeadlineAt !== null && currentDeadlineAt > Date.now()) {
      this.scheduleApprovalTimer(agentId, jobId, currentDeadlineAt);
      return;
    }
    this.store.setApprovalDeadline(current.id, null);
    const failure = abortError ? "Approval timeout expired; remote abort failed: " + abortError : "Approval timeout expired";
    this.store.updateJobStatus(current.id, "failed", failure);
    const agent = this.store.getAgent(agentId);
    if (agent && agent.status === "needs_approval") this.store.updateAgentStatus(agent.id, "failed", failure);
    if (agent) this.recordActivity(agent, current, "error", abortError ? "Approval expired and OpenCode abort failed" : "Approval expired and the active worker was aborted");
    await this.inbox.writeNotice({
      kind: "approval_timeout",
      agentId,
      jobId,
      topic: agent?.topic ?? "DeepSeek task",
      message: "The approval window expired. Start an explicit continuation if the work is still needed.",
      permissionId: current.permissionId,
    });
  }

  private activeJob(agentId: string): JobRecord | null {
    return this.store.listJobs().find((job) => job.agentId === agentId && ACTIVE_JOB_STATUSES.has(job.status)) ?? null;
  }

  private previousAssistantMessageId(agentId: string, currentJobId: string): string | null {
    return this.store.listJobs()
      .find((job) => job.agentId === agentId && job.id !== currentJobId && typeof job.lastAssistantMessageId === "string")
      ?.lastAssistantMessageId ?? null;
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

function latestAssistantMessageId(messages: OpenCodeMessage[]): string | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message?.info?.role !== "assistant") continue;
    return typeof message.info.id === "string" ? message.info.id : null;
  }
  return null;
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

function normalizeActivityLimit(value: number | undefined): number {
  if (value === undefined) return 10;
  if (!Number.isInteger(value) || value < 1 || value > 20) {
    throw new Error("activityLimit must be an integer between 1 and 20");
  }
  return value;
}

function normalizeFollowMinutes(value: number | undefined, minimum: number, maximum: number, fallback: number): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized < minimum || normalized > maximum) {
    throw new Error("follow timeout values must be whole minutes within the configured range");
  }
  return normalized;
}

function parseTimestamp(value: string | null | undefined): number | null {
  if (!value) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function mapJobToFollowStatus(status: string): FollowResult["status"] {
  switch (status) {
    case "completed":
    case "completed_partial":
    case "timed_out":
    case "failed":
    case "aborted":
    case "needs_approval":
      return status;
    case "delivery_pending":
    case "delivered":
      return "completed";
    default:
      return "failed";
  }
}

function waitWithAbort<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(new FollowCancelledError());
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      signal.removeEventListener("abort", onAbort);
      reject(new FollowCancelledError());
    };
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

function isBusyError(error: unknown): boolean {
  const status = error && typeof error === "object" && "status" in error
    ? (error as { status?: unknown }).status
    : undefined;
  if (status === 409) return true;
  const message = redactSecrets(String(error)).toLowerCase();
  return message.includes("busy") || message.includes("already running") || message.includes("active turn") || message.includes("conflict");
}

function activityTypeForEvent(event: OpenCodeEvent): Parameters<BridgeStore["recordActivity"]>[0]["activityType"] {
  if (isApprovalRequestEvent(event.type, event.properties)) return "approval";
  if (event.type.includes("error")) return "error";
  if (event.type === "session.idle") return "result";
  return "event";
}

function observableEventSummary(event: OpenCodeEvent): string {
  if (isApprovalRequestEvent(event.type, event.properties)) return "OpenCode emitted an approval request";
  if (event.type.includes("error")) return "OpenCode emitted an error event";
  if (event.type === "session.idle") return "OpenCode emitted session.idle";
  return "OpenCode emitted observable event " + truncate(event.type, 120);
}

function isApprovalRequestEvent(type: string, properties: Record<string, unknown>): boolean {
  const normalized = type.toLowerCase();
  if (!normalized.includes("permission") && !normalized.includes("approval")) return false;
  if (/(?:^|[._-])(replied|updated|resolved|responded|granted|denied|rejected|cancelled|closed)(?:$|[._-])/.test(normalized)) return false;
  return normalized === "permission.asked"
    || normalized === "permission.requested"
    || normalized === "approval.asked"
    || normalized === "approval.requested"
    || findPermissionId(properties) !== null;
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
