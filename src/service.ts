import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { canRead, defaultWorkspace, assertInside, isProcessAlive, isSamePath, newId, normalizeTitle, redactSecrets, shouldIncludeGlobalGeminiContext, truncate, validateContextFiles, validateContextFilesStrict } from "./security.js";
import { buildWorkerPrompt, GRACEFUL_FINALIZE_PROMPT, type PromptBuildOptions, type WorkerPromptInput } from "./prompts.js";
import { BridgeStore } from "./store.js";
import { InboxDelivery } from "./delivery/inbox.js";
import {
  CodexAppServerDeliveryAdapter,
  type CodexCorrelation,
  type CodexDeliveryAdapter,
  UnavailableCodexDeliveryAdapter,
} from "./codex/adapter.js";
import { DefaultCodexCliTransport, type CodexCliTransport } from "./codex/cli-resolver.js";
import { TranscriptAttestor } from "./codex/transcript-attestor.js";
import { OpenCodeManager, type ManagedOpenCode } from "./opencode/manager.js";
import { OpenCodeHttpError, OpenCodeTransportError } from "./opencode/client.js";
import { AntigravityAdapter, type AntigravityProviderLike } from "./antigravity/adapter.js";
import { AntigravityProcessError, AGY_DEFAULT_TIMEOUT_MS } from "./antigravity/runner.js";
import { AGY_MAX_PROMPT_LENGTH } from "./antigravity/args.js";
import { AntigravitySpool, isHeartbeatLive } from "./antigravity/spool.js";
import { FOLLOW_MAX_TOTAL_MINUTES } from "./config.js";

import type { AntigravityAttemptManifest, AntigravityHeartbeat, AntigravityRunResult, AntigravityStreamProgress } from "./antigravity/types.js";

import { assistantTextAfterBaseline, formatHumanResult, persistAntigravityResult, persistResult, sanitizePersistedEnvelope, sanitizePersistedResult } from "./result.js";
import { ConflictError, InvalidRequestError, NotFoundError, RouteOverrideDeniedError, UnknownAgentError, UnknownJobError } from "./errors.js";
import { evaluateRetentionPolicy, runRetentionPrune, type RetentionPolicyState } from "./retention.js";
import type {
  AcceptedBatchOperation,
  ActiveRouteSource,
  AgentActivity,
  AgentMode,
  AgentRecord,
  AuthoritativeLivenessStatus,
  BatchItemInput,
  BatchItemReceipt,
  BridgeConfig,
  CodexBinding,
  ConsultInput,
  ContinueInput,
  EarlyExitSignal,
  EscalationProposal,
  FollowInput,
  FollowResult,
  JobRecord,
  ModelRoute,
  OpenCodeClientLike,
  OpenCodeEvent,
  OpenCodeMessage,
  ParkBarrierRecord,
  ParkInput,
  ParkPredicateType,
  ParkReceipt,
  ProgressActivity,
  ProgressSnapshot,
  ResolvedRoute,
  ResultEnvelope,
  RouteStatusInfo,
  SemanticProgress,
  SpawnBatchInput,
  SpawnInput,
  SwarmOperationalCounters,
  WakeEnvelope,
  WakeOutboxRecord,
  WorkspaceStrategy,
} from "./types.js";

export const RETENTION_INTERVAL_MS = 60 * 60_000;

function workerPromptOptions(config: BridgeConfig, isAntigravity: boolean, allowedExternalFiles?: string[]): PromptBuildOptions {
  return {
    maxLength: config.maxTaskLength,
    ...(isAntigravity ? {
      contextFileDelivery: "reference" as const,
      maxPromptLength: AGY_MAX_PROMPT_LENGTH,
    } : {}),
    ...(allowedExternalFiles ? { allowedExternalFiles } : {}),
  };
}

export interface ServiceDependencies {
  store?: BridgeStore;
  manager?: OpenCodeManagerLike;
  codex?: CodexDeliveryAdapter;
  cliTransport?: CodexCliTransport;
  inbox?: InboxDelivery;
  antigravity?: AntigravityProviderLike;
  transcriptAttestor?: TranscriptAttestor;
  sessionsDir?: string;
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
  outcome?: "accepted" | "dispatch_unknown";
  priority?: number;
  exclusiveResources?: string[];
}

export class BridgeBusyError extends ConflictError {
  override readonly code = "busy" as const;

  constructor(readonly jobId: string) {
    super("Agent is busy with job " + jobId + ". Do not retry in a loop; wait for its asynchronous result or call deepseek_abort.", "busy");
    this.name = "BridgeBusyError";
  }
}

function isBackpressureError(error: unknown): boolean {
  if (error instanceof BridgeBusyError) return true;
  if (typeof error === "object" && error !== null) {
    const status = (error as any).status ?? (error as any).statusCode;
    if (status === 429 || status === 503) return true;
    const code = String((error as any).code ?? "").toLowerCase();
    if (code.includes("rate_limit") || code.includes("busy") || code.includes("overloaded") || code.includes("capacity")) return true;
    const msg = String((error as any).message ?? "").toLowerCase();
    if (msg.includes("429") || msg.includes("rate limit") || msg.includes("too many requests") || msg.includes("overloaded") || msg.includes("capacity") || msg.includes("busy")) return true;
  }
  return false;
}

export type DaemonLifecycleState = "starting" | "recovering" | "ready" | "degraded";

export interface BridgeServiceMetrics {
  sqliteProgressUpdates: number;
  singleflightRunsStarted: number;
  singleflightRunsSkipped: number;
  activeTimersCount: number;
  reconnectReconciledCount: number;
}

export interface ServiceStatus {
  state: DaemonLifecycleState;
  ready: boolean;
  running: boolean;
  opencodeUrl: string | null;
  provider: string;
  model: string;
  variant: string | null;
  experimentalSameChatDelivery: boolean;
  followDefaultWaitMinutes: number;
  followDefaultGraceMinutes: number;
  codexDelivery: { available: boolean; reason: string | null };
  correlation: { hints: number; bindings: number };
  retention: { mode: string; dbState: string; pruningEnabled: boolean };
  lastStreamError: string | null;
  activeRoute: ResolvedRoute | null;
  activeRouteSource: ActiveRouteSource;
  capabilities?: Record<string, boolean>;
  swarm?: SwarmOperationalCounters;
  workerMaxExecutionMinutes?: number | null;
  workerExecutionTimeout?: string;
  error?: string | null;
}

export interface QuiescenceProof {
  stopped: boolean;
  jobId?: string;
  supervisorPid?: number | null;
  agyPid?: number | null;
  workerPid?: number | null;
  pidsChecked: number[];
  alivePids: number[];
  verifiedAt: string;
  error?: string;
}

const ACTIVE_JOB_STATUSES = new Set(["queued", "dispatching", "running", "following", "finalizing", "needs_approval"]);
const TERMINAL_JOB_STATUSES = new Set(["completed", "completed_partial", "timed_out", "failed", "aborted", "delivered"]);

const DISPATCH_UNKNOWN_WARNING = "DeepSeek Sub-Agent accepted the task, but OpenCode dispatch acceptance is uncertain after a transport failure. Follow this job (deepseek_follow) or abort it (deepseek_abort) to settle the obligation.";

interface FollowLifecycle {
  jobId: string;
  waitMinutes: number;
  graceMinutes: number;
  autoArmed: boolean;
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
  private readonly antigravity: AntigravityProviderLike;
  private readonly cliTransport: CodexCliTransport;
  private readonly transcriptAttestor: TranscriptAttestor;
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
  private readonly repoPreparationLocks = new Map<string, Promise<void>>();
  private readonly activeExclusiveResources = new Map<string, string>();
  private drainingQueue = false;
  private drainPending = false;
  private readonly pendingDispatches = new Map<string, { prompt: string; workerInput?: WorkerPromptInput; contextFiles?: string[] }>();
  private readonly dispatchWaiters = new Map<string, { resolve: (op: AcceptedOperation) => void; reject: (err: unknown) => void }>();
  private readonly correlationFallbackTimers = new Map<string, NodeJS.Timeout>();
  private readonly inboxFallbackJobs = new Set<string>();
  private readonly approvalTimers = new Map<string, NodeJS.Timeout>();
  private readonly eventProcessing = new Set<string>();
  private readonly eventRetryTimers = new Map<string, NodeJS.Timeout>();
  private readonly followLifecycles = new Map<string, FollowLifecycle>();
  private readonly antigravityAbortControllers = new Map<string, AbortController>();
  private readonly antigravityTasks = new Set<Promise<void>>();
  private readonly antigravityTasksByJob = new Map<string, Promise<void>>();
  private readonly parkWaiters = new Map<string, {
    parkId: string;
    generation: number;
    jobIds: Set<string>;
    isAlias: boolean;
    resolve: (receipt: ParkReceipt) => void;
    reject: (err: unknown) => void;
    signal?: AbortSignal | undefined;
    cleanupSignal?: (() => void) | undefined;
  }>();
  private readonly wakeRetryTimers = new Map<string, NodeJS.Timeout>();
  private lastSseEventAt: number | null = null;
  private retentionTimer: NodeJS.Timeout | null = null;
  private advisorySchedulerTimer: NodeJS.Timeout | null = null;
  private readonly jobInactivityTimers = new Map<string, NodeJS.Timeout>();
  private advisoryPassRunning = false;
  private advisoryAbortController: AbortController | null = null;
  private readonly metrics: BridgeServiceMetrics = {
    sqliteProgressUpdates: 0,
    singleflightRunsStarted: 0,
    singleflightRunsSkipped: 0,
    activeTimersCount: 0,
    reconnectReconciledCount: 0,
  };
  private retentionState: RetentionPolicyState | null = null;
  private lifecycleState: DaemonLifecycleState = "starting";
  private startupError: string | null = null;
  private targetCredits: number;
  private readonly successfulJobIds = new Set<string>();

  getMetrics(): BridgeServiceMetrics {
    return {
      ...this.metrics,
      activeTimersCount: this.jobInactivityTimers.size,
    };
  }

  getActiveInactivityTimerCount(): number {
    return this.jobInactivityTimers.size;
  }

  clearJobInactivityTimer(jobId: string): void {
    const existing = this.jobInactivityTimers.get(jobId);
    if (existing) {
      clearTimeout(existing);
      this.jobInactivityTimers.delete(jobId);
    }
  }

  clearAllJobInactivityTimers(): void {
    for (const timer of this.jobInactivityTimers.values()) {
      clearTimeout(timer);
    }
    this.jobInactivityTimers.clear();
  }

  hasParkWaiter(parkId: string): boolean {
    return this.parkWaiters.has(parkId);
  }

  constructor(private readonly config: BridgeConfig, dependencies: ServiceDependencies = {}) {
    this.targetCredits = this.config.swarmCreditCeiling ?? 8;
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
    this.cliTransport = dependencies.cliTransport ?? new DefaultCodexCliTransport({ config });
    this.antigravity = dependencies.antigravity ?? new AntigravityAdapter({
      ...(config.antigravityCommand ? { command: config.antigravityCommand } : {}),
      sandbox: config.antigravitySandbox,
      addDirs: config.antigravityAddDirs,
      dangerouslySkipPermissions: config.antigravityAutoApprovePermissions,
      dataDir: config.dataDir,
      timeoutMs: this.effectiveWorkerTimeoutMs(),
    });
    this.transcriptAttestor = dependencies.transcriptAttestor ?? new TranscriptAttestor({ sessionsDir: dependencies.sessionsDir });
  }

  getTargetCredits(): number {
    return this.targetCredits;
  }

  recordBackpressure(reason?: string): void {
    const floor = 1;
    this.targetCredits = Math.max(floor, Math.floor(this.targetCredits / 2));
    if (reason) {
      this.lastStreamError = redactSecrets(`Swarm backpressure recorded (${reason}); targetCredits reduced to ${this.targetCredits}`);
    }
  }

  recordSuccess(): void {
    const ceiling = this.config.swarmCreditCeiling ?? 8;
    if (this.targetCredits < ceiling) {
      this.targetCredits += 1;
    }
  }

  private recordTerminalSuccess(jobId: string): void {
    if (!this.successfulJobIds.has(jobId)) {
      this.successfulJobIds.add(jobId);
      this.recordSuccess();
    }
  }

  isReady(): boolean {
    return this.lifecycleState === "ready";
  }

  getLifecycleState(): DaemonLifecycleState {
    return this.lifecycleState;
  }

  async start(): Promise<void> {
    if (this.running || this.lifecycleState === "ready") return;
    this.lifecycleState = "starting";
    this.startupError = null;
    try {
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
      this.streamAbort = new AbortController();
      this.streamTask = managed.client.subscribe(
        (event) => this.handleEvent(event),
        this.streamAbort.signal,
      ).catch((error: unknown) => {
        if (!this.streamAbort?.signal.aborted) this.lastStreamError = redactSecrets(String(error));
      });
      this.lifecycleState = "recovering";
      await this.recoverPendingJobs();
      this.lifecycleState = "ready";
      this.running = true;
      this.scheduleDrain();
      this.scheduleRetentionPolicy();
      this.startAdvisoryScheduler();
    } catch (error) {
      this.lifecycleState = "degraded";
      this.running = false;
      this.startupError = redactSecrets(String(error));
    }
  }

  async stop(options?: { abortWorkers?: boolean }): Promise<void> {
    const abortWorkers = options?.abortWorkers ?? true;
    this.running = false;
    this.lifecycleState = "starting";
    this.startupError = null;
    this.stopAdvisoryScheduler();
    if (this.retentionTimer) clearInterval(this.retentionTimer);
    this.retentionTimer = null;
    for (const timer of this.approvalTimers.values()) clearTimeout(timer);
    this.approvalTimers.clear();
    for (const timer of this.correlationFallbackTimers.values()) clearTimeout(timer);
    this.correlationFallbackTimers.clear();
    for (const timer of this.eventRetryTimers.values()) clearTimeout(timer);
    this.eventRetryTimers.clear();
    for (const timer of this.wakeRetryTimers.values()) clearTimeout(timer);
    this.wakeRetryTimers.clear();
    for (const waiter of this.parkWaiters.values()) {
      waiter.cleanupSignal?.();
      waiter.reject(new Error("Bridge daemon stopped while waiting for park wake"));
    }
    this.parkWaiters.clear();
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
    if (abortWorkers) {
      for (const controller of this.antigravityAbortControllers.values()) controller.abort();
      this.antigravityAbortControllers.clear();
      if (this.antigravityTasks.size > 0) {
        await Promise.race([
          Promise.allSettled([...this.antigravityTasks]),
          new Promise<void>((resolve) => setTimeout(resolve, 2_000)),
        ]);
        this.antigravityTasks.clear();
        this.antigravityTasksByJob.clear();
      }
    } else {
      this.antigravityAbortControllers.clear();
      this.antigravityTasks.clear();
      this.antigravityTasksByJob.clear();
    }
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
    for (const waiter of this.dispatchWaiters.values()) {
      waiter.reject(new Error("Bridge daemon stopped"));
    }
    this.dispatchWaiters.clear();
    this.client = null;
    this.managed = null;
  }

  status(): ServiceStatus {
    const defaultRoute = this.config.modelRoutes.find((route) => route.name === this.config.defaultModelRoute);
    const active = this.safeActiveRoute();
    return {
      state: this.lifecycleState,
      ready: this.lifecycleState === "ready",
      running: this.lifecycleState === "ready",
      opencodeUrl: this.managed?.baseUrl ?? null,
      provider: defaultRoute?.providerId ?? this.config.opencodeProviderId,
      model: defaultRoute?.modelId ?? this.config.opencodeModelId,
      variant: defaultRoute?.variant ?? this.config.opencodeVariant,
      experimentalSameChatDelivery: this.config.experimentalSameChatDelivery,
      followDefaultWaitMinutes: this.config.followDefaultWaitMinutes,
      followDefaultGraceMinutes: this.config.followDefaultGraceMinutes,
      codexDelivery: { available: this.codex.available, reason: this.codex.reason },
      correlation: {
        hints: this.store.countJobsWithCorrelationHints(),
        bindings: this.store.countCodexBindings(),
      },
      retention: this.retentionState ?? { mode: this.config.retentionMode, dbState: "legacy", pruningEnabled: false },
      lastStreamError: this.lastStreamError,
      activeRoute: active,
      activeRouteSource: this.effectiveRouteSource(),
      capabilities: {
        batch_scheduler: true,
      },
      swarm: {
        queueDepth: this.store.getQueueDepth(),
        active: this.store.getActiveJobCount(),
        targetCredits: this.targetCredits,
        oldestWaitMs: this.store.getOldestWaitMs(),
        resourceClaims: this.getActiveResourceClaimsList(),
        capabilities: {
          batch_scheduler: true,
        },
      },
      workerMaxExecutionMinutes: this.config.workerMaxExecutionMinutes ?? null,
      workerExecutionTimeout: this.config.workerMaxExecutionMinutes ? `${this.config.workerMaxExecutionMinutes}m` : "unlimited",
      ...(this.startupError ? { error: this.startupError } : {}),
    };
  }

  private scheduleRetentionPolicy(): void {
    const policy = evaluateRetentionPolicy(this.store, this.config.retentionMode);
    this.retentionState = policy;
    if (!policy.pruningEnabled) return;
    const runPass = () => {
      try {
        runRetentionPrune(this.store, {});
      } catch (error) {
        this.lastStreamError = redactSecrets(String(error));
      }
    };
    this.retentionTimer = setInterval(runPass, RETENTION_INTERVAL_MS);
    this.retentionTimer.unref?.();
  }

  private startAdvisoryScheduler(): void {
    if (this.advisorySchedulerTimer) return;
    this.advisoryAbortController = new AbortController();
    const intervalMs = this.config.advisoryCheckIntervalMs ?? 1000;
    this.advisorySchedulerTimer = setInterval(() => {
      void this.runAdvisoryCheckPass().catch((err) => {
        this.lastStreamError = redactSecrets(String(err));
      });
    }, intervalMs);
    this.advisorySchedulerTimer.unref?.();
  }

  private stopAdvisoryScheduler(): void {
    if (this.advisoryAbortController) {
      this.advisoryAbortController.abort();
      this.advisoryAbortController = null;
    }
    if (this.advisorySchedulerTimer) {
      clearInterval(this.advisorySchedulerTimer);
      this.advisorySchedulerTimer = null;
    }
    this.clearAllJobInactivityTimers();
  }

  scheduleJobInactivityTimer(jobId: string): void {
    const existing = this.jobInactivityTimers.get(jobId);
    if (existing) clearTimeout(existing);

    const job = this.store.getJob(jobId);
    if (!job || TERMINAL_JOB_STATUSES.has(job.status) || job.status === "needs_approval") {
      this.jobInactivityTimers.delete(jobId);
      return;
    }

    const threshold = this.config.inactivityThresholdSeconds ?? 300;
    const lastProgressTime = parseTimestamp(job.lastProgressAt)
      ?? parseTimestamp(job.startedAt)
      ?? parseTimestamp(job.createdAt)
      ?? Date.now();
    const elapsedMs = Math.max(0, Date.now() - lastProgressTime);
    const remainingMs = Math.max(0, (threshold * 1000) - elapsedMs);

    const timer = setTimeout(() => {
      this.jobInactivityTimers.delete(jobId);
      void this.onJobInactivityTimeout(jobId).catch(() => undefined);
    }, remainingMs);
    timer.unref?.();
    this.jobInactivityTimers.set(jobId, timer);
  }

  private async onJobInactivityTimeout(jobId: string): Promise<void> {
    try {
      const job = this.store.getJob(jobId);
      if (!job || TERMINAL_JOB_STATUSES.has(job.status) || job.status === "needs_approval") {
        this.clearJobInactivityTimer(jobId);
        return;
      }
      await this.syncJobStreamProgressFromSpool(job);
      const updated = this.store.getJob(jobId);
      if (updated && this.isJobWakeEligible(updated)) {
        await this.evaluateParkWakes(updated.id);
      }
    } catch {}
  }

  private async syncJobStreamProgressFromSpool(job: JobRecord): Promise<void> {
    try {
      const spool = new AntigravitySpool(this.config.dataDir);
      let attemptId = job.attempt ?? null;
      let attemptFence = job.fence ?? 1;

      // Exact known attempt path first: avoids directory scan every interval
      let prog: AntigravityStreamProgress | null = null;
      if (attemptId) {
        prog = await spool.readProgress(attemptId, job.id);
      }

      // Bounded fallback ONLY if no attempt known on job
      if (!prog && !attemptId) {
        const latestAttempt = await spool.getLatestAttempt(job.id);
        if (latestAttempt) {
          attemptId = latestAttempt.attemptId;
          attemptFence = latestAttempt.fence ?? job.fence ?? 1;
          prog = await spool.readProgress(latestAttempt.attemptId, job.id);
        }
      }

      if (prog && prog.attemptId === (attemptId ?? prog.attemptId)) {
        if (
          prog.progressRevision > (job.progressRevision ?? 0) ||
          (prog.lastProgressAt && (!job.lastProgressAt || prog.lastProgressAt > job.lastProgressAt))
        ) {
          this.metrics.sqliteProgressUpdates++;
          this.store.updateJobProgress(job.id, {
            lastProgressAt: prog.lastProgressAt,
            progressRevision: prog.progressRevision,
            fence: attemptFence,
          });
          this.scheduleJobInactivityTimer(job.id);
        }
      }
    } catch {}
  }

  private async runAdvisoryCheckPass(): Promise<void> {
    if (this.lifecycleState !== "ready" && this.lifecycleState !== "recovering") return;
    if (this.advisoryPassRunning) {
      this.metrics.singleflightRunsSkipped++;
      return;
    }
    this.advisoryPassRunning = true;
    this.metrics.singleflightRunsStarted++;

    const signal = this.advisoryAbortController?.signal;
    if (signal?.aborted) {
      this.advisoryPassRunning = false;
      return;
    }

    try {
      const armedBarriers = this.store.listArmedBarriers();
      if (armedBarriers.length === 0) return;
      if (this.lifecycleState !== "ready" && this.lifecycleState !== "recovering") return;
      if (signal?.aborted) return;

      // Dedup jobs across barriers per pass
      const seenJobIds = new Set<string>();
      for (const barrier of armedBarriers) {
        const jobIds = this.store.getParkBarrierJobs(barrier.id);
        for (const jobId of jobIds) {
          seenJobIds.add(jobId);
        }
      }

      for (const jobId of seenJobIds) {
        if (this.lifecycleState !== "ready" && this.lifecycleState !== "recovering") return;
        if (signal?.aborted) return;

        const job = this.store.getJob(jobId);
        if (!job || TERMINAL_JOB_STATUSES.has(job.status) || job.status === "needs_approval") {
          this.clearJobInactivityTimer(jobId);
          continue;
        }

        await this.syncJobStreamProgressFromSpool(job);

        if (this.lifecycleState !== "ready" && this.lifecycleState !== "recovering") return;
        if (signal?.aborted) return;

        const updatedJob = this.store.getJob(jobId);
        if (updatedJob && this.isJobWakeEligible(updatedJob)) {
          await this.evaluateParkWakes(updatedJob.id);
        }
      }
    } finally {
      this.advisoryPassRunning = false;
    }
  }

  async spawn(input: SpawnInput): Promise<AcceptedOperation> {
    this.requireRunning();
    if (!input.task.trim()) throw new InvalidRequestError("Task must not be empty");
    if (input.task.length > this.config.maxTaskLength) throw new InvalidRequestError("Task exceeds configured length limit");
    const requestId = input.requestId ?? newId("request");
    const op = await this.withRequestIdLock(requestId, async () => {
      const existing = input.requestId ? this.store.getJobByRequestId(input.requestId) : null;
      if (existing) return this.acceptedRequest(existing);
      const route = this.resolveRouteForSpawn(input.modelRoute);
      const repositoryRoot = path.resolve(input.cwd ?? defaultWorkspace());
      await ensureDirectory(repositoryRoot);
      const mode = input.mode ?? "analyze";
      const strategy = input.workspaceStrategy ?? (mode === "edit" ? "worktree" : "shared");
      const agentId = newId("agent");
      const workspacePath = strategy === "shared"
        ? repositoryRoot
        : path.join(repositoryRoot, ".deepseek-worktrees", agentId);
      const allowedExternalFiles = [path.resolve(this.config.globalGeminiContextPath)];
      const candidateContextFiles = await this.resolveCandidateContextFiles(
        input.task,
        input.topic,
        input.contextFiles ?? [],
        this.config.globalGeminiContextPath,
      );
      const validatedContextFiles = await validateContextFilesStrict(
        repositoryRoot,
        candidateContextFiles,
        this.config.maxContextFileBytes,
        allowedExternalFiles,
      );
      const contextFiles = strategy === "worktree"
        ? this.mapContextIntoWorktree(workspacePath, repositoryRoot, validatedContextFiles, allowedExternalFiles)
        : validatedContextFiles;
      const isAntigravity = route.providerId === "antigravity";
      const title = normalizeTitle(input.topic);
      const promptWorkerInput: WorkerPromptInput = {
        ...input,
        topic: input.topic?.trim() || title,
        mode,
        workspaceStrategy: strategy,
        contextFiles,
        ...(input.visualContext ? { visualContext: input.visualContext } : {}),
      };
      const prompt = strategy === "worktree"
        ? ""
        : await buildWorkerPrompt(
            promptWorkerInput,
            workspacePath,
            workerPromptOptions(this.config, isAntigravity, allowedExternalFiles),
          );
      const promptHash = hashPrompt(prompt);
      const priority = typeof input.priority === "number" ? Math.max(1, Math.min(100, Math.floor(input.priority))) : 50;
      const exclusiveResources = Array.isArray(input.exclusiveResources)
        ? input.exclusiveResources.map((r) => r.trim()).filter((r) => r.length > 0)
        : [];
      const { agent, job } = this.store.admitUnary({
        agent: {
          id: agentId,
          title,
          topic: input.topic.trim() || title,
          repositoryRoot,
          workspacePath,
          workspaceStrategy: strategy,
          mode,
          opencodeServerId: isAntigravity ? "antigravity" : (this.managed?.serverId ?? "unknown"),
          opencodeSessionId: isAntigravity ? "antigravity:" + agentId : "pending:" + agentId,
          modelProviderId: route.providerId,
          modelId: route.modelId,
          modelVariant: route.variant,
          modelRoute: route.name,
        },
        job: {
          id: newId("job"),
          agentId,
          kind: "spawn",
          status: "queued",
          priority,
          exclusiveResources,
          requestId,
          promptHash,
          queuedAt: new Date().toISOString(),
          mcpSessionId: input.mcpSessionId ?? null,
          trustedThreadId: input.trustedThreadId ?? null,
        },
        correlationHint: {
          threadId: input.threadId,
          turnId: input.turnId,
        },
        dispatchEnvelope: {
          prompt,
          promptHash,
          workerInput: promptWorkerInput,
          contextFiles: contextFiles ?? [],
        },
      });
      this.recordActivity(agent, null, "dispatch", isAntigravity
        ? "Created an Antigravity agent for the task (no OpenCode session)"
        : "Created OpenCode session for the DeepSeek task");
      this.pendingDispatches.set(job.id, {
        prompt,
        workerInput: promptWorkerInput,
        contextFiles,
      });

      let dispatchResult: AcceptedOperation | undefined;
      let dispatchError: unknown;
      this.dispatchWaiters.set(job.id, {
        resolve: (op) => { dispatchResult = op; },
        reject: (err) => { dispatchError = err; },
      });

      try {
        await this.drainQueue();

        if (dispatchError) {
          throw dispatchError;
        }
        if (dispatchResult) {
          return dispatchResult;
        }
        return this.accepted(this.store.getJob(job.id) ?? job);
      } finally {
        this.dispatchWaiters.delete(job.id);
      }
    });
    return op;
  }

  async spawnBatch(input: SpawnBatchInput): Promise<AcceptedBatchOperation> {
    this.requireRunning();
    if (!input || !Array.isArray(input.items) || input.items.length === 0) {
      throw new InvalidRequestError("Batch items must be a non-empty array");
    }
    const batchRequestId = input.batchRequestId?.trim() || newId("batch_req");

    const seenRequestIds = new Set<string>();
    for (const item of input.items) {
      if (!item || typeof item.task !== "string" || !item.task.trim()) {
        throw new InvalidRequestError("Batch item task must not be empty");
      }
      if (item.task.length > this.config.maxTaskLength) {
        throw new InvalidRequestError("Batch item task exceeds configured length limit");
      }
      if (item.priority !== undefined) {
        if (typeof item.priority !== "number" || !Number.isFinite(item.priority) || item.priority < 1 || item.priority > 100) {
          throw new InvalidRequestError("Batch item priority must be a number between 1 and 100");
        }
      }
      if (item.exclusiveResources !== undefined) {
        if (!Array.isArray(item.exclusiveResources) || item.exclusiveResources.some((r) => typeof r !== "string" || !r.trim())) {
          throw new InvalidRequestError("Batch item exclusiveResources must be an array of non-empty strings");
        }
      }
      if (item.requestId) {
        if (seenRequestIds.has(item.requestId)) {
          throw new InvalidRequestError("Duplicate requestId within batch: " + item.requestId);
        }
        seenRequestIds.add(item.requestId);
      }
    }

    const batchHash = computeBatchHash(input.items);

    return this.withRequestIdLock(batchRequestId, async () => {
      const existingBatch = this.store.getBatchByRequestId(batchRequestId);
      if (existingBatch) {
        if (existingBatch.batchHash === batchHash) {
          const existingJobs = this.store.listBatchJobs(existingBatch.id);
          const items: BatchItemReceipt[] = existingJobs.map((j) => ({
            jobId: j.id,
            agentId: j.agentId,
            requestId: j.requestId ?? undefined,
            status: j.status === "queued" ? "queued" : "accepted",
          }));
          return {
            accepted: true,
            batchId: existingBatch.id,
            batchRequestId: existingBatch.requestId,
            items,
          };
        }
        throw new ConflictError("Conflicting payload for existing batch request ID: " + batchRequestId);
      }

      for (const item of input.items) {
        const raw = item as unknown as Record<string, unknown>;
        const reqId = item.requestId ?? (typeof raw.request_id === "string" ? raw.request_id : undefined);
        if (reqId) {
          const existingJob = this.store.getJobByRequestId(reqId);
          if (existingJob) {
            throw new ConflictError("Job request ID already exists: " + reqId);
          }
        }
      }

      const preparedItems: Array<{
        item: BatchItemInput;
        agentId: string;
        jobId: string;
        title: string;
        topic: string;
        repositoryRoot: string;
        workspacePath: string;
        strategy: WorkspaceStrategy;
        mode: AgentMode;
        route: ResolvedRoute;
        isAntigravity: boolean;
        prompt: string;
        promptWorkerInput: WorkerPromptInput;
        contextFiles: string[];
        priority: number;
        exclusiveResources: string[];
        requestId: string;
        threadId?: string | undefined;
        turnId?: string | undefined;
        mcpSessionId?: string | undefined;
        trustedThreadId?: string | undefined;
        opencodeServerId: string;
        opencodeSessionId: string;
      }> = [];

      for (const item of input.items) {
        const raw = item as unknown as Record<string, unknown>;
        const routeName = item.modelRoute ?? (typeof raw.model_route === "string" ? raw.model_route : undefined);
        const route = this.resolveRouteForSpawn(routeName);
        const cwd = item.cwd ?? (typeof raw.cwd === "string" ? raw.cwd : undefined);
        const repositoryRoot = path.resolve(cwd ?? defaultWorkspace());
        await ensureDirectory(repositoryRoot);
        const mode = item.mode ?? (typeof raw.mode === "string" ? raw.mode as AgentMode : undefined) ?? "analyze";
        const stratRaw = item.workspaceStrategy ?? (typeof raw.workspace_strategy === "string" ? raw.workspace_strategy as WorkspaceStrategy : undefined);
        const strategy = stratRaw ?? (mode === "edit" ? "worktree" : "shared");
        const agentId = newId("agent");
        const jobId = newId("job");
        const workspacePath = strategy === "shared"
          ? repositoryRoot
          : path.join(repositoryRoot, ".deepseek-worktrees", agentId);
        const allowedExternalFiles = [path.resolve(this.config.globalGeminiContextPath)];
        const inputFiles = item.contextFiles ?? (Array.isArray(raw.context_files) ? raw.context_files as string[] : []);
        const topic = (item.topic ?? (typeof raw.topic === "string" ? raw.topic : "") ?? "").trim();
        const candidateContextFiles = await this.resolveCandidateContextFiles(
          item.task,
          topic,
          inputFiles,
          this.config.globalGeminiContextPath,
        );
        const validatedContextFiles = await validateContextFilesStrict(
          repositoryRoot,
          candidateContextFiles,
          this.config.maxContextFileBytes,
          allowedExternalFiles,
        );
        const contextFiles = strategy === "worktree"
          ? this.mapContextIntoWorktree(workspacePath, repositoryRoot, validatedContextFiles, allowedExternalFiles)
          : validatedContextFiles;
        const isAntigravity = route.providerId === "antigravity";
        const promptOptions = workerPromptOptions(this.config, isAntigravity, allowedExternalFiles);
        const visualContext = item.visualContext ?? (typeof raw.visual_context === "string" ? raw.visual_context : undefined);
        const promptWorkerInput: WorkerPromptInput = {
          topic,
          task: item.task,
          mode,
          workspaceStrategy: strategy,
          contextFiles,
          ...(visualContext ? { visualContext } : {}),
        };
        const prompt = strategy === "worktree"
          ? ""
          : await buildWorkerPrompt(promptWorkerInput, workspacePath, promptOptions);
        const title = normalizeTitle(topic);
        const rawPriority = typeof item.priority === "number" ? item.priority : (typeof raw.priority === "number" ? raw.priority : 50);
        const priority = Math.max(1, Math.min(100, Math.floor(rawPriority)));
        const rawRes = item.exclusiveResources ?? (Array.isArray(raw.exclusive_resources) ? raw.exclusive_resources as string[] : []);
        const exclusiveResources = Array.from(new Set(rawRes.map((r) => String(r).trim()).filter((r) => r.length > 0))).sort();
        const requestId = item.requestId ?? (typeof raw.request_id === "string" ? raw.request_id : undefined) ?? newId("request");
        const threadId = item.threadId ?? (typeof raw.thread_id === "string" ? raw.thread_id : undefined);
        const turnId = item.turnId ?? (typeof raw.turn_id === "string" ? raw.turn_id : undefined);
        const mcpSessionId = item.mcpSessionId ?? (typeof raw.mcp_session_id === "string" ? raw.mcp_session_id : undefined);
        const trustedThreadId = item.trustedThreadId ?? (typeof raw.trusted_thread_id === "string" ? raw.trusted_thread_id : undefined);
        const session = isAntigravity
          ? null
          : await this.clientOrThrow().createSession(workspacePath, title);
        const opencodeSessionId = isAntigravity ? "antigravity:" + agentId : session!.id;
        const opencodeServerId = isAntigravity ? "antigravity" : (this.managed?.serverId ?? "unknown");

        preparedItems.push({
          item,
          agentId,
          jobId,
          title,
          topic: topic || title,
          repositoryRoot,
          workspacePath,
          strategy,
          mode,
          route,
          isAntigravity,
          prompt,
          promptWorkerInput,
          contextFiles,
          priority,
          exclusiveResources,
          requestId,
          threadId,
          turnId,
          mcpSessionId,
          trustedThreadId,
          opencodeServerId,
          opencodeSessionId,
        });
      }

      const batchId = newId("batch");
      this.store.admitBatch({
        batch: {
          id: batchId,
          requestId: batchRequestId,
          batchHash,
          status: "queued",
        },
        items: preparedItems.map((prep, prepIndex) => ({
          agent: {
            id: prep.agentId,
            title: prep.title,
            topic: prep.topic,
            repositoryRoot: prep.repositoryRoot,
            workspacePath: prep.workspacePath,
            workspaceStrategy: prep.strategy,
            mode: prep.mode,
            opencodeServerId: prep.opencodeServerId,
            opencodeSessionId: prep.opencodeSessionId,
            modelProviderId: prep.route.providerId,
            modelId: prep.route.modelId,
            modelVariant: prep.route.variant,
            modelRoute: prep.route.name,
          },
          job: {
            id: prep.jobId,
            agentId: prep.agentId,
            kind: "spawn",
            status: "queued",
            batchId,
            priority: prep.priority,
            exclusiveResources: prep.exclusiveResources,
            requestId: prep.requestId,
            promptHash: hashPrompt(prep.prompt),
            queuedAt: new Date(Date.now() + prepIndex).toISOString(),
            mcpSessionId: prep.mcpSessionId ?? null,
            trustedThreadId: prep.trustedThreadId ?? null,
          },
          correlationHint: (prep.threadId || prep.turnId) ? {
            threadId: prep.threadId,
            turnId: prep.turnId,
          } : undefined,
          dispatchEnvelope: {
            prompt: prep.prompt,
            promptHash: hashPrompt(prep.prompt),
            workerInput: prep.promptWorkerInput,
            contextFiles: prep.contextFiles,
          },
        })),
      });

      for (const prep of preparedItems) {
        this.pendingDispatches.set(prep.jobId, {
          prompt: prep.prompt,
          workerInput: prep.promptWorkerInput,
          contextFiles: prep.contextFiles,
        });
      }

      await this.drainQueue();

      const receipts: BatchItemReceipt[] = preparedItems.map((prep) => {
        const currentJob = this.store.getJob(prep.jobId);
        const status = currentJob?.status === "queued" ? "queued" : "accepted";
        return {
          jobId: prep.jobId,
          agentId: prep.agentId,
          requestId: prep.requestId,
          status,
        };
      });

      return {
        accepted: true,
        batchId,
        batchRequestId,
        items: receipts,
        capabilities: {
          batch_scheduler: true,
        },
      };
    });
  }

  private async withRepoPreparationLock<T>(repositoryRoot: string, fn: () => Promise<T>): Promise<T> {
    const canonical = path.resolve(repositoryRoot).toLowerCase();
    const prevLock = this.repoPreparationLocks.get(canonical) ?? Promise.resolve();
    let releaseLock!: () => void;
    const nextLock = new Promise<void>((r) => { releaseLock = r; });
    this.repoPreparationLocks.set(canonical, nextLock);
    try {
      await prevLock;
      return await fn();
    } finally {
      if (this.repoPreparationLocks.get(canonical) === nextLock) {
        this.repoPreparationLocks.delete(canonical);
      }
      releaseLock();
    }
  }

  private getActiveExclusiveResources(): Set<string> {
    const claimed = new Set<string>();
    for (const [res, jobId] of this.activeExclusiveResources.entries()) {
      const job = this.store.getJob(jobId);
      if (!job || !["dispatching", "running", "following", "finalizing", "needs_approval"].includes(job.status)) {
        this.activeExclusiveResources.delete(res);
      } else {
        claimed.add(res);
      }
    }
    const activeJobs = this.store.listActiveJobs();
    for (const job of activeJobs) {
      if (Array.isArray(job.exclusiveResources)) {
        for (const res of job.exclusiveResources) {
          claimed.add(res);
          this.activeExclusiveResources.set(res, job.id);
        }
      }
    }
    return claimed;
  }

  private releaseExclusiveResources(jobId: string): void {
    for (const [res, claimJobId] of this.activeExclusiveResources.entries()) {
      if (claimJobId === jobId) {
        this.activeExclusiveResources.delete(res);
      }
    }
  }

  private onJobSettled(jobId: string, batchId?: string | null): void {
    this.clearJobInactivityTimer(jobId);
    this.releaseExclusiveResources(jobId);
    this.syncBatchStatus(batchId);
    this.scheduleDrain();
  }

  private getActiveResourceClaimsList(): Array<{ resource: string; jobId: string }> {
    this.getActiveExclusiveResources();
    const list: Array<{ resource: string; jobId: string }> = [];
    for (const [resource, jobId] of this.activeExclusiveResources.entries()) {
      list.push({ resource, jobId });
    }
    return list;
  }

  private syncBatchStatus(batchId: string | null | undefined): void {
    if (!batchId) return;
    const batch = this.store.getBatch(batchId);
    if (!batch) return;
    const jobs = this.store.listBatchJobs(batchId);
    if (jobs.length === 0) return;
    const allTerminal = jobs.every((j) => TERMINAL_JOB_STATUSES.has(j.status));
    const anyRunning = jobs.some((j) => ["dispatching", "running", "following", "finalizing", "needs_approval"].includes(j.status));
    if (allTerminal) {
      const anyFailed = jobs.some((j) => ["failed", "aborted", "timed_out"].includes(j.status));
      this.store.updateBatchStatus(batchId, anyFailed ? "failed" : "completed");
    } else if (anyRunning) {
      this.store.updateBatchStatus(batchId, "running");
    }
  }

  private scheduleDrain(): void {
    if (this.drainingQueue) {
      this.drainPending = true;
      return;
    }
    this.drainPending = false;
    this.drainQueue().catch((err) => {
      this.lastStreamError = redactSecrets(String(err));
    });
  }

  private async drainQueue(): Promise<void> {
    if (this.drainingQueue) {
      this.drainPending = true;
      return;
    }
    this.drainingQueue = true;
    try {
      while (this.lifecycleState === "ready" || this.lifecycleState === "recovering") {
        const activeCount = this.store.getActiveJobCount();
        const availableCredits = this.targetCredits - activeCount;
        if (availableCredits <= 0) break;

        const queuedJobs = this.store.listQueuedJobs();
        if (queuedJobs.length === 0) break;

        const claimedResources = this.getActiveExclusiveResources();
        const now = Date.now();

        const candidates = queuedJobs.map((job) => {
          const waitMs = Math.max(0, now - (parseTimestamp(job.queuedAt ?? job.createdAt) ?? now));
          const elapsedSec = Math.floor(waitMs / 1000);
          const agingBonus = Math.min(50, Math.floor(elapsedSec / 5));
          const basePriority = job.priority ?? 50;
          const effectivePriority = basePriority + agingBonus;
          return { job, effectivePriority, waitMs };
        });

        candidates.sort((a, b) => {
          if (b.effectivePriority !== a.effectivePriority) {
            return b.effectivePriority - a.effectivePriority;
          }
          if (b.waitMs !== a.waitMs) {
            return b.waitMs - a.waitMs;
          }
          const timeA = parseTimestamp(a.job.queuedAt ?? a.job.createdAt) ?? 0;
          const timeB = parseTimestamp(b.job.queuedAt ?? b.job.createdAt) ?? 0;
          if (timeA !== timeB) {
            return timeA - timeB;
          }
          const seqA = a.job.sequence ?? 0;
          const seqB = b.job.sequence ?? 0;
          if (seqA !== seqB) {
            return seqA - seqB;
          }
          return a.job.id.localeCompare(b.job.id);
        });

        const activeJobs = this.store.listActiveJobs();
        const activeAgents = new Set(activeJobs.map((j) => j.agentId));

        let dispatchedAny = false;
        for (const candidate of candidates) {
          if (activeAgents.has(candidate.job.agentId)) continue;

          const resources = candidate.job.exclusiveResources ?? [];
          const hasConflict = resources.some((r) => claimedResources.has(r));
          if (hasConflict) continue;

          for (const r of resources) {
            claimedResources.add(r);
          }
          activeAgents.add(candidate.job.agentId);

          try {
            await this.dispatchQueuedJob(candidate.job);
            dispatchedAny = true;
          } catch (err) {
            this.lastStreamError = redactSecrets(String(err));
            dispatchedAny = true;
          }
          break;
        }

        if (!dispatchedAny) {
          break;
        }
      }
    } finally {
      this.drainingQueue = false;
      if (this.drainPending) {
        this.scheduleDrain();
      }
    }
  }

  private async dispatchQueuedJob(job: JobRecord): Promise<void> {
    const waiter = this.dispatchWaiters.get(job.id);
    this.dispatchWaiters.delete(job.id);

    const claimedJob = this.store.claimQueuedJobForDispatch(job.id, job.fence);
    if (!claimedJob) {
      const current = this.store.getJob(job.id);
      if (current?.status === "dispatching" || current?.status === "running") {
        return;
      }
      this.store.deleteDispatchEnvelope(job.id);
      this.pendingDispatches.delete(job.id);
      this.onJobSettled(job.id, job.batchId);
      const err = new Error(
        current?.status === "aborted"
          ? "Invalid job transition: aborted -> dispatching"
          : `Failed to claim queued job ${job.id} for dispatch`
      );
      waiter?.reject(err);
      if (current?.status === "aborted") {
        throw err;
      }
      return;
    }
    job = claimedJob;

    const agent = this.store.getAgent(job.agentId);
    if (!agent || agent.status === "closed" || agent.status === "aborted") {
      const err = new Error(
        !agent
          ? "Agent not found for queued job: " + job.agentId
          : `Agent ${job.agentId} is in invalid state '${agent.status}' for dispatch`
      );
      this.store.updateJobStatus(job.id, "failed", err.message);
      this.store.deleteDispatchEnvelope(job.id);
      this.pendingDispatches.delete(job.id);
      this.onJobSettled(job.id, job.batchId);
      if (this.followLifecycles.has(job.id)) {
        await this.resolveFollow(job.id, { status: "failed", error: err.message });
      }
      await this.evaluateParkWakes(job.id).catch((e: unknown) => {
        this.lastStreamError = redactSecrets(String(e));
      });
      waiter?.reject(err);
      return;
    }

    const envelope = this.store.getDispatchEnvelope(job.id);
    const pending = this.pendingDispatches.get(job.id);
    let prompt: string | undefined;
    let workerInput: WorkerPromptInput | undefined;
    let contextFiles: string[] | undefined;

    if (envelope) {
      if (envelope.prompt) {
        const computedHash = hashPrompt(envelope.prompt);
        if (computedHash !== job.promptHash || computedHash !== envelope.promptHash) {
          const err = new Error(`Dispatch envelope integrity mismatch for job ${job.id}`);
          this.store.updateJobStatus(job.id, "failed", err.message);
          const currentAgent = this.store.getAgent(agent.id);
          if (currentAgent && currentAgent.status !== "closed" && currentAgent.status !== "aborted") {
            this.store.updateAgentStatus(agent.id, "failed", err.message);
          }
          this.store.deleteDispatchEnvelope(job.id);
          this.pendingDispatches.delete(job.id);
          this.onJobSettled(job.id, job.batchId);
          if (this.followLifecycles.has(job.id)) {
            await this.resolveFollow(job.id, { status: "failed", error: err.message });
          }
          await this.evaluateParkWakes(job.id).catch((e: unknown) => {
            this.lastStreamError = redactSecrets(String(e));
          });
          waiter?.reject(err);
          return;
        }
      }
      prompt = envelope.prompt;
      workerInput = envelope.workerInput as WorkerPromptInput;
      contextFiles = envelope.contextFiles;
    } else if (pending) {
      prompt = pending.prompt;
      workerInput = pending.workerInput;
      contextFiles = pending.contextFiles;
    }

    try {
      if (workerInput && (workerInput as any).permissionReply) {
        const permInput = workerInput as any;
        const currentAgent = this.store.getAgent(agent.id);
        if (currentAgent && currentAgent.status !== "working") {
          this.store.updateAgentStatus(agent.id, "working");
        }
        this.store.updateJobStatus(job.id, "running");
        if (Array.isArray(job.exclusiveResources)) {
          for (const res of job.exclusiveResources) {
            this.activeExclusiveResources.set(res, job.id);
          }
        }
        try {
          await this.clientOrThrow().replyPermission(
            agent.opencodeSessionId,
            permInput.permissionId,
            permInput.permissionReply,
            permInput.permissionMessage,
          );
          const afterReply = this.store.getJob(job.id);
          if (afterReply?.status === "running" && afterReply.permissionId === permInput.permissionId) {
            this.store.setJobPermission(job.id, null);
          }
          this.store.deleteDispatchEnvelope(job.id);
          this.pendingDispatches.delete(job.id);
          waiter?.resolve(this.accepted(this.store.getJob(job.id) ?? job));
          return;
        } catch (error) {
          const message = redactSecrets(String(error instanceof Error ? error.message : error));
          this.store.updateJobStatus(job.id, "failed", message);
          const curAgent = this.store.getAgent(agent.id);
          if (curAgent && curAgent.status !== "closed" && curAgent.status !== "aborted") {
            this.store.updateAgentStatus(agent.id, "failed", message);
          }
          this.store.deleteDispatchEnvelope(job.id);
          this.pendingDispatches.delete(job.id);
          this.onJobSettled(job.id, job.batchId);
          waiter?.reject(error);
          throw error;
        }
      }

      if (agent.workspaceStrategy === "worktree") {
        await this.withRepoPreparationLock(agent.repositoryRoot, async () => {
          return prepareWorkspace(agent.repositoryRoot, "worktree", agent.id);
        });
      }

      const isAntigravity = agent.modelProviderId === "antigravity";
      if (!isAntigravity && agent.opencodeSessionId.startsWith("pending:")) {
        const session = await this.clientOrThrow().createSession(agent.workspacePath, agent.title);
        this.store.updateAgentSession(agent.id, this.managed?.serverId ?? "unknown", session.id);
        agent.opencodeSessionId = session.id;
        agent.opencodeServerId = this.managed?.serverId ?? "unknown";
      }

      if (!prompt || (agent.workspaceStrategy === "worktree" && job.kind !== "continue")) {
        if (!workerInput) {
          const err = new Error(`Dispatch envelope missing or prompt empty for job ${job.id}`);
          this.store.updateJobStatus(job.id, "failed", err.message);
          const currentAgent = this.store.getAgent(agent.id);
          if (currentAgent && currentAgent.status !== "closed" && currentAgent.status !== "aborted") {
            this.store.updateAgentStatus(agent.id, "failed", err.message);
          }
          this.store.deleteDispatchEnvelope(job.id);
          this.pendingDispatches.delete(job.id);
          this.onJobSettled(job.id, job.batchId);
          if (this.followLifecycles.has(job.id)) {
            await this.resolveFollow(job.id, { status: "failed", error: err.message });
          }
          await this.evaluateParkWakes(job.id).catch((e: unknown) => {
            this.lastStreamError = redactSecrets(String(e));
          });
          waiter?.reject(err);
          return;
        }
        const workerContextFiles = "contextFiles" in workerInput && Array.isArray(workerInput.contextFiles) ? workerInput.contextFiles : undefined;
        const effectiveContextFiles = workerContextFiles ?? contextFiles ?? [];
        const effectiveWorkerInput: WorkerPromptInput = {
          ...workerInput,
          contextFiles: effectiveContextFiles,
        };
        const allowedExternalFiles = [path.resolve(this.config.globalGeminiContextPath)];
        const promptOptions = workerPromptOptions(this.config, isAntigravity, allowedExternalFiles);
        prompt = await buildWorkerPrompt(effectiveWorkerInput, agent.workspacePath, promptOptions);
      }

      if (!prompt) {
        const err = new Error(`Dispatch envelope missing or prompt empty for job ${job.id}`);
        this.store.updateJobStatus(job.id, "failed", err.message);
        const currentAgent = this.store.getAgent(agent.id);
        if (currentAgent && currentAgent.status !== "closed" && currentAgent.status !== "aborted") {
          this.store.updateAgentStatus(agent.id, "failed", err.message);
        }
        this.store.deleteDispatchEnvelope(job.id);
        this.pendingDispatches.delete(job.id);
        this.onJobSettled(job.id, job.batchId);
        if (this.followLifecycles.has(job.id)) {
          await this.resolveFollow(job.id, { status: "failed", error: err.message });
        }
        await this.evaluateParkWakes(job.id).catch((e: unknown) => {
          this.lastStreamError = redactSecrets(String(e));
        });
        waiter?.reject(err);
        return;
      }

      this.store.updateJobStatus(job.id, "dispatching");

      const currentFollow = this.followLifecycles.get(job.id);
      if (currentFollow && !currentFollow.settled) {
        const now = Date.now();
        const startedAt = now;
        const deadlineAt = startedAt + currentFollow.waitMinutes * 60_000;
        this.store.setFollowWindow(job.id, {
          startedAt: new Date(startedAt).toISOString(),
          deadlineAt: new Date(deadlineAt).toISOString(),
          graceMinutes: currentFollow.graceMinutes,
          graceDeadlineAt: null,
          gracefulFinalizeAttempted: false,
        });
        const timeoutMs = this.effectiveWorkerTimeoutMs(currentFollow.waitMinutes, currentFollow.graceMinutes);
        const leaseExpiresAt = timeoutMs !== null ? new Date(startedAt + timeoutMs).toISOString() : null;
        this.store.updateJobLiveness(job.id, { leaseExpiresAt });
        if (this.config.workerMaxExecutionMinutes !== null) {
          this.scheduleDeadlineTimer(currentFollow, deadlineAt);
        }
      }

      if (Array.isArray(job.exclusiveResources)) {
        for (const res of job.exclusiveResources) {
          this.activeExclusiveResources.set(res, job.id);
        }
      }

      if (job.batchId) {
        this.store.updateBatchStatus(job.batchId, "running");
      }

      const op = await this.dispatch(agent, job, prompt, workerInput, contextFiles);
      this.store.deleteDispatchEnvelope(job.id);
      this.pendingDispatches.delete(job.id);
      waiter?.resolve(op);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      const current = this.store.getJob(job.id);
      const currentAgent = this.store.getAgent(agent.id);
      const preservesApproval = current?.status === "needs_approval" || currentAgent?.status === "needs_approval";
      if (!preservesApproval) {
        this.store.updateJobStatus(job.id, "failed", errorMessage);
        if (currentAgent && currentAgent.status !== "closed" && currentAgent.status !== "aborted") {
          this.store.updateAgentStatus(agent.id, "failed", errorMessage);
        }
        this.store.deleteDispatchEnvelope(job.id);
        this.pendingDispatches.delete(job.id);
        this.onJobSettled(job.id, job.batchId);
        if (this.followLifecycles.has(job.id)) {
          await this.resolveFollow(job.id, { status: "failed", error: errorMessage });
        }
        await this.evaluateParkWakes(job.id).catch((err: unknown) => {
          this.lastStreamError = redactSecrets(String(err));
        });
      } else {
        this.store.deleteDispatchEnvelope(job.id);
        this.pendingDispatches.delete(job.id);
      }
      waiter?.reject(error);
      if (isBackpressureError(error) && !(error as any)?.backpressureRecorded) {
        this.recordBackpressure("bridge_busy");
        (error as any).backpressureRecorded = true;
      }
      throw error;
    }
  }

  async continueJob(input: ContinueInput): Promise<AcceptedOperation> {
    this.requireRunning();
    if (!input.task.trim()) throw new InvalidRequestError("Task must not be empty");
    if (input.task.length > this.config.maxTaskLength) throw new InvalidRequestError("Task exceeds configured length limit");
    return this.withAgentOperationLock(input.agentId, async () => {
      const existing = this.store.getJobByRequestId(input.requestId);
      if (existing) return this.acceptedRequest(existing);
      const agent = this.store.getAgent(input.agentId);
      if (!agent) throw new UnknownAgentError(input.agentId);
      const active = this.activeJob(agent.id);
      if (active && active.status !== "needs_approval") throw new BridgeBusyError(active.id);
      if (agent.status === "closed" || agent.status === "aborted") {
        if (!input.allowRespawn) throw new ConflictError("Agent is not continuable", "not_continuable");
        return this.respawnClosedAgent(agent, input);
      }
      const prompt = await buildWorkerPrompt({
        task: input.task,
        relation: input.relation,
        ...(input.visualContext ? { visualContext: input.visualContext } : {}),
      }, agent.workspacePath, workerPromptOptions(this.config, agent.modelProviderId === "antigravity"));
      if (active?.status === "needs_approval") {
        if (input.permissionId || input.permissionReply || input.permissionMessage) {
          if (!input.permissionId || !input.permissionReply) {
            throw new InvalidRequestError("permissionId and permissionReply are both required to answer an approval request", "permission_required");
          }
          return this.replyApproval(agent, active, input.permissionId, input.permissionReply, input.permissionMessage);
        }
        return this.resumeApproval(agent, active, prompt);
      }
      const priorJob = this.store.getLatestJobForAgent(agent.id);
      const priority = priorJob?.priority ?? 50;
      const exclusiveResources = priorJob?.exclusiveResources ?? [];
      const jobId = newId("job");
      const promptHash = hashPrompt(prompt);
      const workerInput: WorkerPromptInput = {
        task: input.task,
        relation: input.relation,
        ...(input.visualContext ? { visualContext: input.visualContext } : {}),
      };
      const { job } = this.store.admitContinuation({
        job: {
          id: jobId,
          agentId: agent.id,
          kind: "continue",
          status: "queued",
          priority,
          exclusiveResources,
          requestId: input.requestId,
          promptHash,
          queuedAt: new Date().toISOString(),
          mcpSessionId: input.mcpSessionId ?? null,
          trustedThreadId: input.trustedThreadId ?? null,
        },
        correlationHint: (input.threadId || input.turnId) ? {
          threadId: input.threadId,
          turnId: input.turnId,
          source: "mcp",
        } : undefined,
        dispatchEnvelope: {
          prompt,
          promptHash,
          workerInput,
          contextFiles: [],
        },
      });
      this.pendingDispatches.set(job.id, {
        prompt,
        workerInput,
        contextFiles: [],
      });

      let dispatchResult: AcceptedOperation | undefined;
      let dispatchError: unknown;
      this.dispatchWaiters.set(job.id, {
        resolve: (op) => { dispatchResult = op; },
        reject: (err) => { dispatchError = err; },
      });

      try {
        await this.drainQueue();

        if (dispatchError) {
          throw dispatchError;
        }
        if (dispatchResult) {
          return dispatchResult;
        }
        return this.accepted(this.store.getJob(job.id) ?? job);
      } finally {
        this.dispatchWaiters.delete(job.id);
      }
    });
  }

  /**
   * Closed-agent recovery with lineage. Only reachable with allow_respawn:
   * a closed agent is NEVER reopened or made continuable; a brand-new agent
   * and a brand-new OpenCode session are created in the parent's persisted
   * workspace/topic/strategy with the parent's pinned route columns (never
   * the live config registry, so no provider fallback and no redirect).
   * Fails closed when the parent was explicitly aborted, has no terminal
   * job with a persisted result, is busy, or when permission fields are
   * supplied (a closed agent has no pending approval to answer).
   */
  private async respawnClosedAgent(agent: AgentRecord, input: ContinueInput): Promise<AcceptedOperation> {
    if (agent.status === "aborted") throw new ConflictError("Agent was explicitly aborted; it cannot be resumed", "not_continuable");
    if (input.permissionId || input.permissionReply || input.permissionMessage) {
      throw new InvalidRequestError("permission fields are not applicable when resuming a closed agent", "invalid_request");
    }
    const lastJob = this.store.getLatestJobForAgent(agent.id);
    if (!lastJob) throw new ConflictError("Agent has no completed job to resume from", "not_continuable");
    if (lastJob.status === "aborted") {
      throw new ConflictError("Agent was explicitly aborted; it cannot be resumed", "not_continuable");
    }
    if (!TERMINAL_JOB_STATUSES.has(lastJob.status) || !lastJob.resultPath) {
      throw new ConflictError("Agent was closed without a persisted result; it cannot be resumed", "not_continuable");
    }

    const childId = newId("agent");
    const workspacePath = agent.workspacePath;
    const title = normalizeTitle(agent.topic);
    const isAntigravity = agent.modelProviderId === "antigravity";
    const prompt = await buildWorkerPrompt({
      task: input.task,
      relation: input.relation ?? "followup",
      ...(input.visualContext ? { visualContext: input.visualContext } : {}),
    }, workspacePath, workerPromptOptions(this.config, isAntigravity));

    const opencodeServerId = isAntigravity ? "antigravity" : (this.managed?.serverId ?? agent.opencodeServerId);
    const opencodeSessionId = isAntigravity ? "antigravity:" + childId : "pending:" + childId;

    const correlationHint = (input.threadId || input.turnId)
      ? {
          threadId: input.threadId,
          turnId: input.turnId,
          source: "mcp",
        }
      : (lastJob.hintThreadId || lastJob.hintTurnId)
      ? {
          threadId: lastJob.hintThreadId,
          turnId: lastJob.hintTurnId,
          source: lastJob.hintSource ?? "inherited",
        }
      : undefined;

    const workerInput: WorkerPromptInput = {
      task: input.task,
      relation: input.relation ?? "followup",
      ...(input.visualContext ? { visualContext: input.visualContext } : {}),
    };

    const jobId = newId("job");
    const promptHash = hashPrompt(prompt);

    const { agent: child, job } = this.store.admitContinuation({
      agent: {
        id: childId,
        title,
        topic: agent.topic,
        repositoryRoot: agent.repositoryRoot,
        workspacePath,
        workspaceStrategy: agent.workspaceStrategy,
        mode: agent.mode ?? "analyze",
        opencodeServerId,
        opencodeSessionId,
        modelProviderId: agent.modelProviderId,
        modelId: agent.modelId,
        modelVariant: agent.modelVariant,
        modelRoute: agent.modelRoute,
        parentAgentId: agent.id,
      },
      job: {
        id: jobId,
        agentId: childId,
        kind: "continue",
        status: "queued",
        priority: lastJob.priority ?? 50,
        exclusiveResources: lastJob.exclusiveResources ?? [],
        requestId: input.requestId,
        promptHash,
        queuedAt: new Date().toISOString(),
        mcpSessionId: input.mcpSessionId ?? null,
        trustedThreadId: input.trustedThreadId ?? null,
      },
      correlationHint,
      dispatchEnvelope: {
        prompt,
        promptHash,
        workerInput,
        contextFiles: [],
      },
    });

    this.recordActivity(agent, lastJob, "dispatch", "Closed agent resumed: spawned lineage agent " + child!.id + " after job " + lastJob.id);
    this.recordActivity(child!, job, "dispatch", "Resumed from closed agent " + agent.id + " after job " + lastJob.id + "; new " + (isAntigravity ? "Antigravity run (no OpenCode session)" : "OpenCode session"));

    this.pendingDispatches.set(job.id, {
      prompt,
      workerInput,
      contextFiles: [],
    });

    let dispatchResult: AcceptedOperation | undefined;
    let dispatchError: unknown;
    this.dispatchWaiters.set(job.id, {
      resolve: (op) => { dispatchResult = op; },
      reject: (err) => { dispatchError = err; },
    });

    try {
      await this.drainQueue();

      if (dispatchError) {
        throw dispatchError;
      }
      if (dispatchResult) {
        return dispatchResult;
      }
      return this.accepted(this.store.getJob(job.id) ?? job);
    } finally {
      this.dispatchWaiters.delete(job.id);
    }
  }

  async consult(input: ConsultInput): Promise<ProgressSnapshot> {
    this.requireRunning();
    const agent = this.store.getAgent(input.agentId);
    if (!agent) throw new UnknownAgentError(input.agentId);
    const job = this.resolveJobForAgent(agent.id, input.jobId);
    if (input.jobId && (!job || job.agentId !== agent.id)) {
      throw new InvalidRequestError("Job does not belong to the requested agent", "job_agent_mismatch");
    }
    const snapshot = await this.progressSnapshot(agent, job, normalizeActivityLimit(input.activityLimit));
    if (job && snapshot.semanticProgress?.isStalled) {
      await this.evaluateParkWakes(job.id).catch((err) => {
        this.lastStreamError = redactSecrets(String(err));
      });
    }
    return snapshot;
  }

  async getAuthoritativeStatus(agentId: string, jobId?: string): Promise<AuthoritativeLivenessStatus> {
    const snapshot = await this.consult(jobId ? { agentId, jobId } : { agentId });
    return snapshot.authoritativeStatus!;
  }

  private async ensureAntigravityQuiescence(
    jobId: string,
    spool: AntigravitySpool,
    maxWaitMs = 5000,
  ): Promise<QuiescenceProof> {
    const deadline = Date.now() + maxWaitMs;
    const task = this.antigravityTasksByJob.get(jobId);
    if (task) {
      const remaining = Math.max(0, deadline - Date.now());
      await Promise.race([
        task,
        new Promise((r) => setTimeout(r, remaining)),
      ]).catch(() => undefined);
    }
    const latestAttempt = await spool.getLatestAttempt(jobId).catch(() => null);
    let supervisorPid: number | null = null;
    let agyPid: number | null = null;
    let supervisorAlive = false;
    let agyAlive = false;

    while (Date.now() < deadline) {
      if (latestAttempt) {
        const heartbeat = await spool.readHeartbeat(latestAttempt.heartbeatPath).catch(() => null);
        supervisorPid = heartbeat?.supervisorPid ?? null;
        agyPid = heartbeat?.agyPid ?? null;
        supervisorAlive = supervisorPid === process.pid
          ? this.antigravityTasksByJob.has(jobId)
          : (supervisorPid ? isProcessAlive(supervisorPid) : false);
        agyAlive = agyPid ? isProcessAlive(agyPid) : false;
      } else {
        supervisorAlive = this.antigravityTasksByJob.has(jobId);
      }
      if (!supervisorAlive && !agyAlive) {
        const pidsChecked = [supervisorPid, agyPid].filter((p): p is number => typeof p === "number");
        return {
          stopped: true,
          jobId,
          supervisorPid,
          agyPid,
          pidsChecked,
          alivePids: [],
          verifiedAt: new Date().toISOString(),
        };
      }
      await new Promise((r) => setTimeout(r, 25));
    }

    if (latestAttempt) {
      const heartbeat = await spool.readHeartbeat(latestAttempt.heartbeatPath).catch(() => null);
      supervisorPid = heartbeat?.supervisorPid ?? supervisorPid;
      agyPid = heartbeat?.agyPid ?? agyPid;
      supervisorAlive = supervisorPid === process.pid
        ? this.antigravityTasksByJob.has(jobId)
        : (supervisorPid ? isProcessAlive(supervisorPid) : false);
      agyAlive = agyPid ? isProcessAlive(agyPid) : false;
    } else {
      supervisorAlive = this.antigravityTasksByJob.has(jobId);
    }
    const stopped = !supervisorAlive && !agyAlive;
    const pidsChecked = [supervisorPid, agyPid].filter((p): p is number => typeof p === "number");
    const alivePids: number[] = [];
    if (supervisorAlive && supervisorPid && supervisorPid !== process.pid) alivePids.push(supervisorPid);
    if (agyAlive && agyPid) alivePids.push(agyPid);
    return {
      stopped,
      jobId,
      supervisorPid,
      agyPid,
      pidsChecked,
      alivePids,
      verifiedAt: new Date().toISOString(),
      ...(!stopped ? { error: "Antigravity process did not reach quiescence within " + maxWaitMs + "ms (supervisorPid: " + supervisorPid + ", agyPid: " + agyPid + ")" } : {}),
    };
  }

  private async ensureOpenCodeQuiescence(
    pid?: number | null,
    maxWaitMs = 5000,
  ): Promise<QuiescenceProof> {
    if (!pid || (this.managed?.processId && pid === this.managed.processId)) {
      return {
        stopped: true,
        workerPid: null,
        pidsChecked: [],
        alivePids: [],
        verifiedAt: new Date().toISOString(),
      };
    }
    const deadline = Date.now() + maxWaitMs;
    while (Date.now() < deadline) {
      if (!isProcessAlive(pid)) {
        return {
          stopped: true,
          workerPid: pid,
          pidsChecked: [pid],
          alivePids: [],
          verifiedAt: new Date().toISOString(),
        };
      }
      await new Promise((r) => setTimeout(r, 25));
    }
    const stopped = !isProcessAlive(pid);
    return {
      stopped,
      workerPid: pid,
      pidsChecked: [pid],
      alivePids: stopped ? [] : [pid],
      verifiedAt: new Date().toISOString(),
      ...(!stopped ? { error: "OpenCode worker process (PID " + pid + ") did not reach quiescence within " + maxWaitMs + "ms" } : {}),
    };
  }

  async park(input: ParkInput, isAlias = false, signal?: AbortSignal): Promise<ParkReceipt> {
    this.requireRunning();

    if (input.wait === true) {
      const nextAction = isAlias ? "deepseek_follow" : "subagents_follow";
      throw new InvalidRequestError(
        `wait=true is no longer supported for park. Use ${nextAction} for same-run in-turn waiting, or omit wait for external park_and_wake.`,
        "invalid_request",
      );
    }

    const rawJobIds = input.job_ids ?? input.jobIds;
    const jobIds = Array.isArray(rawJobIds)
      ? rawJobIds.filter((j): j is string => typeof j === "string" && j.length > 0)
      : (typeof input.job_id === "string" ? [input.job_id] : (typeof input.jobId === "string" ? [input.jobId] : []));

    if (jobIds.length === 0) {
      throw new InvalidRequestError("job_ids must contain at least one job id", "invalid_request");
    }

    const rawPred = (input.predicate ?? input.predicate_type ?? input.predicateType ?? "ALL").toUpperCase();
    if (!["ALL", "ANY", "QUORUM", "REQUIRED"].includes(rawPred)) {
      throw new InvalidRequestError(`Invalid predicate "${rawPred}". Supported predicates: ALL, ANY, QUORUM, REQUIRED.`, "invalid_request");
    }
    const predicateType = rawPred as ParkPredicateType;
    let quorumCount: number | null = null;
    if (predicateType === "QUORUM") {
      const k = input.quorum_count ?? input.quorumCount;
      if (typeof k !== "number" || !Number.isInteger(k) || k < 1 || k > jobIds.length) {
        throw new InvalidRequestError(`QUORUM predicate requires quorum_count between 1 and ${jobIds.length}, received ${k}`, "invalid_request");
      }
      quorumCount = k;
    }
    let requiredJobIds: string[] | null = null;
    if (predicateType === "REQUIRED") {
      const req = input.required_job_ids ?? input.requiredJobIds;
      if (!Array.isArray(req) || req.length === 0) {
        throw new InvalidRequestError(`REQUIRED predicate requires a non-empty required_job_ids array`, "invalid_request");
      }
      const jobSet = new Set(jobIds);
      for (const rId of req) {
        if (typeof rId !== "string" || !jobSet.has(rId)) {
          throw new InvalidRequestError(`REQUIRED job_id "${rId}" is not among the parked jobs`, "invalid_request");
        }
      }
      requiredJobIds = req;
    }
    const wakeOnException = input.wake_on_exception !== undefined
      ? Boolean(input.wake_on_exception)
      : (input.wakeOnException !== undefined ? Boolean(input.wakeOnException) : true);

    const jobs: JobRecord[] = [];
    for (const id of jobIds) {
      const job = this.store.getJob(id);
      if (!job) throw new UnknownJobError(id);
      jobs.push(job);
    }

    let commonThreadId: string | null = null;
    let commonTurnId: string | null = null;
    let commonMcpSessionId: string | null = null;
    let allCorrelated = true;

    for (const job of jobs) {
      if (job.mcpSessionId) {
        if (commonMcpSessionId === null) {
          commonMcpSessionId = job.mcpSessionId;
        } else if (commonMcpSessionId !== job.mcpSessionId) {
          throw new ConflictError(
            `Mixed session correlation rejected: job ${job.id} belongs to session ${job.mcpSessionId}, but preceding jobs belong to session ${commonMcpSessionId}. All jobs in a park set must belong to the same caller session.`,
            "mixed_session_conflict",
          );
        }
      }

      let binding = this.store.getBinding(job.id);
      if (!binding && this.transcriptAttestor) {
        const match = await this.transcriptAttestor.attestJob(job.id, {
          callerHint: {
            threadId: job.hintThreadId ?? job.trustedThreadId ?? undefined,
            turnId: job.hintTurnId ?? undefined,
          },
          jobCreatedAt: job.createdAt,
        });
        if (match) {
          binding = this.store.bindJob({
            jobId: job.id,
            threadId: match.threadId,
            originatingTurnId: match.turnId,
            originatingItemId: match.itemId,
          });
        }
      }

      const threadId = binding?.threadId ?? job.trustedThreadId ?? null;
      const turnId = binding?.originatingTurnId ?? null;
      if (!threadId) {
        allCorrelated = false;
        continue;
      }
      if (commonThreadId === null) {
        commonThreadId = threadId;
        commonTurnId = turnId;
      } else if (commonThreadId !== threadId) {
        throw new ConflictError(
          `Mixed thread correlation rejected: job ${job.id} belongs to thread ${threadId}, but preceding jobs belong to thread ${commonThreadId}. All jobs in a park set must belong to the same authoritative thread.`,
          "mixed_thread_conflict",
        );
      }
    }

    const callerThread = input.thread_id ?? input.threadId;
    if (callerThread && commonThreadId && callerThread !== commonThreadId) {
      throw new ConflictError(
        `Caller thread_id "${callerThread}" does not match trusted target identity "${commonThreadId}"`,
        "identity_mismatch",
      );
    }
    const trustedThreadInput = input.trusted_thread_id ?? input.trustedThreadId;
    if (trustedThreadInput && commonThreadId && trustedThreadInput !== commonThreadId) {
      throw new ConflictError(
        `Caller trusted_thread_id "${trustedThreadInput}" does not match trusted target identity "${commonThreadId}"`,
        "identity_mismatch",
      );
    }
    const callerTurn = input.turn_id ?? input.turnId;
    if (callerTurn && commonTurnId && callerTurn !== commonTurnId) {
      throw new ConflictError(
        `Caller turn_id "${callerTurn}" does not match trusted target identity "${commonTurnId}"`,
        "identity_mismatch",
      );
    }
    const callerMcpSession = (input.mcp_session_id ?? input.mcpSessionId)?.trim() || null;
    if (callerMcpSession && commonMcpSessionId && callerMcpSession !== commonMcpSessionId) {
      throw new ConflictError(
        `Caller mcp_session_id "${callerMcpSession}" does not match trusted session identity "${commonMcpSessionId}"`,
        "identity_mismatch",
      );
    }

    const hasAuthoritativeAttachment = this.codex.capabilities?.authoritativeAttachment === true;
    let cliCompatible = false;
    if (!hasAuthoritativeAttachment && allCorrelated && commonThreadId !== null && this.cliTransport) {
      try {
        const probe = await this.cliTransport.probeCapabilities(
          this.config.codexAppServerCommand || undefined,
        );
        cliCompatible = probe.compatible;
      } catch {
        cliCompatible = false;
      }
    }

    const armed = allCorrelated && commonThreadId !== null && (hasAuthoritativeAttachment || cliCompatible);
    const deliveryMode: "cli_resume" | "none" = armed ? "cli_resume" : "none";

    let parkId = input.park_id ?? input.parkId;
    let existingBarrier = parkId ? this.store.getParkBarrier(parkId) : null;
    if (!existingBarrier && commonThreadId) {
      existingBarrier = this.store.getParkBarrierByThread(commonThreadId);
    }
    if (existingBarrier) {
      parkId = existingBarrier.id;
    } else if (!parkId) {
      parkId = newId("park");
    }

    const generation = existingBarrier ? existingBarrier.generation + 1 : 1;
    const targetIdentity = commonThreadId ?? "unbound";

    if (existingBarrier) {
      const prevOutbox = this.store.getWakeOutbox(existingBarrier.id, existingBarrier.generation);
      if (prevOutbox && ["pending", "deferred_active_writer", "waking"].includes(prevOutbox.status)) {
        (this.store as any).updateWakeOutboxStatus(prevOutbox.id, "superseded", `Superseded by barrier generation ${generation}`, {
          wakeState: "failed",
        });
        const timer = this.wakeRetryTimers.get(prevOutbox.id);
        if (timer) {
          clearTimeout(timer);
          this.wakeRetryTimers.delete(prevOutbox.id);
        }
      }
    }

    const barrier = this.store.createOrUpdateParkBarrier({
      id: parkId,
      threadId: targetIdentity,
      turnId: commonTurnId,
      generation,
      armed,
      deliveryMode,
      state: armed ? "armed" : "idle",
      reason: input.reason ?? null,
      goalId: input.goal_id ?? input.goalId ?? null,
      mcpSessionId: input.mcp_session_id ?? input.mcpSessionId ?? null,
      predicateType,
      quorumCount,
      requiredJobIds,
      wakeOnException,
    });

    this.store.setParkJobs(barrier.id, jobIds);

    for (const job of jobs) {
      if (["dispatching", "running", "following", "finalizing"].includes(job.status)) {
        this.ensureFollowLifecycle(
          job,
          this.followWindowMinutes(undefined, 1, 60, this.config.followDefaultWaitMinutes),
          this.followWindowMinutes(undefined, 1, 10, this.config.followDefaultGraceMinutes),
          true,
        );
      }
    }

    const readyJobs = jobs.filter((j) => this.isJobWakeEligible(j));
    const readyJobIds = readyJobs.map((j) => j.id);
    const readyCount = readyJobs.length;
    const pendingCount = jobs.length - readyCount;
    const nextAction = isAlias ? ("deepseek_follow" as const) : ("subagents_follow" as const);

    if (armed) {
      for (const j of jobs) {
        this.scheduleJobInactivityTimer(j.id);
      }
      await this.evaluateParkWakesForBarrier(barrier).catch((err) => {
        this.lastStreamError = redactSecrets(String(err));
      });
    }

    return {
      parkId: barrier.id,
      generation: barrier.generation,
      armed,
      targetIdentity,
      deliveryMode,
      wakeState: "waiting",
      obligationState: "pending",
      nextAction,
      nextRequiredAction: nextAction,
      jobIds,
      readyJobIds,
      reason: input.reason ?? null,
      pendingCount,
      readyCount,
      predicateType,
      quorumCount,
      requiredJobIds,
    };
  }

  isJobStalled(job: JobRecord): boolean {
    if (TERMINAL_JOB_STATUSES.has(job.status) || job.status === "needs_approval") {
      return false;
    }

    const threshold = this.config.inactivityThresholdSeconds ?? 300;
    const lastProgressTime = parseTimestamp(job.lastProgressAt)
      ?? parseTimestamp(job.startedAt)
      ?? parseTimestamp(job.createdAt);
    const lastActivityAgoSeconds = lastProgressTime
      ? Math.max(0, Math.floor((Date.now() - lastProgressTime) / 1_000))
      : 0;

    if (job.escalationProposal) {
      try {
        const parsed = JSON.parse(job.escalationProposal) as EscalationProposal;
        // Structured typed current attempt/fence provenance:
        const attemptMismatch = Boolean(parsed.attempt && job.attempt && parsed.attempt !== job.attempt);
        const fenceMismatch = Boolean(parsed.fence !== undefined && parsed.fence !== null && job.fence !== undefined && job.fence !== null && parsed.fence !== job.fence);
        if (!attemptMismatch && !fenceMismatch) {
          const alertedTime = parseTimestamp(parsed.alertedAt);
          const hasNewProgress = Boolean(
            (lastProgressTime && alertedTime && lastProgressTime > alertedTime) ||
            (parsed.progressRevision !== undefined && parsed.progressRevision !== null && (job.progressRevision ?? 0) > parsed.progressRevision),
          );
          if (!hasNewProgress && lastActivityAgoSeconds > threshold) {
            return true;
          }
        }
      } catch {
        // Stale or unparseable proposal: do not treat as sticky stall
      }
    }

    if (lastActivityAgoSeconds > threshold) {
      const diagnosticEvidence = `Inactivity warning: job ${job.id} has no observable activity for ${lastActivityAgoSeconds}s (threshold: ${threshold}s, fence: ${job.fence ?? 1}, attempt: ${job.attempt ?? "none"}). Advisory only, NOT an execution timeout.`;
      const advisoryProposal: EscalationProposal = {
        reason: diagnosticEvidence,
        advisoryOnly: true,
        suggestedAction: "inspect_worker_process",
        diagnosticEvidence,
        attempt: job.attempt ?? null,
        fence: job.fence ?? null,
        progressRevision: job.progressRevision ?? 0,
        alertedAt: new Date().toISOString(),
      };
      try {
        this.store.setJobEscalation(job.id, JSON.stringify(advisoryProposal), job.fence ?? null);
      } catch {
        // Stale fence error caught! Attempt or fence changed under us.
        // Catch MUST NOT return eligible!
        return false;
      }
      return true;
    }
    return false;
  }

  isJobWakeEligible(job: JobRecord): boolean {
    if (job.status === "needs_approval") {
      return true;
    }
    if (["completed", "completed_partial", "delivered", "delivery_pending"].includes(job.status)) {
      return job.resultPath !== null;
    }
    if (["failed", "aborted", "timed_out"].includes(job.status)) {
      return true;
    }
    if (this.isJobStalled(job)) {
      return true;
    }
    return false;
  }

  async evaluateParkWakes(jobId: string): Promise<void> {
    const job = this.store.getJob(jobId);
    if (!job || !this.isJobWakeEligible(job)) return;
    const barriers = this.store.findArmedParksForJob(job.id);
    for (const barrier of barriers) {
      await this.evaluateParkWakesForBarrier(barrier);
    }
  }

  private async evaluateParkWakesForBarrier(barrier: ParkBarrierRecord): Promise<void> {
    if (!barrier.armed) return;

    const jobIds = this.store.getParkBarrierJobs(barrier.id);
    const jobs = jobIds.map((id) => this.store.getJob(id)).filter((j): j is JobRecord => j !== null);
    if (jobs.length === 0) return;

    const isSuccessfulTerminal = (j: JobRecord) =>
      ["completed", "completed_partial", "delivered", "delivery_pending"].includes(j.status) && j.resultPath !== null;

    const isException = (j: JobRecord) =>
      ["needs_approval", "failed", "aborted", "timed_out"].includes(j.status) || this.isJobStalled(j);

    const isTerminal = (j: JobRecord) =>
      ["completed", "completed_partial", "delivered", "delivery_pending", "failed", "aborted", "timed_out"].includes(j.status);

    const successfulJobs = jobs.filter(isSuccessfulTerminal);
    const successfulSet = new Set(successfulJobs.map((j) => j.id));
    const successfulCount = successfulJobs.length;

    // Check exception wake: by default (wakeOnException !== false), approval/failure wake immediately
    const hasException = barrier.wakeOnException !== false && jobs.some(isException);

    let satisfied = hasException;

    const allTerminal = jobs.length > 0 && jobs.every(isTerminal);

    const pred = barrier.predicateType ?? "ALL";
    if (!satisfied) {
      if (pred === "ALL") {
        satisfied = (successfulCount === jobs.length && jobs.length > 0) || allTerminal;
      } else if (pred === "ANY") {
        satisfied = successfulCount >= 1 || allTerminal;
      } else if (pred === "QUORUM") {
        const quorum = barrier.quorumCount ?? jobs.length;
        satisfied = successfulCount >= quorum || allTerminal;
      } else if (pred === "REQUIRED") {
        const required = barrier.requiredJobIds ?? [];
        satisfied = (required.length > 0 && required.every((id) => successfulSet.has(id))) || allTerminal;
      }
    }

    // If configured otherwise (wakeOnException === false), wake when quorum becomes impossible or all jobs are terminal so no deadlock
    if (!satisfied && barrier.wakeOnException === false) {
      const allTerminal = jobs.every(isTerminal);
      if (allTerminal) {
        satisfied = true;
      } else {
        const potentiallySuccessful = jobs.filter((j) => !isTerminal(j) || isSuccessfulTerminal(j)).length;
        if (pred === "QUORUM") {
          const quorum = barrier.quorumCount ?? jobs.length;
          if (potentiallySuccessful < quorum) {
            satisfied = true;
          }
        } else if (pred === "ALL") {
          const hasUnsuccessfulTerminal = jobs.some((j) => isTerminal(j) && !isSuccessfulTerminal(j));
          if (hasUnsuccessfulTerminal) {
            satisfied = true;
          }
        } else if (pred === "REQUIRED") {
          const required = barrier.requiredJobIds ?? [];
          const requiredFailed = required.some((reqId) => {
            const j = jobs.find((job) => job.id === reqId);
            return j && isTerminal(j) && !isSuccessfulTerminal(j);
          });
          if (requiredFailed) {
            satisfied = true;
          }
        }
      }
    }

    if (!satisfied) {
      return;
    }

    // Dedup across unchanged repark generations:
    // If barrier was reparked (generation > 1) and the only reason to wake is stalled jobs (no non-stall exceptions and not normally satisfied)
    if (barrier.generation > 1) {
      const hasNonStallException = jobs.some((j) => ["needs_approval", "failed", "aborted", "timed_out"].includes(j.status));
      let normallySatisfied = false;
      if (pred === "ALL") normallySatisfied = (successfulCount === jobs.length && jobs.length > 0) || allTerminal;
      else if (pred === "ANY") normallySatisfied = successfulCount >= 1 || allTerminal;
      else if (pred === "QUORUM") normallySatisfied = successfulCount >= (barrier.quorumCount ?? jobs.length) || allTerminal;
      else if (pred === "REQUIRED") normallySatisfied = ((barrier.requiredJobIds ?? []).length > 0 && (barrier.requiredJobIds ?? []).every((id) => successfulSet.has(id))) || allTerminal;

      if (!hasNonStallException && !normallySatisfied) {
        const priorOutboxes = this.store.listWakeOutboxesForPark(barrier.id).filter((o) => o.generation < barrier.generation);
        if (priorOutboxes.length > 0) {
          const stalledJobs = jobs.filter((j) => this.isJobStalled(j));
          const allStalledUnchanged = stalledJobs.length > 0 && stalledJobs.every((sj) => {
            const currentHash = createHash("sha256").update(sj.escalationProposal || sj.error || sj.permissionId || sj.status).digest("hex").slice(0, 16);
            return priorOutboxes.some((po) => {
              try {
                const pPayload = JSON.parse(po.payloadJson) as WakeEnvelope;
                return pPayload.readyJobIds.includes(sj.id) && pPayload.resultHashes?.[sj.id] === currentHash;
              } catch {
                return false;
              }
            });
          });
          if (allStalledUnchanged) {
            return;
          }
        }
      }
    }

    const existingOutbox = this.store.getWakeOutbox(barrier.id, barrier.generation);
    if (existingOutbox && (existingOutbox.status === "pending" || existingOutbox.status === "deferred_active_writer" || existingOutbox.status === "waking" || existingOutbox.status === "delivered")) {
      return;
    }

    const claimed = this.store.claimParkWake(barrier.id, barrier.generation);
    if (!claimed) return;

    const readyJobs = jobs.filter((j) => this.isJobWakeEligible(j));
    const readyJobIds = readyJobs.map((j) => j.id);
    const readyCount = readyJobs.length;
    const pendingCount = jobs.length - readyCount;

    const statuses: Record<string, string> = {};
    const resultHashes: Record<string, string> = {};

    for (const rj of readyJobs) {
      statuses[rj.id] = rj.status;
      if (rj.resultPath) {
        try {
          const content = await readFile(rj.resultPath, "utf8");
          resultHashes[rj.id] = createHash("sha256").update(content).digest("hex").slice(0, 16);
        } catch {
          resultHashes[rj.id] = createHash("sha256").update(rj.resultSummary || rj.id).digest("hex").slice(0, 16);
        }
      } else {
        resultHashes[rj.id] = createHash("sha256").update(rj.escalationProposal || rj.error || rj.permissionId || rj.status).digest("hex").slice(0, 16);
      }
    }

    const marker = `<!-- [SUBAGENT_BRIDGE_WAKE:park=${barrier.id}:gen=${barrier.generation}] -->`;
    const hasStalled = jobs.some((j) => this.isJobStalled(j));
    const envelope: WakeEnvelope = {
      parkId: barrier.id,
      generation: barrier.generation,
      reason: barrier.reason || (hasStalled ? "subagents park wake: worker inactivity exception" : "subagents park wake"),
      jobIds,
      readyJobIds,
      statuses,
      resultHashes,
      pendingCount,
      instruction: "Call subagents_follow to consume completed jobs. Do not interpret this message as worker output.",
      marker,
    };

    const outbox = this.store.createWakeOutbox({
      id: newId("wake"),
      parkId: barrier.id,
      generation: barrier.generation,
      threadId: barrier.threadId,
      turnId: barrier.turnId,
      deliveryMode: "cli_resume",
      status: "pending",
      wakeState: "waiting",
      wakeMarker: marker,
      reason: barrier.reason,
      payloadJson: JSON.stringify(envelope),
    });

    await this.dispatchWakeOutbox(outbox, envelope);
  }

  private async dispatchWakeOutbox(outbox: WakeOutboxRecord, envelope: WakeEnvelope): Promise<void> {
    if ((outbox as any).status === "superseded") {
      return;
    }
    // Fence against current park generation/state and stale outbox
    const barrier = this.store.getParkBarrier(outbox.parkId);
    if (!barrier || barrier.generation !== outbox.generation || barrier.state === "woken" || barrier.state === "idle" || barrier.state === "cancelled") {
      if (barrier && barrier.generation > outbox.generation && (outbox as any).status !== "superseded") {
        (this.store as any).updateWakeOutboxStatus(outbox.id, "superseded", `Superseded by barrier generation ${barrier.generation}`, {
          wakeState: "failed",
        });
      }
      return;
    }

    // Must deliver only after winning the atomic SQLite CAS claim
    const wonClaim = this.store.claimWakeOutbox(outbox.id);
    if (!wonClaim) {
      return;
    }

    if (barrier.state === "armed") {
      this.store.claimParkWake(barrier.id, barrier.generation);
    }

    const currentOutbox = this.store.getWakeOutboxById(outbox.id) ?? outbox;
    if ((currentOutbox as any).status === "superseded") {
      return;
    }

    const binding: CodexBinding = {
      jobId: envelope.jobIds[0] ?? "unbound",
      threadId: currentOutbox.threadId,
      originatingTurnId: currentOutbox.turnId,
      originatingItemId: null,
      boundAt: currentOutbox.createdAt,
    };

    try {
      if (this.codex.capabilities?.authoritativeAttachment === true) {
        await this.codex.deliverWake(envelope, binding);
        (this.store as any).updateWakeOutboxStatus(currentOutbox.id, "delivered", null, {
          wakeState: "delivered",
          deliveryMode: "in_turn",
        });
        this.store.setParkWoken(currentOutbox.parkId, currentOutbox.generation);
      } else {
        const cliResult = await this.cliTransport.deliverWake(currentOutbox.threadId, currentOutbox.wakeMarker);
        if (cliResult.success) {
          const deliveryMode = (cliResult as any).deliveryMode ?? "cli_resume";
          const messageId = (cliResult as any).messageId ?? null;
          (this.store as any).updateWakeOutboxStatus(currentOutbox.id, "delivered", null, {
            wakeState: "delivered",
            deliveryMode,
            messageId,
            selectedExecutable: cliResult.executablePath ?? null,
            executableVersion: cliResult.version ?? null,
            nextAttemptAt: null,
          });
          this.store.setParkWoken(currentOutbox.parkId, currentOutbox.generation);
        } else if (cliResult.activeWriter) {
          const attempts = currentOutbox.attempts;
          const baseDelayMs = 2000;
          const maxDelayMs = 300_000;
          const backoffMs = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempts - 1));
          const jitterMs = Math.floor(Math.random() * 1000);
          const totalDelay = backoffMs + jitterMs;
          const nextAttemptAt = new Date(Date.now() + totalDelay).toISOString();

          (this.store as any).updateWakeOutboxStatus(currentOutbox.id, "deferred_active_writer", cliResult.error ?? "Active writer conflict", {
            wakeState: "deferred_active_writer",
            deliveryMode: (cliResult as any).deliveryMode ?? currentOutbox.deliveryMode,
            nextAttemptAt,
            selectedExecutable: cliResult.executablePath ?? null,
            executableVersion: cliResult.version ?? null,
          });
          this.store.setParkArmed(currentOutbox.parkId, currentOutbox.generation);
          this.scheduleWakeRetry(currentOutbox.id, totalDelay);
        } else {
          const diagnostic = cliResult.error ?? "CLI wake delivery indeterminate";
          this.lastStreamError = diagnostic;
          (this.store as any).updateWakeOutboxStatus(currentOutbox.id, "waking", diagnostic, {
            wakeState: (currentOutbox as any).wakeState ?? "waiting",
            deliveryMode: (cliResult as any).deliveryMode ?? currentOutbox.deliveryMode,
            selectedExecutable: cliResult.executablePath ?? null,
            executableVersion: cliResult.version ?? null,
          });
        }
      }
    } catch (error: any) {
      const message = redactSecrets(String(error));
      const isActiveWriter = error?.code === "active_writer" ||
        message.toLowerCase().includes("active writer") ||
        message.toLowerCase().includes("thread-store conflict");

      if (isActiveWriter) {
        const attempts = currentOutbox.attempts;
        const baseDelayMs = 2000;
        const maxDelayMs = 300_000;
        const backoffMs = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempts - 1));
        const jitterMs = Math.floor(Math.random() * 1000);
        const totalDelay = backoffMs + jitterMs;
        const nextAttemptAt = new Date(Date.now() + totalDelay).toISOString();

        (this.store as any).updateWakeOutboxStatus(currentOutbox.id, "deferred_active_writer", message, {
          wakeState: "deferred_active_writer",
          deliveryMode: currentOutbox.deliveryMode,
          nextAttemptAt,
        });
        this.store.setParkArmed(currentOutbox.parkId, currentOutbox.generation);
        this.scheduleWakeRetry(currentOutbox.id, totalDelay);
        return;
      }

      const reconciled = await this.codex.reconcileSend(currentOutbox.threadId, currentOutbox.wakeMarker).catch(() => false);
      if (reconciled) {
        (this.store as any).updateWakeOutboxStatus(currentOutbox.id, "delivered", null, { wakeState: "delivered" });
        this.store.setParkWoken(currentOutbox.parkId, currentOutbox.generation);
      } else if (this.codex.capabilities?.authoritativeAttachment !== true) {
        this.lastStreamError = message;
        (this.store as any).updateWakeOutboxStatus(currentOutbox.id, "waking", message, {
          wakeState: (currentOutbox as any).wakeState ?? "waiting",
          deliveryMode: currentOutbox.deliveryMode,
        });
      } else {
        (this.store as any).updateWakeOutboxStatus(currentOutbox.id, "failed", message, { wakeState: "failed" });
      }
    }
  }

  private scheduleWakeRetry(outboxId: string, delayMs: number): void {
    if (!this.running) return;
    const existing = this.wakeRetryTimers.get(outboxId);
    if (existing) clearTimeout(existing);
    const timer = setTimeout(async () => {
      this.wakeRetryTimers.delete(outboxId);
      const outbox = this.store.getWakeOutboxById(outboxId);
      if (!outbox || outbox.status !== "deferred_active_writer") return;
      try {
        const envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
        await this.dispatchWakeOutbox(outbox, envelope);
      } catch {}
    }, delayMs);
    timer.unref?.();
    this.wakeRetryTimers.set(outboxId, timer);
  }

  private async recoverWakeOutbox(): Promise<void> {
    let pendingOutbox: WakeOutboxRecord[];
    if ((this.store as any).db?.prepare) {
      const rows = (this.store as any).db.prepare(`
        SELECT w.id FROM wake_outbox w
        JOIN park_barriers p ON p.id = w.park_id
        WHERE w.status IN ('pending', 'waking', 'deferred_active_writer')
          AND w.generation = p.generation
        ORDER BY w.created_at ASC
      `).all() as Array<{ id: string }>;
      pendingOutbox = rows
        .map((r) => this.store.getWakeOutboxById(r.id))
        .filter((o): o is WakeOutboxRecord => o !== null);
    } else {
      pendingOutbox = this.store.listPendingWakeOutbox();
    }

    for (const outbox of pendingOutbox) {
      if ((outbox as any).status === "superseded") {
        continue;
      }

      // Recovery must fence stale/superseded generation before dispatch and reconciliation
      const barrier = this.store.getParkBarrier(outbox.parkId);
      if (
        !barrier ||
        barrier.generation !== outbox.generation ||
        barrier.state === "woken" ||
        barrier.state === "idle" ||
        barrier.state === "cancelled"
      ) {
        if (barrier && barrier.generation > outbox.generation && (outbox as any).status !== "superseded") {
          (this.store as any).updateWakeOutboxStatus(outbox.id, "superseded", `Superseded by barrier generation ${barrier.generation}`, {
            wakeState: "failed",
          });
        }
        continue;
      }

      let envelope: WakeEnvelope;
      try {
        envelope = JSON.parse(outbox.payloadJson) as WakeEnvelope;
      } catch {
        continue;
      }

      // Integrate optional cliTransport.reconcileQueuedWake(threadId, wakeMarker)
      // during recoverWakeOutbox BEFORE transcript reconciliation/dispatch.
      let queuedDelivery: { messageId?: string | null; deliveryMode?: string } | null = null;
      if (typeof (this.cliTransport as any)?.reconcileQueuedWake === "function") {
        try {
          const raw = await (this.cliTransport as any).reconcileQueuedWake(outbox.threadId, outbox.wakeMarker);
          if (raw) {
            if (typeof raw === "object") {
              if (raw.found !== false && raw.success !== false && raw.matched !== false) {
                queuedDelivery = {
                  messageId: raw.messageId ?? raw.message_id ?? raw.id ?? null,
                  deliveryMode: raw.deliveryMode ?? "queued",
                };
              }
            } else if (raw === true) {
              queuedDelivery = { messageId: null, deliveryMode: "queued" };
            } else if (typeof raw === "string") {
              queuedDelivery = { messageId: raw, deliveryMode: "queued" };
            }
          }
        } catch {
          queuedDelivery = null;
        }
      }

      // If a waking outbox marker exists in Codex queue DB, mark delivered with deliveryMode queued/messageId and set park woken.
      if (queuedDelivery) {
        (this.store as any).updateWakeOutboxStatus(outbox.id, "delivered", null, {
          wakeState: "delivered",
          deliveryMode: (queuedDelivery.deliveryMode as any) ?? "queued",
          messageId: queuedDelivery.messageId ?? null,
        });
        this.store.setParkWoken(outbox.parkId, outbox.generation);
        continue;
      }

      // If transcript reconciliation succeeds, mark delivered.
      const reconciled = await this.codex.reconcileSend(outbox.threadId, outbox.wakeMarker).catch(() => false);
      if (reconciled) {
        (this.store as any).updateWakeOutboxStatus(outbox.id, "delivered", null, { wakeState: "delivered" });
        this.store.setParkWoken(outbox.parkId, outbox.generation);
        continue;
      }

      // If waking remains unproven, NEVER call dispatchWakeOutbox or reset/retry;
      // leave it indeterminate/fail-closed with a diagnostic so no duplicate queue can be created.
      if (outbox.status === "waking") {
        const diagnostic = `Indeterminate waking outbox marker ${outbox.wakeMarker} unproven during recovery; fail-closed to prevent duplicate queue`;
        this.lastStreamError = diagnostic;
        (this.store as any).updateWakeOutboxStatus(outbox.id, "waking", diagnostic, {
          wakeState: (outbox as any).wakeState ?? "waiting",
        });
        continue;
      }

      // Pending/deferred rows retain normal routing.
      if (outbox.status === "deferred_active_writer" && outbox.nextAttemptAt) {
        const waitRemaining = new Date(outbox.nextAttemptAt).getTime() - Date.now();
        if (waitRemaining > 0) {
          this.scheduleWakeRetry(outbox.id, waitRemaining);
          continue;
        }
      }
      await this.dispatchWakeOutbox(outbox, envelope);
    }
  }

  private async recoverArmedBarriers(): Promise<void> {
    const armedBarriers = this.store.listArmedBarriers();
    for (const barrier of armedBarriers) {
      await this.evaluateParkWakesForBarrier(barrier).catch((error) => {
        this.lastStreamError = redactSecrets(String(error));
      });
    }
  }

  async follow(input: FollowInput, signal?: AbortSignal): Promise<FollowResult> {
    this.requireRunning();
    const agent = this.store.getAgent(input.agentId);
    if (!agent) throw new UnknownAgentError(input.agentId);
    const job = this.resolveJobForAgent(agent.id, input.jobId);
    if (!job) throw new UnknownJobError("No DeepSeek job exists for agent " + agent.id);
    if (input.jobId && job.agentId !== agent.id) throw new InvalidRequestError("Job does not belong to the requested agent", "job_agent_mismatch");

    if (job.status === "needs_approval") {
      return this.followNeedsApproval(agent, job);
    }
    if (TERMINAL_JOB_STATUSES.has(job.status) || ["delivery_pending"].includes(job.status)) {
      const result = await this.followResultForJob(agent, job);
      if (["completed", "completed_partial", "timed_out", "failed", "aborted"].includes(result.status) && result.resultAvailable) {
        this.store.consumeResult(job.id);
      }
      return result;
    }
    if (!ACTIVE_JOB_STATUSES.has(job.status)) {
      throw new ConflictError("Job " + job.id + " is not followable in state " + job.status, "not_followable");
    }

    const lifecycle = this.ensureFollowLifecycle(
      job,
      this.followWindowMinutes(input.waitMinutes, 1, 60, this.config.followDefaultWaitMinutes),
      this.followWindowMinutes(input.graceMinutes, 1, 10, this.config.followDefaultGraceMinutes),
    );
    const waiter = Symbol("follow-waiter");
    lifecycle.waiters.add(waiter);
    try {
      const result = await waitWithAbort(lifecycle.promise, signal);
      if (["completed", "completed_partial", "timed_out", "failed", "aborted"].includes(result.status) && result.resultAvailable) {
        this.store.consumeResult(job.id);
      }
      return result;
    } finally {
      lifecycle.waiters.delete(waiter);
    }
  }

  async abort(agentId: string, reason?: string): Promise<{ agentId: string; jobId: string | null; status: string; proof?: QuiescenceProof; quiescent?: boolean }> {
    this.requireRunning();
    const agent = this.store.getAgent(agentId);
    if (!agent) throw new UnknownAgentError(agentId);
    // A "created" job is the pre-dispatch window (job created, dispatch not
    // reached yet). Aborting it must terminalize the job so the dispatch
    // guard never launches the provider for an aborted agent.
    const active = this.activeJob(agentId)
      ?? this.store.listJobs().find((job) => job.agentId === agentId && (job.status === "created" || job.status === "queued"))
      ?? null;
    if (!active) {
      // A stopped agent is non-continuable; auto-close it so the obligation
      // ends and the agent is not left in an open intermediate state.
      if (agent.status !== "closed" && agent.status !== "aborted") this.store.updateAgentStatus(agentId, "closed", reason ?? null);
      return { agentId, jobId: null, status: "aborted", quiescent: true };
    }
    if (active.status === "queued") {
      const localReason = reason ?? "Aborted by orchestrator";
      this.store.updateJobStatus(active.id, "aborted", localReason);
      if (agent.status !== "closed" && agent.status !== "aborted") this.store.updateAgentStatus(agentId, "closed", localReason);
      this.store.deleteDispatchEnvelope(active.id);
      this.pendingDispatches.delete(active.id);
      const waiter = this.dispatchWaiters.get(active.id);
      if (waiter) {
        this.dispatchWaiters.delete(active.id);
        waiter.reject(new Error(localReason));
      }
      if (this.followLifecycles.has(active.id)) {
        await this.resolveFollow(active.id, { status: "aborted", error: localReason, workerAborted: true });
      }
      this.onJobSettled(active.id, active.batchId);
      await this.evaluateParkWakes(active.id).catch((error: unknown) => {
        this.lastStreamError = redactSecrets(String(error));
      });
      return { agentId, jobId: active.id, status: "aborted", quiescent: true };
    }
    this.clearApprovalTimer(agentId);
    this.store.setApprovalDeadline(active.id, null);
    let remoteError: string | null = null;
    let proof: QuiescenceProof | undefined;
    const isAntigravityWithoutSession = agent.modelProviderId === "antigravity" && (!agent.opencodeSessionId || agent.opencodeSessionId.startsWith("antigravity:"));
    if (agent.modelProviderId === "antigravity") {
      const controller = this.antigravityAbortControllers.get(active.id);
      if (controller) {
        controller.abort();
        this.recordActivity(agent, active, "abort", "Sent abort signal to the active Antigravity process tree");
      } else if (active.status === "created") {
        this.recordActivity(agent, active, "abort", "Abort landed before Antigravity dispatch; the launch will be prevented");
      } else if (isAntigravityWithoutSession) {
        this.recordActivity(agent, active, "abort", "Antigravity job was aborted locally after its process was no longer controllable");
      }
      const spool = new AntigravitySpool(this.config.dataDir);
      await spool.writeCancelSignal(active.id, reason ?? "Aborted by orchestrator").catch(() => undefined);
      const qResult = await this.ensureAntigravityQuiescence(active.id, spool);
      if (!qResult.stopped) {
        const errorMsg = qResult.error ?? "Antigravity process failed to stop within deadline";
        this.recordActivity(agent, active, "error", errorMsg);
        throw new ConflictError(errorMsg, "state_conflict");
      }
      proof = qResult;
    }
    if (!isAntigravityWithoutSession) {
      try {
        await this.clientOrThrow().abort(agent.opencodeSessionId);
      } catch (error) {
        remoteError = redactSecrets(String(error));
      }
      if (remoteError) {
        this.recordActivity(agent, active, "error", "OpenCode abort failed: " + remoteError);
        throw new ConflictError("OpenCode abort failed: " + remoteError, "state_conflict");
      }
      const ephemeralWorkerPid = active.workerPid && active.workerPid !== this.managed?.processId
        ? active.workerPid
        : null;
      if (ephemeralWorkerPid && isProcessAlive(ephemeralWorkerPid)) {
        const qResult = await this.ensureOpenCodeQuiescence(ephemeralWorkerPid);
        if (!qResult.stopped) {
          const errorMsg = qResult.error ?? "OpenCode worker process failed to stop within deadline";
          this.recordActivity(agent, active, "error", errorMsg);
          throw new ConflictError(errorMsg, "state_conflict");
        }
        proof = qResult;
      } else {
        proof = {
          stopped: true,
          workerPid: null,
          pidsChecked: [],
          alivePids: [],
          verifiedAt: new Date().toISOString(),
        };
      }
    }
    const localReason = reason ?? "Aborted by orchestrator";

    if (active.status !== "aborted") this.store.updateJobStatus(active.id, "aborted", localReason);
    // Aborted agents are non-continuable; auto-close them safely.
    if (agent.status !== "closed" && agent.status !== "aborted") this.store.updateAgentStatus(agent.id, "closed", reason ?? null);
    this.recordActivity(agent, active, "abort", "Abort requested for the active DeepSeek task");
    this.store.deleteDispatchEnvelope(active.id);
    this.pendingDispatches.delete(active.id);
    const waiter = this.dispatchWaiters.get(active.id);
    if (waiter) {
      this.dispatchWaiters.delete(active.id);
      waiter.reject(new Error(localReason));
    }
    await this.resolveFollow(active.id, {
      status: "aborted",
      error: localReason,
      workerAborted: true,
    });
    this.onJobSettled(active.id, active.batchId);
    await this.evaluateParkWakes(active.id).catch((error: unknown) => {
      this.lastStreamError = redactSecrets(String(error));
    });
    return {
      agentId,
      jobId: active.id,
      status: "aborted",
      ...(proof !== undefined ? { proof } : {}),
      quiescent: true,
    };
  }

  async close(agentId: string): Promise<{ agentId: string; status: string; proof?: QuiescenceProof; quiescent: boolean }> {
    const agent = this.store.getAgent(agentId);
    if (!agent) throw new UnknownAgentError(agentId);
    const active = this.activeJob(agentId);
    let proof: QuiescenceProof | undefined;
    if (active) {
      const abortRes = await this.abort(agentId, "Closed by orchestrator");
      proof = abortRes.proof;
    }
    const jobs = this.store.listJobs().filter((j) => j.agentId === agentId);
    for (const job of jobs) {
      const ephemeralWorkerPid = job.workerPid && job.workerPid !== this.managed?.processId
        ? job.workerPid
        : null;
      if (ephemeralWorkerPid && isProcessAlive(ephemeralWorkerPid)) {
        const qResult = await this.ensureOpenCodeQuiescence(ephemeralWorkerPid);
        if (!qResult.stopped) {
          throw new ConflictError("Cannot close agent: process PID " + ephemeralWorkerPid + " is still alive", "state_conflict");
        }
        proof = qResult;
      }
      if (agent.modelProviderId === "antigravity" && this.antigravityTasksByJob.has(job.id)) {
        const spool = new AntigravitySpool(this.config.dataDir);
        const qResult = await this.ensureAntigravityQuiescence(job.id, spool);
        if (!qResult.stopped) {
          throw new ConflictError("Cannot close agent: antigravity process for job " + job.id + " is still alive", "state_conflict");
        }
        proof = qResult;
      }
    }
    if (!proof) {
      proof = {
        stopped: true,
        workerPid: null,
        pidsChecked: [],
        alivePids: [],
        verifiedAt: new Date().toISOString(),
      };
    }
    const refreshed = this.store.getAgent(agentId);
    if (refreshed && refreshed.status !== "closed") this.store.updateAgentStatus(agentId, "closed");
    return {
      agentId,
      status: "closed",
      ...(proof !== undefined ? { proof } : {}),
      quiescent: true,
    };
  }

  async recoverResult(jobId: string, agentId?: string): Promise<unknown> {
    let job = this.store.getJob(jobId);
    if (!job) throw new UnknownJobError(jobId);
    if (agentId && job.agentId !== agentId) throw new InvalidRequestError("Job does not belong to the requested agent", "job_agent_mismatch");
    if (!job.resultPath && this.client && ["dispatching", "running", "completed", "delivery_pending"].includes(job.status)) {
      await this.reconcileJob(job);
      job = this.store.getJob(jobId);
    }
    if (!job?.resultPath && job?.status === "timed_out" && this.client) {
      const agent = this.store.getAgent(job.agentId);
      if (agent) await this.captureTimedOutEvidence(agent, job);
      job = this.store.getJob(jobId);
    }
    if (!job?.resultPath) throw new NotFoundError("No persisted result is available for job " + jobId);
    const result = sanitizePersistedResult(JSON.parse(await readFile(job.resultPath, "utf8")), this.config.maxResultLength);
    // Recover returns a usable final result: the terminal obligation is
    // explicitly consumed here, separate from agent close.
    this.store.consumeResult(jobId);
    return result;
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
    if (!job) throw new UnknownJobError(jobId);
    if (!job.resultPath) throw new NotFoundError("Job has no persisted result");
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

  private followWindowMinutes(value: number | undefined, minimum: number, maximum: number, fallback: number): number {
    return Math.max(normalizeFollowMinutes(value, minimum, maximum, fallback), fallback);
  }

  /**
   * The effective active route name: the operator-set pointer persisted in the
   * bridge store when one exists, otherwise the configured default route. New
   * spawns always resolve through this pointer; it never affects existing
   * agents, whose dispatch identity comes from their persisted spawn columns.
   */
  private effectiveRouteName(): string {
    return this.store.getActiveRoute() ?? this.config.defaultModelRoute;
  }

  private effectiveRouteSource(): ActiveRouteSource {
    return this.store.getActiveRoute() === null ? "configured-default" : "operator-set";
  }

  /** Registry lookup that fails closed with typed 400 for unknown/disabled routes. */
  private resolveRouteByName(name: string): ResolvedRoute {
    const route = this.config.modelRoutes.find((candidate) => candidate.name === name);
    if (!route) {
      throw new InvalidRequestError("Unknown model route: " + name, "unknown_route", { route: name });
    }
    if (!route.enabled) {
      throw new InvalidRequestError("Model route is disabled: " + name, "route_disabled", { route: name });
    }
    return { name: route.name, providerId: route.providerId, modelId: route.modelId, variant: route.variant, display: route.display };
  }

  /** Non-throwing active-route resolution for health/status output. */
  private safeActiveRoute(): ResolvedRoute | null {
    try {
      return this.resolveRouteByName(this.effectiveRouteName());
    } catch {
      return null;
    }
  }

  listRoutes(): ModelRoute[] {
    return this.config.modelRoutes.map((route) => ({ ...route }));
  }

  routeStatus(): RouteStatusInfo {
    let activeRoute: ResolvedRoute | null = null;
    let activeRouteError: string | null = null;
    try {
      activeRoute = this.resolveRouteByName(this.effectiveRouteName());
    } catch (error) {
      activeRouteError = redactSecrets(String(error));
    }
    return {
      activeRoute,
      activeRouteError,
      defaultModelRoute: this.config.defaultModelRoute,
      source: this.effectiveRouteSource(),
      routes: this.listRoutes(),
    };
  }

  /**
   * Operator control plane: sets the active route pointer for NEW spawns.
   * The target must be registered AND enabled (typed 400 otherwise). The
   * pointer is persisted to the store BEFORE it becomes effective: the
   * effective route reads the persisted pointer, so persistence is the apply
   * and there is no separate in-memory copy to drift. Applies without any
   * daemon restart. Existing agents are never affected — they keep their
   * persisted spawn-time route.
   */
  setActiveRoute(name: string): RouteStatusInfo {
    this.resolveRouteByName(name);
    this.store.setActiveRoute(name);
    return this.routeStatus();
  }

  /**
   * Resolves the route for a new spawn. An explicit model_route must be
   * registered AND enabled, otherwise the dispatch fails closed with a typed
   * 400 before any side effect; there is no silent fallback route. Ordinary
   * callers may only name the route that is currently active: any other
   * registered, enabled route is denied with a typed route_override_denied
   * (403) — route changes are operator-only through the control plane. An
   * explicit route matching the active route is accepted for compatibility.
   * Without an explicit route the effective active route applies (and also
   * fails closed if it is disabled).
   */
  private resolveRouteForSpawn(modelRoute: string | undefined): ResolvedRoute {
    if (modelRoute !== undefined) {
      const route = this.resolveRouteByName(modelRoute);
      const activeName = this.effectiveRouteName();
      if (route.name !== activeName) {
        throw new RouteOverrideDeniedError(
          "Model route override denied: only the active route " + activeName + " is selectable at spawn; " + route.name + " is not active",
          { route: route.name, activeRoute: activeName },
        );
      }
      return route;
    }
    return this.resolveRouteByName(this.effectiveRouteName());
  }

  /**
   * Resolves the dispatch identity of an existing agent EXCLUSIVELY from the
   * persisted spawn-time columns (modelProviderId/modelId/modelVariant).
   * Continue, approval resume/reply, graceful finalization, recovery and
   * reconciliation always use this persisted identity, never the mutable live
   * config registry: changing, removing, disabling or repointing a config
   * route after spawn can never silently redirect an existing agent.
   * modelRoute is an immutable diagnostic label only. Agents created before
   * route pinning (model_route IS NULL) take the same persisted-columns path,
   * keeping legacy agents compatible.
   */
  private resolveAgentRoute(agent: AgentRecord): ResolvedRoute {
    const baseDisplay = staticModelDisplayName(agent.modelId, agent.modelVariant);
    return {
      name: agent.modelRoute ?? "legacy",
      providerId: agent.modelProviderId,
      modelId: agent.modelId,
      variant: agent.modelVariant,
      display: agent.modelProviderId === "antigravity"
        ? "Antigravity · " + baseDisplay
        : baseDisplay,
    };
  }

  private dispatchOptions(agent: AgentRecord, job?: JobRecord | null): { providerId: string; modelId: string; variant?: string; agent?: string } {
    if (job?.fallbackTo) {
      try {
        const fallbackRoute = this.resolveRouteByName(job.fallbackTo);
        return {
          providerId: fallbackRoute.providerId,
          modelId: fallbackRoute.modelId,
          ...(fallbackRoute.variant ? { variant: fallbackRoute.variant } : {}),
          ...(this.config.opencodeAgent ? { agent: this.config.opencodeAgent } : {}),
        };
      } catch {}
    }
    const route = this.resolveAgentRoute(agent);
    return {
      providerId: route.providerId,
      modelId: route.modelId,
      ...(route.variant ? { variant: route.variant } : {}),
      ...(this.config.opencodeAgent ? { agent: this.config.opencodeAgent } : {}),
    };
  }


  /**
   * Resolves candidate context files for a task, automatically including the
   * canonical global GEMINI.md governance context file when the task/topic
   * involves MCP, PromptPad, or AGENTS.md/GEMINI.md/skills governance,
   * preserving explicit files and deduplicating them in order.
   */
  private async resolveCandidateContextFiles(
    task: string,
    topic: string,
    explicitFiles: string[],
    globalGeminiPath: string,
  ): Promise<string[]> {
    const files = [...explicitFiles];
    if (shouldIncludeGlobalGeminiContext(task, topic)) {
      const resolvedGlobal = path.resolve(globalGeminiPath);
      const exists = await canRead(resolvedGlobal);
      if (exists) {
        const alreadyPresent = files.some((f) => isSamePath(f, resolvedGlobal));
        if (!alreadyPresent) {
          files.push(resolvedGlobal);
        }
      }
    }
    const seen = new Set<string>();
    const deduplicated: string[] = [];
    for (const f of files) {
      const normalized = path.resolve(f);
      const key = process.platform === "win32" ? normalized.toLowerCase() : normalized;
      if (!seen.has(key)) {
        seen.add(key);
        deduplicated.push(f);
      }
    }
    return deduplicated;
  }

  /**
   * Maps already-validated repository-root context paths into the would-be
   * worktree (the worktree mirrors the repository root, so the repo-relative
   * path is the worktree-relative path). This keeps worker-visible paths
   * inside the worktree (no main-repository path leakage) and rejects, with a
   * typed 400, files that would not exist inside the worktree (for example
   * untracked files under .deepseek-worktrees) before any side effect.
   * Explicitly allowlisted global files outside the repository are preserved
   * as their canonical external paths.
   */
  private mapContextIntoWorktree(
    workspacePath: string,
    repositoryRoot: string,
    files: string[],
    allowedExternalFiles: string[] = [],
  ): string[] {
    return files.map((file) => {
      if (allowedExternalFiles.some((allowed) => isSamePath(allowed, file))) {
        return file;
      }
      const relative = path.relative(repositoryRoot, file);
      if (relative === ".deepseek-worktrees" || relative.startsWith(".deepseek-worktrees" + path.sep)) {
        throw new InvalidRequestError(
          "context file inside .deepseek-worktrees is not available inside a worktree: " + file,
          "context_file_invalid",
          { file, reason: "inside_worktrees_directory" },
        );
      }
      const mapped = path.join(workspacePath, relative);
      try {
        return assertInside(workspacePath, mapped);
      } catch {
        throw new InvalidRequestError(
          "context file escapes the worktree: " + file,
          "context_file_invalid",
          { file, reason: "outside_workspace" },
        );
      }
    });
  }

  private effectiveWorkerTimeoutMs(waitMinutes?: number, graceMinutes?: number): number | null {
    if (this.config.workerMaxExecutionMinutes === null || this.config.workerMaxExecutionMinutes === undefined) {
      return null;
    }
    const wait = waitMinutes ?? this.config.followDefaultWaitMinutes;
    const grace = graceMinutes ?? this.config.followDefaultGraceMinutes;
    return Math.min(wait + grace, this.config.workerMaxExecutionMinutes) * 60_000;
  }

  private ensureFollowLifecycle(job: JobRecord, waitMinutes: number, graceMinutes: number, autoArmed = false): FollowLifecycle {
    const existing = this.followLifecycles.get(job.id);
    if (existing) {
      this.extendFollowLifecycle(job.id, existing, waitMinutes, graceMinutes);
      return existing;
    }
    const now = Date.now();
    const current = this.store.getJob(job.id) ?? job;
    if (current.status === "queued") {
      let resolve!: (result: FollowResult) => void;
      let reject!: (error: unknown) => void;
      const promise = new Promise<FollowResult>((resolvePromise, rejectPromise) => {
        resolve = resolvePromise;
        reject = rejectPromise;
      });
      const lifecycle: FollowLifecycle = {
        jobId: job.id,
        waitMinutes,
        graceMinutes,
        autoArmed,
        promise,
        resolve,
        reject,
        deadlineTimer: null,
        graceTimer: null,
        waiters: new Set(),
        settled: false,
      };
      this.followLifecycles.set(job.id, lifecycle);
      return lifecycle;
    }
    const startedAt = parseTimestamp(current.followStartedAt) ?? now;
    const isGracefulFinalization = current.status === "finalizing" || current.gracefulFinalizeAttempted;
    const requestedDeadlineAt = startedAt + waitMinutes * 60_000;
    const deadlineAt = isGracefulFinalization
      ? (parseTimestamp(current.followDeadlineAt) ?? requestedDeadlineAt)
      : Math.max(parseTimestamp(current.followDeadlineAt) ?? requestedDeadlineAt, requestedDeadlineAt);
    const effectiveGraceMinutes = isGracefulFinalization
      ? (current.followGraceMinutes ?? graceMinutes)
      : Math.max(current.followGraceMinutes ?? graceMinutes, graceMinutes);
    const graceDeadlineAt = parseTimestamp(current.graceDeadlineAt)
      ?? (isGracefulFinalization ? deadlineAt + effectiveGraceMinutes * 60_000 : null);
    if (["dispatching", "running"].includes(current.status)) {
      this.store.updateJobStatus(job.id, "following");
      const followAgent = this.store.getAgent(job.agentId);
      this.recordActivity(followAgent, this.store.getJob(job.id), "event", followAgent?.modelProviderId === "antigravity"
        ? "Follow mode started; waiting for the Antigravity run to complete"
        : "Follow mode started; waiting for an OpenCode completion event");
    }
    const after = this.store.setFollowWindow(job.id, {
      startedAt: new Date(startedAt).toISOString(),
      deadlineAt: new Date(deadlineAt).toISOString(),
      graceMinutes: effectiveGraceMinutes,
      graceDeadlineAt: graceDeadlineAt === null ? null : new Date(graceDeadlineAt).toISOString(),
      gracefulFinalizeAttempted: current.gracefulFinalizeAttempted,
    });
    const timeoutMs = this.effectiveWorkerTimeoutMs(waitMinutes, effectiveGraceMinutes);
    const leaseExpiresAt = timeoutMs !== null ? new Date(startedAt + timeoutMs).toISOString() : null;
    this.store.updateJobLiveness(job.id, { leaseExpiresAt });
    if (timeoutMs !== null) {
      const spool = new AntigravitySpool(this.config.dataDir);
      void spool.writeDeadlineExtension(job.id, timeoutMs).catch(() => undefined);
      if (typeof (this.antigravity as any)?.extendTimeout === "function") {
        (this.antigravity as any).extendTimeout(job.id, timeoutMs);
      }
    }
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
      autoArmed,
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
    } else if (this.config.workerMaxExecutionMinutes !== null && this.config.workerMaxExecutionMinutes !== undefined) {
      this.scheduleDeadlineTimer(lifecycle, deadlineAt);
    }
    return lifecycle;
  }

  private extendFollowLifecycle(jobId: string, lifecycle: FollowLifecycle, waitMinutes: number, graceMinutes: number): void {
    const job = this.store.getJob(jobId);
    if (!job || lifecycle.settled || TERMINAL_JOB_STATUSES.has(job.status) || job.status === "needs_approval") return;
    const startedAt = parseTimestamp(job.followStartedAt) ?? Date.now();
    const currentDeadlineAt = parseTimestamp(job.followDeadlineAt);
    if (currentDeadlineAt === null || currentDeadlineAt <= Date.now()) return;
    const requestedDeadlineAt = startedAt + waitMinutes * 60_000;
    const extendedDeadlineAt = Math.max(currentDeadlineAt, requestedDeadlineAt);
    const currentGrace = job.followGraceMinutes ?? lifecycle.graceMinutes;
    const extendedGrace = Math.max(currentGrace, graceMinutes);
    const deadlineExtended = extendedDeadlineAt > currentDeadlineAt;
    const graceExtended = extendedGrace > currentGrace;
    if (!deadlineExtended && !graceExtended) return;
    const finalizing = job.status === "finalizing";
    let graceDeadlineAt: string | null = job.graceDeadlineAt;
    if (finalizing && graceExtended) {
      const currentGraceDeadlineAt = parseTimestamp(job.graceDeadlineAt);
      const baseDeadlineAt = parseTimestamp(job.followDeadlineAt) ?? extendedDeadlineAt;
      if (currentGraceDeadlineAt !== null) {
        graceDeadlineAt = new Date(currentGraceDeadlineAt + (extendedGrace - currentGrace) * 60_000).toISOString();
      } else {
        graceDeadlineAt = new Date(baseDeadlineAt + extendedGrace * 60_000).toISOString();
      }
    }
    this.store.setFollowWindow(jobId, {
      startedAt: new Date(startedAt).toISOString(),
      deadlineAt: new Date(extendedDeadlineAt).toISOString(),
      graceMinutes: extendedGrace,
      graceDeadlineAt,
      gracefulFinalizeAttempted: job.gracefulFinalizeAttempted,
    });
    const timeoutMs = this.effectiveWorkerTimeoutMs(waitMinutes, extendedGrace);
    const extendedLeaseExpiresAt = timeoutMs !== null ? new Date(startedAt + timeoutMs).toISOString() : null;
    this.store.updateJobLiveness(jobId, { leaseExpiresAt: extendedLeaseExpiresAt });
    if (timeoutMs !== null) {
      const spool = new AntigravitySpool(this.config.dataDir);
      void spool.writeDeadlineExtension(jobId, timeoutMs).catch(() => undefined);
      if (typeof (this.antigravity as any)?.extendTimeout === "function") {
        (this.antigravity as any).extendTimeout(jobId, timeoutMs);
      }
    }
    lifecycle.graceMinutes = extendedGrace;
    if (finalizing) {
      if (graceExtended && graceDeadlineAt) {
        if (lifecycle.graceTimer) clearTimeout(lifecycle.graceTimer);
        lifecycle.graceTimer = null;
        this.scheduleGraceTimer(lifecycle, Date.parse(graceDeadlineAt));
      }
    } else if (deadlineExtended && ["dispatching", "running", "following"].includes(job.status)) {
      if (lifecycle.deadlineTimer) clearTimeout(lifecycle.deadlineTimer);
      lifecycle.deadlineTimer = null;
      if (this.config.workerMaxExecutionMinutes !== null && this.config.workerMaxExecutionMinutes !== undefined) {
        this.scheduleDeadlineTimer(lifecycle, extendedDeadlineAt);
      }
    }
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
    const agent = this.store.getAgent(job.agentId);
    if (!agent) return;
    const liveness = await this.getAuthoritativeStatus(agent.id, job.id);
    if (liveness.isLive) {
      return;
    }
    const graceDeadlineAt = Date.now() + lifecycle.graceMinutes * 60_000;
    this.store.updateJobStatus(jobId, "finalizing");
    const marked = this.store.markGracefulFinalize(jobId, new Date(graceDeadlineAt).toISOString());
    this.recordActivity(agent, marked, "deadline", "Follow deadline reached; starting graceful finalization");
    this.scheduleGraceTimer(lifecycle, graceDeadlineAt);
    await this.requestGracefulFinalize(agent, marked);
  }

  private async requestGracefulFinalize(agent: AgentRecord, job: JobRecord): Promise<void> {
    if (agent.modelProviderId === "antigravity" && (!agent.opencodeSessionId || agent.opencodeSessionId.startsWith("antigravity:"))) {
      this.recordActivity(agent, job, "finalize", "Antigravity has no OpenCode session to finalize; the follow deadline settles the job");
      return;
    }
    const client = this.clientOrThrow();
    try {
      await client.promptAsync(agent.opencodeSessionId, GRACEFUL_FINALIZE_PROMPT, this.dispatchOptions(agent, job));
      this.recordActivity(agent, job, "finalize", "Graceful finalization prompt submitted in the same OpenCode session");
    } catch (error) {
      if (isUnknownDispatchOutcome(error)) {
        this.recordActivity(agent, job, "error", "Graceful finalization dispatch outcome is unknown after a transport failure; the deadline will settle it");
        return;
      }
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
        await client.promptAsync(agent.opencodeSessionId, GRACEFUL_FINALIZE_PROMPT, this.dispatchOptions(agent, job));
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
    const liveness = await this.getAuthoritativeStatus(agent.id, job.id);
    if (liveness.isLive) {
      return;
    }
    if (!liveness.pid && !liveness.attempt) {
      if (agent.modelProviderId !== "antigravity") {
        await this.reconcileJob(job).catch(() => undefined);
      }
      return;
    }
    const reason = "Follow deadline and graceful-finalize grace period expired";
    this.store.updateJobStatus(job.id, "timed_out", reason);
    if (agent.status === "working") this.store.updateAgentStatus(agent.id, "timed_out", reason);
    const isAntigravityWithoutSession = agent.modelProviderId === "antigravity" && (!agent.opencodeSessionId || agent.opencodeSessionId.startsWith("antigravity:"));
    this.recordActivity(agent, this.store.getJob(job.id), "deadline", isAntigravityWithoutSession
      ? "Follow grace period expired; the Antigravity process will be aborted"
      : "Grace period expired; the worker will be aborted and partial evidence captured");
    let abortError: string | null = null;
    if (agent.modelProviderId === "antigravity") {
      const controller = this.antigravityAbortControllers.get(job.id);
      if (controller) {
        controller.abort();
        this.recordActivity(agent, this.store.getJob(job.id), "abort", "Sent abort signal to the Antigravity process tree after the follow grace period");
      } else if (isAntigravityWithoutSession) {
        this.recordActivity(agent, this.store.getJob(job.id), "error", "Antigravity process was no longer controllable at the follow grace expiry");
      }
    }
    if (!isAntigravityWithoutSession) {
      try {
        await this.clientOrThrow().abort(agent.opencodeSessionId);
      } catch (error) {
        abortError = redactSecrets(String(error));
        this.recordActivity(agent, this.store.getJob(job.id), "error", "Worker abort failed after the follow grace period");
      }
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
    } else {
      await this.evaluateParkWakes(job.id).catch((error: unknown) => {
        this.lastStreamError = redactSecrets(String(error));
      });
    }
  }

  private async captureTimedOutEvidence(agent: AgentRecord, job: JobRecord): Promise<Awaited<ReturnType<typeof persistResult>> | null> {
    if (agent.modelProviderId === "antigravity" && (!agent.opencodeSessionId || agent.opencodeSessionId.startsWith("antigravity:"))) {
      this.recordActivity(agent, job, "error", "Antigravity has no session messages to capture as timeout evidence");
      return null;
    }

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
    const followStatus = envelope?.status ?? mapJobToFollowStatus(job.status);
    return this.followResultForState(agent, job, {
      ...(envelope ? { envelope } : {}),
      status: followStatus,
      resultAvailable: envelope !== null,
      deadlineReached: Boolean(job.gracefulFinalizeAttempted || envelope?.deadlineReached || followStatus === "timed_out"),
      gracefulFinalize: Boolean(job.gracefulFinalizeAttempted || envelope?.gracefulFinalize),
      partial: Boolean(envelope?.partial || followStatus === "completed_partial" || followStatus === "timed_out"),
      workerAborted: Boolean(envelope?.workerAborted || followStatus === "timed_out"),
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
    const resultAvailable = overrides.resultAvailable ?? Boolean(job.resultPath || envelope);
    // Explicit consumption semantics: result_consumed_at is ONLY set by explicit
    // public follow/recover operations (never by background park readiness or
    // background follow lifecycle resolution). Needs_approval and non-terminal
    // states keep the obligation pending. Consumption is persisted and is
    // separate from closing the agent.
    const receipt = envelope?.receipt;
    const earlyExit = envelope?.earlyExit ?? progress.earlyExit;
    const escalation = envelope?.escalation ?? progress.escalation;
    const semanticProgress = progress.semanticProgress;
    return {
      agentId: agent.id,
      jobId: job.id,
      status,
      deadlineReached: overrides.deadlineReached ?? Boolean(job.gracefulFinalizeAttempted || envelope?.deadlineReached),
      gracefulFinalize: overrides.gracefulFinalize ?? Boolean(job.gracefulFinalizeAttempted || envelope?.gracefulFinalize),
      partial: overrides.partial ?? Boolean(envelope?.partial || status === "completed_partial" || status === "timed_out"),
      workerAborted: overrides.workerAborted ?? Boolean(envelope?.workerAborted || status === "timed_out"),
      resultAvailable,
      ...(envelope ? { result: { envelope } } : {}),
      progress,
      ...(failure ? { error: failure } : {}),
      ...(status === "needs_approval" ? { permissionId: overrides.permissionId ?? job.permissionId } : {}),
      ...(overrides.message ? { message: overrides.message } : {}),
      ...(receipt ? { receipt } : {}),
      ...(earlyExit ? { earlyExit } : {}),
      ...(escalation ? { escalation } : {}),
      ...(semanticProgress ? { semanticProgress } : {}),
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
    const job = this.store.getJob(jobId);
    this.onJobSettled(jobId, job?.batchId);
    const lifecycle = this.followLifecycles.get(jobId);
    if (!lifecycle || lifecycle.settled) return;
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
    const activities = this.store.listActivity(agent.id, limit, job?.id);
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

    let heartbeatAt: string | null = job?.heartbeatAt ?? null;
    let heartbeatAgoSeconds: number | null = null;
    let leaseExpiresAt: string | null = job?.leaseExpiresAt ?? job?.graceDeadlineAt ?? job?.followDeadlineAt ?? null;
    let attemptId: string | null = job?.attempt ?? null;
    let fence: number | null = job?.fence ?? 1;
    let pid: number | null = job?.workerPid ?? null;
    let sessionId: string | null = agent.opencodeSessionId ?? null;
    let resultPersisted = Boolean(job?.resultPath);
    let isLive = false;

    if (job) {
      if (agent.modelProviderId === "antigravity" || !agent.opencodeSessionId || agent.opencodeSessionId.startsWith("antigravity:")) {
        const spool = new AntigravitySpool(this.config.dataDir);
        const latestAttempt = await spool.getLatestAttempt(job.id).catch(() => null);
        if (latestAttempt) {
          attemptId = latestAttempt.attemptId;
          const heartbeat = await spool.readHeartbeat(latestAttempt.heartbeatPath).catch(() => null);
          if (heartbeat) {
            heartbeatAt = heartbeat.timestamp || (heartbeat.updatedAt ? new Date(heartbeat.updatedAt).toISOString() : null);
            if (heartbeat.updatedAt) {
              heartbeatAgoSeconds = Math.max(0, Math.floor((Date.now() - heartbeat.updatedAt) / 1000));
            }
            pid = heartbeat.supervisorPid || heartbeat.agyPid || null;
            const supervisorAlive = heartbeat.supervisorPid ? isProcessAlive(heartbeat.supervisorPid) : false;
            const agyAlive = heartbeat.agyPid ? isProcessAlive(heartbeat.agyPid) : false;
            isLive = supervisorAlive || agyAlive;
          }
          if (!leaseExpiresAt) {
            const effectiveTimeout = latestAttempt.timeoutMs ?? this.effectiveWorkerTimeoutMs();
            if (effectiveTimeout !== null) {
              const createdAtMs = parseTimestamp(latestAttempt.createdAt) ?? start;
              leaseExpiresAt = new Date(createdAtMs + effectiveTimeout).toISOString();
            }
          }
        }
      } else {
        pid = job.workerPid ?? null;
        heartbeatAt = job.heartbeatAt ?? null;
        if (heartbeatAt) {
          const hbMs = parseTimestamp(heartbeatAt);
          if (hbMs !== null) {
            heartbeatAgoSeconds = Math.max(0, Math.floor((Date.now() - hbMs) / 1000));
          }
        }
        if (pid) {
          isLive = isProcessAlive(pid);
        }
      }
    }

    const authoritativeStatus: AuthoritativeLivenessStatus = {
      heartbeatAt,
      heartbeatAgoSeconds,
      leaseExpiresAt,
      attempt: attemptId,
      fence,
      pid,
      sessionId,
      resultPersisted,
      isLive,
    };

    const earlyExit = envelope?.earlyExit ?? (job?.earlyExitAt && job?.earlyExitReason ? {
      triggered: true,
      reason: job.earlyExitReason,
      signaledAt: job.earlyExitAt,
    } : undefined);

    const threshold = this.config.inactivityThresholdSeconds ?? 300;
    const latestWorkerActivity = activities.find((a) => a.activityType !== "result");
    const lastProgressTime = parseTimestamp(job?.lastProgressAt)
      ?? (latestWorkerActivity ? parseTimestamp(latestWorkerActivity.createdAt) : null)
      ?? (parseTimestamp(job?.startedAt) ?? parseTimestamp(job?.createdAt));
    const lastActivityAgoSeconds = lastProgressTime
      ? Math.max(0, Math.floor((Date.now() - lastProgressTime) / 1_000))
      : null;

    const isJobInactivityStalled = job ? this.isJobStalled(job) : false;

    let diagnosticEvidence: string | null = null;
    let escalation: EscalationProposal | undefined = envelope?.escalation;

    if (job) {
      if (job.escalationProposal) {
        try {
          const parsed = JSON.parse(job.escalationProposal) as EscalationProposal;
          const attemptMismatch = Boolean(parsed.attempt && job.attempt && parsed.attempt !== job.attempt);
          const fenceMismatch = Boolean(parsed.fence !== undefined && parsed.fence !== null && job.fence !== undefined && job.fence !== null && parsed.fence !== job.fence);
          if (!attemptMismatch && !fenceMismatch) {
            diagnosticEvidence = parsed.diagnosticEvidence ?? parsed.reason ?? null;
            if (!escalation) {
              escalation = parsed;
            }
          }
        } catch {
          diagnosticEvidence = job.escalationProposal;
          if (!escalation) {
            escalation = { reason: job.escalationProposal, advisoryOnly: true as const };
          }
        }
      }
      if (!diagnosticEvidence && isJobInactivityStalled) {
        diagnosticEvidence = `Inactivity warning: job ${job.id} has no observable activity for ${lastActivityAgoSeconds ?? 0}s (threshold: ${threshold}s, fence: ${fence ?? 1}, attempt: ${attemptId ?? "none"}). Advisory only, NOT an execution timeout.`;
      }
      if (!escalation && isJobInactivityStalled && diagnosticEvidence) {
        escalation = {
          reason: diagnosticEvidence,
          advisoryOnly: true as const,
          suggestedAction: "inspect_worker_process",
          diagnosticEvidence,
          attempt: attemptId,
          fence,
        };
      }
    }

    const semanticProgress = deriveSemanticProgress(
      activities,
      job,
      isLive,
      heartbeatAt,
      heartbeatAgoSeconds,
      lastActivityAgoSeconds,
      earlyExit,
      diagnosticEvidence,
      threshold,
    );

    return {
      agentId: agent.id,
      jobId: job?.id ?? null,
      topic: agent.topic,
      status: job?.status ?? agent.status,
      elapsedSeconds: Math.max(0, Math.floor((end - start) / 1_000)),
      lastActivityAgoSeconds,
      currentActivity: latest?.summary ?? "No observable activity recorded.",
      recentActivity: activities.map((activity): ProgressActivity => ({
        type: activity.activityType,
        summary: activity.summary,
        timestamp: activity.createdAt,
      })),
      filesTouched: envelope?.files ?? [],
      testSummary: envelope?.tests?.join("; ") || "No test result observed yet.",
      resultAvailable: Boolean(job?.resultPath),
      heartbeatAt,
      heartbeatAgoSeconds,
      leaseExpiresAt,
      attempt: attemptId,
      fence,
      pid,
      sessionId,
      resultPersisted,
      authoritativeStatus,
      semanticProgress,
      ...(earlyExit ? { earlyExit } : {}),
      ...(escalation ? { escalation } : {}),
      ...(diagnosticEvidence ? { diagnosticEvidence } : {}),
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

  private async dispatch(
    agent: AgentRecord,
    job: JobRecord,
    prompt: string,
    workerInput?: WorkerPromptInput,
    contextFiles?: string[],
  ): Promise<AcceptedOperation> {
    if (agent.modelProviderId === "antigravity") {
      return this.dispatchAntigravity(agent, job, prompt, workerInput, contextFiles);
    }
    const currentJob = this.store.getJob(job.id);
    const currentAgent = this.store.getAgent(agent.id);
    if (currentJob?.status === "aborted" || currentAgent?.status === "closed" || currentAgent?.status === "aborted") {
      throw new Error("Invalid job transition: aborted -> dispatching");
    }
    this.store.updateAgentStatus(agent.id, "working");
    this.store.updateJobStatus(job.id, "dispatching");
    const timeoutMs = this.effectiveWorkerTimeoutMs();
    const leaseExpiresAt = timeoutMs !== null ? new Date(Date.now() + timeoutMs).toISOString() : null;
    const workerPid = null;
    this.store.updateJobLiveness(job.id, {
      leaseExpiresAt,
      attempt: "1",
      fence: job.fence ?? 1,
      workerPid,
      heartbeatAt: new Date().toISOString(),
    });
    if (job.kind === "continue" && !job.lastAssistantMessageId) {
      const baselineAssistantMessageId = this.previousAssistantMessageId(agent.id, job.id);
      if (baselineAssistantMessageId) this.store.setJobMessages(job.id, null, baselineAssistantMessageId);
    }
    try {
      await this.clientOrThrow().promptAsync(jobAgentSession(agent), prompt, this.dispatchOptions(agent, job));
      const current = this.store.getJob(job.id);
      if (current?.status === "dispatching") this.store.updateJobStatus(job.id, "running");
      this.recordActivity(agent, job, "dispatch", "Dispatched task to the OpenCode session");
      return this.accepted(this.store.getJob(job.id) ?? job);
    } catch (error) {
      const message = redactSecrets(String(error instanceof Error ? error.message : error));
      if (isUnknownDispatchOutcome(error)) {
        // The prompt may still have been accepted server-side. Do not mark the
        // job or agent failed and do not throw: resolve normally as an
        // accepted pending bridge obligation so the caller receives the exact
        // agentId/jobId it must follow or abort. Persist the diagnostic,
        // keep the job active (which blocks a duplicate continuation), and arm
        // the existing follow deadline/grace so a prompt that was never
        // accepted cannot become immortal.
        const currentJob = this.store.getJob(job.id);
        if (currentJob?.status === "dispatching") this.store.updateJobStatus(job.id, "running");
        this.recordActivity(agent, this.store.getJob(job.id), "error", "OpenCode dispatch outcome is unknown after a transport failure; the job stays active");
        const pending = this.store.getJob(job.id) ?? job;
        this.ensureFollowLifecycle(
          pending,
          this.followWindowMinutes(undefined, 1, 60, this.config.followDefaultWaitMinutes),
          this.followWindowMinutes(undefined, 1, 10, this.config.followDefaultGraceMinutes),
          true,
        );
        this.store.setJobError(job.id, "Dispatch outcome unknown after a transport failure: " + message);
        this.store.markDispatchUnknown(job.id);
        return this.accepted(pending, { outcome: "dispatch_unknown", warning: DISPATCH_UNKNOWN_WARNING });
      }
      const current = this.store.getJob(job.id);
      const currentAgent = this.store.getAgent(agent.id);
      const preservesApproval = current?.status === "needs_approval" || currentAgent?.status === "needs_approval";
      if (current && current.status !== "failed" && !preservesApproval) this.store.updateJobStatus(job.id, "failed", message);
      if (currentAgent && currentAgent.status !== "closed" && !preservesApproval) this.store.updateAgentStatus(agent.id, "failed", message);
      this.recordActivity(agent, job, "error", "OpenCode rejected the task dispatch");
      if (!preservesApproval) {
        this.onJobSettled(job.id, job.batchId);
        if (this.followLifecycles.has(job.id)) {
          await this.resolveFollow(job.id, { status: "failed", error: message });
        }
        await this.evaluateParkWakes(job.id).catch((err: unknown) => {
          this.lastStreamError = redactSecrets(String(err));
        });
      }
      const backpressure = isBackpressureError(error);
      if (backpressure && !(error as any)?.backpressureRecorded) {
        this.recordBackpressure("bridge_busy");
      }
      const propagatedError = error instanceof Error ? error : new Error(message);
      if (backpressure) {
        (propagatedError as any).backpressureRecorded = true;
      }
      throw propagatedError;
    }
  }

  /**
   * Antigravity dispatch path: preserves the MCP asynchronous contract. The
   * spawn resolves with an accepted pending obligation immediately after job
   * creation; the agy executable runs once via the AntigravityAdapter (never
   * OpenCode, never a session) in a background task, and completion/result
   * persistence/delivery happen asynchronously so deepseek_follow observes
   * them.
   *
   * Pre-dispatch abort race: an abort that lands between job creation and
   * this point (job still "created", agent already closed/aborted) must
   * prevent the agy launch entirely — the abort path marks the job aborted
   * and closes the agent, and the guard below detects that terminal state
   * before launching, so a closed/aborted agent is never transitioned back
   * to working and no result is ever delivered for an aborted job.
   */
  private dispatchAntigravity(
    agent: AgentRecord,
    job: JobRecord,
    prompt: string,
    workerInput?: WorkerPromptInput,
    contextFiles?: string[],
  ): AcceptedOperation {
    const controller = new AbortController();
    this.antigravityAbortControllers.set(job.id, controller);
    const currentJob = this.store.getJob(job.id);
    const currentAgent = this.store.getAgent(agent.id);
    if (currentJob?.status === "aborted" || currentAgent?.status === "closed" || currentAgent?.status === "aborted" || controller.signal.aborted) {
      this.antigravityAbortControllers.delete(job.id);
      this.recordActivity(currentAgent ?? agent, currentJob ?? job, "abort", "Antigravity launch prevented: the agent was aborted before dispatch started");
      return this.accepted(currentJob ?? job);
    }
    const adapterTimeout = (this.antigravity as any)?.timeoutMs;
    const timeoutMs: number | null = (typeof adapterTimeout === "number" && adapterTimeout !== AGY_DEFAULT_TIMEOUT_MS)
      ? adapterTimeout
      : this.effectiveWorkerTimeoutMs();
    const leaseExpiresAt = timeoutMs !== null ? new Date(Date.now() + timeoutMs).toISOString() : null;
    const capturedFence = job.fence ?? 1;
    this.store.updateJobLiveness(job.id, {
      leaseExpiresAt,
      attempt: null,
      fence: capturedFence,
      workerPid: null,
      heartbeatAt: new Date().toISOString(),
    });
    this.store.updateAgentStatus(agent.id, "working");
    this.store.updateJobStatus(job.id, "dispatching");
    this.store.updateJobStatus(job.id, "running");
    const task = this.runAntigravityAsync(agent, job, prompt, controller, timeoutMs, workerInput, contextFiles, capturedFence)
      .catch((error: unknown) => {
        if (this.running) this.lastStreamError = redactSecrets(String(error));
      })
      .finally(() => {
        this.antigravityTasks.delete(task);
        this.antigravityTasksByJob.delete(job.id);
      });
    this.antigravityTasks.add(task);
    this.antigravityTasksByJob.set(job.id, task);
    return this.accepted(this.store.getJob(job.id) ?? job);
  }

  private async runAntigravityAttemptAsync(
    agent: AgentRecord,
    job: JobRecord,
    manifest: AntigravityAttemptManifest,
    controller: AbortController,
    workerInput?: WorkerPromptInput,
    contextFiles?: string[],
  ): Promise<void> {
    const spool = new AntigravitySpool(this.config.dataDir);
    const capturedFence = manifest.fence ?? job.fence ?? 1;
    try {
      const onHeartbeat = (hb: AntigravityHeartbeat) => {
        if (!this.running && this.lifecycleState !== "recovering") return;
        try {
          this.store.updateJobLiveness(job.id, {
            attempt: manifest.attemptId,
            workerPid: hb.agyPid ?? hb.supervisorPid ?? null,
            heartbeatAt: hb.timestamp,
            fence: capturedFence,
          });
        } catch {}
      };
      const onProgress = (prog: AntigravityStreamProgress) => {
        if (!this.running && this.lifecycleState !== "recovering") return;
        try {
          this.metrics.sqliteProgressUpdates++;
          this.store.updateJobProgress(job.id, {
            lastProgressAt: prog.lastProgressAt,
            progressRevision: prog.progressRevision,
            fence: capturedFence,
            attempt: manifest.attemptId,
          });
          this.scheduleJobInactivityTimer(job.id);
        } catch {}
      };
      let result: AntigravityRunResult;
      if (typeof (this.antigravity as any).runAttempt === "function") {
        result = await (this.antigravity as any).runAttempt(manifest, controller.signal, onHeartbeat, onProgress);
      } else {
        result = await this.antigravity.runPrompt({
          prompt: manifest.promptPath && existsSync(manifest.promptPath) ? await readFile(manifest.promptPath, "utf8").catch(() => "") : "",
          cwd: manifest.cwd,
          model: manifest.modelId,
          signal: controller.signal,
          attemptManifest: manifest,
          onHeartbeat,
          onProgress,
        });
      }
      if (!this.running && this.lifecycleState !== "recovering") return;
      const current = this.store.getJob(job.id);
      if (controller.signal.aborted || current?.status === "aborted") {
        this.recordActivity(agent, current ?? job, "abort", "Antigravity process ended after the bridge abort signal");
        return;
      }
      if (current?.status === "timed_out") {
        this.recordActivity(agent, current, "abort", "Antigravity process ended after the follow timeout; the timed-out job stays terminal");
        return;
      }
      const stored = await persistAntigravityResult(this.config.dataDir, agent, job, result, this.config.maxResultLength);
      try {
        this.store.setJobResult(job.id, stored.resultPath, stored.envelope.summary, capturedFence);
        if (stored.envelope.earlyExit?.triggered) {
          this.store.setJobEarlyExit(job.id, {
            earlyExitAt: stored.envelope.earlyExit.signaledAt || new Date().toISOString(),
            reason: stored.envelope.earlyExit.reason,
          }, capturedFence);
        }
        if (stored.envelope.escalation) {
          this.store.setJobEscalation(job.id, JSON.stringify(stored.envelope.escalation), capturedFence);
        }
        const completed = this.store.getJob(job.id) ?? job;
        if (["running", "following", "finalizing"].includes(completed.status)) {
          this.store.updateJobStatus(job.id, "completed", null, capturedFence);
        }
        const completedAgent = this.store.getAgent(agent.id) ?? agent;
        if (completedAgent.status === "working") this.store.updateAgentStatus(agent.id, "completed");
        this.recordActivity(agent, this.store.getJob(job.id), "result", "Antigravity run completed and the result was persisted");
        this.recordTerminalSuccess(job.id);
        this.onJobSettled(job.id, job.batchId);
        if (this.followLifecycles.has(job.id)) {
          await this.resolveFollow(job.id, {
            status: "completed",
            resultAvailable: true,
            envelope: stored.envelope,
          });
        }
        const pending = this.store.getJob(job.id) ?? job;
        if (["completed", "completed_partial"].includes(pending.status)) this.store.updateJobStatus(job.id, "delivery_pending", null, capturedFence);
        const deliveryJob = this.store.getJob(job.id) ?? job;
        await this.deliverEnvelope(stored.envelope, deliveryJob);
        await spool.cleanupPrompt(manifest.attemptId, job.id);
      } catch (err) {
        if (err instanceof ConflictError) {
          this.recordActivity(agent, this.store.getJob(job.id) ?? job, "error", "Stale attempt completion rejected by fence check: " + err.message);
          return;
        }
        throw err;
      }
    } catch (error) {
      const message = redactSecrets(String(error));
      let current: JobRecord | null = null;
      try {
        current = this.store.getJob(job.id);
      } catch {}
      if (current?.status === "aborted" || controller.signal.aborted) {
        try {
          this.recordActivity(agent, current ?? job, "abort", "Antigravity process ended after the bridge abort signal");
        } catch {}
        return;
      }
      if (!this.running && this.lifecycleState !== "recovering") return;
      if (current?.resultPath || ["completed", "completed_partial", "delivery_pending", "delivered"].includes(current?.status ?? "")) {
        this.recordActivity(agent, current ?? job, "error", "Delivery failed for persisted Antigravity result: " + message);
        this.lastStreamError = message;
        return;
      }
      if (this.isEligibleForTimeoutFallback(agent, current ?? job, error)) {
        await this.executeTimeoutFallback(agent, current ?? job, error as AntigravityProcessError, controller, workerInput, contextFiles);
        return;
      }
      if (current && current.status !== "failed") {
        try {
          this.store.updateJobStatus(job.id, "failed", message, capturedFence);
        } catch (err) {
          if (err instanceof ConflictError) {
            this.recordActivity(agent, this.store.getJob(job.id) ?? job, "error", "Stale attempt failure rejected by fence check: " + err.message);
            return;
          }
          throw err;
        }
      }
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, "failed", message);
      this.recordActivity(agent, job, "error", "Antigravity rejected the task dispatch: " + message);
      if (isBackpressureError(error) && !(error as any)?.backpressureRecorded) {
        this.recordBackpressure("bridge_busy");
        (error as any).backpressureRecorded = true;
      }
      this.onJobSettled(job.id, job.batchId);
      if (this.followLifecycles.has(job.id)) {
        await this.resolveFollow(job.id, { status: "failed", error: message });
      }
      await this.evaluateParkWakes(job.id).catch((err: unknown) => {
        this.lastStreamError = redactSecrets(String(err));
      });
    } finally {
      this.clearJobInactivityTimer(job.id);
      if (this.antigravityAbortControllers.get(job.id) === controller) this.antigravityAbortControllers.delete(job.id);
    }
  }

  /**
   * Background Antigravity execution: awaits agy, persists the literal result
   * envelope, completes the job, delivers it and settles any waiting follow.
   * Runs fire-and-forget after spawn acceptance; never throws to the caller.
   */
  private async runAntigravityAsync(
    agent: AgentRecord,
    job: JobRecord,
    prompt: string,
    controller: AbortController,
    timeoutMs?: number | null,
    workerInput?: WorkerPromptInput,
    contextFiles?: string[],
    fence?: number,
  ): Promise<void> {
    const capturedFence = fence ?? job.fence ?? 1;
    try {
      const adapterTimeout = (this.antigravity as any)?.timeoutMs;
      const effectiveTimeoutMs: number | null = timeoutMs ?? (
        (typeof adapterTimeout === "number" && adapterTimeout !== AGY_DEFAULT_TIMEOUT_MS)
          ? adapterTimeout
          : this.effectiveWorkerTimeoutMs()
      );
      const onHeartbeat = (hb: AntigravityHeartbeat) => {
        if (!this.running && this.lifecycleState !== "recovering") return;
        try {
          this.store.updateJobLiveness(job.id, {
            attempt: hb.attemptId ?? null,
            workerPid: hb.agyPid ?? hb.supervisorPid ?? null,
            heartbeatAt: hb.timestamp,
            fence: capturedFence,
          });
        } catch {}
      };
      const onProgress = (prog: AntigravityStreamProgress) => {
        if (!this.running && this.lifecycleState !== "recovering") return;
        try {
          this.metrics.sqliteProgressUpdates++;
          this.store.updateJobProgress(job.id, {
            lastProgressAt: prog.lastProgressAt,
            progressRevision: prog.progressRevision,
            fence: capturedFence,
            attempt: prog.attemptId ?? null,
          });
          this.scheduleJobInactivityTimer(job.id);
        } catch {}
      };
      const result: AntigravityRunResult = await this.antigravity.runPrompt({
        prompt,
        cwd: agent.workspacePath,
        model: agent.modelId,
        signal: controller.signal,
        dataDir: this.config.dataDir,
        agentId: agent.id,
        jobId: job.id,
        requestId: job.requestId,
        timeoutMs: effectiveTimeoutMs,
        fence: capturedFence,
        onHeartbeat,
        onProgress,
      });
      if (!this.running && this.lifecycleState !== "recovering") return;
      const current = this.store.getJob(job.id);
      if (controller.signal.aborted || current?.status === "aborted") {
        this.recordActivity(agent, current ?? job, "abort", "Antigravity process ended after the bridge abort signal");
        return;
      }
      if (current?.status === "timed_out") {
        this.recordActivity(agent, current, "abort", "Antigravity process ended after the follow timeout; the timed-out job stays terminal");
        return;
      }
      const stored = await persistAntigravityResult(this.config.dataDir, agent, job, result, this.config.maxResultLength);
      try {
        this.store.setJobResult(job.id, stored.resultPath, stored.envelope.summary, capturedFence);
        if (stored.envelope.earlyExit?.triggered) {
          this.store.setJobEarlyExit(job.id, {
            earlyExitAt: stored.envelope.earlyExit.signaledAt || new Date().toISOString(),
            reason: stored.envelope.earlyExit.reason,
          }, capturedFence);
        }
        if (stored.envelope.escalation) {
          this.store.setJobEscalation(job.id, JSON.stringify(stored.envelope.escalation), capturedFence);
        }
        const completed = this.store.getJob(job.id) ?? job;
        if (["running", "following", "finalizing"].includes(completed.status)) {
          this.store.updateJobStatus(job.id, "completed", null, capturedFence);
        }
        const completedAgent = this.store.getAgent(agent.id) ?? agent;
        if (completedAgent.status === "working") this.store.updateAgentStatus(agent.id, "completed");
        this.recordActivity(agent, this.store.getJob(job.id), "result", "Antigravity run completed and the result was persisted");
        this.recordTerminalSuccess(job.id);
        this.onJobSettled(job.id, job.batchId);
        if (this.followLifecycles.has(job.id)) {
          await this.resolveFollow(job.id, {
            status: "completed",
            resultAvailable: true,
            envelope: stored.envelope,
          });
        }
        const pending = this.store.getJob(job.id) ?? job;
        if (["completed", "completed_partial"].includes(pending.status)) this.store.updateJobStatus(job.id, "delivery_pending", null, capturedFence);
        const deliveryJob = this.store.getJob(job.id) ?? job;
        await this.deliverEnvelope(stored.envelope, deliveryJob);
        const spool = new AntigravitySpool(this.config.dataDir);
        const attempts = await spool.listAttempts(job.id);
        if (attempts.length > 0) {
          await spool.cleanupPrompt(attempts[attempts.length - 1]!.attemptId, job.id);
        }
      } catch (err) {
        if (err instanceof ConflictError) {
          this.recordActivity(agent, this.store.getJob(job.id) ?? job, "error", "Stale attempt completion rejected by fence check: " + err.message);
          return;
        }
        throw err;
      }
    } catch (error) {
      const message = redactSecrets(String(error));
      let current: JobRecord | null = null;
      try {
        current = this.store.getJob(job.id);
      } catch {}
      // A cancellation is never a rejected dispatch: when the abort signal is
      // set, classify the outcome as an abort even if the job row has not been
      // marked aborted yet (abort() marks it immediately afterwards; a stop()
      // leaves the stranded job to startup recovery). The job/agent failure
      // guards below stay untouched for genuine dispatch rejections.
      if (current?.status === "aborted" || controller.signal.aborted) {
        try {
          this.recordActivity(agent, current ?? job, "abort", "Antigravity process ended after the bridge abort signal");
        } catch {}
        return;
      }
      if (!this.running && this.lifecycleState !== "recovering") return;
      // If the result was already persisted or completed, never downgrade to failed.
      if (current?.resultPath || ["completed", "completed_partial", "delivery_pending", "delivered"].includes(current?.status ?? "")) {
        this.recordActivity(agent, current ?? job, "error", "Delivery failed for persisted Antigravity result: " + message);
        this.lastStreamError = message;
        return;
      }
      if (this.isEligibleForTimeoutFallback(agent, current ?? job, error)) {
        await this.executeTimeoutFallback(agent, current ?? job, error as AntigravityProcessError, controller, workerInput, contextFiles);
        return;
      }
      if (current && current.status !== "failed") {
        try {
          this.store.updateJobStatus(job.id, "failed", message, capturedFence);
        } catch (err) {
          if (err instanceof ConflictError) {
            this.recordActivity(agent, this.store.getJob(job.id) ?? job, "error", "Stale attempt failure rejected by fence check: " + err.message);
            return;
          }
          throw err;
        }
      }
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, "failed", message);
      this.recordActivity(agent, job, "error", "Antigravity rejected the task dispatch: " + message);
      if (isBackpressureError(error) && !(error as any)?.backpressureRecorded) {
        this.recordBackpressure("bridge_busy");
        (error as any).backpressureRecorded = true;
      }
      this.onJobSettled(job.id, job.batchId);
      if (this.followLifecycles.has(job.id)) {
        await this.resolveFollow(job.id, { status: "failed", error: message });
      }
      await this.evaluateParkWakes(job.id).catch((err: unknown) => {
        this.lastStreamError = redactSecrets(String(err));
      });
    } finally {
      this.clearJobInactivityTimer(job.id);
      if (this.antigravityAbortControllers.get(job.id) === controller) this.antigravityAbortControllers.delete(job.id);
    }
  }

  private isEligibleForTimeoutFallback(_agent: AgentRecord, _job: JobRecord, _error: unknown): boolean {
    return false;
  }


  private async executeTimeoutFallback(
    agent: AgentRecord,
    job: JobRecord,
    error: AntigravityProcessError,
    controller: AbortController,
    workerInput?: WorkerPromptInput,
    contextFiles?: string[],
  ): Promise<void> {
    if (controller.signal.aborted || this.store.getJob(job.id)?.status === "aborted") return;
    const fallbackRoute = this.resolveRouteByName(this.config.antigravityTimeoutFallbackRoute!);
    this.store.setJobFallback(job.id, {
      from: agent.modelRoute ?? agent.modelProviderId,
      to: fallbackRoute.name,
      reason: error.message,
      status: "attempted",
      count: 1,
    });
    this.recordActivity(
      agent,
      job,
      "dispatch",
      "Antigravity process timed out; executing fallback to OpenCode route " + fallbackRoute.name + " (" + fallbackRoute.display + ")",
    );

    const allowedExternalFiles = [path.resolve(this.config.globalGeminiContextPath)];
    const promptOptions = workerPromptOptions(this.config, false, allowedExternalFiles);
    const opencodePrompt = await buildWorkerPrompt(
      {
        ...(workerInput ?? { task: agent.topic }),
        contextFiles: contextFiles ?? [],
        mode: agent.mode ?? "analyze",
        workspaceStrategy: agent.workspaceStrategy,
      },
      agent.workspacePath,
      promptOptions,
    );

    const client = this.clientOrThrow();
    let session: { id: string };
    try {
      session = await client.createSession(agent.workspacePath, agent.title);
    } catch (sessionError) {
      const message = redactSecrets(String(sessionError));
      this.store.updateJobFallbackStatus(job.id, "failed");
      const current = this.store.getJob(job.id);
      if (current && current.status !== "failed") this.store.updateJobStatus(job.id, "failed", message);
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, "failed", message);
      this.recordActivity(agent, job, "error", "Failed to create OpenCode session for fallback: " + message);
      if (this.followLifecycles.has(job.id)) {
        await this.resolveFollow(job.id, { status: "failed", error: message });
      }
      return;
    }

    if (controller.signal.aborted || this.store.getJob(job.id)?.status === "aborted") {
      await client.abort(session.id).catch(() => {});
      return;
    }

    const updatedAgent = this.store.updateAgentSession(agent.id, this.managed?.serverId ?? "unknown", session.id);
    this.recordActivity(updatedAgent, job, "dispatch", "Created OpenCode session " + session.id + " for timeout fallback");

    const dispatchOpts = {
      providerId: fallbackRoute.providerId,
      modelId: fallbackRoute.modelId,
      ...(fallbackRoute.variant ? { variant: fallbackRoute.variant } : {}),
      ...(this.config.opencodeAgent ? { agent: this.config.opencodeAgent } : {}),
    };

    try {
      await client.promptAsync(session.id, opencodePrompt, dispatchOpts);
      const current = this.store.getJob(job.id);
      if (current && current.status === "dispatching") this.store.updateJobStatus(job.id, "running");
      this.recordActivity(updatedAgent, job, "dispatch", "Dispatched task to fallback OpenCode session");
    } catch (dispatchError) {
      const message = redactSecrets(String(dispatchError));
      if (isUnknownDispatchOutcome(dispatchError)) {
        this.store.markDispatchUnknown(job.id);
        this.recordActivity(updatedAgent, this.store.getJob(job.id), "error", "Fallback OpenCode dispatch outcome is unknown after a transport failure; the job stays active");
        const pending = this.store.getJob(job.id) ?? job;
        this.ensureFollowLifecycle(
          pending,
          this.followWindowMinutes(undefined, 1, 60, this.config.followDefaultWaitMinutes),
          this.followWindowMinutes(undefined, 1, 10, this.config.followDefaultGraceMinutes),
          true,
        );
        this.store.setJobError(job.id, "Fallback dispatch outcome unknown after a transport failure: " + message);
        return;
      }
      this.store.updateJobFallbackStatus(job.id, "failed");
      const current = this.store.getJob(job.id);
      if (current && current.status !== "failed") this.store.updateJobStatus(job.id, "failed", message);
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, "failed", message);
      this.recordActivity(updatedAgent, job, "error", "Fallback OpenCode rejected task dispatch: " + message);
      if (this.followLifecycles.has(job.id)) {
        await this.resolveFollow(job.id, { status: "failed", error: message });
      }
    }
  }


  private async resumeApproval(agent: AgentRecord, job: JobRecord, prompt: string): Promise<AcceptedOperation> {
    this.clearApprovalTimer(agent.id);
    this.store.setApprovalDeadline(job.id, null);
    this.store.setJobPermission(job.id, null);

    const activeCount = this.store.getActiveJobCount();
    const availableCredits = this.targetCredits - activeCount;
    const claimedResources = this.getActiveExclusiveResources();
    const hasResourceConflict = (job.exclusiveResources ?? []).some((r) => claimedResources.has(r));

    if (availableCredits <= 0 || hasResourceConflict) {
      this.store.updateJobStatus(job.id, "queued");
      const queuedAt = new Date().toISOString();
      this.store.db.prepare("UPDATE jobs SET queued_at = ?, prompt_hash = ? WHERE id = ?").run(queuedAt, hashPrompt(prompt), job.id);
      this.store.saveDispatchEnvelope(job.id, {
        prompt,
        promptHash: hashPrompt(prompt),
        workerInput: { task: prompt },
        contextFiles: [],
      });
      this.pendingDispatches.set(job.id, {
        prompt,
        workerInput: { task: prompt } as any,
        contextFiles: [],
      });
      return this.accepted(this.store.getJob(job.id) ?? job);
    }

    this.store.updateJobStatus(job.id, "running");
    if (agent.status === "needs_approval") this.store.updateAgentStatus(agent.id, "working");
    try {
      await this.clientOrThrow().promptAsync(agent.opencodeSessionId, prompt, this.dispatchOptions(agent));
      return this.accepted(this.store.getJob(job.id) ?? job);
    } catch (error) {
      const message = redactSecrets(String(error));
      if (isUnknownDispatchOutcome(error)) {
        // Same accepted dispatch_unknown contract as spawn/continue: the
        // caller keeps the exact job id, the follow deadline/grace is armed so
        // an unaccepted prompt cannot become immortal, and no second
        // submission occurs while the job stays active.
        this.store.markDispatchUnknown(job.id);
        this.recordActivity(agent, this.store.getJob(job.id), "error", "Approval continuation outcome is unknown after a transport failure; the job stays active");
        const pending = this.store.getJob(job.id) ?? job;
        this.ensureFollowLifecycle(
          pending,
          this.followWindowMinutes(undefined, 1, 60, this.config.followDefaultWaitMinutes),
          this.followWindowMinutes(undefined, 1, 10, this.config.followDefaultGraceMinutes),
          true,
        );
        this.store.setJobError(job.id, "Approval continuation outcome unknown after a transport failure: " + message);
        return this.accepted(pending, { outcome: "dispatch_unknown", warning: DISPATCH_UNKNOWN_WARNING });
      }
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
      throw new ConflictError("permissionId does not match the active approval request", "permission_mismatch");
    }
    this.clearApprovalTimer(agent.id);
    this.store.setApprovalDeadline(job.id, null);

    const activeCount = this.store.getActiveJobCount();
    const availableCredits = this.targetCredits - activeCount;
    const claimedResources = this.getActiveExclusiveResources();
    const hasResourceConflict = (job.exclusiveResources ?? []).some((r) => claimedResources.has(r));

    if (availableCredits <= 0 || hasResourceConflict) {
      this.store.updateJobStatus(job.id, "queued");
      const queuedAt = new Date().toISOString();
      this.store.db.prepare("UPDATE jobs SET queued_at = ? WHERE id = ?").run(queuedAt, job.id);
      const workerInput = {
        permissionId,
        permissionReply: reply,
        permissionMessage: message,
      };
      this.store.saveDispatchEnvelope(job.id, {
        prompt: "",
        promptHash: hashPrompt(""),
        workerInput,
        contextFiles: [],
      });
      this.pendingDispatches.set(job.id, {
        prompt: "",
        workerInput: workerInput as any,
        contextFiles: [],
      });
      return this.accepted(this.store.getJob(job.id) ?? job);
    }

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
      if (isUnknownDispatchOutcome(error)) {
        // Same accepted dispatch_unknown contract as resume: keep the exact
        // job id, clear the answered permission like the success path, arm the
        // follow deadline/grace, and prevent a second submission while the job
        // stays active.
        this.store.markDispatchUnknown(job.id);
        const afterReply = this.store.getJob(job.id);
        if (afterReply?.status === "running" && afterReply.permissionId === permissionId) {
          this.store.setJobPermission(job.id, null);
        }
        this.recordActivity(agent, this.store.getJob(job.id), "error", "Permission reply outcome is unknown after a transport failure; the job stays active");
        const pending = this.store.getJob(job.id) ?? job;
        this.ensureFollowLifecycle(
          pending,
          this.followWindowMinutes(undefined, 1, 60, this.config.followDefaultWaitMinutes),
          this.followWindowMinutes(undefined, 1, 10, this.config.followDefaultGraceMinutes),
          true,
        );
        this.store.setJobError(job.id, "Permission reply outcome unknown after a transport failure: " + errorText);
        return this.accepted(pending, { outcome: "dispatch_unknown", warning: DISPATCH_UNKNOWN_WARNING });
      }
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
    // High-volume streaming deltas never change job state and would bloat the
    // event ledger and activity table; skip them while keeping meaningful
    // activity and events.
    if (event.type === "message.part.delta") return;
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
      if (observedJob) {
        try {
          this.store.updateJobLiveness(observedJob.id, {
            heartbeatAt: new Date().toISOString(),
          });
        } catch {}
      }
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
    const assistants = messages.filter((message) => message.info?.role === "assistant");
    const baselineAssistantId = currentJob.lastAssistantMessageId ?? null;
    const baselineIndex = baselineAssistantId
      ? assistants.findIndex((message) => message.info?.id === baselineAssistantId)
      : -1;
    const relevantAssistants = baselineAssistantId === null
      ? assistants
      : baselineIndex < 0
        ? []
        : assistants.slice(baselineIndex + 1);
    const assistantWithError = relevantAssistants.find((m) => m.info?.error != null);
    if (assistantWithError) {
      const errorDetail = formatAssistantError(assistantWithError.info!.error);
      await this.failJob(currentJob, agent, errorDetail);
      return;
    }
    const latestAssistantId = latestAssistantMessageId(messages);
    if (currentJob.lastAssistantMessageId && (!latestAssistantId || latestAssistantId === currentJob.lastAssistantMessageId)) return;
    if (!assistantTextAfterBaseline(messages, currentJob.lastAssistantMessageId).hasText) {
      // Idle with no non-empty assistant text (tool-only or reasoning-only
      // tails included) must never become a usable completed success. Leave
      // the job active so the follow deadline, reconciliation or a later
      // event settles it fail-closed.
      if (currentJob.status === "dispatching") this.store.updateJobStatus(job.id, "running");
      this.recordActivity(agent, this.store.getJob(job.id), "event", "OpenCode session became idle without non-empty assistant output; the job remains active");
      return;
    }
    const partial = currentJob.status === "finalizing" || currentJob.gracefulFinalizeAttempted;
    if (currentJob.status === "dispatching") this.store.updateJobStatus(job.id, "running");
    if (currentJob.fallbackTo) {
      this.store.updateJobFallbackStatus(job.id, "succeeded");
    }
    const jobToPersist = this.store.getJob(job.id) ?? currentJob;
    const stored = await persistResult(this.config.dataDir, currentAgent, jobToPersist, messages, diff, this.config.maxResultLength, {
      ...(partial ? {
        statusOverride: "completed_partial",
        deadlineReached: true,
        gracefulFinalize: true,
        partial: true,
      } : {}),
    });

    const capturedFence = currentJob.fence ?? job.fence ?? 1;
    try {
      this.store.setJobMessages(job.id, stored.parsed.userMessageId, stored.parsed.assistantMessageId);
      this.store.setJobResult(job.id, stored.resultPath, stored.envelope.summary, capturedFence);
      if (stored.envelope.earlyExit?.triggered) {
        this.store.setJobEarlyExit(job.id, {
          earlyExitAt: stored.envelope.earlyExit.signaledAt || new Date().toISOString(),
          reason: stored.envelope.earlyExit.reason,
        }, capturedFence);
      }
      if (stored.envelope.escalation) {
        this.store.setJobEscalation(job.id, JSON.stringify(stored.envelope.escalation), capturedFence);
      }
      const completedJob = this.store.getJob(job.id) ?? job;
      if (["running", "following", "finalizing"].includes(completedJob.status)) {
        this.store.updateJobStatus(job.id, partial ? "completed_partial" : "completed", null, capturedFence);
      }
      const completedAgent = this.store.getAgent(agent.id) ?? agent;
      if (completedAgent.status === "working") this.store.updateAgentStatus(agent.id, partial ? "completed_partial" : "completed");
      this.recordActivity(agent, this.store.getJob(job.id), "result", partial ? "Graceful finalization produced a partial result" : "OpenCode session became idle and the result was persisted");
      this.recordTerminalSuccess(job.id);
      this.onJobSettled(job.id, job.batchId);
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
      if (["completed", "completed_partial"].includes(deliveryJob.status)) this.store.updateJobStatus(job.id, "delivery_pending", null, capturedFence);
      await this.deliverEnvelope(stored.envelope, this.store.getJob(job.id) ?? job);
    } catch (err) {
      if (err instanceof ConflictError) {
        this.recordActivity(agent, this.store.getJob(job.id) ?? job, "error", "Stale session completion rejected by fence check: " + err.message);
        return;
      }
      throw err;
    }
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
    await this.evaluateParkWakes(job.id);
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
      this.clearJobInactivityTimer(job.id);
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
      this.clearJobInactivityTimer(job.id);
      const current = this.store.getJob(job.id);
      if (current?.status === "delivery_pending") this.store.updateJobStatus(job.id, "delivered");
    } catch (error) {
      const message = redactSecrets(String(error));
      try {
        await this.inbox.deliver(envelope, humanText);
        this.store.setDeliveryMethod(delivery.id, "inbox", message);
        this.store.updateDelivery(delivery.id, "delivered");
        this.clearJobInactivityTimer(job.id);
        const current = this.store.getJob(job.id);
        if (current?.status === "delivery_pending") this.store.updateJobStatus(job.id, "delivered");
      } catch (fallbackError) {
        this.store.updateDelivery(delivery.id, "failed", message + "; inbox: " + redactSecrets(String(fallbackError)));
        this.lastStreamError = "Delivery failed: " + message + "; inbox: " + redactSecrets(String(fallbackError));
      }
    }
  }

  private async reconcileJob(job: JobRecord, options: { fromRecovery?: boolean } = {}): Promise<void> {
    return this.withAgentOperationLock("reconcile:" + job.id, async () => {
      const currentJob = this.store.getJob(job.id) ?? job;
      if (currentJob.resultPath || TERMINAL_JOB_STATUSES.has(currentJob.status) || currentJob.status === "delivery_pending") {
        return;
      }
      const agent = this.store.getAgent(currentJob.agentId);
      if (!agent || !this.client) return;
      if (agent.modelProviderId === "antigravity") return;

      let messages: OpenCodeMessage[];
      try {
        messages = await this.client.listMessages(agent.opencodeSessionId);
      } catch (error) {
        if (isSessionAbsentError(error)) {
          await this.failJob(currentJob, agent, "OpenCode session absent (404): " + redactSecrets(String(error)));
          return;
        }
        this.lastStreamError = redactSecrets(String(error));
        this.store.markDispatchUnknown(currentJob.id);
        this.recordActivity(agent, currentJob, "error", "Reconciliation error: " + redactSecrets(String(error)));
        return;
      }

      const assistants = messages.filter((message) => message.info?.role === "assistant");
      const baselineAssistantId = currentJob.lastAssistantMessageId ?? null;
      const baselineIndex = baselineAssistantId
        ? assistants.findIndex((message) => message.info?.id === baselineAssistantId)
        : -1;
      const relevantAssistants = baselineAssistantId === null
        ? assistants
        : baselineIndex < 0
          ? []
          : assistants.slice(baselineIndex + 1);

      // Contract 2: info.error on the relevant assistant message is authoritative failure
      const assistantWithError = relevantAssistants.find((m) => m.info?.error != null);
      if (assistantWithError) {
        const errorDetail = formatAssistantError(assistantWithError.info!.error);
        await this.failJob(currentJob, agent, errorDetail);
        return;
      }

      // Contract 1: OpenCode assistant text is streaming unless the newest relevant assistant message has a non-empty terminal info.finish value
      const newestAssistant = relevantAssistants.at(-1);
      const finish = newestAssistant?.info?.finish;
      const hasTerminalFinish = typeof finish === "string" && finish.trim().length > 0;

      if (relevantAssistants.length === 0 || !hasTerminalFinish) {
        if (options.fromRecovery) {
          this.store.markDispatchUnknown(currentJob.id);
          this.recordActivity(agent, this.store.getJob(currentJob.id) ?? currentJob, "event", "Startup recovery retained active job with streaming assistant message");
        }
        return;
      }

      // Contract 1 & 3: terminal finish value completes exactly once
      const textOutput = assistantTextAfterBaseline(messages, baselineAssistantId);
      if (!textOutput.hasText) {
        if (currentJob.status === "dispatching") this.store.updateJobStatus(currentJob.id, "running");
        this.recordActivity(agent, this.store.getJob(currentJob.id) ?? currentJob, "event", "OpenCode session finished without non-empty assistant output; the job remains active");
        return;
      }

      const diff = await this.client.getDiff(agent.opencodeSessionId).catch(() => "");
      const currentAgent = this.store.getAgent(agent.id) ?? agent;
      const partial = currentJob.status === "finalizing" || currentJob.gracefulFinalizeAttempted;
      if (currentJob.status === "dispatching") this.store.updateJobStatus(currentJob.id, "running");
      if (currentJob.fallbackTo) {
        this.store.updateJobFallbackStatus(currentJob.id, "succeeded");
      }
      const jobToPersist = this.store.getJob(currentJob.id) ?? currentJob;
      const stored = await persistResult(this.config.dataDir, currentAgent, jobToPersist, messages, diff, this.config.maxResultLength, {
        ...(partial ? {
          statusOverride: "completed_partial",
          deadlineReached: true,
          gracefulFinalize: true,
          partial: true,
        } : {}),
      });

      const capturedFence = currentJob.fence ?? job.fence ?? 1;
      try {
        this.store.setJobMessages(currentJob.id, stored.parsed.userMessageId, stored.parsed.assistantMessageId);
        this.store.setJobResult(currentJob.id, stored.resultPath, stored.envelope.summary, capturedFence);
        if (stored.envelope.earlyExit?.triggered) {
          this.store.setJobEarlyExit(currentJob.id, {
            earlyExitAt: stored.envelope.earlyExit.signaledAt || new Date().toISOString(),
            reason: stored.envelope.earlyExit.reason,
          }, capturedFence);
        }
        if (stored.envelope.escalation) {
          this.store.setJobEscalation(currentJob.id, JSON.stringify(stored.envelope.escalation), capturedFence);
        }
        const completedJob = this.store.getJob(currentJob.id) ?? currentJob;
        if (["running", "following", "finalizing"].includes(completedJob.status)) {
          this.store.updateJobStatus(currentJob.id, partial ? "completed_partial" : "completed", null, capturedFence);
        }
        const completedAgent = this.store.getAgent(agent.id) ?? agent;
        if (completedAgent.status === "working") {
          this.store.updateAgentStatus(agent.id, partial ? "completed_partial" : "completed");
        }
        this.recordActivity(agent, this.store.getJob(currentJob.id) ?? currentJob, "result", partial ? "Graceful finalization produced a partial result" : "Reconciled terminal assistant completion");
        this.recordTerminalSuccess(currentJob.id);
        this.onJobSettled(currentJob.id, currentJob.batchId);
        if (this.followLifecycles.has(currentJob.id)) {
          await this.resolveFollow(currentJob.id, {
            status: partial ? "completed_partial" : "completed",
            deadlineReached: partial,
            gracefulFinalize: partial,
            partial,
            resultAvailable: true,
            envelope: stored.envelope,
          });
        }
        const deliveryJob = this.store.getJob(currentJob.id) ?? currentJob;
        if (deliveryJob && ["completed", "completed_partial"].includes(deliveryJob.status)) {
          this.store.updateJobStatus(currentJob.id, "delivery_pending", null, capturedFence);
        }
        const pending = this.store.getJob(currentJob.id);
        if (pending) await this.deliverEnvelope(stored.envelope, pending);
      } catch (err) {
        if (err instanceof ConflictError) {
          this.recordActivity(agent, this.store.getJob(currentJob.id) ?? currentJob, "error", "Stale session completion rejected by fence check: " + err.message);
          return;
        }
        throw err;
      }
    });
  }

  private async recoverPendingJobs(): Promise<void> {
    for (const job of this.store.recoverPendingJobs()) {
      if (job.status === "queued") {
        continue;
      }
      const agent = this.store.getAgent(job.agentId);
      if (job.resultPath) {
        if (job.status === "dispatching" || job.status === "needs_approval") {
          this.store.updateJobStatus(job.id, "running");
          this.store.updateJobStatus(job.id, "completed");
          this.store.updateJobStatus(job.id, "delivery_pending");
        } else if (["running", "following", "finalizing"].includes(job.status)) {
          this.store.updateJobStatus(job.id, "completed");
          this.store.updateJobStatus(job.id, "delivery_pending");
        } else if (["completed", "completed_partial", "timed_out"].includes(job.status)) {
          this.store.updateJobStatus(job.id, "delivery_pending");
        }
        const pending = this.store.getJob(job.id);
        if (pending?.status === "delivery_pending") {
          await this.deliverPersistedJob(pending).catch((error) => {
            this.lastStreamError = redactSecrets(String(error));
          });
        }
        continue;
      }
      if (agent?.modelProviderId === "antigravity" && ["dispatching", "running", "following", "finalizing"].includes(job.status)) {
        if (agent.opencodeSessionId && !agent.opencodeSessionId.startsWith("antigravity:")) {
          await this.reconcileJob(job, { fromRecovery: true }).catch((error) => {
            this.lastStreamError = redactSecrets(String(error));
          });
          continue;
        }
        await this.recoverAntigravityJob(agent, job).catch((error) => {
          this.lastStreamError = redactSecrets(String(error));
        });
        continue;
      }
      if (["dispatching", "running"].includes(job.status)) {
        try {
          await this.reconcileJob(job, { fromRecovery: true });
        } catch (error) {
          const message = redactSecrets(String(error));
          this.lastStreamError = message;
          if (isSessionAbsentError(error)) {
            if (agent) {
              await this.failJob(job, agent, "OpenCode session absent (404): " + message).catch(() => undefined);
            }
          } else {
            this.store.markDispatchUnknown(job.id);
            if (agent) {
              this.recordActivity(agent, job, "error", "Recovery unknown reconciliation outcome: " + message);
            }
          }
        }
      } else if (["following", "finalizing"].includes(job.status)) {
        this.ensureFollowLifecycle(
          job,
          normalizeFollowMinutes(undefined, 1, 60, this.config.followDefaultWaitMinutes),
          normalizeFollowMinutes(undefined, 1, 10, this.config.followDefaultGraceMinutes),
        );
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
    await this.recoverArmedBarriers().catch((error) => {
      this.lastStreamError = redactSecrets(String(error));
    });
    await this.recoverWakeOutbox().catch((error) => {
      this.lastStreamError = redactSecrets(String(error));
    });
    this.scheduleDrain();
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

  private async failJob(job: JobRecord, agent: AgentRecord, error: string): Promise<void> {
    const currentJob = this.store.getJob(job.id) ?? job;
    if (TERMINAL_JOB_STATUSES.has(currentJob.status)) return;
    this.clearApprovalTimer(agent.id);
    this.store.setApprovalDeadline(currentJob.id, null);
    this.store.updateJobStatus(currentJob.id, "failed", error);
    const currentAgent = this.store.getAgent(agent.id);
    if (currentAgent && currentAgent.status !== "closed") {
      this.store.updateAgentStatus(agent.id, "failed", error);
    }
    this.recordActivity(agent, this.store.getJob(currentJob.id) ?? currentJob, "error", "OpenCode reported a terminal error: " + error);
    this.onJobSettled(currentJob.id, currentJob.batchId);
    if (this.followLifecycles.has(currentJob.id)) {
      await this.resolveFollow(currentJob.id, { status: "failed", error });
    }
    await this.evaluateParkWakes(currentJob.id).catch((err: unknown) => {
      this.lastStreamError = redactSecrets(String(err));
    });
  }

  private async failActive(agent: AgentRecord, error: string): Promise<void> {
    const job = this.activeJob(agent.id);
    if (!job) return;
    await this.failJob(job, agent, error);
  }

  private async markNeedsApproval(agent: AgentRecord, properties: Record<string, unknown> = {}): Promise<void> {
    const job = this.activeJob(agent.id);
    if (!job) return;
    this.clearJobInactivityTimer(job.id);
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
    await this.evaluateParkWakes(job.id);
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
    this.onJobSettled(current.id, current.batchId);
    if (this.followLifecycles.has(current.id)) {
      await this.resolveFollow(current.id, { status: "failed", error: failure });
    }
    await this.evaluateParkWakes(current.id).catch((error: unknown) => {
      this.lastStreamError = redactSecrets(String(error));
    });
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

  private accepted(job: JobRecord, extra: { outcome?: "dispatch_unknown"; warning?: string } = {}): AcceptedOperation {
    const agent = this.store.getAgent(job.agentId);
    if (!agent) throw new Error("Job has no agent: " + job.id);
    return {
      accepted: true,
      status: "accepted",
      agentId: agent.id,
      jobId: job.id,
      topic: agent.topic,
      modelDisplayName: this.resolveAgentRoute(agent).display,
      state: "Starting",
      message: extra.warning ?? "DeepSeek Sub-Agent accepted the task and will report asynchronously.",
      ...(extra.outcome ? { outcome: extra.outcome } : {}),
      ...(job.priority !== undefined && job.priority !== null ? { priority: job.priority } : {}),
      ...(job.exclusiveResources !== undefined && job.exclusiveResources !== null ? { exclusiveResources: job.exclusiveResources } : {}),
    };
  }

  private persistCorrelationHint(job: JobRecord, threadId: string | undefined, turnId: string | undefined): void {
    if (!threadId && !turnId) return;
    this.store.setCorrelationHint(job.id, {
      threadId: threadId ?? null,
      turnId: turnId ?? null,
      source: "mcp",
    });
  }

  private acceptedRequest(job: JobRecord): AcceptedOperation {
    if (job.dispatchUnknown) {
      return this.accepted(job, { outcome: "dispatch_unknown", warning: DISPATCH_UNKNOWN_WARNING });
    }
    return this.accepted(job);
  }

  private async recoverAntigravityJob(agent: AgentRecord, job: JobRecord): Promise<void> {
    const spool = new AntigravitySpool(this.config.dataDir);
    const attempts = await spool.listAttempts(job.id);
    if (attempts.length === 0) {
      const reason = "Antigravity run stranded: no durable spool found for job " + job.id + "; the job cannot be recovered";
      this.recordActivity(agent, job, "error", reason);
      this.store.updateJobStatus(job.id, "failed", reason);
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status === "working") this.store.updateAgentStatus(agent.id, "failed", reason);
      this.onJobSettled(job.id, job.batchId);
      await this.evaluateParkWakes(job.id).catch((error: unknown) => {
        this.lastStreamError = redactSecrets(String(error));
      });
      return;
    }

    const latestAttempt = attempts[attempts.length - 1];
    if (!latestAttempt) return;

    // 1. Check if terminal status was already written by supervisor
    const terminalStatus = await spool.readStatus(latestAttempt.statusPath);
    if (terminalStatus) {
      if (terminalStatus.status === "completed" || terminalStatus.status === "completed_partial") {
        const result: AntigravityRunResult = {
          status: terminalStatus.status,
          runId: terminalStatus.runId,
          summary: terminalStatus.summary,
          files: terminalStatus.files,
          tests: terminalStatus.tests,
          risks: terminalStatus.risks,
          diffSummary: terminalStatus.diffSummary,
          model: latestAttempt.modelId,
          modelDisplayName: "Antigravity · " + latestAttempt.modelId,
          workspace: latestAttempt.cwd,
          rawOutput: terminalStatus.stdout,
        };
        const stored = await persistAntigravityResult(this.config.dataDir, agent, job, result, this.config.maxResultLength);
        const attemptFence = latestAttempt.fence ?? job.fence ?? 1;
        try {
          this.store.setJobResult(job.id, stored.resultPath, stored.envelope.summary, attemptFence);
          this.store.updateJobStatus(job.id, terminalStatus.status, null, attemptFence);
          const currentAgent = this.store.getAgent(agent.id) ?? agent;
          if (currentAgent.status === "working") this.store.updateAgentStatus(agent.id, terminalStatus.status);
          this.recordActivity(agent, this.store.getJob(job.id), "result", "Recovered completed Antigravity result from durable spool");
          this.recordTerminalSuccess(job.id);
          this.onJobSettled(job.id, job.batchId);
          this.store.updateJobStatus(job.id, "delivery_pending", null, attemptFence);
          const deliveryJob = this.store.getJob(job.id) ?? job;
          await this.deliverEnvelope(stored.envelope, deliveryJob).catch((error) => {
            this.lastStreamError = redactSecrets(String(error));
          });
          await spool.cleanupPrompt(latestAttempt.attemptId, job.id);
          return;
        } catch (err) {
          if (err instanceof ConflictError) {
            this.recordActivity(agent, this.store.getJob(job.id) ?? job, "error", "Stale recovery rejected by fence check: " + err.message);
            return;
          }
          throw err;
        }
      } else {
        const reason = terminalStatus.error || ("Antigravity run ended with status " + terminalStatus.status);
        const finalStatus = terminalStatus.status === "aborted" ? "aborted" : terminalStatus.status === "timed_out" ? "timed_out" : "failed";
        this.store.updateJobStatus(job.id, finalStatus, reason);
        const currentAgent = this.store.getAgent(agent.id);
        if (currentAgent && currentAgent.status === "working") this.store.updateAgentStatus(agent.id, finalStatus, reason);
        this.recordActivity(agent, job, "error", "Recovered terminal " + finalStatus + " Antigravity status from durable spool");
        this.onJobSettled(job.id, job.batchId);
        await this.evaluateParkWakes(job.id).catch((error: unknown) => {
          this.lastStreamError = redactSecrets(String(error));
        });
        await spool.cleanupPrompt(latestAttempt.attemptId, job.id);
        return;
      }
    }

    // 2. Check if supervisor heartbeat is live or process is running
    const heartbeat = await spool.readHeartbeat(latestAttempt.heartbeatPath);
    const supervisorAlive = heartbeat?.supervisorPid ? isProcessAlive(heartbeat.supervisorPid) : false;
    const agyAlive = heartbeat?.agyPid ? isProcessAlive(heartbeat.agyPid) : false;
    const processAlive = supervisorAlive || agyAlive;
    const isHeartbeatLive = spool.isHeartbeatLive(heartbeat);
    const startTime = heartbeat?.updatedAt
      ? Math.min(parseTimestamp(latestAttempt.createdAt) ?? heartbeat.updatedAt, heartbeat.updatedAt)
      : (parseTimestamp(latestAttempt.createdAt) ?? 0);
    const effectiveTimeout = latestAttempt.timeoutMs ?? this.effectiveWorkerTimeoutMs();
    const fallbackLeaseMs = effectiveTimeout !== null ? startTime + effectiveTimeout : null;
    const leaseExpiresAt = job.leaseExpiresAt ? parseTimestamp(job.leaseExpiresAt) : fallbackLeaseMs;
    const leaseActive = leaseExpiresAt !== null && Date.now() < leaseExpiresAt;

    if (isHeartbeatLive || processAlive) {
      this.recordActivity(agent, job, "dispatch", "Reattached to live Antigravity attempt " + latestAttempt.attemptId + " (supervisor PID " + (heartbeat?.supervisorPid ?? "unknown") + ")");
      this.store.updateJobLiveness(job.id, {
        attempt: latestAttempt.attemptId,
        workerPid: heartbeat?.agyPid ?? heartbeat?.supervisorPid ?? null,
        heartbeatAt: heartbeat?.timestamp ?? new Date().toISOString(),
        fence: job.fence ?? 1,
      });
      const prog = await spool.readProgress(latestAttempt.attemptId, job.id);
      if (prog) {
        this.metrics.sqliteProgressUpdates++;
        this.store.updateJobProgress(job.id, {
          lastProgressAt: prog.lastProgressAt,
          progressRevision: prog.progressRevision,
          attempt: latestAttempt.attemptId,
          fence: latestAttempt.fence ?? job.fence ?? 1,
        });
      }
      this.metrics.reconnectReconciledCount++;
      this.scheduleJobInactivityTimer(job.id);
      if (["following", "finalizing"].includes(job.status)) {
        this.ensureFollowLifecycle(
          job,
          normalizeFollowMinutes(undefined, 1, 60, this.config.followDefaultWaitMinutes),
          normalizeFollowMinutes(undefined, 1, 10, this.config.followDefaultGraceMinutes),
        );
      }
      const task = this.monitorReattachedAntigravityAttempt(agent, job, latestAttempt, spool)
        .catch((error) => {
          if (this.running) this.lastStreamError = redactSecrets(String(error));
        })
        .finally(() => {
          this.antigravityTasks.delete(task);
          this.antigravityTasksByJob.delete(job.id);
        });
      this.antigravityTasks.add(task);
      this.antigravityTasksByJob.set(job.id, task);
      return;
    }

    // Explicitly differentiate provably dead from unknown / lease active:
    // If lease is still active or process liveness is unknown (e.g. no heartbeat / no verified PID),
    // takeover is blocked! Never start a replacement worker or release the workspace.
    const hasKnownPids = Boolean(heartbeat?.supervisorPid || heartbeat?.agyPid);
    const provablyDead = !leaseActive && !processAlive && !isHeartbeatLive && hasKnownPids;

    if (!provablyDead) {
      const reason = leaseActive
        ? "Antigravity takeover blocked: lease is still active until " + new Date(leaseExpiresAt!).toISOString()
        : "Antigravity takeover blocked: worker liveness is unknown (no verifiable PID or heartbeat)";
      this.recordActivity(agent, job, "dispatch", reason);
      return;
    }

    // 3. Provably dead non-terminal attempt: take exclusive recovery claim
    const claimantId = "daemon_" + process.pid + "_" + Date.now();
    const claimed = await spool.claimRecovery(job.id, claimantId);
    if (!claimed) {
      return;
    }

    // Check replay budget (at most 1 replacement attempt; total 2 attempts)
    if (attempts.length >= 2 || latestAttempt.parentAttemptId !== null) {
      const reason = "Antigravity replay budget exhausted: replacement attempt already attempted; job cannot be recovered";
      this.recordActivity(agent, job, "error", reason);
      this.store.updateJobStatus(job.id, "failed", reason);
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status === "working") this.store.updateAgentStatus(agent.id, "failed", reason);
      return;
    }

    // Check prompt availability
    let prompt: string | null = null;
    try {
      if (existsSync(latestAttempt.promptPath)) {
        prompt = await readFile(latestAttempt.promptPath, "utf8");
      }
    } catch {}

    if (!prompt || !prompt.trim()) {
      const reason = "Antigravity replacement recovery failed: original transient prompt is unavailable in spool";
      this.recordActivity(agent, job, "error", reason);
      this.store.updateJobStatus(job.id, "failed", reason);
      const currentAgent = this.store.getAgent(agent.id);
      if (currentAgent && currentAgent.status === "working") this.store.updateAgentStatus(agent.id, "failed", reason);
      return;
    }

    // Calculate monotonic fence before creating replacement attempt
    const currentJob = this.store.getJob(job.id) ?? job;
    const currentFence = Math.max(
      typeof currentJob.fence === "number" ? currentJob.fence : 1,
      typeof job.fence === "number" ? job.fence : 1,
      typeof latestAttempt.fence === "number" ? latestAttempt.fence : 1,
      1,
    );
    const nextFence = currentFence + 1;

    // Create replacement attempt
    const replacement = await spool.createAttempt({
      agentId: agent.id,
      jobId: job.id,
      requestId: job.requestId,
      prompt,
      cwd: agent.workspacePath,
      modelProviderId: agent.modelProviderId,
      modelId: agent.modelId,
      modelVariant: agent.modelVariant,
      modelRoute: agent.modelRoute,
      command: latestAttempt.command,
      timeoutMs: latestAttempt.timeoutMs,
      sandbox: latestAttempt.sandbox,
      addDirs: latestAttempt.addDirs,
      dangerouslySkipPermissions: latestAttempt.dangerouslySkipPermissions,
      parentAttemptId: latestAttempt.attemptId,
      maxOutputBytes: latestAttempt.maxOutputBytes,
      fence: nextFence,
    });

    this.store.updateJobLiveness(job.id, {
      attempt: replacement.attemptId,
      fence: nextFence,
      heartbeatAt: new Date().toISOString(),
      leaseExpiresAt: replacement.timeoutMs !== null ? new Date(Date.now() + replacement.timeoutMs).toISOString() : null,
    });

    this.recordActivity(
      agent,
      job,
      "dispatch",
      "Started recovery replacement attempt " + replacement.attemptId + " for dead attempt " + latestAttempt.attemptId,
    );

    if (["following", "finalizing"].includes(job.status)) {
      this.ensureFollowLifecycle(
        job,
        normalizeFollowMinutes(undefined, 1, 60, this.config.followDefaultWaitMinutes),
        normalizeFollowMinutes(undefined, 1, 10, this.config.followDefaultGraceMinutes),
      );
    }

    const controller = new AbortController();
    this.antigravityAbortControllers.set(job.id, controller);
    const task = this.runAntigravityAttemptAsync(agent, job, replacement, controller)
      .catch((error) => {
        if (this.running) this.lastStreamError = redactSecrets(String(error));
      })
      .finally(() => {
        this.antigravityTasks.delete(task);
        this.antigravityTasksByJob.delete(job.id);
      });
    this.antigravityTasks.add(task);
    this.antigravityTasksByJob.set(job.id, task);
  }

  private async monitorReattachedAntigravityAttempt(
    agent: AgentRecord,
    job: JobRecord,
    manifest: AntigravityAttemptManifest,
    spool: AntigravitySpool,
  ): Promise<void> {
    const controller = new AbortController();
    this.antigravityAbortControllers.set(job.id, controller);
    try {
      const pollIntervalMs = 250;
      while (true) {
        if (!this.running && this.lifecycleState !== "recovering") return;
        if (controller.signal.aborted || this.store.getJob(job.id)?.status === "aborted") {
          await spool.writeCancelSignal(manifest.attemptId, "Aborted by orchestrator");
          return;
        }

        const status = await spool.readStatus(manifest.statusPath);
        if (status) {
          if (status.status === "completed" || status.status === "completed_partial") {
            const result: AntigravityRunResult = {
              status: status.status,
              runId: status.runId,
              summary: status.summary,
              files: status.files,
              tests: status.tests,
              risks: status.risks,
              diffSummary: status.diffSummary,
              model: manifest.modelId,
              modelDisplayName: "Antigravity · " + manifest.modelId,
              workspace: manifest.cwd,
              rawOutput: status.stdout,
            };
            const stored = await persistAntigravityResult(this.config.dataDir, agent, job, result, this.config.maxResultLength);
            const attemptFence = manifest.fence ?? job.fence ?? 1;
            try {
              this.store.setJobResult(job.id, stored.resultPath, stored.envelope.summary, attemptFence);
              const current = this.store.getJob(job.id) ?? job;
              if (["running", "following", "finalizing"].includes(current.status)) {
                this.store.updateJobStatus(job.id, status.status, null, attemptFence);
              }
              const currentAgent = this.store.getAgent(agent.id) ?? agent;
              if (currentAgent.status === "working") this.store.updateAgentStatus(agent.id, status.status);
              this.recordActivity(agent, this.store.getJob(job.id), "result", "Antigravity run completed and result was persisted");
              if (this.followLifecycles.has(job.id)) {
                await this.resolveFollow(job.id, {
                  status: status.status,
                  resultAvailable: true,
                  envelope: stored.envelope,
                });
              }
              const pending = this.store.getJob(job.id) ?? job;
              if (["completed", "completed_partial"].includes(pending.status)) this.store.updateJobStatus(job.id, "delivery_pending", null, attemptFence);
              const deliveryJob = this.store.getJob(job.id) ?? job;
              await this.deliverEnvelope(stored.envelope, deliveryJob);
              await spool.cleanupPrompt(manifest.attemptId, job.id);
              return;
            } catch (err) {
              if (err instanceof ConflictError) {
                this.recordActivity(agent, this.store.getJob(job.id) ?? job, "error", "Stale recovery rejected by fence check: " + err.message);
                return;
              }
              throw err;
            }
          } else {
            const reason = status.error || ("Antigravity run ended with status " + status.status);
            const finalStatus = status.status === "aborted" ? "aborted" : status.status === "timed_out" ? "timed_out" : "failed";
            this.store.updateJobStatus(job.id, finalStatus, reason);
            const currentAgent = this.store.getAgent(agent.id);
            if (currentAgent && currentAgent.status !== "closed") this.store.updateAgentStatus(agent.id, finalStatus, reason);
            this.recordActivity(agent, job, "error", reason);
            if (this.followLifecycles.has(job.id)) {
              await this.resolveFollow(job.id, { status: "failed", error: reason });
            }
            await spool.cleanupPrompt(manifest.attemptId, job.id);
            return;
          }
        }

        const heartbeat = await spool.readHeartbeat(manifest.heartbeatPath);
        if (heartbeat) {
          try {
            this.store.updateJobLiveness(job.id, {
              attempt: manifest.attemptId,
              workerPid: heartbeat.agyPid ?? heartbeat.supervisorPid ?? null,
              heartbeatAt: heartbeat.timestamp,
              fence: manifest.fence ?? job.fence ?? 1,
            });
          } catch {}
        }
        const prog = await spool.readProgress(manifest.attemptId, job.id);
        if (prog) {
          const current = this.store.getJob(job.id);
          if (
            current &&
            (prog.progressRevision > (current.progressRevision ?? 0) ||
              (prog.lastProgressAt && (!current.lastProgressAt || prog.lastProgressAt > current.lastProgressAt)))
          ) {
            this.metrics.sqliteProgressUpdates++;
            this.store.updateJobProgress(job.id, {
              lastProgressAt: prog.lastProgressAt,
              progressRevision: prog.progressRevision,
              attempt: manifest.attemptId,
              fence: manifest.fence ?? current.fence ?? 1,
            });
            this.scheduleJobInactivityTimer(job.id);
          }
        }
        const supervisorAlive = heartbeat?.supervisorPid ? isProcessAlive(heartbeat.supervisorPid) : false;
        const agyAlive = heartbeat?.agyPid ? isProcessAlive(heartbeat.agyPid) : false;
        const processAlive = supervisorAlive || agyAlive;
        if (!spool.isHeartbeatLive(heartbeat) && !processAlive) {
          await this.recoverAntigravityJob(agent, job);
          return;
        }

        await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
      }
    } finally {
      this.clearJobInactivityTimer(job.id);
      if (this.antigravityAbortControllers.get(job.id) === controller) {
        this.antigravityAbortControllers.delete(job.id);
      }
    }
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

/**
 * Static, immutable display name for persisted model identity. Deliberately
 * not derived from the live config registry so renaming a route never changes
 * an existing agent's label; unknown models fall back to the model id.
 */
function staticModelDisplayName(modelId: string, variant: string | null): string {
  const base = modelId === "deepseek-v4-flash"
    ? "DeepSeek V4 Flash"
    : modelId === "deepseek-v4-pro"
      ? "DeepSeek V4 Pro"
      : modelId === "gemini-3.8-flash-high"
        ? "Gemini 3.8 Flash High"
        : modelId === "gemini-3.7-flash-high"
          ? "Gemini 3.7 Flash High"
          : modelId;
  return variant === "max" ? base + " · Max" : base;
}

export function computeBatchHash(items: BatchItemInput[]): string {
  const normalized = items.map((it) => {
    const raw = it as unknown as Record<string, unknown>;
    const requestId = (it.requestId ?? (typeof raw.request_id === "string" ? raw.request_id : "") ?? "").trim();
    const topic = (it.topic ?? (typeof raw.topic === "string" ? raw.topic : "") ?? "").trim();
    const task = (it.task ?? (typeof raw.task === "string" ? raw.task : "") ?? "").trim();
    const cwd = (it.cwd ?? (typeof raw.cwd === "string" ? raw.cwd : "") ?? "").trim();
    const mode = (it.mode ?? (typeof raw.mode === "string" ? raw.mode : "analyze") ?? "analyze");
    const workspaceStrategy = (it.workspaceStrategy ?? (typeof raw.workspace_strategy === "string" ? raw.workspace_strategy : "shared") ?? "shared");
    const visualContext = (it.visualContext ?? (typeof raw.visual_context === "string" ? raw.visual_context : "") ?? "").trim();
    const modelRoute = (it.modelRoute ?? (typeof raw.model_route === "string" ? raw.model_route : "") ?? "").trim();
    const threadId = (it.threadId ?? (typeof raw.thread_id === "string" ? raw.thread_id : "") ?? "").trim();
    const turnId = (it.turnId ?? (typeof raw.turn_id === "string" ? raw.turn_id : "") ?? "").trim();
    const mcpSessionId = (it.mcpSessionId ?? (typeof raw.mcp_session_id === "string" ? raw.mcp_session_id : "") ?? "").trim();
    const trustedThreadId = (it.trustedThreadId ?? (typeof raw.trusted_thread_id === "string" ? raw.trusted_thread_id : "") ?? "").trim();
    const priority = typeof it.priority === "number" ? Math.max(1, Math.min(100, Math.floor(it.priority))) : 50;

    const rawResources = (it.exclusiveResources ?? (Array.isArray(raw.exclusive_resources) ? raw.exclusive_resources : [])) as string[];
    const exclusiveResources = Array.from(new Set(rawResources.map((r) => String(r).trim()).filter((r) => r.length > 0))).sort();

    const rawFiles = (it.contextFiles ?? (Array.isArray(raw.context_files) ? raw.context_files : [])) as string[];
    const contextFiles = Array.from(new Set(rawFiles.map((f) => String(f).trim()).filter((f) => f.length > 0))).sort();

    return {
      requestId,
      topic,
      task,
      cwd,
      mode,
      workspaceStrategy,
      visualContext,
      modelRoute,
      threadId,
      turnId,
      mcpSessionId,
      trustedThreadId,
      priority,
      exclusiveResources,
      contextFiles,
    };
  });
  return createHash("sha256").update(JSON.stringify(normalized)).digest("hex");
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

function isUnknownDispatchOutcome(error: unknown): boolean {
  // A transport/timeout failure means the prompt may still have been accepted
  // server-side; a definite HTTP rejection means it was not.
  return error instanceof OpenCodeTransportError;
}

function formatAssistantError(error: unknown): string {
  if (typeof error === "string") return redactSecrets(error);
  if (error && typeof error === "object") {
    const obj = error as Record<string, unknown>;
    const code = typeof obj.code === "string" ? obj.code : null;
    const message = typeof obj.message === "string" ? obj.message : typeof obj.error === "string" ? obj.error : null;
    if (code && message) return redactSecrets(`[${code}] ${message}`);
    if (message) return redactSecrets(message);
    if (code) return redactSecrets(code);
    try {
      return redactSecrets(JSON.stringify(error));
    } catch {
      return redactSecrets(String(error));
    }
  }
  return redactSecrets(String(error));
}

function isSessionAbsentError(error: unknown): boolean {
  if (error instanceof OpenCodeHttpError && error.status === 404) return true;
  const status = typeof (error as any)?.status === "number"
    ? (error as any).status
    : typeof (error as any)?.statusCode === "number"
      ? (error as any).statusCode
      : undefined;
  if (status === 404) return true;
  const message = redactSecrets(String(error)).toLowerCase();
  return message.includes("404") || message.includes("session not found") || message.includes("unknown session") || message.includes("session absent");
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

function deriveSemanticProgress(
  activities: AgentActivity[],
  job: JobRecord | null,
  isLive: boolean,
  heartbeatAt: string | null,
  heartbeatAgoSeconds: number | null,
  lastActivityAgoSeconds: number | null,
  earlyExitSignal?: EarlyExitSignal,
  diagnosticEvidence?: string | null,
  inactivityThresholdSeconds = 300,
): SemanticProgress {
  const latest = activities[0];
  let stage: SemanticProgress["stage"] = "executing";

  if (job && (TERMINAL_JOB_STATUSES.has(job.status) || job.resultPath !== null)) {
    stage = "completed";
  } else if (job?.status === "finalizing" || job?.gracefulFinalizeAttempted) {
    stage = "finalizing";
  } else if (job?.status === "needs_approval") {
    stage = "awaiting_approval";
  } else if (latest) {
    const sum = latest.summary.toLowerCase();
    if (sum.includes("test") || sum.includes("pytest") || sum.includes("vitest")) {
      stage = "testing";
    } else if (sum.includes("edit") || sum.includes("patch") || sum.includes("write") || sum.includes("modify")) {
      stage = "modifying";
    } else if (sum.includes("read") || sum.includes("inspect") || sum.includes("analyze") || sum.includes("grep") || sum.includes("search")) {
      stage = "analyzing";
    }
  }

  // Contract: 900s is only a window/deadline, NEVER proof of death. A job is stalled only if not live and heartbeat/activity is stale, OR conservative inactivity threshold reached.
  // Separate process liveness from observed current job progress, no heartbeat counts as work.
  const isProcessDeadStall = !isLive && (heartbeatAgoSeconds !== null ? heartbeatAgoSeconds > 120 : (lastActivityAgoSeconds !== null ? lastActivityAgoSeconds > inactivityThresholdSeconds : false));
  const isJobInactivityStall = Boolean(
    job && !TERMINAL_JOB_STATUSES.has(job.status) && job.status !== "needs_approval"
    && lastActivityAgoSeconds !== null && lastActivityAgoSeconds > inactivityThresholdSeconds,
  );
  const isStalled = isProcessDeadStall || isJobInactivityStall;
  if (isStalled && stage !== "completed" && stage !== "awaiting_approval") {
    stage = "stalled";
  }

  const milestones = activities
    .filter((a) => a.activityType === "result" || a.activityType === "approval")
    .map((a) => a.summary)
    .slice(0, 5);

  // No heartbeat counts as work: lastActiveAt comes from latest observable job progress, not heartbeat
  const latestWorker = activities.find((a) => a.activityType !== "result");
  const lastActiveAt = job?.lastProgressAt ?? (latestWorker ? latestWorker.createdAt : (job?.startedAt ?? job?.createdAt ?? null));

  return {
    stage,
    summary: latest?.summary ?? "No observable activity recorded.",
    ...(milestones.length > 0 ? { milestones } : {}),
    lastActiveAt,
    isStalled,
    earlyExitTriggered: Boolean(earlyExitSignal?.triggered),
    ...(diagnosticEvidence ? { diagnosticEvidence } : {}),
    ...(isStalled ? { suspected: true } : {}),
  };
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
  const workspacePath = path.join(repositoryRoot, ".deepseek-worktrees", agentId);
  if (existsSync(workspacePath)) return workspacePath;
  await assertCleanGitRepository(repositoryRoot);
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
  const dirtyLines = status.stdout.split("\n").filter((line) => {
    const trimmed = line.trim();
    if (!trimmed) return false;
    const entry = trimmed.slice(2).trim();
    return !entry.startsWith(".deepseek-worktrees");
  });
  if (dirtyLines.length > 0) {
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
