import { DatabaseSync } from "node:sqlite";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { assertAgentTransition, assertJobTransition } from "./state.js";
import { newId, redactSecrets, truncate } from "./security.js";
import type {
  ActivityType,
  AgentActivity,
  AgentRecord,
  AgentStatus,
  CodexBinding,
  DeliveryMethod,
  DeliveryRecord,
  DeliveryStatus,
  JobKind,
  JobRecord,
  JobStatus,
  WorkspaceStrategy,
} from "./types.js";

type Row = Record<string, unknown>;

function stringValue(row: Row, key: string): string {
  const value = row[key];
  if (typeof value !== "string") throw new Error("Expected string column " + key);
  return value;
}

function nullableString(row: Row, key: string): string | null {
  const value = row[key];
  return typeof value === "string" ? value : null;
}

function numberValue(row: Row, key: string): number {
  const value = row[key];
  if (typeof value !== "number" && typeof value !== "bigint") throw new Error("Expected numeric column " + key);
  return Number(value);
}

export class BridgeStore {
  readonly db: DatabaseSync;

  constructor(readonly databasePath: string) {
    this.db = new DatabaseSync(databasePath);
    try {
      this.db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;");
      this.migrate();
    } catch (error) {
      this.db.close();
      throw error;
    }
  }

  static async open(dataDir: string): Promise<BridgeStore> {
    await mkdir(dataDir, { recursive: true });
    return new BridgeStore(path.join(dataDir, "bridge.sqlite"));
  }

  close(): void {
    this.db.close();
  }

  integrityCheck(): string {
    const row = this.db.prepare("PRAGMA quick_check").get() as Row | undefined;
    return row?.quick_check === undefined ? "unknown" : String(row.quick_check);
  }

  migrate(): void {
    const schema = [
      "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);",
      "CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, title TEXT NOT NULL, topic TEXT NOT NULL, repository_root TEXT NOT NULL, workspace_path TEXT NOT NULL, workspace_strategy TEXT NOT NULL CHECK (workspace_strategy IN ('shared','worktree')), opencode_server_id TEXT NOT NULL, opencode_session_id TEXT NOT NULL UNIQUE, model_provider_id TEXT NOT NULL, model_id TEXT NOT NULL, model_variant TEXT, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, closed_at TEXT, last_error TEXT);",
      "CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, agent_id TEXT NOT NULL REFERENCES agents(id), sequence INTEGER NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('spawn','continue')), request_id TEXT NOT NULL UNIQUE, prompt_hash TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT, completed_at TEXT, last_user_message_id TEXT, last_assistant_message_id TEXT, permission_id TEXT, result_path TEXT, result_summary TEXT, error TEXT, follow_started_at TEXT, follow_deadline_at TEXT, follow_grace_minutes REAL, grace_deadline_at TEXT, graceful_finalize_attempted INTEGER NOT NULL DEFAULT 0, approval_deadline_at TEXT, UNIQUE(agent_id, sequence));",
      "CREATE TABLE IF NOT EXISTS codex_bindings (job_id TEXT PRIMARY KEY REFERENCES jobs(id), thread_id TEXT NOT NULL, originating_turn_id TEXT, originating_item_id TEXT, bound_at TEXT NOT NULL);",
      "CREATE TABLE IF NOT EXISTS deliveries (id TEXT PRIMARY KEY, job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id), thread_id TEXT NOT NULL, expected_turn_id TEXT, delivery_method TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, delivered_at TEXT, last_error TEXT);",
      "CREATE TABLE IF NOT EXISTS servers (id TEXT PRIMARY KEY, workspace_root TEXT NOT NULL, base_url TEXT NOT NULL, process_id INTEGER, status TEXT NOT NULL, started_at TEXT NOT NULL, stopped_at TEXT);",
      "CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, source TEXT NOT NULL, source_event_id TEXT NOT NULL, event_type TEXT NOT NULL, session_id TEXT, job_id TEXT REFERENCES jobs(id), received_at TEXT NOT NULL, processed_at TEXT, UNIQUE(source, source_event_id));",
      "CREATE TABLE IF NOT EXISTS agent_activity (id TEXT PRIMARY KEY, agent_id TEXT NOT NULL REFERENCES agents(id), job_id TEXT REFERENCES jobs(id), session_id TEXT, activity_type TEXT NOT NULL, summary TEXT NOT NULL, metadata_json TEXT, created_at TEXT NOT NULL);",
      "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);",
      "CREATE INDEX IF NOT EXISTS idx_jobs_agent ON jobs(agent_id);",
      "CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id, received_at);",
      "CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);",
      "CREATE INDEX IF NOT EXISTS idx_agent_activity_agent ON agent_activity(agent_id, created_at DESC);",
      "CREATE INDEX IF NOT EXISTS idx_agent_activity_job ON agent_activity(job_id, created_at DESC);",
    ].join("\n");
    this.db.exec(schema);
    const applied = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 1").get() as Row | undefined;
    if (!applied) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(1, ?)").run(new Date().toISOString());
    }
    const jobColumns = this.db.prepare("PRAGMA table_info(jobs)").all() as Row[];
    if (!jobColumns.some((column) => column.name === "permission_id")) {
      this.db.exec("ALTER TABLE jobs ADD COLUMN permission_id TEXT");
    }
    const permissionMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 2").get() as Row | undefined;
    if (!permissionMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(2, ?)").run(new Date().toISOString());
    }
    const bindingUniquenessMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 3").get() as Row | undefined;
    if (!bindingUniquenessMigration) {
      const duplicate = this.db.prepare(
        "SELECT thread_id, originating_turn_id, originating_item_id, COUNT(*) AS count FROM codex_bindings WHERE originating_turn_id IS NOT NULL AND originating_item_id IS NOT NULL GROUP BY thread_id, originating_turn_id, originating_item_id HAVING COUNT(*) > 1 LIMIT 1",
      ).get() as Row | undefined;
      if (duplicate) {
        throw new Error("Cannot enforce unique Codex correlation: existing duplicate binding tuple requires manual review");
      }
      this.db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_codex_bindings_correlation ON codex_bindings(thread_id, originating_turn_id, originating_item_id) WHERE originating_turn_id IS NOT NULL AND originating_item_id IS NOT NULL");
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(3, ?)").run(new Date().toISOString());
    } else {
      this.db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_codex_bindings_correlation ON codex_bindings(thread_id, originating_turn_id, originating_item_id) WHERE originating_turn_id IS NOT NULL AND originating_item_id IS NOT NULL");
    }
    const followColumns = this.db.prepare("PRAGMA table_info(jobs)").all() as Row[];
    for (const [name, definition] of [
      ["follow_started_at", "TEXT"],
      ["follow_deadline_at", "TEXT"],
      ["follow_grace_minutes", "REAL"],
      ["grace_deadline_at", "TEXT"],
      ["graceful_finalize_attempted", "INTEGER NOT NULL DEFAULT 0"],
      ["approval_deadline_at", "TEXT"],
      ["hint_thread_id", "TEXT"],
      ["hint_turn_id", "TEXT"],
      ["hint_source", "TEXT"],
      ["dispatch_unknown", "INTEGER NOT NULL DEFAULT 0"],
    ] as const) {
      if (!followColumns.some((column) => column.name === name)) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN " + name + " " + definition);
      }
    }
    const followMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 4").get() as Row | undefined;
    if (!followMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(4, ?)").run(new Date().toISOString());
    }
    const hintMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 5").get() as Row | undefined;
    if (!hintMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(5, ?)").run(new Date().toISOString());
    }
    const dispatchUnknownMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 6").get() as Row | undefined;
    if (!dispatchUnknownMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(6, ?)").run(new Date().toISOString());
    }
    const agentColumns = this.db.prepare("PRAGMA table_info(agents)").all() as Row[];
    if (!agentColumns.some((column) => column.name === "model_route")) {
      this.db.exec("ALTER TABLE agents ADD COLUMN model_route TEXT");
    }
    const jobColumnsAfter = this.db.prepare("PRAGMA table_info(jobs)").all() as Row[];
    if (!jobColumnsAfter.some((column) => column.name === "result_consumed_at")) {
      this.db.exec("ALTER TABLE jobs ADD COLUMN result_consumed_at TEXT");
    }
    const routeMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 7").get() as Row | undefined;
    if (!routeMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(7, ?)").run(new Date().toISOString());
    }
    // Lineage: parent_agent_id links a respawned agent to the closed agent it
    // was created from. It is a diagnostic/provenance column only; it never
    // reopens the parent and never participates in state transitions.
    const lineageColumns = this.db.prepare("PRAGMA table_info(agents)").all() as Row[];
    if (!lineageColumns.some((column) => column.name === "parent_agent_id")) {
      this.db.exec("ALTER TABLE agents ADD COLUMN parent_agent_id TEXT");
    }
    const lineageMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 9").get() as Row | undefined;
    if (!lineageMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(9, ?)").run(new Date().toISOString());
    }
    // Intrinsic retention gate: the explicit offline CLI flow writes a marker
    // into the database; a hand-edited retentionMode alone can never arm
    // online pruning on a legacy database.
    this.db.exec("CREATE TABLE IF NOT EXISTS retention_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);");
    const retentionMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 8").get() as Row | undefined;
    if (!retentionMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(8, ?)").run(new Date().toISOString());
    }
    // Retention prune-support indexes are cheap on an empty database and are
    // created here; on a legacy (non-empty) database they are only created by
    // the explicit offline retention CLI path.
    if (this.isProvablyEmpty()) {
      this.db.exec("CREATE INDEX IF NOT EXISTS idx_events_received_at ON events(received_at);");
      this.db.exec("CREATE INDEX IF NOT EXISTS idx_agent_activity_created_at ON agent_activity(created_at);");
    }
  }

  /** True only when no business rows exist at all (fresh database). */
  isProvablyEmpty(): boolean {
    const row = this.db.prepare(
      "SELECT (SELECT COUNT(*) FROM agents) + (SELECT COUNT(*) FROM jobs) + (SELECT COUNT(*) FROM events) + (SELECT COUNT(*) FROM agent_activity) + (SELECT COUNT(*) FROM deliveries) + (SELECT COUNT(*) FROM codex_bindings) AS total",
    ).get() as Row;
    return numberValue(row, "total") === 0;
  }

  /**
   * Records explicit offline retention preparation (the `retention dry-run` or
   * `retention enabled --confirm` CLI flow). Without this in-database marker,
   * online pruning never runs on a non-empty legacy database, even if the
   * config file was hand-edited to retentionMode=enabled.
   */
  markRetentionPrepared(): void {
    this.db.prepare(
      "INSERT INTO retention_meta(key, value, updated_at) VALUES('legacy_prepared', ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
    ).run(new Date().toISOString(), new Date().toISOString());
  }

  isRetentionPrepared(): boolean {
    const row = this.db.prepare("SELECT 1 AS found FROM retention_meta WHERE key = 'legacy_prepared'").get() as Row | undefined;
    return row !== undefined;
  }

  createAgent(input: {
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
    modelRoute?: string | null;
    parentAgentId?: string | null;
  }): AgentRecord {
    const now = new Date().toISOString();
    this.db.prepare("INSERT INTO agents (id,title,topic,repository_root,workspace_path,workspace_strategy,opencode_server_id,opencode_session_id,model_provider_id,model_id,model_variant,model_route,parent_agent_id,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").run(
      input.id,
      input.title,
      input.topic,
      input.repositoryRoot,
      input.workspacePath,
      input.workspaceStrategy,
      input.opencodeServerId,
      input.opencodeSessionId,
      input.modelProviderId,
      input.modelId,
      input.modelVariant,
      input.modelRoute ?? null,
      input.parentAgentId ?? null,
      "created",
      now,
      now,
    );
    const agent = this.getAgent(input.id);
    if (!agent) throw new Error("Agent was not persisted");
    return agent;
  }

  createJob(input: { id: string; agentId: string; kind: JobKind; requestId: string; promptHash: string }): JobRecord {
    const sequenceRow = this.db.prepare("SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM jobs WHERE agent_id = ?").get(input.agentId) as Row;
    const sequence = numberValue(sequenceRow, "sequence");
    const now = new Date().toISOString();
    this.db.prepare("INSERT INTO jobs(id,agent_id,sequence,kind,request_id,prompt_hash,status,created_at) VALUES(?,?,?,?,?,?,?,?)").run(
      input.id,
      input.agentId,
      sequence,
      input.kind,
      input.requestId,
      input.promptHash,
      "created",
      now,
    );
    const job = this.getJob(input.id);
    if (!job) throw new Error("Job was not persisted");
    return job;
  }

  getAgent(id: string): AgentRecord | null {
    const row = this.db.prepare("SELECT * FROM agents WHERE id = ?").get(id) as Row | undefined;
    return row ? this.toAgent(row) : null;
  }

  getAgentBySession(sessionId: string): AgentRecord | null {
    const row = this.db.prepare("SELECT * FROM agents WHERE opencode_session_id = ?").get(sessionId) as Row | undefined;
    return row ? this.toAgent(row) : null;
  }

  listAgents(): AgentRecord[] {
    const rows = this.db.prepare("SELECT * FROM agents ORDER BY created_at DESC").all() as Row[];
    return rows.map((row) => this.toAgent(row));
  }

  getJob(id: string): JobRecord | null {
    const row = this.db.prepare("SELECT * FROM jobs WHERE id = ?").get(id) as Row | undefined;
    return row ? this.toJob(row) : null;
  }

  getJobByRequestId(requestId: string): JobRecord | null {
    const row = this.db.prepare("SELECT * FROM jobs WHERE request_id = ?").get(requestId) as Row | undefined;
    return row ? this.toJob(row) : null;
  }

  listJobs(status?: JobStatus): JobRecord[] {
    const rows = status
      ? (this.db.prepare("SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC").all(status) as Row[])
      : (this.db.prepare("SELECT * FROM jobs ORDER BY created_at DESC").all() as Row[]);
    return rows.map((row) => this.toJob(row));
  }

  updateJobStatus(id: string, status: JobStatus, error: string | null = null): JobRecord {
    const current = this.getJob(id);
    if (!current) throw new Error("Unknown job: " + id);
    assertJobTransition(current.status, status);
    const now = new Date().toISOString();
    const startedAt = status === "running" && current.startedAt === null ? now : current.startedAt;
    const completedAt = ["completed", "completed_partial", "timed_out", "failed", "aborted"].includes(status) ? now : current.completedAt;
    this.db.prepare("UPDATE jobs SET status = ?, started_at = ?, completed_at = ?, error = ? WHERE id = ?").run(
      status,
      startedAt,
      completedAt,
      error,
      id,
    );
    const updated = this.getJob(id);
    if (!updated) throw new Error("Job disappeared: " + id);
    return updated;
  }

  setJobMessages(id: string, userMessageId: string | null, assistantMessageId: string | null): void {
    this.db.prepare("UPDATE jobs SET last_user_message_id = ?, last_assistant_message_id = ? WHERE id = ?").run(
      userMessageId,
      assistantMessageId,
      id,
    );
  }

  setJobPermission(id: string, permissionId: string | null): void {
    this.db.prepare("UPDATE jobs SET permission_id = ? WHERE id = ?").run(permissionId, id);
  }

  setFollowWindow(id: string, input: {
    startedAt: string;
    deadlineAt: string;
    graceMinutes?: number | null;
    graceDeadlineAt?: string | null;
    gracefulFinalizeAttempted?: boolean;
  }): JobRecord {
    this.db.prepare("UPDATE jobs SET follow_started_at = ?, follow_deadline_at = ?, follow_grace_minutes = ?, grace_deadline_at = ?, graceful_finalize_attempted = ? WHERE id = ?").run(
      input.startedAt,
      input.deadlineAt,
      input.graceMinutes ?? null,
      input.graceDeadlineAt ?? null,
      input.gracefulFinalizeAttempted === true ? 1 : 0,
      id,
    );
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  markGracefulFinalize(id: string, graceDeadlineAt: string): JobRecord {
    this.db.prepare("UPDATE jobs SET grace_deadline_at = ?, graceful_finalize_attempted = 1 WHERE id = ?").run(
      graceDeadlineAt,
      id,
    );
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  clearFollowWindow(id: string): JobRecord {
    this.db.prepare("UPDATE jobs SET follow_started_at = NULL, follow_deadline_at = NULL, follow_grace_minutes = NULL, grace_deadline_at = NULL, graceful_finalize_attempted = 0 WHERE id = ?").run(id);
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  setApprovalDeadline(id: string, deadlineAt: string | null): JobRecord {
    this.db.prepare("UPDATE jobs SET approval_deadline_at = ? WHERE id = ?").run(deadlineAt, id);
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  setJobError(id: string, error: string | null): JobRecord {
    this.db.prepare("UPDATE jobs SET error = ? WHERE id = ?").run(error, id);
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  setCorrelationHint(id: string, input: { threadId?: string | null; turnId?: string | null; source: string }): JobRecord {
    const current = this.getJob(id);
    if (!current) throw new Error("Unknown job: " + id);
    const threadId = input.threadId ?? current.hintThreadId;
    const turnId = input.turnId ?? current.hintTurnId;
    const source = threadId || turnId ? input.source : null;
    this.db.prepare("UPDATE jobs SET hint_thread_id = ?, hint_turn_id = ?, hint_source = ? WHERE id = ?").run(
      threadId,
      turnId,
      source,
      id,
    );
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  markDispatchUnknown(id: string): JobRecord {
    this.db.prepare("UPDATE jobs SET dispatch_unknown = 1 WHERE id = ?").run(id);
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  countJobsWithCorrelationHints(): number {
    const row = this.db.prepare("SELECT COUNT(*) AS count FROM jobs WHERE hint_thread_id IS NOT NULL OR hint_turn_id IS NOT NULL").get() as Row;
    return numberValue(row, "count");
  }

  countCodexBindings(): number {
    const row = this.db.prepare("SELECT COUNT(*) AS count FROM codex_bindings").get() as Row;
    return numberValue(row, "count");
  }

  hasActivity(input: { agentId: string; jobId: string; activityType: ActivityType }): boolean {
    const row = this.db.prepare("SELECT 1 AS found FROM agent_activity WHERE agent_id = ? AND job_id = ? AND activity_type = ? LIMIT 1").get(
      input.agentId,
      input.jobId,
      input.activityType,
    ) as Row | undefined;
    return row !== undefined;
  }

  setJobResult(id: string, resultPath: string, summary: string): JobRecord {
    this.db.prepare("UPDATE jobs SET result_path = ?, result_summary = ? WHERE id = ?").run(resultPath, summary, id);
    const updated = this.getJob(id);
    if (!updated) throw new Error("Job disappeared: " + id);
    return updated;
  }

  /**
   * Marks a terminal job result as explicitly consumed (follow/recover
   * returned a usable final result). Idempotent; only meaningful for jobs
   * with a persisted result.
   */
  consumeResult(id: string): JobRecord {
    const current = this.getJob(id);
    if (!current) throw new Error("Unknown job: " + id);
    if (!current.resultPath) return current;
    if (current.resultConsumedAt === null) {
      this.db.prepare("UPDATE jobs SET result_consumed_at = ? WHERE id = ?").run(new Date().toISOString(), id);
    }
    const updated = this.getJob(id);
    if (!updated) throw new Error("Job disappeared: " + id);
    return updated;
  }

  countUnconsumedTerminalResults(): number {
    const row = this.db.prepare(
      "SELECT COUNT(*) AS count FROM jobs WHERE result_path IS NOT NULL AND result_consumed_at IS NULL",
    ).get() as Row;
    return numberValue(row, "count");
  }

  listUnconsumedTerminalResults(): JobRecord[] {
    return (this.db.prepare(
      "SELECT * FROM jobs WHERE result_path IS NOT NULL AND result_consumed_at IS NULL ORDER BY created_at DESC",
    ).all() as Row[]).map((row) => this.toJob(row));
  }

  countOpenTerminalAgents(): number {
    const row = this.db.prepare(
      "SELECT COUNT(*) AS count FROM agents WHERE status IN ('completed','completed_partial','timed_out','failed') AND closed_at IS NULL",
    ).get() as Row;
    return numberValue(row, "count");
  }

  countOpenObligations(): number {
    const row = this.db.prepare(
      "SELECT COUNT(*) AS count FROM jobs WHERE status IN ('dispatching','running','following','finalizing','needs_approval')",
    ).get() as Row;
    return numberValue(row, "count");
  }

  /**
   * Counts genuinely stale follow windows: a job still following/finalizing
   * after its grace deadline AND not auto-armed (auto-armed windows are
   * safety nets for unknown dispatch outcomes and are never flagged). Fresh
   * windows whose deadline has not passed are never counted.
   */
  countStaleFollowWindows(now = Date.now()): number {
    const row = this.db.prepare(
      "SELECT COUNT(*) AS count FROM jobs WHERE status IN ('following','finalizing') AND grace_deadline_at IS NOT NULL AND grace_deadline_at < ? AND dispatch_unknown = 0",
    ).get(new Date(now).toISOString()) as Row;
    return numberValue(row, "count");
  }

  /** Job ids whose events/activity must never be pruned: active jobs plus
   *  terminal jobs with an unconsumed or undelivered result. */
  protectedJobIds(): Set<string> {
    const rows = this.db.prepare(
      "SELECT id FROM jobs WHERE status IN ('dispatching','running','following','finalizing','needs_approval','delivery_pending') OR (result_path IS NOT NULL AND result_consumed_at IS NULL)",
    ).all() as Row[];
    return new Set(rows.map((row) => stringValue(row, "id")));
  }

  recordActivity(input: {
    agentId: string;
    jobId?: string | null;
    sessionId?: string | null;
    activityType: ActivityType;
    summary: string;
    metadata?: Record<string, unknown>;
  }): AgentActivity {
    const createdAt = new Date().toISOString();
    const summary = truncate(redactSecrets(input.summary.replace(/[\r\n]+/g, " ").trim()), 500);
    const metadataJson = input.metadata
      ? truncate(redactSecrets(JSON.stringify(input.metadata)), 2_000)
      : null;
    this.db.prepare("INSERT INTO agent_activity(id,agent_id,job_id,session_id,activity_type,summary,metadata_json,created_at) VALUES(?,?,?,?,?,?,?,?)").run(
      newId("activity"),
      input.agentId,
      input.jobId ?? null,
      input.sessionId ?? null,
      input.activityType,
      summary || "Observable activity recorded",
      metadataJson,
      createdAt,
    );
    const row = this.db.prepare("SELECT * FROM agent_activity WHERE agent_id = ? AND created_at = ? ORDER BY id DESC LIMIT 1").get(input.agentId, createdAt) as Row | undefined;
    if (!row) throw new Error("Activity was not persisted");
    return this.toActivity(row);
  }

  listActivity(agentId: string, limit = 20): AgentActivity[] {
    const bounded = Math.max(1, Math.min(20, Math.trunc(limit)));
    const rows = this.db.prepare("SELECT * FROM agent_activity WHERE agent_id = ? ORDER BY created_at DESC, id DESC LIMIT ?").all(agentId, bounded) as Row[];
    return rows.map((row) => this.toActivity(row));
  }

  updateAgentStatus(id: string, status: AgentStatus, error: string | null = null): AgentRecord {
    const current = this.getAgent(id);
    if (!current) throw new Error("Unknown agent: " + id);
    assertAgentTransition(current.status, status);
    const now = new Date().toISOString();
    const closedAt = status === "closed" ? now : current.closedAt;
    this.db.prepare("UPDATE agents SET status = ?, updated_at = ?, closed_at = ?, last_error = ? WHERE id = ?").run(
      status,
      now,
      closedAt,
      error,
      id,
    );
    const updated = this.getAgent(id);
    if (!updated) throw new Error("Agent disappeared: " + id);
    return updated;
  }

  bindJob(binding: Omit<CodexBinding, "boundAt"> & { boundAt?: string }): CodexBinding {
    const existing = this.getBinding(binding.jobId);
    if (existing) {
      const matches = existing.threadId === binding.threadId &&
        existing.originatingTurnId === binding.originatingTurnId &&
        existing.originatingItemId === binding.originatingItemId;
      if (!matches) {
        throw new Error("Conflicting Codex binding for job " + binding.jobId);
      }
      return existing;
    }
    if (binding.originatingTurnId !== null && binding.originatingItemId !== null) {
      const owner = this.getBindingByCorrelation(binding.threadId, binding.originatingTurnId, binding.originatingItemId);
      if (owner && owner.jobId !== binding.jobId) {
        throw new Error("Codex correlation tuple is already bound to job " + owner.jobId);
      }
    }
    const boundAt = binding.boundAt ?? new Date().toISOString();
    try {
      this.db.prepare("INSERT INTO codex_bindings(job_id,thread_id,originating_turn_id,originating_item_id,bound_at) VALUES(?,?,?,?,?)").run(
        binding.jobId,
        binding.threadId,
        binding.originatingTurnId,
        binding.originatingItemId,
        boundAt,
      );
    } catch (error) {
      const owner = binding.originatingTurnId !== null && binding.originatingItemId !== null
        ? this.getBindingByCorrelation(binding.threadId, binding.originatingTurnId, binding.originatingItemId)
        : null;
      if (owner && owner.jobId !== binding.jobId) {
        throw new Error("Codex correlation tuple is already bound to job " + owner.jobId);
      }
      throw error;
    }
    const result = this.getBinding(binding.jobId);
    if (!result) throw new Error("Binding was not persisted");
    return result;
  }

  getBinding(jobId: string): CodexBinding | null {
    const row = this.db.prepare("SELECT * FROM codex_bindings WHERE job_id = ?").get(jobId) as Row | undefined;
    if (!row) return null;
    return {
      jobId: stringValue(row, "job_id"),
      threadId: stringValue(row, "thread_id"),
      originatingTurnId: nullableString(row, "originating_turn_id"),
      originatingItemId: nullableString(row, "originating_item_id"),
      boundAt: stringValue(row, "bound_at"),
    };
  }

  getBindingByCorrelation(threadId: string, originatingTurnId: string, originatingItemId: string): CodexBinding | null {
    const row = this.db.prepare(
      "SELECT * FROM codex_bindings WHERE thread_id = ? AND originating_turn_id = ? AND originating_item_id = ?",
    ).get(threadId, originatingTurnId, originatingItemId) as Row | undefined;
    if (!row) return null;
    return {
      jobId: stringValue(row, "job_id"),
      threadId: stringValue(row, "thread_id"),
      originatingTurnId: nullableString(row, "originating_turn_id"),
      originatingItemId: nullableString(row, "originating_item_id"),
      boundAt: stringValue(row, "bound_at"),
    };
  }

  getLatestBindingForAgent(agentId: string): CodexBinding | null {
    const row = this.db.prepare(
      "SELECT b.* FROM codex_bindings b JOIN jobs j ON j.id = b.job_id WHERE j.agent_id = ? ORDER BY b.bound_at DESC LIMIT 1",
    ).get(agentId) as Row | undefined;
    if (!row) return null;
    return {
      jobId: stringValue(row, "job_id"),
      threadId: stringValue(row, "thread_id"),
      originatingTurnId: nullableString(row, "originating_turn_id"),
      originatingItemId: nullableString(row, "originating_item_id"),
      boundAt: stringValue(row, "bound_at"),
    };
  }

  createDelivery(input: { jobId: string; threadId: string; expectedTurnId: string | null; deliveryMethod: DeliveryMethod }): DeliveryRecord {
    const existing = this.db.prepare("SELECT * FROM deliveries WHERE job_id = ?").get(input.jobId) as Row | undefined;
    if (existing) return this.toDelivery(existing);
    const now = new Date().toISOString();
    const id = newId("delivery");
    this.db.prepare("INSERT INTO deliveries(id,job_id,thread_id,expected_turn_id,delivery_method,status,attempts,created_at) VALUES(?,?,?,?,?,?,0,?)").run(
      id,
      input.jobId,
      input.threadId,
      input.expectedTurnId,
      input.deliveryMethod,
      "pending",
      now,
    );
    const delivery = this.getDeliveryByJob(input.jobId);
    if (!delivery) throw new Error("Delivery was not persisted");
    return delivery;
  }

  getDeliveryByJob(jobId: string): DeliveryRecord | null {
    const row = this.db.prepare("SELECT * FROM deliveries WHERE job_id = ?").get(jobId) as Row | undefined;
    return row ? this.toDelivery(row) : null;
  }

  updateDelivery(id: string, status: DeliveryStatus, error: string | null = null): DeliveryRecord {
    const current = this.db.prepare("SELECT * FROM deliveries WHERE id = ?").get(id) as Row | undefined;
    if (!current) throw new Error("Unknown delivery: " + id);
    const now = new Date().toISOString();
    this.db.prepare("UPDATE deliveries SET status = ?, attempts = attempts + 1, delivered_at = ?, last_error = ? WHERE id = ?").run(
      status,
      status === "delivered" ? now : null,
      error,
      id,
    );
    const delivery = this.db.prepare("SELECT * FROM deliveries WHERE id = ?").get(id) as Row | undefined;
    if (!delivery) throw new Error("Delivery disappeared: " + id);
    return this.toDelivery(delivery);
  }

  setDeliveryMethod(id: string, method: DeliveryMethod, error: string | null = null): DeliveryRecord {
    this.db.prepare("UPDATE deliveries SET delivery_method = ?, last_error = ? WHERE id = ?").run(method, error, id);
    const delivery = this.db.prepare("SELECT * FROM deliveries WHERE id = ?").get(id) as Row | undefined;
    if (!delivery) throw new Error("Delivery disappeared: " + id);
    return this.toDelivery(delivery);
  }

  insertEvent(input: { source: string; sourceEventId: string; eventType: string; sessionId: string | null; jobId: string | null }): boolean {
    const result = this.db.prepare("INSERT OR IGNORE INTO events(id,source,source_event_id,event_type,session_id,job_id,received_at) VALUES(?,?,?,?,?,?,?)").run(
      newId("event"),
      input.source,
      input.sourceEventId,
      input.eventType,
      input.sessionId,
      input.jobId,
      new Date().toISOString(),
    );
    return Number(result.changes) === 1;
  }

  markEventProcessed(source: string, sourceEventId: string): void {
    this.db.prepare("UPDATE events SET processed_at = ? WHERE source = ? AND source_event_id = ?").run(
      new Date().toISOString(),
      source,
      sourceEventId,
    );
  }

  isEventProcessed(source: string, sourceEventId: string): boolean {
    const row = this.db.prepare("SELECT processed_at FROM events WHERE source = ? AND source_event_id = ?").get(source, sourceEventId) as Row | undefined;
    return typeof row?.processed_at === "string" && row.processed_at.length > 0;
  }

  listInbox(): JobRecord[] {
    return this.listJobs().filter((job) => job.resultPath !== null && job.status !== "delivered");
  }

  recoverPendingJobs(): JobRecord[] {
    return this.listJobs().filter((job) => ["dispatching", "running", "following", "finalizing", "needs_approval", "completed", "completed_partial", "timed_out", "delivery_pending"].includes(job.status));
  }

  registerServer(input: { id: string; workspaceRoot: string; baseUrl: string; processId: number | null }): void {
    this.db.prepare("INSERT INTO servers(id,workspace_root,base_url,process_id,status,started_at) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET workspace_root=excluded.workspace_root,base_url=excluded.base_url,process_id=excluded.process_id,status=excluded.status,started_at=excluded.started_at,stopped_at=NULL").run(
      input.id,
      input.workspaceRoot,
      input.baseUrl,
      input.processId,
      "running",
      new Date().toISOString(),
    );
  }

  stopServers(): void {
    this.db.prepare("UPDATE servers SET status = 'stopped', stopped_at = ? WHERE status = 'running'").run(new Date().toISOString());
  }

  private toAgent(row: Row): AgentRecord {
    return {
      id: stringValue(row, "id"),
      title: stringValue(row, "title"),
      topic: stringValue(row, "topic"),
      repositoryRoot: stringValue(row, "repository_root"),
      workspacePath: stringValue(row, "workspace_path"),
      workspaceStrategy: stringValue(row, "workspace_strategy") as WorkspaceStrategy,
      opencodeServerId: stringValue(row, "opencode_server_id"),
      opencodeSessionId: stringValue(row, "opencode_session_id"),
      modelProviderId: stringValue(row, "model_provider_id"),
      modelId: stringValue(row, "model_id"),
      modelVariant: nullableString(row, "model_variant"),
      modelRoute: nullableString(row, "model_route"),
      parentAgentId: nullableString(row, "parent_agent_id"),
      status: stringValue(row, "status") as AgentStatus,
      createdAt: stringValue(row, "created_at"),
      updatedAt: stringValue(row, "updated_at"),
      closedAt: nullableString(row, "closed_at"),
      lastError: nullableString(row, "last_error"),
    };
  }

  private toJob(row: Row): JobRecord {
    return {
      id: stringValue(row, "id"),
      agentId: stringValue(row, "agent_id"),
      sequence: numberValue(row, "sequence"),
      kind: stringValue(row, "kind") as JobKind,
      requestId: stringValue(row, "request_id"),
      promptHash: stringValue(row, "prompt_hash"),
      status: stringValue(row, "status") as JobStatus,
      createdAt: stringValue(row, "created_at"),
      startedAt: nullableString(row, "started_at"),
      completedAt: nullableString(row, "completed_at"),
      lastUserMessageId: nullableString(row, "last_user_message_id"),
      lastAssistantMessageId: nullableString(row, "last_assistant_message_id"),
      permissionId: nullableString(row, "permission_id"),
      resultPath: nullableString(row, "result_path"),
      resultSummary: nullableString(row, "result_summary"),
      error: nullableString(row, "error"),
      followStartedAt: nullableString(row, "follow_started_at"),
      followDeadlineAt: nullableString(row, "follow_deadline_at"),
      followGraceMinutes: row.follow_grace_minutes === null || row.follow_grace_minutes === undefined ? null : Number(row.follow_grace_minutes),
      graceDeadlineAt: nullableString(row, "grace_deadline_at"),
      gracefulFinalizeAttempted: numberValue(row, "graceful_finalize_attempted") === 1,
      approvalDeadlineAt: nullableString(row, "approval_deadline_at"),
      hintThreadId: nullableString(row, "hint_thread_id"),
      hintTurnId: nullableString(row, "hint_turn_id"),
      hintSource: nullableString(row, "hint_source"),
      dispatchUnknown: numberValue(row, "dispatch_unknown") === 1,
      resultConsumedAt: nullableString(row, "result_consumed_at"),
    };
  }

  private toActivity(row: Row): AgentActivity {
    return {
      id: stringValue(row, "id"),
      agentId: stringValue(row, "agent_id"),
      jobId: nullableString(row, "job_id"),
      sessionId: nullableString(row, "session_id"),
      activityType: stringValue(row, "activity_type") as ActivityType,
      summary: stringValue(row, "summary"),
      createdAt: stringValue(row, "created_at"),
    };
  }

  private toDelivery(row: Row): DeliveryRecord {
    return {
      id: stringValue(row, "id"),
      jobId: stringValue(row, "job_id"),
      threadId: stringValue(row, "thread_id"),
      expectedTurnId: nullableString(row, "expected_turn_id"),
      deliveryMethod: stringValue(row, "delivery_method") as DeliveryMethod,
      status: stringValue(row, "status") as DeliveryStatus,
      attempts: numberValue(row, "attempts"),
      createdAt: stringValue(row, "created_at"),
      deliveredAt: nullableString(row, "delivered_at"),
      lastError: nullableString(row, "last_error"),
    };
  }
}
