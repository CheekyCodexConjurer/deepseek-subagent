import { DatabaseSync } from "node:sqlite";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { assertAgentTransition, assertJobTransition } from "./state.js";
import { newId, redactSecrets, truncate } from "./security.js";
import { ConflictError } from "./errors.js";
import type {
  ActivityType,
  AgentActivity,
  AgentMode,
  AgentRecord,
  AgentStatus,
  BatchRecord,
  CodexBinding,
  DeliveryMethod,
  DeliveryRecord,
  DeliveryStatus,
  JobKind,
  JobRecord,
  JobStatus,
  ParkBarrierRecord,
  ParkPredicateType,
  WakeOutboxRecord,
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

  integrityCheck(options?: { full?: boolean | undefined }): string {
    if (options?.full) {
      const row = this.db.prepare("PRAGMA quick_check").get() as Row | undefined;
      return row?.quick_check === undefined ? "unknown" : String(row.quick_check);
    }
    const row = this.db.prepare("SELECT 1 AS ok FROM schema_migrations LIMIT 1").get() as Row | undefined;
    return row?.ok === 1 ? "ok" : "unknown";
  }

  createSnapshot(destinationPath: string): void {
    this.db.exec(`VACUUM INTO '${destinationPath.replace(/'/g, "''")}';`);
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
    // Operator-controlled active model route pointer. Additive state: the
    // pointer lives in the database (never rewritten into config.json while a
    // daemon is live); the effective route is this pointer when set, otherwise
    // the configured default route.
    this.db.exec("CREATE TABLE IF NOT EXISTS route_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);");
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
    const routeStateMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 10").get() as Row | undefined;
    if (!routeStateMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(10, ?)").run(new Date().toISOString());
    }
    const agentCols = this.db.prepare("PRAGMA table_info(agents)").all() as Row[];
    if (!agentCols.some((column) => column.name === "mode")) {
      this.db.exec("ALTER TABLE agents ADD COLUMN mode TEXT NOT NULL DEFAULT 'analyze'");
    }
    const modeMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 11").get() as Row | undefined;
    if (!modeMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(11, ?)").run(new Date().toISOString());
    }
    const jobCols = this.db.prepare("PRAGMA table_info(jobs)").all() as Row[];
    for (const [name, definition] of [
      ["fallback_from", "TEXT"],
      ["fallback_to", "TEXT"],
      ["fallback_reason", "TEXT"],
      ["fallback_status", "TEXT"],
      ["fallback_count", "INTEGER NOT NULL DEFAULT 0"],
    ] as const) {
      if (!jobCols.some((column) => column.name === name)) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN " + name + " " + definition);
      }
    }
    const fallbackMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 12").get() as Row | undefined;
    if (!fallbackMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(12, ?)").run(new Date().toISOString());
    }
    const livenessCols = this.db.prepare("PRAGMA table_info(jobs)").all() as Row[];
    for (const [name, definition] of [
      ["lease_expires_at", "TEXT"],
      ["attempt", "TEXT"],
      ["fence", "INTEGER NOT NULL DEFAULT 1"],
      ["worker_pid", "INTEGER"],
      ["heartbeat_at", "TEXT"],
    ] as const) {
      if (!livenessCols.some((column) => column.name === name)) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN " + name + " " + definition);
      }
    }
    const livenessMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 13").get() as Row | undefined;
    if (!livenessMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(13, ?)").run(new Date().toISOString());
    }
    const adaptiveCols = this.db.prepare("PRAGMA table_info(jobs)").all() as Row[];
    for (const [name, definition] of [
      ["early_exit_at", "TEXT"],
      ["early_exit_reason", "TEXT"],
      ["escalation_proposal", "TEXT"],
    ] as const) {
      if (!adaptiveCols.some((column) => column.name === name)) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN " + name + " " + definition);
      }
    }
    const adaptiveMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 14").get() as Row | undefined;
    if (!adaptiveMigration) {
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(14, ?)").run(new Date().toISOString());
    }
    const parkMigration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 15").get() as Row | undefined;
    if (!parkMigration) {
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS park_barriers (
          id TEXT PRIMARY KEY,
          thread_id TEXT NOT NULL,
          turn_id TEXT,
          generation INTEGER NOT NULL DEFAULT 1,
          armed INTEGER NOT NULL DEFAULT 0,
          state TEXT NOT NULL CHECK (state IN ('armed','waking','woken','idle','cancelled')),
          reason TEXT,
          goal_id TEXT,
          paused_by_bridge INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS park_jobs (
          park_id TEXT NOT NULL REFERENCES park_barriers(id) ON DELETE CASCADE,
          job_id TEXT NOT NULL REFERENCES jobs(id),
          created_at TEXT NOT NULL,
          PRIMARY KEY(park_id, job_id)
        );
        CREATE INDEX IF NOT EXISTS idx_park_jobs_job ON park_jobs(job_id);
        CREATE TABLE IF NOT EXISTS wake_outbox (
          id TEXT PRIMARY KEY,
          park_id TEXT NOT NULL REFERENCES park_barriers(id),
          generation INTEGER NOT NULL,
          thread_id TEXT NOT NULL,
          turn_id TEXT,
          status TEXT NOT NULL CHECK (status IN ('pending','waking','delivered','failed')),
          wake_marker TEXT NOT NULL,
          reason TEXT,
          payload_json TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          woken_at TEXT,
          last_error TEXT,
          UNIQUE(park_id, generation)
        );
        CREATE INDEX IF NOT EXISTS idx_wake_outbox_status ON wake_outbox(status);
        CREATE TABLE IF NOT EXISTS bridge_goals (
          goal_id TEXT PRIMARY KEY,
          goal_hash TEXT NOT NULL,
          paused_at TEXT NOT NULL,
          invalidated_at TEXT
        );
      `);
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(15, ?)").run(new Date().toISOString());
    }
    const v16Migration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 16").get() as Row | undefined;
    if (!v16Migration) {
      const barrierCols = (this.db.prepare("PRAGMA table_info(park_barriers)").all() as Row[]).map((c) => stringValue(c, "name"));
      if (!barrierCols.includes("delivery_mode")) {
        this.db.exec("ALTER TABLE park_barriers ADD COLUMN delivery_mode TEXT NOT NULL DEFAULT 'none';");
      }
      if (!barrierCols.includes("mcp_session_id")) {
        this.db.exec("ALTER TABLE park_barriers ADD COLUMN mcp_session_id TEXT;");
      }

      const outboxTableSql = ((this.db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='wake_outbox'").get() as Row | undefined)?.sql as string | undefined) ?? "";
      const outboxCols = (this.db.prepare("PRAGMA table_info(wake_outbox)").all() as Row[]).map((c) => stringValue(c, "name"));
      if (!outboxTableSql.includes("deferred_active_writer") || !outboxCols.includes("wake_state")) {
        this.db.exec(`
          CREATE TABLE IF NOT EXISTS wake_outbox_v16 (
            id TEXT PRIMARY KEY,
            park_id TEXT NOT NULL REFERENCES park_barriers(id),
            generation INTEGER NOT NULL,
            thread_id TEXT NOT NULL,
            turn_id TEXT,
            delivery_mode TEXT NOT NULL DEFAULT 'cli_resume',
            status TEXT NOT NULL CHECK (status IN ('pending','waking','deferred_active_writer','delivered','failed')),
            wake_state TEXT NOT NULL DEFAULT 'waiting' CHECK (wake_state IN ('waiting','deferred_active_writer','delivered','failed')),
            wake_marker TEXT NOT NULL,
            reason TEXT,
            payload_json TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            selected_executable TEXT,
            executable_version TEXT,
            created_at TEXT NOT NULL,
            woken_at TEXT,
            last_error TEXT,
            UNIQUE(park_id, generation)
          );
          INSERT OR IGNORE INTO wake_outbox_v16 (
            id, park_id, generation, thread_id, turn_id, status, wake_marker, reason, payload_json, attempts, created_at, woken_at, last_error
          ) SELECT id, park_id, generation, thread_id, turn_id, status, wake_marker, reason, payload_json, attempts, created_at, woken_at, last_error FROM wake_outbox;
          DROP TABLE wake_outbox;
          ALTER TABLE wake_outbox_v16 RENAME TO wake_outbox;
          CREATE INDEX IF NOT EXISTS idx_wake_outbox_status ON wake_outbox(status);
        `);
      }

      const jobCols = (this.db.prepare("PRAGMA table_info(jobs)").all() as Row[]).map((c) => stringValue(c, "name"));
      if (!jobCols.includes("mcp_session_id")) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN mcp_session_id TEXT;");
      }
      if (!jobCols.includes("trusted_thread_id")) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN trusted_thread_id TEXT;");
      }

      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(16, ?)").run(new Date().toISOString());
    }
    const v17Migration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 17").get() as Row | undefined;
    if (!v17Migration) {
      const barrierCols = (this.db.prepare("PRAGMA table_info(park_barriers)").all() as Row[]).map((c) => stringValue(c, "name"));
      if (!barrierCols.includes("predicate_type")) {
        this.db.exec("ALTER TABLE park_barriers ADD COLUMN predicate_type TEXT NOT NULL DEFAULT 'ALL';");
      }
      if (!barrierCols.includes("quorum_count")) {
        this.db.exec("ALTER TABLE park_barriers ADD COLUMN quorum_count INTEGER;");
      }
      if (!barrierCols.includes("required_job_ids")) {
        this.db.exec("ALTER TABLE park_barriers ADD COLUMN required_job_ids TEXT;");
      }
      if (!barrierCols.includes("wake_on_exception")) {
        this.db.exec("ALTER TABLE park_barriers ADD COLUMN wake_on_exception INTEGER NOT NULL DEFAULT 1;");
      }
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(17, ?)").run(new Date().toISOString());
    }
    const v18Migration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 18").get() as Row | undefined;
    if (!v18Migration) {
      const outboxCols = (this.db.prepare("PRAGMA table_info(wake_outbox)").all() as Row[]).map((c) => stringValue(c, "name"));
      const outboxTableSql = ((this.db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='wake_outbox'").get() as Row | undefined)?.sql as string | undefined) ?? "";
      if (!outboxTableSql.includes("superseded") || !outboxCols.includes("message_id")) {
        this.db.exec(`
          CREATE TABLE IF NOT EXISTS wake_outbox_v18 (
            id TEXT PRIMARY KEY,
            park_id TEXT NOT NULL REFERENCES park_barriers(id),
            generation INTEGER NOT NULL,
            thread_id TEXT NOT NULL,
            turn_id TEXT,
            delivery_mode TEXT NOT NULL DEFAULT 'cli_resume',
            status TEXT NOT NULL CHECK (status IN ('pending','waking','deferred_active_writer','delivered','failed','superseded')),
            wake_state TEXT NOT NULL DEFAULT 'waiting' CHECK (wake_state IN ('waiting','deferred_active_writer','delivered','failed','superseded')),
            wake_marker TEXT NOT NULL,
            reason TEXT,
            payload_json TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            selected_executable TEXT,
            executable_version TEXT,
            message_id TEXT,
            created_at TEXT NOT NULL,
            woken_at TEXT,
            last_error TEXT,
            UNIQUE(park_id, generation)
          );
          INSERT OR IGNORE INTO wake_outbox_v18 (
            id, park_id, generation, thread_id, turn_id, delivery_mode, status, wake_state, wake_marker, reason, payload_json, attempts, next_attempt_at, selected_executable, executable_version, message_id, created_at, woken_at, last_error
          ) SELECT id, park_id, generation, thread_id, turn_id, delivery_mode, status, wake_state, wake_marker, reason, payload_json, attempts, next_attempt_at, selected_executable, executable_version, ${outboxCols.includes("message_id") ? "message_id" : "NULL"}, created_at, woken_at, last_error FROM wake_outbox;
          DROP TABLE wake_outbox;
          ALTER TABLE wake_outbox_v18 RENAME TO wake_outbox;
          CREATE INDEX IF NOT EXISTS idx_wake_outbox_status ON wake_outbox(status);
        `);
      }
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(18, ?)").run(new Date().toISOString());
    }
    const v19Migration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 19").get() as Row | undefined;
    if (!v19Migration) {
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS batches (
          id TEXT PRIMARY KEY,
          request_id TEXT NOT NULL UNIQUE,
          batch_hash TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
      `);
      const jobCols = (this.db.prepare("PRAGMA table_info(jobs)").all() as Row[]).map((c) => stringValue(c, "name"));
      if (!jobCols.includes("batch_id")) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN batch_id TEXT REFERENCES batches(id);");
      }
      if (!jobCols.includes("priority")) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 50;");
      }
      if (!jobCols.includes("exclusive_resources")) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN exclusive_resources TEXT;");
      }
      if (!jobCols.includes("queued_at")) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN queued_at TEXT;");
      }
      if (!jobCols.includes("dispatched_at")) {
        this.db.exec("ALTER TABLE jobs ADD COLUMN dispatched_at TEXT;");
      }
      this.db.exec("CREATE INDEX IF NOT EXISTS idx_jobs_batch ON jobs(batch_id);");
      this.db.exec("CREATE INDEX IF NOT EXISTS idx_jobs_queued ON jobs(status, priority DESC, queued_at ASC);");
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(19, ?)").run(new Date().toISOString());
    }
    const v20Migration = this.db.prepare("SELECT 1 AS found FROM schema_migrations WHERE version = 20").get() as Row | undefined;
    if (!v20Migration) {
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS dispatch_envelopes (
          job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
          prompt TEXT NOT NULL,
          prompt_hash TEXT NOT NULL,
          worker_input_json TEXT NOT NULL,
          context_files_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_dispatch_envelopes_job ON dispatch_envelopes(job_id);
      `);
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(20, ?)").run(new Date().toISOString());
    }
  }

  /** True only when no business rows exist at all (fresh database). */
  isProvablyEmpty(): boolean {
    const hasParks = this.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='park_barriers'").get() !== undefined;
    const parkCountSql = hasParks ? "(SELECT COUNT(*) FROM park_barriers)" : "0";
    const hasBatches = this.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='batches'").get() !== undefined;
    const batchCountSql = hasBatches ? "(SELECT COUNT(*) FROM batches)" : "0";
    const row = this.db.prepare(
      `SELECT (SELECT COUNT(*) FROM agents) + (SELECT COUNT(*) FROM jobs) + (SELECT COUNT(*) FROM events) + (SELECT COUNT(*) FROM agent_activity) + (SELECT COUNT(*) FROM deliveries) + (SELECT COUNT(*) FROM codex_bindings) + ${parkCountSql} + ${batchCountSql} AS total`,
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

  /**
   * The operator-set active model route pointer, or null when no explicit
   * pointer exists (the effective route is then the configured default).
   * Written only by the daemon process through the authenticated loopback
   * control plane; the CLI never writes it while the daemon is stopped.
   */
  getActiveRoute(): string | null {
    const row = this.db.prepare("SELECT value FROM route_state WHERE key = 'active_route'").get() as Row | undefined;
    const value = row?.value;
    return typeof value === "string" && value.length > 0 ? value : null;
  }

  setActiveRoute(route: string): void {
    this.db.prepare(
      "INSERT INTO route_state(key, value, updated_at) VALUES('active_route', ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
    ).run(route, new Date().toISOString());
  }

  createAgent(input: {
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
    modelRoute?: string | null;
    parentAgentId?: string | null;
  }): AgentRecord {
    const now = new Date().toISOString();
    this.db.prepare("INSERT INTO agents (id,title,topic,repository_root,workspace_path,workspace_strategy,mode,opencode_server_id,opencode_session_id,model_provider_id,model_id,model_variant,model_route,parent_agent_id,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").run(
      input.id,
      input.title,
      input.topic,
      input.repositoryRoot,
      input.workspacePath,
      input.workspaceStrategy,
      input.mode ?? "analyze",
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

  createBatch(input: {
    id: string;
    requestId: string;
    batchHash: string;
    status?: string;
  }): BatchRecord {
    const now = new Date().toISOString();
    this.db.prepare("INSERT INTO batches (id, request_id, batch_hash, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)").run(
      input.id,
      input.requestId,
      input.batchHash,
      input.status ?? "queued",
      now,
      now,
    );
    const batch = this.getBatch(input.id);
    if (!batch) throw new Error("Batch was not persisted");
    return batch;
  }

  getBatch(id: string): BatchRecord | null {
    const row = this.db.prepare("SELECT * FROM batches WHERE id = ?").get(id) as Row | undefined;
    return row ? this.toBatch(row) : null;
  }

  getBatchByRequestId(requestId?: string | null): BatchRecord | null {
    if (!requestId) return null;
    const row = this.db.prepare("SELECT * FROM batches WHERE request_id = ?").get(requestId) as Row | undefined;
    return row ? this.toBatch(row) : null;
  }

  listBatchJobs(batchId: string): JobRecord[] {
    const rows = this.db.prepare("SELECT * FROM jobs WHERE batch_id = ? ORDER BY sequence ASC").all(batchId) as Row[];
    return rows.map((r) => this.toJob(r));
  }

  updateBatchStatus(id: string, status: string): void {
    this.db.prepare("UPDATE batches SET status = ?, updated_at = ? WHERE id = ?").run(status, new Date().toISOString(), id);
  }

  transaction<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE;");
    try {
      const result = fn();
      this.db.exec("COMMIT;");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK;");
      throw error;
    }
  }

  saveDispatchEnvelope(jobId: string, envelope: {
    prompt: string;
    promptHash: string;
    workerInput: unknown;
    contextFiles: string[];
  }): void {
    const now = new Date().toISOString();
    this.db.prepare(
      "INSERT INTO dispatch_envelopes(job_id, prompt, prompt_hash, worker_input_json, context_files_json, created_at) VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET prompt = excluded.prompt, prompt_hash = excluded.prompt_hash, worker_input_json = excluded.worker_input_json, context_files_json = excluded.context_files_json, created_at = excluded.created_at",
    ).run(
      jobId,
      envelope.prompt,
      envelope.promptHash,
      JSON.stringify(envelope.workerInput),
      JSON.stringify(envelope.contextFiles),
      now,
    );
  }

  getDispatchEnvelope(jobId: string): {
    jobId: string;
    prompt: string;
    promptHash: string;
    workerInput: Record<string, unknown>;
    contextFiles: string[];
    createdAt: string;
  } | null {
    const row = this.db.prepare("SELECT * FROM dispatch_envelopes WHERE job_id = ?").get(jobId) as Row | undefined;
    if (!row) return null;
    return {
      jobId: stringValue(row, "job_id"),
      prompt: stringValue(row, "prompt"),
      promptHash: stringValue(row, "prompt_hash"),
      workerInput: JSON.parse(stringValue(row, "worker_input_json")),
      contextFiles: JSON.parse(stringValue(row, "context_files_json")),
      createdAt: stringValue(row, "created_at"),
    };
  }

  deleteDispatchEnvelope(jobId: string): void {
    this.db.prepare("DELETE FROM dispatch_envelopes WHERE job_id = ?").run(jobId);
  }

  admitBatch(admission: {
    batch: {
      id: string;
      requestId: string;
      batchHash: string;
      status?: string;
    };
    items: Array<{
      agent: {
        id: string;
        title: string;
        topic: string;
        repositoryRoot: string;
        workspacePath: string;
        workspaceStrategy: WorkspaceStrategy;
        mode: AgentMode;
        opencodeServerId: string;
        opencodeSessionId: string;
        modelProviderId: string;
        modelId: string;
        modelVariant: string | null;
        modelRoute: string;
      };
      job: {
        id: string;
        agentId: string;
        kind: JobKind;
        status?: JobStatus;
        batchId?: string | null;
        priority?: number;
        exclusiveResources?: string[] | null;
        requestId: string;
        promptHash: string;
        queuedAt?: string | null;
        mcpSessionId?: string | null;
        trustedThreadId?: string | null;
      };
      correlationHint?: {
        threadId?: string | null | undefined;
        turnId?: string | null | undefined;
      } | undefined;
      dispatchEnvelope?: {
        prompt: string;
        promptHash: string;
        workerInput: unknown;
        contextFiles: string[];
      } | undefined;
    }>;
  }): void {
    this.transaction(() => {
      this.createBatch(admission.batch);
      for (const entry of admission.items) {
        this.createAgent(entry.agent);
        this.createJob(entry.job);
        if (entry.correlationHint?.threadId || entry.correlationHint?.turnId) {
          this.setCorrelationHint(entry.job.id, {
            threadId: entry.correlationHint.threadId ?? null,
            turnId: entry.correlationHint.turnId ?? null,
            source: "mcp",
          });
        }
        if (entry.dispatchEnvelope) {
          this.saveDispatchEnvelope(entry.job.id, entry.dispatchEnvelope);
        }
      }
    });
  }

  admitUnary(admission: {
    agent: {
      id: string;
      title: string;
      topic: string;
      repositoryRoot: string;
      workspacePath: string;
      workspaceStrategy: WorkspaceStrategy;
      mode: AgentMode;
      opencodeServerId: string;
      opencodeSessionId: string;
      modelProviderId: string;
      modelId: string;
      modelVariant: string | null;
      modelRoute: string;
    };
    job: {
      id: string;
      agentId: string;
      kind: JobKind;
      status?: JobStatus;
      batchId?: string | null;
      priority?: number;
      exclusiveResources?: string[] | null;
      requestId: string;
      promptHash: string;
      queuedAt?: string | null;
      mcpSessionId?: string | null;
      trustedThreadId?: string | null;
    };
    correlationHint?: {
      threadId?: string | null | undefined;
      turnId?: string | null | undefined;
    } | undefined;
    dispatchEnvelope?: {
      prompt: string;
      promptHash: string;
      workerInput: unknown;
      contextFiles: string[];
    } | undefined;
  }): { agent: AgentRecord; job: JobRecord } {
    return this.transaction(() => {
      const agent = this.createAgent(admission.agent);
      const job = this.createJob(admission.job);
      if (admission.correlationHint?.threadId || admission.correlationHint?.turnId) {
        this.setCorrelationHint(job.id, {
          threadId: admission.correlationHint.threadId ?? null,
          turnId: admission.correlationHint.turnId ?? null,
          source: "mcp",
        });
      }
      if (admission.dispatchEnvelope) {
        this.saveDispatchEnvelope(job.id, admission.dispatchEnvelope);
      }
      return { agent, job };
    });
  }

  admitContinuation(admission: {
    agent?: {
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
      modelRoute?: string | null;
      parentAgentId?: string | null;
    } | undefined;
    job: {
      id: string;
      agentId: string;
      kind: JobKind;
      status?: JobStatus;
      batchId?: string | null;
      priority?: number;
      exclusiveResources?: string[] | null;
      requestId: string;
      promptHash: string;
      queuedAt?: string | null;
      mcpSessionId?: string | null;
      trustedThreadId?: string | null;
    };
    correlationHint?: {
      threadId?: string | null | undefined;
      turnId?: string | null | undefined;
      source?: string | undefined;
    } | undefined;
    dispatchEnvelope?: {
      prompt: string;
      promptHash: string;
      workerInput: unknown;
      contextFiles: string[];
    } | undefined;
  }): { agent?: AgentRecord | null; job: JobRecord } {
    return this.transaction(() => {
      let agent: AgentRecord | null = null;
      if (admission.agent) {
        agent = this.createAgent(admission.agent);
      }
      const job = this.createJob(admission.job);
      if (admission.correlationHint?.threadId || admission.correlationHint?.turnId) {
        this.setCorrelationHint(job.id, {
          threadId: admission.correlationHint.threadId ?? null,
          turnId: admission.correlationHint.turnId ?? null,
          source: admission.correlationHint.source ?? "mcp",
        });
      }
      if (admission.dispatchEnvelope) {
        this.saveDispatchEnvelope(job.id, admission.dispatchEnvelope);
      }
      return { agent, job: this.getJob(job.id) ?? job };
    });
  }

  private toBatch(row: Row): BatchRecord {
    return {
      id: stringValue(row, "id"),
      requestId: stringValue(row, "request_id"),
      batchHash: stringValue(row, "batch_hash"),
      status: stringValue(row, "status"),
      createdAt: stringValue(row, "created_at"),
      updatedAt: stringValue(row, "updated_at"),
    };
  }

  createJob(input: {
    id: string;
    agentId: string;
    kind: JobKind;
    requestId: string;
    promptHash: string;
    mcpSessionId?: string | null;
    trustedThreadId?: string | null;
    status?: JobStatus;
    batchId?: string | null;
    priority?: number;
    exclusiveResources?: string[] | null;
    queuedAt?: string | null;
  }): JobRecord {
    const sequenceRow = this.db.prepare("SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM jobs WHERE agent_id = ?").get(input.agentId) as Row;
    const sequence = numberValue(sequenceRow, "sequence");
    const now = new Date().toISOString();
    const status = input.status ?? "created";
    const queuedAt = status === "queued" ? (input.queuedAt ?? now) : (input.queuedAt ?? null);
    const exclusiveResources = input.exclusiveResources ? JSON.stringify(input.exclusiveResources) : null;
    const priority = input.priority ?? 50;

    this.db.prepare("INSERT INTO jobs(id,agent_id,sequence,kind,request_id,prompt_hash,status,created_at,mcp_session_id,trusted_thread_id,batch_id,priority,exclusive_resources,queued_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)").run(
      input.id,
      input.agentId,
      sequence,
      input.kind,
      input.requestId ?? null,
      input.promptHash,
      status,
      now,
      input.mcpSessionId ?? null,
      input.trustedThreadId ?? null,
      input.batchId ?? null,
      priority,
      exclusiveResources,
      queuedAt,
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

  getJobByRequestId(requestId?: string | null): JobRecord | null {
    if (!requestId) return null;
    const row = this.db.prepare("SELECT * FROM jobs WHERE request_id = ?").get(requestId) as Row | undefined;
    return row ? this.toJob(row) : null;
  }

  getLatestJobForAgent(agentId: string): JobRecord | null {
    const row = this.db.prepare("SELECT * FROM jobs WHERE agent_id = ? ORDER BY sequence DESC LIMIT 1").get(agentId) as Row | undefined;
    return row ? this.toJob(row) : null;
  }

  listJobs(status?: JobStatus): JobRecord[] {
    const rows = status
      ? (this.db.prepare("SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC").all(status) as Row[])
      : (this.db.prepare("SELECT * FROM jobs ORDER BY created_at DESC").all() as Row[]);
    return rows.map((row) => this.toJob(row));
  }

  updateJobStatus(id: string, status: JobStatus, error: string | null = null, expectedFence?: number | null): JobRecord {
    const current = this.getJob(id);
    if (!current) throw new Error("Unknown job: " + id);
    if (expectedFence !== undefined && expectedFence !== null && current.fence !== null && current.fence !== undefined && expectedFence < current.fence) {
      throw new ConflictError("Stale write rejected: fence " + expectedFence + " is lower than current fence " + current.fence, "state_conflict");
    }
    assertJobTransition(current.status, status);
    const now = new Date().toISOString();
    const startedAt = status === "running" && current.startedAt === null ? now : current.startedAt;
    const completedAt = ["completed", "completed_partial", "timed_out", "failed", "aborted"].includes(status) ? now : current.completedAt;
    const dispatchedAt = ["dispatching", "running"].includes(status) && (current.dispatchedAt === null || current.dispatchedAt === undefined) ? now : (current.dispatchedAt ?? null);

    let sql = "UPDATE jobs SET status = ?, started_at = ?, completed_at = ?, dispatched_at = ?, error = ? WHERE id = ?";
    const params: (string | number | null)[] = [status, startedAt, completedAt, dispatchedAt, error, id];
    if (expectedFence !== undefined && expectedFence !== null) {
      sql += " AND (fence IS NULL OR fence <= ?)";
      params.push(expectedFence);
    }
    const info = this.db.prepare(sql).run(...params);
    if (info.changes === 0) {
      const existing = this.getJob(id);
      if (!existing) throw new Error("Job disappeared: " + id);
      if (expectedFence !== undefined && expectedFence !== null && existing.fence !== null && existing.fence !== undefined && expectedFence < existing.fence) {
        throw new ConflictError("Stale write rejected: fence " + expectedFence + " is lower than current fence " + existing.fence, "state_conflict");
      }
      throw new ConflictError("Stale write rejected: fence is obsolete or job disappeared", "state_conflict");
    }
    const updated = this.getJob(id);
    if (!updated) throw new Error("Job disappeared: " + id);
    return updated;
  }

  claimQueuedJobForDispatch(id: string, expectedFence?: number | null): JobRecord | null {
    const current = this.getJob(id);
    if (!current || current.status !== "queued") return null;
    if (expectedFence !== undefined && expectedFence !== null && current.fence !== null && current.fence !== undefined && expectedFence < current.fence) {
      return null;
    }
    const now = new Date().toISOString();
    let sql = "UPDATE jobs SET status = 'dispatching', dispatched_at = ? WHERE id = ? AND status = 'queued'";
    const params: (string | number)[] = [now, id];
    if (expectedFence !== undefined && expectedFence !== null) {
      sql += " AND (fence IS NULL OR fence <= ?)";
      params.push(expectedFence);
    }
    const info = this.db.prepare(sql).run(...params);
    if (Number(info.changes) !== 1) {
      return null;
    }
    return this.getJob(id);
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

  setCorrelationHint(id: string, input: {
    threadId?: string | null;
    turnId?: string | null;
    source: string;
    mcpSessionId?: string | null;
    trustedThreadId?: string | null;
  }): JobRecord {
    const current = this.getJob(id);
    if (!current) throw new Error("Unknown job: " + id);
    const threadId = input.threadId ?? current.hintThreadId;
    const turnId = input.turnId ?? current.hintTurnId;
    const source = threadId || turnId ? input.source : null;
    const mcpSessionId = input.mcpSessionId !== undefined ? input.mcpSessionId : current.mcpSessionId;
    const trustedThreadId = input.trustedThreadId !== undefined ? input.trustedThreadId : current.trustedThreadId;
    this.db.prepare("UPDATE jobs SET hint_thread_id = ?, hint_turn_id = ?, hint_source = ?, mcp_session_id = ?, trusted_thread_id = ? WHERE id = ?").run(
      threadId,
      turnId,
      source,
      mcpSessionId ?? null,
      trustedThreadId ?? null,
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

  setJobFallback(id: string, input: { from: string; to: string; reason: string; status: string; count?: number }): JobRecord {
    this.db.prepare("UPDATE jobs SET fallback_from = ?, fallback_to = ?, fallback_reason = ?, fallback_status = ?, fallback_count = ? WHERE id = ?").run(
      input.from,
      input.to,
      input.reason,
      input.status,
      input.count ?? 1,
      id,
    );
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  updateJobFallbackStatus(id: string, status: string): JobRecord {
    this.db.prepare("UPDATE jobs SET fallback_status = ? WHERE id = ?").run(status, id);
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  updateAgentSession(id: string, serverId: string, sessionId: string): AgentRecord {
    this.db.prepare("UPDATE agents SET opencode_server_id = ?, opencode_session_id = ?, updated_at = ? WHERE id = ?").run(
      serverId,
      sessionId,
      new Date().toISOString(),
      id,
    );
    const agent = this.getAgent(id);
    if (!agent) throw new Error("Agent disappeared: " + id);
    return agent;
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

  setJobResult(id: string, resultPath: string, summary: string, expectedFence?: number | null): JobRecord {
    let sql = "UPDATE jobs SET result_path = ?, result_summary = ? WHERE id = ?";
    const params: (string | number | null)[] = [resultPath, summary, id];
    if (expectedFence !== undefined && expectedFence !== null) {
      sql += " AND (fence IS NULL OR fence <= ?)";
      params.push(expectedFence);
    }
    const info = this.db.prepare(sql).run(...params);
    if (info.changes === 0) {
      const existing = this.getJob(id);
      if (!existing) throw new Error("Job disappeared: " + id);
      if (expectedFence !== undefined && expectedFence !== null && existing.fence !== null && existing.fence !== undefined && expectedFence < existing.fence) {
        throw new ConflictError("Stale write rejected: fence " + expectedFence + " is lower than current fence " + existing.fence, "state_conflict");
      }
      throw new ConflictError("Stale write rejected: fence is obsolete or job disappeared", "state_conflict");
    }
    const updated = this.getJob(id);
    if (!updated) throw new Error("Job disappeared: " + id);
    return updated;
  }

  updateJobLiveness(id: string, update: {
    leaseExpiresAt?: string | null;
    attempt?: string | null;
    fence?: number;
    workerPid?: number | null;
    heartbeatAt?: string | null;
  }): JobRecord {
    const setClauses: string[] = [];
    const params: (string | number | null)[] = [];

    if (update.leaseExpiresAt !== undefined) {
      setClauses.push("lease_expires_at = ?");
      params.push(update.leaseExpiresAt);
    }
    if (update.attempt !== undefined) {
      setClauses.push("attempt = ?");
      params.push(update.attempt);
    }
    if (update.fence !== undefined) {
      setClauses.push("fence = ?");
      params.push(update.fence);
    }
    if (update.workerPid !== undefined) {
      setClauses.push("worker_pid = ?");
      params.push(update.workerPid);
    }
    if (update.heartbeatAt !== undefined) {
      if (update.heartbeatAt === null) {
        setClauses.push("heartbeat_at = NULL");
      } else {
        setClauses.push("heartbeat_at = CASE WHEN heartbeat_at IS NULL OR heartbeat_at <= ? THEN ? ELSE heartbeat_at END");
        params.push(update.heartbeatAt, update.heartbeatAt);
      }
    }

    if (setClauses.length === 0) {
      const existing = this.getJob(id);
      if (!existing) throw new Error("Unknown job: " + id);
      return existing;
    }

    let sql = `UPDATE jobs SET ${setClauses.join(", ")} WHERE id = ?`;
    params.push(id);

    if (update.fence !== undefined) {
      sql += " AND (fence IS NULL OR fence <= ?)";
      params.push(update.fence);
    }

    const info = this.db.prepare(sql).run(...params);
    if (info.changes === 0) {
      const existing = this.getJob(id);
      if (!existing) {
        throw new Error("Job disappeared: " + id);
      }
      if (update.fence !== undefined && existing.fence !== null && existing.fence !== undefined && update.fence < existing.fence) {
        throw new ConflictError("Stale write rejected: fence " + update.fence + " is lower than current fence " + existing.fence, "state_conflict");
      }
      throw new ConflictError("Stale write rejected: fence is obsolete or job disappeared", "state_conflict");
    }

    const updated = this.getJob(id);
    if (!updated) throw new Error("Job disappeared: " + id);
    return updated;
  }

  setJobEarlyExit(id: string, input: { earlyExitAt: string; reason: string }, expectedFence?: number | null): JobRecord {
    let sql = "UPDATE jobs SET early_exit_at = ?, early_exit_reason = ? WHERE id = ?";
    const params: (string | number | null)[] = [input.earlyExitAt, input.reason, id];
    if (expectedFence !== undefined && expectedFence !== null) {
      sql += " AND (fence IS NULL OR fence <= ?)";
      params.push(expectedFence);
    }
    const info = this.db.prepare(sql).run(...params);
    if (info.changes === 0) {
      const existing = this.getJob(id);
      if (!existing) throw new Error("Job disappeared: " + id);
      if (expectedFence !== undefined && expectedFence !== null && existing.fence !== null && existing.fence !== undefined && expectedFence < existing.fence) {
        throw new ConflictError("Stale write rejected: fence " + expectedFence + " is lower than current fence " + existing.fence, "state_conflict");
      }
      throw new ConflictError("Stale write rejected: fence is obsolete or job disappeared", "state_conflict");
    }
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
  }

  setJobEscalation(id: string, proposalJson: string, expectedFence?: number | null): JobRecord {
    let sql = "UPDATE jobs SET escalation_proposal = ? WHERE id = ?";
    const params: (string | number | null)[] = [proposalJson, id];
    if (expectedFence !== undefined && expectedFence !== null) {
      sql += " AND (fence IS NULL OR fence <= ?)";
      params.push(expectedFence);
    }
    const info = this.db.prepare(sql).run(...params);
    if (info.changes === 0) {
      const existing = this.getJob(id);
      if (!existing) throw new Error("Job disappeared: " + id);
      if (expectedFence !== undefined && expectedFence !== null && existing.fence !== null && existing.fence !== undefined && expectedFence < existing.fence) {
        throw new ConflictError("Stale write rejected: fence " + expectedFence + " is lower than current fence " + existing.fence, "state_conflict");
      }
      throw new ConflictError("Stale write rejected: fence is obsolete or job disappeared", "state_conflict");
    }
    const job = this.getJob(id);
    if (!job) throw new Error("Job disappeared: " + id);
    return job;
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
      "SELECT COUNT(*) AS count FROM jobs WHERE status IN ('queued','dispatching','running','following','finalizing','needs_approval')",
    ).get() as Row;
    return numberValue(row, "count");
  }

  getQueueDepth(): number {
    const row = this.db.prepare("SELECT COUNT(*) AS count FROM jobs WHERE status = 'queued'").get() as Row;
    return numberValue(row, "count");
  }

  getActiveJobCount(): number {
    const row = this.db.prepare("SELECT COUNT(*) AS count FROM jobs WHERE status IN ('dispatching','running','following','finalizing')").get() as Row;
    return numberValue(row, "count");
  }

  getOldestWaitMs(now = Date.now()): number | null {
    const row = this.db.prepare("SELECT queued_at FROM jobs WHERE status = 'queued' AND queued_at IS NOT NULL ORDER BY queued_at ASC LIMIT 1").get() as Row | undefined;
    if (!row || !row.queued_at) return null;
    const queuedTime = new Date(stringValue(row, "queued_at")).getTime();
    return Math.max(0, now - queuedTime);
  }

  listQueuedJobs(): JobRecord[] {
    const rows = this.db.prepare("SELECT * FROM jobs WHERE status = 'queued' ORDER BY priority DESC, queued_at ASC").all() as Row[];
    return rows.map((r) => this.toJob(r));
  }

  listActiveJobs(): JobRecord[] {
    const rows = this.db.prepare(
      "SELECT * FROM jobs WHERE status IN ('dispatching','running','following','finalizing','needs_approval')",
    ).all() as Row[];
    return rows.map((r) => this.toJob(r));
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
      "SELECT id FROM jobs WHERE status IN ('queued','dispatching','running','following','finalizing','needs_approval','delivery_pending') OR (result_path IS NOT NULL AND result_consumed_at IS NULL)",
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
    const statuses = [
      "queued",
      "dispatching",
      "running",
      "following",
      "finalizing",
      "needs_approval",
      "completed",
      "completed_partial",
      "timed_out",
      "delivery_pending",
    ];
    const placeholders = statuses.map(() => "?").join(",");
    const rows = this.db.prepare(
      `SELECT * FROM jobs WHERE status IN (${placeholders}) ORDER BY created_at DESC`,
    ).all(...statuses) as Row[];
    return rows.map((row) => this.toJob(row));
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
      mode: (nullableString(row, "mode") as AgentMode) ?? "analyze",
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
      fallbackFrom: nullableString(row, "fallback_from"),
      fallbackTo: nullableString(row, "fallback_to"),
      fallbackReason: nullableString(row, "fallback_reason"),
      fallbackStatus: nullableString(row, "fallback_status"),
      fallbackCount: typeof row.fallback_count === "number" || typeof row.fallback_count === "bigint" ? Number(row.fallback_count) : 0,
      leaseExpiresAt: nullableString(row, "lease_expires_at"),
      attempt: nullableString(row, "attempt"),
      fence: typeof row.fence === "number" || typeof row.fence === "bigint" ? Number(row.fence) : 1,
      workerPid: typeof row.worker_pid === "number" || typeof row.worker_pid === "bigint" ? Number(row.worker_pid) : null,
      heartbeatAt: nullableString(row, "heartbeat_at"),
      earlyExitAt: nullableString(row, "early_exit_at"),
      earlyExitReason: nullableString(row, "early_exit_reason"),
      escalationProposal: nullableString(row, "escalation_proposal"),
      mcpSessionId: nullableString(row, "mcp_session_id"),
      trustedThreadId: nullableString(row, "trusted_thread_id"),
      batchId: nullableString(row, "batch_id"),
      priority: typeof row.priority === "number" || typeof row.priority === "bigint" ? Number(row.priority) : 50,
      exclusiveResources: row.exclusive_resources ? JSON.parse(stringValue(row, "exclusive_resources")) : null,
      queuedAt: nullableString(row, "queued_at"),
      dispatchedAt: nullableString(row, "dispatched_at"),
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

  createOrUpdateParkBarrier(input: {
    id?: string;
    threadId: string;
    turnId?: string | null;
    generation?: number;
    armed: boolean;
    deliveryMode?: "in_turn" | "cli_resume" | "queued" | "none";
    state: "armed" | "waking" | "woken" | "idle" | "cancelled";
    reason?: string | null;
    goalId?: string | null;
    pausedByBridge?: boolean;
    mcpSessionId?: string | null;
    predicateType?: ParkPredicateType;
    quorumCount?: number | null;
    requiredJobIds?: string[] | null;
    wakeOnException?: boolean;
  }): ParkBarrierRecord {
    const id = input.id ?? newId("park");
    const existing = this.getParkBarrier(id);
    const now = new Date().toISOString();
    const deliveryMode = input.deliveryMode ?? (existing ? existing.deliveryMode : "none");
    const mcpSessionId = input.mcpSessionId !== undefined ? input.mcpSessionId : (existing ? existing.mcpSessionId : null);
    const predicateType = input.predicateType ?? (existing ? existing.predicateType : "ALL");
    const quorumCount = input.quorumCount !== undefined ? input.quorumCount : (existing ? existing.quorumCount : null);
    const requiredJobIds = input.requiredJobIds !== undefined
      ? (input.requiredJobIds ? JSON.stringify(input.requiredJobIds) : null)
      : (existing && existing.requiredJobIds ? JSON.stringify(existing.requiredJobIds) : null);
    const wakeOnException = input.wakeOnException !== undefined
      ? (input.wakeOnException ? 1 : 0)
      : (existing ? (existing.wakeOnException ? 1 : 0) : 1);

    if (existing) {
      const generation = input.generation ?? (existing.generation + 1);
      const advances = generation > existing.generation;
      if (advances) {
        this.db.exec("BEGIN IMMEDIATE");
        try {
          this.db.prepare(`
            UPDATE park_barriers
            SET thread_id = ?, turn_id = ?, generation = ?, armed = ?, delivery_mode = ?, state = ?, reason = ?, goal_id = ?, paused_by_bridge = ?, mcp_session_id = ?, predicate_type = ?, quorum_count = ?, required_job_ids = ?, wake_on_exception = ?, updated_at = ?
            WHERE id = ?
          `).run(
            input.threadId,
            input.turnId !== undefined ? input.turnId : existing.turnId,
            generation,
            input.armed ? 1 : 0,
            deliveryMode,
            input.state,
            input.reason !== undefined ? input.reason : existing.reason,
            input.goalId !== undefined ? input.goalId : existing.goalId,
            input.pausedByBridge !== undefined ? (input.pausedByBridge ? 1 : 0) : (existing.pausedByBridge ? 1 : 0),
            mcpSessionId,
            predicateType,
            quorumCount,
            requiredJobIds,
            wakeOnException,
            now,
            id,
          );
          this.db.prepare(`
            UPDATE wake_outbox
            SET status = 'superseded',
                wake_state = 'superseded',
                last_error = COALESCE(last_error, 'Superseded by barrier generation ' || ?)
            WHERE park_id = ?
              AND generation < ?
              AND status IN ('pending', 'waking', 'deferred_active_writer')
          `).run(generation, id, generation);
          this.db.exec("COMMIT");
        } catch (error) {
          this.db.exec("ROLLBACK");
          throw error;
        }
      } else {
        this.db.prepare(`
          UPDATE park_barriers
          SET thread_id = ?, turn_id = ?, generation = ?, armed = ?, delivery_mode = ?, state = ?, reason = ?, goal_id = ?, paused_by_bridge = ?, mcp_session_id = ?, predicate_type = ?, quorum_count = ?, required_job_ids = ?, wake_on_exception = ?, updated_at = ?
          WHERE id = ?
        `).run(
          input.threadId,
          input.turnId !== undefined ? input.turnId : existing.turnId,
          generation,
          input.armed ? 1 : 0,
          deliveryMode,
          input.state,
          input.reason !== undefined ? input.reason : existing.reason,
          input.goalId !== undefined ? input.goalId : existing.goalId,
          input.pausedByBridge !== undefined ? (input.pausedByBridge ? 1 : 0) : (existing.pausedByBridge ? 1 : 0),
          mcpSessionId,
          predicateType,
          quorumCount,
          requiredJobIds,
          wakeOnException,
          now,
          id,
        );
      }
    } else {
      this.db.prepare(`
        INSERT INTO park_barriers (id, thread_id, turn_id, generation, armed, delivery_mode, state, reason, goal_id, paused_by_bridge, mcp_session_id, predicate_type, quorum_count, required_job_ids, wake_on_exception, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        id,
        input.threadId,
        input.turnId ?? null,
        input.generation ?? 1,
        input.armed ? 1 : 0,
        deliveryMode,
        input.state,
        input.reason ?? null,
        input.goalId ?? null,
        input.pausedByBridge ? 1 : 0,
        mcpSessionId,
        predicateType,
        quorumCount,
        requiredJobIds,
        wakeOnException,
        now,
        now,
      );
    }
    const result = this.getParkBarrier(id);
    if (!result) throw new Error("Park barrier was not persisted: " + id);
    return result;
  }

  setParkJobs(parkId: string, jobIds: string[]): void {
    this.db.prepare("DELETE FROM park_jobs WHERE park_id = ?").run(parkId);
    const now = new Date().toISOString();
    for (const jobId of jobIds) {
      this.db.prepare("INSERT OR IGNORE INTO park_jobs(park_id, job_id, created_at) VALUES(?, ?, ?)").run(parkId, jobId, now);
    }
  }

  getParkBarrier(id: string): ParkBarrierRecord | null {
    const row = this.db.prepare("SELECT * FROM park_barriers WHERE id = ?").get(id) as Row | undefined;
    return row ? this.toParkBarrier(row) : null;
  }

  getParkBarrierByThread(threadId: string): ParkBarrierRecord | null {
    const row = this.db.prepare("SELECT * FROM park_barriers WHERE thread_id = ? ORDER BY updated_at DESC LIMIT 1").get(threadId) as Row | undefined;
    return row ? this.toParkBarrier(row) : null;
  }

  getParkBarrierJobs(parkId: string): string[] {
    const rows = this.db.prepare("SELECT job_id FROM park_jobs WHERE park_id = ?").all(parkId) as Row[];
    return rows.map((r) => stringValue(r, "job_id"));
  }

  findArmedParksForJob(jobId: string): ParkBarrierRecord[] {
    const rows = this.db.prepare(
      "SELECT pb.* FROM park_barriers pb JOIN park_jobs pj ON pj.park_id = pb.id WHERE pj.job_id = ? AND pb.state = 'armed'",
    ).all(jobId) as Row[];
    return rows.map((r) => this.toParkBarrier(r));
  }

  listArmedBarriers(): ParkBarrierRecord[] {
    const rows = this.db.prepare(
      "SELECT * FROM park_barriers WHERE state = 'armed' AND armed = 1",
    ).all() as Row[];
    return rows.map((r) => this.toParkBarrier(r));
  }

  claimParkWake(parkId: string, generation: number): boolean {
    const now = new Date().toISOString();
    const info = this.db.prepare(
      "UPDATE park_barriers SET state = 'waking', armed = 0, updated_at = ? WHERE id = ? AND generation = ? AND state = 'armed'",
    ).run(now, parkId, generation);
    return Number(info.changes) === 1;
  }

  setParkWoken(parkId: string, generation: number): void {
    const now = new Date().toISOString();
    this.db.prepare(
      "UPDATE park_barriers SET state = 'woken', armed = 0, updated_at = ? WHERE id = ? AND generation = ?",
    ).run(now, parkId, generation);
  }

  setParkArmed(parkId: string, generation: number): void {
    const now = new Date().toISOString();
    this.db.prepare(
      "UPDATE park_barriers SET state = 'armed', armed = 1, updated_at = ? WHERE id = ? AND generation = ?",
    ).run(now, parkId, generation);
  }

  claimWakeOutbox(id: string): boolean;
  claimWakeOutbox(parkId: string, generation: number): boolean;
  claimWakeOutbox(idOrParkId: string, generation?: number): boolean {
    const sql = generation !== undefined
      ? `UPDATE wake_outbox
         SET status = 'waking', attempts = attempts + 1
         WHERE park_id = ?
           AND generation = ?
           AND status IN ('pending', 'deferred_active_writer')
           AND generation = (SELECT pb.generation FROM park_barriers pb WHERE pb.id = wake_outbox.park_id)`
      : `UPDATE wake_outbox
         SET status = 'waking', attempts = attempts + 1
         WHERE id = ?
           AND status IN ('pending', 'deferred_active_writer')
           AND generation = (SELECT pb.generation FROM park_barriers pb WHERE pb.id = wake_outbox.park_id)`;
    const info = generation !== undefined
      ? this.db.prepare(sql).run(idOrParkId, generation)
      : this.db.prepare(sql).run(idOrParkId);
    return Number(info.changes) === 1;
  }

  createWakeOutbox(record: {
    id: string;
    parkId: string;
    generation: number;
    threadId: string;
    turnId?: string | null;
    deliveryMode?: "in_turn" | "cli_resume" | "queued" | "none";
    status: "pending" | "waking" | "deferred_active_writer" | "delivered" | "failed" | "superseded";
    wakeState?: "waiting" | "deferred_active_writer" | "delivered" | "failed" | "superseded";
    wakeMarker: string;
    reason?: string | null;
    payloadJson: string;
    nextAttemptAt?: string | null;
    selectedExecutable?: string | null;
    executableVersion?: string | null;
    messageId?: string | null;
  }): WakeOutboxRecord {
    const now = new Date().toISOString();
    const deliveryMode = record.deliveryMode ?? "cli_resume";
    const wakeState = record.wakeState ?? (
      record.status === "delivered" ? "delivered" :
      record.status === "deferred_active_writer" ? "deferred_active_writer" :
      record.status === "superseded" ? "superseded" :
      "waiting"
    );
    this.db.prepare(`
      INSERT INTO wake_outbox (id, park_id, generation, thread_id, turn_id, delivery_mode, status, wake_state, wake_marker, reason, payload_json, attempts, next_attempt_at, selected_executable, executable_version, message_id, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
      ON CONFLICT(park_id, generation) DO NOTHING
    `).run(
      record.id,
      record.parkId,
      record.generation,
      record.threadId,
      record.turnId ?? null,
      deliveryMode,
      record.status,
      wakeState,
      record.wakeMarker,
      record.reason ?? null,
      record.payloadJson,
      record.nextAttemptAt ?? null,
      record.selectedExecutable ?? null,
      record.executableVersion ?? null,
      record.messageId ?? null,
      now,
    );
    const result = this.getWakeOutbox(record.parkId, record.generation);
    if (!result) throw new Error("Wake outbox entry was not persisted");
    return result;
  }

  getWakeOutbox(parkId: string, generation: number): WakeOutboxRecord | null {
    const row = this.db.prepare("SELECT * FROM wake_outbox WHERE park_id = ? AND generation = ?").get(parkId, generation) as Row | undefined;
    return row ? this.toWakeOutbox(row) : null;
  }

  getWakeOutboxById(id: string): WakeOutboxRecord | null {
    const row = this.db.prepare("SELECT * FROM wake_outbox WHERE id = ?").get(id) as Row | undefined;
    return row ? this.toWakeOutbox(row) : null;
  }

  updateWakeOutboxStatus(
    id: string,
    status: "pending" | "waking" | "deferred_active_writer" | "delivered" | "failed" | "superseded",
    error?: string | null,
    extra?: {
      wakeState?: "waiting" | "deferred_active_writer" | "delivered" | "failed" | "superseded";
      nextAttemptAt?: string | null;
      selectedExecutable?: string | null;
      executableVersion?: string | null;
      deliveryMode?: "in_turn" | "cli_resume" | "queued" | "none";
      messageId?: string | null;
    },
  ): void {
    const now = new Date().toISOString();
    const wakeState = extra?.wakeState ?? (
      status === "delivered" ? "delivered" :
      status === "deferred_active_writer" ? "deferred_active_writer" :
      status === "failed" ? "failed" :
      status === "superseded" ? "superseded" :
      "waiting"
    );
    this.db.prepare(`
      UPDATE wake_outbox
      SET status = ?,
          wake_state = ?,
          woken_at = CASE WHEN ? = 'delivered' THEN ? ELSE woken_at END,
          last_error = ?,
          next_attempt_at = CASE WHEN ? IS NOT NULL THEN ? ELSE next_attempt_at END,
          selected_executable = CASE WHEN ? IS NOT NULL THEN ? ELSE selected_executable END,
          executable_version = CASE WHEN ? IS NOT NULL THEN ? ELSE executable_version END,
          delivery_mode = CASE WHEN ? IS NOT NULL THEN ? ELSE delivery_mode END,
          message_id = CASE WHEN ? IS NOT NULL THEN ? ELSE message_id END
      WHERE id = ? AND status != 'delivered'
    `).run(
      status,
      wakeState,
      status,
      now,
      error ?? null,
      extra?.nextAttemptAt ?? null,
      extra?.nextAttemptAt ?? null,
      extra?.selectedExecutable ?? null,
      extra?.selectedExecutable ?? null,
      extra?.executableVersion ?? null,
      extra?.executableVersion ?? null,
      extra?.deliveryMode ?? null,
      extra?.deliveryMode ?? null,
      extra?.messageId ?? null,
      extra?.messageId ?? null,
      id,
    );
  }

  listPendingWakeOutbox(): WakeOutboxRecord[] {
    const rows = this.db.prepare(`
      SELECT w.* FROM wake_outbox w
      JOIN park_barriers p ON p.id = w.park_id
      WHERE w.status IN ('pending', 'deferred_active_writer', 'waking')
        AND w.generation = p.generation
      ORDER BY w.created_at ASC
    `).all() as Row[];
    return rows.map((r) => this.toWakeOutbox(r));
  }

  recordBridgeGoalPause(goalId: string, goalHash: string): void {
    const now = new Date().toISOString();
    this.db.prepare(
      "INSERT INTO bridge_goals (goal_id, goal_hash, paused_at, invalidated_at) VALUES (?, ?, ?, NULL) ON CONFLICT(goal_id) DO UPDATE SET goal_hash = excluded.goal_hash, paused_at = excluded.paused_at, invalidated_at = NULL",
    ).run(goalId, goalHash, now);
  }

  validateBridgeGoalOwnership(goalId: string, currentHash: string): boolean {
    const row = this.db.prepare(
      "SELECT 1 AS valid FROM bridge_goals WHERE goal_id = ? AND goal_hash = ? AND invalidated_at IS NULL",
    ).get(goalId, currentHash) as Row | undefined;
    return row !== undefined;
  }

  invalidateBridgeGoalOwnership(goalId: string): void {
    const now = new Date().toISOString();
    this.db.prepare("UPDATE bridge_goals SET invalidated_at = ? WHERE goal_id = ?").run(now, goalId);
  }

  private toParkBarrier(row: Row): ParkBarrierRecord {
    return {
      id: stringValue(row, "id"),
      threadId: stringValue(row, "thread_id"),
      turnId: nullableString(row, "turn_id"),
      generation: numberValue(row, "generation"),
      armed: numberValue(row, "armed") === 1,
      deliveryMode: (stringValue(row, "delivery_mode") as ParkBarrierRecord["deliveryMode"]) || "none",
      state: stringValue(row, "state") as ParkBarrierRecord["state"],
      reason: nullableString(row, "reason"),
      goalId: nullableString(row, "goal_id"),
      pausedByBridge: numberValue(row, "paused_by_bridge") === 1,
      mcpSessionId: nullableString(row, "mcp_session_id"),
      predicateType: (stringValue(row, "predicate_type") as ParkPredicateType) || "ALL",
      quorumCount: row.quorum_count !== null && row.quorum_count !== undefined ? numberValue(row, "quorum_count") : null,
      requiredJobIds: row.required_job_ids ? JSON.parse(stringValue(row, "required_job_ids")) : null,
      wakeOnException: row.wake_on_exception !== null && row.wake_on_exception !== undefined ? numberValue(row, "wake_on_exception") === 1 : true,
      createdAt: stringValue(row, "created_at"),
      updatedAt: stringValue(row, "updated_at"),
    };
  }

  private toWakeOutbox(row: Row): WakeOutboxRecord {
    return {
      id: stringValue(row, "id"),
      parkId: stringValue(row, "park_id"),
      generation: numberValue(row, "generation"),
      threadId: stringValue(row, "thread_id"),
      turnId: nullableString(row, "turn_id"),
      deliveryMode: (stringValue(row, "delivery_mode") as WakeOutboxRecord["deliveryMode"]) || "cli_resume",
      status: stringValue(row, "status") as WakeOutboxRecord["status"],
      wakeState: (stringValue(row, "wake_state") as WakeOutboxRecord["wakeState"]) || "waiting",
      wakeMarker: stringValue(row, "wake_marker"),
      reason: nullableString(row, "reason"),
      payloadJson: stringValue(row, "payload_json"),
      attempts: numberValue(row, "attempts"),
      nextAttemptAt: nullableString(row, "next_attempt_at"),
      selectedExecutable: nullableString(row, "selected_executable"),
      executableVersion: nullableString(row, "executable_version"),
      messageId: nullableString(row, "message_id"),
      createdAt: stringValue(row, "created_at"),
      wokenAt: nullableString(row, "woken_at"),
      lastError: nullableString(row, "last_error"),
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
