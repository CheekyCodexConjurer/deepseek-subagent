import type { BridgeStore } from "./store.js";
import type { RetentionMode } from "./types.js";

export const RETAIN_EVENT_AGE_DAYS = 7;
export const RETAIN_ACTIVITY_FLOOR_PER_AGENT = 50;
export const RETAIN_CHUNK_SIZE = 500;
export const RETAIN_BUDGET_MS = 2_000;

const PROTECTED_JOB_IDS_SQL =
  "SELECT id FROM jobs WHERE status IN ('dispatching','running','following','finalizing','needs_approval','delivery_pending') OR (result_path IS NOT NULL AND result_consumed_at IS NULL)";

export interface RetentionPolicyState {
  mode: RetentionMode;
  dbState: "empty" | "legacy";
  pruningEnabled: boolean;
  reason?: string;
}

/**
 * Evaluates whether pruning may run for the given mode. Enforcement is
 * intrinsic to the database, not CLI-only: `enabled` on a non-empty legacy
 * database requires the in-database offline-preparation marker written by the
 * explicit `retention dry-run` / `retention enabled --confirm` CLI flow, so a
 * hand-edited retentionMode=enabled alone can never arm online pruning.
 * auto enables only on a provably empty database; disabled and dry-run never
 * delete.
 */
export function evaluateRetentionPolicy(store: BridgeStore, mode: RetentionMode): RetentionPolicyState {
  const empty = store.isProvablyEmpty();
  if (mode === "enabled") {
    if (empty) return { mode, dbState: "empty", pruningEnabled: true };
    if (store.isRetentionPrepared()) return { mode, dbState: "legacy", pruningEnabled: true };
    return {
      mode,
      dbState: "legacy",
      pruningEnabled: false,
      reason: "non-empty legacy database without explicit offline preparation; run `retention dry-run` or `retention enabled --confirm` while the daemon is stopped",
    };
  }
  if (mode === "auto") {
    return { mode, dbState: empty ? "empty" : "legacy", pruningEnabled: empty };
  }
  return { mode, dbState: "legacy", pruningEnabled: false };
}

export interface RetentionPruneResult {
  mode: RetentionMode;
  dbState: "empty" | "legacy";
  pruningEnabled: boolean;
  dryRun: boolean;
  prunedEvents: number;
  prunedActivity: number;
  remainingEvents: number;
  remainingActivity: number;
  protectedEvents: number;
  protectedActivity: number;
  budgetExceeded: boolean;
  checkpoint: string;
  reason?: string;
}

export interface RetentionPruneOptions {
  dryRun?: boolean;
  budgetMs?: number;
  chunkSize?: number;
  eventAgeMs?: number;
  activityFloor?: number;
  now?: number;
}

/**
 * Prunes ONLY events and agent_activity. Agents, jobs, results, deliveries,
 * codex_bindings and the inbox are never touched. Rows linked to active,
 * open, unconsumed or undelivered jobs are protected, fresh rows newer than
 * the retention horizon are kept, and the newest activity rows per agent are
 * always retained (activity floor). The pass is chunked and time-bounded, and
 * deleting is idempotent; the only maintenance step is a PASSIVE WAL
 * checkpoint — there is never an online VACUUM.
 */
export function runRetentionPrune(store: BridgeStore, options: RetentionPruneOptions = {}): RetentionPruneResult {
  const dryRun = options.dryRun === true;
  const mode: RetentionMode = dryRun ? "dry-run" : "enabled";
  const chunkSize = options.chunkSize ?? RETAIN_CHUNK_SIZE;
  const budgetMs = options.budgetMs ?? RETAIN_BUDGET_MS;
  const now = options.now ?? Date.now();
  const cutoffAt = new Date(now - (options.eventAgeMs ?? RETAIN_EVENT_AGE_DAYS * 24 * 60 * 60_000)).toISOString();
  const activityFloor = options.activityFloor ?? RETAIN_ACTIVITY_FLOOR_PER_AGENT;

  const beforeEvents = countAll(store, "events");
  const beforeActivity = countAll(store, "agent_activity");
  const eligibleBeforeEvents = countEligible(store, "events", cutoffAt);
  const eligibleBeforeActivity = countEligible(store, "agent_activity", cutoffAt, activityFloor);
  // Protected rows are rows that are never eligible for pruning, computed
  // from the pre-pass eligibility so dry-run cannot falsely report zero
  // protected rows just because nothing was deleted.
  const protectedEvents = Math.max(0, beforeEvents - eligibleBeforeEvents);
  const protectedActivity = Math.max(0, beforeActivity - eligibleBeforeActivity);

  let prunedEvents = 0;
  let prunedActivity = 0;
  let budgetExceeded = false;
  if (dryRun) {
    // Preview semantics: report exactly what a real pass would delete.
    prunedEvents = eligibleBeforeEvents;
    prunedActivity = eligibleBeforeActivity;
  } else {
    const deadline = Date.now() + budgetMs;
    while (Date.now() < deadline) {
      const result = store.db.prepare(
        "DELETE FROM events WHERE id IN (SELECT id FROM events WHERE " +
          "received_at < ? AND job_id IS NOT NULL AND job_id NOT IN (" + PROTECTED_JOB_IDS_SQL + ") " +
          "ORDER BY received_at ASC LIMIT ?)",
      ).run(cutoffAt, chunkSize);
      const changes = Number(result.changes);
      if (changes === 0) break;
      prunedEvents += changes;
    }
    budgetExceeded = Date.now() >= deadline;
    while (Date.now() < deadline) {
      const result = store.db.prepare(
        "DELETE FROM agent_activity WHERE id IN (SELECT id FROM agent_activity WHERE " +
          "created_at < ? AND id NOT IN (SELECT a2.id FROM agent_activity a2 WHERE a2.agent_id = agent_activity.agent_id ORDER BY a2.created_at DESC, a2.id DESC LIMIT ?) " +
          "ORDER BY created_at ASC LIMIT ?)",
      ).run(cutoffAt, activityFloor, chunkSize);
      const changes = Number(result.changes);
      if (changes === 0) break;
      prunedActivity += changes;
    }
    budgetExceeded = budgetExceeded || Date.now() >= deadline;
  }

  const remainingEvents = countAll(store, "events");
  const remainingActivity = countAll(store, "agent_activity");

  let checkpoint = "none";
  if (!dryRun) {
    try {
      const row = store.db.prepare("PRAGMA wal_checkpoint(PASSIVE)").get() as { busy?: number; log?: number; checkpointed?: number } | undefined;
      checkpoint = row ? "busy=" + Number(row.busy ?? 0) + " log=" + Number(row.log ?? 0) + " checkpointed=" + Number(row.checkpointed ?? 0) : "unknown";
    } catch {
      checkpoint = "unavailable";
    }
  }

  return {
    mode,
    dbState: "legacy",
    pruningEnabled: !dryRun,
    dryRun,
    prunedEvents,
    prunedActivity,
    remainingEvents,
    remainingActivity,
    protectedEvents,
    protectedActivity,
    budgetExceeded,
    checkpoint,
  };
}

/**
 * Creates the retention prune-support indexes. Intended only for the explicit
 * offline CLI path (dry-run / enabled --confirm) on legacy databases; a fresh
 * (empty) database gets the same cheap indexes during migrate().
 */
export function createLegacyPruneIndexes(store: BridgeStore): void {
  store.db.exec("CREATE INDEX IF NOT EXISTS idx_events_received_at ON events(received_at);");
  store.db.exec("CREATE INDEX IF NOT EXISTS idx_agent_activity_created_at ON agent_activity(created_at);");
}

function countEligible(store: BridgeStore, table: "events" | "agent_activity", cutoffAt: string, floor?: number): number {
  const args: Array<string | number> = floor === undefined ? [cutoffAt] : [cutoffAt, floor];
  const row = store.db.prepare(
    "SELECT COUNT(*) AS count FROM " + table + " WHERE " +
      (table === "events"
        ? "received_at < ? AND job_id IS NOT NULL AND job_id NOT IN (" + PROTECTED_JOB_IDS_SQL + ")"
        : "created_at < ? AND id NOT IN (SELECT a2.id FROM agent_activity a2 WHERE a2.agent_id = agent_activity.agent_id ORDER BY a2.created_at DESC, a2.id DESC LIMIT ?)"),
  ).get(...args) as { count: number | bigint } | undefined;
  return Number(row?.count ?? 0);
}

function countAll(store: BridgeStore, table: "events" | "agent_activity"): number {
  const row = store.db.prepare("SELECT COUNT(*) AS count FROM " + table).get() as { count: number | bigint } | undefined;
  return Number(row?.count ?? 0);
}
