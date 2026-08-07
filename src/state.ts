import type { AgentStatus, JobStatus } from "./types.js";

const JOB_TRANSITIONS: Record<JobStatus, readonly JobStatus[]> = {
  created: ["dispatching", "failed", "aborted"],
  dispatching: ["running", "following", "needs_approval", "failed", "aborted"],
  running: ["needs_approval", "following", "completed", "failed", "aborted", "timed_out"],
  following: ["needs_approval", "finalizing", "completed", "completed_partial", "failed", "aborted", "timed_out"],
  finalizing: ["needs_approval", "completed", "completed_partial", "failed", "aborted", "timed_out"],
  needs_approval: ["running", "failed", "aborted"],
  completed: ["delivery_pending", "delivered", "failed"],
  completed_partial: ["delivery_pending", "delivered", "failed"],
  timed_out: ["delivery_pending", "delivered", "failed"],
  delivery_pending: ["delivered", "failed"],
  delivered: [],
  failed: [],
  aborted: [],
};

const AGENT_TRANSITIONS: Record<AgentStatus, readonly AgentStatus[]> = {
  created: ["working", "failed", "aborted", "closed"],
  working: ["needs_approval", "completed", "completed_partial", "timed_out", "failed", "aborted", "closed"],
  needs_approval: ["working", "failed", "aborted", "closed"],
  completed: ["working", "closed"],
  completed_partial: ["working", "closed"],
  timed_out: ["working", "closed"],
  failed: ["working", "closed"],
  aborted: ["closed"],
  closed: [],
};

export function canTransitionJob(from: JobStatus, to: JobStatus): boolean {
  return from === to || JOB_TRANSITIONS[from].includes(to);
}

export function assertJobTransition(from: JobStatus, to: JobStatus): void {
  if (!canTransitionJob(from, to)) {
    throw new Error("Invalid job transition: " + from + " -> " + to);
  }
}

export function canTransitionAgent(from: AgentStatus, to: AgentStatus): boolean {
  return from === to || AGENT_TRANSITIONS[from].includes(to);
}

export function assertAgentTransition(from: AgentStatus, to: AgentStatus): void {
  if (!canTransitionAgent(from, to)) {
    throw new Error("Invalid agent transition: " + from + " -> " + to);
  }
}
