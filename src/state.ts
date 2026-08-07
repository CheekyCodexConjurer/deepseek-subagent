import type { AgentStatus, JobStatus } from "./types.js";

const JOB_TRANSITIONS: Record<JobStatus, readonly JobStatus[]> = {
  created: ["dispatching", "failed", "aborted"],
  dispatching: ["running", "failed", "aborted"],
  running: ["needs_approval", "completed", "failed", "aborted"],
  needs_approval: ["running", "failed", "aborted"],
  completed: ["delivery_pending", "delivered", "failed"],
  delivery_pending: ["delivered", "failed"],
  delivered: [],
  failed: [],
  aborted: [],
};

const AGENT_TRANSITIONS: Record<AgentStatus, readonly AgentStatus[]> = {
  created: ["working", "failed", "aborted", "closed"],
  working: ["needs_approval", "completed", "failed", "aborted", "closed"],
  needs_approval: ["working", "failed", "aborted", "closed"],
  completed: ["working", "closed"],
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
