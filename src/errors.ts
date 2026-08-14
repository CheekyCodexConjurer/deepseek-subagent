export type BridgeErrorStatus = 400 | 401 | 403 | 404 | 409 | 500;

export type BridgeErrorCode =
  | "invalid_request"
  | "unknown_route"
  | "route_disabled"
  | "route_override_denied"
  | "context_file_invalid"
  | "job_agent_mismatch"
  | "permission_required"
  | "permission_mismatch"
  | "unknown_agent"
  | "unknown_job"
  | "not_found"
  | "busy"
  | "not_continuable"
  | "not_followable"
  | "state_conflict"
  | "unauthorized"
  | "internal";

/**
 * Stable typed bridge error. Every error that crosses the HTTP or MCP
 * boundary carries an explicit HTTP status and a stable machine-readable
 * code so callers can branch without sniffing message text.
 */
export class BridgeError extends Error {
  readonly status: BridgeErrorStatus;
  readonly code: BridgeErrorCode;
  readonly details: unknown;

  constructor(status: BridgeErrorStatus, code: BridgeErrorCode, message: string, details?: unknown) {
    super(message);
    this.name = "BridgeError";
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export class InvalidRequestError extends BridgeError {
  constructor(message: string, code: BridgeErrorCode = "invalid_request", details?: unknown) {
    super(400, code, message, details);
    this.name = "InvalidRequestError";
  }
}

export class NotFoundError extends BridgeError {
  constructor(message: string, code: BridgeErrorCode = "not_found") {
    super(404, code, message);
    this.name = "NotFoundError";
  }
}

export class UnknownAgentError extends NotFoundError {
  constructor(agentId: string) {
    super("Unknown agent: " + agentId, "unknown_agent");
    this.name = "UnknownAgentError";
  }
}

export class UnknownJobError extends NotFoundError {
  constructor(jobId: string) {
    super("Unknown job: " + jobId, "unknown_job");
    this.name = "UnknownJobError";
  }
}

export class ConflictError extends BridgeError {
  constructor(message: string, code: BridgeErrorCode = "state_conflict") {
    super(409, code, message);
    this.name = "ConflictError";
  }
}

/**
 * Route changes are operator-only through the authenticated control plane.
 * An ordinary spawn caller naming a registered, enabled route that is NOT the
 * currently active route fails closed with this typed 403; there is no
 * fallback and no silent route override.
 */
export class RouteOverrideDeniedError extends BridgeError {
  constructor(message: string, details?: unknown) {
    super(403, "route_override_denied", message, details);
    this.name = "RouteOverrideDeniedError";
  }
}

export class InternalBridgeError extends BridgeError {
  constructor(message: string) {
    super(500, "internal", message);
    this.name = "InternalBridgeError";
  }
}
