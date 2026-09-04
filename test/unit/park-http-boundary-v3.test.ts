import test from "node:test";
import assert from "node:assert/strict";
import { createServer, type AddressInfo } from "node:net";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createDefaultConfig } from "../../src/config.js";
import { InvalidRequestError, BridgeError } from "../../src/errors.js";
import { BridgeHttpClient, BridgeHttpError, BridgeHttpServer } from "../../src/http-server.js";
import { createMcpServer } from "../../src/mcp.js";
import type { BridgeService } from "../../src/service.js";
import type { ParkInput, ParkReceipt, ParkPredicateType } from "../../src/types.js";

/**
 * Ephemeral port finder ensuring independent port allocation per test case.
 */
async function freePort(): Promise<number> {
  const probe = createServer();
  return new Promise<number>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      const port = typeof address === "object" && address ? (address as AddressInfo).port : 0;
      probe.close((error) => (error ? reject(error) : resolve(port)));
    });
  });
}

interface RecordedParkCall {
  input: ParkInput;
  isAlias: boolean;
}

/**
 * Fake BridgeService that captures every invocation of service.park
 * and validates the domain invariants of ParkInput.
 */
function createFakeParkService(options: {
  enforceValidation?: boolean;
  onPark?: (input: ParkInput, isAlias: boolean) => Promise<ParkReceipt> | ParkReceipt;
} = {}) {
  const calls: RecordedParkCall[] = [];
  const service = {
    isReady: () => true,
    status: () => ({ running: true, state: "ready", ready: true }),
    park: async (input: ParkInput, isAlias = false, _signal?: AbortSignal): Promise<ParkReceipt> => {
      calls.push({ input, isAlias });
      if (options.onPark) {
        return options.onPark(input, isAlias);
      }
      if (options.enforceValidation) {
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

        const rawPred = input.predicate ?? input.predicate_type ?? input.predicateType ?? "ALL";
        const predStr = typeof rawPred === "string" ? rawPred.toUpperCase() : String(rawPred);
        if (!["ALL", "ANY", "QUORUM", "REQUIRED"].includes(predStr)) {
          throw new InvalidRequestError(`Invalid predicate "${rawPred}". Supported predicates: ALL, ANY, QUORUM, REQUIRED.`, "invalid_request");
        }
        const predicateType = predStr as ParkPredicateType;
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
        const deliveryMode = input.delivery_mode ?? input.deliveryMode;
        if (deliveryMode !== undefined && !["in_turn", "cli_resume", "none", "queued"].includes(deliveryMode)) {
          throw new InvalidRequestError(`Invalid delivery_mode "${deliveryMode}"`, "invalid_request");
        }
        const wakeOnException = input.wake_on_exception ?? input.wakeOnException;
        if (wakeOnException !== undefined && typeof wakeOnException !== "boolean") {
          throw new InvalidRequestError("wake_on_exception must be a boolean", "invalid_request");
        }
      }

      const jobIds = input.job_ids ?? input.jobIds ?? ["job_1"];
      const rawPred = input.predicate ?? input.predicate_type ?? input.predicateType ?? "ALL";
      const predStr = typeof rawPred === "string" ? rawPred.toUpperCase() : "ALL";
      const queueMessageId = (input as any).queueMessageId ?? (input as any).queue_message_id ?? (input as any).messageId ?? (input as any).message_id ?? null;
      return {
        parkId: input.park_id ?? input.parkId ?? "park_test_1",
        generation: 1,
        armed: true,
        targetIdentity: input.thread_id ?? input.threadId ?? "thread_test_1",
        deliveryMode: (input.delivery_mode === "in_turn" ? "cli_resume" : (input.delivery_mode ?? input.deliveryMode ?? "cli_resume")) as any,
        wakeState: "waiting",
        obligationState: "pending",
        nextAction: isAlias ? "deepseek_follow" : "subagents_follow",
        nextRequiredAction: isAlias ? "deepseek_follow" : "subagents_follow",
        jobIds,
        reason: input.reason ?? null,
        pendingCount: jobIds.length,
        readyCount: 0,
        predicateType: predStr as ParkPredicateType,
        quorumCount: input.quorum_count ?? input.quorumCount ?? null,
        requiredJobIds: input.required_job_ids ?? input.requiredJobIds ?? null,
        ...(queueMessageId ? { queueMessageId, queue_message_id: queueMessageId } : {}),
      };
    },
  } as unknown as BridgeService;

  return { service, calls };
}

/**
 * Spin up an ephemeral HTTP server and matching BridgeHttpClient on a dynamic port.
 */
async function createTestHarness(options: { enforceValidation?: boolean } = {}) {
  const { service, calls } = createFakeParkService(options);
  const port = await freePort();
  const config = createDefaultConfig({
    daemonHost: "127.0.0.1",
    daemonPort: port,
    daemonToken: "test-boundary-token-" + Math.random().toString(36).slice(2),
    dataDir: "C:\\deepseek-http-test-boundary",
    configPath: "C:\\deepseek-http-test-boundary\\config.json",
  });
  const server = new BridgeHttpServer(config, service);
  await server.start();
  const client = new BridgeHttpClient(config);

  const postPark = async (body: unknown) => {
    return fetch(`http://${config.daemonHost}:${config.daemonPort}/v1/jobs/park`, {
      method: "POST",
      headers: {
        authorization: "Bearer " + config.daemonToken,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    });
  };

  return {
    config,
    server,
    client,
    service,
    calls,
    postPark,
    async cleanup() {
      await client.close();
      await server.stop();
    },
  };
}

/**
 * Create connected in-memory MCP client and server pair.
 */
async function createMcpHarness(bridgeClient: BridgeHttpClient) {
  const server = createMcpServer(bridgeClient);
  const client = new Client({ name: "mcp-test-boundary-client", version: "1.0.0" }, { capabilities: {} });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);

  return {
    server,
    client,
    async cleanup() {
      await client.close();
      await server.close();
    },
  };
}

// ============================================================================
// Suite 1: HTTP Boundary - Field Survival & Snake/Camel Aliases into service.park
// ============================================================================

test("HTTP boundary preserves ALL predicate with snake_case and camelCase aliases into service.park", async () => {
  const harness = await createTestHarness();
  try {
    // 1. Snake case POST
    const resSnake = await harness.postPark({
      job_ids: ["job_all_1", "job_all_2"],
      predicate: "ALL",
      wake_on_exception: false,
      delivery_mode: "cli_resume",
      reason: "snake all park",
    });
    assert.equal(resSnake.status, 200);
    assert.equal(harness.calls.length, 1);
    const call0 = harness.calls[0]!.input;
    assert.deepEqual(call0.job_ids ?? call0.jobIds, ["job_all_1", "job_all_2"]);
    assert.equal(call0.predicate ?? call0.predicateType ?? call0.predicate_type, "ALL");
    assert.equal(call0.wake_on_exception ?? call0.wakeOnException, false);
    assert.equal(call0.delivery_mode ?? call0.deliveryMode, "cli_resume");
    assert.equal(call0.reason, "snake all park");

    // 2. Camel case POST
    const resCamel = await harness.postPark({
      jobIds: ["job_all_3", "job_all_4"],
      predicateType: "ALL",
      wakeOnException: true,
      deliveryMode: "none",
      reason: "camel all park",
    });
    assert.equal(resCamel.status, 200);
    assert.equal(harness.calls.length, 2);
    const call1 = harness.calls[1]!.input;
    assert.deepEqual(call1.job_ids ?? call1.jobIds, ["job_all_3", "job_all_4"]);
    assert.equal(call1.predicate ?? call1.predicateType ?? call1.predicate_type, "ALL");
    assert.equal(call1.wake_on_exception ?? call1.wakeOnException, true);
    assert.equal(call1.delivery_mode ?? call1.deliveryMode, "none");

    // 3. predicate_type alias POST
    const resType = await harness.postPark({
      job_ids: ["job_all_5"],
      predicate_type: "ALL",
    });
    assert.equal(resType.status, 200);
    assert.equal(harness.calls.length, 3);
    const call2 = harness.calls[2]!.input;
    assert.equal(call2.predicate ?? call2.predicateType ?? call2.predicate_type, "ALL");
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary preserves ANY predicate across snake and camel aliases into service.park", async () => {
  const harness = await createTestHarness();
  try {
    // Snake
    const resSnake = await harness.postPark({
      job_ids: ["job_any_1", "job_any_2"],
      predicate: "ANY",
    });
    assert.equal(resSnake.status, 200);
    const call0 = harness.calls[0]!.input;
    assert.equal(call0.predicate ?? call0.predicateType ?? call0.predicate_type, "ANY");

    // Camel
    const resCamel = await harness.postPark({
      jobIds: ["job_any_3", "job_any_4"],
      predicateType: "ANY",
    });
    assert.equal(resCamel.status, 200);
    const call1 = harness.calls[1]!.input;
    assert.equal(call1.predicate ?? call1.predicateType ?? call1.predicate_type, "ANY");

    // predicate_type alias
    const resType = await harness.postPark({
      job_ids: ["job_any_5"],
      predicate_type: "ANY",
    });
    assert.equal(resType.status, 200);
    const call2 = harness.calls[2]!.input;
    assert.equal(call2.predicate ?? call2.predicateType ?? call2.predicate_type, "ANY");
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary preserves QUORUM predicate and quorum_count / quorumCount aliases into service.park", async () => {
  const harness = await createTestHarness();
  try {
    // Snake case quorum
    const resSnake = await harness.postPark({
      job_ids: ["job_q_1", "job_q_2", "job_q_3"],
      predicate: "QUORUM",
      quorum_count: 2,
      wake_on_exception: false,
    });
    assert.equal(resSnake.status, 200);
    assert.equal(harness.calls.length, 1);
    const call0 = harness.calls[0]!.input;
    assert.equal(call0.predicate ?? call0.predicateType ?? call0.predicate_type, "QUORUM");
    assert.equal(call0.quorum_count ?? call0.quorumCount, 2);
    assert.equal(call0.wake_on_exception ?? call0.wakeOnException, false);

    // Camel case quorum
    const resCamel = await harness.postPark({
      jobIds: ["job_q_4", "job_q_5", "job_q_6"],
      predicateType: "QUORUM",
      quorumCount: 3,
      wakeOnException: true,
    });
    assert.equal(resCamel.status, 200);
    assert.equal(harness.calls.length, 2);
    const call1 = harness.calls[1]!.input;
    assert.equal(call1.predicate ?? call1.predicateType ?? call1.predicate_type, "QUORUM");
    assert.equal(call1.quorum_count ?? call1.quorumCount, 3);
    assert.equal(call1.wake_on_exception ?? call1.wakeOnException, true);

    // Mixed aliases: predicate_type + quorum_count
    const resMixed = await harness.postPark({
      job_ids: ["job_q_7", "job_q_8"],
      predicate_type: "QUORUM",
      quorum_count: 1,
    });
    assert.equal(resMixed.status, 200);
    assert.equal(harness.calls.length, 3);
    const call2 = harness.calls[2]!.input;
    assert.equal(call2.predicate ?? call2.predicateType ?? call2.predicate_type, "QUORUM");
    assert.equal(call2.quorum_count ?? call2.quorumCount, 1);
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary preserves REQUIRED predicate and required_job_ids / requiredJobIds aliases into service.park", async () => {
  const harness = await createTestHarness();
  try {
    // Snake case REQUIRED
    const resSnake = await harness.postPark({
      job_ids: ["job_r_1", "job_r_2"],
      predicate: "REQUIRED",
      required_job_ids: ["job_r_1"],
    });
    assert.equal(resSnake.status, 200);
    assert.equal(harness.calls.length, 1);
    const call0 = harness.calls[0]!.input;
    assert.equal(call0.predicate ?? call0.predicateType ?? call0.predicate_type, "REQUIRED");
    assert.deepEqual(call0.required_job_ids ?? call0.requiredJobIds, ["job_r_1"]);

    // Camel case REQUIRED
    const resCamel = await harness.postPark({
      jobIds: ["job_r_3", "job_r_4"],
      predicateType: "REQUIRED",
      requiredJobIds: ["job_r_4"],
    });
    assert.equal(resCamel.status, 200);
    assert.equal(harness.calls.length, 2);
    const call1 = harness.calls[1]!.input;
    assert.equal(call1.predicate ?? call1.predicateType ?? call1.predicate_type, "REQUIRED");
    assert.deepEqual(call1.required_job_ids ?? call1.requiredJobIds, ["job_r_4"]);
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary preserves job ID single/array aliases, correlation metadata, and is_alias into service.park", async () => {
  const harness = await createTestHarness();
  try {
    // Single job_id (snake)
    await harness.postPark({ job_id: "job_single_1" });
    assert.deepEqual(harness.calls[0]!.input.job_ids ?? harness.calls[0]!.input.jobIds, ["job_single_1"]);
    assert.equal(harness.calls[0]!.isAlias, false);

    // Single jobId (camel)
    await harness.postPark({ jobId: "job_single_2" });
    assert.deepEqual(harness.calls[1]!.input.job_ids ?? harness.calls[1]!.input.jobIds, ["job_single_2"]);

    // Metadata: park_id, thread_id, turn_id, goal_id, mcp_session_id, is_alias
    await harness.postPark({
      job_ids: ["job_meta_1"],
      park_id: "custom_park_1",
      thread_id: "thread_meta_1",
      turn_id: "turn_meta_1",
      goal_id: "goal_meta_1",
      mcp_session_id: "session_mcp_1",
      is_alias: true,
    });
    const callMeta = harness.calls[2]!;
    assert.equal(callMeta.isAlias, true);
    assert.equal(callMeta.input.park_id ?? callMeta.input.parkId, "custom_park_1");
    assert.equal(callMeta.input.thread_id ?? callMeta.input.threadId, "thread_meta_1");
    assert.equal(callMeta.input.turn_id ?? callMeta.input.turnId, "turn_meta_1");
    assert.equal(callMeta.input.goal_id ?? callMeta.input.goalId, "goal_meta_1");
    assert.equal(callMeta.input.mcp_session_id ?? callMeta.input.mcpSessionId, "session_mcp_1");

    // Client seam park method with isAlias = true
    await harness.client.park({ job_ids: ["job_meta_2"] }, true);
    assert.equal(harness.calls[3]!.isAlias, true);
  } finally {
    await harness.cleanup();
  }
});

// ============================================================================
// Suite 2: Invalid Values Fail Closed
// ============================================================================

test("HTTP boundary fails closed with 400 invalid_request on missing or malformed job_ids", async () => {
  const harness = await createTestHarness({ enforceValidation: true });
  try {
    // Missing job_ids
    const resEmpty = await harness.postPark({});
    assert.equal(resEmpty.status, 400);
    const bodyEmpty = (await resEmpty.json()) as { code: string };
    assert.equal(bodyEmpty.code, "invalid_request");

    // Empty array
    const resEmptyArr = await harness.postPark({ job_ids: [] });
    assert.equal(resEmptyArr.status, 400);

    // Array with whitespace only
    const resWhitespace = await harness.postPark({ job_ids: ["  ", ""] });
    assert.equal(resWhitespace.status, 400);

    // Non-array number
    const resNum = await harness.postPark({ job_ids: 12345 });
    assert.equal(resNum.status, 400);

    assert.equal(harness.calls.length, 0, "service.park must not be reached when job_ids is invalid");
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary fails closed with 400 invalid_request on invalid predicate value", async () => {
  const harness = await createTestHarness({ enforceValidation: true });
  try {
    const resBadPred = await harness.postPark({
      job_ids: ["job_bad_1"],
      predicate: "INVALID_PREDICATE_MODE",
    });
    assert.equal(resBadPred.status, 400);
    const body = (await resBadPred.json()) as { code: string };
    assert.equal(body.code, "invalid_request");
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary fails closed with 400 invalid_request on invalid QUORUM bounds", async () => {
  const harness = await createTestHarness({ enforceValidation: true });
  try {
    // Quorum count 0
    const resZero = await harness.postPark({
      job_ids: ["job_q_bad_1", "job_q_bad_2"],
      predicate: "QUORUM",
      quorum_count: 0,
    });
    assert.equal(resZero.status, 400);
    const bodyZero = (await resZero.json()) as { code: string };
    assert.equal(bodyZero.code, "invalid_request");

    // Quorum count exceeding number of jobs
    const resExceed = await harness.postPark({
      job_ids: ["job_q_bad_1", "job_q_bad_2"],
      predicate: "QUORUM",
      quorum_count: 5,
    });
    assert.equal(resExceed.status, 400);

    // Negative quorum count
    const resNeg = await harness.postPark({
      job_ids: ["job_q_bad_1"],
      predicate: "QUORUM",
      quorum_count: -1,
    });
    assert.equal(resNeg.status, 400);

    // Non-integer quorum count
    const resFloat = await harness.postPark({
      job_ids: ["job_q_bad_1"],
      predicate: "QUORUM",
      quorum_count: "two",
    });
    assert.equal(resFloat.status, 400);
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary fails closed with 400 invalid_request on invalid REQUIRED job IDs", async () => {
  const harness = await createTestHarness({ enforceValidation: true });
  try {
    // Empty required_job_ids array
    const resEmpty = await harness.postPark({
      job_ids: ["job_r_bad_1"],
      predicate: "REQUIRED",
      required_job_ids: [],
    });
    assert.equal(resEmpty.status, 400);
    const bodyEmpty = (await resEmpty.json()) as { code: string };
    assert.equal(bodyEmpty.code, "invalid_request");

    // Required job ID not in parked jobs
    const resMismatch = await harness.postPark({
      job_ids: ["job_r_bad_1"],
      predicate: "REQUIRED",
      required_job_ids: ["job_foreign_123"],
    });
    assert.equal(resMismatch.status, 400);

    // Non-array required_job_ids
    const resNotArr = await harness.postPark({
      job_ids: ["job_r_bad_1"],
      predicate: "REQUIRED",
      required_job_ids: "not-an-array",
    });
    assert.equal(resNotArr.status, 400);
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary fails closed with 400 on invalid delivery_mode or non-boolean wake_on_exception", async () => {
  const harness = await createTestHarness({ enforceValidation: true });
  try {
    const resBadMode = await harness.postPark({
      job_ids: ["job_mode_1"],
      delivery_mode: "unsupported_delivery_strategy",
    });
    assert.equal(resBadMode.status, 400);
    const bodyMode = (await resBadMode.json()) as { code: string };
    assert.equal(bodyMode.code, "invalid_request");

    const resBadWake = await harness.postPark({
      job_ids: ["job_wake_1"],
      wake_on_exception: "not_a_boolean",
    });
    assert.equal(resBadWake.status, 400);
    const bodyWake = (await resBadWake.json()) as { code: string };
    assert.equal(bodyWake.code, "invalid_request");
  } finally {
    await harness.cleanup();
  }
});

// ============================================================================
// Suite 3: wait=true is Rejected and Omitted wait is Immediate
// ============================================================================

test("HTTP boundary rejects wait=true with typed 400 invalid_request directing caller to follow", async () => {
  const harness = await createTestHarness({ enforceValidation: true });
  try {
    // 1. Raw HTTP POST with wait: true
    const resHttp = await harness.postPark({
      job_ids: ["job_wait_1"],
      wait: true,
    });
    assert.equal(resHttp.status, 400);
    const bodyHttp = (await resHttp.json()) as { error: string; code: string };
    assert.equal(bodyHttp.code, "invalid_request");
    assert.match(bodyHttp.error, /wait=true is no longer supported for park/i);
    assert.match(bodyHttp.error, /subagents_follow/);

    // 2. Client seam park call with wait: true
    await assert.rejects(
      () => harness.client.park({ job_ids: ["job_wait_2"], wait: true }),
      (err: unknown) => {
        assert.ok(err instanceof BridgeHttpError);
        assert.equal(err.status, 400);
        assert.equal(err.code, "invalid_request");
        assert.match(err.message, /wait=true is no longer supported for park/i);
        return true;
      },
    );

    // 3. Client seam park alias call with wait: true directs to deepseek_follow
    await assert.rejects(
      () => harness.client.park({ job_ids: ["job_wait_3"], wait: true }, true),
      (err: unknown) => {
        assert.ok(err instanceof BridgeHttpError);
        assert.equal(err.status, 400);
        assert.match(err.message, /deepseek_follow/);
        return true;
      },
    );
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary treats omitted wait as immediate return with armed receipt", async () => {
  const harness = await createTestHarness({ enforceValidation: true });
  try {
    // Omitted wait via HTTP POST
    const startHttp = Date.now();
    const resHttp = await harness.postPark({
      job_ids: ["job_imm_1"],
    });
    const elapsedHttp = Date.now() - startHttp;
    assert.equal(resHttp.status, 200);
    assert.ok(elapsedHttp < 1000, "Omitted wait must respond immediately without polling or sleeping");

    const receiptHttp = (await resHttp.json()) as ParkReceipt;
    assert.equal(receiptHttp.armed, true);
    assert.equal(receiptHttp.obligationState, "pending");
    assert.equal(receiptHttp.deliveryMode, "cli_resume");
    assert.equal(harness.calls[0]!.input.wait, undefined, "Service must receive undefined wait when omitted");

    // Omitted wait via BridgeHttpClient
    const startClient = Date.now();
    const receiptClient = await harness.client.park({ job_ids: ["job_imm_2"] });
    const elapsedClient = Date.now() - startClient;
    assert.ok(elapsedClient < 1000, "Client seam must resolve immediately");
    assert.equal(receiptClient.armed, true);
    assert.equal(receiptClient.obligationState, "pending");
  } finally {
    await harness.cleanup();
  }
});

// ============================================================================
// Suite 4: MCP Handlers Forward Fields Losslessly
// ============================================================================

test("MCP subagents_park forwards ANY, ALL, QUORUM, REQUIRED fields losslessly to bridge client", async () => {
  const forwardedCalls: Array<{ pathname: string; body: unknown }> = [];
  const fakeBridgeClient = {
    call: async (pathname: string, body?: unknown) => {
      forwardedCalls.push({ pathname, body });
      const val = (body ?? {}) as Record<string, unknown>;
      return {
        parkId: "park_mcp_1",
        generation: 1,
        armed: true,
        targetIdentity: "thread_mcp_target",
        deliveryMode: "cli_resume",
        wakeState: "waiting",
        obligationState: "pending",
        nextAction: "subagents_follow",
        nextRequiredAction: "subagents_follow",
        jobIds: val.job_ids ?? ["job_mcp_1"],
        reason: val.reason ?? null,
        pendingCount: 1,
        readyCount: 0,
        predicateType: val.predicate ?? "ALL",
        quorumCount: val.quorum_count ?? null,
        requiredJobIds: val.required_job_ids ?? null,
      };
    },
  } as unknown as BridgeHttpClient;

  const mcp = await createMcpHarness(fakeBridgeClient);
  try {
    // 1. Call subagents_park with QUORUM
    const quorumRes = await mcp.client.callTool({
      name: "subagents_park",
      arguments: {
        job_ids: ["job_m_1", "job_m_2"],
        predicate: "QUORUM",
        quorum_count: 2,
        wake_on_exception: false,
        reason: "waiting for 2 of 2",
      },
    });
    assert.equal(quorumRes.isError, undefined);
    assert.equal(forwardedCalls.length, 1);
    const fwd0 = forwardedCalls[0]!;
    assert.equal(fwd0.pathname, "/v1/jobs/park");
    const payload0 = fwd0.body as Record<string, unknown>;
    assert.deepEqual(payload0.job_ids, ["job_m_1", "job_m_2"]);
    assert.equal(payload0.predicate, "QUORUM");
    assert.equal(payload0.quorum_count, 2);
    assert.equal(payload0.wake_on_exception, false);
    assert.equal(payload0.reason, "waiting for 2 of 2");
    assert.equal(payload0.wait, false, "MCP must normalize wait to false for external parking");
    assert.equal(payload0.is_alias, undefined, "Canonical subagents_park must not set is_alias");

    const struct0 = quorumRes.structuredContent as Record<string, unknown>;
    assert.equal(struct0.nextRequiredAction, "subagents_follow");
    assert.equal(struct0.predicateType, "QUORUM");
    assert.equal(struct0.quorumCount, 2);

    // 2. Call subagents_park with REQUIRED
    const reqRes = await mcp.client.callTool({
      name: "subagents_park",
      arguments: {
        job_ids: ["job_m_3", "job_m_4"],
        predicate: "REQUIRED",
        required_job_ids: ["job_m_3"],
      },
    });
    assert.equal(reqRes.isError, undefined);
    assert.equal(forwardedCalls.length, 2);
    const payload1 = forwardedCalls[1]!.body as Record<string, unknown>;
    assert.equal(payload1.predicate, "REQUIRED");
    assert.deepEqual(payload1.required_job_ids, ["job_m_3"]);

    const struct1 = reqRes.structuredContent as Record<string, unknown>;
    assert.equal(struct1.predicateType, "REQUIRED");
    assert.deepEqual(struct1.requiredJobIds, ["job_m_3"]);

    // 3. Reject wait: true in subagents_park
    const waitRes = await mcp.client.callTool({
      name: "subagents_park",
      arguments: {
        job_ids: ["job_m_5"],
        wait: true,
      },
    });
    assert.equal(waitRes.isError, true);
    const waitText = (waitRes.content as Array<{ type: string; text?: string }>).find((c) => c.type === "text")?.text ?? "";
    assert.match(waitText, /In-turn waiting \(wait=true\) is no longer supported on park/i);
    assert.match(waitText, /subagents_follow/);
    assert.equal(forwardedCalls.length, 2, "wait=true must be rejected at MCP tool boundary before calling bridge");
  } finally {
    await mcp.cleanup();
  }
});

test("MCP deepseek_park alias forwards ANY, ALL, QUORUM, REQUIRED fields losslessly with is_alias: true", async () => {
  const forwardedCalls: Array<{ pathname: string; body: unknown }> = [];
  const fakeBridgeClient = {
    call: async (pathname: string, body?: unknown) => {
      forwardedCalls.push({ pathname, body });
      const val = (body ?? {}) as Record<string, unknown>;
      return {
        parkId: "park_ds_1",
        generation: 1,
        armed: true,
        targetIdentity: "thread_ds_target",
        deliveryMode: "cli_resume",
        wakeState: "waiting",
        obligationState: "pending",
        nextAction: "deepseek_follow",
        nextRequiredAction: "deepseek_follow",
        jobIds: val.job_ids ?? ["job_ds_1"],
        reason: val.reason ?? null,
        pendingCount: 1,
        readyCount: 0,
        predicateType: val.predicate ?? "ALL",
        quorumCount: val.quorum_count ?? null,
        requiredJobIds: val.required_job_ids ?? null,
      };
    },
  } as unknown as BridgeHttpClient;

  const mcp = await createMcpHarness(fakeBridgeClient);
  try {
    // 1. deepseek_park with QUORUM
    const quorumRes = await mcp.client.callTool({
      name: "deepseek_park",
      arguments: {
        job_ids: ["job_ds_1", "job_ds_2"],
        predicate: "QUORUM",
        quorum_count: 1,
        wake_on_exception: true,
      },
    });
    assert.equal(quorumRes.isError, undefined);
    assert.equal(forwardedCalls.length, 1);
    const payload0 = forwardedCalls[0]!.body as Record<string, unknown>;
    assert.equal(payload0.is_alias, true, "deepseek_park alias must set is_alias: true");
    assert.equal(payload0.predicate, "QUORUM");
    assert.equal(payload0.quorum_count, 1);
    assert.equal(payload0.wake_on_exception, true);
    assert.equal(payload0.wait, false);

    const struct0 = quorumRes.structuredContent as Record<string, unknown>;
    assert.equal(struct0.nextRequiredAction, "deepseek_follow");

    // 2. deepseek_park with REQUIRED
    const reqRes = await mcp.client.callTool({
      name: "deepseek_park",
      arguments: {
        job_ids: ["job_ds_3", "job_ds_4"],
        predicate: "REQUIRED",
        required_job_ids: ["job_ds_4"],
      },
    });
    assert.equal(reqRes.isError, undefined);
    assert.equal(forwardedCalls.length, 2);
    const payload1 = forwardedCalls[1]!.body as Record<string, unknown>;
    assert.equal(payload1.is_alias, true);
    assert.equal(payload1.predicate, "REQUIRED");
    assert.deepEqual(payload1.required_job_ids, ["job_ds_4"]);

    // 3. deepseek_park with wait: true directs to deepseek_follow
    const waitRes = await mcp.client.callTool({
      name: "deepseek_park",
      arguments: {
        job_ids: ["job_ds_5"],
        wait: true,
      },
    });
    assert.equal(waitRes.isError, true);
    const waitText = (waitRes.content as Array<{ type: string; text?: string }>).find((c) => c.type === "text")?.text ?? "";
    assert.match(waitText, /deepseek_follow/);
    assert.equal(forwardedCalls.length, 2);
  } finally {
    await mcp.cleanup();
  }
});

// ============================================================================
// Suite 5: Full End-to-End Traverse: MCP -> HttpClient -> HttpServer -> service.park
// ============================================================================

test("Full boundary traverse: MCP Client -> MCP Server -> BridgeHttpClient -> BridgeHttpServer -> service.park", async () => {
  const harness = await createTestHarness();
  const mcp = await createMcpHarness(harness.client);
  try {
    // Traverse canonical subagents_park through the live HTTP wire
    const resCanonical = await mcp.client.callTool({
      name: "subagents_park",
      arguments: {
        job_ids: ["job_e2e_1", "job_e2e_2"],
        predicate: "QUORUM",
        quorum_count: 2,
        wake_on_exception: false,
        reason: "end to end quorum",
      },
    });
    assert.equal(resCanonical.isError, undefined);
    assert.equal(harness.calls.length, 1);
    const call0 = harness.calls[0]!;
    assert.equal(call0.isAlias, false);
    assert.deepEqual(call0.input.job_ids ?? call0.input.jobIds, ["job_e2e_1", "job_e2e_2"]);
    assert.equal(call0.input.predicate ?? call0.input.predicateType ?? call0.input.predicate_type, "QUORUM");
    assert.equal(call0.input.quorum_count ?? call0.input.quorumCount, 2);
    assert.equal(call0.input.wake_on_exception ?? call0.input.wakeOnException, false);
    assert.equal(call0.input.reason, "end to end quorum");

    // Traverse alias deepseek_park through the live HTTP wire
    const resAlias = await mcp.client.callTool({
      name: "deepseek_park",
      arguments: {
        job_ids: ["job_e2e_3", "job_e2e_4"],
        predicate: "REQUIRED",
        required_job_ids: ["job_e2e_3"],
      },
    });
    assert.equal(resAlias.isError, undefined);
    assert.equal(harness.calls.length, 2);
    const call1 = harness.calls[1]!;
    assert.equal(call1.isAlias, true);
    assert.equal(call1.input.predicate ?? call1.input.predicateType ?? call1.input.predicate_type, "REQUIRED");
    assert.deepEqual(call1.input.required_job_ids ?? call1.input.requiredJobIds, ["job_e2e_3"]);
  } finally {
    await mcp.cleanup();
    await harness.cleanup();
  }
});

// ============================================================================
// Suite 6: Queued Wake Delivery Mode and Queue Message ID Boundary Contract
// ============================================================================

test("HTTP boundary preserves 'queued' delivery mode and snake/camel queue message ID into service.park and echoes them in response receipt", async () => {
  const harness = await createTestHarness();
  try {
    // 1. Snake case POST with delivery_mode: queued and queue_message_id
    const resSnake = await harness.postPark({
      job_ids: ["job_q_s1", "job_q_s2"],
      delivery_mode: "queued",
      queue_message_id: "qmsg_snake_101",
      reason: "snake queued park",
    });
    assert.equal(resSnake.status, 200);
    assert.equal(harness.calls.length, 1);
    const call0 = harness.calls[0]!.input;
    assert.equal(call0.delivery_mode ?? call0.deliveryMode, "queued");
    assert.equal(
      (call0 as any).queue_message_id ?? (call0 as any).queueMessageId,
      "qmsg_snake_101",
    );

    const bodySnake = (await resSnake.json()) as Record<string, unknown>;
    assert.equal(bodySnake.deliveryMode, "queued");
    assert.equal(bodySnake.delivery_mode, "queued");
    assert.equal(bodySnake.queueMessageId, "qmsg_snake_101");
    assert.equal(bodySnake.queue_message_id, "qmsg_snake_101");

    // 2. Camel case POST with deliveryMode: queued and queueMessageId
    const resCamel = await harness.postPark({
      jobIds: ["job_q_c1", "job_q_c2"],
      deliveryMode: "queued",
      queueMessageId: "qmsg_camel_102",
      reason: "camel queued park",
    });
    assert.equal(resCamel.status, 200);
    assert.equal(harness.calls.length, 2);
    const call1 = harness.calls[1]!.input;
    assert.equal(call1.delivery_mode ?? call1.deliveryMode, "queued");
    assert.equal(
      (call1 as any).queue_message_id ?? (call1 as any).queueMessageId,
      "qmsg_camel_102",
    );

    const bodyCamel = (await resCamel.json()) as Record<string, unknown>;
    assert.equal(bodyCamel.deliveryMode, "queued");
    assert.equal(bodyCamel.delivery_mode, "queued");
    assert.equal(bodyCamel.queueMessageId, "qmsg_camel_102");
    assert.equal(bodyCamel.queue_message_id, "qmsg_camel_102");

    // 3. Aliases: message_id and messageId
    const resMsgSnake = await harness.postPark({
      job_ids: ["job_q_m1"],
      delivery_mode: "queued",
      message_id: "qmsg_msgid_103",
    });
    assert.equal(resMsgSnake.status, 200);
    const bodyMsgSnake = (await resMsgSnake.json()) as Record<string, unknown>;
    assert.equal(bodyMsgSnake.queueMessageId, "qmsg_msgid_103");

    const resMsgCamel = await harness.postPark({
      job_ids: ["job_q_m2"],
      deliveryMode: "queued",
      messageId: "qmsg_msgid_104",
    });
    assert.equal(resMsgCamel.status, 200);
    const bodyMsgCamel = (await resMsgCamel.json()) as Record<string, unknown>;
    assert.equal(bodyMsgCamel.queueMessageId, "qmsg_msgid_104");
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary preserves backward compatibility for legacy in_turn input and advertises queued, cli_resume, and none delivery modes only", async () => {
  const harness = await createTestHarness();
  try {
    // cli_resume
    const resCli = await harness.postPark({
      job_ids: ["job_compat_1"],
      delivery_mode: "cli_resume",
    });
    assert.equal(resCli.status, 200);
    const bodyCli = (await resCli.json()) as Record<string, unknown>;
    assert.equal(bodyCli.deliveryMode, "cli_resume");

    // in_turn: accepted on input for backward compatibility, but never serialized as armed in_turn receipt
    const resInTurn = await harness.postPark({
      job_ids: ["job_compat_2"],
      delivery_mode: "in_turn",
    });
    assert.equal(resInTurn.status, 200);
    const bodyInTurn = (await resInTurn.json()) as Record<string, unknown>;
    assert.equal(bodyInTurn.deliveryMode, "cli_resume");
    assert.notEqual(bodyInTurn.deliveryMode, "in_turn");

    // none
    const resNone = await harness.postPark({
      job_ids: ["job_compat_3"],
      delivery_mode: "none",
    });
    assert.equal(resNone.status, 200);
    const bodyNone = (await resNone.json()) as Record<string, unknown>;
    assert.equal(bodyNone.deliveryMode, "none");
  } finally {
    await harness.cleanup();
  }
});

test("HTTP boundary faithfully exposes queue message ID surfaced by outbox status objects in ParkReceipt", async () => {
  // 1. Receipt with nested outbox object containing message_id
  const harnessOutbox = await createTestHarness({
    enforceValidation: false,
  });
  (harnessOutbox.service as any).park = async () => ({
    parkId: "park_outbox_test",
    generation: 1,
    armed: true,
    targetIdentity: "thread_outbox_target",
    deliveryMode: "queued",
    wakeState: "delivered",
    obligationState: "pending",
    nextAction: "subagents_follow",
    nextRequiredAction: "subagents_follow",
    jobIds: ["job_ob_1"],
    reason: null,
    pendingCount: 1,
    readyCount: 1,
    outbox: {
      id: "outbox_row_123",
      message_id: "msg_from_outbox_status_555",
      status: "delivered",
    },
  });

  try {
    const res = await harnessOutbox.postPark({ job_ids: ["job_ob_1"] });
    assert.equal(res.status, 200);
    const body = (await res.json()) as Record<string, unknown>;
    assert.equal(body.deliveryMode, "queued");
    assert.equal(body.queueMessageId, "msg_from_outbox_status_555");
    assert.equal(body.queue_message_id, "msg_from_outbox_status_555");
  } finally {
    await harnessOutbox.cleanup();
  }

  // 2. Receipt with outboxStatus object containing queueMessageId
  const harnessOutboxStatus = await createTestHarness({
    enforceValidation: false,
  });
  (harnessOutboxStatus.service as any).park = async () => ({
    parkId: "park_outbox_status_test",
    generation: 1,
    armed: true,
    targetIdentity: "thread_outbox_status_target",
    deliveryMode: "queued",
    wakeState: "delivered",
    obligationState: "pending",
    nextAction: "subagents_follow",
    nextRequiredAction: "subagents_follow",
    jobIds: ["job_obs_1"],
    reason: null,
    pendingCount: 1,
    readyCount: 1,
    outboxStatus: {
      queueMessageId: "msg_from_outbox_status_666",
      status: "delivered",
    },
  });

  try {
    const res = await harnessOutboxStatus.postPark({ job_ids: ["job_obs_1"] });
    assert.equal(res.status, 200);
    const body = (await res.json()) as Record<string, unknown>;
    assert.equal(body.deliveryMode, "queued");
    assert.equal(body.queueMessageId, "msg_from_outbox_status_666");
    assert.equal(body.queue_message_id, "msg_from_outbox_status_666");
  } finally {
    await harnessOutboxStatus.cleanup();
  }
});

test("MCP subagents_park tool accepts queued delivery mode and queue message id, forwarding to bridge and exposing in structuredContent", async () => {
  const forwardedCalls: Array<{ pathname: string; body: unknown }> = [];
  const fakeBridgeClient = {
    call: async (pathname: string, body?: unknown) => {
      forwardedCalls.push({ pathname, body });
      const val = (body ?? {}) as Record<string, unknown>;
      return {
        parkId: "park_mcp_q1",
        generation: 1,
        armed: true,
        targetIdentity: "thread_mcp_q_target",
        deliveryMode: val.deliveryMode ?? val.delivery_mode ?? "queued",
        wakeState: "waiting",
        obligationState: "pending",
        nextAction: "subagents_follow",
        nextRequiredAction: "subagents_follow",
        jobIds: val.job_ids ?? ["job_mcp_q1"],
        reason: val.reason ?? null,
        pendingCount: 1,
        readyCount: 0,
        queueMessageId: val.queueMessageId ?? val.queue_message_id ?? "msg_default_q",
      };
    },
  } as unknown as BridgeHttpClient;

  const mcp = await createMcpHarness(fakeBridgeClient);
  try {
    const res = await mcp.client.callTool({
      name: "subagents_park",
      arguments: {
        job_ids: ["job_m_q1", "job_m_q2"],
        delivery_mode: "queued",
        queue_message_id: "msg_subagents_queued_01",
        reason: "mcp subagents queued test",
      },
    });

    assert.equal(res.isError, undefined);
    assert.equal(forwardedCalls.length, 1);
    const payload = forwardedCalls[0]!.body as Record<string, unknown>;
    assert.equal(payload.delivery_mode, "queued");
    assert.equal(payload.deliveryMode, "queued");
    assert.equal(payload.queue_message_id, "msg_subagents_queued_01");
    assert.equal(payload.queueMessageId, "msg_subagents_queued_01");

    const struct = res.structuredContent as Record<string, unknown>;
    assert.equal(struct.deliveryMode, "queued");
    assert.equal(struct.delivery_mode, "queued");
    assert.equal(struct.queueMessageId, "msg_subagents_queued_01");
    assert.equal(struct.queue_message_id, "msg_subagents_queued_01");
    assert.equal(struct.nextRequiredAction, "subagents_follow");
  } finally {
    await mcp.cleanup();
  }
});

test("MCP deepseek_park alias tool accepts queued delivery mode and queue message id, forwarding with is_alias: true and exposing in structuredContent", async () => {
  const forwardedCalls: Array<{ pathname: string; body: unknown }> = [];
  const fakeBridgeClient = {
    call: async (pathname: string, body?: unknown) => {
      forwardedCalls.push({ pathname, body });
      const val = (body ?? {}) as Record<string, unknown>;
      return {
        parkId: "park_ds_q1",
        generation: 1,
        armed: true,
        targetIdentity: "thread_ds_q_target",
        deliveryMode: val.deliveryMode ?? val.delivery_mode ?? "queued",
        wakeState: "waiting",
        obligationState: "pending",
        nextAction: "deepseek_follow",
        nextRequiredAction: "deepseek_follow",
        jobIds: val.job_ids ?? ["job_ds_q1"],
        reason: val.reason ?? null,
        pendingCount: 1,
        readyCount: 0,
        queueMessageId: val.queueMessageId ?? val.queue_message_id ?? "msg_ds_default_q",
      };
    },
  } as unknown as BridgeHttpClient;

  const mcp = await createMcpHarness(fakeBridgeClient);
  try {
    const res = await mcp.client.callTool({
      name: "deepseek_park",
      arguments: {
        job_ids: ["job_ds_q1"],
        deliveryMode: "queued",
        queueMessageId: "msg_deepseek_queued_02",
        reason: "mcp deepseek queued test",
      },
    });

    assert.equal(res.isError, undefined);
    assert.equal(forwardedCalls.length, 1);
    const payload = forwardedCalls[0]!.body as Record<string, unknown>;
    assert.equal(payload.is_alias, true);
    assert.equal(payload.deliveryMode, "queued");
    assert.equal(payload.delivery_mode, "queued");
    assert.equal(payload.queueMessageId, "msg_deepseek_queued_02");
    assert.equal(payload.queue_message_id, "msg_deepseek_queued_02");

    const struct = res.structuredContent as Record<string, unknown>;
    assert.equal(struct.deliveryMode, "queued");
    assert.equal(struct.delivery_mode, "queued");
    assert.equal(struct.queueMessageId, "msg_deepseek_queued_02");
    assert.equal(struct.queue_message_id, "msg_deepseek_queued_02");
    assert.equal(struct.nextRequiredAction, "deepseek_follow");
  } finally {
    await mcp.cleanup();
  }
});

test("Full boundary traverse with queued delivery mode: MCP Client -> MCP Server -> BridgeHttpClient -> BridgeHttpServer -> service.park", async () => {
  const harness = await createTestHarness();
  const mcp = await createMcpHarness(harness.client);
  try {
    // 1. Canonical subagents_park with queued delivery mode across live HTTP boundary
    const resCanonical = await mcp.client.callTool({
      name: "subagents_park",
      arguments: {
        job_ids: ["job_e2e_q1", "job_e2e_q2"],
        delivery_mode: "queued",
        queue_message_id: "msg_e2e_wire_canonical",
        reason: "e2e queued canonical",
      },
    });
    assert.equal(resCanonical.isError, undefined);
    assert.equal(harness.calls.length, 1);
    const call0 = harness.calls[0]!;
    assert.equal(call0.isAlias, false);
    assert.equal(call0.input.delivery_mode ?? call0.input.deliveryMode, "queued");
    assert.equal(
      (call0.input as any).queue_message_id ?? (call0.input as any).queueMessageId,
      "msg_e2e_wire_canonical",
    );

    const struct0 = resCanonical.structuredContent as Record<string, unknown>;
    assert.equal(struct0.deliveryMode, "queued");
    assert.equal(struct0.delivery_mode, "queued");
    assert.equal(struct0.queueMessageId, "msg_e2e_wire_canonical");
    assert.equal(struct0.queue_message_id, "msg_e2e_wire_canonical");
    assert.equal(struct0.nextRequiredAction, "subagents_follow");

    // 2. Alias deepseek_park with queued delivery mode across live HTTP boundary
    const resAlias = await mcp.client.callTool({
      name: "deepseek_park",
      arguments: {
        job_ids: ["job_e2e_q3", "job_e2e_q4"],
        deliveryMode: "queued",
        queueMessageId: "msg_e2e_wire_alias",
        reason: "e2e queued alias",
      },
    });
    assert.equal(resAlias.isError, undefined);
    assert.equal(harness.calls.length, 2);
    const call1 = harness.calls[1]!;
    assert.equal(call1.isAlias, true);
    assert.equal(call1.input.delivery_mode ?? call1.input.deliveryMode, "queued");
    assert.equal(
      (call1.input as any).queue_message_id ?? (call1.input as any).queueMessageId,
      "msg_e2e_wire_alias",
    );

    const struct1 = resAlias.structuredContent as Record<string, unknown>;
    assert.equal(struct1.deliveryMode, "queued");
    assert.equal(struct1.delivery_mode, "queued");
    assert.equal(struct1.queueMessageId, "msg_e2e_wire_alias");
    assert.equal(struct1.queue_message_id, "msg_e2e_wire_alias");
    assert.equal(struct1.nextRequiredAction, "deepseek_follow");
  } finally {
    await mcp.cleanup();
    await harness.cleanup();
  }
});

test("Metadata-only contract: queued wake delivery mode and queue message id preserve metadata purity with zero worker result leakage", async () => {
  const secretWorkerText = "TOP_SECRET_CODE_DIFF_OR_WORKER_OUTPUT_SHOULD_NEVER_LEAK";
  const fakeBridgeClient = {
    call: async () => ({
      parkId: "park_pure_meta",
      generation: 1,
      armed: true,
      targetIdentity: "thread_pure_meta",
      deliveryMode: "queued",
      wakeState: "delivered",
      obligationState: "pending",
      nextAction: "subagents_follow",
      nextRequiredAction: "subagents_follow",
      jobIds: ["job_pure_1"],
      reason: "metadata only reason",
      pendingCount: 1,
      readyCount: 1,
      queueMessageId: "msg_clean_meta_id_999",
    }),
  } as unknown as BridgeHttpClient;

  const mcp = await createMcpHarness(fakeBridgeClient);
  try {
    const res = await mcp.client.callTool({
      name: "subagents_park",
      arguments: {
        job_ids: ["job_pure_1"],
        delivery_mode: "queued",
        queue_message_id: "msg_clean_meta_id_999",
      },
    });

    const textContent = (res.content as Array<{ type: string; text?: string }>).find((c) => c.type === "text")?.text ?? "";
    assert.equal(textContent.includes(secretWorkerText), false);
    const structJson = JSON.stringify(res.structuredContent);
    assert.equal(structJson.includes(secretWorkerText), false);

    const struct = res.structuredContent as Record<string, unknown>;
    assert.equal(struct.deliveryMode, "queued");
    assert.equal(struct.queueMessageId, "msg_clean_meta_id_999");
  } finally {
    await mcp.cleanup();
  }
});

test("MCP public output schema advertises queued|cli_resume|none only (never in_turn)", async () => {
  const harness = await createTestHarness();
  const mcp = await createMcpHarness(harness.client);
  try {
    const tools = await mcp.client.listTools();
    const subTool = tools.tools.find((t) => t.name === "subagents_park")!;
    const dsTool = tools.tools.find((t) => t.name === "deepseek_park")!;
    assert.ok(subTool, "subagents_park must be registered");
    assert.ok(dsTool, "deepseek_park must be registered");

    const subDeliveryModeStr = JSON.stringify((subTool.outputSchema as any)?.properties?.deliveryMode ?? {});
    assert.match(subDeliveryModeStr, /queued/);
    assert.match(subDeliveryModeStr, /cli_resume/);
    assert.match(subDeliveryModeStr, /none/);
    assert.doesNotMatch(subDeliveryModeStr, /in_turn/);

    const dsDeliveryModeStr = JSON.stringify((dsTool.outputSchema as any)?.properties?.deliveryMode ?? {});
    assert.match(dsDeliveryModeStr, /queued/);
    assert.match(dsDeliveryModeStr, /cli_resume/);
    assert.match(dsDeliveryModeStr, /none/);
    assert.doesNotMatch(dsDeliveryModeStr, /in_turn/);
  } finally {
    await mcp.cleanup();
    await harness.cleanup();
  }
});


