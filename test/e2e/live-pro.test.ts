// Dedicated live smoke for the exactly-one real Pro inference.
// Runs ONLY when BRIDGE_LIVE_PRO_E2E=1 is set; the existing multi-call Flash
// live tests (live.test.ts) require their own BRIDGE_LIVE_E2E flag, so
// running this file separately never invokes them.
// It uses a disposable temp config/dataDir and never touches live config.
//
// Exactly-one semantics: the bridge may legitimately attempt a second
// promptAsync after the follow deadline (graceful finalization). Timing
// bounds alone therefore cannot guarantee a single provider inference. The
// OneShotPromptClient guard is the guarantee: the first promptAsync is
// delegated to the real OpenCode client, and ANY second promptAsync is
// rejected BEFORE it reaches the provider, failing the test loudly.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig, defaultConfigPath } from "../../src/config.js";
import { defaultUserDataRoot } from "../../src/security.js";
import { OpenCodeManager } from "../../src/opencode/manager.js";
import { BridgeService, type ManagedOpenCodeLike, type OpenCodeManagerLike } from "../../src/service.js";
import type { OpenCodeClientLike, OpenCodeEvent, OpenCodeMessage, OpenCodeSession } from "../../src/types.js";

const LIVE_PRO_FLAG = process.env.BRIDGE_LIVE_PRO_E2E === "1";

/**
 * Wraps the real OpenCode client so that at most ONE promptAsync ever reaches
 * the provider. The first call is delegated unchanged; every later call is
 * counted and rejected before delegation. Graceful-finalization prompts that
 * the bridge may attempt after the follow deadline are therefore blocked, and
 * the test asserts the counter stayed zero.
 */
class OneShotPromptClient implements OpenCodeClientLike {
  promptDelegations = 0;
  blockedPromptAttempts = 0;

  constructor(private readonly inner: OpenCodeClientLike) {}

  async health(): Promise<{ healthy: boolean; version?: string }> {
    return this.inner.health();
  }
  async createSession(directory: string, title: string): Promise<OpenCodeSession> {
    return this.inner.createSession(directory, title);
  }
  async promptAsync(sessionId: string, task: string, options: { providerId: string; modelId: string; variant?: string; agent?: string }): Promise<void> {
    if (this.promptDelegations >= 1) {
      this.blockedPromptAttempts += 1;
      throw new Error("LIVE_PRO_GUARD: a second promptAsync was attempted (graceful finalization or retry); refused before reaching the provider");
    }
    this.promptDelegations += 1;
    return this.inner.promptAsync(sessionId, task, options);
  }
  async listMessages(sessionId: string): Promise<OpenCodeMessage[]> {
    return this.inner.listMessages(sessionId);
  }
  async getDiff(sessionId: string): Promise<unknown> {
    return this.inner.getDiff(sessionId);
  }
  async abort(sessionId: string): Promise<void> {
    return this.inner.abort(sessionId);
  }
  async replyPermission(sessionId: string, permissionId: string, reply: "once" | "always" | "reject", message?: string): Promise<void> {
    return this.inner.replyPermission(sessionId, permissionId, reply, message);
  }
  async subscribe(onEvent: (event: OpenCodeEvent) => Promise<void> | void, signal?: AbortSignal): Promise<void> {
    return this.inner.subscribe(onEvent, signal);
  }
}

/**
 * Injects the guarded client into the bridge while the real OpenCode child
 * stays owned by a real OpenCodeManager. stop() is idempotent so it is safe
 * to call from both the service shutdown and the test finally.
 */
class LiveProManagerAdapter implements OpenCodeManagerLike {
  private managed: ManagedOpenCodeLike | null = null;
  private guard: OneShotPromptClient | null = null;

  constructor(private readonly real: OpenCodeManager) {}

  async start(workspaceRoot: string): Promise<ManagedOpenCodeLike> {
    if (!this.managed) {
      const managed = await this.real.start(workspaceRoot);
      this.managed = managed;
      this.guard = new OneShotPromptClient(managed.client);
    }
    const managed = this.managed;
    const guard = this.guard;
    if (!managed || !guard) throw new Error("LiveProManagerAdapter failed to start");
    return {
      serverId: managed.serverId,
      baseUrl: managed.baseUrl,
      client: guard,
      processId: managed.processId,
      stop: async () => {
        await this.stop();
      },
    };
  }

  async stop(): Promise<void> {
    await this.real.stop();
  }

  get guardClient(): OneShotPromptClient | null {
    return this.guard;
  }
}

/**
 * Metadata snapshot (stat only, never file contents) used to prove the
 * persistent default config and database are untouched by this test.
 */
async function pathSnapshot(filePath: string): Promise<{ exists: boolean; mtimeMs: number; size: number } | null> {
  try {
    const info = await stat(filePath);
    return { exists: true, mtimeMs: info.mtimeMs, size: info.size };
  } catch {
    return null;
  }
}

test("live bridge spawn model_route=pro-max smoke with exactly one real inference", { skip: LIVE_PRO_FLAG ? false : "set BRIDGE_LIVE_PRO_E2E=1 to run the live Pro smoke" }, async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-live-pro-"));
  const defaultConfigBefore = await pathSnapshot(defaultConfigPath());
  const defaultDbBefore = await pathSnapshot(path.join(defaultUserDataRoot(), "bridge.sqlite"));
  let service: BridgeService | null = null;
  let adapter: LiveProManagerAdapter | null = null;
  try {
    const config = createDefaultConfig({
      dataDir: directory,
      configPath: path.join(directory, "config.json"),
      opencodeMode: "managed",
      opencodeStartupTimeoutMs: 45_000,
      opencodeEventReconnectMaxMs: 5_000,
      // Genuinely short bounded follow window: the config defaults are the
      // bridge's enforced MINIMUM (values below the defaults are raised), so
      // 1 minute wait + 1 minute grace is the shortest supported window.
      // Timing is a bound, not the exactly-one guarantee: the guard enforces
      // a single provider prompt even if the deadline triggers a graceful
      // finalization attempt.
      followDefaultWaitMinutes: 1,
      followDefaultGraceMinutes: 1,
      // Disposable registry for this test only: flash-max stays the enabled
      // default; pro-max is enabled only here so the bridge's real
      // spawn model_route="pro-max" path can be exercised.
      modelRoutes: [
        { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Flash · Max" },
        { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-pro", variant: "max", enabled: true, default: false, display: "DeepSeek V4 Pro · Max" },
      ],
    });
    adapter = new LiveProManagerAdapter(new OpenCodeManager(config));
    service = new BridgeService(config, { manager: adapter });
    await service.start();
    const accepted = await service.spawn({
      requestId: "live_pro_" + Date.now(),
      topic: "Live Pro route smoke",
      task: "Reply with exactly LIVE_PRO_OK and nothing else.",
      cwd: process.cwd(),
      mode: "analyze",
      modelRoute: "pro-max",
    });
    assert.equal(accepted.modelDisplayName, "DeepSeek V4 Pro · Max");
    assert.equal(accepted.outcome, undefined, "the single dispatch must settle normally");
    // Exactly one job/agent: one spawn, no continuation, one real inference.
    assert.equal(service.listJobs().length, 1);
    assert.equal(service.listAgents().length, 1);
    const agent = service.getAgent(accepted.agentId);
    assert.ok(agent);
    assert.equal(agent.modelRoute, "pro-max");

    // Bounded follow: the minimum enforced window is the config default
    // (1 min wait + 1 min grace); explicit 1/1 cannot shrink it further.
    // Completion events settle the follow; only a deadline path would attempt
    // a graceful-finalization prompt, which the guard blocks.
    const result = await service.follow({ agentId: accepted.agentId, jobId: accepted.jobId, waitMinutes: 1, graceMinutes: 1 });
    assert.ok(["completed", "completed_partial"].includes(result.status), "follow must reach a usable terminal result, got " + result.status);
    assert.equal(result.resultAvailable, true);
    assert.equal(result.progress.jobId, accepted.jobId);

    // The exactly-one guarantee: exactly one delegation happened and the
    // bridge never attempted a second provider prompt (deadline-triggered
    // graceful finalization included).
    const guard = adapter.guardClient;
    assert.ok(guard, "the guard client must be attached");
    assert.equal(guard.promptDelegations, 1, "exactly one promptAsync delegation is allowed");
    assert.equal(guard.blockedPromptAttempts, 0, "the bridge must not attempt a second provider prompt (graceful finalize or retry)");

    // Actual OpenCode assistant message info, persisted at completion.
    const persisted = JSON.parse(await readFile(path.join(directory, "results", accepted.jobId + ".json"), "utf8")) as {
      envelope?: { model?: unknown; modelDisplayName?: unknown };
      messages?: Array<{ info?: Record<string, unknown> }>;
    };
    assert.equal(persisted.envelope?.model, "opencode-go/deepseek-v4-pro · max");
    const assistant = persisted.messages?.find((message) => message.info?.role === "assistant");
    assert.ok(assistant, "persisted session must contain an actual assistant message");
    assert.equal(assistant.info?.providerID, "opencode-go");
    assert.equal(assistant.info?.modelID, "deepseek-v4-pro");
    assert.equal(assistant.info?.variant, "max");

    // Prove the persistent default config/data were untouched: the temp test
    // directory was the only data location used and the live paths' metadata
    // (existence/size/mtime, not contents) must be unchanged.
    const defaultConfigAfter = await pathSnapshot(defaultConfigPath());
    const defaultDbAfter = await pathSnapshot(path.join(defaultUserDataRoot(), "bridge.sqlite"));
    assert.deepEqual(defaultConfigAfter, defaultConfigBefore, "live config file must not be touched");
    assert.deepEqual(defaultDbAfter, defaultDbBefore, "live database must not be touched");
  } finally {
    // Robust teardown even when setup threw partway (for example OpenCode
    // failed to start): stop whatever was created, then delete every temp
    // resource (sqlite database + WAL/SHM, results, inbox). The teardown
    // itself never asserts, so it cannot mask the real failure.
    await service?.stop().catch(() => undefined);
    await adapter?.stop().catch(() => undefined);
    await rm(directory, { recursive: true, force: true });
  }
});
