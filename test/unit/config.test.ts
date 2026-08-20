import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig, DEFAULT_MODEL_ROUTE_NAME, loadConfig, MODEL_ROUTE_REGISTRY } from "../../src/config.js";

test("same-chat delivery is disabled and follow defaults are persisted in the config contract", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-config-"));
  const configPath = path.join(directory, "config.json");
  try {
    const defaults = createDefaultConfig({ dataDir: directory, configPath });
    assert.equal(defaults.experimentalSameChatDelivery, false);
    assert.equal(defaults.followDefaultWaitMinutes, 20);
    assert.equal(defaults.followDefaultGraceMinutes, 5);
    await writeFile(configPath, JSON.stringify({
      ...defaults,
      experimentalSameChatDelivery: true,
      followDefaultWaitMinutes: 12,
      followDefaultGraceMinutes: 4,
    }), "utf8");
    const loaded = await loadConfig(configPath);
    assert.equal(loaded.experimentalSameChatDelivery, true);
    assert.equal(loaded.followDefaultWaitMinutes, 12);
    assert.equal(loaded.followDefaultGraceMinutes, 4);
    assert.match(await readFile(configPath, "utf8"), /experimentalSameChatDelivery/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("invalid follow defaults fail closed to bounded defaults", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-config-invalid-"));
  const configPath = path.join(directory, "config.json");
  try {
    const defaults = createDefaultConfig({ dataDir: directory, configPath });
    const constructed = createDefaultConfig({
      dataDir: directory,
      configPath,
      followDefaultWaitMinutes: 0,
      followDefaultGraceMinutes: 11,
    });
    assert.equal(constructed.followDefaultWaitMinutes, 20);
    assert.equal(constructed.followDefaultGraceMinutes, 5);
    await writeFile(configPath, JSON.stringify({
      ...defaults,
      followDefaultWaitMinutes: 61,
      followDefaultGraceMinutes: 0,
    }), "utf8");
    const loaded = await loadConfig(configPath);
    assert.equal(loaded.followDefaultWaitMinutes, 20);
    assert.equal(loaded.followDefaultGraceMinutes, 5);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("route registry ships flash-max enabled by default and pro-max registered but disabled", () => {
  const config = createDefaultConfig({
    dataDir: "C:\\deepseek-config-routes",
    configPath: "C:\\deepseek-config-routes\\config.json",
  });
  const flash = config.modelRoutes.find((route) => route.name === "flash-max");
  const pro = config.modelRoutes.find((route) => route.name === "pro-max");
  assert.ok(flash);
  assert.equal(flash.enabled, true);
  assert.equal(flash.default, true);
  assert.equal(flash.providerId, "opencode-go");
  assert.equal(flash.modelId, "deepseek-v4-flash");
  assert.equal(flash.variant, "max");
  assert.ok(pro);
  assert.equal(pro.enabled, false);
  assert.equal(pro.default, false);
  assert.equal(pro.providerId, "opencode-go");
  assert.equal(pro.modelId, "deepseek-v4-pro");
  assert.equal(config.defaultModelRoute, DEFAULT_MODEL_ROUTE_NAME);
  assert.equal(MODEL_ROUTE_REGISTRY.some((route) => route.name === "flash-max" && route.enabled), true);
  assert.equal(MODEL_ROUTE_REGISTRY.some((route) => route.name === "pro-max" && !route.enabled), true);
});

test("antigravity route is registered, enabled for selection, and never becomes the default", () => {
  const config = createDefaultConfig({
    dataDir: "C:\\deepseek-config-antigravity",
    configPath: "C:\\deepseek-config-antigravity\\config.json",
  });
  const route = config.modelRoutes.find((candidate) => candidate.name === "antigravity-flash-high");
  assert.ok(route);
  assert.equal(route.providerId, "antigravity");
  assert.equal(route.modelId, "gemini-3.7-flash-high");
  assert.equal(route.variant, null);
  assert.equal(route.enabled, true, "selectable by the operator control plane");
  assert.equal(route.default, false, "never the default");
  assert.equal(config.defaultModelRoute, DEFAULT_MODEL_ROUTE_NAME, "flash-max stays the initial effective default");
  assert.equal(MODEL_ROUTE_REGISTRY.find((candidate) => candidate.name === "antigravity-flash-high")?.enabled, true);
});

test("antigravity permission auto-approval is opt-in, independent of the sandbox, and path-limited", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-config-antigravity-permissions-"));
  const configPath = path.join(directory, "config.json");
  try {
    const defaults = createDefaultConfig({ dataDir: directory, configPath });
    assert.equal(defaults.antigravitySandbox, false);
    assert.deepEqual(defaults.antigravityAddDirs, []);
    assert.equal(defaults.antigravityAutoApprovePermissions, false);
    assert.equal(
      createDefaultConfig({ dataDir: directory, configPath, antigravitySandbox: true, antigravityAutoApprovePermissions: false })
        .antigravityAutoApprovePermissions,
      false,
      "sandbox alone never implies auto-approval",
    );
    assert.equal(
      createDefaultConfig({ dataDir: directory, configPath, antigravitySandbox: false, antigravityAutoApprovePermissions: true })
        .antigravityAutoApprovePermissions,
      true,
      "auto-approval is preserved without the sandbox",
    );
    assert.equal(
      createDefaultConfig({ dataDir: directory, configPath, antigravitySandbox: false, antigravityAutoApprovePermissions: true })
        .antigravitySandbox,
      false,
      "auto-approval never implies the sandbox",
    );
    await writeFile(configPath, JSON.stringify({
      ...defaults,
      antigravitySandbox: true,
      antigravityAddDirs: [directory, "relative-path", directory],
      antigravityAutoApprovePermissions: true,
    }), "utf8");
    const enabled = await loadConfig(configPath);
    assert.equal(enabled.antigravitySandbox, true);
    assert.deepEqual(enabled.antigravityAddDirs, [directory]);
    assert.equal(enabled.antigravityAutoApprovePermissions, true);
    await writeFile(configPath, JSON.stringify({
      ...defaults,
      antigravitySandbox: false,
      antigravityAutoApprovePermissions: true,
    }), "utf8");
    const unsandboxed = await loadConfig(configPath);
    assert.equal(unsandboxed.antigravitySandbox, false);
    assert.equal(unsandboxed.antigravityAutoApprovePermissions, true, "unsandboxed runs keep explicit auto-approval");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("old flat config stays backward compatible and keeps the default route", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-config-flat-"));
  const configPath = path.join(directory, "config.json");
  try {
    await writeFile(configPath, JSON.stringify({
      dataDir: directory,
      configPath,
      opencodeProviderId: "opencode-go",
      opencodeModelId: "deepseek-v4-flash",
      opencodeVariant: "max",
    }), "utf8");
    const loaded = await loadConfig(configPath);
    assert.equal(loaded.defaultModelRoute, "flash-max");
    assert.equal(loaded.opencodeModelId, "deepseek-v4-flash");
    assert.equal(loaded.modelRoutes.find((route) => route.name === "flash-max")?.enabled, true);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("old flat config naming a non-default model promotes that route to default and enabled", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-config-flat-pro-"));
  const configPath = path.join(directory, "config.json");
  try {
    await writeFile(configPath, JSON.stringify({
      dataDir: directory,
      configPath,
      opencodeProviderId: "opencode-go",
      opencodeModelId: "deepseek-v4-pro",
      opencodeVariant: "max",
    }), "utf8");
    const loaded = await loadConfig(configPath);
    assert.equal(loaded.defaultModelRoute, "pro-max");
    assert.equal(loaded.modelRoutes.find((route) => route.name === "pro-max")?.enabled, true);
    assert.equal(loaded.modelRoutes.find((route) => route.name === "pro-max")?.default, true);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("explicit modelRoutes registry wins over flat defaults and is persisted", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-config-registry-"));
  const configPath = path.join(directory, "config.json");
  try {
    const config = createDefaultConfig({
      dataDir: directory,
      configPath,
      modelRoutes: [
        { name: "pro-max", providerId: "opencode-go", modelId: "deepseek-v4-pro", variant: "max", enabled: true, default: true, display: "DeepSeek V4 Pro · Max" },
        { name: "flash-max", providerId: "opencode-go", modelId: "deepseek-v4-flash", variant: "max", enabled: true, default: false, display: "DeepSeek V4 Flash · Max" },
      ],
      defaultModelRoute: "pro-max",
    });
    await writeFile(configPath, JSON.stringify(config), "utf8");
    const loaded = await loadConfig(configPath);
    assert.equal(loaded.defaultModelRoute, "pro-max");
    assert.equal(loaded.modelRoutes.find((route) => route.name === "pro-max")?.enabled, true);
    assert.equal(loaded.modelRoutes.length, 2);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("antigravityCommand defaults to null, validates proportionally, and survives a load round trip", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-config-antigravity-command-"));
  const configPath = path.join(directory, "config.json");
  try {
    const defaults = createDefaultConfig({ dataDir: directory, configPath });
    assert.equal(defaults.antigravityCommand, null, "omitted command keeps the default PATH lookup");
    const explicit = createDefaultConfig({
      dataDir: directory,
      configPath,
      antigravityCommand: "C:\\Users\\lab\\antigravity\\staging\\agy.exe",
    });
    assert.equal(explicit.antigravityCommand, "C:\\Users\\lab\\antigravity\\staging\\agy.exe", "an absolute Windows path is preserved");
    assert.equal(
      createDefaultConfig({ dataDir: directory, configPath, antigravityCommand: "   " }).antigravityCommand,
      null,
      "whitespace-only is treated as unset",
    );
    assert.equal(
      createDefaultConfig({ dataDir: directory, configPath, antigravityCommand: "agy-custom" }).antigravityCommand,
      "agy-custom",
      "a bare command name is preserved for PATH lookup",
    );
    if (process.platform === "win32") {
      assert.equal(
        createDefaultConfig({
          dataDir: directory,
          configPath,
          antigravityCommand: "C:\\Program Files\\Antigravity Lab\\agy.exe",
        }).antigravityCommand,
        "C:\\Program Files\\Antigravity Lab\\agy.exe",
        "an absolute executable path containing spaces is preserved",
      );
      assert.equal(
        createDefaultConfig({ dataDir: directory, configPath, antigravityCommand: "C:\\agy.exe --sandbox" }).antigravityCommand,
        "C:\\agy.exe --sandbox",
        "an absolute-prefixed value is one argv (a Windows filename may contain spaces); only non-absolute command lines are rejected",
      );
    }
    assert.equal(
      createDefaultConfig({ dataDir: directory, configPath, antigravityCommand: "agy --sandbox" }).antigravityCommand,
      null,
      "a shell-like command line with arguments is rejected",
    );
    assert.equal(
      createDefaultConfig({ dataDir: directory, configPath, antigravityCommand: "lab tools\\agy.exe" }).antigravityCommand,
      null,
      "a relative path with spaces is not a bare name nor an absolute path and is rejected",
    );
    await writeFile(configPath, JSON.stringify({
      ...defaults,
      antigravityCommand: "C:\\Users\\lab\\antigravity\\staging\\agy.exe",
    }), "utf8");
    const loaded = await loadConfig(configPath);
    assert.equal(loaded.antigravityCommand, "C:\\Users\\lab\\antigravity\\staging\\agy.exe");
    await writeFile(configPath, JSON.stringify({ ...defaults, antigravityCommand: 42 }), "utf8");
    const nonString = await loadConfig(configPath);
    assert.equal(nonString.antigravityCommand, null, "non-string values fail closed to unset");
    await writeFile(configPath, JSON.stringify({ ...defaults, antigravityCommand: "" }), "utf8");
    const empty = await loadConfig(configPath);
    assert.equal(empty.antigravityCommand, null, "empty values fail closed to unset");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("retention and context file bounds default to safe values and survive a load round trip", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-config-retention-"));
  const configPath = path.join(directory, "config.json");
  try {
    const defaults = createDefaultConfig({ dataDir: directory, configPath });
    assert.equal(defaults.retentionMode, "disabled");
    assert.equal(defaults.maxContextFileBytes, 1_000_000);
    await writeFile(configPath, JSON.stringify({
      ...defaults,
      retentionMode: "dry-run",
      maxContextFileBytes: 512_000,
    }), "utf8");
    const loaded = await loadConfig(configPath);
    assert.equal(loaded.retentionMode, "dry-run");
    assert.equal(loaded.maxContextFileBytes, 512_000);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("antigravityTimeoutFallbackRoute defaults to null and survives a load round trip", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-config-fallback-"));
  const configPath = path.join(directory, "config.json");
  try {
    const defaults = createDefaultConfig({ dataDir: directory, configPath });
    assert.equal(defaults.antigravityTimeoutFallbackRoute, null);
    await writeFile(configPath, JSON.stringify({
      ...defaults,
      antigravityTimeoutFallbackRoute: "flash-max",
    }), "utf8");
    const loaded = await loadConfig(configPath);
    assert.equal(loaded.antigravityTimeoutFallbackRoute, "flash-max");
    await writeFile(configPath, JSON.stringify({
      ...defaults,
      antigravityTimeoutFallbackRoute: "",
    }), "utf8");
    const emptyLoaded = await loadConfig(configPath);
    assert.equal(emptyLoaded.antigravityTimeoutFallbackRoute, null);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
