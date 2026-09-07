import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  createDefaultConfig,
  DEFAULT_MODEL_ROUTE_NAME,
  DEFAULT_PROVIDER_ID,
  isLegacyRoute,
  isLegacyRouteName,
  loadConfig,
  MODEL_ROUTE_REGISTRY,
} from "../../src/config.js";

test("createDefaultConfig() without overrides selects antigravity-flash-high as defaultModelRoute", () => {
  const config = createDefaultConfig();
  assert.equal(config.defaultModelRoute, "antigravity-flash-high");
  assert.equal(DEFAULT_MODEL_ROUTE_NAME, "antigravity-flash-high");
  assert.equal(DEFAULT_PROVIDER_ID, "antigravity");

  const defaultRoute = config.modelRoutes.find((route) => route.name === "antigravity-flash-high");
  assert.ok(defaultRoute, "antigravity-flash-high must exist in modelRoutes");
  assert.equal(defaultRoute.providerId, "antigravity");
  assert.equal(defaultRoute.modelId, "gemini-3.8-flash-high");
  assert.equal(defaultRoute.variant, null);
  assert.equal(defaultRoute.enabled, true);
  assert.equal(defaultRoute.default, true);
  assert.equal(defaultRoute.display, "Antigravity · Gemini 3.8 Flash High");
});

test("built-in registry keeps legacy routes disabled and non-default", () => {
  const flash = MODEL_ROUTE_REGISTRY.find((route) => route.name === "flash-max");
  const pro = MODEL_ROUTE_REGISTRY.find((route) => route.name === "pro-max");
  const antigravity = MODEL_ROUTE_REGISTRY.find((route) => route.name === "antigravity-flash-high");

  assert.ok(flash, "flash-max should be present as historical record");
  assert.equal(flash.enabled, false, "flash-max must be disabled");
  assert.equal(flash.default, false, "flash-max must not be default");

  assert.ok(pro, "pro-max should be present as historical record");
  assert.equal(pro.enabled, false, "pro-max must be disabled");
  assert.equal(pro.default, false, "pro-max must not be default");

  assert.ok(antigravity);
  assert.equal(antigravity.enabled, true);
  assert.equal(antigravity.default, true);

  assert.equal(isLegacyRouteName("flash-max"), true);
  assert.equal(isLegacyRouteName("pro-max"), true);
  assert.equal(isLegacyRouteName("antigravity-flash-high"), false);

  assert.equal(isLegacyRoute(flash), true);
  assert.equal(isLegacyRoute(pro), true);
  assert.equal(isLegacyRoute(antigravity), false);
});

test("createDefaultConfig rejects legacy defaultModelRoute overrides", () => {
  const configWithFlash = createDefaultConfig({ defaultModelRoute: "flash-max" });
  assert.equal(configWithFlash.defaultModelRoute, "antigravity-flash-high");
  assert.equal(configWithFlash.modelRoutes.find((r) => r.name === "flash-max")?.default, false);

  const configWithPro = createDefaultConfig({ defaultModelRoute: "pro-max" });
  assert.equal(configWithPro.defaultModelRoute, "antigravity-flash-high");
  assert.equal(configWithPro.modelRoutes.find((r) => r.name === "pro-max")?.default, false);
});

test("antigravityTimeoutFallbackRoute defaults to null without breaking compatibility", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "antigravity-fallback-test-"));
  const configPath = path.join(directory, "config.json");
  try {
    const defaults = createDefaultConfig({ dataDir: directory, configPath });
    assert.equal(defaults.antigravityTimeoutFallbackRoute, null, "fallback route must default to null");

    await writeFile(configPath, JSON.stringify({
      ...defaults,
      antigravityTimeoutFallbackRoute: null,
    }), "utf8");
    const loaded = await loadConfig(configPath);
    assert.equal(loaded.antigravityTimeoutFallbackRoute, null);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("loadConfig rejects legacy defaultModelRoute and disabled routes in persisted config", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "antigravity-load-test-"));
  const configPath = path.join(directory, "config.json");
  try {
    const defaults = createDefaultConfig({ dataDir: directory, configPath });
    await writeFile(configPath, JSON.stringify({
      ...defaults,
      defaultModelRoute: "flash-max",
      modelRoutes: [
        {
          name: "flash-max",
          providerId: "opencode-go",
          modelId: "deepseek-v4-flash",
          variant: "max",
          enabled: true,
          default: true,
          display: "DeepSeek V4 Flash · Max",
        },
        {
          name: "antigravity-flash-high",
          providerId: "antigravity",
          modelId: "gemini-3.8-flash-high",
          variant: null,
          enabled: true,
          default: false,
          display: "Antigravity · Gemini 3.8 Flash High",
        },
      ],
    }), "utf8");

    const loaded = await loadConfig(configPath);
    assert.equal(loaded.defaultModelRoute, "antigravity-flash-high", "legacy route must be rejected as default");
    const flash = loaded.modelRoutes.find((r) => r.name === "flash-max");
    assert.ok(flash);
    assert.equal(flash.enabled, false, "legacy route must remain disabled");
    assert.equal(flash.default, false, "legacy route must not be marked default");

    const ag = loaded.modelRoutes.find((r) => r.name === "antigravity-flash-high");
    assert.ok(ag);
    assert.equal(ag.enabled, true);
    assert.equal(ag.default, true);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
