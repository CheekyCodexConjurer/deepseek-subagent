import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig, loadConfig } from "../../src/config.js";

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
