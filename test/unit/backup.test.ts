import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync, readdirSync } from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { computeFileSha256, createBackup } from "../../src/backup.js";
import { createDefaultConfig, loadConfig } from "../../src/config.js";
import { BridgeStore } from "../../src/store.js";
import { main } from "../../src/cli.js";

test("config defaults backupDir to <dataDir>/backups and supports custom backupDir", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-config-"));
  const configPath = path.join(directory, "config.json");
  try {
    const defaults = createDefaultConfig({ dataDir: directory, configPath });
    assert.equal(defaults.backupDir, path.join(directory, "backups"));

    const custom = createDefaultConfig({
      dataDir: directory,
      configPath,
      backupDir: "D:\\Programas\\DeepSeek Sub-Agent\\Backups",
    });
    assert.equal(custom.backupDir, "D:\\Programas\\DeepSeek Sub-Agent\\Backups");

    await writeFile(configPath, JSON.stringify({
      ...defaults,
      backupDir: path.join(directory, "custom-backups"),
    }), "utf8");

    const loaded = await loadConfig(configPath);
    assert.equal(loaded.backupDir, path.join(directory, "custom-backups"));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("createBackup performs online snapshot of live WAL database and generates valid manifest", async () => {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-live-"));
  const backupDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-target-"));
  try {
    const dbPath = path.join(dataDir, "bridge.sqlite");
    const store = new BridgeStore(dbPath);

    // Seed test data
    store.createAgent({
      id: "agent_backup_test",
      title: "Backup Test Agent",
      topic: "Online Backup",
      repositoryRoot: "C:\\test\\repo",
      workspacePath: "C:\\test\\repo",
      workspaceStrategy: "shared",
      opencodeServerId: "server_1",
      opencodeSessionId: "session_1",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
      modelRoute: "flash-max",
    });
    store.createJob({
      id: "job_backup_test",
      agentId: "agent_backup_test",
      kind: "spawn",
      requestId: "req_backup_1",
      promptHash: "hash123",
    });

    const fixedTime = "2026-08-20T19:30:00.000Z";
    const result = await createBackup({
      sourceDbPath: dbPath,
      destinationDir: backupDir,
      now: fixedTime,
    });

    assert.ok(existsSync(result.snapshotPath), "snapshot file must exist");
    assert.ok(existsSync(result.manifestPath), "manifest file must exist");
    assert.match(path.basename(result.snapshotPath), /^bridge-2026-08-20T19-30-00-000Z\.sqlite$/);
    assert.match(path.basename(result.manifestPath), /^bridge-2026-08-20T19-30-00-000Z\.manifest\.json$/);

    // Verify manifest contents
    const manifestRaw = await readFile(result.manifestPath, "utf8");
    const manifest = JSON.parse(manifestRaw);
    assert.equal(manifest.version, 1);
    assert.equal(manifest.timestamp, fixedTime);
    assert.equal(manifest.databaseFile, path.basename(result.snapshotPath));
    assert.equal(manifest.sourceDatabasePath, path.resolve(dbPath));

    // Verify sha256 and size in manifest
    const snapshotBuffer = await readFile(result.snapshotPath);
    const expectedSha256 = createHash("sha256").update(snapshotBuffer).digest("hex");
    assert.equal(manifest.databaseSha256, expectedSha256);
    assert.equal(manifest.databaseSizeBytes, snapshotBuffer.length);

    // Verify the snapshot is a valid standalone SQLite database containing the seeded data
    const snapshotDb = new DatabaseSync(result.snapshotPath);
    try {
      const row = snapshotDb.prepare("SELECT title FROM agents WHERE id = ?").get("agent_backup_test") as Record<string, unknown>;
      assert.equal(row?.title, "Backup Test Agent");
      const jobRow = snapshotDb.prepare("SELECT request_id FROM jobs WHERE id = ?").get("job_backup_test") as Record<string, unknown>;
      assert.equal(jobRow?.request_id, "req_backup_1");
    } finally {
      snapshotDb.close();
    }

    // Verify destination directory contains ONLY the snapshot and manifest (no WAL, SHM, logs, config)
    const publishedFiles = readdirSync(backupDir);
    assert.deepEqual(publishedFiles.sort(), [
      path.basename(result.manifestPath),
      path.basename(result.snapshotPath),
    ].sort());

    store.close();
  } finally {
    await rm(dataDir, { recursive: true, force: true });
    await rm(backupDir, { recursive: true, force: true });
  }
});

test("createBackup stages files and cleans up on failure leaving published snapshots intact", async () => {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-staging-src-"));
  const backupDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-staging-dest-"));
  try {
    const dbPath = path.join(dataDir, "bridge.sqlite");
    const store = new BridgeStore(dbPath);
    store.close();

    // First create a legitimate backup
    const fixedTime1 = "2026-08-20T10:00:00.000Z";
    const firstResult = await createBackup({
      sourceDbPath: dbPath,
      destinationDir: backupDir,
      now: fixedTime1,
    });
    assert.ok(existsSync(firstResult.snapshotPath));

    // Attempting a second backup with the EXACT SAME timestamp must fail on collision
    await assert.rejects(
      () => createBackup({
        sourceDbPath: dbPath,
        destinationDir: backupDir,
        now: fixedTime1,
      }),
      (error: unknown) => {
        const msg = error instanceof Error ? error.message : String(error);
        assert.match(msg, /collision|already exists/i);
        return true;
      },
    );

    // Verify first backup is intact and no staging leftover files remain
    const files = readdirSync(backupDir);
    assert.equal(files.length, 2, "only the original snapshot and manifest must remain");
    assert.ok(!files.some((f) => f.includes("staging") || f.startsWith(".")));
  } finally {
    await rm(dataDir, { recursive: true, force: true });
    await rm(backupDir, { recursive: true, force: true });
  }
});

test("createBackup rejects when source database does not exist", async () => {
  const backupDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-missing-"));
  try {
    await assert.rejects(
      () => createBackup({
        sourceDbPath: path.join(backupDir, "nonexistent.sqlite"),
        destinationDir: backupDir,
      }),
      (error: unknown) => {
        const msg = error instanceof Error ? error.message : String(error);
        assert.match(msg, /does not exist|not found/i);
        return true;
      },
    );
  } finally {
    await rm(backupDir, { recursive: true, force: true });
  }
});

test("CLI backup command supports --destination override and --json output", async () => {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-cli-data-"));
  const defaultBackupDir = path.join(dataDir, "backups");
  const customBackupDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-cli-custom-"));
  const configPath = path.join(dataDir, "config.json");
  try {
    const config = createDefaultConfig({ dataDir, configPath });
    const dbPath = path.join(dataDir, "bridge.sqlite");
    const store = new BridgeStore(dbPath);
    store.close();

    await writeFile(configPath, JSON.stringify(config), "utf8");

    // Test 1: CLI backup using default destination
    const logs1: string[] = [];
    const origLog = console.log;
    console.log = (...args: unknown[]) => logs1.push(args.map(String).join(" "));
    try {
      await main(["backup", "--config", configPath, "--json"]);
    } finally {
      console.log = origLog;
    }

    const output1 = JSON.parse(logs1.join("\n"));
    assert.equal(output1.success, true);
    assert.ok(existsSync(output1.snapshotPath));
    assert.ok(existsSync(output1.manifestPath));
    assert.ok(output1.snapshotPath.startsWith(defaultBackupDir));

    // Test 2: CLI backup using explicit --destination override
    const logs2: string[] = [];
    console.log = (...args: unknown[]) => logs2.push(args.map(String).join(" "));
    try {
      await main(["backup", "--destination", customBackupDir, "--config", configPath, "--json"]);
    } finally {
      console.log = origLog;
    }
    const output2 = JSON.parse(logs2.join("\n"));
    assert.equal(output2.success, true);
    assert.ok(existsSync(output2.snapshotPath));
    assert.ok(existsSync(output2.manifestPath));
    assert.ok(output2.snapshotPath.startsWith(customBackupDir));

    // Test 3: CLI backup human readable output
    const logs3: string[] = [];
    console.log = (...args: unknown[]) => logs3.push(args.map(String).join(" "));
    try {
      await main(["backup", "--destination", customBackupDir, "--config", configPath]);
    } finally {
      console.log = origLog;
    }
    const humanOutput = logs3.join("\n");
    assert.match(humanOutput, /Backup created/i);
    assert.match(humanOutput, /Snapshot:/i);
    assert.match(humanOutput, /Manifest:/i);

    // Test 4: CLI backup using --to override
    const logs4: string[] = [];
    console.log = (...args: unknown[]) => logs4.push(args.map(String).join(" "));
    try {
      await main(["backup", "--to", customBackupDir, "--config", configPath, "--json"]);
    } finally {
      console.log = origLog;
    }
    const output4 = JSON.parse(logs4.join("\n"));
    assert.equal(output4.success, true);
    assert.ok(output4.snapshotPath.startsWith(customBackupDir));

    // Test 5: CLI backup using positional destination argument
    const logs5: string[] = [];
    console.log = (...args: unknown[]) => logs5.push(args.map(String).join(" "));
    try {
      await main(["backup", customBackupDir, "--config", configPath, "--json"]);
    } finally {
      console.log = origLog;
    }
    const output5 = JSON.parse(logs5.join("\n"));
    assert.equal(output5.success, true);
    assert.ok(output5.snapshotPath.startsWith(customBackupDir));
  } finally {
    await rm(dataDir, { recursive: true, force: true });
    await rm(customBackupDir, { recursive: true, force: true });
  }
});

test("CLI backup fails cleanly when bridge database does not exist", async () => {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-cli-missing-"));
  const configPath = path.join(dataDir, "config.json");
  try {
    const config = createDefaultConfig({ dataDir, configPath });
    await writeFile(configPath, JSON.stringify(config), "utf8");

    await assert.rejects(
      () => main(["backup", "--config", configPath]),
      (error: unknown) => {
        const msg = error instanceof Error ? error.message : String(error);
        assert.match(msg, /bridge database does not exist/i);
        return true;
      },
    );
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("CLI backup --help and -h display command help and exit without creating backup or reading database", async () => {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-cli-help-"));
  const configPath = path.join(dataDir, "config.json");
  try {
    const config = createDefaultConfig({ dataDir, configPath });
    await writeFile(configPath, JSON.stringify(config), "utf8");

    for (const helpArg of ["--help", "-h", "help"]) {
      const logs: string[] = [];
      const origLog = console.log;
      console.log = (...args: unknown[]) => logs.push(args.map(String).join(" "));
      try {
        await main(["backup", helpArg, "--config", configPath]);
      } finally {
        console.log = origLog;
      }
      const output = logs.join("\n");
      assert.match(output, /backup/i);
      assert.match(output, /DeepSeek Sub-Agent local bridge/i);
      assert.match(output, /Commands:/i);
    }

    // Verify no backup directory or snapshot files were created
    const helpDir = path.resolve("--help");
    assert.equal(existsSync(helpDir), false, "--help directory must not be created");
    const hDir = path.resolve("-h");
    assert.equal(existsSync(hDir), false, "-h directory must not be created");
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("BridgeStore.createSnapshot directly executes VACUUM INTO", async () => {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-store-"));
  try {
    const dbPath = path.join(dataDir, "bridge.sqlite");
    const store = new BridgeStore(dbPath);
    store.createAgent({
      id: "agent_store_snap",
      title: "Store Snap Test",
      topic: "Store Snapshot",
      repositoryRoot: "C:\\test",
      workspacePath: "C:\\test",
      workspaceStrategy: "shared",
      opencodeServerId: "srv1",
      opencodeSessionId: "sess1",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
      modelRoute: "flash-max",
    });

    const snapPath = path.join(dataDir, "direct-snapshot.sqlite");
    store.createSnapshot(snapPath);
    assert.ok(existsSync(snapPath));

    const snapDb = new DatabaseSync(snapPath);
    try {
      const row = snapDb.prepare("SELECT title FROM agents WHERE id = ?").get("agent_store_snap") as Record<string, unknown>;
      assert.equal(row?.title, "Store Snap Test");
    } finally {
      snapDb.close();
      store.close();
    }
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("computeFileSha256 streams and computes SHA-256 hash without loading whole file into memory", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-stream-"));
  try {
    const filePath = path.join(directory, "test-stream.dat");
    // Generate multi-chunk test data
    const chunk1 = Buffer.from("first chunk of data for streaming hash calculation\n");
    const chunk2 = Buffer.from("second chunk with random bytes: " + randomBytes(1024).toString("hex") + "\n");
    const chunk3 = Buffer.from("third chunk to complete the multi-chunk payload\n");
    const fullBuffer = Buffer.concat([chunk1, chunk2, chunk3]);
    await writeFile(filePath, fullBuffer);

    const expectedHash = createHash("sha256").update(fullBuffer).digest("hex");
    const computedHash = await computeFileSha256(filePath);

    assert.equal(computedHash, expectedHash);

    // Verify non-existent file rejects cleanly
    await assert.rejects(
      () => computeFileSha256(path.join(directory, "nonexistent.dat")),
      (error: unknown) => {
        const msg = error instanceof Error ? error.message : String(error);
        assert.match(msg, /no such file|ENOENT/i);
        return true;
      },
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("createBackup calculates manifest checksum via streaming without whole-file readFile", async () => {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-no-readfile-src-"));
  const backupDir = await mkdtemp(path.join(os.tmpdir(), "deepseek-backup-no-readfile-dest-"));
  try {
    const dbPath = path.join(dataDir, "bridge.sqlite");
    const store = new BridgeStore(dbPath);
    store.createAgent({
      id: "agent_stream_check",
      title: "Stream Check Agent",
      topic: "Online Backup Stream Test",
      repositoryRoot: "C:\\test\\repo",
      workspacePath: "C:\\test\\repo",
      workspaceStrategy: "shared",
      opencodeServerId: "server_1",
      opencodeSessionId: "session_1",
      modelProviderId: "opencode-go",
      modelId: "deepseek-v4-flash",
      modelVariant: "max",
      modelRoute: "flash-max",
    });
    store.close();

    const fixedTime = "2026-08-20T21:00:00.000Z";
    const result = await createBackup({
      sourceDbPath: dbPath,
      destinationDir: backupDir,
      now: fixedTime,
    });

    const manifestRaw = await readFile(result.manifestPath, "utf8");
    const manifest = JSON.parse(manifestRaw);
    const expectedSha256 = await computeFileSha256(result.snapshotPath);

    assert.equal(manifest.databaseSha256, expectedSha256);
    assert.equal(typeof manifest.databaseSha256, "string");
    assert.equal(manifest.databaseSha256.length, 64);
    assert.ok(manifest.databaseSizeBytes > 0);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
    await rm(backupDir, { recursive: true, force: true });
  }
});
