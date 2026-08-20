import { createHash, randomBytes } from "node:crypto";
import { existsSync } from "node:fs";
import { readFile, rename, stat, unlink } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { canRead, ensurePrivateDir, writePrivateFile } from "./security.js";
import type { BackupManifest, BackupOptions, BackupResult } from "./types.js";

export async function createBackup(options: BackupOptions): Promise<BackupResult> {
  const sourceDbPath = path.resolve(options.sourceDbPath);
  if (!(await canRead(sourceDbPath))) {
    throw new Error("Bridge database file does not exist or is not readable: " + sourceDbPath);
  }

  const destinationDir = path.resolve(options.destinationDir);
  await ensurePrivateDir(destinationDir);

  const now = options.now ? new Date(options.now) : new Date();
  const timestamp = now.toISOString();
  const fileSafeTimestamp = timestamp.replace(/[:.]/g, "-");
  const snapshotFilename = `bridge-${fileSafeTimestamp}.sqlite`;
  const manifestFilename = `bridge-${fileSafeTimestamp}.manifest.json`;

  const finalSnapshotPath = path.join(destinationDir, snapshotFilename);
  const finalManifestPath = path.join(destinationDir, manifestFilename);

  if (existsSync(finalSnapshotPath) || existsSync(finalManifestPath)) {
    throw new Error(
      "Backup collision: destination file already exists: " +
        (existsSync(finalSnapshotPath) ? finalSnapshotPath : finalManifestPath),
    );
  }

  // Stage directly in destinationDir so staging and final destination share the same volume/filesystem.
  const stagingId = randomBytes(8).toString("hex");
  const stagedSnapshotPath = path.join(destinationDir, `.${snapshotFilename}.staging-${stagingId}`);
  const stagedManifestPath = path.join(destinationDir, `.${manifestFilename}.staging-${stagingId}`);

  try {
    // Perform online SQLite snapshot via VACUUM INTO on the live database.
    const db = new DatabaseSync(sourceDbPath);
    try {
      db.exec("PRAGMA busy_timeout = 5000;");
      db.exec(`VACUUM INTO '${stagedSnapshotPath.replace(/'/g, "''")}';`);
    } finally {
      db.close();
    }

    const fileBuffer = await readFile(stagedSnapshotPath);
    const databaseSha256 = createHash("sha256").update(fileBuffer).digest("hex");
    const fileStats = await stat(stagedSnapshotPath);

    const manifest: BackupManifest = {
      version: 1,
      timestamp,
      databaseFile: snapshotFilename,
      databaseSizeBytes: fileStats.size,
      databaseSha256,
      sourceDatabasePath: sourceDbPath,
    };

    await writePrivateFile(stagedManifestPath, JSON.stringify(manifest, null, 2) + "\n");

    // Guard against collision before publishing
    if (existsSync(finalSnapshotPath) || existsSync(finalManifestPath)) {
      throw new Error(
        "Backup collision: destination file already exists: " +
          (existsSync(finalSnapshotPath) ? finalSnapshotPath : finalManifestPath),
      );
    }

    // Atomic publish on the destination volume
    await rename(stagedSnapshotPath, finalSnapshotPath);
    await rename(stagedManifestPath, finalManifestPath);

    return {
      snapshotPath: finalSnapshotPath,
      manifestPath: finalManifestPath,
      manifest,
    };
  } catch (error) {
    // Clean up staging files on failure leaving published snapshots intact
    await unlink(stagedSnapshotPath).catch(() => undefined);
    await unlink(stagedManifestPath).catch(() => undefined);
    throw error;
  }
}
