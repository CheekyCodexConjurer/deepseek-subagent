import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { canRead, ensurePrivateDir, redactSecrets, truncate, writePrivateFileExclusive } from "../security.js";
import type { ResultEnvelope } from "../types.js";

export type UserNotifier = (title: string, message: string) => Promise<void>;

export class InboxDelivery {
  constructor(
    private readonly dataDir: string,
    private readonly notify: UserNotifier = noopNotifier,
  ) {}

  async deliver(envelope: ResultEnvelope, humanText: string): Promise<string> {
    const directory = path.join(this.dataDir, "inbox");
    await ensurePrivateDir(directory);
    const filePath = path.join(directory, envelope.jobId + ".json");
    const created = await writePrivateFileExclusive(filePath, JSON.stringify({
      envelope,
      humanText: truncate(redactSecrets(humanText), 100_000),
      createdAt: new Date().toISOString(),
      recovered: false,
    }, null, 2) + "\n");
    if (!created) return filePath;
    await this.notify("DeepSeek Sub-Agent", envelope.topic + ": " + envelope.summary);
    return filePath;
  }

  async read(filePath: string): Promise<unknown> {
    return JSON.parse(await readFile(filePath, "utf8"));
  }

  async writeNotice(notice: {
    kind: string;
    agentId: string;
    jobId: string;
    topic: string;
    message: string;
    permissionId?: string | null;
  }): Promise<string> {
    const directory = path.join(this.dataDir, "inbox");
    await ensurePrivateDir(directory);
    const filePath = noticeFilePath(this.dataDir, notice.jobId, notice.kind, notice.permissionId);
    const payload = {
      ...notice,
      ...(notice.permissionId ? { permissionId: notice.permissionId } : {}),
      topic: truncate(redactSecrets(notice.topic), 240),
      message: truncate(redactSecrets(notice.message), 2_000),
      createdAt: new Date().toISOString(),
    };
    const created = await writePrivateFileExclusive(filePath, JSON.stringify(payload, null, 2) + "\n");
    if (!created) return filePath;
    await this.notify("DeepSeek Sub-Agent", payload.topic + ": " + payload.message);
    return filePath;
  }

  async noticeExists(jobId: string, kind: string, permissionId?: string | null): Promise<boolean> {
    return canRead(noticeFilePath(this.dataDir, jobId, kind, permissionId));
  }
}

function noticeFilePath(dataDir: string, jobId: string, kind: string, permissionId?: string | null): string {
  const permissionSuffix = permissionId
    ? "-" + createHash("sha256").update(permissionId, "utf8").digest("hex").slice(0, 16)
    : "";
  return path.join(dataDir, "inbox", jobId + "-" + kind + permissionSuffix + ".json");
}

async function noopNotifier(): Promise<void> {
  return undefined;
}
