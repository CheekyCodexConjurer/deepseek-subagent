import { randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { defaultUserDataRoot, ensurePrivateDir, isLoopbackHost, writePrivateFile } from "./security.js";
import type { BridgeConfig } from "./types.js";

export function defaultConfigPath(): string {
  return path.join(defaultUserDataRoot(), "config.json");
}

function asString(value: unknown, fallback: string): string {
  return typeof value === "string" && value.length > 0 ? value : fallback;
}

function asNullableString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function asNumber(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

export function createDefaultConfig(overrides: Partial<BridgeConfig> = {}): BridgeConfig {
  const dataDir = overrides.dataDir ?? defaultUserDataRoot();
  const configPath = overrides.configPath ?? path.join(dataDir, "config.json");
  return {
    dataDir,
    configPath,
    daemonHost: overrides.daemonHost ?? "127.0.0.1",
    daemonPort: overrides.daemonPort ?? 42653,
    daemonToken: overrides.daemonToken ?? randomBytes(32).toString("hex"),
    opencodeMode: overrides.opencodeMode ?? "managed",
    opencodeUrl: overrides.opencodeUrl ?? null,
    opencodeUsername: overrides.opencodeUsername ?? "opencode",
    opencodePassword: overrides.opencodePassword ?? null,
    opencodeBinary: overrides.opencodeBinary ?? null,
    opencodeProviderId: overrides.opencodeProviderId ?? "opencode-go",
    opencodeModelId: overrides.opencodeModelId ?? "deepseek-v4-flash",
    opencodeVariant: overrides.opencodeVariant ?? "max",
    opencodeAgent: overrides.opencodeAgent ?? "build",
    opencodeStartupTimeoutMs: overrides.opencodeStartupTimeoutMs ?? 30_000,
    opencodeEventReconnectMaxMs: overrides.opencodeEventReconnectMaxMs ?? 30_000,
    approvalTimeoutMs: overrides.approvalTimeoutMs ?? 300_000,
    codexAppServerSocket: overrides.codexAppServerSocket ?? null,
    codexAppServerCommand: overrides.codexAppServerCommand ?? null,
    codexAppServerArgs: overrides.codexAppServerArgs ?? [],
    maxTaskLength: overrides.maxTaskLength ?? 120_000,
    maxResultLength: overrides.maxResultLength ?? 2_000_000,
  };
}

export async function loadConfig(configPath = defaultConfigPath()): Promise<BridgeConfig> {
  const defaults = createDefaultConfig({ configPath, dataDir: path.dirname(configPath) });
  try {
    const parsed: unknown = JSON.parse(await readFile(configPath, "utf8"));
    if (!parsed || typeof parsed !== "object") return defaults;
    const raw = parsed as Record<string, unknown>;
    return {
      ...defaults,
      dataDir: asString(raw.dataDir, defaults.dataDir),
      configPath,
      daemonHost: isLoopbackHost(asString(raw.daemonHost, defaults.daemonHost))
        ? asString(raw.daemonHost, defaults.daemonHost)
        : defaults.daemonHost,
      daemonPort: asNumber(raw.daemonPort, defaults.daemonPort),
      daemonToken: asString(raw.daemonToken, defaults.daemonToken),
      opencodeMode: raw.opencodeMode === "attach" ? "attach" : "managed",
      opencodeUrl: asNullableString(raw.opencodeUrl),
      opencodeUsername: asString(raw.opencodeUsername, defaults.opencodeUsername),
      opencodePassword: asNullableString(raw.opencodePassword),
      opencodeBinary: asNullableString(raw.opencodeBinary),
      opencodeProviderId: asString(raw.opencodeProviderId, defaults.opencodeProviderId),
      opencodeModelId: asString(raw.opencodeModelId, defaults.opencodeModelId),
      opencodeVariant: asNullableString(raw.opencodeVariant),
      opencodeAgent: asString(raw.opencodeAgent, defaults.opencodeAgent),
      opencodeStartupTimeoutMs: asNumber(raw.opencodeStartupTimeoutMs, defaults.opencodeStartupTimeoutMs),
      opencodeEventReconnectMaxMs: asNumber(raw.opencodeEventReconnectMaxMs, defaults.opencodeEventReconnectMaxMs),
      approvalTimeoutMs: asNumber(raw.approvalTimeoutMs, defaults.approvalTimeoutMs),
      codexAppServerSocket: asNullableString(raw.codexAppServerSocket),
      codexAppServerCommand: asNullableString(raw.codexAppServerCommand),
      codexAppServerArgs: Array.isArray(raw.codexAppServerArgs)
        ? raw.codexAppServerArgs.filter((item): item is string => typeof item === "string")
        : defaults.codexAppServerArgs,
      maxTaskLength: asNumber(raw.maxTaskLength, defaults.maxTaskLength),
      maxResultLength: asNumber(raw.maxResultLength, defaults.maxResultLength),
    };
  } catch {
    return defaults;
  }
}

export async function saveConfig(config: BridgeConfig): Promise<void> {
  await ensurePrivateDir(config.dataDir);
  await writePrivateFile(config.configPath, JSON.stringify(config, null, 2) + "\n");
}
