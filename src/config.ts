import { randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { DEFAULT_MAX_CONTEXT_FILE_BYTES, defaultGlobalGeminiContextPath, defaultUserDataRoot, ensurePrivateDir, isLoopbackHost, writePrivateFile } from "./security.js";
import type { BridgeConfig, ModelRoute, RetentionMode } from "./types.js";

export const FOLLOW_MAX_WAIT_MINUTES = 60;
export const FOLLOW_MAX_GRACE_MINUTES = 10;
export const FOLLOW_MAX_TOTAL_MINUTES = FOLLOW_MAX_WAIT_MINUTES + FOLLOW_MAX_GRACE_MINUTES;
export const DEFAULT_CODEX_MCP_TOOL_TIMEOUT_SEC = 4_500;

export const DEFAULT_MODEL_ROUTE_NAME = "flash-max";
export const DEFAULT_PROVIDER_ID = "opencode-go";

/**
 * Built-in route registry. flash-max is enabled and default; pro-max is
 * registered but disabled until explicitly enabled; antigravity-flash-high is
 * registered and enabled for operator selection but never the default.
 * Dispatches resolve strictly through this registry: an unknown or disabled
 * route fails closed with a stable typed 400 and there is no silent fallback
 * route. New spawns follow the operator-controlled active route (persisted in
 * the bridge store; effectively the configured default until an operator sets
 * it).
 */
export const MODEL_ROUTE_REGISTRY: readonly ModelRoute[] = [
  {
    name: "flash-max",
    providerId: DEFAULT_PROVIDER_ID,
    modelId: "deepseek-v4-flash",
    variant: "max",
    enabled: true,
    default: true,
    display: "DeepSeek V4 Flash · Max",
  },
  {
    name: "pro-max",
    providerId: DEFAULT_PROVIDER_ID,
    modelId: "deepseek-v4-pro",
    variant: "max",
    enabled: false,
    default: false,
    display: "DeepSeek V4 Pro · Max",
  },
  {
    name: "antigravity-flash-high",
    providerId: "antigravity",
    modelId: "gemini-3.7-flash-high",
    variant: null,
    enabled: true,
    default: false,
    display: "Antigravity · Gemini 3.7 Flash High",
  },
];

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

function asAbsoluteStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.filter((item): item is string => typeof item === "string" && item.length > 0 && path.isAbsolute(item)))];
}

/**
 * Optional antigravity executable override. The contract is ONE executable:
 * a bare command name resolved through PATH, or an absolute path to the
 * executable (which may contain spaces, for example a Windows install path).
 * Shell-like command lines with arguments are rejected at load time: any
 * value containing whitespace that is not an absolute path is "unset".
 * Empty/whitespace/non-string values also mean "unset" and fall back to the
 * default PATH lookup. The command is spawned argv-style with shell:false.
 */
function asNullableCommand(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  if (/\s/.test(trimmed) && !path.isAbsolute(trimmed)) return null;
  return trimmed;
}

function boundedInteger(value: unknown, fallback: number, minimum: number, maximum: number): number {
  return typeof value === "number" && Number.isInteger(value) && value >= minimum && value <= maximum
    ? value
    : fallback;
}

function asRetentionMode(value: unknown, fallback: RetentionMode): RetentionMode {
  return value === "auto" || value === "disabled" || value === "dry-run" || value === "enabled"
    ? value
    : fallback;
}

function parseRouteRegistry(value: unknown): ModelRoute[] | null {
  if (!Array.isArray(value) || value.length === 0) return null;
  const routes: ModelRoute[] = [];
  for (const item of value) {
    if (!item || typeof item !== "object") return null;
    const raw = item as Record<string, unknown>;
    if (typeof raw.name !== "string" || raw.name.length === 0) return null;
    if (typeof raw.providerId !== "string" || raw.providerId.length === 0) return null;
    if (typeof raw.modelId !== "string" || raw.modelId.length === 0) return null;
    const variant = asNullableString(raw.variant);
    const enabled = raw.enabled !== false;
    const defaultRoute = raw.default === true;
    const display = typeof raw.display === "string" && raw.display.length > 0
      ? raw.display
      : raw.providerId + "/" + raw.modelId + (variant ? " · " + variant : "");
    routes.push({ name: raw.name, providerId: raw.providerId, modelId: raw.modelId, variant, enabled, default: defaultRoute, display });
  }
  if (routes.some((route) => route.default)) return routes;
  const first = routes[0];
  if (!first) return null;
  return routes.map((route) => route.name === first.name ? { ...route, default: true } : route);
}

function registryWithFlatDefault(routes: ModelRoute[], providerId: string, modelId: string, variant: string | null): ModelRoute[] {
  if (routes.some((route) => route.default)) return routes.map((route) => ({ ...route }));
  const flatMatches = routes.find((route) =>
    route.providerId === providerId && route.modelId === modelId && (route.variant ?? null) === variant);
  const first = routes[0];
  const winner = flatMatches ?? first;
  if (!winner) return [];
  return routes.map((route) => route.name === winner.name ? { ...route, default: true } : route);
}

/**
 * Backward compatibility for old flat configs (opencodeProviderId /
 * opencodeModelId / opencodeVariant): when no modelRoutes registry is present
 * in the config file, the default route is derived from the flat fields. A
 * flat configuration that explicitly names a non-default model (for example
 * deepseek-v4-pro) promotes that route to default and enabled.
 */
function defaultModelRoutes(flat: { providerId: string; modelId: string; variant: string | null }): ModelRoute[] {
  const matches = MODEL_ROUTE_REGISTRY.filter((route) =>
    route.providerId === flat.providerId && route.modelId === flat.modelId && (route.variant ?? null) === flat.variant);
  if (matches.length === 0) return MODEL_ROUTE_REGISTRY.map((route) => ({ ...route }));
  return MODEL_ROUTE_REGISTRY.map((route) => {
    if (route.name === matches[0]?.name) return { ...route, default: true, enabled: true };
    return route.default ? { ...route, default: false } : { ...route };
  });
}

function defaultRouteName(routes: ModelRoute[]): string {
  const defaultRoute = routes.find((route) => route.default);
  return defaultRoute?.name ?? DEFAULT_MODEL_ROUTE_NAME;
}

export function isValidFollowDefaults(config: Pick<BridgeConfig, "followDefaultWaitMinutes" | "followDefaultGraceMinutes">): boolean {
  return boundedInteger(config.followDefaultWaitMinutes, Number.NaN, 1, FOLLOW_MAX_WAIT_MINUTES) === config.followDefaultWaitMinutes &&
    boundedInteger(config.followDefaultGraceMinutes, Number.NaN, 1, FOLLOW_MAX_GRACE_MINUTES) === config.followDefaultGraceMinutes;
}

export function createDefaultConfig(overrides: Partial<BridgeConfig> = {}): BridgeConfig {
  const dataDir = overrides.dataDir ?? defaultUserDataRoot();
  const configPath = overrides.configPath ?? path.join(dataDir, "config.json");
  const modelRoutes = overrides.modelRoutes ?? defaultModelRoutes({
    providerId: overrides.opencodeProviderId ?? DEFAULT_PROVIDER_ID,
    modelId: overrides.opencodeModelId ?? "deepseek-v4-flash",
    variant: overrides.opencodeVariant ?? "max",
  });
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
    opencodeProviderId: overrides.opencodeProviderId ?? DEFAULT_PROVIDER_ID,
    opencodeModelId: overrides.opencodeModelId ?? "deepseek-v4-flash",
    opencodeVariant: overrides.opencodeVariant ?? "max",
    opencodeAgent: overrides.opencodeAgent ?? "build",
    opencodeStartupTimeoutMs: overrides.opencodeStartupTimeoutMs ?? 30_000,
    opencodeEventReconnectMaxMs: overrides.opencodeEventReconnectMaxMs ?? 30_000,
    approvalTimeoutMs: overrides.approvalTimeoutMs ?? 300_000,
    codexCorrelationWindowMs: overrides.codexCorrelationWindowMs ?? 10_000,
    experimentalSameChatDelivery: overrides.experimentalSameChatDelivery ?? false,
    followDefaultWaitMinutes: boundedInteger(overrides.followDefaultWaitMinutes, 20, 1, FOLLOW_MAX_WAIT_MINUTES),
    followDefaultGraceMinutes: boundedInteger(overrides.followDefaultGraceMinutes, 5, 1, FOLLOW_MAX_GRACE_MINUTES),
    codexAppServerSocket: overrides.codexAppServerSocket ?? null,
    codexAppServerCommand: overrides.codexAppServerCommand ?? null,
    codexAppServerArgs: overrides.codexAppServerArgs ?? [],
    maxTaskLength: overrides.maxTaskLength ?? 120_000,
    maxResultLength: overrides.maxResultLength ?? 2_000_000,
    antigravitySandbox: overrides.antigravitySandbox === true,
    antigravityAddDirs: asAbsoluteStringArray(overrides.antigravityAddDirs),
    antigravityAutoApprovePermissions: overrides.antigravityAutoApprovePermissions === true,
    antigravityCommand: asNullableCommand(overrides.antigravityCommand),
    modelRoutes,
    defaultModelRoute: overrides.defaultModelRoute ?? defaultRouteName(modelRoutes),
    retentionMode: overrides.retentionMode ?? "disabled",
    maxContextFileBytes: boundedInteger(overrides.maxContextFileBytes, DEFAULT_MAX_CONTEXT_FILE_BYTES, 1_024, 64_000_000),
    globalGeminiContextPath: overrides.globalGeminiContextPath ?? defaultGlobalGeminiContextPath(),
  };
}

export async function loadConfig(configPath = defaultConfigPath()): Promise<BridgeConfig> {
  const defaults = createDefaultConfig({ configPath, dataDir: path.dirname(configPath) });
  try {
    const parsed: unknown = JSON.parse(await readFile(configPath, "utf8"));
    if (!parsed || typeof parsed !== "object") return defaults;
    const raw = parsed as Record<string, unknown>;
    const flatProviderId = asString(raw.opencodeProviderId, defaults.opencodeProviderId);
    const flatModelId = asString(raw.opencodeModelId, defaults.opencodeModelId);
    const flatVariant = asNullableString(raw.opencodeVariant);
    const parsedRoutes = parseRouteRegistry(raw.modelRoutes);
    const modelRoutes = parsedRoutes ?? defaultModelRoutes({ providerId: flatProviderId, modelId: flatModelId, variant: flatVariant });
    const configuredDefaultRoute = typeof raw.defaultModelRoute === "string"
      && modelRoutes.some((route) => route.name === raw.defaultModelRoute)
      ? raw.defaultModelRoute
      : defaultRouteName(modelRoutes);
    const retentionMode = asRetentionMode(raw.retentionMode, defaults.retentionMode);
    const antigravitySandbox = raw.antigravitySandbox === true;
    const antigravityAddDirs = asAbsoluteStringArray(raw.antigravityAddDirs);
    const antigravityAutoApprovePermissions = raw.antigravityAutoApprovePermissions === true;
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
      opencodeProviderId: flatProviderId,
      opencodeModelId: flatModelId,
      opencodeVariant: flatVariant,
      opencodeAgent: asString(raw.opencodeAgent, defaults.opencodeAgent),
      opencodeStartupTimeoutMs: asNumber(raw.opencodeStartupTimeoutMs, defaults.opencodeStartupTimeoutMs),
      opencodeEventReconnectMaxMs: asNumber(raw.opencodeEventReconnectMaxMs, defaults.opencodeEventReconnectMaxMs),
      approvalTimeoutMs: asNumber(raw.approvalTimeoutMs, defaults.approvalTimeoutMs),
      codexCorrelationWindowMs: asNumber(raw.codexCorrelationWindowMs, defaults.codexCorrelationWindowMs),
      experimentalSameChatDelivery: raw.experimentalSameChatDelivery === true,
      followDefaultWaitMinutes: boundedInteger(raw.followDefaultWaitMinutes, defaults.followDefaultWaitMinutes, 1, FOLLOW_MAX_WAIT_MINUTES),
      followDefaultGraceMinutes: boundedInteger(raw.followDefaultGraceMinutes, defaults.followDefaultGraceMinutes, 1, FOLLOW_MAX_GRACE_MINUTES),
      codexAppServerSocket: asNullableString(raw.codexAppServerSocket),
      codexAppServerCommand: asNullableString(raw.codexAppServerCommand),
      codexAppServerArgs: Array.isArray(raw.codexAppServerArgs)
        ? raw.codexAppServerArgs.filter((item): item is string => typeof item === "string")
        : defaults.codexAppServerArgs,
      maxTaskLength: asNumber(raw.maxTaskLength, defaults.maxTaskLength),
      maxResultLength: asNumber(raw.maxResultLength, defaults.maxResultLength),
      antigravitySandbox,
      antigravityAddDirs,
      antigravityAutoApprovePermissions,
      antigravityCommand: asNullableCommand(raw.antigravityCommand),
      modelRoutes,
      defaultModelRoute: configuredDefaultRoute,
      retentionMode,
      maxContextFileBytes: boundedInteger(raw.maxContextFileBytes, defaults.maxContextFileBytes, 1_024, 64_000_000),
      globalGeminiContextPath: asString(raw.globalGeminiContextPath, defaults.globalGeminiContextPath),
    };
  } catch {
    return defaults;
  }
}

export async function saveConfig(config: BridgeConfig): Promise<void> {
  await ensurePrivateDir(config.dataDir);
  await writePrivateFile(config.configPath, JSON.stringify(config, null, 2) + "\n");
}
