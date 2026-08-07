import { loadConfig } from "../src/config.js";
import { CodexAppServerDeliveryAdapter } from "../src/codex/adapter.js";

const config = await loadConfig();
if (!config.codexAppServerCommand && !config.codexAppServerSocket) {
  console.log(JSON.stringify({
    status: "not_run",
    reason: "No explicit Codex App Server connection is configured.",
    completeDeliverySupported: false,
  }, null, 2));
  process.exit(0);
}

const adapter = new CodexAppServerDeliveryAdapter(config);
try {
  await adapter.start();
  console.log(JSON.stringify({
    status: "initialized",
    note: "initialize succeeded; an end-to-end MCP item/completed correlation still requires invoking the registered tool from the same Codex App Server client.",
    completeDeliverySupported: false,
  }, null, 2));
} catch (error) {
  console.log(JSON.stringify({
    status: "failed",
    error: String(error),
    completeDeliverySupported: false,
  }, null, 2));
  process.exitCode = 1;
} finally {
  await adapter.close().catch(() => undefined);
}
