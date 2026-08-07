import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createDefaultConfig } from "../../src/config.js";
import { OpenCodeManager } from "../../src/opencode/manager.js";

test("managed OpenCode is restarted on the same loopback URL after an unexpected exit", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-manager-"));
  const marker = path.join(directory, "first-launch.marker");
  const executableScript = path.join(directory, "serve");
  await writeFile(executableScript, [
    "const http = require('node:http');",
    "const fs = require('node:fs');",
    `const marker = ${JSON.stringify(marker)};`,
    "const port = Number(process.argv[process.argv.indexOf('--port') + 1]);",
    "const server = http.createServer((req, res) => {",
    "  if (req.url === '/global/health') { res.writeHead(200, {'content-type': 'application/json'}); res.end(JSON.stringify({healthy: true, version: 'fixture'})); return; }",
    "  res.writeHead(404); res.end();",
    "});",
    "server.listen(port, '127.0.0.1');",
    "if (!fs.existsSync(marker)) { fs.writeFileSync(marker, '1'); setTimeout(() => process.exit(17), 350); }",
  ].join("\n"), "utf8");

  const manager = new OpenCodeManager(createDefaultConfig({
    dataDir: directory,
    configPath: path.join(directory, "config.json"),
    opencodeBinary: process.execPath,
    opencodeStartupTimeoutMs: 5_000,
    opencodeEventReconnectMaxMs: 500,
  }));
  try {
    const managed = await manager.start(directory);
    const firstPid = managed.processId;
    assert.ok(firstPid);
    const deadline = Date.now() + 8_000;
    while (Date.now() < deadline && (managed.processId === null || managed.processId === firstPid)) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    assert.notEqual(managed.processId, null);
    assert.notEqual(managed.processId, firstPid);
    assert.match(managed.baseUrl, /^http:\/\/127\.0\.0\.1:\d+$/);
  } finally {
    await manager.stop();
    await rm(directory, { recursive: true, force: true });
  }
});
