import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { readCodexMcpToolTimeout } from "../../src/cli.js";

test("doctor timeout parser accepts hyphen and underscore Codex MCP section names", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "deepseek-cli-timeout-"));
  try {
    for (const section of ["deepseek-subagent", "deepseek_subagent", '"deepseek-subagent"', '"deepseek_subagent"']) {
      const configPath = path.join(directory, section.replace(/[^a-z0-9]/gi, "_") + ".toml");
      await writeFile(configPath, `[mcp_servers.${section}]\ntool_timeout_sec = 4500\n[mcp_servers.other]\ntool_timeout_sec = 30\n`, "utf8");
      assert.equal(await readCodexMcpToolTimeout(configPath), 4500);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
