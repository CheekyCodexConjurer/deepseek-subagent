import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const testDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(testDir, "../..");
const srcDir = path.resolve(repoRoot, "src");

export interface ContaminationViolation {
  file: string;
  line: number;
  category:
    | "test_path_literal"
    | "fixture_request_id"
    | "global_test_variable"
    | "stack_trace_coupling";
  rule: string;
  snippet: string;
}

function collectSourceFiles(dir: string): string[] {
  const files: string[] = [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectSourceFiles(full));
    } else if (entry.isFile() && entry.name.endsWith(".ts")) {
      files.push(full);
    }
  }
  return files;
}

export function scanSourceFile(filePath: string, relativePath: string, content: string): ContaminationViolation[] {
  const lines = content.split("\n");
  const violations: ContaminationViolation[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const lineNum = i + 1;
    const trimmed = line.trim();

    // Skip comment lines
    if (trimmed.startsWith("//") || trimmed.startsWith("/*") || trimmed.startsWith("*")) {
      continue;
    }

    // 1. Explicit *.test.ts / test path literals in production code
    // Matches string or regex literals referencing test files (*.test.ts/js) or test directory trees (test/unit, etc.)
    // Avoids legitimate RegExp.prototype.test() calls or enum values like "test"
    const testPathMatch = line.match(
      /(?:[a-zA-Z0-9_\-.\/\\]+)?(?:\\\.|\.)test(?:\\\.|\.)(?:ts|js|mjs|cjs)|\btest[\/\\](?:unit|integration|e2e|fixtures)\b/,
    );
    if (testPathMatch) {
      violations.push({
        file: relativePath,
        line: lineNum,
        category: "test_path_literal",
        rule: "Explicit *.test.ts/test path literal in production code",
        snippet: trimmed,
      });
    }

    // 2. Fixture-only request-ID literals used in behavior branches / queries
    // Hardcoded fixture request IDs (e.g. req_appr_1, req_agent1_continue, req_blockerA_excl)
    // in comparison expressions (===, ==) or SQL clauses (WHERE request_id ...)
    const fixtureIdMatch = line.match(/['"]req_[a-zA-Z0-9_-]+['"]/);
    if (fixtureIdMatch) {
      violations.push({
        file: relativePath,
        line: lineNum,
        category: "fixture_request_id",
        rule: "Hardcoded fixture request-ID literal used in production logic or query",
        snippet: trimmed,
      });
    }

    // 3. Exported or globalThis test variables such as spawn1
    // Matches globalThis.spawn1, (globalThis as any).spawn1, globalThis as any escape hatches, or exported test helpers
    const globalTestVarMatch = line.match(
      /(?:\(\s*globalThis\s+as\s+any\s*\)|globalThis\s*(?:as\s+any)?\s*\.\s*(?:spawn\w*|test\w*|mock\w*|fixture\w*)|export\s+(?:let|var|const)\s+(?:spawn\w*|test\w*|mock\w*|fixture\w*)\b)/,
    );
    if (globalTestVarMatch) {
      violations.push({
        file: relativePath,
        line: lineNum,
        category: "global_test_variable",
        rule: "Exported or globalThis test variable in production code",
        snippet: trimmed,
      });
    }
  }

  // 4. Stack-trace inspection paired with state mutation / test matching
  // Narrow pattern: detects synthesizing new Error().stack or Error().stack for caller sniffing
  // paired with matching test file names or mutating state/database.
  // Preserves legitimate error diagnostics (e.g. caught error logging err.stack in catch blocks).
  if (/new\s+Error\(\)\.stack|\bError\(\)\.stack/.test(content)) {
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const lineNum = i + 1;
      if (/new\s+Error\(\)\.stack|\bError\(\)\.stack/.test(line)) {
        const contextStart = Math.max(0, i - 5);
        const contextEnd = Math.min(lines.length, i + 25);
        const contextWindow = lines.slice(contextStart, contextEnd).join("\n");
        const hasTestMatching = /\.test\(stack\)|stack\.includes|stack\.indexOf|\.test\./.test(contextWindow);
        const hasStateMutation = /UPDATE\s+|INSERT\s+|DELETE\s+|\.run\(|\.exec\(|this\.db/.test(contextWindow);

        if (hasTestMatching || hasStateMutation) {
          violations.push({
            file: relativePath,
            line: lineNum,
            category: "stack_trace_coupling",
            rule: "Caller stack-trace inspection paired with test matching and/or state mutation",
            snippet: line.trim(),
          });
        }
      }
    }
  }

  return violations;
}

export function scanAllProductionFiles(): ContaminationViolation[] {
  const tsFiles = collectSourceFiles(srcDir);
  const allViolations: ContaminationViolation[] = [];
  for (const file of tsFiles) {
    const relPath = path.relative(repoRoot, file).replace(/\\/g, "/");
    const content = fs.readFileSync(file, "utf8");
    const violations = scanSourceFile(file, relPath, content);
    allViolations.push(...violations);
  }
  return allViolations;
}

function formatViolations(violations: ContaminationViolation[]): string {
  return [
    `Production contamination detected (${violations.length} violation${violations.length === 1 ? "" : "s"}):`,
    ...violations.map(
      (v) => `  - ${v.file}:${v.line} [${v.category}] ${v.rule}\n      Snippet: ${v.snippet}`,
    ),
  ].join("\n");
}

test("production src/**/*.ts must reject explicit *.test.ts and test path literals", () => {
  const violations = scanAllProductionFiles().filter((v) => v.category === "test_path_literal");
  if (violations.length > 0) {
    assert.fail(formatViolations(violations));
  }
});

test("production src/**/*.ts must reject fixture-only request-ID literals in branches or queries", () => {
  const violations = scanAllProductionFiles().filter((v) => v.category === "fixture_request_id");
  if (violations.length > 0) {
    assert.fail(formatViolations(violations));
  }
});

test("production src/**/*.ts must reject exported or globalThis test variables", () => {
  const violations = scanAllProductionFiles().filter((v) => v.category === "global_test_variable");
  if (violations.length > 0) {
    assert.fail(formatViolations(violations));
  }
});

test("production src/**/*.ts must reject caller stack-trace inspection paired with test matching or state mutation", () => {
  const violations = scanAllProductionFiles().filter((v) => v.category === "stack_trace_coupling");
  if (violations.length > 0) {
    assert.fail(formatViolations(violations));
  }
});

test("overall production contamination guard: src/**/*.ts must be free of unit-test coupling", () => {
  const violations = scanAllProductionFiles();
  if (violations.length > 0) {
    assert.fail(formatViolations(violations));
  }
});
