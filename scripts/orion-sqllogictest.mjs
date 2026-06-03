#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { basename } from "node:path";

const DEFAULT_URL = "http://127.0.0.1:8091/sqllogictest";

const args = parseArgs(process.argv.slice(2));
const url = args.url ?? process.env.ORION_SQLLOGICTEST_URL ?? DEFAULT_URL;
const files = args.files.length > 0 ? args.files : ["testdata/sqllogictest/orion-core.slt"];

const results = [];
let failed = false;
let baton;

for (const file of files) {
  const cases = parseSqlLogicTest(await readFile(file, "utf8"), file);
  const summary = { file, cases: cases.length, passed: 0, failed: 0 };
  for (const testCase of cases) {
    try {
      await runCase(url, testCase);
      summary.passed += 1;
    } catch (error) {
      summary.failed += 1;
      failed = true;
      console.error(formatCaseError(testCase, error));
      if (args.stopOnFailure) {
        results.push(summary);
        printSummary(results);
        process.exit(1);
      }
    }
  }
  results.push(summary);
}

printSummary(results);
if (failed) {
  process.exit(1);
}

function parseArgs(argv) {
  const parsed = { files: [], stopOnFailure: false };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--url") {
      parsed.url = requireValue(argv, (index += 1), arg);
    } else if (arg.startsWith("--url=")) {
      parsed.url = arg.slice("--url=".length);
    } else if (arg === "--stop-on-failure") {
      parsed.stopOnFailure = true;
    } else if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    } else if (arg.startsWith("-")) {
      throw new Error(`unknown option ${arg}`);
    } else {
      parsed.files.push(arg);
    }
  }
  return parsed;
}

function requireValue(argv, index, option) {
  const value = argv[index];
  if (!value) {
    throw new Error(`${option} requires a value`);
  }
  return value;
}

function printUsage() {
  console.log([
    "Usage: node scripts/orion-sqllogictest.mjs [--url URL] [--stop-on-failure] [FILE ...]",
    "",
    "Runs a sqllogictest subset against Orion's libSQL HTTP pipeline endpoint.",
    "Supported records: statement ok, statement error, query <types> <nosort|rowsort|valuesort>.",
  ].join("\n"));
}

function parseSqlLogicTest(input, file) {
  const lines = input.replace(/\r\n/g, "\n").split("\n");
  const cases = [];
  let index = 0;

  while (index < lines.length) {
    index = skipWhitespaceAndComments(lines, index);
    if (index >= lines.length) {
      break;
    }

    const headerLine = lines[index];
    const lineNumber = index + 1;
    index += 1;
    const [kind, ...headerParts] = headerLine.trim().split(/\s+/);

    if (kind === "statement") {
      const expected = headerParts[0];
      if (!["ok", "error"].includes(expected)) {
        throw new Error(`${file}:${lineNumber}: unsupported statement expectation ${expected}`);
      }
      const block = readSqlBlock(lines, index);
      index = block.nextIndex;
      cases.push({
        kind,
        expected,
        sql: block.sql,
        file,
        lineNumber,
        name: `${basename(file)}:${lineNumber}`,
      });
      continue;
    }

    if (kind === "query") {
      const types = headerParts[0];
      const sortMode = headerParts[1] ?? "nosort";
      if (!types || !/^[TIR]+$/.test(types)) {
        throw new Error(`${file}:${lineNumber}: unsupported query type string ${types}`);
      }
      if (!["nosort", "rowsort", "valuesort"].includes(sortMode)) {
        throw new Error(`${file}:${lineNumber}: unsupported query sort mode ${sortMode}`);
      }
      const block = readQueryBlock(lines, index);
      index = block.nextIndex;
      cases.push({
        kind,
        types,
        sortMode,
        sql: block.sql,
        expectedValues: normalizeExpectedValues(block.expectedValues, sortMode, types.length),
        file,
        lineNumber,
        name: `${basename(file)}:${lineNumber}`,
      });
      continue;
    }

    throw new Error(`${file}:${lineNumber}: unsupported sqllogictest directive ${kind}`);
  }

  return cases;
}

function skipWhitespaceAndComments(lines, index) {
  while (index < lines.length) {
    const trimmed = lines[index].trim();
    if (trimmed === "" || trimmed.startsWith("#")) {
      index += 1;
      continue;
    }
    break;
  }
  return index;
}

function readSqlBlock(lines, index) {
  const sqlLines = [];
  while (index < lines.length) {
    const line = lines[index];
    const trimmed = line.trim();
    if (trimmed === "" || trimmed.startsWith("#")) {
      break;
    }
    sqlLines.push(line);
    index += 1;
  }
  return {
    sql: sqlLines.join("\n").trim(),
    nextIndex: index,
  };
}

function readQueryBlock(lines, index) {
  const sqlLines = [];
  while (index < lines.length && lines[index].trim() !== "----") {
    sqlLines.push(lines[index]);
    index += 1;
  }
  if (index >= lines.length) {
    throw new Error("query is missing ---- result separator");
  }
  index += 1;

  const expectedValues = [];
  while (index < lines.length) {
    const line = lines[index];
    const trimmed = line.trim();
    if (trimmed === "" || trimmed.startsWith("#")) {
      break;
    }
    expectedValues.push(line);
    index += 1;
  }
  return {
    sql: sqlLines.join("\n").trim(),
    expectedValues,
    nextIndex: index,
  };
}

async function runCase(baseUrl, testCase) {
  const wantRows = testCase.kind === "query";
  const body = await execute(baseUrl, testCase.sql, wantRows);
  const result = body.results?.[0];
  if (!result) {
    throw new Error(`missing result in response ${JSON.stringify(body)}`);
  }

  if (testCase.kind === "statement") {
    if (testCase.expected === "ok" && result.type !== "ok") {
      throw new Error(`expected statement ok, got ${JSON.stringify(result.error)}`);
    }
    if (testCase.expected === "error" && result.type !== "error") {
      throw new Error("expected statement error, got ok");
    }
    return;
  }

  if (result.type !== "ok") {
    throw new Error(`expected query ok, got ${JSON.stringify(result.error)}`);
  }
  const actualValues = normalizeActualValues(
    result.response?.result?.rows ?? [],
    testCase.sortMode,
    testCase.types.length,
  );
  if (JSON.stringify(actualValues) !== JSON.stringify(testCase.expectedValues)) {
    throw new Error([
      "query result mismatch",
      `expected: ${JSON.stringify(testCase.expectedValues)}`,
      `actual:   ${JSON.stringify(actualValues)}`,
    ].join("\n"));
  }
}

async function execute(baseUrl, sql, wantRows) {
  const response = await fetch(`${baseUrl.replace(/\/$/, "")}/v2/pipeline`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      baton,
      requests: [
        {
          type: "execute",
          stmt: { sql, want_rows: wantRows },
        },
      ],
    }),
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${await response.text()}`);
  }
  const body = await response.json();
  baton = body.baton ?? baton;
  return body;
}

function normalizeActualValues(rows, sortMode, columnCount) {
  const values = [];
  for (const row of rows) {
    for (const value of row) {
      values.push(formatHranaValue(value));
    }
  }
  return normalizeValues(values, sortMode, columnCount);
}

function normalizeExpectedValues(values, sortMode, columnCount) {
  return normalizeValues(values, sortMode, columnCount);
}

function normalizeValues(values, sortMode, columnCount) {
  if (sortMode === "valuesort") {
    return [...values].sort();
  }
  if (sortMode === "rowsort") {
    const rows = [];
    for (let index = 0; index < values.length; index += columnCount) {
      rows.push(values.slice(index, index + columnCount));
    }
    rows.sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
    return rows.flat();
  }
  return values;
}

function formatHranaValue(value) {
  switch (value?.type) {
    case "null":
      return "NULL";
    case "integer":
      return value.value;
    case "float":
      return Number(value.value).toFixed(3);
    case "text":
      return value.value;
    case "blob":
      return value.base64;
    default:
      throw new Error(`unsupported Hrana value ${JSON.stringify(value)}`);
  }
}

function formatCaseError(testCase, error) {
  return [
    `${testCase.file}:${testCase.lineNumber}: ${testCase.kind} failed`,
    testCase.sql,
    String(error?.stack ?? error),
  ].join("\n");
}

function printSummary(summaries) {
  const total = summaries.reduce(
    (acc, summary) => ({
      cases: acc.cases + summary.cases,
      passed: acc.passed + summary.passed,
      failed: acc.failed + summary.failed,
    }),
    { cases: 0, passed: 0, failed: 0 },
  );
  console.log(JSON.stringify({ ok: total.failed === 0, total, files: summaries }, null, 2));
}
