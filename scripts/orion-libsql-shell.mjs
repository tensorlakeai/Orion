#!/usr/bin/env node

import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";

const DEFAULT_URL = "http://127.0.0.1:8091/appdb";

const endpoint = pipelineEndpoint(process.argv[2] ?? process.env.LIBSQL_URL ?? DEFAULT_URL);
const authToken = process.env.LIBSQL_AUTH_TOKEN;
let baton = null;
let buffer = "";

const rl = createInterface({ input, output });

console.log(`Connected to ${endpoint.displayUrl}`);
console.log("Enter SQL terminated by ';'. Use .help for shell commands.");

try {
  while (true) {
    const line = await readLine(buffer ? "   ...> " : "orion> ");
    if (line === null) {
      break;
    }
    const trimmed = line.trim();

    if (!buffer && trimmed.startsWith(".")) {
      const keepGoing = await runDotCommand(trimmed);
      if (!keepGoing) {
        break;
      }
      continue;
    }

    buffer = buffer ? `${buffer}\n${line}` : line;
    if (!statementIsComplete(buffer)) {
      continue;
    }

    const sql = stripTrailingSemicolon(buffer.trim());
    buffer = "";
    if (!sql) {
      continue;
    }

    try {
      await runSql(sql);
    } catch (error) {
      console.error(`ERROR: ${error.message}`);
    }
  }
} finally {
  rl.close();
  await closeSession().catch(() => {});
}

async function readLine(prompt) {
  try {
    return await rl.question(prompt);
  } catch (error) {
    if (error?.code === "ERR_USE_AFTER_CLOSE") {
      return null;
    }
    throw error;
  }
}

async function runDotCommand(command) {
  if (command === ".exit" || command === ".quit" || command === ".q") {
    return false;
  }

  if (command === ".help") {
    console.log([
      "Shell commands:",
      "  .help              Show this help",
      "  .tables            List tables",
      "  .databases         Show the current database URL",
      "  .system            List Orion system tables",
      "  .metrics           Show live Raft metrics",
      "  .schema [table]    Show CREATE statements",
      "  .quit              Exit",
      "",
      "Environment:",
      "  LIBSQL_URL         Default database URL",
      "  LIBSQL_AUTH_TOKEN  Bearer token for authenticated endpoints",
    ].join("\n"));
    return true;
  }

  if (command === ".tables") {
    if (endpoint.database === "_orion") {
      await runSql("select name from sqlite_schema where type = 'table' and name not like 'sqlite_%' union all select 'raft_metrics' union all select 'storage_pressure' order by name");
    } else {
      await runSql("select name from sqlite_schema where type = 'table' and name not like 'sqlite_%' order by name");
    }
    return true;
  }

  if (command === ".databases") {
    console.log(`current: ${endpoint.displayUrl}`);
    console.log("system:  " + endpoint.systemUrl);
    return true;
  }

  if (command === ".system") {
    console.log("System namespace: _orion");
    console.log("Tables:");
    console.log("  compaction_runs");
    console.log("  compaction_state");
    console.log("  raft_metrics       (virtual, live)");
    console.log("  storage_pressure   (virtual, live)");
    return true;
  }

  if (command === ".metrics") {
    await runSql("select * from raft_metrics");
    return true;
  }

  if (command === ".schema" || command.startsWith(".schema ")) {
    const table = command.slice(".schema".length).trim();
    if (table) {
      await runSql(
        "select sql from sqlite_schema where sql is not null and name = ? order by type, name",
        [table],
      );
    } else {
      await runSql("select sql from sqlite_schema where sql is not null order by type, name");
    }
    return true;
  }

  console.error(`Unknown command: ${command}`);
  return true;
}

async function runSql(sql, args = []) {
  const response = await pipeline([
    {
      type: "execute",
      stmt: {
        sql,
        args: args.map(toHranaValue),
        want_rows: statementCanReturnRows(sql),
      },
    },
  ]);

  const result = response.results[0];
  if (result.type === "error") {
    throw new Error(result.error?.message ?? "unknown libSQL error");
  }

  const stmtResult = result.response.result;
  printStatementResult(stmtResult);
}

async function closeSession() {
  if (!baton) {
    return;
  }
  await pipeline([{ type: "close" }], { keepBaton: false });
}

async function pipeline(requests, options = {}) {
  const body = {
    requests,
    ...(baton ? { baton } : {}),
  };

  const response = await fetch(endpoint.pipelineUrl, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(authToken ? { authorization: `Bearer ${authToken}` } : {}),
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status} ${response.statusText}`);
  }

  const payload = await response.json();
  if (options.keepBaton !== false) {
    baton = payload.baton ?? baton;
  } else {
    baton = null;
  }
  return payload;
}

function printStatementResult(result) {
  const columns = result.cols?.map((col, index) => col.name || `column${index + 1}`) ?? [];
  const rows = result.rows?.map((row) => row.map(fromHranaValue)) ?? [];

  if (columns.length > 0) {
    printTable(columns, rows);
    console.log(`(${rows.length} ${rows.length === 1 ? "row" : "rows"})`);
    return;
  }

  const affected = result.affected_row_count ?? 0;
  console.log(`OK (${affected} ${affected === 1 ? "row" : "rows"} affected)`);
}

function printTable(columns, rows) {
  const renderedRows = rows.map((row) => row.map(renderCell));
  const widths = columns.map((column, index) => {
    const rowWidths = renderedRows.map((row) => row[index]?.length ?? 0);
    return Math.max(column.length, ...rowWidths);
  });

  console.log(formatRow(columns, widths));
  console.log(widths.map((width) => "-".repeat(width)).join("-+-"));
  for (const row of renderedRows) {
    console.log(formatRow(row, widths));
  }
}

function formatRow(row, widths) {
  return row
    .map((value, index) => {
      const text = value ?? "";
      return text.padEnd(widths[index], " ");
    })
    .join(" | ");
}

function renderCell(value) {
  if (value === null) {
    return "NULL";
  }
  if (value instanceof Uint8Array) {
    return `x'${Buffer.from(value).toString("hex")}'`;
  }
  return String(value);
}

function statementCanReturnRows(sql) {
  const keyword = sql.trimStart().match(/^[a-zA-Z]+/)?.[0]?.toLowerCase();
  return ["select", "with", "pragma", "explain", "values"].includes(keyword);
}

function statementIsComplete(sql) {
  let quote = null;
  let escaped = false;
  for (const char of sql) {
    if (escaped) {
      escaped = false;
      continue;
    }
    if (char === "\\") {
      escaped = true;
      continue;
    }
    if (quote) {
      if (char === quote) {
        quote = null;
      }
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      quote = char;
    }
  }
  return !quote && sql.trimEnd().endsWith(";");
}

function stripTrailingSemicolon(sql) {
  return sql.replace(/;\s*$/, "");
}

function toHranaValue(value) {
  if (value === null || value === undefined) {
    return { type: "null" };
  }
  if (typeof value === "number") {
    return Number.isInteger(value)
      ? { type: "integer", value: String(value) }
      : { type: "float", value };
  }
  if (typeof value === "bigint") {
    return { type: "integer", value: value.toString() };
  }
  if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
    return { type: "blob", base64: Buffer.from(value).toString("base64") };
  }
  return { type: "text", value: String(value) };
}

function fromHranaValue(value) {
  switch (value.type) {
    case "null":
      return null;
    case "integer":
      return value.value;
    case "float":
      return value.value;
    case "text":
      return value.value;
    case "blob":
      return Buffer.from(value.base64, "base64");
    default:
      return JSON.stringify(value);
  }
}

function pipelineEndpoint(rawUrl) {
  const url = new URL(rawUrl);
  let pathname = url.pathname.replace(/\/+$/, "");
  if (!pathname.endsWith("/v2/pipeline")) {
    pathname = `${pathname || ""}/v2/pipeline`;
  }
  url.pathname = pathname;

  return {
    displayUrl: rawUrl,
    pipelineUrl: url.toString(),
    database: databaseNameFromPath(url.pathname),
    systemUrl: systemNamespaceUrl(url).toString(),
  };
}

function databaseNameFromPath(pathname) {
  const parts = pathname.split("/").filter(Boolean);
  const v2Index = parts.indexOf("v2");
  if (v2Index > 0) {
    return parts[v2Index - 1];
  }
  return parts[0] || "orion";
}

function systemNamespaceUrl(url) {
  const system = new URL(url.toString());
  system.pathname = "/_orion";
  return system;
}
