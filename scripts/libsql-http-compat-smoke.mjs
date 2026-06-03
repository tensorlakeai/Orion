#!/usr/bin/env node

/*
 * Black-box protocol smoke for Orion's raw libSQL/Hrana HTTP endpoint.
 * Covers multi-baton session isolation, transaction visibility, writer
 * contention, and explicit non-goal endpoint behavior.
 */

const DEFAULT_URL = "http://127.0.0.1:8091/http_compat_smoke";
const url = process.argv[2] ?? process.env.LIBSQL_URL ?? DEFAULT_URL;
const baseUrl = url.replace(/\/$/, "");
const table = `orion_http_compat_${process.pid}_${Date.now().toString(36)}`;
const checks = [];
const sessions = new Map();

try {
  await pass("setup", async () => {
    await execute("setup", `create table ${quoteIdentifier(table)} (id integer primary key, label text not null)`);
    await execute("setup", `insert into ${quoteIdentifier(table)} values (1, 'committed')`);
  });

  await pass("session transaction visibility", async () => {
    await execute("writer", "begin immediate");
    await execute("writer", `insert into ${quoteIdentifier(table)} values (2, 'uncommitted')`);

    const writerRows = await query("writer", `select count(*) from ${quoteIdentifier(table)}`);
    assertEqual(writerRows, [["2"]], "writer sees its uncommitted row");

    const readerRows = await query("reader", `select count(*) from ${quoteIdentifier(table)}`);
    assertEqual(readerRows, [["1"]], "reader sees committed snapshot only");
  });

  await pass("writer contention returns busy or locked", async () => {
    const response = await pipeline("contender", [
      executeRequest(`insert into ${quoteIdentifier(table)} values (3, 'contender')`),
    ]);
    const result = response.results?.[0];
    if (result?.type !== "error") {
      throw new Error(`expected contention error, got ${JSON.stringify(result)}`);
    }
    const code = result.error?.code;
    if (!["SQLITE_BUSY", "SQLITE_LOCKED"].includes(code)) {
      throw new Error(`expected SQLITE_BUSY or SQLITE_LOCKED, got ${JSON.stringify(result.error)}`);
    }
  });

  await pass("commit releases writer lock", async () => {
    await execute("writer", "commit");
    await execute("contender", `insert into ${quoteIdentifier(table)} values (3, 'contender')`);
    const rows = await query("reader", `select id, label from ${quoteIdentifier(table)} order by id`);
    assertEqual(rows, [
      ["1", "committed"],
      ["2", "uncommitted"],
      ["3", "contender"],
    ], "committed rows after contention clears");
  });

  await pass("unknown request type is protocol error", async () => {
    const response = await rawPipeline({
      requests: [{ type: "sync", generation: 1 }],
    });
    if (![400, 422].includes(response.status)) {
      throw new Error(`expected HTTP 400 or 422 for unknown request, got ${response.status}: ${JSON.stringify(response.body)}`);
    }
    if (!String(response.body.error ?? "").includes("unknown variant")) {
      throw new Error(`expected serde unknown variant error, got ${JSON.stringify(response.body)}`);
    }
  });

  await pass("replication endpoints are absent", async () => {
    for (const path of [
      "/sync",
      "/v2/sync",
      "/v2/replicate",
      "/v2/frames",
      "/v2/generation",
      "/v2/replication",
    ]) {
      const response = await fetch(`${baseUrl}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
      });
      if (response.status !== 404 && response.status !== 405) {
        throw new Error(`${path}: expected 404 or 405, got ${response.status}: ${await response.text()}`);
      }
    }
  });

  console.log(JSON.stringify({ ok: true, url, checks }, null, 2));
} catch (error) {
  console.error(JSON.stringify({
    ok: false,
    url,
    checks,
    error: {
      name: error?.name,
      message: error?.message,
      stack: error?.stack,
    },
  }, null, 2));
  process.exitCode = 1;
} finally {
  await execute("cleanup", `drop table if exists ${quoteIdentifier(table)}`).catch(() => {});
}

async function pass(name, fn) {
  await fn();
  checks.push(name);
}

async function execute(session, sql) {
  const response = await pipeline(session, [executeRequest(sql)]);
  const result = response.results?.[0];
  if (result?.type === "error") {
    throw new Error(`${result.error?.code ?? "ERROR"}: ${result.error?.message ?? "unknown error"}`);
  }
  return result?.response?.result;
}

async function query(session, sql) {
  const response = await pipeline(session, [sqlWithRows(sql)]);
  const result = response.results?.[0];
  if (result?.type === "error") {
    throw new Error(`${result.error?.code ?? "ERROR"}: ${result.error?.message ?? "unknown error"}`);
  }
  return (result?.response?.result?.rows ?? []).map((row) => row.map(formatHranaValue));
}

function executeRequest(sql) {
  return {
    type: "execute",
    stmt: { sql, want_rows: false },
  };
}

function sqlWithRows(sql) {
  return {
    type: "execute",
    stmt: { sql, want_rows: true },
  };
}

async function pipeline(session, requests) {
  const response = await rawPipeline({
    baton: sessions.get(session),
    requests,
  });
  if (response.status !== 200) {
    throw new Error(`HTTP ${response.status}: ${JSON.stringify(response.body)}`);
  }
  if (response.body.baton) {
    sessions.set(session, response.body.baton);
  }
  return response.body;
}

async function rawPipeline(body) {
  const response = await fetch(`${baseUrl}/v2/pipeline`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = { error: text };
  }
  return {
    status: response.status,
    body: parsed,
  };
}

function formatHranaValue(value) {
  switch (value?.type) {
    case "null":
      return null;
    case "integer":
      return value.value;
    case "float":
      return value.value;
    case "text":
      return value.value;
    case "blob":
      return value.base64;
    default:
      throw new Error(`unsupported Hrana value ${JSON.stringify(value)}`);
  }
}

function quoteIdentifier(value) {
  return `"${value.replaceAll('"', '""')}"`;
}

function assertEqual(actual, expected, label) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label}: expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}`);
  }
}
