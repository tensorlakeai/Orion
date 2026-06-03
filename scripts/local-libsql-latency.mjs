#!/usr/bin/env node

const url = process.argv[2] ?? process.env.LIBSQL_URL ?? "http://127.0.0.1:8091/latency";
const iterations = Number(process.env.ORION_LATENCY_ITERATIONS ?? process.argv[3] ?? 50);
const endpoint = pipelineEndpoint(url);
let baton = null;

await execute("drop table if exists latency_probe");
await execute("create table latency_probe (id integer primary key, payload text)");

const singleInserts = [];
for (let i = 0; i < iterations; i += 1) {
  singleInserts.push(await time(() => execute({
    sql: "insert into latency_probe values (?, ?)",
    args: [i, `single-${i}`],
  })));
}

const selects = [];
for (let i = 0; i < iterations; i += 1) {
  selects.push(await time(() => execute({
    sql: "select payload from latency_probe where id = ?",
    args: [i],
  })));
}

const transactionalInsert = await time(async () => {
  await execute("begin immediate");
  for (let i = 0; i < iterations; i += 1) {
    await execute({
      sql: "insert into latency_probe values (?, ?)",
      args: [iterations + i, `txn-${i}`],
    });
  }
  await execute("commit");
});

await closeSession();

console.log(JSON.stringify({
  url,
  iterations,
  single_insert_ms: summary(singleInserts),
  select_ms: summary(selects),
  transaction_insert_total_ms: Number(transactionalInsert.toFixed(3)),
  transaction_insert_per_row_ms: Number((transactionalInsert / iterations).toFixed(3)),
}, null, 2));

async function execute(stmt) {
  const sql = typeof stmt === "string" ? stmt : stmt.sql;
  const args = typeof stmt === "string" ? [] : stmt.args ?? [];
  const response = await pipeline([{
    type: "execute",
    stmt: {
      sql,
      args: args.map(toHranaValue),
      want_rows: statementCanReturnRows(sql),
    },
  }]);
  const result = response.results[0];
  if (result.type === "error") {
    throw new Error(result.error?.message ?? "unknown libSQL error");
  }
  return result.response.result;
}

async function closeSession() {
  if (!baton) {
    return;
  }
  await pipeline([{ type: "close" }], false);
}

async function pipeline(requests, keepBaton = true) {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(process.env.LIBSQL_AUTH_TOKEN
        ? { authorization: `Bearer ${process.env.LIBSQL_AUTH_TOKEN}` }
        : {}),
    },
    body: JSON.stringify({
      requests,
      ...(baton ? { baton } : {}),
    }),
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} ${response.statusText}`);
  }
  const body = await response.json();
  if (keepBaton) {
    baton = body.baton ?? baton;
  } else {
    baton = null;
  }
  return body;
}

async function time(fn) {
  const start = performance.now();
  await fn();
  return performance.now() - start;
}

function summary(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    min: round(sorted[0]),
    p50: percentile(sorted, 0.50),
    p90: percentile(sorted, 0.90),
    p99: percentile(sorted, 0.99),
    max: round(sorted[sorted.length - 1]),
    avg: round(values.reduce((sum, value) => sum + value, 0) / values.length),
  };
}

function percentile(sorted, p) {
  const index = Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1);
  return round(sorted[index]);
}

function round(value) {
  return Number(value.toFixed(3));
}

function statementCanReturnRows(sql) {
  const keyword = sql.trimStart().match(/^[a-zA-Z]+/)?.[0]?.toLowerCase();
  return ["select", "with", "pragma", "explain", "values"].includes(keyword);
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

function pipelineEndpoint(rawUrl) {
  const parsed = new URL(rawUrl);
  let pathname = parsed.pathname.replace(/\/+$/, "");
  if (!pathname.endsWith("/v2/pipeline")) {
    pathname = `${pathname || ""}/v2/pipeline`;
  }
  parsed.pathname = pathname;
  return parsed.toString();
}
