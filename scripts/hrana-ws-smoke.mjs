#!/usr/bin/env node

/*
 * Black-box Hrana/WebSocket smoke for Orion's libSQL-compatible endpoint.
 * This exercises the persistent stream protocol shape used by libSQL clients
 * without depending on Orion's internal HTTP pipeline helpers.
 */

import WebSocket from "ws";

const DEFAULT_URL = "ws://127.0.0.1:8091/ws_smoke/v2";
const url = process.argv[2] ?? process.env.LIBSQL_WS_URL ?? DEFAULT_URL;
const table = `orion_ws_smoke_${process.pid}_${Date.now().toString(36)}`;
const checks = [];

const ws = await connect(url);
let nextRequestId = 1;

try {
  await pass("hello", async () => {
    const response = await sendRaw({ type: "hello" });
    assertEqual(response, { type: "hello_ok" }, "hello response");
  });

  await pass("open stream", async () => {
    const response = await request({ type: "open_stream", stream_id: 0 });
    assertEqual(response, { type: "open_stream" }, "open_stream response");
  });

  await pass("execute create and insert", async () => {
    await execute(`create table ${quoteIdentifier(table)} (id integer primary key, label text not null)`);
    await execute(`insert into ${quoteIdentifier(table)} values (1, 'alpha'), (2, 'beta')`);
  });

  await pass("query rows", async () => {
    const rows = await query(`select id, label from ${quoteIdentifier(table)} order by id`);
    assertEqual(rows, [
      ["1", "alpha"],
      ["2", "beta"],
    ], "selected rows");
  });

  await pass("stored sql", async () => {
    const sqlId = 7;
    await request({
      type: "store_sql",
      sql_id: sqlId,
      sql: `select label from ${quoteIdentifier(table)} where id = ?`,
    });
    const response = await request({
      type: "execute",
      stream_id: 0,
      stmt: {
        sql_id: sqlId,
        args: [{ type: "integer", value: "2" }],
        want_rows: true,
      },
    });
    assertEqual(rowsFromResult(response.result), [["beta"]], "stored SQL rows");
    await request({ type: "close_sql", sql_id: sqlId });
  });

  await pass("batch and autocommit", async () => {
    const batch = await request({
      type: "batch",
      stream_id: 0,
      batch: {
        steps: [
          { stmt: { sql: `insert into ${quoteIdentifier(table)} values (3, 'gamma')` } },
          {
            condition: { type: "ok", step: 0 },
            stmt: {
              sql: `select count(*) from ${quoteIdentifier(table)}`,
              want_rows: true,
            },
          },
        ],
      },
    });
    assertEqual(
      rowsFromResult(batch.result.step_results[1]),
      [["3"]],
      "conditional batch select"
    );

    const autocommit = await request({ type: "get_autocommit", stream_id: 0 });
    assertEqual(autocommit, { type: "get_autocommit", is_autocommit: true }, "autocommit");
  });

  await pass("describe", async () => {
    const response = await request({
      type: "describe",
      stream_id: 0,
      sql: `select id, label from ${quoteIdentifier(table)}`,
    });
    assertEqual(
      response.result.cols.map((col) => col.name),
      ["id", "label"],
      "describe columns"
    );
  });

  await pass("unsupported cursors are protocol errors", async () => {
    const response = await requestEnvelope({
      type: "open_cursor",
      stream_id: 0,
      cursor_id: 1,
      batch: { steps: [{ stmt: { sql: `select * from ${quoteIdentifier(table)}`, want_rows: true } }] },
    });
    if (response.type !== "response_error" || response.error?.code !== "HRANA_PROTO_ERROR") {
      throw new Error(`expected cursor protocol error, got ${JSON.stringify(response)}`);
    }
  });

  await pass("close stream", async () => {
    const response = await request({ type: "close_stream", stream_id: 0 });
    assertEqual(response, { type: "close_stream" }, "close_stream response");
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
  ws.close();
}

async function connect(endpoint) {
  const socket = new WebSocket(endpoint, ["hrana3", "hrana2", "hrana1"]);
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out connecting to ${endpoint}`)), 10_000);
    socket.once("open", () => {
      clearTimeout(timer);
      resolve();
    });
    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
  return socket;
}

async function pass(name, fn) {
  await fn();
  checks.push(name);
}

async function execute(sql) {
  const response = await request({
    type: "execute",
    stream_id: 0,
    stmt: { sql, want_rows: false },
  });
  return response.result;
}

async function query(sql) {
  const response = await request({
    type: "execute",
    stream_id: 0,
    stmt: { sql, want_rows: true },
  });
  return rowsFromResult(response.result);
}

async function request(requestBody) {
  const response = await requestEnvelope(requestBody);
  if (response.type !== "response_ok") {
    throw new Error(`request failed: ${JSON.stringify(response)}`);
  }
  return response.response;
}

async function requestEnvelope(requestBody) {
  return sendRaw({
    type: "request",
    request_id: nextRequestId++,
    request: requestBody,
  });
}

async function sendRaw(message) {
  const response = waitForMessage();
  ws.send(JSON.stringify(message));
  return response;
}

async function waitForMessage() {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("timed out waiting for WebSocket response"));
    }, 10_000);
    function onMessage(data) {
      cleanup();
      try {
        resolve(JSON.parse(data.toString()));
      } catch (error) {
        reject(error);
      }
    }
    function onError(error) {
      cleanup();
      reject(error);
    }
    function onClose(code, reason) {
      cleanup();
      reject(new Error(`WebSocket closed while waiting for response: ${code} ${reason}`));
    }
    function cleanup() {
      clearTimeout(timer);
      ws.off("message", onMessage);
      ws.off("error", onError);
      ws.off("close", onClose);
    }
    ws.once("message", onMessage);
    ws.once("error", onError);
    ws.once("close", onClose);
  });
}

function rowsFromResult(result) {
  return (result?.rows ?? []).map((row) => row.map(formatHranaValue));
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
