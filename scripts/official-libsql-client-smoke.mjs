#!/usr/bin/env node

/*
 * Smoke test for Orion's libSQL HTTP endpoint using the official
 * JavaScript client package.
 *
 * Required:
 *   LIBSQL_URL or first positional argument
 *     Defaults to http://127.0.0.1:8091/appdb for local runs.
 *
 * Optional:
 *   LIBSQL_AUTH_TOKEN
 *     Sent as the official client's authToken.
 *
 * If this repository does not have @libsql/client installed, use:
 *   scripts/local-libsql-client-smoke.sh
 */

import { createClient } from "@libsql/client";

const DEFAULT_URL = "http://127.0.0.1:8091/appdb";

const config = {
  url: process.argv[2] ?? process.env.LIBSQL_URL ?? DEFAULT_URL,
  authToken: process.env.LIBSQL_AUTH_TOKEN,
};

const table = `orion_official_js_smoke_${process.pid}_${Date.now().toString(36)}`;
const checks = [];
let client;

try {
  client = createClient(config);

  await pass("create table", async () => {
    await client.execute([
      `create table ${table} (`,
      "  id integer primary key,",
      "  label text not null unique,",
      "  amount integer not null,",
      "  payload blob not null",
      ")",
    ].join(" "));
  });

  await pass("insert with positional args", async () => {
    await client.execute({
      sql: `insert into ${table} (label, amount, payload) values (?, ?, ?)`,
      args: ["positional", 7, bytes("positional blob")],
    });
  });

  await pass("insert with named args", async () => {
    await client.execute({
      sql: `insert into ${table} (label, amount, payload) values ($label, $amount, $payload)`,
      args: {
        label: "named",
        amount: 11,
        payload: bytes("named blob"),
      },
    });
  });

  await pass("select rows by public row API", async () => {
    const result = await client.execute({
      sql: `select label, amount from ${table} where amount >= ? order by amount`,
      args: [7],
    });
    assertEqual(result.columns, ["label", "amount"], "select columns");
    assertEqual(result.rows.length, 2, "selected row count");
    assertEqual(result.rows[0].label, "positional", "row object accessor");
    assertEqual(result.rows[1][0], "named", "row positional accessor");
  });

  await pass("interactive transaction", async () => {
    const transaction = await client.transaction("write");
    try {
      await transaction.execute({
        sql: `insert into ${table} (label, amount, payload) values (?, ?, ?)`,
        args: ["transaction", 19, bytes("transaction blob")],
      });
      await transaction.execute({
        sql: `update ${table} set amount = amount + ? where label = ?`,
        args: [4, "transaction"],
      });
      await transaction.commit();
    } finally {
      transaction.close();
    }

    const result = await client.execute({
      sql: `select amount from ${table} where label = ?`,
      args: ["transaction"],
    });
    assertEqual(result.rows[0].amount, 23, "committed transaction result");
  });

  await pass("blob round trip", async () => {
    const expected = bytes("named blob");
    const result = await client.execute({
      sql: `select payload from ${table} where label = ?`,
      args: ["named"],
    });
    assertBufferEqual(result.rows[0].payload, expected, "blob payload");
  });

  await pass("constraint error", async () => {
    let error;
    try {
      await client.execute({
        sql: `insert into ${table} (label, amount, payload) values (?, ?, ?)`,
        args: ["named", 99, bytes("duplicate")],
      });
    } catch (caught) {
      error = caught;
    }

    if (!error) {
      throw new Error("duplicate unique value did not fail");
    }
    if (!String(error.code ?? error.message).includes("CONSTRAINT")) {
      throw new Error(`expected constraint error, received ${formatError(error)}`);
    }
  });

  console.log(JSON.stringify({
    ok: true,
    url: config.url,
    client_protocol: client.protocol,
    checks,
  }, null, 2));
} catch (error) {
  console.error(JSON.stringify({
    ok: false,
    url: config.url,
    client_protocol: client?.protocol,
    checks,
    error: describeError(error),
  }, null, 2));
  process.exitCode = 1;
} finally {
  await client?.execute(`drop table if exists ${table}`).catch(() => {});
  client?.close();
}

async function pass(name, fn) {
  await fn();
  checks.push(name);
}

function bytes(value) {
  return new TextEncoder().encode(value);
}

function assertEqual(actual, expected, label) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label}: expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}`);
  }
}

function assertBufferEqual(actual, expected, label) {
  const actualBuffer = Buffer.from(toUint8Array(actual));
  const expectedBuffer = Buffer.from(expected);
  if (!actualBuffer.equals(expectedBuffer)) {
    throw new Error(`${label}: expected ${expectedBuffer.toString("hex")}, received ${actualBuffer.toString("hex")}`);
  }
}

function toUint8Array(value) {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new Error(`expected blob result, received ${Object.prototype.toString.call(value)}`);
}

function formatError(error) {
  return JSON.stringify({
    name: error?.name,
    message: error?.message,
    code: error?.code,
    extendedCode: error?.extendedCode,
  });
}

function describeError(error) {
  return {
    name: error?.name,
    message: error?.message,
    code: error?.code,
    extendedCode: error?.extendedCode,
    cause: error?.cause
      ? {
          name: error.cause.name,
          message: error.cause.message,
          code: error.cause.code,
        }
      : undefined,
  };
}
