#!/usr/bin/env node

const DEFAULT_URL = "http://127.0.0.1:8091/blob_smoke";
const url = process.argv[2] ?? process.env.LIBSQL_URL ?? DEFAULT_URL;
const baseUrl = url.replace(/\/$/, "");
const table = `orion_blob_smoke_${process.pid}_${Date.now().toString(36)}`;
const checks = [];
let baton;
let blobId;

try {
  await pass("setup", async () => {
    await execute(`create table ${quoteIdentifier(table)} (id integer primary key, data blob not null)`);
    await execute(`insert into ${quoteIdentifier(table)} (id, data) values (1, zeroblob(11)), (2, zeroblob(5))`);
  });

  await pass("open writable blob", async () => {
    const response = await blob("open", {
      table,
      column: "data",
      rowid: 1,
      read_only: false,
    });
    assertEqual(response.result.type, "open", "open type");
    assertEqual(response.result.size, 11, "open size");
    blobId = response.result.blob_id;
  });

  await pass("write chunks", async () => {
    await blob("write", {
      blob_id: blobId,
      offset: 0,
      base64: toBase64("hello "),
    });
    await blob("write", {
      blob_id: blobId,
      offset: 6,
      base64: toBase64("world"),
    });
  });

  await pass("read slice", async () => {
    const response = await blob("read", {
      blob_id: blobId,
      offset: 0,
      length: 11,
    });
    assertEqual(fromBase64(response.result.base64), "hello world", "blob data");
  });

  await pass("verify through SQL", async () => {
    const rows = await query(`select data from ${quoteIdentifier(table)} where id = 1`);
    assertEqual(rows, [toBase64("hello world")], "SQL blob readback");
  });

  await pass("reopen and close", async () => {
    const reopened = await blob("reopen", {
      blob_id: blobId,
      rowid: 2,
    });
    assertEqual(reopened.result.size, 5, "reopened size");
  });

  await pass("write raw bytes", async () => {
    const response = await blobWriteBytes({
      blob_id: blobId,
      offset: 0,
      bytes: Buffer.from("bytes"),
    });
    assertEqual(response.bytes_written, 5, "binary write length");
    assertEqual(response.size, 5, "binary write blob size");
  });

  await pass("read raw bytes", async () => {
    const response = await blobReadBytes({
      blob_id: blobId,
      offset: 0,
      length: 5,
    });
    assertEqual(response.bytes.toString("utf8"), "bytes", "binary blob data");
    assertEqual(response.bytes_read, 5, "binary read length");
    assertEqual(response.content_type, "application/octet-stream", "binary content type");
  });

  await pass("verify raw bytes through SQL", async () => {
    const rows = await query(`select data from ${quoteIdentifier(table)} where id = 2`);
    assertEqual(rows, [toBase64("bytes")], "SQL binary blob readback");
  });

  await pass("write streamed bytes", async () => {
    const response = await blobWriteStream({
      blob_id: blobId,
      offset: 0,
      bytes: Buffer.from("strea"),
    });
    assertEqual(response.bytes_written, 5, "stream write length");
    assertEqual(response.size, 5, "stream write blob size");
  });

  await pass("read streamed bytes", async () => {
    const response = await blobReadStream({
      blob_id: blobId,
      offset: 0,
      length: 5,
    });
    assertEqual(response.bytes.toString("utf8"), "strea", "stream blob data");
    assertEqual(response.bytes_read, 5, "stream read length");
    assertEqual(response.content_type, "application/octet-stream", "stream content type");
  });

  await pass("verify streamed bytes through SQL", async () => {
    const rows = await query(`select data from ${quoteIdentifier(table)} where id = 2`);
    assertEqual(rows, [toBase64("strea")], "SQL streamed blob readback");
  });

  await pass("close", async () => {
    const closed = await blob("close", { blob_id: blobId });
    assertEqual(closed.result.type, "close", "close type");
  });

  await pass("closed handle rejected", async () => {
    const response = await rawBlob("read", {
      baton,
      blob_id: blobId,
      offset: 0,
      length: 1,
    });
    if (response.error?.code !== "HRANA_PROTO_ERROR") {
      throw new Error(`expected closed handle protocol error, got ${JSON.stringify(response)}`);
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
  await execute(`drop table if exists ${quoteIdentifier(table)}`).catch(() => {});
}

async function pass(name, fn) {
  await fn();
  checks.push(name);
}

async function execute(sql) {
  const response = await pipeline([{ type: "execute", stmt: { sql } }]);
  const result = response.results?.[0];
  if (result?.type === "error") {
    throw new Error(`${result.error?.code ?? "ERROR"}: ${result.error?.message ?? "unknown error"}`);
  }
  return result.response.result;
}

async function query(sql) {
  const response = await pipeline([{ type: "execute", stmt: { sql, want_rows: true } }]);
  const result = response.results?.[0];
  if (result?.type === "error") {
    throw new Error(`${result.error?.code ?? "ERROR"}: ${result.error?.message ?? "unknown error"}`);
  }
  return (result.response.result.rows ?? []).map((row) => row[0]?.base64);
}

async function pipeline(requests) {
  const response = await fetch(`${baseUrl}/v2/pipeline`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ requests }),
  });
  const body = await response.json();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function blob(action, body) {
  const response = await rawBlob(action, { baton, ...body });
  if (response.error) {
    throw new Error(`${response.error.code ?? "ERROR"}: ${response.error.message}`);
  }
  baton = response.baton;
  return response;
}

async function rawBlob(action, body) {
  const response = await fetch(`${baseUrl}/v2/blob/${action}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const parsed = await response.json();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${JSON.stringify(parsed)}`);
  }
  return parsed;
}

async function blobWriteBytes({ blob_id, offset, bytes }) {
  const response = await fetch(blobBytesUrl("write-bytes", { baton, blob_id, offset }), {
    method: "POST",
    headers: { "content-type": "application/octet-stream" },
    body: bytes,
  });
  const body = await response.arrayBuffer();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${Buffer.from(body).toString("utf8")}`);
  }
  baton = response.headers.get("x-orion-session-token") ?? baton;
  return {
    blob_id: response.headers.get("x-orion-blob-id"),
    offset: Number(response.headers.get("x-orion-blob-offset")),
    bytes_written: Number(response.headers.get("x-orion-blob-bytes-written")),
    size: Number(response.headers.get("x-orion-blob-size")),
  };
}

async function blobReadBytes({ blob_id, offset, length }) {
  const response = await fetch(blobBytesUrl("read-bytes", { baton, blob_id, offset, length }), {
    method: "GET",
  });
  const bytes = Buffer.from(await response.arrayBuffer());
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${bytes.toString("utf8")}`);
  }
  baton = response.headers.get("x-orion-session-token") ?? baton;
  return {
    bytes,
    content_type: response.headers.get("content-type"),
    blob_id: response.headers.get("x-orion-blob-id"),
    offset: Number(response.headers.get("x-orion-blob-offset")),
    bytes_read: Number(response.headers.get("x-orion-blob-bytes-read")),
    size: Number(response.headers.get("x-orion-blob-size")),
  };
}

async function blobWriteStream({ blob_id, offset, bytes }) {
  const response = await fetch(blobBytesUrl("write-stream", {
    baton,
    blob_id,
    offset,
    length: bytes.length,
  }), {
    method: "POST",
    headers: { "content-type": "application/octet-stream" },
    body: bytes,
  });
  const body = await response.arrayBuffer();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${Buffer.from(body).toString("utf8")}`);
  }
  baton = response.headers.get("x-orion-session-token") ?? baton;
  return {
    blob_id: response.headers.get("x-orion-blob-id"),
    offset: Number(response.headers.get("x-orion-blob-offset")),
    bytes_written: Number(response.headers.get("x-orion-blob-bytes-written")),
    size: Number(response.headers.get("x-orion-blob-size")),
  };
}

async function blobReadStream({ blob_id, offset, length }) {
  const response = await fetch(blobBytesUrl("read-stream", { baton, blob_id, offset, length }), {
    method: "GET",
  });
  const bytes = Buffer.from(await response.arrayBuffer());
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${bytes.toString("utf8")}`);
  }
  baton = response.headers.get("x-orion-session-token") ?? baton;
  return {
    bytes,
    content_type: response.headers.get("content-type"),
    blob_id: response.headers.get("x-orion-blob-id"),
    offset: Number(response.headers.get("x-orion-blob-offset")),
    bytes_read: Number(response.headers.get("x-orion-blob-bytes-read")),
    size: Number(response.headers.get("x-orion-blob-size")),
  };
}

function blobBytesUrl(action, params) {
  const target = new URL(`${baseUrl}/v2/blob/${action}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) {
      target.searchParams.set(key, String(value));
    }
  }
  return target;
}

function toBase64(value) {
  return Buffer.from(value).toString("base64");
}

function fromBase64(value) {
  return Buffer.from(value, "base64").toString("utf8");
}

function quoteIdentifier(value) {
  return `"${value.replaceAll('"', '""')}"`;
}

function assertEqual(actual, expected, label) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label}: expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}`);
  }
}
