#!/usr/bin/env node

import os from "node:os";

const DEFAULT_URL = "http://127.0.0.1:8091/bench";
const DEFAULT_WORKLOADS = ["select", "insert-autocommit", "insert-transaction", "mixed"];
const ALL_WORKLOADS = [
  ...DEFAULT_WORKLOADS,
  "blob-json-write",
  "blob-binary-write",
  "blob-stream-write",
  "blob-json-read",
  "blob-binary-read",
  "blob-stream-read",
];
const DEFAULT_SESSION_MODES = ["reused", "new"];
const DEFAULT_CONCURRENCY = [1, 4, 16];
const ERROR_SAMPLE_LIMIT = 10;

const config = parseArgs(process.argv.slice(2));
const baseUrl = databaseBaseUrl(config.url);
const endpoint = pipelineEndpoint(config.url);
const authToken = process.env.LIBSQL_AUTH_TOKEN;
const runId = createRunId();
const seedBlob = deterministicBytes(config.blobBytes, 17);

await withSetupClient(async (client) => {
  await client.execute("drop table if exists libsql_benchmark_matrix");
  await client.execute("drop table if exists libsql_benchmark_blobs");
  await client.execute([
    "create table libsql_benchmark_matrix (",
    "  id integer primary key,",
    "  workload text not null,",
    "  session_mode text not null,",
    "  case_id text not null,",
    "  payload text not null",
    ")",
  ].join(" "));

  const requests = [];
  for (let i = 0; i < config.seedRows; i += 1) {
    requests.push(executeRequest({
      sql: "insert into libsql_benchmark_matrix values (?, ?, ?, ?, ?)",
      args: [i, "seed", "setup", "seed", `seed-${i}`],
    }));
  }
  requests.push(executeRequest(
    "create table libsql_benchmark_blobs (id integer primary key, workload text not null, data blob not null)",
  ));
  requests.push(executeRequest({
    sql: "insert into libsql_benchmark_blobs values (?, ?, ?)",
    args: [1, "seed", seedBlob],
  }));
  await client.pipeline(requests);
});

const startedAt = new Date().toISOString();
const cases = [];

for (const concurrency of config.concurrency) {
  for (const sessionMode of config.sessionModes) {
    for (const workload of config.workloads) {
      cases.push(await runCase({ workload, sessionMode, concurrency }));
    }
  }
}

const result = {
  run_id: runId,
  url: config.url,
  endpoint,
  started_at: startedAt,
  finished_at: new Date().toISOString(),
  metadata: runMetadata(),
  options: {
    iterations: config.iterations,
    concurrency: config.concurrency,
    session_modes: config.sessionModes,
    workloads: config.workloads,
    transaction_size: config.transactionSize,
    mixed_write_ratio: config.mixedWriteRatio,
    seed_rows: config.seedRows,
    blob_bytes: config.blobBytes,
  },
  cases,
};

console.log(JSON.stringify(result, null, config.pretty ? 2 : 0));

async function runCase({ workload, sessionMode, concurrency }) {
  const caseId = `${workload}-${sessionMode}-c${concurrency}-${Date.now().toString(36)}`;
  const sampler = createSampler(workload, sessionMode, caseId);
  const caseStartedAt = new Date().toISOString();
  const workers = Array.from({ length: concurrency }, (_, workerId) =>
    runWorker({ workerId, concurrency, sampler, sessionMode }));
  const started = performance.now();
  const workerResults = await Promise.all(workers);
  const elapsedMs = performance.now() - started;
  const successSamples = workerResults.flatMap((worker) => worker.successSamples);
  const attemptSamples = workerResults.flatMap((worker) => worker.attemptSamples);
  const errors = workerResults.flatMap((worker) => worker.errors);
  const rowsWritten = workerResults.reduce((sum, worker) => sum + worker.rowsWritten, 0);
  const bytesTransferred = workerResults.reduce((sum, worker) => sum + worker.bytesTransferred, 0);
  const attemptedOperations = attemptSamples.length;
  const operations = successSamples.length;

  return {
    workload,
    session_mode: sessionMode,
    concurrency,
    case_id: caseId,
    started_at: caseStartedAt,
    finished_at: new Date().toISOString(),
    attempted_operations: attemptedOperations,
    operations,
    successful_operations: operations,
    failed_operations: errors.length,
    rows_written: rowsWritten,
    bytes_transferred: bytesTransferred,
    error_rate: attemptedOperations === 0 ? 0 : round(errors.length / attemptedOperations),
    errors: errorSummary(errors),
    elapsed_ms: round(elapsedMs),
    attempted_throughput_ops_per_sec: throughput(attemptedOperations, elapsedMs),
    throughput_ops_per_sec: throughput(operations, elapsedMs),
    successful_throughput_ops_per_sec: throughput(operations, elapsedMs),
    throughput_bytes_per_sec: throughput(bytesTransferred, elapsedMs),
    throughput_mib_per_sec: mibThroughput(bytesTransferred, elapsedMs),
    latency_ms: summary(successSamples),
    attempt_latency_ms: summary(attemptSamples),
  };
}

async function runWorker({ workerId, concurrency, sampler, sessionMode }) {
  const client = new HranaClient();
  const successSamples = [];
  const attemptSamples = [];
  const errors = [];
  let rowsWritten = 0;
  let bytesTransferred = 0;

  try {
    for (let index = workerId; index < sampler.operationCount; index += concurrency) {
      const started = performance.now();
      try {
        const outcome = normalizeSamplerOutcome(
          await sampler.run(index, sessionMode === "reused" ? client : null),
        );
        const elapsedMs = performance.now() - started;
        successSamples.push(elapsedMs);
        attemptSamples.push(elapsedMs);
        rowsWritten += outcome.rowsWritten;
        bytesTransferred += outcome.bytesTransferred;
      } catch (error) {
        const elapsedMs = performance.now() - started;
        attemptSamples.push(elapsedMs);
        errors.push(errorRecord(error, workerId, index, elapsedMs));
      }
    }
  } finally {
    if (sessionMode === "reused") {
      await client.close().catch(() => {});
    }
  }

  return { successSamples, attemptSamples, errors, rowsWritten, bytesTransferred };
}

function createSampler(workload, sessionMode, caseId) {
  if (workload === "select") {
    return {
      operationCount: config.iterations,
      async run(index, client) {
        await executeWithMode(client, sessionMode, {
          sql: "select payload from libsql_benchmark_matrix where id = ?",
          args: [index % config.seedRows],
        });
        return 0;
      },
    };
  }

  if (workload === "insert-autocommit") {
    return {
      operationCount: config.iterations,
      async run(index, client) {
        await executeWithMode(client, sessionMode, {
          sql: "insert into libsql_benchmark_matrix values (?, ?, ?, ?, ?)",
          args: [caseRowId(caseId, index), workload, sessionMode, caseId, `autocommit-${index}`],
        });
        return 1;
      },
    };
  }

  if (workload === "insert-transaction") {
    return {
      operationCount: Math.ceil(config.iterations / config.transactionSize),
      async run(index, client) {
        const firstRow = index * config.transactionSize;
        const rowCount = Math.min(config.transactionSize, config.iterations - firstRow);
        const requests = [executeRequest("begin immediate")];
        for (let offset = 0; offset < rowCount; offset += 1) {
          const rowIndex = firstRow + offset;
          requests.push(executeRequest({
            sql: "insert into libsql_benchmark_matrix values (?, ?, ?, ?, ?)",
            args: [caseRowId(caseId, rowIndex), workload, sessionMode, caseId, `txn-${rowIndex}`],
          }));
        }
        requests.push(executeRequest("commit"));
        await pipelineWithMode(client, sessionMode, requests);
        return rowCount;
      },
    };
  }

  if (workload === "mixed") {
    const writeEvery = Math.max(1, Math.round(1 / config.mixedWriteRatio));
    return {
      operationCount: config.iterations,
      async run(index, client) {
        if (index % writeEvery === 0) {
          await executeWithMode(client, sessionMode, {
            sql: "insert into libsql_benchmark_matrix values (?, ?, ?, ?, ?)",
            args: [caseRowId(caseId, index), workload, sessionMode, caseId, `mixed-${index}`],
          });
          return 1;
        }

        await executeWithMode(client, sessionMode, {
          sql: "select payload from libsql_benchmark_matrix where id = ?",
          args: [index % config.seedRows],
        });
        return 0;
      },
    };
  }

  if (
    workload === "blob-json-write"
      || workload === "blob-binary-write"
      || workload === "blob-stream-write"
  ) {
    const bytes = deterministicBytes(config.blobBytes, blobWorkloadSeed(workload));
    return {
      operationCount: config.iterations,
      async run(index, client) {
        const activeClient = client ?? new HranaClient();
        const rowId = caseRowId(caseId, index);
        try {
          await activeClient.execute({
            sql: "insert into libsql_benchmark_blobs values (?, ?, zeroblob(?))",
            args: [rowId, workload, bytes.length],
          });
          const opened = await activeClient.blobOpen({
            table: "libsql_benchmark_blobs",
            column: "data",
            rowid: rowId,
            read_only: false,
          });
          if (workload === "blob-binary-write") {
            await activeClient.blobWriteBytes({
              blobId: opened.result.blob_id,
              offset: 0,
              bytes,
            });
          } else if (workload === "blob-stream-write") {
            await activeClient.blobWriteStream({
              blobId: opened.result.blob_id,
              offset: 0,
              bytes,
            });
          } else {
            await activeClient.blob("write", {
              blob_id: opened.result.blob_id,
              offset: 0,
              base64: bytes.toString("base64"),
            });
          }
          await activeClient.blob("close", { blob_id: opened.result.blob_id });
          return { rowsWritten: 1, bytesTransferred: bytes.length };
        } finally {
          if (sessionMode === "new") {
            await activeClient.close().catch(() => {});
          }
        }
      },
    };
  }

  if (
    workload === "blob-json-read"
      || workload === "blob-binary-read"
      || workload === "blob-stream-read"
  ) {
    return {
      operationCount: config.iterations,
      async run(_index, client) {
        const activeClient = client ?? new HranaClient();
        try {
          const opened = await activeClient.blobOpen({
            table: "libsql_benchmark_blobs",
            column: "data",
            rowid: 1,
            read_only: true,
          });
          if (workload === "blob-binary-read") {
            await activeClient.blobReadBytes({
              blobId: opened.result.blob_id,
              offset: 0,
              length: config.blobBytes,
            });
          } else if (workload === "blob-stream-read") {
            await activeClient.blobReadStream({
              blobId: opened.result.blob_id,
              offset: 0,
              length: config.blobBytes,
            });
          } else {
            await activeClient.blob("read", {
              blob_id: opened.result.blob_id,
              offset: 0,
              length: config.blobBytes,
            });
          }
          await activeClient.blob("close", { blob_id: opened.result.blob_id });
          return { rowsWritten: 0, bytesTransferred: config.blobBytes };
        } finally {
          if (sessionMode === "new") {
            await activeClient.close().catch(() => {});
          }
        }
      },
    };
  }

  throw new Error(`unknown workload: ${workload}`);
}

function blobWorkloadSeed(workload) {
  if (workload === "blob-json-write") {
    return 31;
  }
  if (workload === "blob-binary-write") {
    return 47;
  }
  return 59;
}

async function executeWithMode(client, sessionMode, stmt) {
  const requests = [executeRequest(stmt)];
  const response = await pipelineWithMode(client, sessionMode, requests);
  return response.results[0].response.result;
}

async function pipelineWithMode(client, sessionMode, requests) {
  const activeClient = client ?? new HranaClient();
  try {
    return await activeClient.pipeline(requests);
  } finally {
    if (sessionMode === "new") {
      await activeClient.close().catch(() => {});
    }
  }
}

async function withSetupClient(fn) {
  const client = new HranaClient();
  try {
    await fn(client);
  } finally {
    await client.close().catch(() => {});
  }
}

class HranaClient {
  baton = null;

  async execute(stmt) {
    const response = await this.pipeline([executeRequest(stmt)]);
    return response.results[0].response.result;
  }

  async pipeline(requests, options = {}) {
    let response;
    try {
      response = await fetch(endpoint, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...(authToken ? { authorization: `Bearer ${authToken}` } : {}),
        },
        body: JSON.stringify({
          requests,
          ...(this.baton ? { baton: this.baton } : {}),
        }),
      });
    } catch (error) {
      throw benchmarkError(error.message ?? "fetch failed", {
        code: error.cause?.code ?? "FETCH_ERROR",
        cause: error,
      });
    }

    if (!response.ok) {
      throw benchmarkError(`HTTP ${response.status} ${response.statusText}`, {
        code: `HTTP_${response.status}`,
        httpStatus: response.status,
      });
    }

    let body;
    try {
      body = await response.json();
    } catch (error) {
      throw benchmarkError(`invalid JSON response: ${error.message}`, {
        code: "INVALID_JSON_RESPONSE",
        cause: error,
      });
    }

    for (const result of body.results ?? []) {
      if (result.type === "error") {
        throw benchmarkError(result.error?.message ?? "unknown libSQL error", {
          code: result.error?.code ?? "LIBSQL_ERROR",
        });
      }
    }

    if (options.keepBaton !== false) {
      this.baton = body.baton ?? this.baton;
    } else {
      this.baton = null;
    }
    return body;
  }

  async close() {
    if (!this.baton) {
      return;
    }
    await this.pipeline([{ type: "close" }], { keepBaton: false });
  }

  async blobOpen(request) {
    return this.blob("open", request);
  }

  async blob(action, request) {
    const response = await fetch(`${baseUrl}/v2/blob/${action}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(authToken ? { authorization: `Bearer ${authToken}` } : {}),
      },
      body: JSON.stringify({
        ...(this.baton ? { baton: this.baton } : {}),
        ...request,
      }),
    });

    let body;
    try {
      body = await response.json();
    } catch (error) {
      throw benchmarkError(`invalid blob JSON response: ${error.message}`, {
        code: "INVALID_JSON_RESPONSE",
        cause: error,
      });
    }
    if (!response.ok || body.error) {
      throw benchmarkError(body.error?.message ?? `HTTP ${response.status} ${response.statusText}`, {
        code: body.error?.code ?? `HTTP_${response.status}`,
        httpStatus: response.ok ? undefined : response.status,
      });
    }
    this.baton = body.baton ?? this.baton;
    return body;
  }

  async blobWriteBytes({ blobId, offset, bytes }) {
    const response = await fetch(blobBytesUrl("write-bytes", {
      baton: this.baton,
      blob_id: blobId,
      offset,
    }), {
      method: "POST",
      headers: {
        "content-type": "application/octet-stream",
        ...(authToken ? { authorization: `Bearer ${authToken}` } : {}),
      },
      body: bytes,
    });
    const body = Buffer.from(await response.arrayBuffer());
    if (!response.ok) {
      throw benchmarkError(body.toString("utf8") || `HTTP ${response.status} ${response.statusText}`, {
        code: `HTTP_${response.status}`,
        httpStatus: response.status,
      });
    }
    this.baton = response.headers.get("x-orion-session-token") ?? this.baton;
    return {
      bytesWritten: Number(response.headers.get("x-orion-blob-bytes-written")),
      size: Number(response.headers.get("x-orion-blob-size")),
    };
  }

  async blobWriteStream({ blobId, offset, bytes }) {
    const response = await fetch(blobBytesUrl("write-stream", {
      baton: this.baton,
      blob_id: blobId,
      offset,
      length: bytes.length,
    }), {
      method: "POST",
      headers: {
        "content-type": "application/octet-stream",
        ...(authToken ? { authorization: `Bearer ${authToken}` } : {}),
      },
      body: bytes,
    });
    const body = Buffer.from(await response.arrayBuffer());
    if (!response.ok) {
      throw benchmarkError(body.toString("utf8") || `HTTP ${response.status} ${response.statusText}`, {
        code: `HTTP_${response.status}`,
        httpStatus: response.status,
      });
    }
    this.baton = response.headers.get("x-orion-session-token") ?? this.baton;
    return {
      bytesWritten: Number(response.headers.get("x-orion-blob-bytes-written")),
      size: Number(response.headers.get("x-orion-blob-size")),
    };
  }

  async blobReadBytes({ blobId, offset, length }) {
    const response = await fetch(blobBytesUrl("read-bytes", {
      baton: this.baton,
      blob_id: blobId,
      offset,
      length,
    }), {
      method: "GET",
      headers: authToken ? { authorization: `Bearer ${authToken}` } : {},
    });
    const bytes = Buffer.from(await response.arrayBuffer());
    if (!response.ok) {
      throw benchmarkError(bytes.toString("utf8") || `HTTP ${response.status} ${response.statusText}`, {
        code: `HTTP_${response.status}`,
        httpStatus: response.status,
      });
    }
    this.baton = response.headers.get("x-orion-session-token") ?? this.baton;
    return bytes;
  }

  async blobReadStream({ blobId, offset, length }) {
    const response = await fetch(blobBytesUrl("read-stream", {
      baton: this.baton,
      blob_id: blobId,
      offset,
      length,
    }), {
      method: "GET",
      headers: authToken ? { authorization: `Bearer ${authToken}` } : {},
    });
    const bytes = Buffer.from(await response.arrayBuffer());
    if (!response.ok) {
      throw benchmarkError(bytes.toString("utf8") || `HTTP ${response.status} ${response.statusText}`, {
        code: `HTTP_${response.status}`,
        httpStatus: response.status,
      });
    }
    this.baton = response.headers.get("x-orion-session-token") ?? this.baton;
    return bytes;
  }
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

function executeRequest(stmt) {
  const sql = typeof stmt === "string" ? stmt : stmt.sql;
  const args = typeof stmt === "string" ? [] : stmt.args ?? [];
  return {
    type: "execute",
    stmt: {
      sql,
      args: args.map(toHranaValue),
      want_rows: statementCanReturnRows(sql),
    },
  };
}

function summary(values) {
  if (values.length === 0) {
    return null;
  }

  const sorted = [...values].sort((a, b) => a - b);
  return {
    min: round(sorted[0]),
    p50: percentile(sorted, 0.50),
    p90: percentile(sorted, 0.90),
    p95: percentile(sorted, 0.95),
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

function throughput(operations, elapsedMs) {
  if (elapsedMs <= 0) {
    return 0;
  }
  return round((operations / elapsedMs) * 1000);
}

function mibThroughput(bytes, elapsedMs) {
  return round(throughput(bytes, elapsedMs) / (1024 * 1024));
}

function normalizeSamplerOutcome(outcome) {
  if (typeof outcome === "number") {
    return { rowsWritten: outcome, bytesTransferred: 0 };
  }
  return {
    rowsWritten: outcome?.rowsWritten ?? 0,
    bytesTransferred: outcome?.bytesTransferred ?? 0,
  };
}

function benchmarkError(message, { code, httpStatus, cause } = {}) {
  const error = new Error(message, cause ? { cause } : undefined);
  if (code) {
    error.code = code;
  }
  if (httpStatus) {
    error.httpStatus = httpStatus;
  }
  return error;
}

function errorRecord(error, workerId, operationIndex, elapsedMs) {
  return {
    code: error.code ?? "UNKNOWN",
    message: error.message ?? String(error),
    http_status: error.httpStatus ?? null,
    worker_id: workerId,
    operation_index: operationIndex,
    elapsed_ms: round(elapsedMs),
  };
}

function errorSummary(errors) {
  const groups = new Map();
  for (const error of errors) {
    const key = `${error.code}\0${error.message}`;
    const group = groups.get(key) ?? {
      code: error.code,
      message: error.message,
      count: 0,
      http_statuses: {},
      first_worker_id: error.worker_id,
      first_operation_index: error.operation_index,
      first_elapsed_ms: error.elapsed_ms,
    };

    group.count += 1;
    if (error.http_status !== null) {
      const status = String(error.http_status);
      group.http_statuses[status] = (group.http_statuses[status] ?? 0) + 1;
    }
    groups.set(key, group);
  }

  return {
    count: errors.length,
    groups: [...groups.values()].sort((left, right) => right.count - left.count),
    samples: errors.slice(0, ERROR_SAMPLE_LIMIT),
    sample_limit: ERROR_SAMPLE_LIMIT,
  };
}

function createRunId() {
  return `${new Date().toISOString().replace(/[-:.TZ]/g, "")}-${process.pid}`;
}

function runMetadata() {
  return {
    script: "scripts/libsql-benchmark-matrix.mjs",
    node: process.version,
    platform: process.platform,
    arch: process.arch,
    hostname: os.hostname(),
    cpu_count: os.cpus().length,
    total_memory_bytes: os.totalmem(),
    pid: process.pid,
    auth_token_present: Boolean(authToken),
  };
}

function caseRowId(caseId, index) {
  let hash = 0;
  for (let i = 0; i < caseId.length; i += 1) {
    hash = ((hash * 31) + caseId.charCodeAt(i)) >>> 0;
  }
  return 1_000_000_000 + (hash % 1_000_000) * 100_000 + index;
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

function deterministicBytes(size, seed) {
  const buffer = Buffer.alloc(size);
  for (let i = 0; i < size; i += 1) {
    buffer[i] = (seed + i * 13) & 0xff;
  }
  return buffer;
}

function databaseBaseUrl(rawUrl) {
  const parsed = new URL(rawUrl);
  parsed.pathname = parsed.pathname.replace(/\/+$/, "").replace(/\/v2\/pipeline$/, "");
  parsed.search = "";
  parsed.hash = "";
  return parsed.toString().replace(/\/$/, "");
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

function parseArgs(args) {
  const parsed = {
    url: process.env.LIBSQL_URL ?? DEFAULT_URL,
    iterations: numberEnv("ORION_BENCH_ITERATIONS", 100),
    concurrency: listEnv("ORION_BENCH_CONCURRENCY", DEFAULT_CONCURRENCY, Number),
    sessionModes: listEnv("ORION_BENCH_SESSIONS", DEFAULT_SESSION_MODES, String),
    workloads: listEnv("ORION_BENCH_WORKLOADS", DEFAULT_WORKLOADS, String),
    transactionSize: numberEnv("ORION_BENCH_TRANSACTION_SIZE", 10),
    mixedWriteRatio: numberEnv("ORION_BENCH_MIXED_WRITE_RATIO", 0.2),
    seedRows: numberEnv("ORION_BENCH_SEED_ROWS", 1000),
    blobBytes: numberEnv("ORION_BENCH_BLOB_BYTES", 64 * 1024),
    pretty: true,
  };
  let positionalUrlSeen = false;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    const readValue = () => {
      const value = args[i + 1];
      if (!value || value.startsWith("--")) {
        throw new Error(`${arg} requires a value`);
      }
      i += 1;
      return value;
    };

    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    } else if (arg === "--url") {
      parsed.url = readValue();
    } else if (arg === "--iterations") {
      parsed.iterations = parsePositiveInteger(readValue(), arg);
    } else if (arg === "--concurrency") {
      parsed.concurrency = parseList(readValue(), Number).map((value) =>
        parsePositiveInteger(String(value), arg));
    } else if (arg === "--sessions") {
      parsed.sessionModes = parseList(readValue(), String);
    } else if (arg === "--workloads") {
      parsed.workloads = parseList(readValue(), String);
    } else if (arg === "--transaction-size") {
      parsed.transactionSize = parsePositiveInteger(readValue(), arg);
    } else if (arg === "--mixed-write-ratio") {
      parsed.mixedWriteRatio = parseRatio(readValue(), arg);
    } else if (arg === "--seed-rows") {
      parsed.seedRows = parsePositiveInteger(readValue(), arg);
    } else if (arg === "--blob-bytes") {
      parsed.blobBytes = parsePositiveInteger(readValue(), arg);
    } else if (arg === "--compact-json") {
      parsed.pretty = false;
    } else if (arg === "--json") {
      parsed.pretty = true;
    } else if (!arg.startsWith("--") && !positionalUrlSeen) {
      parsed.url = arg;
      positionalUrlSeen = true;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  parsed.iterations = parsePositiveInteger(String(parsed.iterations), "iterations");
  parsed.concurrency = parsed.concurrency.map((value) =>
    parsePositiveInteger(String(value), "concurrency"));
  parsed.transactionSize = parsePositiveInteger(String(parsed.transactionSize), "transaction size");
  parsed.mixedWriteRatio = parseRatio(String(parsed.mixedWriteRatio), "mixed write ratio");
  parsed.seedRows = parsePositiveInteger(String(parsed.seedRows), "seed rows");
  parsed.blobBytes = parsePositiveInteger(String(parsed.blobBytes), "blob bytes");

  validateChoices("session", parsed.sessionModes, DEFAULT_SESSION_MODES);
  validateChoices("workload", parsed.workloads, ALL_WORKLOADS);
  return parsed;
}

function parseList(value, map) {
  return value.split(",").map((item) => map(item.trim())).filter((item) => item !== "");
}

function listEnv(name, fallback, map) {
  return process.env[name] ? parseList(process.env[name], map) : fallback;
}

function numberEnv(name, fallback) {
  return process.env[name] ? Number(process.env[name]) : fallback;
}

function parsePositiveInteger(value, label) {
  const number = Number(value);
  if (!Number.isInteger(number) || number < 1) {
    throw new Error(`${label} must be a positive integer`);
  }
  return number;
}

function parseRatio(value, label) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0 || number >= 1) {
    throw new Error(`${label} must be greater than 0 and less than 1`);
  }
  return number;
}

function validateChoices(label, values, allowed) {
  const unknown = values.filter((value) => !allowed.includes(value));
  if (unknown.length > 0) {
    throw new Error(`unknown ${label} value(s): ${unknown.join(", ")}`);
  }
}

function printHelp() {
  console.log([
    "Usage: node scripts/libsql-benchmark-matrix.mjs [url] [options]",
    "",
    "Options:",
    "  --url <url>                    libSQL database URL (default: LIBSQL_URL or local bench URL)",
    "  --iterations <n>               total rows/operations per case (default: 100)",
    "  --concurrency <list>           comma-separated worker counts (default: 1,4,16)",
    "  --sessions <list>              reused,new or a comma-separated subset",
    "  --workloads <list>             select,insert-autocommit,insert-transaction,mixed",
    "                                 plus blob-json-write,blob-binary-write,blob-stream-write,",
    "                                 blob-json-read,blob-binary-read,blob-stream-read",
    "  --transaction-size <n>         rows per insert-transaction operation (default: 10)",
    "  --mixed-write-ratio <number>   write ratio for mixed workload, exclusive 0..1 (default: 0.2)",
    "  --seed-rows <n>                rows available for select workloads (default: 1000)",
    "  --blob-bytes <n>               bytes per blob read/write operation (default: 65536)",
    "  --compact-json                 emit compact JSON",
    "  --help                         show this help",
    "",
    "Environment:",
    "  LIBSQL_URL, LIBSQL_AUTH_TOKEN, ORION_BENCH_ITERATIONS,",
    "  ORION_BENCH_CONCURRENCY, ORION_BENCH_SESSIONS,",
    "  ORION_BENCH_WORKLOADS, ORION_BENCH_TRANSACTION_SIZE,",
    "  ORION_BENCH_MIXED_WRITE_RATIO, ORION_BENCH_SEED_ROWS,",
    "  ORION_BENCH_BLOB_BYTES",
  ].join("\n"));
}
