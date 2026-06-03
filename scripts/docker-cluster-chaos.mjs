#!/usr/bin/env node

import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const project = process.env.ORION_DOCKER_PROJECT ?? "orion-cluster-chaos";
const database = process.env.ORION_DOCKER_DATABASE ?? "cluster_chaos";
const keepRunning = process.env.ORION_DOCKER_KEEP_RUNNING === "1";
const buildTimeoutMs = numberEnv("ORION_DOCKER_BUILD_TIMEOUT_MS", 900_000);
const startupTimeoutMs = numberEnv("ORION_DOCKER_STARTUP_TIMEOUT_MS", 120_000);
const failoverTimeoutMs = numberEnv("ORION_DOCKER_FAILOVER_TIMEOUT_MS", 90_000);
const settleTimeoutMs = numberEnv("ORION_DOCKER_SETTLE_TIMEOUT_MS", 60_000);
const requestTimeoutMs = numberEnv("ORION_DOCKER_REQUEST_TIMEOUT_MS", 15_000);
const pollMs = numberEnv("ORION_DOCKER_POLL_MS", 500);
const killSignal = process.env.ORION_DOCKER_KILL_SIGNAL ?? "SIGKILL";

const defaultHttpPorts = [8081, 8082, 8083];
const defaultRaftPorts = [7101, 7102, 7103];
const httpPorts = configureDockerPorts("ORION_DOCKER_HTTP_PORT", defaultHttpPorts);
configureDockerPorts("ORION_DOCKER_RAFT_PORT", defaultRaftPorts);

const nodes = [
  { id: 1, service: "orion-node1", url: `http://127.0.0.1:${httpPorts[0]}/${database}` },
  { id: 2, service: "orion-node2", url: `http://127.0.0.1:${httpPorts[1]}/${database}` },
  { id: 3, service: "orion-node3", url: `http://127.0.0.1:${httpPorts[2]}/${database}` },
];

const checks = [];
const expectedRows = [];
let nextId = 1;
let cleaningUp = false;

process.on("SIGINT", () => cleanup().finally(() => process.exit(130)));
process.on("SIGTERM", () => cleanup().finally(() => process.exit(143)));

try {
  await ensureDocker();
  await pass("fresh data volumes", resetDataVolumes);
  await pass("start three-node docker cluster", startCluster);
  await pass("all libSQL endpoints accept connections", async () => {
    await Promise.all(nodes.map((node) => waitForHttp(node, startupTimeoutMs)));
  });
  await pass("cluster elects initial leader", async () => {
    await waitForLeader(nodes, startupTimeoutMs);
  });
  await pass("initialize chaos table", async () => {
    await eventually("create chaos table", async () => {
      await createDatabase(nodes[0], database);
      await execute(nodes[0], [
        "create table if not exists cluster_chaos_events (",
        "  id integer primary key,",
        "  scenario text not null,",
        "  writer_node integer not null,",
        "  value text not null",
        ")",
      ].join(" "));
      await execute(nodes[0], "delete from cluster_chaos_events");
      await writeEvent(nodes[0], "baseline", "boot");
    }, failoverTimeoutMs);
    await expectRowsOnNodes(nodes, settleTimeoutMs);
  });

  await pass("sigkill current leader and continue writes on surviving quorum", async () => {
    const leader = serviceForNode(await waitForLeader(nodes, settleTimeoutMs));
    await docker(["kill", "--signal", killSignal, containerName(leader.service)]);
    await waitForHttpDown(leader, settleTimeoutMs);
    const survivors = nodes.filter((node) => node.id !== leader.id);
    await waitForLeader(survivors, failoverTimeoutMs);
    for (let i = 0; i < 3; i += 1) {
      await writeThroughAny(survivors, "leader-sigkill", `survivor-write-${i}`, failoverTimeoutMs);
    }
    await expectRowsOnNodes(survivors, settleTimeoutMs);
    await startNode(leader);
    await waitForHttp(leader, startupTimeoutMs);
    await expectRowsOnNodes(nodes, startupTimeoutMs);
  });

  await pass("restart a follower and write through the restarted follower", async () => {
    const leaderId = await waitForLeader(nodes, settleTimeoutMs);
    const follower = nodes.find((node) => node.id !== leaderId);
    if (!follower) {
      throw new Error("no follower found");
    }
    await compose(["restart", follower.service], { timeoutMs: startupTimeoutMs });
    await waitForHttp(follower, startupTimeoutMs);
    await expectRowsOnNodes(nodes, startupTimeoutMs);
    await writeEvent(follower, "follower-restart", "write-through-restarted-follower");
    await expectRowsOnNodes(nodes, settleTimeoutMs);
  });

  await pass("rolling one-node-at-a-time crashes preserve availability", async () => {
    for (const node of nodes) {
      await docker(["kill", "--signal", killSignal, containerName(node.service)]);
      await waitForHttpDown(node, settleTimeoutMs);
      const liveNodes = nodes.filter((candidate) => candidate.id !== node.id);
      await waitForLeader(liveNodes, failoverTimeoutMs);
      await writeThroughAny(liveNodes, "rolling-crash", `node-${node.id}-down`, failoverTimeoutMs);
      await expectRowsOnNodes(liveNodes, settleTimeoutMs);
      await startNode(node);
      await waitForHttp(node, startupTimeoutMs);
      await expectRowsOnNodes(nodes, startupTimeoutMs);
    }
  });

  await pass("quorum loss yields idempotent indeterminate write and recovers cleanly", async () => {
    const leaderId = await waitForLeader(nodes, settleTimeoutMs);
    const leader = serviceForNode(leaderId);
    const stopped = nodes.filter((node) => node.id !== leaderId);
    for (const node of stopped) {
      await compose(["stop", node.service]);
      await waitForHttpDown(node, settleTimeoutMs);
    }
    const indeterminate = await expectNoWriteAck(leader, "quorum-loss");
    for (const node of stopped) {
      await startNode(node);
    }
    for (const node of stopped) {
      await waitForHttp(node, startupTimeoutMs);
    }
    await waitForLeader(nodes, startupTimeoutMs);
    await retryIndeterminateWrite(indeterminate);
    await expectRowsOnNodes(nodes, startupTimeoutMs);
    await writeThroughAny(nodes, "quorum-recovery", "after-quorum-restore", failoverTimeoutMs);
    await expectRowsOnNodes(nodes, settleTimeoutMs);
  });

  await pass("final convergence after chaos", async () => {
    await waitForLeader(nodes, startupTimeoutMs);
    await expectRowsOnNodes(nodes, startupTimeoutMs);
  });

  console.log(JSON.stringify({
    ok: true,
    project,
    database,
    checks,
    expected_rows: expectedRows.length,
    host_ports: hostPortsSummary(),
    endpoints: Object.fromEntries(nodes.map((node) => [`node${node.id}`, node.url])),
  }, null, 2));
} catch (error) {
  console.error(JSON.stringify({
    ok: false,
    project,
    database,
    checks,
    expected_rows: expectedRows,
    error: {
      name: error?.name,
      message: error?.message,
      stack: error?.stack,
    },
  }, null, 2));
  await printDiagnostics().catch(() => {});
  process.exitCode = 1;
} finally {
  if (!keepRunning) {
    await cleanup();
  } else {
    console.error(`kept docker cluster running under compose project ${project}`);
  }
}

async function pass(name, fn) {
  await fn();
  checks.push(name);
}

async function ensureDocker() {
  await run("docker", ["compose", "version"]);
  await run("docker", ["info"]);
}

async function resetDataVolumes() {
  await compose(["down", "--remove-orphans"]);
  await run("docker", [
    "volume",
    "rm",
    "-f",
    `${project}_orion-node1`,
    `${project}_orion-node2`,
    `${project}_orion-node3`,
    `${project}_orion-object-store`,
    `${project}_orion-object-store-node1`,
    `${project}_orion-object-store-node2`,
    `${project}_orion-object-store-node3`,
  ], { allowFailure: true });
}

async function startCluster() {
  await compose([
    "up",
    "--build",
    "-d",
    "orion-node1",
    "orion-node2",
    "orion-node3",
  ], { timeoutMs: buildTimeoutMs });
}

async function cleanup() {
  if (cleaningUp) {
    return;
  }
  cleaningUp = true;
  await compose(["down", "--remove-orphans"], { allowFailure: true });
}

async function startNode(node) {
  await compose(["up", "-d", "--no-deps", node.service], { timeoutMs: startupTimeoutMs });
}

async function waitForHttp(node, timeoutMs) {
  await eventually(`node${node.id} HTTP ready`, async () => {
    const response = await fetchWithTimeout(`${node.url}/v2`);
    if (!response.ok) {
      throw new Error(`node${node.id} HTTP ${response.status}`);
    }
  }, timeoutMs);
}

async function waitForHttpDown(node, timeoutMs) {
  await eventually(`node${node.id} HTTP stopped`, async () => {
    try {
      const response = await fetchWithTimeout(`${node.url}/v2`, {}, Math.min(requestTimeoutMs, 2_000));
      if (response.ok) {
        throw new Error(`node${node.id} still accepts HTTP`);
      }
    } catch (error) {
      if (String(error?.message ?? "").includes("still accepts HTTP")) {
        throw error;
      }
    }
  }, timeoutMs);
}

async function waitForLeader(candidateNodes, timeoutMs) {
  return eventually("known raft leader", async () => {
    const snapshots = await Promise.all(candidateNodes.map((node) =>
      raftMetrics(node).catch(() => null)));
    for (const snapshot of snapshots.filter(Boolean)) {
      for (const entry of snapshot.raft_metrics ?? []) {
        const metrics = entry.metrics ?? {};
        if (metrics.state === "Leader" && metrics.current_leader === metrics.node_id) {
          return metrics.node_id;
        }
      }
    }
    for (const snapshot of snapshots.filter(Boolean)) {
      for (const entry of snapshot.raft_metrics ?? []) {
        const leader = entry.metrics?.current_leader;
        if (candidateNodes.some((node) => node.id === leader)) {
          return leader;
        }
      }
    }
    throw new Error("leader is not known yet");
  }, timeoutMs);
}

async function raftMetrics(node) {
  const response = await fetchWithTimeout(
    `http://127.0.0.1:${portForNode(node)}/_orion/metrics/raft`,
  );
  if (!response.ok) {
    throw new Error(`node${node.id} metrics HTTP ${response.status}: ${await response.text()}`);
  }
  return response.json();
}

async function createDatabase(node, name) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/databases`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name }),
  });
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (response.status === 409) {
    return body;
  }
  if (!response.ok) {
    throw new Error(`node${node.id} create database HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

function portForNode(node) {
  return httpPorts[node.id - 1];
}

function serviceForNode(nodeId) {
  const node = nodes.find((candidate) => candidate.id === nodeId);
  if (!node) {
    throw new Error(`unknown node id ${nodeId}`);
  }
  return node;
}

function containerName(service) {
  return `${project}-${service}-1`;
}

async function writeEvent(node, scenario, value) {
  const row = [String(nextId++), scenario, String(node.id), value];
  await execute(node, "insert into cluster_chaos_events values (?, ?, ?, ?)", row);
  expectedRows.push(row);
}

async function writeThroughAny(candidateNodes, scenario, value, timeoutMs = failoverTimeoutMs) {
  return eventually(`write ${scenario}/${value} through any candidate node`, async () => {
    let lastError;
    for (const node of candidateNodes) {
      try {
        await writeEvent(node, scenario, value);
        return node;
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError ?? new Error("no writable node");
  }, timeoutMs);
}

async function expectNoWriteAck(node, scenario) {
  const id = nextId;
  const row = [String(id), scenario, String(node.id), "should-not-commit"];
  const requests = insertEventRequests(row);
  const idempotencyKey = `chaos-${scenario}-${id}`;
  try {
    await pipeline(node, requests, { idempotencyKey });
  } catch {
    nextId += 1;
    return { row, requests, idempotencyKey };
  }
  throw new Error("write unexpectedly succeeded during quorum loss");
}

async function retryIndeterminateWrite(indeterminate) {
  await eventually("retry indeterminate idempotent write after quorum recovery", async () => {
    let lastError;
    for (const node of nodes) {
      try {
        const response = await pipeline(node, indeterminate.requests, {
          idempotencyKey: indeterminate.idempotencyKey,
        });
        const metadata = response.orion?.idempotency;
        if (metadata?.status !== "committed") {
          throw new Error(`expected committed idempotency metadata, got ${JSON.stringify(metadata)}`);
        }
        return;
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError ?? new Error("no node accepted idempotent retry");
  }, failoverTimeoutMs);

  await eventually("reconcile indeterminate idempotent write after retry", async () => {
    await reconcileIndeterminateWrite(indeterminate.row);
  }, settleTimeoutMs);
}

async function reconcileIndeterminateWrite(row) {
  const [id] = row;
  let rows = [];
  let lastError;
  for (const node of nodes) {
    try {
      rows = await query(node, "select id, scenario, writer_node, value from cluster_chaos_events where id = ?", [id]);
      lastError = undefined;
      break;
    } catch (error) {
      lastError = error;
    }
  }
  if (lastError && rows.length === 0) {
    throw lastError;
  }
  if (rows.length === 0) {
    throw new Error(`indeterminate quorum-loss row ${id} was not committed after retry`);
  }
  if (JSON.stringify(rows[0]) !== JSON.stringify(row)) {
    throw new Error(`indeterminate quorum-loss row changed: expected ${JSON.stringify(row)}, got ${JSON.stringify(rows[0])}`);
  }
  if (!expectedRows.some((expected) => JSON.stringify(expected) === JSON.stringify(row))) {
    expectedRows.push(row);
  }
}

async function expectRowsOnNodes(candidateNodes, timeoutMs) {
  await eventually("all requested nodes converge on expected rowset", async () => {
    await Promise.all(candidateNodes.map(async (node) => {
      const rows = await query(
        node,
        "select id, scenario, writer_node, value from cluster_chaos_events order by id",
      );
      if (JSON.stringify(rows) !== JSON.stringify(expectedRows)) {
        throw new Error(`node${node.id}: expected ${JSON.stringify(expectedRows)}, got ${JSON.stringify(rows)}`);
      }
    }));
  }, timeoutMs);
}

async function execute(node, sql, args = []) {
  const result = await pipeline(node, [{
    type: "execute",
    stmt: { sql, args: args.map(toHranaValue), want_rows: false },
  }]);
  return result.results?.[0]?.response?.result;
}

async function query(node, sql, args = []) {
  const result = await pipeline(node, [{
    type: "execute",
    stmt: { sql, args: args.map(toHranaValue), want_rows: true },
  }]);
  return (result.results?.[0]?.response?.result?.rows ?? []).map((row) =>
    row.map(formatHranaValue));
}

function insertEventRequests(row) {
  return [{
    type: "execute",
    stmt: {
      sql: "insert into cluster_chaos_events values (?, ?, ?, ?)",
      args: row.map(toHranaValue),
      want_rows: false,
    },
  }];
}

async function pipeline(node, requests, options = {}) {
  const headers = {
    "content-type": "application/json",
    "x-orion-read-policy": "strong",
  };
  if (options.idempotencyKey) {
    headers["x-orion-idempotency-key"] = options.idempotencyKey;
  }
  const response = await fetchWithTimeout(`${node.url}/v2/pipeline`, {
    method: "POST",
    headers,
    body: JSON.stringify({ requests }),
  }, options.timeoutMs ?? requestTimeoutMs);
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { error: text };
  }
  if (!response.ok) {
    throw new Error(`node${node.id} HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  for (const result of body.results ?? []) {
    if (result.type === "error") {
      throw new Error(`node${node.id} ${result.error?.code ?? "ERROR"}: ${result.error?.message ?? "unknown error"}`);
    }
  }
  return body;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = requestTimeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

function toHranaValue(value) {
  if (value === null || value === undefined) {
    return { type: "null" };
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return { type: "integer", value: String(value) };
  }
  return { type: "text", value: String(value) };
}

function formatHranaValue(value) {
  switch (value?.type) {
    case "null":
      return null;
    case "integer":
    case "float":
    case "text":
      return value.value;
    case "blob":
      return value.base64;
    default:
      throw new Error(`unsupported Hrana value ${JSON.stringify(value)}`);
  }
}

async function eventually(label, fn, timeoutMs) {
  const started = Date.now();
  let lastError;
  while (Date.now() - started < timeoutMs) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      await sleep(pollMs);
    }
  }
  throw new Error(`${label} timed out after ${timeoutMs}ms: ${lastError?.message ?? "unknown error"}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function compose(args, options = {}) {
  return docker(["compose", "-p", project, ...args], options);
}

async function docker(args, options = {}) {
  return run("docker", args, options);
}

async function run(command, args, options = {}) {
  try {
    return await execFileAsync(command, args, {
      cwd: new URL("..", import.meta.url),
      timeout: options.timeoutMs ?? 60_000,
      maxBuffer: 16 * 1024 * 1024,
    });
  } catch (error) {
    if (options.allowFailure) {
      return error;
    }
    const stdout = error.stdout ? `\nstdout:\n${error.stdout}` : "";
    const stderr = error.stderr ? `\nstderr:\n${error.stderr}` : "";
    throw new Error(`${command} ${args.join(" ")} failed: ${error.message}${stdout}${stderr}`);
  }
}

async function printDiagnostics() {
  console.error("\n--- docker compose ps ---");
  await pipeDiagnostic(["ps"]);
  console.error("\n--- recent logs ---");
  await pipeDiagnostic(["logs", "--tail", "240", "orion-node1", "orion-node2", "orion-node3"]);
}

async function pipeDiagnostic(args) {
  const result = await compose(args, { allowFailure: true, timeoutMs: 30_000 });
  if (result.stdout) {
    console.error(result.stdout);
  }
  if (result.stderr) {
    console.error(result.stderr);
  }
}

function numberEnv(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function configureDockerPorts(prefix, defaults) {
  return defaults.map((fallback, index) => {
    const name = `${prefix}_${index + 1}`;
    const value = numberEnv(name, fallback);
    process.env[name] = String(value);
    return value;
  });
}

function hostPortsSummary() {
  return Object.fromEntries(nodes.map((node) => [`node${node.id}`, {
    http: httpPorts[node.id - 1],
    raft: Number(process.env[`ORION_DOCKER_RAFT_PORT_${node.id}`]),
  }]));
}
