#!/usr/bin/env node

import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const project = process.env.ORION_DOCKER_PROJECT ?? "orion-cluster-smoke";
const database = process.env.ORION_DOCKER_DATABASE ?? "cluster_smoke";
const keepRunning = process.env.ORION_DOCKER_KEEP_RUNNING === "1";
const buildTimeoutMs = numberEnv("ORION_DOCKER_BUILD_TIMEOUT_MS", 900_000);
const startupTimeoutMs = numberEnv("ORION_DOCKER_STARTUP_TIMEOUT_MS", 120_000);
const failoverTimeoutMs = numberEnv("ORION_DOCKER_FAILOVER_TIMEOUT_MS", 90_000);
const settleTimeoutMs = numberEnv("ORION_DOCKER_SETTLE_TIMEOUT_MS", 45_000);
const pollMs = numberEnv("ORION_DOCKER_POLL_MS", 500);

const defaultHttpPorts = [8281, 8282, 8283];
const defaultRaftPorts = [7301, 7302, 7303];
const httpPorts = configureDockerPorts("ORION_DOCKER_HTTP_PORT", defaultHttpPorts);
configureDockerPorts("ORION_DOCKER_RAFT_PORT", defaultRaftPorts);

const nodes = [
  { id: 1, service: "orion-node1", url: `http://127.0.0.1:${httpPorts[0]}/${database}` },
  { id: 2, service: "orion-node2", url: `http://127.0.0.1:${httpPorts[1]}/${database}` },
  { id: 3, service: "orion-node3", url: `http://127.0.0.1:${httpPorts[2]}/${database}` },
];

const checks = [];
let cleaningUp = false;

process.on("SIGINT", () => {
  cleanup().finally(() => process.exit(130));
});
process.on("SIGTERM", () => {
  cleanup().finally(() => process.exit(143));
});

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

  await pass("write through node1 and read from every node", async () => {
    await eventually("initial write through node1", async () => {
      await createDatabase(nodes[0], database);
      await execute(nodes[0], "create table if not exists cluster_crash_smoke (id integer primary key, value text not null)");
      await execute(nodes[0], "delete from cluster_crash_smoke");
      await execute(nodes[0], "insert into cluster_crash_smoke values (1, 'before-crash')");
    }, failoverTimeoutMs);
    await expectValueOnNodes(nodes, 1, "before-crash", settleTimeoutMs);
  });

  const initialLeader = await waitForLeader(nodes, settleTimeoutMs);
  const killed = serviceForNode(initialLeader);
  await pass(`stop leader node${initialLeader}`, async () => {
    await compose(["stop", killed.service]);
    await waitForHttpDown(killed, settleTimeoutMs);
  });

  const survivingNodes = nodes.filter((node) => node.id !== killed.id);
  let postCrashWriter = survivingNodes[0];
  await pass("surviving quorum accepts writes after leader loss", async () => {
    await eventually("post-crash write through surviving node", async () => {
      postCrashWriter = await firstWritable(survivingNodes, "insert into cluster_crash_smoke values (2, 'after-failover')");
    }, failoverTimeoutMs);
  });

  await pass("surviving replicas read post-failover write", async () => {
    await expectValueOnNodes(survivingNodes, 2, "after-failover", settleTimeoutMs);
  });

  await pass(`restart node${killed.id}`, async () => {
    await compose(["up", "-d", "--no-deps", killed.service]);
    await waitForHttp(killed, startupTimeoutMs);
  });

  await pass("restarted node catches up and forwards writes", async () => {
    await expectValueOnNodes([killed], 2, "after-failover", startupTimeoutMs);
    await eventually("write through restarted node", async () => {
      await execute(killed, "insert into cluster_crash_smoke values (3, 'after-restart')");
    }, failoverTimeoutMs);
    await expectValueOnNodes(nodes, 3, "after-restart", settleTimeoutMs);
  });

  await pass("strong read consistency after crash/restart", async () => {
    await expectRowsOnNodes(nodes, [
      ["1", "before-crash"],
      ["2", "after-failover"],
      ["3", "after-restart"],
    ], settleTimeoutMs);
  });

  console.log(JSON.stringify({
    ok: true,
    project,
    database,
    checks,
    host_ports: hostPortsSummary(),
    endpoints: Object.fromEntries(nodes.map((node) => [`node${node.id}`, node.url])),
  }, null, 2));
} catch (error) {
  console.error(JSON.stringify({
    ok: false,
    project,
    database,
    checks,
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

async function waitForHttp(node, timeoutMs) {
  await eventually(`node${node.id} HTTP ready`, async () => {
    const response = await fetch(`${node.url}/v2`);
    if (!response.ok) {
      throw new Error(`node${node.id} HTTP ${response.status}`);
    }
  }, timeoutMs);
}

async function waitForHttpDown(node, timeoutMs) {
  await eventually(`node${node.id} HTTP stopped`, async () => {
    try {
      const response = await fetch(`${node.url}/v2`);
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
        if (nodes.some((node) => node.id === leader)) {
          return leader;
        }
      }
    }
    throw new Error("leader is not known yet");
  }, timeoutMs);
}

async function raftMetrics(node) {
  const response = await fetch(`http://127.0.0.1:${portForNode(node)}/_orion/metrics/raft`);
  if (!response.ok) {
    throw new Error(`node${node.id} metrics HTTP ${response.status}: ${await response.text()}`);
  }
  return response.json();
}

async function createDatabase(node, name) {
  const response = await fetch(`http://127.0.0.1:${portForNode(node)}/_orion/databases`, {
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

async function firstWritable(candidateNodes, sql) {
  let lastError;
  for (const node of candidateNodes) {
    try {
      await execute(node, sql);
      return node;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError ?? new Error("no writable node");
}

async function expectValueOnNodes(candidateNodes, id, expected, timeoutMs) {
  await eventually(`all requested nodes read id=${id}`, async () => {
    await Promise.all(candidateNodes.map(async (node) => {
      const rows = await query(node, "select value from cluster_crash_smoke where id = ?", [id]);
      const value = rows[0]?.[0];
      if (value !== expected) {
        throw new Error(`node${node.id}: expected ${expected}, got ${JSON.stringify(value)}`);
      }
    }));
  }, timeoutMs);
}

async function expectRowsOnNodes(candidateNodes, expected, timeoutMs) {
  await eventually("all requested nodes read full rowset", async () => {
    await Promise.all(candidateNodes.map(async (node) => {
      const rows = await query(
        node,
        "select id, value from cluster_crash_smoke order by id",
        [],
      );
      if (JSON.stringify(rows) !== JSON.stringify(expected)) {
        throw new Error(`node${node.id}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(rows)}`);
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

async function pipeline(node, requests) {
  const response = await fetch(`${node.url}/v2/pipeline`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-orion-read-policy": "strong",
    },
    body: JSON.stringify({ requests }),
  });
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
  return run("docker", ["compose", "-p", project, ...args], options);
}

async function run(command, args, options = {}) {
  try {
    const result = await execFileAsync(command, args, {
      cwd: new URL("..", import.meta.url),
      timeout: options.timeoutMs ?? 60_000,
      maxBuffer: 16 * 1024 * 1024,
    });
    return result;
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
  await pipeDiagnostic(["logs", "--tail", "200", "orion-node1", "orion-node2", "orion-node3"]);
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
