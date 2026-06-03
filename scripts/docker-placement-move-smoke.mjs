#!/usr/bin/env node

import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const project = process.env.ORION_DOCKER_PROJECT ?? "orion-placement-smoke";
const database = process.env.ORION_DOCKER_DATABASE ?? "placement_smoke";
const targetGroup = process.env.ORION_DOCKER_TARGET_GROUP ?? "rg_placement_smoke";
const keepRunning = process.env.ORION_DOCKER_KEEP_RUNNING === "1";
const buildTimeoutMs = numberEnv("ORION_DOCKER_BUILD_TIMEOUT_MS", 900_000);
const startupTimeoutMs = numberEnv("ORION_DOCKER_STARTUP_TIMEOUT_MS", 120_000);
const settleTimeoutMs = numberEnv("ORION_DOCKER_SETTLE_TIMEOUT_MS", 60_000);
const moveTimeoutMs = numberEnv("ORION_DOCKER_MOVE_TIMEOUT_MS", 120_000);
const pollMs = numberEnv("ORION_DOCKER_POLL_MS", 500);

const defaultHttpPorts = [8381, 8382, 8383];
const defaultRaftPorts = [7401, 7402, 7403];
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

  await pass("create database and seed source data", async () => {
    await eventually("seed database through node1", async () => {
      await createDatabase(nodes[0], database);
      const body = await pipeline(nodes[0], [
        executeRequest("create table if not exists placement_items (id integer primary key, value text not null)", false),
        executeRequest("delete from placement_items", false),
        executeRequest("insert into placement_items values (1, 'before-move')", false),
      ]);
      await closeBaton(nodes[0], body.baton);
    }, settleTimeoutMs);
    await expectValue(nodes[0], "before-move", settleTimeoutMs);
  });

  await pass("create target replication group", async () => {
    await createReplicationGroup(nodes[0], targetGroup);
  });

  await pass("target replication group loads on node1", async () => {
    await eventually(`runtime group ${targetGroup} loaded`, async () => {
      const runtime = await runtimeGroups(nodes[0]);
      const record = runtime.runtime_groups?.find((group) => group.group_id === targetGroup);
      if (!record?.loaded || !record?.ready_for_linearizable_reads) {
        throw new Error(`target runtime not ready: ${JSON.stringify(record)}`);
      }
    }, startupTimeoutMs);
  });

  await pass("start placement move", async () => {
    const operation = await moveDatabase(nodes[0], database, targetGroup);
    if (operation.status !== "running" || operation.phase !== "planned") {
      throw new Error(`unexpected move operation: ${JSON.stringify(operation)}`);
    }
  });

  await pass("reconcile placement move to completion", async () => {
    await eventually("placement move completed", async () => {
      await reconcilePlacement(nodes[0]);
      const operations = await placementOperations(nodes[0], database);
      const latest = operations.operations?.[0];
      if (latest?.status !== "completed" || latest?.phase !== "completed") {
        throw new Error(`move not complete yet: ${JSON.stringify(latest)}`);
      }
      if (!latest.source_fence_applied_index || !latest.target_clone_applied_index) {
        throw new Error(`move completed without watermarks: ${JSON.stringify(latest)}`);
      }
    }, moveTimeoutMs);
  });

  await pass("routing switches to target group and data is readable", async () => {
    await eventually("post-move placement visible", async () => {
      const placement = await databasePlacement(nodes[0], database);
      if (placement.group?.group_id !== targetGroup) {
        throw new Error(`expected ${targetGroup}, got ${JSON.stringify(placement.group)}`);
      }
    }, settleTimeoutMs);
    await expectValue(nodes[0], "before-move", settleTimeoutMs);
  });

  console.log(JSON.stringify({
    ok: true,
    project,
    database,
    targetGroup,
    checks,
    host_ports: hostPortsSummary(),
    endpoints: Object.fromEntries(nodes.map((node) => [`node${node.id}`, node.url])),
  }, null, 2));
} catch (error) {
  console.error(JSON.stringify({
    ok: false,
    project,
    database,
    targetGroup,
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

async function waitForHttp(node, timeoutMs) {
  await eventually(`node${node.id} HTTP ready`, async () => {
    const response = await fetch(`http://127.0.0.1:${portForNode(node)}/_orion/metrics/raft`);
    if (!response.ok) {
      throw new Error(`node${node.id} HTTP ${response.status}`);
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

async function createReplicationGroup(node, groupId) {
  const response = await fetch(`http://127.0.0.1:${portForNode(node)}/_orion/replication-groups`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      group_id: groupId,
      placement: { mode: "single_region" },
      members: [{ node_id: 1, role: "voter", priority: 0 }],
    }),
  });
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (response.status === 409) {
    return body;
  }
  if (!response.ok && response.status !== 201) {
    throw new Error(`node${node.id} create group HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function runtimeGroups(node) {
  const response = await fetch(`http://127.0.0.1:${portForNode(node)}/_orion/replication-groups/runtime`);
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} runtime groups HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function moveDatabase(node, name, targetGroupId) {
  const response = await fetch(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${name}/placement/move`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ target_group_id: targetGroupId }),
  });
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} move database HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function reconcilePlacement(node) {
  const response = await fetch(`http://127.0.0.1:${portForNode(node)}/_orion/placement/reconcile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{}",
  });
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} reconcile HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function placementOperations(node, name) {
  const response = await fetch(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${name}/placement/operations`);
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} operations HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function databasePlacement(node, name) {
  const response = await fetch(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${name}/placement`);
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} placement HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function expectValue(node, expected, timeoutMs) {
  await eventually(`node${node.id} reads placement value`, async () => {
    const rows = await query(node, "select value from placement_items where id = 1");
    const value = rows[0]?.[0];
    if (value !== expected) {
      throw new Error(`expected ${expected}, got ${JSON.stringify(value)}`);
    }
  }, timeoutMs);
}

async function query(node, sql, args = []) {
  const result = await pipeline(node, [executeRequest(sql, true, args)]);
  await closeBaton(node, result.baton);
  return (result.results?.[0]?.response?.result?.rows ?? []).map((row) =>
    row.map(formatHranaValue));
}

function executeRequest(sql, wantRows, args = []) {
  return {
    type: "execute",
    stmt: { sql, args: args.map(toHranaValue), want_rows: wantRows },
  };
}

async function pipeline(node, requests, baton = null) {
  const response = await fetch(`${node.url}/v2/pipeline`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-orion-read-policy": "strong",
    },
    body: JSON.stringify({
      requests,
      ...(baton ? { baton } : {}),
    }),
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

async function closeBaton(node, baton) {
  if (!baton) {
    return;
  }
  await pipeline(node, [{ type: "close" }], baton);
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

function portForNode(node) {
  return httpPorts[node.id - 1];
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
