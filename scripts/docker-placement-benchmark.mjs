#!/usr/bin/env node

import { execFile } from "node:child_process";
import { performance } from "node:perf_hooks";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const project = process.env.ORION_DOCKER_PROJECT ?? "orion-placement-benchmark";
const database = process.env.ORION_BENCH_DATABASE ?? "placement_benchmark";
const targetGroup = process.env.ORION_BENCH_TARGET_GROUP ?? "rg_placement_benchmark";
const keepRunning = process.env.ORION_DOCKER_KEEP_RUNNING === "1";
const buildTimeoutMs = numberEnv("ORION_DOCKER_BUILD_TIMEOUT_MS", 900_000);
const startupTimeoutMs = numberEnv("ORION_DOCKER_STARTUP_TIMEOUT_MS", 120_000);
const settleTimeoutMs = numberEnv("ORION_DOCKER_SETTLE_TIMEOUT_MS", 60_000);
const moveTimeoutMs = numberEnv("ORION_DOCKER_MOVE_TIMEOUT_MS", 300_000);
const requestTimeoutMs = numberEnv("ORION_DOCKER_REQUEST_TIMEOUT_MS", 30_000);
const pollMs = numberEnv("ORION_DOCKER_POLL_MS", 500);
const progressMs = numberEnv("ORION_DOCKER_PROGRESS_MS", 30_000);
const totalBytes = numberEnv("ORION_BENCH_TOTAL_BYTES", 64 * 1024 * 1024);
const blobBytes = numberEnv("ORION_BENCH_BLOB_BYTES", 128 * 1024);
const seedBatchRows = numberEnv("ORION_BENCH_SEED_BATCH_ROWS", 128);
const objectStoreMode = objectStoreModeEnv();

const defaultHttpPorts = [8481, 8482, 8483];
const defaultRaftPorts = [7501, 7502, 7503];
const httpPorts = configureDockerPorts("ORION_DOCKER_HTTP_PORT", defaultHttpPorts);
configureDockerPorts("ORION_DOCKER_RAFT_PORT", defaultRaftPorts);

const nodes = [
  { id: 1, service: "orion-node1", url: `http://127.0.0.1:${httpPorts[0]}/${database}` },
  { id: 2, service: "orion-node2", url: `http://127.0.0.1:${httpPorts[1]}/${database}` },
  { id: 3, service: "orion-node3", url: `http://127.0.0.1:${httpPorts[2]}/${database}` },
];

const checks = [];
let cleaningUp = false;

process.on("SIGINT", () => cleanup().finally(() => process.exit(130)));
process.on("SIGTERM", () => cleanup().finally(() => process.exit(143)));

try {
  logProgress(`starting placement benchmark project=${project} database=${database} objectStoreMode=${objectStoreMode} totalBytes=${totalBytes} blobBytes=${blobBytes}`);
  await ensureDocker();
  await pass("fresh data volumes", resetDataVolumes);
  await pass("start three-node docker cluster", startCluster);
  await pass("all libSQL endpoints accept connections", async () => {
    await Promise.all(nodes.map((node) => waitForHttp(node, startupTimeoutMs)));
  });
  await pass("cluster elects initial leader", async () => {
    await waitForLeader(nodes, startupTimeoutMs);
  });

  const rowCount = Math.max(1, Math.ceil(totalBytes / blobBytes));
  const expectedBytes = rowCount * blobBytes;
  const timings = {};

  await pass("create database and seed benchmark payload", async () => {
    const started = performance.now();
    await createDatabase(nodes[0], database);
    const setup = await pipeline(nodes[0], [
      executeRequest("create table if not exists placement_benchmark_payload (id integer primary key, payload blob not null)", false),
      executeRequest("delete from placement_benchmark_payload", false),
    ]);
    await closeBaton(nodes[0], setup.baton);
    for (let first = 1; first <= rowCount; first += seedBatchRows) {
      const count = Math.min(seedBatchRows, rowCount - first + 1);
      await insertZeroblobBatch(nodes[0], first, count, blobBytes);
      if (performance.now() - started > progressMs) {
        logProgress(`seeded rows through id ${first + count - 1}/${rowCount}`);
      }
    }
    await verifyPayload(nodes[0], rowCount, expectedBytes, settleTimeoutMs);
    timings.seed_ms = elapsedMs(started);
  });

  await pass("create multi-voter target replication group", async () => {
    await createReplicationGroup(nodes[0], targetGroup, nodes);
    await waitForRuntimeGroupReady(targetGroup, nodes, startupTimeoutMs);
  });

  const placementBefore = await aggregatePlacementMetrics(nodes);
  const largePayloadBefore = await aggregateLargePayloadMetrics(nodes);

  await pass("move benchmark database and measure transfer", async () => {
    const started = performance.now();
    const operation = await moveDatabase(nodes[0], database, targetGroup);
    if (operation.status !== "running" || operation.phase !== "planned") {
      throw new Error(`unexpected move operation: ${JSON.stringify(operation)}`);
    }
    await reconcileMoveToCompletion(database);
    timings.move_ms = elapsedMs(started);
  });

  await pass("verify target placement and payload", async () => {
    await eventually("post-move placement visible", async () => {
      const placement = await databasePlacement(nodes[0], database);
      if (placement.group?.group_id !== targetGroup) {
        throw new Error(`expected ${targetGroup}, got ${JSON.stringify(placement.group)}`);
      }
    }, settleTimeoutMs);
    await verifyPayload(nodes[0], rowCount, expectedBytes, settleTimeoutMs);
    await verifyPayload(nodes[1], rowCount, expectedBytes, settleTimeoutMs);
    await verifyPayload(nodes[2], rowCount, expectedBytes, settleTimeoutMs);
  });

  const placementAfter = await aggregatePlacementMetrics(nodes);
  const largePayloadAfter = await aggregateLargePayloadMetrics(nodes);
  const placementDelta = diffMetricObject(placementAfter.placement_move_transfer, placementBefore.placement_move_transfer);
  const placementVoterDelta = diffMetricObject(placementAfter.placement_transfer_voters, placementBefore.placement_transfer_voters);
  const largePayloadDelta = diffMetricObject(largePayloadAfter, largePayloadBefore);

  if (numericMetric(placementDelta.backup_attempts) !== 0) {
    throw new Error(`benchmark unexpectedly used backup fallback: ${JSON.stringify(placementDelta)}`);
  }
  const successfulTransfers = numericMetric(placementDelta.page_delta_successes)
    + numericMetric(placementDelta.checkpoint_successes);
  if (successfulTransfers < 1) {
    throw new Error(`benchmark did not observe object-native transfer: ${JSON.stringify(placementDelta)}`);
  }
  if (numericMetric(placementDelta.page_delta_failures) !== 0
    || numericMetric(placementDelta.checkpoint_failures) !== 0) {
    throw new Error(`benchmark observed transfer failures: ${JSON.stringify(placementDelta)}`);
  }
  if (numericMetric(placementDelta.checkpoint_successes) < 1) {
    throw new Error(`benchmark did not observe checkpoint placement transfer: ${JSON.stringify(placementDelta)}`);
  }
  if (numericMetric(placementDelta.page_delta_attempts) !== 0
    || numericMetric(placementDelta.page_delta_successes) !== 0) {
    throw new Error(`benchmark unexpectedly used page-delta placement transfer: ${JSON.stringify(placementDelta)}`);
  }
  if (numericMetric(placementVoterDelta.ready) < 1 || numericMetric(placementVoterDelta.failed) !== 0) {
    throw new Error(`benchmark did not observe successful transfer voter readiness: ${JSON.stringify(placementVoterDelta)}`);
  }
  if (objectStoreMode === "regional") {
    assertRegionalObjectStoreTransfer(placementDelta, placementVoterDelta);
  }

  console.log(JSON.stringify({
    ok: true,
    project,
    database,
    target_group: targetGroup,
    object_store_mode: objectStoreMode,
    checks,
    workload: {
      rows: rowCount,
      blob_bytes: blobBytes,
      expected_bytes: expectedBytes,
    },
    timings,
    throughput: {
      seed_mib_per_sec: mibPerSecond(expectedBytes, timings.seed_ms),
      move_mib_per_sec: mibPerSecond(expectedBytes, timings.move_ms),
    },
    metrics_delta: {
      placement_move_transfer: placementDelta,
      placement_transfer_voters: placementVoterDelta,
      large_payload: largePayloadDelta,
    },
    metrics_after: {
      placement: placementAfter,
      large_payload: largePayloadAfter,
    },
    host_ports: hostPortsSummary(),
    endpoints: Object.fromEntries(nodes.map((node) => [`node${node.id}`, node.url])),
  }, null, 2));
} catch (error) {
  console.error(JSON.stringify({
    ok: false,
    project,
    database,
    target_group: targetGroup,
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
  const started = performance.now();
  logProgress(`start: ${name}`);
  try {
    await fn();
    checks.push(name);
    logProgress(`ok: ${name} (${elapsedMs(started)}ms)`);
  } catch (error) {
    logProgress(`failed: ${name} (${elapsedMs(started)}ms): ${error?.message ?? error}`);
    throw error;
  }
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
    `${project}_orion-object-store-region-us-east-1`,
    `${project}_orion-object-store-region-eastus`,
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
    const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/metrics/raft`);
    if (!response.ok) {
      throw new Error(`node${node.id} HTTP ${response.status}`);
    }
  }, timeoutMs);
}

async function waitForLeader(candidateNodes, timeoutMs) {
  return eventually("known raft leader", async () => {
    const samples = (await Promise.all(candidateNodes.map(async (node) => ({
      node,
      snapshot: await raftMetrics(node).catch(() => null),
    })))).filter((sample) => sample.snapshot);
    const liveNodeIds = new Set(samples.map((sample) => sample.node.id));
    for (const sample of samples) {
      for (const entry of sample.snapshot.raft_metrics ?? []) {
        const metrics = entry.metrics ?? {};
        if (liveNodeIds.has(metrics.node_id) && metrics.state === "Leader" && metrics.current_leader === metrics.node_id) {
          return metrics.node_id;
        }
      }
    }
    throw new Error("leader is not known yet");
  }, timeoutMs);
}

async function raftMetrics(node) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/metrics/raft`);
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

async function createReplicationGroup(node, groupId, memberNodes) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/replication-groups`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      group_id: groupId,
      placement: { mode: "single_region" },
      members: memberNodes.map((candidate, index) => ({
        node_id: candidate.id,
        role: "voter",
        priority: index,
      })),
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

async function insertZeroblobBatch(node, firstId, count, bytesPerRow) {
  await execute(
    node,
    `with recursive seq(i) as (
       select 0
       union all
       select i + 1 from seq where i + 1 < ${count}
     )
     insert into placement_benchmark_payload(id, payload)
     select ${firstId} + i, zeroblob(${bytesPerRow}) from seq`,
  );
}

async function verifyPayload(node, expectedRows, expectedBytes, timeoutMs) {
  await eventually(`node${node.id} sees benchmark payload`, async () => {
    const rows = await query(node, "select count(*), coalesce(sum(length(payload)), 0) from placement_benchmark_payload");
    const count = Number(rows[0]?.[0] ?? 0);
    const bytes = Number(rows[0]?.[1] ?? 0);
    if (count !== expectedRows || bytes !== expectedBytes) {
      throw new Error(`expected rows=${expectedRows} bytes=${expectedBytes}, got rows=${count} bytes=${bytes}`);
    }
  }, timeoutMs);
}

async function reconcileMoveToCompletion(name) {
  await eventually(`placement move ${name} completes`, async () => {
    const reconcileNode = await placementReconcileNode(name);
    await reconcilePlacement(reconcileNode);
    const latest = await latestPlacementOperation(reconcileNode, name);
    if (latest.status !== "completed" || latest.phase !== "completed") {
      throw new Error(`move not complete yet: ${JSON.stringify(latest)}`);
    }
    if (!latest.source_fence_applied_index || !latest.target_clone_applied_index) {
      throw new Error(`move completed without watermarks: ${JSON.stringify(latest)}`);
    }
  }, moveTimeoutMs);
}

async function placementReconcileNode(name) {
  const operations = await placementOperations(nodes[0], name).catch(() => null);
  const latest = operations?.operations?.[0];
  if (latest?.status === "running"
    && ["cloning", "catching_up", "switching"].includes(latest.phase)) {
    const leaderId = await waitForRuntimeGroupLeader(latest.target_group_id, nodes, startupTimeoutMs);
    return serviceForNode(leaderId);
  }
  return nodes[0];
}

async function waitForRuntimeGroupReady(groupId, candidateNodes, timeoutMs) {
  await eventually(`runtime group ${groupId} ready`, async () => {
    await Promise.all(candidateNodes.map(async (node) => {
      const runtime = await runtimeGroups(node);
      const record = runtime.runtime_groups?.find((group) => group.group_id === groupId);
      if (!record?.loaded || !record?.ready_for_linearizable_reads) {
        throw new Error(`node${node.id} runtime not ready: ${JSON.stringify(record)}`);
      }
    }));
  }, timeoutMs);
}

async function waitForRuntimeGroupLeader(groupId, candidateNodes, timeoutMs) {
  return eventually(`runtime group ${groupId} leader`, async () => {
    const samples = (await Promise.all(candidateNodes.map(async (node) => ({
      node,
      snapshot: await runtimeGroups(node).catch(() => null),
    })))).filter((sample) => sample.snapshot);
    const liveNodeIds = new Set(samples.map((sample) => sample.node.id));
    for (const sample of samples) {
      const record = sample.snapshot.runtime_groups?.find((group) => group.group_id === groupId);
      const leader = record?.current_leader;
      if (leader && liveNodeIds.has(leader)) {
        return leader;
      }
    }
    throw new Error(`leader for runtime group ${groupId} is not known yet`);
  }, timeoutMs);
}

async function runtimeGroups(node) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/replication-groups/runtime`);
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} runtime groups HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function moveDatabase(node, name, targetGroupId) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${name}/placement/move`, {
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
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/placement/reconcile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{}",
  }, moveTimeoutMs);
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} reconcile HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function latestPlacementOperation(node, name) {
  const operations = await placementOperations(node, name);
  const latest = operations.operations?.[0];
  if (!latest) {
    throw new Error(`no placement operation found for ${name}`);
  }
  return latest;
}

async function placementOperations(node, name) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${name}/placement/operations`);
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} operations HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function databasePlacement(node, name) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${name}/placement`);
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} placement HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function placementMetrics(node) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/metrics/placement`);
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} placement metrics HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function aggregatePlacementMetrics(candidateNodes) {
  const samples = await Promise.all(candidateNodes.map(async (node) => ({
    node_id: node.id,
    metrics: await placementMetrics(node),
  })));
  return {
    checked_at_ms: Date.now(),
    nodes_sampled: samples.map((sample) => sample.node_id),
    placement_move_transfer: sumMetricSection(samples, "placement_move_transfer"),
    placement_transfer_voters: sumMetricSection(samples, "placement_transfer_voters"),
  };
}

async function aggregateLargePayloadMetrics(candidateNodes) {
  const totals = {};
  for (const node of candidateNodes) {
    const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/metrics/large-payload`);
    const body = await response.json().catch(async () => ({ error: await response.text() }));
    if (!response.ok) {
      throw new Error(`node${node.id} large payload metrics HTTP ${response.status}: ${JSON.stringify(body)}`);
    }
    for (const row of body.large_payload_metrics ?? []) {
      for (const [key, value] of Object.entries(row.metrics ?? {})) {
        totals[key] = numericMetric(totals[key]) + numericMetric(value);
      }
    }
  }
  return totals;
}

function sumMetricSection(samples, sectionName) {
  const totals = {};
  for (const sample of samples) {
    const section = sample.metrics?.[sectionName];
    if (!section || typeof section !== "object") {
      continue;
    }
    for (const [key, value] of Object.entries(section)) {
      totals[key] = numericMetric(totals[key]) + numericMetric(value);
    }
  }
  return totals;
}

function diffMetricObject(after, before) {
  const keys = new Set([...Object.keys(after ?? {}), ...Object.keys(before ?? {})]);
  const diff = {};
  for (const key of keys) {
    diff[key] = numericMetric(after?.[key]) - numericMetric(before?.[key]);
  }
  return diff;
}

async function execute(node, sql, args = []) {
  const result = await pipeline(node, [executeRequest(sql, false, args)]);
  await closeBaton(node, result.baton);
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
  const response = await fetchWithTimeout(`${node.url}/v2/pipeline`, {
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

function serviceForNode(nodeId) {
  const node = nodes.find((candidate) => candidate.id === nodeId);
  if (!node) {
    throw new Error(`unknown node id ${nodeId}`);
  }
  return node;
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

async function fetchWithTimeout(url, options = {}, timeoutMs = requestTimeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function printDiagnostics() {
  await compose(["ps", "-a"], { allowFailure: true });
  await compose(["logs", "--tail=200"], { allowFailure: true });
}

async function compose(args, options = {}) {
  return run(
    "docker",
    ["compose", "--project-name", project, ...args],
    {
      timeoutMs: options.timeoutMs,
      allowFailure: options.allowFailure,
      env: {
        ...process.env,
        ORION_DOCKER_HTTP_PORT_1: String(httpPorts[0]),
        ORION_DOCKER_HTTP_PORT_2: String(httpPorts[1]),
        ORION_DOCKER_HTTP_PORT_3: String(httpPorts[2]),
        ...dockerObjectStoreEnv(),
      },
    },
  );
}

async function run(command, args, options = {}) {
  try {
    return await execFileAsync(command, args, {
      timeout: options.timeoutMs ?? 120_000,
      maxBuffer: 32 * 1024 * 1024,
      env: options.env ?? process.env,
    });
  } catch (error) {
    if (options.allowFailure) {
      return { stdout: error.stdout ?? "", stderr: error.stderr ?? "" };
    }
    error.message = `${command} ${args.join(" ")} failed: ${error.message}\nstdout:\n${error.stdout ?? ""}\nstderr:\n${error.stderr ?? ""}`;
    throw error;
  }
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
  return Object.fromEntries(nodes.map((node, index) => [
    `node${node.id}`,
    {
      http: httpPorts[index],
      raft: Number(process.env[`ORION_DOCKER_RAFT_PORT_${index + 1}`]),
    },
  ]));
}

function numberEnv(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number, got ${raw}`);
  }
  return Math.floor(value);
}

function objectStoreModeEnv() {
  const raw = process.env.ORION_DOCKER_OBJECT_STORE_MODE
    ?? (process.env.ORION_DOCKER_REGIONAL_OBJECT_STORE === "1" ? "regional" : "per_node");
  const normalized = raw.trim().toLowerCase().replaceAll("-", "_");
  if (!["per_node", "regional"].includes(normalized)) {
    throw new Error(`ORION_DOCKER_OBJECT_STORE_MODE must be per_node or regional, got ${raw}`);
  }
  return normalized;
}

function dockerObjectStoreEnv() {
  if (objectStoreMode !== "regional") {
    return {};
  }
  return {
    ORION_DOCKER_NODE1_CONFIG: "/workspace/docker/cluster-regional-object-store/node1.yaml",
    ORION_DOCKER_NODE2_CONFIG: "/workspace/docker/cluster-regional-object-store/node2.yaml",
    ORION_DOCKER_NODE3_CONFIG: "/workspace/docker/cluster-regional-object-store/node3.yaml",
    ORION_DOCKER_OBJECT_STORE_VOLUME_1: "orion-object-store-region-us-east-1",
    ORION_DOCKER_OBJECT_STORE_VOLUME_2: "orion-object-store-region-us-east-1",
    ORION_DOCKER_OBJECT_STORE_VOLUME_3: "orion-object-store-region-eastus",
  };
}

function assertRegionalObjectStoreTransfer(placementDelta, placementVoterDelta) {
  const objectsSeen = numericMetric(placementDelta.checkpoint_objects_seen);
  const objectsCopied = numericMetric(placementDelta.checkpoint_objects_copied);
  const objectsReused = numericMetric(placementDelta.checkpoint_objects_reused);
  const bytesSeen = numericMetric(placementDelta.checkpoint_bytes_seen);
  const bytesCopied = numericMetric(placementDelta.checkpoint_bytes_copied);

  if (objectsSeen < 1 || bytesSeen < 1) {
    throw new Error(`regional benchmark did not observe checkpoint objects: ${JSON.stringify(placementDelta)}`);
  }
  if (objectsReused < 1) {
    throw new Error(`regional benchmark did not reuse same-region checkpoint objects: ${JSON.stringify(placementDelta)}`);
  }
  if (objectsCopied !== 0 || bytesCopied !== 0) {
    throw new Error(`regional benchmark copied checkpoint objects even though every target voter has a regional source: ${JSON.stringify(placementDelta)}`);
  }
  if (objectsReused !== objectsSeen) {
    throw new Error(`regional benchmark did not reuse every checkpoint object: ${JSON.stringify(placementDelta)}`);
  }

  const voterObjectsSeen = numericMetric(placementVoterDelta.checkpoint_objects_seen);
  const voterObjectsCopied = numericMetric(placementVoterDelta.checkpoint_objects_copied);
  const voterObjectsReused = numericMetric(placementVoterDelta.checkpoint_objects_reused);
  if (voterObjectsSeen < 1
    || voterObjectsReused < 1
    || voterObjectsCopied !== 0
    || voterObjectsReused !== voterObjectsSeen) {
    throw new Error(`regional benchmark voter metrics do not show full regional reuse: ${JSON.stringify(placementVoterDelta)}`);
  }
}

function numericMetric(value) {
  const number = Number(value ?? 0);
  return Number.isFinite(number) ? number : 0;
}

function mibPerSecond(bytes, ms) {
  if (!ms) {
    return null;
  }
  return Number(((bytes / 1024 / 1024) / (ms / 1000)).toFixed(3));
}

function elapsedMs(started) {
  return Number((performance.now() - started).toFixed(3));
}

function logProgress(message) {
  console.error(`[placement-benchmark] ${new Date().toISOString()} ${message}`);
}
