#!/usr/bin/env node

import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const project = process.env.ORION_DOCKER_PROJECT ?? "orion-placement-chaos";
const keepRunning = process.env.ORION_DOCKER_KEEP_RUNNING === "1";
const buildTimeoutMs = numberEnv("ORION_DOCKER_BUILD_TIMEOUT_MS", 900_000);
const startupTimeoutMs = numberEnv("ORION_DOCKER_STARTUP_TIMEOUT_MS", 120_000);
const settleTimeoutMs = numberEnv("ORION_DOCKER_SETTLE_TIMEOUT_MS", 60_000);
const moveTimeoutMs = numberEnv("ORION_DOCKER_MOVE_TIMEOUT_MS", 180_000);
const requestTimeoutMs = numberEnv("ORION_DOCKER_REQUEST_TIMEOUT_MS", 20_000);
const pollMs = numberEnv("ORION_DOCKER_POLL_MS", 500);
const progressMs = numberEnv("ORION_DOCKER_PROGRESS_MS", 30_000);
const killSignal = process.env.ORION_DOCKER_KILL_SIGNAL ?? "SIGKILL";

const defaultHttpPorts = [8181, 8182, 8183];
const defaultRaftPorts = [7201, 7202, 7203];
const httpPorts = configureDockerPorts("ORION_DOCKER_HTTP_PORT", defaultHttpPorts);
configureDockerPorts("ORION_DOCKER_RAFT_PORT", defaultRaftPorts);

const nodes = [
  { id: 1, service: "orion-node1" },
  { id: 2, service: "orion-node2" },
  { id: 3, service: "orion-node3" },
];

const phaseAdvanceCounts = new Map([
  ["planned", 0],
  ["fenced", 1],
  ["cloning", 2],
  ["catching_up", 3],
  ["switching", 4],
]);
const scenarioNames = [
  ...phaseAdvanceCounts.keys(),
  "leader_crash_survivor",
  "dead_source_standby_promotion",
  "automatic_standby_refresh",
];

const checks = [];
let automaticStandbyMetrics = null;
let observedPlacementMetrics = null;
let cleaningUp = false;

process.on("SIGINT", () => cleanup().finally(() => process.exit(130)));
process.on("SIGTERM", () => cleanup().finally(() => process.exit(143)));

try {
  logProgress(`starting placement chaos project=${project} keepRunning=${keepRunning} timeouts=${JSON.stringify({
    buildTimeoutMs,
    startupTimeoutMs,
    settleTimeoutMs,
    moveTimeoutMs,
    requestTimeoutMs,
    pollMs,
    progressMs,
  })}`);
  await ensureDocker();
  await pass("fresh data volumes", resetDataVolumes);
  await pass("start three-node docker cluster", startCluster);
  await pass("all libSQL endpoints accept connections", async () => {
    await Promise.all(nodes.map((node) => waitForHttp(node, "_orion", startupTimeoutMs)));
  });
  await pass("cluster elects initial leader", async () => {
    await waitForLeader(nodes, startupTimeoutMs);
  });

  await pass("dead source standby promotion keeps database available", async () => {
    await runDeadSourceStandbyPromotionScenario();
  });

  await pass("automatic standby refresh keeps failover target warm", async () => {
    await runAutomaticStandbyRefreshScenario();
  });

  for (const phase of phaseAdvanceCounts.keys()) {
    await pass(`crash/restart resumes placement move from ${phase}`, async () => {
      await runRestartResumeScenario(phase);
    });
  }

  await pass("leader crash during fenced move completes on surviving quorum", async () => {
    await runLeaderCrashSurvivorScenario();
  });

  await pass("node logs contain no storage corruption errors", async () => {
    await assertNoForbiddenLogs();
  });

  await pass("placement move metrics show object-native transfer without backup fallback", async () => {
    observedPlacementMetrics = await aggregatePlacementMetrics(nodes);
    assertPlacementMoveObjectNativeMetrics(observedPlacementMetrics);
  });

  console.log(JSON.stringify({
    ok: true,
    project,
    checks,
    scenarios: scenarioNames,
    ops_knobs: {
      keep_running: keepRunning,
      kill_signal: killSignal,
      host_ports: hostPortsSummary(),
      startup_timeout_ms: startupTimeoutMs,
      settle_timeout_ms: settleTimeoutMs,
      move_timeout_ms: moveTimeoutMs,
      request_timeout_ms: requestTimeoutMs,
      poll_ms: pollMs,
      progress_ms: progressMs,
    },
    observed_placement_metrics: {
      automatic_standby: automaticStandbyMetrics,
      aggregate: observedPlacementMetrics,
    },
    endpoints: Object.fromEntries(nodes.map((node) => [`node${node.id}`, nodeUrl(node, "_orion")])),
  }, null, 2));
} catch (error) {
  console.error(JSON.stringify({
    ok: false,
    project,
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

async function runRestartResumeScenario(phase) {
  const safePhase = phase.replaceAll("_", "");
  const database = `placement_${safePhase}`;
  const targetGroup = `rg_${safePhase}`;
  const value = `value-${phase.replaceAll("_", "-")}`;
  await createScenario(database, targetGroup, value);
  await startMoveAndAdvanceToPhase(database, targetGroup, phase);
  await expectWriteRejected(nodes[0], database);

  const leader = serviceForNode(await waitForLeader(nodes, settleTimeoutMs));
  await docker(["kill", "--signal", killSignal, containerName(leader.service)]);
  await waitForHttpDown(leader, settleTimeoutMs);
  await startNode(leader);
  await waitForHttp(leader, "_orion", startupTimeoutMs);
  await waitForLeader(nodes, startupTimeoutMs);
  await waitForRuntimeGroupReady("rg_default", nodes, startupTimeoutMs);

  await reconcileMoveToCompletion(database);
  await expectValue(nodes[0], database, value, settleTimeoutMs);
  await expectPlacement(database, targetGroup, settleTimeoutMs);
}

async function runLeaderCrashSurvivorScenario() {
  const database = "placement_leader_failover";
  const targetGroup = "rg_leader_failover";
  await waitForRuntimeGroupReady("rg_default", nodes, startupTimeoutMs);
  await createScenario(database, targetGroup, "value-leader-failover");
  await startMoveAndAdvanceToPhase(database, targetGroup, "fenced");
  await expectWriteRejected(nodes[0], database);

  const leader = serviceForNode(await waitForLeader(nodes, settleTimeoutMs));
  await docker(["kill", "--signal", killSignal, containerName(leader.service)]);
  await waitForHttpDown(leader, settleTimeoutMs);
  const survivors = nodes.filter((node) => node.id !== leader.id);
  const survivorLeaderId = await waitForLeader(survivors, startupTimeoutMs);
  const survivorLeader = serviceForNode(survivorLeaderId);

  await reconcileMoveToCompletion(database, survivorLeader);
  await expectValue(survivorLeader, database, "value-leader-failover", settleTimeoutMs);
  await expectPlacement(database, targetGroup, settleTimeoutMs, survivorLeader);

  await startNode(leader);
  await waitForHttp(leader, "_orion", startupTimeoutMs);
  await waitForLeader(nodes, startupTimeoutMs);
  await expectValue(leader, database, "value-leader-failover", startupTimeoutMs);
}

async function runDeadSourceStandbyPromotionScenario() {
  const database = "placement_dead_source_standby";
  const sourceGroup = "rg_dead_source_standby_source";
  const targetGroup = "rg_dead_source_standby_target";
  const sourceNode = nodes[0];
  const targetMembers = nodes.filter((node) => node.id !== sourceNode.id);
  let promoter = targetMembers[0];
  const sourceValue = "value-before-source-death";
  const promotedValue = "value-after-standby-promotion";

  await createScenario(database, sourceGroup, sourceValue, [sourceNode]);
  await createReplicationGroup(sourceNode, targetGroup, targetMembers);
  await waitForRuntimeGroupReady(targetGroup, targetMembers, startupTimeoutMs);
  promoter = serviceForNode(await waitForRuntimeGroupLeader(targetGroup, targetMembers, startupTimeoutMs));
  const otherTarget = targetMembers.find((node) => node.id !== promoter.id) ?? promoter;

  await startMoveAndAdvanceToPhase(database, sourceGroup, "planned");
  await reconcileMoveToCompletion(database);
  await expectPlacement(database, sourceGroup, settleTimeoutMs);
  await execute(sourceNode, database, "update placement_items set value = ? where id = 1", [sourceValue]);
  await expectValue(sourceNode, database, sourceValue, settleTimeoutMs);

  const refresh = await refreshStandbyOrWait(promoter, database, sourceGroup, targetGroup);
  assertStandbyRecord(refresh.standby, database, sourceGroup, targetGroup);
  await expectStandby(sourceNode, database, targetGroup, sourceGroup, settleTimeoutMs);
  await expectStandby(promoter, database, targetGroup, sourceGroup, settleTimeoutMs);

  await docker(["kill", "--signal", killSignal, containerName(sourceNode.service)]);
  await waitForHttpDown(sourceNode, settleTimeoutMs);
  const survivors = nodes.filter((node) => node.id !== sourceNode.id);
  await waitForLeader(survivors, startupTimeoutMs);
  await waitForRuntimeGroupLeader("rg_default", survivors, startupTimeoutMs);
  await waitForRuntimeGroupReady("rg_default", survivors, startupTimeoutMs);

  const promotion = await promoteStandbyWithTransientRetry(promoter, database, targetGroup, startupTimeoutMs);
  assertStandbyRecord(promotion.standby, database, sourceGroup, targetGroup);
  if (promotion.database?.replication_group_id !== targetGroup) {
    throw new Error(`promotion did not assign ${database} to ${targetGroup}: ${JSON.stringify(promotion)}`);
  }
  await expectPlacement(database, targetGroup, settleTimeoutMs, promoter);
  await expectNoStandbys(promoter, database, settleTimeoutMs);
  await expectValue(promoter, database, sourceValue, settleTimeoutMs);

  await execute(promoter, database, "update placement_items set value = ? where id = 1", [promotedValue]);
  await expectValue(promoter, database, promotedValue, settleTimeoutMs);
  await expectValue(otherTarget, database, promotedValue, settleTimeoutMs);

  await startNode(sourceNode);
  await waitForHttp(sourceNode, "_orion", startupTimeoutMs);
  await waitForLeader(nodes, startupTimeoutMs);
  await expectPlacement(database, targetGroup, startupTimeoutMs, sourceNode);
  await expectPlacement(database, targetGroup, settleTimeoutMs, promoter);
  await expectValue(promoter, database, promotedValue, startupTimeoutMs);
}

async function runAutomaticStandbyRefreshScenario() {
  const database = "placement_automatic_standby_refresh";
  const sourceGroup = "rg_automatic_standby_source";
  const targetGroup = "rg_automatic_standby_target";
  const sourceMembers = [nodes[0]];
  const targetMembers = nodes.filter((node) => node.id !== nodes[0].id);
  const value = "value-for-automatic-standby-refresh";

  await createAndSeedDatabase(database, value);
  await createReplicationGroup(nodes[0], sourceGroup, sourceMembers, automaticFailoverPlacement([targetGroup]));
  await createReplicationGroup(nodes[0], targetGroup, targetMembers, automaticFailoverPlacement());
  await waitForRuntimeGroupReady(sourceGroup, sourceMembers, startupTimeoutMs);
  await waitForRuntimeGroupReady(targetGroup, targetMembers, startupTimeoutMs);
  const targetLeader = serviceForNode(await waitForRuntimeGroupLeader(targetGroup, targetMembers, startupTimeoutMs));

  await startMoveAndAdvanceToPhase(database, sourceGroup, "planned");
  await reconcileMoveToCompletion(database);
  await expectPlacement(database, sourceGroup, settleTimeoutMs);
  await expectValue(nodes[0], database, value, settleTimeoutMs);

  await reconcileStandbyRefresh(targetLeader);
  await expectStandby(targetLeader, database, targetGroup, sourceGroup, settleTimeoutMs);

  automaticStandbyMetrics = await placementMetrics(targetLeader);
  if ((automaticStandbyMetrics.standbys_total ?? 0) < 1 || (automaticStandbyMetrics.standbys_promotable ?? 0) < 1) {
    throw new Error(`placement metrics did not observe warm standby: ${JSON.stringify(automaticStandbyMetrics)}`);
  }
  if ((automaticStandbyMetrics.standbys_errors ?? 0) !== 0) {
    throw new Error(`placement metrics reported standby errors: ${JSON.stringify(automaticStandbyMetrics)}`);
  }
  assertStandbyCheckpointMetricsIfAvailable(automaticStandbyMetrics);
}

async function createScenario(database, targetGroup, value, targetMembers = nodes) {
  await createAndSeedDatabase(database, value);
  await createReplicationGroup(nodes[0], targetGroup, targetMembers);
  await waitForRuntimeGroupReady(targetGroup, targetMembers, startupTimeoutMs);
}

async function createAndSeedDatabase(database, value) {
  await eventually(`create and seed ${database}`, async () => {
    await createDatabase(nodes[0], database);
    const body = await pipeline(nodes[0], database, [
      executeRequest("create table if not exists placement_items (id integer primary key, value text not null)", false),
      executeRequest("delete from placement_items", false),
      executeRequest("insert into placement_items values (1, ?)", false, [value]),
    ]);
    await closeBaton(nodes[0], database, body.baton);
  }, settleTimeoutMs);
  await expectValue(nodes[0], database, value, settleTimeoutMs);
}

async function startMoveAndAdvanceToPhase(database, targetGroup, phase) {
  const operation = await moveDatabase(nodes[0], database, targetGroup);
  if (operation.status !== "running" || operation.phase !== "planned") {
    throw new Error(`unexpected move operation: ${JSON.stringify(operation)}`);
  }
  const advanceCount = phaseAdvanceCounts.get(phase);
  for (let i = 0; i < advanceCount; i += 1) {
    await reconcilePlacementForDatabase(database, nodes[0]);
  }
  await eventually(`move ${database} reaches phase ${phase}`, async () => {
    const latest = await latestPlacementOperation(nodes[0], database);
    if (latest.status !== "running" || latest.phase !== phase) {
      throw new Error(`expected running/${phase}, got ${JSON.stringify(latest)}`);
    }
  }, settleTimeoutMs);
}

async function reconcileMoveToCompletion(database, preferredNode = nodes[0]) {
  await eventually(`placement move ${database} completes`, async () => {
    await reconcilePlacementForDatabase(database, preferredNode);
    const latest = await latestPlacementOperation(preferredNode, database);
    if (latest.status !== "completed" || latest.phase !== "completed") {
      throw new Error(`move not complete yet: ${JSON.stringify(latest)}`);
    }
    if (!latest.source_fence_applied_index || !latest.target_clone_applied_index) {
      throw new Error(`move completed without watermarks: ${JSON.stringify(latest)}`);
    }
  }, moveTimeoutMs);
}

async function reconcilePlacementForDatabase(database, preferredNode = nodes[0]) {
  const reconcileNode = await placementReconcileNode(database, preferredNode);
  return reconcilePlacement(reconcileNode);
}

async function placementReconcileNode(database, preferredNode = nodes[0]) {
  const operations = await placementOperations(preferredNode, database).catch(() => null);
  const latest = operations?.operations?.[0];
  if (latest?.status === "running"
    && ["cloning", "catching_up", "switching"].includes(latest.phase)) {
    const leaderId = await waitForRuntimeGroupLeader(latest.target_group_id, nodes, startupTimeoutMs);
    return serviceForNode(leaderId);
  }
  return preferredNode;
}

async function pass(name, fn) {
  const started = Date.now();
  logProgress(`start: ${name}`);
  try {
    await fn();
    checks.push(name);
    logProgress(`ok: ${name} (${Date.now() - started}ms)`);
  } catch (error) {
    logProgress(`failed: ${name} (${Date.now() - started}ms): ${error?.message ?? error}`);
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

async function waitForHttp(node, database, timeoutMs) {
  await eventually(`node${node.id} HTTP ready`, async () => {
    const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/metrics/raft`);
    if (!response.ok) {
      throw new Error(`node${node.id} HTTP ${response.status}`);
    }
  }, timeoutMs);
}

async function waitForHttpDown(node, timeoutMs) {
  await eventually(`node${node.id} HTTP stopped`, async () => {
    try {
      const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/metrics/raft`, {}, Math.min(requestTimeoutMs, 2_000));
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
    const samples = (await Promise.all(candidateNodes.map(async (node) => ({
      node,
      snapshot: await raftMetrics(node).catch(() => null),
    })))).filter((sample) => sample.snapshot);
    const liveNodeIds = new Set(samples.map((sample) => sample.node.id));
    for (const sample of samples) {
      for (const entry of sample.snapshot.raft_metrics ?? []) {
        const metrics = entry.metrics ?? {};
        if (
          liveNodeIds.has(metrics.node_id)
          && metrics.state === "Leader"
          && metrics.current_leader === metrics.node_id
        ) {
          return metrics.node_id;
        }
      }
    }
    for (const sample of samples) {
      for (const entry of sample.snapshot.raft_metrics ?? []) {
        const leader = entry.metrics?.current_leader;
        if (liveNodeIds.has(leader)) {
          return leader;
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

async function createReplicationGroup(node, groupId, memberNodes = nodes, placement = { mode: "manual" }) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/replication-groups`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      group_id: groupId,
      placement,
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

function automaticFailoverPlacement(standbyTargets = []) {
  return {
    mode: "manual",
    failover: {
      automatic: true,
      promote_after_ms: 60_000,
      standby_targets: standbyTargets,
    },
  };
}

async function reconcileStandbyRefresh(node) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/placement/standby/reconcile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{}",
  });
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} standby reconcile HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  if ((body.errors ?? 0) !== 0) {
    throw new Error(`standby reconcile reported errors: ${JSON.stringify(body)}`);
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
    standby_checkpoint: sumMetricSection(samples, "standby_checkpoint"),
    standby_page_delta: sumMetricSection(samples, "standby_page_delta"),
  };
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

async function refreshStandby(node, database, targetGroupId) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${database}/placement/standby`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ target_group_id: targetGroupId }),
  });
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} refresh standby HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function refreshStandbyOrWait(node, database, sourceGroupId, targetGroupId) {
  try {
    return await refreshStandby(node, database, targetGroupId);
  } catch (error) {
    if (!String(error?.message ?? error).includes("already running")) {
      throw error;
    }
    await expectStandby(node, database, targetGroupId, sourceGroupId, settleTimeoutMs);
    const standbys = await listStandbys(node, database);
    const standby = standbys.find((record) => record.target_group_id === targetGroupId);
    return { standby };
  }
}

async function promoteStandby(node, database, targetGroupId) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${database}/placement/promote`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      target_group_id: targetGroupId,
      max_staleness_ms: settleTimeoutMs,
    }),
  });
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} promote standby HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function promoteStandbyWithTransientRetry(node, database, targetGroupId, timeoutMs) {
  const started = Date.now();
  let lastError;
  while (Date.now() - started < timeoutMs) {
    try {
      return await promoteStandby(node, database, targetGroupId);
    } catch (error) {
      lastError = error;
      if (!isTransientPlacementCommitError(error)) {
        throw error;
      }
      await sleep(pollMs);
    }
  }
  throw new Error(
    `node${node.id} promote standby timed out after transient failures: ${lastError?.message ?? "unknown error"}`,
  );
}

function isTransientPlacementCommitError(error) {
  const message = String(error?.message ?? error);
  return message.includes("disk I/O error")
    || message.includes("is not ready for linearizable reads")
    || message.includes("leader")
    || message.includes("transport error");
}

async function listStandbys(node, database) {
  const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${database}/placement/standbys`);
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) {
    throw new Error(`node${node.id} list standbys HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body.standbys ?? [];
}

async function waitForRuntimeGroupReady(groupId, candidateNodes, timeoutMs) {
  await eventually(`runtime group ${groupId} ready on requested nodes`, async () => {
    await Promise.all(candidateNodes.map(async (node) => {
      const runtime = await runtimeGroups(node);
      const record = runtime.runtime_groups?.find((group) => group.group_id === groupId);
      if (!record?.loaded || !record?.ready_for_linearizable_reads) {
        throw new Error(`node${node.id} target runtime not ready: ${JSON.stringify(record)}`);
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

async function latestPlacementOperation(node, database) {
  const operations = await placementOperations(node, database);
  const latest = operations.operations?.[0];
  if (!latest) {
    throw new Error(`no placement operation found for ${database}`);
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

async function expectPlacement(database, groupId, timeoutMs, node = nodes[0]) {
  await eventually(`${database} placement visible`, async () => {
    const response = await fetchWithTimeout(`http://127.0.0.1:${portForNode(node)}/_orion/databases/${database}/placement`);
    const body = await response.json().catch(async () => ({ error: await response.text() }));
    if (!response.ok) {
      throw new Error(`node${node.id} placement HTTP ${response.status}: ${JSON.stringify(body)}`);
    }
    if (body.group?.group_id !== groupId) {
      throw new Error(`expected ${groupId}, got ${JSON.stringify(body.group)}`);
    }
  }, timeoutMs);
}

async function expectStandby(node, database, targetGroupId, sourceGroupId, timeoutMs) {
  await eventually(`${database} standby ${targetGroupId} visible`, async () => {
    const standbys = await listStandbys(node, database);
    const standby = standbys.find((record) => record.target_group_id === targetGroupId);
    assertStandbyRecord(standby, database, sourceGroupId, targetGroupId);
  }, timeoutMs);
}

async function expectNoStandbys(node, database, timeoutMs) {
  await eventually(`${database} standbys consumed`, async () => {
    const standbys = await listStandbys(node, database);
    if (standbys.length !== 0) {
      throw new Error(`expected no standbys for ${database}, got ${JSON.stringify(standbys)}`);
    }
  }, timeoutMs);
}

function assertStandbyRecord(record, database, sourceGroupId, targetGroupId) {
  if (!record) {
    throw new Error(`missing standby record for ${database}/${targetGroupId}`);
  }
  if (record.database_name !== database || record.source_group_id !== sourceGroupId || record.target_group_id !== targetGroupId) {
    throw new Error(`unexpected standby record: ${JSON.stringify(record)}`);
  }
  if (!record.source_applied_index || !record.target_applied_index) {
    throw new Error(`standby missing applied-index watermarks: ${JSON.stringify(record)}`);
  }
}

function assertStandbyCheckpointMetricsIfAvailable(metrics) {
  const checkpoint = normalizeStandbyCheckpointMetrics(metrics);
  if (!checkpoint) {
    return;
  }
  if (checkpoint.attempts < 1 || checkpoint.successes < 1) {
    throw new Error(`placement metrics did not observe checkpoint standby refresh: ${JSON.stringify(metrics)}`);
  }
  if (checkpoint.objects < 1) {
    throw new Error(`placement metrics did not observe checkpoint objects: ${JSON.stringify(metrics)}`);
  }
  if (checkpoint.fallbackToBackup !== 0) {
    throw new Error(`placement metrics unexpectedly fell back to backup export: ${JSON.stringify(metrics)}`);
  }
}

function assertPlacementMoveObjectNativeMetrics(metrics) {
  const transfer = metrics.placement_move_transfer ?? {};
  if (numericMetric(transfer.page_delta_successes) < 1) {
    throw new Error(`placement move metrics did not observe live page snapshot transfer success: ${JSON.stringify(metrics)}`);
  }
  if (numericMetric(transfer.page_delta_failures) !== 0) {
    throw new Error(`placement move metrics reported page-delta failures: ${JSON.stringify(metrics)}`);
  }
  if (numericMetric(transfer.checkpoint_attempts) !== 0
    || numericMetric(transfer.checkpoint_failures) !== 0
    || numericMetric(transfer.checkpoint_successes) !== 0) {
    throw new Error(`placement move unexpectedly used checkpoint transfer: ${JSON.stringify(metrics)}`);
  }
  if (numericMetric(transfer.backup_attempts) !== 0) {
    throw new Error(`placement move unexpectedly fell back to backup import: ${JSON.stringify(metrics)}`);
  }
}

function normalizeStandbyCheckpointMetrics(metrics) {
  const nested = metrics.standby_checkpoint;
  if (nested && typeof nested === "object") {
    return {
      attempts: numericMetric(nested.attempts),
      successes: numericMetric(nested.successes),
      objects: numericMetric(nested.objects_seen)
        + numericMetric(nested.objects_copied)
        + numericMetric(nested.objects_reused),
      fallbackToBackup: numericMetric(nested.fallback_to_backup),
    };
  }

  const attempts = metricFromAnyShape(metrics, [
    "standby_checkpoint_attempts",
    "standby_checkpoint_materialization_attempts",
  ]);
  const successes = metricFromAnyShape(metrics, [
    "standby_checkpoint_successes",
    "standby_checkpoint_materialization_successes",
  ]);
  const fallbackToBackup = metricFromAnyShape(metrics, [
    "standby_checkpoint_fallback_to_backup",
    "standby_checkpoint_materialization_fallback_to_backup",
    "standby_checkpoint_fallbacks",
  ]);
  const objects = [
    "standby_checkpoint_objects_seen",
    "standby_checkpoint_objects_copied",
    "standby_checkpoint_objects_reused",
    "standby_checkpoint_materialization_objects",
  ].reduce((total, key) => total + numericMetric(metrics[key]), 0);

  if ([attempts, successes, fallbackToBackup].some((value) => value === undefined) || objects === 0) {
    return null;
  }
  return { attempts, successes, objects, fallbackToBackup };
}

function metricFromAnyShape(metrics, keys) {
  for (const key of keys) {
    const value = metrics[key];
    if (value !== undefined && value !== null) {
      return numericMetric(value);
    }
  }
  return undefined;
}

function numericMetric(value) {
  const number = Number(value ?? 0);
  return Number.isFinite(number) ? number : 0;
}

async function expectValue(node, database, expected, timeoutMs) {
  await eventually(`node${node.id} reads ${database}`, async () => {
    const rows = await query(node, database, "select value from placement_items where id = 1");
    const value = rows[0]?.[0];
    if (value !== expected) {
      throw new Error(`expected ${expected}, got ${JSON.stringify(value)}`);
    }
  }, timeoutMs);
}

async function expectWriteRejected(node, database) {
  await eventually(`${database} rejects writes while fenced`, async () => {
    try {
      await execute(node, database, "insert into placement_items values (2, 'should-be-fenced')");
    } catch (error) {
      const message = String(error?.message ?? "");
      if (message.includes("fenced for placement operation")) {
        return;
      }
      if (isTransientPlacementCommitError(error) || message.includes("not enough for a quorum")) {
        throw error;
      }
      throw error;
    }
    throw new Error(`write unexpectedly succeeded while ${database} has a running placement move`);
  }, settleTimeoutMs);
}

async function execute(node, database, sql, args = []) {
  const result = await pipeline(node, database, [executeRequest(sql, false, args)]);
  await closeBaton(node, database, result.baton);
  return result.results?.[0]?.response?.result;
}

async function query(node, database, sql, args = []) {
  const result = await pipeline(node, database, [executeRequest(sql, true, args)]);
  await closeBaton(node, database, result.baton);
  return (result.results?.[0]?.response?.result?.rows ?? []).map((row) =>
    row.map(formatHranaValue));
}

function executeRequest(sql, wantRows, args = []) {
  return {
    type: "execute",
    stmt: { sql, args: args.map(toHranaValue), want_rows: wantRows },
  };
}

async function pipeline(node, database, requests, baton = null) {
  const response = await fetchWithTimeout(`${nodeUrl(node, database)}/v2/pipeline`, {
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

async function closeBaton(node, database, baton) {
  if (!baton) {
    return;
  }
  await pipeline(node, database, [{ type: "close" }], baton);
}

async function assertNoForbiddenLogs() {
  const result = await compose(["logs", "orion-node1", "orion-node2", "orion-node3"], {
    timeoutMs: 60_000,
    allowFailure: true,
  });
  const logs = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
  const forbidden = [
    "database disk image is malformed",
    "disk I/O error",
    "unable to open database file",
    "detected newer DB client",
    "panic",
  ];
  for (const pattern of forbidden) {
    if (logs.includes(pattern)) {
      throw new Error(`forbidden log pattern found: ${pattern}`);
    }
  }
}

function nodeUrl(node, database) {
  return `http://127.0.0.1:${portForNode(node)}/${database}`;
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

async function fetchWithTimeout(url, options = {}, timeoutMs = requestTimeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function eventually(label, fn, timeoutMs) {
  const started = Date.now();
  let nextProgressAt = started + progressMs;
  let lastError;
  while (Date.now() - started < timeoutMs) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const now = Date.now();
      if (now >= nextProgressAt) {
        logProgress(`waiting: ${label} (${now - started}ms/${timeoutMs}ms): ${error?.message ?? error}`);
        nextProgressAt = now + progressMs;
      }
      await sleep(pollMs);
    }
  }
  throw new Error(`${label} timed out after ${timeoutMs}ms: ${lastError?.message ?? "unknown error"}`);
}

function logProgress(message) {
  console.error(`[placement-chaos] ${new Date().toISOString()} ${message}`);
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
      maxBuffer: 32 * 1024 * 1024,
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
  await pipeDiagnostic(["ps", "-a"]);
  console.error("\n--- build logs ---");
  await pipeDiagnostic(["logs", "--tail", "160", "orion-build"]);
  console.error("\n--- recent node logs ---");
  await pipeDiagnostic(["logs", "--tail", "260", "orion-node1", "orion-node2", "orion-node3"]);
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
