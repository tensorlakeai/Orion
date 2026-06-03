# DB Placement Dead-Source Failover Runbook

This runbook covers operator workflows for recovering a database when its current
replication group is unavailable. The current implementation supports standby
refresh, manual standby promotion, and automatic promotion of fresh standbys
during placement reconciliation. Nodes also run an automatic standby refresh
reconciler for databases assigned to automatic-failover replication groups.

All endpoints below are operator/system endpoints from `src/libsql_http.rs`.

## Phase Status

Phase 1 dead-source failover is complete:

- Standby refresh can pull from a source peer when the source runtime is not
  local. Repeat remote refreshes first try a binary page-delta export, then
  SlateDB checkpoint artifact materialization, and finally the raw SQLite backup
  path if the object-native paths fail.
- Standby imports verify byte count, SHA-256, target readiness, and database
  openability before recording watermarks.
- Manual and automatic standby promotion can move catalog placement to a fresh
  target copy without requiring the old source runtime.
- Placement move clone/import runs on the target-group leader and is retried
  instead of failed when reconciliation lands on the wrong node.
- The Docker placement chaos suite covers dead-source standby promotion,
  automatic standby refresh to explicit standby targets, restart/resume from
  every durable move phase, leader crash during a fenced move,
  checkpoint-native standby materialization metrics, and corruption-log
  scanning.

Phase 2 standby warming has started:

- Each server runs a standby refresh reconciler every 10 seconds.
- The reconciler refreshes one deterministic ready local target-group leader
  for each non-default automatic-failover source group.
- When `placement.failover.standby_targets` is configured on a replication
  group, the reconciler refreshes those target groups instead of using fallback
  target selection.
- Fresh standbys are skipped until they age past half the source group's
  `promote_after_ms`, with a 5 second minimum interval.
- Operators can trigger the same pass with
  `POST /_orion/placement/standby/reconcile`.
- Placement metrics now include standby totals, promotable standbys, stale
  standbys, and standby records with errors.
- Local source-loaded refresh can now reuse an existing standby watermark and
  copy only newer SQLite page versions plus current file metadata. Initial
  refresh and remote peer refresh still use snapshot/backup copy.
- SlateDB checkpoint artifacts are now the shared foundation for blob-native
  materialization. Artifacts record the database path, checkpoint id, manifest
  id, and object prefix; materialization copies only missing objects into the
  target store before opening a checkpoint clone. Raft snapshot manifests also
  carry this artifact shape so snapshot install and remote standby refresh can
  converge on the same transfer primitive.
- Placement moves now use checkpoint artifacts as the large-data transfer
  primitive when the source is local and target voter endpoints are available.
  The source persists a transfer epoch, target voters materialize missing
  checkpoint objects out-of-band, and cutover waits for durable voter readiness.
  Small warm catch-up deltas can still go through raft as an explicit transfer
  mode. Checkpoint failures do not fall back to raft or SQLite backup import for
  placement moves.

See [Object-Native Placement Transfer](db-placement-object-native-transfer.md)
for the long-term movement model and gaps versus mature distributed databases.

## Core Concepts

- A database has one active placement in `database_replication_groups`.
- A standby copy is recorded per database and target replication group.
- Standby promotion updates the catalog mapping to the target group and evicts
  the local cached database handle.
- A normal placement move requires the source runtime for the pre-clone phases.
- A standby promotion does not require the source runtime, but the target runtime
  must be loaded and ready for linearizable reads.

## Relevant Endpoints

Placement state and runtime:

- `GET /_orion/databases/{database}/placement`
- `GET /_orion/databases/{database}/placement/standbys`
- `GET /_orion/databases/{database}/placement/operations`
- `GET /_orion/replication-groups`
- `GET /_orion/replication-groups/runtime`
- `GET /_orion/placement/nodes`

Placement actions:

- `POST /_orion/databases/{database}/placement/standby`
- `POST /_orion/databases/{database}/placement/promote`
- `POST /_orion/databases/{database}/placement/move`
- `POST /_orion/placement/reconcile`
- `POST /_orion/placement/standby/reconcile`

Operator recovery and cleanup:

- `POST /_orion/databases/{database}/placement/operations/{operation_id}/cancel`
- `POST /_orion/databases/{database}/placement/operations/{operation_id}/repair`
- `POST /_orion/placement/gc`

Metrics and SQL observability:

- `GET /_orion/metrics/placement`
- Virtual table: `_orion.database_placement`
- Virtual table: `_orion.database_standbys`
- Virtual table: `_orion.placement_metrics`
- Virtual table: `_orion.placement_nodes`

## Standby Refresh

Refresh a standby before a source outage. Prefer running this from the target
replication-group leader that should be able to promote after the source fails:

```bash
curl -X POST "$BASE/_orion/databases/$DATABASE/placement/standby" \
  -H "content-type: application/json" \
  -d '{"target_group_id":"rg_target"}'
```

Run one automatic refresh planning pass manually:

```bash
curl -X POST "$BASE/_orion/placement/standby/reconcile" \
  -H "content-type: application/json" \
  -d '{}'
```

Request body:

```json
{
  "target_group_id": "rg_target"
}
```

Configure automatic standby targets on the source replication group:

```json
{
  "group_id": "rg_source",
  "placement": {
    "mode": "manual",
    "failover": {
      "automatic": true,
      "promote_after_ms": 600000,
      "standby_targets": ["rg_target"]
    }
  }
}
```

Operational target notes:

- Configure `standby_targets` on the source group for deterministic failover
  readiness. Without explicit targets, automatic refresh uses fallback target
  selection and may not warm the group an operator expects.
- Keep `promote_after_ms` aligned with the service RPO. The warmer skips fresh
  standbys until they age past half this interval, with a 5 second floor.
- Run `POST /_orion/placement/standby/reconcile` after changing group policy
  when you want an immediate refresh pass instead of waiting for the periodic
  reconciler.

The response contains a `standby` object with:

- `source_group_id`
- `target_group_id`
- `source_applied_index`
- `source_commit_ts`
- `target_applied_index`
- `target_commit_ts`
- `refreshed_at_ms`
- `updated_at_ms`
- `age_ms`
- `promotable`
- `error`

Refresh requirements:

- The database must exist and be `ready`.
- The target replication group must exist, be `active`, and be loaded by this node.
- No placement operation may already be running for the database.
- The source runtime must be available somewhere in the cluster.
- If the source runtime is not local, the target node pulls a raw
  `application/octet-stream` SQLite backup export from a source peer and verifies
  byte count plus SHA-256 before import.
- If the source runtime is local and the existing standby still matches the
  database's source group, refresh copies only SQLite page versions newer than
  the last recorded `source_applied_index`, plus current visible-page, file-size,
  and manifest metadata. If the target copy is missing or verification fails,
  refresh falls back to a full clone.
- The refreshed target copy must be openable on the node that performs the
  refresh. With per-node local object stores, a standby refreshed only on a node
  that later dies should not be considered survivable on other nodes.
- Remote cross-store standby refresh uses internal binary page-delta and
  checkpoint-object endpoints. When a matching target copy already exists, the
  target asks the source peer for an `application/octet-stream` page delta after
  the last recorded `source_applied_index`. Initial remote refresh, missing
  target copies, or failed page-delta validation fall back to SlateDB checkpoint
  artifact materialization; the raw SQLite backup export remains as a
  compatibility fallback.
- Automatic refresh only targets ready target-group leaders whose replication
  groups have automatic failover enabled. The built-in `rg_default` group is
  skipped by the automatic warmer to avoid surprise standby fan-out during
  bootstrap and initial placement moves.
- Requests received by a target-group follower are forwarded to the current
  target-group leader when that leader is known.

List recorded standby copies:

```bash
curl "$BASE/_orion/databases/$DATABASE/placement/standbys"
```

## Standby Promote

Promote a standby when the source group is unavailable but a target standby copy
already exists:

```bash
curl -X POST "$BASE/_orion/databases/$DATABASE/placement/promote" \
  -H "content-type: application/json" \
  -d '{"target_group_id":"rg_target","max_staleness_ms":30000}'
```

Request body:

```json
{
  "target_group_id": "rg_target",
  "max_staleness_ms": 30000,
  "force": false
}
```

`max_staleness_ms` is optional. If omitted and `force` is false, the default
staleness limit is the placement failover promotion window.

Set `force` to true only when an operator has accepted the data-loss/RPO risk:

```bash
curl -X POST "$BASE/_orion/databases/$DATABASE/placement/promote" \
  -H "content-type: application/json" \
  -d '{"target_group_id":"rg_target","force":true}'
```

Promotion requirements:

- The target replication group must be loaded by this node.
- The target runtime must be ready for linearizable reads.
- The target database copy must be present and pass SQLite `quick_check` on the
  node performing promotion.
- The standby copy must match the database's current source group.
- No placement operation may already be running for the database.
- Unless `force` is true, the standby age must be within `max_staleness_ms`.
- If commit timestamps are present, the target timestamp must be at least the
  copied source timestamp.
- If applied indexes are present, the target runtime must not have regressed
  below the standby's recorded target applied index.

After successful promotion:

- `database_replication_groups` points at the target group.
- The promoted standby row is removed.
- The local cached database handle is evicted.
- Old source copies should be treated as stale until they observe the catalog.

## Automatic Promotion Behavior

Current automatic behavior covers draining groups and fresh standby promotion:

- `POST /_orion/placement/reconcile` detects `draining` replication groups
  with `failover_automatic = true`.
- Reconcile selects a ready active target group.
- Reconcile enqueues automatic placement moves for ready databases on the
  draining group.
- Reconcile also detects databases whose current group has `failover_automatic =
  true` but is not loaded or not ready for linearizable reads.
- If a fresh standby exists within `failover_promote_after_ms`, Reconcile
  promotes that standby automatically.
- The standby refresh reconciler keeps configured standby targets warm, and
  operators can trigger a pass through
  `POST /_orion/placement/standby/reconcile`.
- Operators can still promote explicitly through
  `POST /_orion/databases/{database}/placement/promote`.

## Metrics And Standby Observability

Use the JSON placement metrics endpoint for operation and group health:

```bash
curl "$BASE/_orion/metrics/placement"
```

The response includes:

- `operations_total`
- `operations_running`
- `operations_completed`
- `operations_failed`
- `running_by_phase`
- `oldest_running_age_ms`
- `stale_running_operations`
- `databases_by_group`
- group totals for active, draining, deleted, failed, unloaded, and not-ready groups
- `standbys_total`
- `standbys_promotable`
- `standbys_stale`
- `standbys_errors`
- `standby_page_delta`
- `placement_move_transfer`
- checkpoint standby refresh counters when the node build exposes them

Alerting guidance:

- `standbys_errors > 0` means at least one recorded standby has a refresh or
  validation error that needs inspection before promotion.
- `standbys_promotable == 0` for an automatic-failover source group means the
  configured target is not currently a safe promotion candidate.
- Page-delta failures or non-zero `fallback_to_checkpoint` mean repeat remote
  refreshes are falling back to checkpoint materialization instead of staying
  incremental.
- `placement_move_transfer.backup_attempts > 0` means placement moves are using
  the raft-backed SQLite backup import fallback instead of page-delta transfer.
  That should be investigated for source export, target raft apply, or
  page-delta validation failures.
- Checkpoint refresh attempts without successes, or non-zero checkpoint fallback
  counters, indicate the remote path is falling back to the compatibility backup
  export and should be investigated for object-store or checkpoint inventory
  issues.

Use virtual tables for SQL-based operator views:

```sql
select *
from _orion.database_placement
where database_name = 'example.db';

select *
from _orion.database_standbys
where database_name = 'example.db';

select *
from _orion.placement_metrics
order by status, phase;

select *
from _orion.placement_nodes
order by node_id;
```

Use the standby list endpoint for standby-specific observability:

```bash
curl "$BASE/_orion/databases/$DATABASE/placement/standbys"
```

The standby endpoint includes node-local `promotable` status. `promotable` is
evaluated on the node serving the request, so it can differ between nodes when
local object stores are not shared. The `_orion.database_standbys` virtual
table exposes catalog-recorded standby inventory and age/watermark fields for
SQL-based operator views.

## Cancel, Repair, And GC

List operations for a database:

```bash
curl "$BASE/_orion/databases/$DATABASE/placement/operations"
```

Cancel a running placement operation:

```bash
curl -X POST "$BASE/_orion/databases/$DATABASE/placement/operations/$OPERATION_ID/cancel" \
  -H "content-type: application/json" \
  -d '{"reason":"operator cancelled during failover investigation"}'
```

Repair a failed or cancelled operation by moving it back to a running phase:

```bash
curl -X POST "$BASE/_orion/databases/$DATABASE/placement/operations/$OPERATION_ID/repair" \
  -H "content-type: application/json" \
  -d '{"phase":"planned","reason":"resume after operator validation"}'
```

`phase` is optional. Supported repair phases are currently:

- `planned`
- `fenced`
- `cloning`
- `catching_up`
- `switching`

Run placement operation garbage collection:

```bash
curl -X POST "$BASE/_orion/placement/gc" \
  -H "content-type: application/json" \
  -d '{"older_than_ms":86400000,"limit":100}'
```

Both `older_than_ms` and `limit` are optional. The response includes
`checked_at_ms`, `older_than_ms`, `limit`, and `deleted`.

## Dead-Source Failover Procedure

1. Confirm current placement:

   ```bash
   curl "$BASE/_orion/databases/$DATABASE/placement"
   ```

2. Confirm target runtime readiness:

   ```bash
   curl "$BASE/_orion/replication-groups/runtime"
   ```

   The target group should be loaded and ready for linearizable reads.

3. Check for a fresh standby:

   ```bash
   curl "$BASE/_orion/databases/$DATABASE/placement/standbys"
   ```

   Verify `target_group_id`, `source_group_id`, `refreshed_at_ms`, `age_ms`,
   `promotable`, and the source/target watermarks. Run this check against the
   node that will perform promotion.

4. Promote the standby:

   ```bash
   curl -X POST "$BASE/_orion/databases/$DATABASE/placement/promote" \
     -H "content-type: application/json" \
     -d '{"target_group_id":"rg_target","max_staleness_ms":30000}'
   ```

5. Confirm placement switched:

   ```bash
   curl "$BASE/_orion/databases/$DATABASE/placement"
   ```

6. Run reconcile and inspect risks:

   ```bash
   curl -X POST "$BASE/_orion/placement/reconcile" \
     -H "content-type: application/json" \
     -d '{}'
   ```

7. Check placement metrics:

   ```bash
   curl "$BASE/_orion/metrics/placement"
   ```

8. Exercise a read/write through the normal database API and confirm the request
   routes to the promoted group.

## Old Source Comeback Runbook

When the old source group or node comes back after a standby promotion:

1. Treat the old source's local copy as stale. Do not route writes to it.

2. Confirm the catalog still points the database to the promoted target:

   ```bash
   curl "$BASE/_orion/databases/$DATABASE/placement"
   ```

3. Confirm runtime groups:

   ```bash
   curl "$BASE/_orion/replication-groups/runtime"
   ```

4. Run reconcile:

   ```bash
   curl -X POST "$BASE/_orion/placement/reconcile" \
     -H "content-type: application/json" \
     -d '{}'
   ```

5. Inspect risks and open operations:

   ```bash
   curl "$BASE/_orion/metrics/placement"
   curl "$BASE/_orion/databases/$DATABASE/placement/operations"
   ```

6. If the old source should become a standby again, refresh it from the current
   active group after it is loaded and ready:

   ```bash
   curl -X POST "$BASE/_orion/databases/$DATABASE/placement/standby" \
     -H "content-type: application/json" \
     -d '{"target_group_id":"rg_old_source"}'
   ```

7. If an old move operation is stuck, cancel or repair it before trying another
   placement action.

8. If completed or failed operation history is noisy, run placement GC with an
   operator-approved retention window.

## Safer Normal Move With Session Drain

For non-emergency moves where the source is healthy, prefer a move with an
explicit drain window:

```bash
curl -X POST "$BASE/_orion/databases/$DATABASE/placement/move" \
  -H "content-type: application/json" \
  -d '{"target_group_id":"rg_target","drain_timeout_ms":30000}'
```

The move path fences the database, clones to the target, catches up, switches
catalog placement, and then completes through reconciliation. Use:

```bash
curl -X POST "$BASE/_orion/placement/reconcile" \
  -H "content-type: application/json" \
  -d '{}'
```

until the operation reaches `completed`.
