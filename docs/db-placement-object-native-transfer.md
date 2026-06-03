# Object-Native Placement Transfer

This note captures the placement-transfer direction after moving large payloads
away from raft. The rule is:

- Raft is the metadata/control plane: fences, transfer epochs, readiness, cutover.
- Object/checkpoint copy is the data plane: SQLite/SlateDB bytes move directly
  between object stores or node HTTP endpoints.
- Page deltas through raft are only for explicit small catch-up deltas.

## Current Implementation

Cold placement moves create SlateDB checkpoint artifacts from source voters and
persist a representative artifact on the placement operation as a transfer
epoch. Source selection is per target voter:

- prefer the target voter itself when it is already a source-group voter;
- otherwise prefer a source voter in the same cloud and region;
- otherwise fall back to the preferred/current source voter.

The epoch records:

- `transfer_epoch_id`
- `transfer_kind = checkpoint`
- checkpoint artifact JSON
- source applied index and commit timestamp
- creation timestamp

Target voters materialize checkpoint objects out-of-band with:

```text
POST /_orion/internal/databases/{database}/placement/checkpoint/materialize
```

The request carries the checkpoint export, target group, operation id, and the
source endpoint that serves checkpoint objects. Each voter checks its local
object store first, copies only missing objects, opens a local checkpoint clone,
verifies database openability, and records a durable
`placement_transfer_voter_status` row. In a regional object-store deployment, a
same-region target can materialize by reusing existing blob/object-store objects
without copying database bytes through raft or HTTP.

Cutover requires:

- the source fence watermark exists;
- the checkpoint transfer source watermark covers the source fence;
- the target group is ready for linearizable reads;
- every target voter has a `ready` row for the transfer epoch.

Metrics expose both aggregate move transfer counters and per-voter transfer
readiness under `GET /_orion/metrics/placement`.

## Current Verification

The Docker placement benchmark covers the multi-voter path end-to-end. The
regional object-store benchmark runs node1 and node2 against one regional
object-store volume and node3 against another; because every target voter is
also a source voter in that scenario, it requires full checkpoint reuse:

```text
checkpoint_objects_copied = 0
checkpoint_bytes_copied = 0
page_delta_attempts = 0
backup_attempts = 0
```

## Remaining Gap

The next production-hardening steps are:

- retry fan-out per voter without re-creating the checkpoint artifact;
- record failed voter rows with error text for easier operator diagnosis;
- add bounded fan-out concurrency and per-object timeout knobs;
- add chaos coverage for one slow target voter and one restarted target voter.

## Future Range And Segment Movement

Orion currently moves an entire SQLite database as one placement unit. Mature
systems such as FoundationDB, Spanner, TiKV, and CockroachDB split placement
into smaller ownership units so data transfer can be scheduled continuously and
balanced incrementally.

The target model for Orion should be:

- Keep raft groups as placement/control groups, not byte-copy workers.
- Introduce a catalog-level `database_segments` table. A segment starts as
  "whole database" and can later become key-range, table, tenant, or page-range
  scoped.
- Give each segment an owner group, serving epoch, source fence watermark, and
  transfer epoch.
- Materialize segment checkpoint artifacts out-of-band into target voters.
- Apply small segment catch-up deltas after checkpoint materialization.
- Switch segment ownership atomically in the catalog after all serving and
  readiness barriers pass.

Suggested schema direction:

```text
database_segments(
  database_id,
  segment_id,
  segment_kind,
  segment_start,
  segment_end,
  owner_group_id,
  serving_epoch,
  state,
  updated_at_ms
)

segment_transfer_epochs(
  transfer_epoch_id,
  database_id,
  segment_id,
  source_group_id,
  target_group_id,
  source_fence_applied_index,
  source_fence_commit_ts,
  checkpoint_artifact_json,
  status,
  created_at_ms,
  updated_at_ms
)

segment_transfer_voter_status(
  transfer_epoch_id,
  node_id,
  status,
  target_applied_index,
  target_commit_ts,
  bytes_copied,
  error,
  updated_at_ms
)
```

This lets the system catch up to TiKV/Cockroach-style movement in stages:

1. Whole-database segment metadata backed by the current placement tables.
2. Multiple segments per database for scheduling and progress accounting.
3. Segment-local checkpoint artifacts and small catch-up deltas.
4. Load-based split/merge policies.
5. Online rebalancing with throttled transfer queues.

## Design Principles

- Never rebuild one giant raft request for large data movement.
- Persist enough metadata to retry after any process crash.
- Prefer idempotent object copy; re-copying an existing object should be cheap.
- Make readiness explicit per voter before serving switches.
- Make every transfer mode explicit; checkpoint failures should retry or fail
  loudly, not silently switch to raft data transfer.
