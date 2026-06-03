# SQLite Object Store Layout

Orion stores tenant SQLite materialization in SlateDB as logical key/value
records. SlateDB owns the physical object-store SST/manifests; the keys below
are the Orion-level records inside SlateDB.

## Current Format

File size metadata:

```text
sqlite/pages/<database>/<path>/size -> u64 big-endian file size
```

Page-oriented materialization:

```text
sqlite/pages/<database>/<path>/current/<page_no>
  -> current SQLite file extent bytes

sqlite/pages/<database>/<path>/manifests/<version>
  -> bincode(FileManifest { version, kind, size })

sqlite/pages/<database>/<path>/latest_manifest
  -> u64 big-endian latest manifest version
```

For Raft-applied writes, `version` is the OpenRaft log index that applied the
SQLite VFS sync batch. Local single-node/non-Raft store calls allocate an
internal monotonically increasing version so they use the same manifest layout.
Page writes update the current file extent directly at the Orion key level.
The final extent of a file may be shorter than the configured internal extent
size; reads zero-fill missing bytes up to the logical file size.

## Why This Is More Efficient

- SQLite page writes avoid rewriting the full database image.
- Orion avoids maintaining a second long-lived page-version history above
  SQLite.
- SlateDB owns immutable SSTs, manifests, checkpoints, and compaction below the
  Orion logical keyspace.
- Current extents make checkpoint materialization and regional object reuse the
  main large-data movement path.

## Compaction

SQLite materialization compaction is now mostly SlateDB compaction: old current
extent values become obsolete inside SlateDB's LSM after newer values are
written to the same logical keys. Orion still keeps replicated operator
control and observability tables for storage maintenance, but it no longer
needs a separate visible-pointer/page-version cleanup loop for normal SQLite
page updates.

Compaction observability is stored through the same replicated SQLite path in
the reserved `_orion` database. Operator-visible tables live there instead of
inside tenant databases:

```text
_orion.compaction_runs
_orion.compaction_state
_orion.compaction_control
_orion.compaction_retention_floor
_orion.compaction_leases
```

`compaction_control` stores operator intent such as pause/resume and a
one-shot force request. `compaction_leases` is the durable leader lease that
prevents multiple nodes from owning the same maintenance work at the same time
after leadership changes.

Live Raft metrics are intentionally not stored here. They are piggybacked on
the Tonic Raft transport and kept in an in-memory cluster metrics registry, so
observability does not create extra Raft log entries.
The `_orion.raft_metrics` and `_orion.storage_pressure` tables exposed by
the libSQL API are virtual tables backed by that registry and a live SlateDB
scan; projection, filters, ordering, and limits are planned through an
in-memory SQLite connection rather than persisted into object storage.

## Database Purge

Logical database deletion is catalog-driven. A dropped database is first
tombstoned so clients cannot reopen it, then the service can physically purge
the materialized SQLite keyspace after the configured retention window.
`purge_tombstoned_sqlite_database` enforces that retention check from
`deleted_at_ms` before it emits any SlateDB deletes; `purge_sqlite_database` is
the lower-level bounded prefix delete used once the caller has already decided
purge is allowed.

The physical purge is a bounded prefix delete over:

```text
sqlite/pages/<database>/
```

Each pass returns the number of keys scanned/deleted, deleted bytes, and whether
the database prefix is empty. Operators can therefore make purge work
incremental instead of issuing one unbounded object-store mutation. The purge
only removes Orion-level SQLite materialization keys for the target database;
neighboring databases under the same SlateDB/object-store instance are left
untouched. SlateDB owns the physical SST/manifests beneath these logical
tombstones, so object-store space is reclaimed by SlateDB compaction and garbage
collection after the prefix has been deleted.
