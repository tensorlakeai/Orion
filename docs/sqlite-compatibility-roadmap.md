# SQLite Compatibility Roadmap

This document tracks the remaining work to make Orion a production-grade
libSQL/SQLite-compatible service. The ordering matters: protocol polish is not
useful if the VFS does not preserve SQLite's durability, locking, and recovery
contract.

## Workstreams

### 1. VFS Correctness

Owner scope:

- `crates/orion-sqlite/src/vfs.rs`
- `crates/orion-sqlite/src/store.rs`
- `crates/orion-raft/src/slatedb_sqlite_store.rs`
- `crates/orion-raft/src/sqlite_runtime.rs`

Questions to close:

- Are `xRead`, `xWrite`, `xTruncate`, `xFileSize`, `xDelete`, `xAccess`, and
  `xSync` faithful enough for rollback journal and WAL mode?
- Are pending writes flushed at exactly the sync/close/delete boundaries SQLite
  expects?
- Can crash recovery rebuild a correct database image from committed Raft log
  entries plus SlateDB materialized state?
- Does the VFS correctly distinguish main DB, WAL, journal, shm, temp, and
  transient files?

Deliverables:

- VFS conformance tests for file open/read/write/truncate/delete/sync.
- Crash/restart tests that kill a node after committed writes and verify the
  SQLite database reopens cleanly.
- A documented stance on which SQLite journaling modes Orion supports.

### 2. Locking, WAL, Shared Memory, And Busy Handling

Owner scope:

- `crates/orion-sqlite/src/vfs.rs`

Questions to close:

- Are `xLock`, `xUnlock`, and `xCheckReservedLock` enforcing SQLite's locking
  state machine rather than only storing a local enum?
- Does WAL shared-memory behavior work across multiple connections in the same
  process?
- What happens when two sessions write to the same tenant database?
- Do busy timeout and contention errors match SQLite expectations?

Deliverables:

- Multi-connection tests for reader/writer, writer/writer, and transaction
  contention.
- WAL-mode tests with multiple connections sharing the same VFS name/database.
- Explicit supported/unsupported behavior for cross-process local cache access.

### 3. SQLite API Surface

Owner scope:

- `src/libsql_http.rs`
- `crates/orion-raft/src/sqlite_runtime.rs`

Questions to close:

- Which PRAGMAs are fixed, allowed, ignored, or rejected?
- How are backup/restore exposed for tenant databases?
- Are extension loading, virtual tables, table-valued functions, update hooks,
  change hooks, and session extension inherited from SQLite, exposed through
  libSQL protocols, or intentionally blocked?
- Are blob API transaction, lifetime, binary-frame, and chunk-limit semantics
  stable enough to become part of the public contract?

Deliverables:

- Compatibility matrix for PRAGMAs and SQLite APIs, maintained in
  [`sqlite-api-policy.md`](sqlite-api-policy.md).
- Tests for standardized PRAGMAs such as `journal_mode`, `synchronous`,
  `busy_timeout`, `foreign_keys`, and `wal_checkpoint`.
- Backup/restore API design.
- Blob API contract and test guidance in
  [`libsql-blob-api.md`](libsql-blob-api.md).

### 4. libSQL Protocol Compatibility

Owner scope:

- `src/libsql_http.rs`
- `scripts/orion-libsql-shell.mjs`

Protocol goal:

Orion should be compatible with libSQL clients at the query protocol layer:
connect, authenticate, execute SQL, bind values, fetch rows, run batches,
describe statements, preserve session state, and surface SQLite-compatible
errors. This work is only about the client query surface. libSQL embedded
replica sync, WAL frame shipping, generation metadata, and remote replication
admin APIs are not part of this workstream.

Implementation sequence:

1. Freeze the supported endpoint contract for Hrana HTTP:
   `/{database}/v2/pipeline` is the primary compatibility target, with bearer
   auth, one baton per logical session, and database names constrained to the
   current tenant-safe path rules.
2. Build a protocol fixture suite around raw Hrana JSON before changing
   handlers. Fixtures should cover request/response shape, omitted optional
   fields, unknown request types, invalid batons, malformed values, HTTP status
   codes, and JSON error bodies.
3. Complete Hrana HTTP stream request semantics:
   `execute`, `batch`, `sequence`, `describe`, `store_sql`, `close_sql`,
   `get_autocommit`, and `close` should match libSQL SDK expectations for
   result tags, baton retention, step-level batch errors, conditional batch
   execution, and close-after-pipeline behavior.
4. Normalize statement and value behavior:
   positional args, named args, nulls, integers, floats, text, blobs, large
   integers encoded as strings, invalid base64, duplicate named args, missing
   params, `want_rows`, `affected_row_count`, `last_insert_rowid`, column names,
   and declared types should be tested against SQLite/libSQL behavior.
5. Tighten session behavior:
   baton creation should be lazy and stable across requests, session SQL
   storage should be isolated per baton, transactions should survive across
   pipeline calls on the same baton, `close` should release server state, and
   expired/unknown batons should return deterministic protocol errors.
6. Tighten error mapping:
   map SQLite errors to stable Hrana/libSQL error codes where possible, preserve
   useful messages without leaking internals, distinguish malformed protocol
   requests from SQLite execution failures, and verify HTTP errors versus
   per-stream errors.
7. Add real SDK compatibility tests for the HTTP query path:
   run the official JavaScript libSQL client, and any lightweight language
   clients practical in CI, against a local Orion node. The tests should use
   the public client API rather than Orion's helper shell.
8. Add Hrana WebSocket query support after HTTP fixtures are stable:
   reuse the same session/runtime semantics, then add WebSocket-specific tests
   for hello/auth, stream open/close, request ids, cursor sequencing,
   concurrent streams, close frames, and reconnect behavior.
9. Add libSQL v3/protobuf query support only after HTTP and WebSocket pass the
   shared query matrix:
   define the protobuf schema boundary, translate v3 messages into the same
   internal stream operations, and run the same query behavior tests through
   protobuf transport.
10. Add explicit negative tests for non-goals:
    replication, sync, generation, frame, and embedded-replica admin endpoints
    must remain unimplemented or return clear unsupported responses.

Deliverables:

- Hrana HTTP raw JSON fixture tests.
- Real libSQL SDK query compatibility tests.
- Process-level official JavaScript client smoke:
  `scripts/process-libsql-client-smoke.sh`.
- Process-level Drizzle ORM libSQL smoke:
  `scripts/process-drizzle-libsql-smoke.sh`.
- Sqllogictest-compatible black-box runner:
  `scripts/orion-sqllogictest.mjs`.
- Process-level sqllogictest smoke:
  `scripts/process-sqllogictest-smoke.sh`.
- Raw libSQL/Hrana HTTP compatibility smoke for multi-session concurrency and
  unsupported replication endpoints:
  `scripts/process-libsql-http-compat-smoke.sh`.
- Curated local corpus:
  `testdata/sqllogictest/orion-core.slt`.
- Shared query behavior suite that can run through HTTP, WebSocket, and
  protobuf/v3 transports.
- WebSocket endpoint implementation after HTTP compatibility is locked.
- Protobuf/v3 endpoint implementation after WebSocket compatibility is locked.
- Negative tests and docs for unsupported libSQL replication APIs.

Protocol compatibility test matrix:

| Area | HTTP `/v2/pipeline` | WebSocket Hrana | v3/protobuf | Expected result |
| --- | --- | --- | --- | --- |
| Authentication | Bearer accepted/rejected | Hello/auth accepted/rejected | Auth metadata accepted/rejected | Same database access decision across transports |
| Session lifecycle | New baton, reused baton, invalid baton, close | Stream open/close, reconnect, invalid stream | Session open/close message flow | Stable session state and deterministic unknown-session errors |
| Basic execution | DDL, DML, `select`, `values`, `pragma`, `with` | Same cases | Same cases | Result shape and SQLite side effects match libSQL clients |
| Value binding | Null, integer string, float, text, blob, named args, positional args | Same cases | Same cases | Values round-trip without type drift |
| Result metadata | Columns, declared types, row values, affected rows, last insert rowid | Same cases | Same cases | Metadata matches SQLite/libSQL expectations |
| Batches | All steps ok, step error, conditional ok/error/not/and/or, autocommit condition | Same cases | Same cases | Step results and step errors preserve ordering |
| Statement storage | `store_sql`, execute by `sql_id`, close stored SQL, missing id | Same cases | Same cases | Stored statements are scoped to one session |
| Transactions | `begin`, cross-pipeline write/read, commit, rollback, autocommit checks | Same cases | Same cases | Transaction state follows the session baton/stream |
| Blob API | JSON/base64 open/read/write/reopen/close, raw binary read/write, chunk limit, handle invalidation | JSON controls and binary frames | Defer until v3 shape is defined | Blob handles are session-scoped and do not expose replication APIs |
| Describe | Readonly, explain, params, columns, missing SQL | Same cases | Same cases | Description is protocol-compatible and deterministic |
| Errors | SQL syntax, constraint, busy/locked, malformed request, bad value encoding | Same cases | Same cases | Protocol errors are distinct from SQLite execution errors |
| Concurrency | Multiple batons against one database, concurrent reads, writer contention | Multiple streams and sockets | Multiple sessions | Behavior matches documented SQLite locking policy |
| SDK smoke | Official JavaScript libSQL client CRUD and transaction tests | SDK support where available | SDK support where available | Public client APIs work without Orion-specific adapters |
| Replication non-goals | Sync/frame/generation/admin endpoints absent or unsupported | Same stance | Same stance | No libSQL replication behavior is exposed |

### 5. Product Boundary: No libSQL Replication API

Orion is not a Turso/libSQL embedded-replica server. It is a globally
distributed, Spanner-like database that speaks the libSQL/Hrana query protocol.
Replication, placement, quorum, durability, and repair are Orion-owned
systems built on OpenRaft, Fjall, SlateDB, object storage, and NVMe cache.

Non-goals:

- Do not implement libSQL WAL-frame replication APIs.
- Do not emulate libSQL generation/frame-number sync metadata.
- Do not expose Orion Raft indexes as libSQL replication indexes.
- Do not shape product architecture around embedded-replica sync clients.

Required boundary behavior:

- Query/client SDK protocols should work.
- Known libSQL replication/admin endpoints should be absent or return a clear
  unsupported response.
- Documentation should state that replication is internal to Orion.

### 6. Performance Parity

Owner scope:

- `scripts/local-libsql-latency.mjs`
- `src/libsql_http.rs`
- `crates/orion-sqlite/src/vfs.rs`
- `crates/orion-raft/src/sqlite_commit_sink.rs`
- `crates/orion-raft/src/openraft_store.rs`

Questions to close:

- What are baseline p50/p95/p99 latencies for single-node and three-node local
  clusters?
- How much overhead comes from Hrana HTTP, SQLite connection/session handling,
  VFS batching, OpenRaft proposal, Fjall fsync, and SlateDB materialization?
- Which workloads need batching, prepared statement reuse, connection pooling,
  or WAL write coalescing?

Deliverables:

- Latency and throughput benchmarks for reads, autocommit writes, explicit
  transactions, concurrent sessions, and multi-node quorum writes.
- Regression thresholds in CI or a local perf gate.
- Trace points that can be enabled without changing code.

## First Implementation Sequence

1. Add multi-connection VFS locking/WAL tests.
2. Fix VFS locking/shared-memory semantics until those tests pass.
3. Add crash/restart recovery tests around committed SQLite writes.
4. Standardize PRAGMA policy.
5. Freeze the Hrana HTTP query contract and add raw JSON fixture tests for
   pipeline, baton, values, batches, statement storage, describe, close, and
   error responses.
6. Add real libSQL client compatibility tests for the Hrana HTTP query path.
7. Add unsupported-endpoint tests proving libSQL replication/sync APIs are not
   exposed.
8. Implement Hrana WebSocket query support against the shared query matrix.
9. Implement libSQL v3/protobuf query support against the shared query matrix.
10. Add multi-node latency benchmark and regression gate.
