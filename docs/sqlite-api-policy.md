# SQLite API Policy

Orion is a libSQL/SQLite-compatible database service. SQLite remains the SQL
engine; Orion owns distributed durability, placement, replication, recovery,
and tenant isolation. This policy documents which SQLite and libSQL API
surfaces are allowed, denied, or deferred for the service boundary.

Status meanings:

- **Allow**: Supported as part of the public compatibility contract, subject to
  tenant isolation and the documented Orion durability model.
- **Deny**: Intentionally unsupported. Requests should fail with a clear
  unsupported/authorization error rather than silently changing local state.
- **Defer**: Not yet part of the compatibility contract. Implementation needs a
  design or tests before it can become allow or deny.

## PRAGMA Policy

| Surface | Status | Policy |
| --- | --- | --- |
| `PRAGMA foreign_keys` | Allow | Per-connection setting inherited from SQLite. Must be accepted and reported normally. |
| `PRAGMA busy_timeout` | Allow | Per-connection/session setting. Contention behavior must remain compatible with SQLite errors and timeout semantics. |
| `PRAGMA query_only` | Allow | Safe per-connection guardrail. Writes in query-only mode should fail with SQLite-compatible errors. |
| `PRAGMA cache_size`, `cache_spill`, `temp_store` | Allow | Local execution hints only. They must not change distributed durability or tenant placement. |
| `PRAGMA table_info`, `table_xinfo`, `index_list`, `index_info`, `foreign_key_list`, `database_list`, `schema_version`, `user_version`, `application_id` | Allow | Metadata PRAGMAs are part of SQLite compatibility. Returned values must reflect the tenant database visible to the session. |
| `PRAGMA journal_mode` | Allow with constraint | Orion standardizes tenant databases on WAL mode. Reads and `journal_mode = wal` are allowed; transitions to other modes are rejected. |
| `PRAGMA synchronous` | Allow with constraint | Connections are pinned to `FULL`; `FULL`, `EXTRA`, and equivalent numeric values are allowed. Values that weaken the Raft durability boundary are rejected. |
| `PRAGMA wal_checkpoint` | Allow with constraint | Passive checkpoints are allowed through SQLite's WAL/VFS contract and do not expose libSQL replication semantics. Checkpoint behavior must remain an implementation detail of local materialization. |
| `PRAGMA locking_mode` | Allow with constraint | Reads and `locking_mode = normal` are allowed. Exclusive locking is rejected because it conflicts with the service concurrency model. |
| `PRAGMA integrity_check`, `quick_check`, `foreign_key_check` | Allow | Operational validation PRAGMAs are allowed for the tenant database visible to the session. |
| `PRAGMA optimize`, `analysis_limit`, `automatic_index` | Defer | Likely acceptable as SQLite-local behavior, but should be covered by compatibility tests before support is promised. |
| `PRAGMA database_list` with attached databases | Deny for attached DBs | Main and temp database reporting is allowed. Additional attached tenant/local paths are not supported. |
| File/path PRAGMAs that redirect storage or change process-global behavior | Deny | Examples include writable schema hacks or APIs that would bypass the tenant VFS/storage boundary. |

## API Surface Matrix

| Surface | Status | Policy |
| --- | --- | --- |
| `ATTACH` / `DETACH` | Deny | Cross-database attachment breaks tenant isolation and distributed placement assumptions. Return a clear unsupported error. |
| `VACUUM` | Deny for now | Plain `VACUUM` rewrites the database file through temp storage and page movement. Support requires a dedicated VFS conformance pass; until then, use Orion compaction. |
| `VACUUM INTO` | Deny | Writes to caller-selected paths are outside the tenant storage boundary and must not be exposed through the service. |
| Extension loading (`load_extension`, `sqlite3_enable_load_extension`) | Deny | Dynamic native code loading is not compatible with a managed multi-tenant service boundary. |
| Built-in virtual tables required by SQLite/libSQL metadata | Allow | Built-ins that are compiled into the engine and do not escape tenant isolation may be exposed. |
| User-defined or extension-backed virtual tables | Deny | They can load native code, access host resources, or require per-node state outside Orion durability. |
| FTS/JSON/RTREE-style compiled SQLite features | Defer | Allow only after confirming compile-time availability, deterministic behavior, and backup/restore compatibility. |
| Blob streaming (`sqlite3_blob_*`, libSQL equivalent) | Allow with constraint | Supported through Orion's session-scoped blob API. Clients allocate blob space with SQL, usually `zeroblob(...)`, then read/write bounded chunks through JSON/base64, raw HTTP binary, or WebSocket binary APIs. See [`libsql-blob-api.md`](libsql-blob-api.md). |
| Backup | Defer | Requires a service-owned backup design that snapshots tenant state through Orion durability, not direct local SQLite files. |
| Restore | Defer | Requires an import/restore design with validation, tenant isolation, version compatibility, and Raft/application ordering. |
| SQLite backup API against local files | Deny | Direct file-level backup bypasses Orion's source-of-truth model. |
| Update/preupdate/commit/rollback hooks | Defer | In-process hooks may be useful for internal tests, but are not a remote client contract until exposed through libSQL-compatible APIs. |
| Change/session extension (`sqlite3session`, changesets, patchsets) | Defer | Potentially useful for sync/import workflows, but must not be confused with libSQL replication APIs. Needs a product design first. |

## Compatibility Rules

- Orion should prefer SQLite-compatible errors over silent no-ops for denied
  surfaces.
- APIs that choose arbitrary filesystem paths are denied unless routed through a
  service-owned import/export mechanism.
- APIs that load native code are denied for managed service use.
- API behavior must not expose local cache files as the source of truth.
- libSQL client query protocols are in scope; libSQL embedded-replica
  replication/admin APIs are out of scope.
- Blob APIs are query-protocol APIs. They must not expose libSQL replication
  frame state, Orion Raft indexes, or local cache file paths.
- Deferred items should not be documented as supported until they have explicit
  tests and a product-level owner decision.

## Read Freshness Policy

Orion exposes read freshness as an HTTP-level service contract for the
libSQL/Hrana query endpoint. Write-only pipelines do not pay a read barrier.
Pipelines that return rows or inspect statement/catalog metadata are classified
as reads and use the selected freshness policy.

| Header | Values | Policy |
| --- | --- | --- |
| `x-orion-read-policy` | `strong`, `revocation_safe`, `session`, `bounded_staleness`, `local` | Selects the freshness policy. Defaults to `strong` for read pipelines. |
| `x-orion-min-applied-index` | unsigned integer | Required lower bound for `session` reads when the client has a known Raft apply index. |
| `x-orion-session-token` | `applied_index:physical_ms:logical` | Session token returned by Orion after writes or reads with an applied timestamp. For `session` reads, this can be used instead of `x-orion-min-applied-index`. |
| `x-orion-read-timeout-ms` | unsigned integer | Optional timeout for `session` reads. Defaults to 1000 ms. |
| `x-orion-max-staleness-ms` | unsigned integer | Required for `bounded_staleness`. `0` is treated as a strong read. Nonzero values validate local Raft readiness and compare the requested bound against the replica's persisted HLC apply timestamp. If a brand-new replica has no applied commit timestamp yet, Orion falls back to a strong read barrier rather than fabricating freshness. |

Policy meanings:

- `strong`: performs an OpenRaft read-index barrier before the read pipeline.
- `revocation_safe`: currently equivalent to `strong`; this is reserved for
  authorization-sensitive reads once auth token revocation has its own version
  clock.
- `session`: waits until the local replica has applied at least
  `x-orion-min-applied-index`, when supplied.
- `bounded_staleness`: serves from the local replica only when Raft metrics show
  a known leader, local applied index has caught up to committed index, and the
  persisted closed HLC timestamp is at or beyond `now - x-orion-max-staleness-ms`.
- `local`: skips the Raft read barrier and serves from the local materialized
  SQLite state.

Invalid read-policy headers fail the Hrana request with `HRANA_PROTO_ERROR`.
Freshness failures fail with `SQLITE_BUSY`, so clients can retry against another
region or with a stronger leader-directed route.

For the current single-Raft-group architecture, the replica closed timestamp is
the HLC commit timestamp of the last applied Raft entry. Future multi-range
execution must compute the minimum closed timestamp across all ranges touched by
the query.

When `libsql_http` has peer HTTP endpoints configured, Orion forwards
non-local read policies to the current Raft leader if the local node cannot
satisfy freshness. Forwarded responses preserve the leader's Hrana response and
include Orion metadata describing the serving node and forwarding node.

Orion responses include a non-Hrana `orion` metadata object with the serving
node id, read policy, freshness metrics, and a session token when one is
available. libSQL clients that ignore unknown response fields can continue using
the standard query API.
