# Orion

This is the developer-focused README for running and testing the repository.
For the product-focused overview, see [`README.md`](README.md).

Orion is an early prototype for a multitenant SQLite/libSQL-compatible
database service with a cloud-native storage plane for multi-cloud control
planes.

The operator-facing product shape is a single `orion` binary with explicit
roles:

- `storage`: WAL quorum participation, page service, object-store sync, NVMe
  cache management, compaction, and repair.
- `compute`: libSQL/Hrana-compatible HTTP frontend backed by SQLite through the
  Orion VFS.
- `router`: future libSQL/Hrana protocol routing and local-region proxying.
- `controller`: future placement and reconciliation controller.
- `all`: local/dev role that runs implemented roles together.

## Durability Model

SQLite parses, plans, and executes SQL. Orion does not reimplement the SQLite
dialect. Tenant databases open through `orion_vfs`; persistent VFS writes are
batched at SQLite sync boundaries and must be accepted by the Raft durability
layer before SQLite sees sync success.

```text
client transaction
  -> SQLite engine
  -> orion_vfs write/sync batch
  -> OpenRaft proposal
  -> Fjall sync write
  -> commit acknowledged
  -> SlateDB materialization
  -> object storage checkpoint/compaction
```

Fjall on local NVMe stores the hot Raft log and is the synchronous durability
boundary. SlateDB stores materialized state over object storage. Local SQLite
files are cache/materialization, not the source of truth.

## Current Slice

- `orion-sqlite`: SQLite VFS integration, Hrana-facing SQLite runtime, and
  the `RaftWalCommitSink` durability boundary.
- `OrionRaftLogStore`: OpenRaft log storage backed by Fjall keyspaces.
- `OrionRaftStateMachine`: OpenRaft state machine that applies committed
  SQLite VFS batches into SlateDB.
- `TonicRaftNetwork`: OpenRaft transport over gRPC/HTTP2 with timeouts and
  bounded message sizes.
- `SlateDbStateStore`: object-store-backed materialized state.
- SlateDB checkpoint-backed snapshots and log compaction.
- `serve_libsql_http_with_shutdown`: libSQL/Hrana HTTP API for clients and the
  local shell.
- `StorageNodePlan`: the first single-service storage-node facade.

The production compatibility work is tracked in
[`docs/sqlite-compatibility-roadmap.md`](docs/sqlite-compatibility-roadmap.md).

## Development

On this machine, the SQLite extension build needs the CommandLineTools
libclang. The repository configures that in `.cargo/config.toml`.

```bash
cargo test
```

## Run One Node

Running without arguments starts a persisted one-node development cluster using
built-in defaults and prints all configured paths and endpoints.

```bash
cargo run
```

To create an editable config:

```bash
cargo run -- init-config
cargo run -- --config orion.yaml
```

The default libSQL/Hrana HTTP endpoint is:

```text
http://127.0.0.1:8091
```

Use the local shell:

```bash
curl -X POST http://127.0.0.1:8091/_orion/databases \
  -H "content-type: application/json" \
  -d '{"name":"appdb"}'
scripts/orion-libsql-shell.mjs http://127.0.0.1:8091/appdb
```

The shell sends Hrana HTTP pipeline requests directly and keeps one session
baton open until exit. It supports ordinary SQL terminated by `;`, plus
`.tables`, `.schema`, `.help`, and `.quit`. For authenticated endpoints, pass
the bearer token with `LIBSQL_AUTH_TOKEN`.

Databases are created and dropped through the Orion lifecycle API. User
traffic is accepted only for databases in the `ready` state.

```bash
curl http://127.0.0.1:8091/_orion/databases
curl http://127.0.0.1:8091/_orion/databases/appdb
curl -X DELETE http://127.0.0.1:8091/_orion/databases/appdb
```

Incremental blob access is documented in
[`docs/libsql-blob-api.md`](docs/libsql-blob-api.md). Orion supports
JSON/base64 blob calls, raw HTTP binary `read-bytes` and `write-bytes`
endpoints, and WebSocket blob control messages with binary frames. The default
per-chunk limit is `524288` bytes and is configurable with
`libsql_http.blob_max_chunk_bytes`.

Idempotent write retries are supported with `x-orion-idempotency-key`:

```bash
curl -X POST http://127.0.0.1:8091/appdb/v2/pipeline \
  -H "content-type: application/json" \
  -H "x-orion-idempotency-key: create-user-123" \
  -d '{"requests":[{"type":"execute","stmt":{"sql":"insert into users values (123)"}}]}'
```

Orion hashes the canonical pipeline body, wraps the write and idempotency
record in one SQLite transaction, and stores the record durably through Raft.
Retries with the same key and same payload return the stored result metadata
instead of applying the write again. Reusing a key with a different payload
returns `ORION_IDEMPOTENCY_CONFLICT`. Current scope is intentionally strict:
keys apply to standalone write pipelines where Orion owns the transaction
boundary. Baton sessions and pipelines containing explicit transaction control
are rejected with `HRANA_PROTO_ERROR`.

If a write times out or returns `ORION_COMMIT_UNKNOWN`, the client should
retry the exact same pipeline body with the exact same idempotency key. Orion
will reconcile the key against the durable idempotency table: if the original
attempt committed, the retry returns the stored response with
`orion.idempotency.reused=true`; if the original attempt did not commit, the
retry applies the write once. Clients must not reuse an idempotency key for a
different request body.

Clients should retry inside the configured retention window. By default,
committed idempotency records are retained for 24 hours, stuck pending records
are retained for 7 days, and GC runs every 60 seconds with a maximum of 1000
records deleted per pass. The policy is configured under
`libsql_http.idempotency`. Operator metrics are exposed at
`/_orion/metrics/idempotency` and through the `_orion` namespace:

```sql
select * from idempotency_metrics;
```

Latency smoke:

```bash
ORION_LATENCY_ITERATIONS=50 scripts/local-libsql-latency.mjs http://127.0.0.1:8091/appdb
```

## Run A Local Three-Node Cluster

Start node2 and node3 first, then node1. Node1 bootstraps a fresh cluster and
skips initialization when local Raft state already exists.

```bash
cargo run -- --config examples/node2.yaml
cargo run -- --config examples/node3.yaml
cargo run -- --config examples/node1.yaml
```

The checked-in examples simulate a three-region, multi-cloud deployment:

| Node | Cloud | Region | Zone | Raft | libSQL HTTP |
| --- | --- | --- | --- | --- | --- |
| 1 | AWS | us-east-1 | use1-az1 | 127.0.0.1:7101 | 127.0.0.1:8081 |
| 2 | GCP | us-central1 | us-central1-a | 127.0.0.1:7102 | 127.0.0.1:8082 |
| 3 | Azure | eastus | 1 | 127.0.0.1:7103 | 127.0.0.1:8083 |

## Docker Cluster

The Docker harness starts three real Orion processes with Tonic Raft between
containers. It bind-mounts the workspace and keeps Cargo registry, Cargo git,
and `target/` in named volumes so rebuilds reuse Docker-side cache. The
`orion-build` service builds the binary once and copies it into a shared
`orion-bin` volume; node containers mount that binary read-only instead of
running `cargo run`.

```bash
docker compose up --build orion-node1 orion-node2 orion-node3
docker compose down
```

Run the automated multi-node crash/restart smoke:

```bash
scripts/docker-cluster-crash-smoke.sh
```

The smoke uses the fixed Compose project `orion-cluster-smoke`, starts three
real nodes, writes through the libSQL API, stops the current Raft leader,
verifies the surviving quorum can write, restarts the stopped node, and checks
that all three nodes converge. It removes only the Orion node/object-store
volumes between runs, leaving Docker-side Cargo cache volumes in place.

Run the heavier black-box chaos suite:

```bash
scripts/docker-cluster-chaos.sh
```

The chaos suite uses the Compose project `orion-cluster-chaos` and exercises
SIGKILL leader loss, follower restart, rolling one-node-at-a-time crashes,
idempotent quorum-loss retry/reconciliation, recovery, and final convergence.
It drives the cluster only through the libSQL HTTP API and validates the full
replicated rowset after every recovery phase. Useful knobs include
`ORION_DOCKER_KEEP_RUNNING=1`, `ORION_DOCKER_KILL_SIGNAL=SIGTERM`, and
`ORION_DOCKER_REQUEST_TIMEOUT_MS=15000`.

Placement chaos is the heavier database-placement gate:

```bash
scripts/docker-placement-chaos.sh
```

It covers dead-source standby promotion, automatic standby refresh with explicit
standby targets, placement move restart/resume from every durable phase, leader
crash during a fenced move, binary page-delta/checkpoint standby metrics,
chunked page-delta placement-move transfer metrics, and node-log corruption scanning.
The `Docker chaos` GitHub Actions workflow
runs both cluster chaos and placement chaos automatically for pull requests and
pushes to `main` that touch runtime, Docker, docs, or chaos-test paths; it also
runs on a daily schedule.

Placement transfer benchmarks can be run with:

```bash
scripts/docker-placement-benchmark.sh
```

By default it seeds a 64 MiB logical SQLite payload with `zeroblob(...)`, moves
the database into a fresh multi-voter target group, and reports seed/move
duration, MiB/s, placement transfer counters, and raft large-payload staging
metrics when raft is used for explicit small page-delta catch-up. Useful knobs:

- `ORION_BENCH_TOTAL_BYTES=1073741824`: logical payload size to seed.
- `ORION_BENCH_BLOB_BYTES=131072`: bytes per seeded row.
- `ORION_BENCH_SEED_BATCH_ROWS=128`: rows inserted per SQL batch.
- `ORION_DOCKER_MOVE_TIMEOUT_MS=600000`: longer timeout for larger moves.

To exercise region-local object-store seeding, run:

```bash
scripts/docker-placement-regional-object-store-benchmark.sh
```

That wrapper starts node1 and node2 in `aws/us-east-1` with a shared regional
object-store volume and node3 in a separate `azure/eastus` volume. Placement
move checkpoint source selection should prefer each target voter's own
source-group copy when available, then a source voter in the same cloud/region,
and only then a remote source. In the default regional benchmark every target
voter is also a source voter, so the assertion requires full checkpoint reuse:
`checkpoint_objects_copied=0`, `checkpoint_bytes_copied=0`, no page-delta
placement transfer, and no backup fallback.

The Docker Compose file supports the regional harness with these env vars:
`ORION_DOCKER_NODE{1,2,3}_CONFIG` chooses per-node YAML config files and
`ORION_DOCKER_OBJECT_STORE_VOLUME_{1,2,3}` chooses the named volume mounted as
that node's local object-store root.

Useful placement-chaos knobs:

- `ORION_DOCKER_KEEP_RUNNING=1`: leave the Compose project up for inspection.
- `ORION_DOCKER_HTTP_PORT_1=8181`: override the node 1 host HTTP port;
  `_2` and `_3` control nodes 2 and 3.
- `ORION_DOCKER_RAFT_PORT_1=7201`: override the node 1 host Raft port;
  `_2` and `_3` control nodes 2 and 3.
- `ORION_DOCKER_KILL_SIGNAL=SIGTERM`: use graceful termination instead of the
  default `SIGKILL`.
- `ORION_DOCKER_MOVE_TIMEOUT_MS=240000`: tune placement-move completion time.
- `ORION_DOCKER_REQUEST_TIMEOUT_MS=30000`: tune per-request HTTP timeout.
- `ORION_DOCKER_PROGRESS_MS=30000`: emit periodic wait progress in CI logs.

Default Docker test host ports:

- `scripts/docker-cluster-chaos.mjs`: HTTP `8081..8083`, Raft `7101..7103`.
- `scripts/docker-placement-chaos.mjs`: HTTP `8181..8183`, Raft `7201..7203`.
- `scripts/docker-cluster-crash-smoke.mjs`: HTTP `8281..8283`, Raft
  `7301..7303`.
- `scripts/docker-placement-move-smoke.mjs`: HTTP `8381..8383`, Raft
  `7401..7403`.

These defaults let the Docker chaos and smoke suites run in parallel on one
machine.

Placement operations and standby warming are documented in
[`docs/db-placement-dead-source-failover.md`](docs/db-placement-dead-source-failover.md).

Published libSQL/Hrana endpoints:

```text
http://127.0.0.1:8081/appdb
http://127.0.0.1:8082/appdb
http://127.0.0.1:8083/appdb
```

Use `docker compose down -v` when you intentionally want to discard node data
and the Docker-side Cargo cache.

## Configuration

Important fields:

- `node_id`
- `roles`
- `raft_addr`
- `advertised_raft_addr`
- `topology`
- `storage.local.raft_log_root`
- `slatedb_path`
- `object_store`
- `peers`
- `bootstrap`
- `raft`
- `transport`
- `metrics`
- `readiness`
- `sqlite.cache_root`
- `libsql_http.bind_addr`
- `libsql_http.auth`

`raft_addr` is the local bind address. `advertised_raft_addr` can be set when
the address other nodes should dial is different from the bind address, such as
Docker Compose service names or Kubernetes DNS names.
