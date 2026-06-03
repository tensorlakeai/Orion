# Orion

Orion is a distributed SQLite/libSQL-compatible database service for control
planes that need to run across regions, clouds, and failure domains without
giving up a simple SQL programming model.

It is built for teams that want the operational shape of a managed cloud
database, the elasticity of object storage, and the placement controls of a
globally distributed system. Applications connect over the libSQL/Hrana HTTP
protocol, execute ordinary SQLite SQL, and let Orion handle consensus,
durability, database placement, standby warming, failover, compaction, and
recovery.

## Why Orion

Most control-plane databases live in an uncomfortable middle ground. They are
too important for best-effort replication, too latency-sensitive for a single
far-away primary, too numerous for one heavyweight database process per tenant,
and too operationally awkward when the product spans AWS, GCP, Azure, and
private regions.

Orion is designed for that gap.

- **SQLite compatibility without a local-file deployment model.** SQLite remains
  the SQL engine. Orion adds a distributed VFS, replicated commits, durable
  object-store materialization, and a libSQL-compatible service API.
- **Multi-tenant by default.** A single Orion cluster can host many small
  databases, each with its own lifecycle and placement.
- **Object storage as the durable data plane.** Database pages are materialized
  into SlateDB over object storage. Local files and NVMe are cache and execution
  state, not the source of truth.
- **Blob-store-native regional seeding.** When Orion can place or seed a new
  node from a source in the same region, it uses object/blob-store checkpoint
  primitives so the node can reuse existing regional objects instead of pulling
  database bytes through Raft.
- **Consensus for the things that must be serialized.** OpenRaft coordinates
  durable SQLite sync batches, catalog changes, placement operations, fencing,
  and promotion.
- **Placement as a product feature.** Operators can move a database between
  replication groups, warm standbys, drain groups, and promote a fresh copy when
  the old source is gone.
- **Designed for control planes.** Strong reads, idempotent write retries,
  session tokens, bounded-staleness reads, operation recovery, and explicit
  runbooks are part of the service contract.
- **Cross-cloud from the beginning.** Nodes advertise cloud, region, and zone;
  placement groups can span one region, multiple regions, or multiple clouds.

## What Orion Runs

Orion is one binary with a simple request path. Applications talk to the
libSQL API. SQLite plans and executes the SQL. The Orion VFS turns SQLite page
changes into replicated sync batches. Raft commits those batches across the
replication group. Storage keeps the hot log local and the durable database
pages in object storage.

```text
                      ╔═ Orion ════════════════════════════════════════════════════╗
                      ║                                                              ║
╔════════╗            ║  ┌────────────┐  ┌────────┐  ┌─────────────┐  ┌──────────┐   ║░
║ Client ║── API ────▶║  │ LibSQL API │─▶│ SQLite │─▶│ Orion VFS │─▶│ OpenRaft │   ║░
╚════════╝░           ║  └────────────┘  └────────┘  └─────────────┘  └────┬─────┘   ║░
 ░░░░░░░░░░           ║                                                    │         ║░
                      ║                     ┌──────────────────────────────┤         ║░
                      ║                     │                              │         ║░
                      ║                     ▼                              ▼         ║░
                      ║            ┌────────┬───────┐   ┌──────────────────┬──────┐  ║░
                      ║            │ RocksDB log    │   │ SlateDB page store      │  ║░
                      ║            │ local, hot     │   │ object storage backed   │  ║░
                      ║            └────────────────┘   └────────────┬────────────┘  ║░
                      ║                                              │               ║░
                      ║                                              ▼               ║░
                      ║                                ┌─────────────┬─────────────┐ ║░
                      ║                                │ S3 / GCS / Azure Blob     │ ║░
                      ║                                └───────────────────────────┘ ║░
                      ║                                                              ║░
                      ╚══════════════════════════════════════════════════════════════╝░
                       ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

- **LibSQL API:** HTTP/Hrana endpoint for clients and application frameworks.
- **SQLite:** the SQL engine, planner, transactions, functions, and type system.
- **Orion VFS:** the distributed file layer that captures SQLite writes.
- **OpenRaft:** consensus for ordered, durable commits inside a replication
  group.
- **Storage:** RocksDB stores the hot Raft log locally; SlateDB materializes
  database pages into object storage. Local SQLite files and NVMe are cache and
  execution state, not the source of truth.

For development and early deployments, a node can run everything in one process.
The architecture still separates the blocks so production deployments can split
API serving, storage, routing, and control loops later.

## Install Orion

Download the latest `orion` binary from GitHub Releases, or install it with
the published installer:

```bash
curl -fsSL https://tensorlake/download-orion.sh | sh
```

Confirm the binary is on your path:

```bash
orion --help
```

Developers working from source can still run the binary with `cargo run`, but
the product-facing path is a downloaded `orion` executable.

## Quick Start: One Local Node

Start a single-node Orion server:

```bash
orion
```

Running without a config starts a persisted one-node cluster with safe local
defaults and prints the data paths, object-store root, Raft address, SQL
endpoint, roles, and bootstrap settings.

The default libSQL/Hrana endpoint is:

```text
http://127.0.0.1:8091
```

Create a database:

```bash
orion db create appdb
```

Open a shell:

```bash
orion cli
```

Try it:

```sql
create table users (id integer primary key, email text not null);
insert into users values (1, 'founder@example.com');
select * from users;
.tables
.schema users
```

Create an editable starter config:

```bash
orion init-config
orion --config orion.yaml
```

The generated YAML is heavily commented and explains how to change storage
paths, object-store roots, topology, peers, runtime limits, Raft timings,
transport limits, compaction, readiness, and libSQL HTTP settings.

## Connect Applications

Orion speaks the libSQL/Hrana HTTP protocol. The local shell and smoke tests
exercise the same API surface used by libSQL-style clients.

`orion cli` uses `LIBSQL_URL` when set. Otherwise it uses `ORION_URL` and
opens `/appdb`; if neither is set, it defaults to
`http://127.0.0.1:8091/appdb`.

Example endpoint for database `appdb`:

```text
http://127.0.0.1:8091/appdb
```

Install the TypeScript client:

```bash
npm install @libsql/client
```

Set the database URL and, when auth is enabled, a bearer token:

```bash
export LIBSQL_URL=http://127.0.0.1:8091/appdb
export LIBSQL_AUTH_TOKEN=local-dev-token
```

Connect with the HTTP client:

```ts
import { createClient } from "@libsql/client/web";

const client = createClient({
  url: process.env.LIBSQL_URL ?? "http://127.0.0.1:8091/appdb",
  authToken: process.env.LIBSQL_AUTH_TOKEN,
});

await client.execute(`
  create table if not exists users (
    id integer primary key,
    email text not null
  )
`);

await client.execute({
  sql: `
    insert into users (id, email)
    values (?, ?)
    on conflict(id) do update set email = excluded.email
  `,
  args: [123, "founder@example.com"],
});

const result = await client.execute({
  sql: "select id, email from users where id = ?",
  args: [123],
});

console.log(result.rows);
```

The `/web` import uses the HTTP protocol, which is the protocol Orion exposes
for libSQL-compatible clients.

Useful compatibility smoke tests:

```bash
scripts/local-libsql-client-smoke.sh
scripts/process-hrana-ws-smoke.sh
scripts/local-drizzle-libsql-smoke.sh
```

For authenticated endpoints, pass the bearer token through `LIBSQL_AUTH_TOKEN`
or send the normal authorization header expected by the service.

## Database Lifecycle

Databases are managed through the Orion lifecycle API. User traffic is routed
only to databases in the `ready` state.

```bash
export ORION_URL=http://127.0.0.1:8091
export BASE="$ORION_URL"

orion db list

orion db create appdb

orion db get appdb

orion db drop appdb
```

Use `--orion-url <url>` to override `ORION_URL` for a single command.

A dropped database is tombstoned first so clients cannot reopen it. Physical
object-store cleanup is retention-aware and incremental.

## Replication Groups

A replication group is a named set of Orion nodes that replicate data
together. Operators create replication groups from cluster nodes; users choose
which group each database should use.

When a database is placed in a replication group, Orion copies its data to the
nodes in that group. If one node, zone, region, or cloud fails, the database can
keep serving from the remaining replicas.

Every Orion node advertises a location:

```text
cloud / region / zone
```

For example:

```text
aws / us-east-1 / use1-az1
gcp / us-central1 / us-central1-a
azure / eastus / 1
```

Those labels do not store policy by themselves. They give the placement
controller enough information to build groups that match the operator's intent:
one region, three zones, two clouds, three clouds, or a dedicated pool for an
important workload.

The default shared group is named `rg_default`. It is good for local
development and small databases. Production operators usually create additional
groups with clearer names:

- `rg_use1_control_plane`
- `rg_global_control_plane`
- `rg_cross_cloud_critical`
- `rg_dedicated_customer_acme`

A database is assigned to one active replication group at a time. That group
owns the OpenRaft durability path for writes and the SlateDB/object-store
keyspace for the database. Orion can move a database to another group by
creating checkpoint artifacts, fencing writes, materializing target voters,
catching up, switching routing, and retiring the old placement after retention.
When source and target voters are in the same region and share the regional
blob/object store, Orion prefers that regional source and reuses the
checkpoint objects directly. Cross-region targets copy only the exact checkpoint
objects they do not already have.

List groups:

```bash
orion db groups list
orion db groups runtime
```

Create an operator-managed group:

```bash
orion db groups create rg_global_control_plane \
  --mode single_region \
  --member 1:voter:0 \
  --member 2:voter:1 \
  --member 3:voter:2
```

Member roles are intentionally simple:

- `voter`: stores data, votes in Raft, and can become leader.
- `learner`: stores data and catches up without voting.
- `read_replica`: product-facing read replica, implemented as a non-voting
  OpenRaft learner.

Witnesses are not part of the current product surface.

You can add or change members after group creation:

```bash
orion db groups add-member rg_global_control_plane 1 --role voter --priority 0
orion db groups add-member rg_global_control_plane 2 --role voter --priority 1
orion db groups add-member rg_global_control_plane 3 --role read_replica --priority 2
```

Current placement mode values are `single_region`, `regional_primary`,
`dual_cloud_quorum`, `follow_the_tenant`, `read_global_write_home`, and
`manual`.

Reconcile so the runtime loads the group and applies membership:

```bash
orion db reconcile
```

## From One Node To A Single-Region Cluster

A production cluster starts by running multiple Orion nodes in one cloud
region, usually across zones. Each node advertises its topology and peers.

The checked-in examples simulate three nodes:

| Node | Cloud | Region | Zone | Raft | libSQL HTTP |
| --- | --- | --- | --- | --- | --- |
| 1 | AWS | us-east-1 | use1-az1 | 127.0.0.1:7101 | 127.0.0.1:8081 |
| 2 | GCP | us-central1 | us-central1-a | 127.0.0.1:7102 | 127.0.0.1:8082 |
| 3 | Azure | eastus | 1 | 127.0.0.1:7103 | 127.0.0.1:8083 |

Start node2 and node3 first, then node1. Node1 bootstraps a fresh default group
and skips bootstrap when it already has local Raft state.

```bash
orion --config examples/node2.yaml
orion --config examples/node3.yaml
orion --config examples/node1.yaml
```

For a real single-region deployment, use the same pattern but set each node's
topology to the same cloud and region with different zones:

```yaml
node:
  id: 1
  raft_addr: "0.0.0.0:7101"
  advertised_raft_addr: "orion-1.use1.example.internal:7101"
  topology:
    cloud: "aws"
    region: "us-east-1"
    zone: "use1-az1"
```

Configure peers with their advertised Raft addresses and optional libSQL HTTP
addresses so reads can forward to the leader when needed.

## Docker Three-Node Cluster

The Docker harness starts three real Orion processes with Tonic Raft between
containers. It bind-mounts the workspace and reuses Docker-side Cargo cache
volumes. A build container compiles once and copies the binary into a shared
volume used by the node containers.

```bash
docker compose up --build orion-node1 orion-node2 orion-node3
docker compose down
```

Published endpoints:

```text
http://127.0.0.1:8081/appdb
http://127.0.0.1:8082/appdb
http://127.0.0.1:8083/appdb
```

Run crash and placement smoke tests:

```bash
scripts/docker-cluster-crash-smoke.sh
scripts/docker-placement-move-smoke.sh
scripts/docker-placement-chaos.sh
```

The placement chaos suite covers dead-source standby promotion, automatic
standby refresh to explicit failover targets, crash/restart resume from every
durable placement-move phase, leader crash during a fenced move, binary
page-delta/checkpoint standby metrics, and log scanning for storage corruption.
The `Docker chaos` GitHub Actions workflow runs it for relevant runtime, Docker,
chaos-test, and placement-doc changes, plus a daily scheduled run.
Default Docker test host ports are split by suite: cluster chaos uses HTTP
`8081..8083` and Raft `7101..7103`; placement chaos uses `8181..8183` and
`7201..7203`; cluster crash smoke uses `8281..8283` and `7301..7303`; placement
move smoke uses `8381..8383` and `7401..7403`. The Docker chaos and smoke
suites can therefore run in parallel on one machine. Override with
`ORION_DOCKER_HTTP_PORT_1..3` and `ORION_DOCKER_RAFT_PORT_1..3` when another
local service owns those ports.

## Multi-Region In One Cloud

For a multi-region deployment in one cloud, keep the same binary and API shape.
Change the topology, peer addresses, object-store roots, and Raft timings.

Example topology:

```text
node 1: aws/us-east-1/use1-az1
node 2: aws/us-west-2/usw2-az1
node 3: aws/eu-west-1/euw1-az1
```

WAN clusters should use larger election timeouts than local development. Start
with conservative values and tune from observed latency:

```yaml
raft:
  heartbeat_interval_ms: 100
  election_timeout_min_ms: 600
  election_timeout_max_ms: 1200
  install_snapshot_timeout_ms: 60000

transport:
  connect_timeout_ms: 1000
  rpc_timeout_ms: 5000
  max_message_size: 8388608
```

Use object-store configuration appropriate for the deployment. In local
examples this is a filesystem-backed object store; in production the boundary is
S3, GCS, Azure Blob, or a compatible object store.

## Cross-Cloud Deployment

Cross-cloud deployment uses the same topology model:

```text
node 1: aws/us-east-1/use1-az1
node 2: gcp/us-central1/us-central1-a
node 3: azure/eastus/1
```

Each node advertises:

- cloud
- region
- zone
- Raft address
- libSQL HTTP address
- object-store configuration
- local cache roots

The value proposition is not that every database must span every cloud. The
point is that each database can be placed where its application needs it:

- a default shared group for small control-plane metadata
- a regional group for latency-sensitive services
- a multi-region group for availability
- a cross-cloud group for control planes that must survive cloud-level outages
- a dedicated group for noisy or high-value databases

## Database Placement

Placement is how operators choose where a database lives.

Check current placement:

```bash
orion db placement appdb
```

Ask Orion for a placement plan:

```bash
orion db plan appdb --target-group rg_global_control_plane
```

Move a database with a drain window:

```bash
orion db move appdb --target-group rg_global_control_plane --drain-timeout-ms 30000
```

Drive reconciliation until the operation completes:

```bash
orion db reconcile
orion db operations appdb
```

The move path:

1. drains or closes active sessions
2. fences writes on the source
3. creates a SlateDB checkpoint artifact from the nearest source voter
4. materializes target voters from local/regional objects when possible
5. copies only missing checkpoint objects for remote regions
6. verifies the target copy
7. records source and target watermarks
8. switches the catalog placement
9. evicts stale local handles

## Standbys And Failover

Orion can warm a standby copy of a database in another replication group.
This is the operational foundation for dead-source failover.

Refresh a standby:

```bash
orion db refresh-standby appdb --target-group rg_global_standby
```

List standbys:

```bash
orion db standbys appdb
```

Promote a fresh standby when the source group is unavailable:

```bash
orion db promote appdb --target-group rg_global_standby --max-staleness-ms 30000
```

Promotion requires a local target copy that is openable, passes validation, and
is within the requested freshness window unless the operator explicitly forces
the promotion.

Run the standby reconciler manually:

```bash
orion db standby-reconcile
```

For automatic failover, configure standby targets on the source group so the
warmer keeps the intended destination hot instead of relying on fallback target
selection. Remote standby refreshes use SlateDB checkpoint artifacts for
object-native materialization and can reuse already-present objects before
copying missing ones. The raw SQLite backup path remains compatibility
machinery rather than the preferred placement data plane.

Useful operator signals:

- `orion db metrics`
- `orion db standbys appdb`
- `GET /_orion/metrics/placement`
- `_orion.database_standbys`
- `_orion.placement_metrics`

## Draining And Maintenance

Drain a replication group before maintenance:

```bash
orion db groups drain rg_old
```

When a group is draining and automatic failover is enabled, placement reconcile
can enqueue automatic moves for ready databases on that group:

```bash
orion db reconcile
```

Inspect open placement work:

```bash
orion db metrics
orion db operations appdb
```

Cancel an operation:

```bash
orion db cancel appdb "$OPERATION_ID" --reason "operator cancelled during maintenance"
```

Repair a failed or cancelled operation:

```bash
orion db repair appdb "$OPERATION_ID" --phase planned --reason "resume after validation"
```

Supported repair phases are `planned`, `fenced`, `cloning`, `catching_up`, and
`switching`.

## Backups, Migration, And Import

Orion uses service-owned checkpoint/export/import paths for placement and
standby movement. Operators should not copy local SQLite cache files directly;
those files are execution cache, not the durable source of truth.

The preferred internal data path is object native:

- source runtime creates a SlateDB checkpoint artifact
- each target voter materializes that checkpoint from its nearest source
- same-region voters can reuse regional blob/object-store objects without
  copying the bytes
- remote voters copy only missing checkpoint objects
- target openability and `quick_check` are validated before promotion

The SQLite backup-like path remains useful as a compatibility and future
external backup foundation, but it is no longer the primary placement transfer
path.

The public backup/restore product surface should build on the same machinery:

- create a consistent service-owned snapshot
- record a manifest and watermarks
- protect needed checkpoint objects with retention policy
- stream chunks with checksums and backpressure
- restore/import through the target runtime
- validate before routing traffic

For migrations between Orion groups, use placement move rather than direct
file copy.

## Read Freshness And Idempotent Writes

The SQL stays ordinary SQLite. Orion attaches freshness and retry guarantees
to the HTTP request that carries the SQL.

Create and write data normally:

```sql
create table users (
  id integer primary key,
  email text not null
);

insert into users values (123, 'a@example.com');

select id, email from users where id = 123;
```

Wrap the SQL in a Hrana HTTP pipeline request:

```bash
curl -X POST "$BASE/appdb/v2/pipeline" \
  -H "content-type: application/json" \
  -d '{
    "requests": [
      {
        "type": "execute",
        "stmt": {
          "sql": "select id, email from users where id = ?",
          "args": [123]
        }
      }
    ]
  }'
```

Reads default to a strong policy for pipelines that return rows. Choose a
different read policy by adding Orion headers to the same HTTP request:

| Header | Values |
| --- | --- |
| `x-orion-read-policy` | `strong`, `revocation_safe`, `session`, `bounded_staleness`, `local` |
| `x-orion-session-token` | session token returned by Orion |
| `x-orion-min-applied-index` | minimum applied index for session reads |
| `x-orion-max-staleness-ms` | bound for bounded-staleness reads |

Strong read:

```bash
curl -X POST "$BASE/appdb/v2/pipeline" \
  -H "content-type: application/json" \
  -H "x-orion-read-policy: strong" \
  -d '{
    "requests": [
      {
        "type": "execute",
        "stmt": {
          "sql": "select id, email from users where id = ?",
          "args": [123]
        }
      }
    ]
  }'
```

Bounded-staleness read:

```bash
curl -X POST "$BASE/appdb/v2/pipeline" \
  -H "content-type: application/json" \
  -H "x-orion-read-policy: bounded_staleness" \
  -H "x-orion-max-staleness-ms: 250" \
  -d '{
    "requests": [
      {
        "type": "execute",
        "stmt": {
          "sql": "select id, email from users where id = ?",
          "args": [123]
        }
      }
    ]
  }'
```

Idempotent write retries use the same pattern. Put the SQL in the request body
and put the idempotency key in the HTTP headers:

```bash
curl -X POST "$BASE/appdb/v2/pipeline" \
  -H "content-type: application/json" \
  -H "x-orion-idempotency-key: create-user-123" \
  -d '{
    "requests": [
      {
        "type": "execute",
        "stmt": {
          "sql": "insert into users values (?, ?)",
          "args": [123, "a@example.com"]
        }
      }
    ]
  }'
```

If that write times out or returns `ORION_COMMIT_UNKNOWN`, retry the exact
same HTTP body with the exact same idempotency key. If the original committed,
Orion returns the stored result metadata instead of applying the insert again.

## Observability

JSON endpoints:

```bash
curl "$BASE/_orion/metrics/raft"
curl "$BASE/_orion/metrics/idempotency"
curl "$BASE/_orion/metrics/placement"
orion db groups runtime
curl "$BASE/_orion/placement/nodes"
```

SQL operator views are exposed through the reserved `_orion` namespace:

```sql
select * from _orion.raft_metrics;
select * from _orion.storage_pressure;
select * from _orion.database_placement;
select * from _orion.database_standbys;
select * from _orion.placement_metrics;
select * from _orion.placement_nodes;
```

## Storage Model

SQLite writes pages through the Orion VFS. Orion stores page-oriented
materialization in SlateDB. At the Orion key level, each database keeps the
current page image, file size metadata, and file manifests.

This avoids rewriting a whole SQLite database image on every update without
inventing a long-lived page-version history above SQLite. SlateDB owns the
physical immutable SSTs, manifests, checkpoints, and compaction beneath that
logical view.

The high-level shape is:

```text
sqlite/pages/<database>/<path>/size
sqlite/pages/<database>/<path>/current/<page_no>
sqlite/pages/<database>/<path>/manifests/<version>
sqlite/pages/<database>/<path>/latest_manifest
```

## Compatibility Policy

Orion uses SQLite as the SQL engine, so applications get SQLite syntax and
behavior rather than a custom SQL dialect. The service boundary intentionally
denies APIs that would escape tenant isolation or bypass distributed durability,
including arbitrary `ATTACH`, native extension loading, and file-path based
backup surfaces.

Supported and constrained surfaces include:

- ordinary SQL over libSQL/Hrana HTTP
- metadata PRAGMAs such as `table_info`, `index_list`, and `database_list`
- safe per-connection PRAGMAs such as `foreign_keys`, `busy_timeout`, and
  `query_only`
- bounded blob reads and writes through the Orion blob API
- service-owned placement export/import, standby refresh, and promotion

See `docs/sqlite-api-policy.md` and `docs/libsql-blob-api.md` for the current
compatibility policy.

## What To Read Next

- `DEVELOPERS.md`: developer-oriented implementation notes
- `docs/db-placement-dead-source-failover.md`: standby, promotion, and recovery
  runbook
- `docs/sqlite-object-store-layout.md`: SQLite page layout in SlateDB
- `docs/sqlite-compatibility-roadmap.md`: compatibility workstream
- `docs/libsql-blob-api.md`: blob API details

## Current Status

Orion is still under active development. The current repository already runs
single-node, process-based, Docker multi-node, placement, standby, and chaos
tests. The public-facing product direction is a multi-tenant, libSQL-compatible,
object-store-backed database service with placement rules that can span one
zone, one region, multiple regions, or multiple clouds.
