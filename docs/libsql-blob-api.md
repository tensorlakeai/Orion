# libSQL Blob API

Orion supports SQLite incremental blob access through the libSQL/Hrana
service boundary. The API is session-scoped, database-scoped, and implemented
on top of SQLite `zeroblob(...)` plus incremental blob reads and writes.

This is a query-protocol feature. It is not a libSQL replication or frame-sync
API.

## Model

Clients create blob storage with normal SQL:

```sql
create table files (
  id integer primary key,
  data blob not null
);

insert into files (id, data) values (1, zeroblob(1048576));
```

Then the client opens a blob handle for a specific row and column, reads or
writes chunks by offset, and closes the handle.

Blob handles belong to the session baton that opened them. A handle cannot be
used from another database or another baton, and it is invalidated when the
session is closed or reaped for idleness.

## Limits

`libsql_http.blob_max_chunk_bytes` limits each blob read or write chunk.

The default is `524288` bytes, matching Orion's default large Raft payload
chunk size. This keeps one client blob chunk aligned with one internal durable
payload chunk by default.

The limit applies to:

- JSON/base64 read `length`.
- JSON/base64 write payloads before base64 decode, using the maximum encoded
  length implied by the configured binary limit.
- JSON/base64 write payloads after base64 decode.
- Raw HTTP binary request bodies.
- WebSocket binary frames.

Requests over the limit fail with a SQLite-style error response. Clients should
split large blobs into multiple chunks no larger than the configured limit.

## JSON HTTP API

The JSON API uses base64 for blob bytes because JSON does not carry raw binary
data.

Endpoints are available at both the default database path and named database
paths:

```text
POST /v2/blob/open
POST /v2/blob/read
POST /v2/blob/write
POST /v2/blob/reopen
POST /v2/blob/close

POST /{database}/v2/blob/open
POST /{database}/v2/blob/read
POST /{database}/v2/blob/write
POST /{database}/v2/blob/reopen
POST /{database}/v2/blob/close
```

### Open

```json
{
  "baton": "optional-session-baton",
  "schema": "main",
  "table": "files",
  "column": "data",
  "rowid": 1,
  "read_only": false
}
```

`schema` is optional. `read_only` defaults to `true`.

Response:

```json
{
  "baton": "session-baton",
  "result": {
    "type": "open",
    "blob_id": "handle-id",
    "size": 1048576,
    "read_only": false
  }
}
```

### Read

```json
{
  "baton": "session-baton",
  "blob_id": "handle-id",
  "offset": 0,
  "length": 65536
}
```

Response:

```json
{
  "baton": "session-baton",
  "result": {
    "type": "read",
    "blob_id": "handle-id",
    "offset": 0,
    "bytes_read": 65536,
    "base64": "AAECAwQ=",
    "size": 1048576
  }
}
```

### Write

```json
{
  "baton": "session-baton",
  "blob_id": "handle-id",
  "offset": 0,
  "base64": "AAECAwQ="
}
```

Response:

```json
{
  "baton": "session-baton",
  "result": {
    "type": "write",
    "blob_id": "handle-id",
    "offset": 0,
    "bytes_written": 5,
    "size": 1048576
  }
}
```

### Reopen

`reopen` retargets an existing blob handle to a different rowid of the same
table and column.

```json
{
  "baton": "session-baton",
  "blob_id": "handle-id",
  "rowid": 2
}
```

Response:

```json
{
  "baton": "session-baton",
  "result": {
    "type": "reopen",
    "blob_id": "handle-id",
    "rowid": 2,
    "size": 4096
  }
}
```

### Close

```json
{
  "baton": "session-baton",
  "blob_id": "handle-id"
}
```

Response:

```json
{
  "baton": "session-baton",
  "result": {
    "type": "close",
    "blob_id": "handle-id"
  }
}
```

## Raw HTTP Binary API

The raw binary endpoints avoid base64 overhead for chunk transfer. Orion
exposes both single-chunk endpoints and streaming endpoints.

`read-bytes` and `write-bytes` are single bounded chunk operations. Each
request must fit within `libsql_http.blob_max_chunk_bytes`.

```text
GET  /v2/blob/read-bytes?baton=...&blob_id=...&offset=0&length=65536
POST /v2/blob/write-bytes?baton=...&blob_id=...&offset=0

GET  /{database}/v2/blob/read-bytes?baton=...&blob_id=...&offset=0&length=65536
POST /{database}/v2/blob/write-bytes?baton=...&blob_id=...&offset=0
```

`read-bytes` returns `application/octet-stream` in the response body.
`write-bytes` accepts an `application/octet-stream` request body and returns an
empty body on success.

`read-stream` and `write-stream` are large transfer operations. The server
internally reads or writes bounded chunks no larger than
`libsql_http.blob_max_chunk_bytes` while applying HTTP backpressure.

```text
GET  /v2/blob/read-stream?baton=...&blob_id=...&offset=0&length=10485760
POST /v2/blob/write-stream?baton=...&blob_id=...&offset=0&length=10485760

GET  /{database}/v2/blob/read-stream?baton=...&blob_id=...&offset=0&length=10485760
POST /{database}/v2/blob/write-stream?baton=...&blob_id=...&offset=0&length=10485760
```

`read-stream` streams the response body from SQLite through a bounded channel.
If the client disconnects, the read task stops early.

`write-stream` streams the request body into a single SQLite incremental blob
writer. It requires either a `length` query parameter or `Content-Length`; when
both are present, they must match. Orion validates the target blob range
before writing the first byte. If a client disconnects or sends fewer bytes
than declared, already-written bytes remain subject to the current SQLite
transaction. Use an explicit transaction when the application needs failed
uploads to roll back atomically.

Successful binary responses include metadata headers:

| Header | Meaning |
| --- | --- |
| `x-orion-session-token` | The session baton to reuse. |
| `x-orion-blob-id` | The blob handle id. |
| `x-orion-blob-offset` | The requested offset. |
| `x-orion-blob-size` | The total blob size. |
| `x-orion-blob-bytes-read` | Bytes returned by `read-bytes`. |
| `x-orion-blob-bytes-written` | Bytes accepted by `write-bytes`. |

Errors use the same JSON error body shape as the JSON blob API.

## WebSocket API

The WebSocket API extends Hrana request messages with blob operations. JSON
control messages use the normal Hrana request envelope:

```json
{
  "type": "request",
  "request_id": 7,
  "request": {
    "type": "blob_open",
    "stream_id": 1,
    "table": "files",
    "column": "data",
    "rowid": 1,
    "read_only": false
  }
}
```

Supported JSON control request types:

- `blob_open`
- `blob_read`
- `blob_write`
- `blob_reopen`
- `blob_close`
- `blob_read_bytes`
- `blob_write_bytes`

`blob_read` and `blob_write` carry base64 bytes in JSON, matching the HTTP JSON
API.

`blob_write_bytes` is a two-step operation:

1. Send a JSON `blob_write_bytes` control message with `stream_id`, `blob_id`,
   and `offset`.
2. Send exactly one binary WebSocket frame containing the raw chunk.

The server sends the `blob_write_bytes` response after receiving and applying
the binary frame.

`blob_read_bytes` returns two frames:

1. A JSON `response_ok` frame with `blob_read_bytes` metadata.
2. One binary WebSocket frame containing the raw bytes.

Only one pending `blob_write_bytes` frame is allowed per WebSocket connection at
a time. A binary frame without a pending `blob_write_bytes` request is a
protocol error.

## Transaction And Lifetime Semantics

Blob operations execute on the SQLite connection associated with the session
baton. If the baton has an explicit SQLite transaction open, blob operations on
that same baton participate in that session state. Autocommit behavior follows
SQLite.

Current lifetime rules:

- Handles are scoped to one baton and one database.
- `close` invalidates the handle immediately.
- Closing a Hrana session invalidates all blob handles in that session.
- Idle session reaping invalidates all blob handles in that session.
- Read-only handles reject writes.
- Out-of-range offsets and writes use SQLite's native blob errors.
- If the underlying row is deleted or changed so the blob can no longer be
  opened, subsequent operations fail with a SQLite-style error.

The API does not expose libSQL replication state, WAL frame numbers, or
generation metadata.

## Testing

Process smoke:

```bash
scripts/process-blob-api-smoke.sh
```

Focused Rust tests:

```bash
DYLD_FALLBACK_LIBRARY_PATH=/Library/Developer/CommandLineTools/usr/lib \
  cargo test -p orion libsql_http::tests::router_blob_api -- --nocapture
```

Full workspace:

```bash
DYLD_FALLBACK_LIBRARY_PATH=/Library/Developer/CommandLineTools/usr/lib \
  cargo test --workspace
```

The blob API test suite should cover:

- JSON/base64 open, read, write, reopen, close.
- Raw HTTP binary `read-bytes` and `write-bytes`.
- WebSocket JSON blob control messages.
- WebSocket binary read and write frames.
- Chunk limit enforcement.
- Read-only write rejection.
- Out-of-range writes.
- Handle invalidation after close, session close, and idle reaping.
- Wrong database or wrong baton usage.
- Row deletion after open.
- Multi-node and failover behavior before declaring launch readiness.
