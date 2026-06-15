#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workdir="${ORION_BLOB_SMOKE_DIR:-$(mktemp -d)}"
keep_workdir="${ORION_BLOB_SMOKE_KEEP:-0}"
raft_port="${ORION_BLOB_SMOKE_RAFT_PORT:-17701}"
http_port="${ORION_BLOB_SMOKE_HTTP_PORT:-18691}"
database="${ORION_BLOB_SMOKE_DATABASE:-blob_smoke}"
config="$workdir/node.yaml"
log_file="$workdir/orion.log"
pid=""

if [[ "$keep_workdir" != "1" ]]; then
  trap 'cleanup' EXIT
else
  trap 'stop_orion' EXIT
fi

cleanup() {
  stop_orion
  rm -rf "$workdir"
}

stop_orion() {
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill -INT "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  pid=""
}

mkdir -p "$workdir"

cat >"$config" <<YAML
node_id: 1
roles: ["all"]
raft_addr: "127.0.0.1:${raft_port}"
topology:
  cloud: "local"
  region: "local"
  zone: "local"
rocksdb_path: "$workdir/raft"
slatedb_path: "node-1/state"
object_store:
  type: local
  root: "$workdir/object-store"
peers: []
bootstrap: true
sqlite:
  cache_root: "$workdir/sqlite-cache"
libsql_http:
  bind_addr: "127.0.0.1:${http_port}"
  session_idle_timeout_ms: 300000
  auth:
    tokens: []
YAML

echo "building orion binary"
(cd "$repo_root" && cargo build --bin orion >/dev/null)

DYLD_FALLBACK_LIBRARY_PATH="${DYLD_FALLBACK_LIBRARY_PATH:-/Library/Developer/CommandLineTools/usr/lib}" \
  "$repo_root/target/debug/orion" server --config "$config" >"$log_file" 2>&1 &
pid="$!"

deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
  if node -e "fetch('http://127.0.0.1:${http_port}/${database}/v2').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"; then
    break
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "orion exited before HTTP became ready" >&2
    tail -n 200 "$log_file" >&2 || true
    exit 1
  fi
  sleep 0.25
done

if ! kill -0 "$pid" 2>/dev/null; then
  echo "orion exited before blob API smoke could run" >&2
  tail -n 200 "$log_file" >&2 || true
  exit 1
fi

if (( SECONDS >= deadline )); then
  echo "timed out waiting for libSQL HTTP on port ${http_port}" >&2
  tail -n 200 "$log_file" >&2 || true
  exit 1
fi

echo "running blob API smoke"
node "$repo_root/scripts/libsql-blob-api-smoke.mjs" \
  "http://127.0.0.1:${http_port}/${database}"

echo "blob API smoke passed"
if [[ "$keep_workdir" == "1" ]]; then
  echo "kept workdir: $workdir"
fi
