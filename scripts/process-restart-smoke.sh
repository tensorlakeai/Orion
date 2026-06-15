#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workdir="${ORION_PROCESS_SMOKE_DIR:-$(mktemp -d)}"
keep_workdir="${ORION_PROCESS_SMOKE_KEEP:-0}"
raft_port="${ORION_PROCESS_SMOKE_RAFT_PORT:-17101}"
http_port="${ORION_PROCESS_SMOKE_HTTP_PORT:-18091}"
database="${ORION_PROCESS_SMOKE_DATABASE:-process_smoke}"
config="$workdir/node.yaml"
log1="$workdir/orion-first.log"
log2="$workdir/orion-second.log"
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

start_orion() {
  local log_file="$1"
  DYLD_FALLBACK_LIBRARY_PATH="${DYLD_FALLBACK_LIBRARY_PATH:-/Library/Developer/CommandLineTools/usr/lib}" \
    "$repo_root/target/debug/orion" server --config "$config" >"$log_file" 2>&1 &
  pid="$!"
  wait_for_http
}

wait_for_http() {
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if node -e "fetch('http://127.0.0.1:${http_port}/${database}/v2').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "orion exited before HTTP became ready" >&2
      tail -n 200 "$log1" "$log2" 2>/dev/null >&2 || true
      exit 1
    fi
    sleep 0.25
  done
  echo "timed out waiting for libSQL HTTP on port ${http_port}" >&2
  tail -n 200 "$log1" "$log2" 2>/dev/null >&2 || true
  exit 1
}

pipeline() {
  local payload="$1"
  node - "$http_port" "$database" "$payload" <<'NODE'
const [port, database, payload] = process.argv.slice(2);
const response = await fetch(`http://127.0.0.1:${port}/${database}/v2/pipeline`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: payload,
});
if (!response.ok) {
  throw new Error(`HTTP ${response.status}: ${await response.text()}`);
}
const body = await response.json();
for (const result of body.results ?? []) {
  if (result.type === "error") {
    throw new Error(`${result.error?.code ?? "ERROR"}: ${result.error?.message ?? "unknown"}`);
  }
}
console.log(JSON.stringify(body));
NODE
}

write_payload='{"requests":[{"type":"execute","stmt":{"sql":"create table if not exists process_smoke (id integer primary key, value text not null)"}},{"type":"execute","stmt":{"sql":"delete from process_smoke"}},{"type":"execute","stmt":{"sql":"insert into process_smoke values (1, '\''survived-restart'\'')"}}]}'
read_payload='{"requests":[{"type":"execute","stmt":{"sql":"select value from process_smoke where id = 1","want_rows":true}}]}'

echo "starting first orion process"
start_orion "$log1"
pipeline "$write_payload" >/dev/null

echo "stopping first orion process"
stop_orion

echo "starting second orion process"
start_orion "$log2"
read_result="$(pipeline "$read_payload")"

node - "$read_result" <<'NODE'
const body = JSON.parse(process.argv[2]);
const value = body.results?.[0]?.response?.result?.rows?.[0]?.[0]?.value;
if (value !== "survived-restart") {
  throw new Error(`expected survived-restart after restart, got ${JSON.stringify(value)}`);
}
NODE

echo "process restart smoke passed"
if [[ "$keep_workdir" == "1" ]]; then
  echo "kept workdir: $workdir"
fi
