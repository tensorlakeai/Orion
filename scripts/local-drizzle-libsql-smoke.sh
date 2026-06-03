#!/usr/bin/env bash
set -euo pipefail

LIBSQL_URL="${LIBSQL_URL:-http://127.0.0.1:8091/drizzle_smoke}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

cd "$workdir"
npm init -y >/dev/null
npm install @libsql/client drizzle-orm >/dev/null
cp "$repo_root/scripts/drizzle-libsql-smoke.mjs" "$workdir/smoke.mjs"

LIBSQL_URL="$LIBSQL_URL" node smoke.mjs "$@"
