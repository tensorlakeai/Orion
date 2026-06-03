#!/usr/bin/env bash
set -euo pipefail

LIBSQL_URL="${LIBSQL_URL:-http://127.0.0.1:8091/appdb}"
LIBSQL_CLIENT_PACKAGE="${LIBSQL_CLIENT_PACKAGE:-@libsql/client}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

cd "$workdir"
npm init -y >/dev/null
npm install "$LIBSQL_CLIENT_PACKAGE" >/dev/null
cp "$repo_root/scripts/official-libsql-client-smoke.mjs" "$workdir/smoke.mjs"

LIBSQL_URL="$LIBSQL_URL" node smoke.mjs "$@"
