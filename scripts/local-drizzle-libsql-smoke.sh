#!/usr/bin/env bash
set -euo pipefail

if [[ "$(pnpm --version)" != "11.28.5" ]]; then
  echo "Use pnpm 11.28.5 with Node.js 22+ for the smoke tests" >&2
  exit 1
fi

LIBSQL_URL="${LIBSQL_URL:-http://127.0.0.1:8091/drizzle_smoke}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

cd "$workdir"
printf '%s\n' '{"private":true,"packageManager":"pnpm@11.28.5"}' > package.json
cp "$repo_root/scripts/node-install-policy.yaml" pnpm-workspace.yaml
pnpm add --ignore-scripts --save-exact @libsql/client drizzle-orm >/dev/null
cp "$repo_root/scripts/drizzle-libsql-smoke.mjs" "$workdir/smoke.mjs"

LIBSQL_URL="$LIBSQL_URL" node smoke.mjs "$@"
