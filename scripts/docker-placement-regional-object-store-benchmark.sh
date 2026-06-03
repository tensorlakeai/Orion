#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export ORION_DOCKER_PROJECT="${ORION_DOCKER_PROJECT:-orion-placement-benchmark-regional}"
export ORION_DOCKER_OBJECT_STORE_MODE=regional
exec node "$repo_root/scripts/docker-placement-benchmark.mjs" "$@"
