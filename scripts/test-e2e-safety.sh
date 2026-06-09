#!/usr/bin/env bash
# Run the end-to-end daemon tests against a real pathlockd process over gRPC.
#
# Spawns pathlockd in single-node mode, drives it over gRPC, and validates
# lock correctness, fencing token monotonicity, deadlock resolution, and
# GC drain — all against the embedded RocksDB engine.
#
# Usage:
#   scripts/test-e2e-safety.sh
#   scripts/test-e2e-safety.sh hierarchical_locks_containment
set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

args=("$@")
net=""

note "Running e2e daemon suite (gRPC, embedded RocksDB)…"

status=0
rust_run "$net" \
  'cargo test --test e2e_tests -- "$@"' \
  "${args[@]}" || status=$?

exit "$status"
