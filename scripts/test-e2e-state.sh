#!/usr/bin/env bash
# Run the state-transition integration tests against an embedded RocksDB engine.
#
# These tests exercise every lock engine primitive (acquire, release, renew,
# force-release, fencing, deadlock detection, GC) directly against the in-process
# RocksDB state machine — no external services required.
#
# Usage:
#   scripts/test-e2e-state.sh
#   scripts/test-e2e-state.sh acquire_write_lock
set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

args=("$@")
net=""

note "Running engine integration suite (RocksDB-backed state machine)…"

status=0
rust_run "$net" \
  'cargo test --test engine_tests -- "$@"' \
  "${args[@]}" || status=$?

exit "$status"
