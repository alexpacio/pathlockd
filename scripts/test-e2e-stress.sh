#!/usr/bin/env bash
# Run chaos / resilience tests against the embedded RocksDB engine.
#
# Tests crash recovery from the RocksDB WAL, crash-before-apply scenarios,
# and checkpoint/restore consistency — all in-process, no external services.
#
# Usage:
#   scripts/test-e2e-stress.sh
#   scripts/test-e2e-stress.sh crash_recovery_from_wal
set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

args=("$@")
net=""

note "Running chaos/resilience suite (RocksDB crash recovery)…"

status=0
rust_run "$net" \
  'cargo test --test chaos -- "$@"' \
  "${args[@]}" || status=$?

exit "$status"
