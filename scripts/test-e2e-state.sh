#!/usr/bin/env bash
# Run the daemon-level state-transition e2e test against a real TiKV cluster.
#
# The test starts a pathlockd daemon, drives it over gRPC, then inspects TiKV
# directly to prove lock keys, owner sets, descendant indexes, fences, wait
# edges, renewals, TTL expiry and GC cleanup are persisted as expected.
#
# Usage:
#   scripts/test-e2e-state.sh
#   scripts/test-e2e-state.sh --no-up
#   scripts/test-e2e-state.sh --down
set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

DO_UP=1
DO_DOWN=0
args=()
for a in "$@"; do
  case "$a" in
    --no-up) DO_UP=0 ;;
    --down)  DO_DOWN=1 ;;
    --)      ;;
    *)       args+=("$a") ;;
  esac
done

need_docker
[ "$DO_UP" -eq 1 ] && "$SCRIPT_DIR/infra.sh" up

net="$(dev_network)"
export PATHLOCKD_PD_ENDPOINTS="pd:2379"
note "Running e2e state-transition suite on network '$net'..."

status=0
rust_run "$net" \
  'cargo test --test e2e_state -- --test-threads=1 "$@"' \
  "${args[@]}" || status=$?

if [ "$DO_DOWN" -eq 1 ]; then
  note "Tearing the cluster down (--down)..."
  "$SCRIPT_DIR/infra.sh" down
fi

exit "$status"
