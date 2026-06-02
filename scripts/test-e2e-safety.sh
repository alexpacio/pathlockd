#!/usr/bin/env bash
# Run the daemon-level e2e SAFETY suite against a real TiKV cluster: hierarchical
# read/write contention across many paths, manufactured deadlock cycles resolved
# with the full client protocol (wait edge → DetectCycle → RequestRevoke →
# ForceRelease), an in-memory mutual-exclusion oracle, fencing-token monotonicity,
# a hard liveness budget, and a post-run "no poisoning" keyspace census. Also runs
# the focused preemption-claim test.
#
# Usage:
#   scripts/test-e2e-safety.sh
#   PATHLOCKD_E2E_SAFETY_CONTENDERS=32 PATHLOCKD_E2E_SAFETY_OPS=80 scripts/test-e2e-safety.sh
#   PATHLOCKD_E2E_SAFETY_DEADLOCK_GROUPS=12 PATHLOCKD_E2E_SAFETY_DEADLOCK_ROUNDS=20 scripts/test-e2e-safety.sh
#   PATHLOCKD_E2E_SAFETY_REPLICAS=3 scripts/test-e2e-safety.sh
#   scripts/test-e2e-safety.sh --no-up            # cluster already running
#
# Tunables (env, with the test's built-in defaults):
#   PATHLOCKD_E2E_SAFETY_REPLICAS         peered daemons               (2)
#   PATHLOCKD_E2E_SAFETY_CONTENDERS       random-contention workers    (12)
#   PATHLOCKD_E2E_SAFETY_OPS              ops per contention worker     (25)
#   PATHLOCKD_E2E_SAFETY_DEADLOCK_GROUPS  manufactured-cycle groups     (6)
#   PATHLOCKD_E2E_SAFETY_DEADLOCK_SIZE    members (=cycle length)/group (3)
#   PATHLOCKD_E2E_SAFETY_DEADLOCK_ROUNDS  rounds per group              (8)
#   PATHLOCKD_E2E_SAFETY_HANDLERS         handler namespaces            (8)
#   PATHLOCKD_E2E_SAFETY_TTL_MS           lease TTL                 (15000)
#   PATHLOCKD_E2E_SAFETY_DEADLINE_SECS    liveness budget             (300)
#   PATHLOCKD_E2E_SAFETY_DRAIN_SECS       post-run drain timeout      (150)
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

note "Running e2e safety/deadlock suite on network '$net'…"

# Forward the PATHLOCKD_E2E_SAFETY_* tunables into the container (only those that
# are set), then run the focused claim test followed by the comprehensive one,
# both with --nocapture so the run summary and any failure detail are visible.
safety_env=()
for v in REPLICAS CONTENDERS OPS DEADLOCK_GROUPS DEADLOCK_SIZE DEADLOCK_ROUNDS HANDLERS TTL_MS DEADLINE_SECS DRAIN_SECS; do
  name="PATHLOCKD_E2E_SAFETY_${v}"
  [ -n "${!name:-}" ] && safety_env+=( -e "${name}=${!name}" )
done

status=0
PATHLOCKD_EXTRA_ENV="${safety_env[*]:-}" rust_run "$net" '
  cargo test --test e2e_stress -- --test-threads=1 --nocapture \
    preemption_claim_blocks_victim_reacquire \
    hierarchical_contention_and_deadlocks_stay_safe_and_drain "$@"
' "${args[@]}" || status=$?

if [ "$DO_DOWN" -eq 1 ]; then
  note "Tearing the cluster down (--down)…"
  "$SCRIPT_DIR/infra.sh" down
fi

exit "$status"
