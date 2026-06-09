#!/usr/bin/env bash
# Integration tests — engine tests against RocksDB, plus e2e daemon tests.
# Uses --test-threads=1 for e2e tests to avoid port conflicts.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test --test engine_tests
cargo test --test e2e_tests -- --test-threads=1
