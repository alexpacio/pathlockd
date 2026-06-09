#!/usr/bin/env bash
# Run all tests: unit + integration.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test
