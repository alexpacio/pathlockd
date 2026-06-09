#!/usr/bin/env bash
# Unit tests — runs fast, no external dependencies needed.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test --lib
