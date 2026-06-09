Cleanup release: removed all vestigial TiKV/PD references from code, docs,
scripts, and proto comments. The project is now fully self-contained with
embedded Multi-Raft + RocksDB.

## Changes

### Removed: all TiKV and PD references

The codebase, documentation (`llmwiki/`), test scripts, Dockerfile comments,
and proto file header no longer mention TiKV, PD, MVCC GC, timestamp oracles,
or optimistic transaction retry loops. All of these were replaced by the
embedded Multi-Raft + RocksDB engine in v0.6.0; this release removes the
remaining legacy text that described the old architecture.

### Removed: stale scripts

- `scripts/infra.sh` — managed the old TiKV/PD cluster lifecycle.
- `scripts/test-in-docker.sh` — stale alias for `test-integration.sh`.

Tests now run directly against the in-process RocksDB engine (`cargo test
--test engine_tests`, `cargo test --test chaos`) or a spawned daemon over
gRPC (`cargo test --test e2e_tests`). No external services required.

### Fixed: Dockerfile comments

- Build dependency comment now describes RocksDB C++ compilation instead of
  TiKV's `grpcio` / `tikv-client`.
- Healthcheck comment no longer references TiKV reachability.

### Fixed: internal log prefixes

Two `warn!` calls in `src/engine.rs` dropped the obsolete `fslock:` prefix.

## Upgrade note

No API, configuration, or data changes. No migration required. The
`pathlockd.v1.PathLock` gRPC API and the RocksDB column family layout are
unchanged.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.6.2-linux-amd64.tar.gz` - optimized, stripped release binary.
- `pathlockd-0.6.2-linux-amd64-debug.tar.gz` - unoptimized binary with debug info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.6.2   # amd64 + arm64
```
