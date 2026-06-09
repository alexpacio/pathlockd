Stability release: degradation, write-stall, and volume-corruption fixes.
Dedicated writer thread with group commit. 12 new e2e tests. All 149 tests
pass.

## Changes

### Fixed: WAL recovery — AbsoluteConsistency → PointInTime

An unclean shutdown previously left RocksDB refusing to open with corruption
errors, permanently bricking the volume. The store now opens with
`PointInTime` recovery, replaying the WAL to the last consistent point
before the crash. Since acks happen only after fsync, no acknowledged write
can be lost. Verified by a new e2e test that SIGKILLs the daemon and
restarts on the same data dir.

### Fixed: GC cursor persistence

The GC expiry sweep (`state_machine::gc_sweep`) now persists its position in
`meta/gc_cursor` instead of calling `seek_to_first()` every tick. This
prevents the sweep from endlessly re-walking its own tombstone wall — the
primary progressive-slowdown mechanism. The sweep now returns
`(scanned, reclaimed)` and the driver in `main.rs` loops until the backlog
is drained (within a 250 ms budget per tick), so GC throughput adapts to
write rate instead of being capped at 1024 keys/sec.

### Fixed: physical expiry-index maintenance

A new periodic task (`gc_compact_interval_secs`, default 600 s) runs
`delete_file_in_range` + `compact_range` over the already-swept region,
physically reclaiming disk space from tombstone accumulation. Additional
compact-on-deletion collectors and 24 h periodic compaction run on the
churn-heavy column families.

### Fixed: fence index spam

Long-TTL records (fences renew with a 1-day TTL on every heartbeat)
previously accumulated one expiry-index row per renewal. They now share one
hour-quantized expiry-index slot, collapsing repeated renewals into a single
entry. Regression-tested: 1 acquire + 3 renews = exactly 1 index row (was 4).

### Added: dedicated writer thread with group commit

All mutating commands now flow through a single dedicated writer thread with
group commit, replacing the previous `Mutex`-in-`spawn_blocking` approach:
- Bounded write queue (`write_queue_depth`, default 1024) rejects overflow
  with `gRPC UNAVAILABLE` — honest backpressure instead of unbounded thread
  pileup.
- One WAL fsync per drained group of up to 256 commands, before any ack.
  Same durability, ~order-of-magnitude better saturation throughput.
- Fail-stop poisoning on fsync failure.
- Monotone `now_ms` clamp so clock steps cannot reorder lease expiry.

### Added: RocksDB tuning (DbTuning)

Configurable RocksDB tuning with sensible defaults:
- Bounded total WAL (512 MB) — cold CFs can no longer pin gigabytes of WAL.
- 4 background jobs, bloom filters, shared block cache, 16 MB write buffers,
  dynamic level bytes.
- All settings overridable via config or environment variables.

### Added: real health check

`Health` now round-trips an `Op::Noop` through the writer with a 2 s
deadline. A wedged write path finally turns the node not-ready instead of
staying green indefinitely.

### Fixed: async runtime freezes

All read RPCs moved to `spawn_blocking` so a slow RocksDB scan can no longer
freeze the entire async runtime (and with it every other RPC including
Health).

### Fixed: read-your-writes in command validation

Every read inside a command now sees the command's own pending writes via an
in-WriteBatch overlay (`WriteTxn`). Fixes the dead-owner case where
validation pruned a stale lock but execution re-read the committed record
and returned a bogus `Conflict`. Regression-tested.

### Fixed: discard-on-fail

Commands ending in `Conflict` or `Lost` no longer commit their partial
writes (phantom owner-set entries, half-applied grants).
Regression-tested.

### Fixed: oversized owner sets

- `release_all` and `force_release` now paginate internally (`smembers_page`,
  4096/page) instead of erroring at 65,536 members.
- Regression-tested with a 66,000-member owner (renew fails bounded,
  `force_release` fully recovers).
- `smembers_limited` now counts live members with a 4× raw-scan cap, so
  expired residue cannot cause spurious `RESOURCE_EXHAUSTED`.
- Unified the duplicated `SetScanLimitExceeded` types — the write path
  previously returned `INTERNAL` instead of `RESOURCE_EXHAUSTED` for this
  case.

### Added: DumpLocks implementation

`DumpLocks` was a stub returning empty. Now does a paginated scan over owner
holds with fences, skipping dead owners.

### Changed: clustering honesty

- Startup logs a prominent warning (and README, compose, and example TOML
  now state plainly) that multi-replica is unsafe until Raft lands — locks
  are per-node, fencing is per-node; only event fan-out crosses instances.
- `replication_factor` default changed from 3 to 1.
- `PATHLOCKD_BOOTSTRAP` and `PATHLOCKD_JOIN` env vars are now actually
  parsed (the compose file was setting one that was silently ignored).

### Removed: dead code

- `macros.rs`: `route_retry_once` never retried.
- `NotLeader` / `QuorumUnavailable` / `WrongGroup` placeholder errors.
- Misleading `rd_key` / `own_key` helpers whose documented layout did not
  match disk format.
- Nonexistent `request_dedupe` CF doc comment.

### Added: observability

- `pathlockd.gc.scanned` counter.
- `pathlockd.writer.queue_depth` gauge.

## Operational note

Existing volumes are serviceable when upgrading — the first GC pass chews
through the accumulated expiry backlog in 250 ms slices, and the first
maintenance tick physically drops the old tombstone region. You do not need
to wipe the volume when upgrading.

The one case where a wipe is still needed: a volume that was already
corrupted by an unclean shutdown on a previous version and currently refuses
to open. The new `PointInTime` recovery prevents that class of corruption
going forward but cannot retroactively repair already-bricked volumes.

## Upgrade note

No API changes. Configuration additions are optional with safe defaults.
Backward-compatible with existing volumes (see operational note above).

- **New optional config fields:** `write_queue_depth` (default 1024),
  `gc_compact_interval_secs` (default 600), and `DbTuning` sub-table for
  RocksDB options.
- **Changed default:** `replication_factor` is now 1. Multi-replica is not
  yet safe; see clustering honesty note above.
- **New metrics:** `pathlockd.gc.scanned`, `pathlockd.writer.queue_depth`.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.7.0-linux-amd64.tar.gz` - optimized, stripped release binary.
- `pathlockd-0.7.0-linux-amd64-debug.tar.gz` - unoptimized binary with debug info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.7.0   # amd64 + arm64
```
