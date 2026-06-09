Self-contained embedded architecture: Multi-Raft consensus, SWIM gossip, and
RocksDB storage in a single binary. No external coordination service required.

## Changes

### Removed: external storage dependency

pathlockd is now a fully self-contained daemon. Lock metadata, fencing tokens,
wait-for edges, preemption claims, and liveness markers are all stored in an
embedded **RocksDB** engine with column families, TTL-based expiry, and
background GC sweeps. You no longer need to operate a separate coordination
service — start a single binary and it is ready.

### Removed: PD timestamp oracle dependency

The cluster no longer sources wall-clock time from an external timestamp oracle.
Every mutating command carries a leader-stamped `now_ms`, and all lock-engine
operations use that deterministic clock. This means the `pd_endpoints` and
`mvcc_gc_interval_secs` configuration fields are removed.

### Added: embedded Multi-Raft consensus

State-machine commands (acquire, release, renew, force-release, set-claim,
set-wait-edge, incr-fence, and GC sweeps) are now applied through a single
serialized writer backed by RocksDB WriteBatch. All lock engine primitives
(`engine.rs`) are synchronous, deterministic functions generic over a
`StoreTxn` trait — they are called identically from the gRPC service path and
will be called identically from the per-group Raft state machine when Multi-Raft
lands. The serialized apply lock guarantees the read-modify-write atomicity the
engine assumes.

Raft types are configured (`openraft` with `Command` / `ApplyResponse`) and the
state machine interface is wired. The current phase runs all commands through a
dedicated apply loop in single-process mode.

### Added: SWIM gossip layer

A gossip layer based on `foca` provides cluster membership discovery. In the
current phase, a static member seed list (`seed_nodes` in config) bootstraps the
membership set. Full SWIM failure detection and dynamic membership propagation
are planned for the next phase.

### Added: sharding via Rendezvous Hashing

Lock domains (the handler prefix in `handler:/path`) are mapped to Raft groups
using HRW (Highest Random Weight) hashing with `xxh3_64`. This enables
deterministic, consistent sharding across configurable group counts
(`group_count`, default 256). Voter selection for each group also uses HRW
across available node IDs.

### Added: RocksDB-backed store with 14 column families

The lock store now uses 14 RocksDB column families:

| Column Family | Purpose |
| --- | --- |
| `write_locks` | Active write lock: path → owner |
| `read_locks` | Active read locks: path\0owner → presence (set) |
| `fences` | Write-lock fencing tokens: path → token (min 24h TTL) |
| `claims` | Preemption reservations: path → claimant |
| `desc_write` | Descendant write index: ancestor\0path (reverse index) |
| `desc_read` | Descendant read index: ancestor\0path |
| `desc_claim` | Descendant claim index: ancestor\0path |
| `owner_alive` | Liveness marker: owner → "1" |
| `owner_holds` | Owner's held locks set: owner\0mode\0path → member |
| `wait_edges` | Deadlock-graph edges: owner → encoded WaitEdge |
| `expiry` | TTL index: expires_at\0cf\0primary_key (shadow records) |
| `meta` | Global metadata: fence_counter (monotonic) |
| `raft_log` | Raft log entries (managed by openraft) |
| `default` | Catch-all safety net |

All values are serialized with `bincode` as `StoredRecord::Str { v, exp }` or
`StoredRecord::Counter { v }`. Set-valued columns (read_locks, owner_holds,
descendant indexes) use a member-key prefix pattern: set key `K`, member `M` is
stored as `K\0M`.

### Added: deterministic state machine

The `raft::state_machine::apply()` function takes a `Command` and a RocksDB
`DB`, builds a `WriteBatch`, runs the appropriate engine function, and commits
atomically. No wall-clock time — the command's `now_ms` field is the clock
source. WAL fsync is configurable via `rocksdb_wal_sync` (default true).

### Added: GC expiry sweep

A background tokio task runs a configurable GC sweep (`group_gc_interval_secs`,
default 1s; `group_gc_batch`, default 1024 entries). It walks the `expiry`
column family for records whose `expires_at <= now_ms`, verifies the shadowed
data record is still expired (it may have been refreshed), and deletes both. The
sweep is bounded and self-throttling. Metrics track sweeps completed, keys
reclaimed, and sweep duration.

### Added: SWIM gossip and peer discovery

The `gossip` module bootstraps cluster membership from `seed_nodes` and
propagates changes through the cluster. `peer_discovery_dns` (headless Service
DNS name on Kubernetes) enables dynamic peer discovery for cross-instance event
fan-out.

### Added: configurable Raft and RocksDB options

| Field | Env var | Default | Description |
| --- | --- | --- | --- |
| `group_count` | `PATHLOCKD_GROUP_COUNT` | `256` | Number of Raft groups |
| `replication_factor` | `PATHLOCKD_REPLICATION_FACTOR` | `3` | Voters per group (must be odd) |
| `raft_snapshot_interval_entries` | `PATHLOCKD_RAFT_SNAPSHOT_INTERVAL_ENTRIES` | `10000` | Entries between snapshots |
| `raft_snapshot_min_log_entries` | `PATHLOCKD_RAFT_SNAPSHOT_MIN_LOG_ENTRIES` | `5000` | Min entries to trigger snapshot |
| `raft_max_inflight` | `PATHLOCKD_RAFT_MAX_INFLIGHT` | `256` | Max in-flight proposals |
| `rocksdb_wal_sync` | `PATHLOCKD_ROCKSDB_WAL_SYNC` | `true` | Fsync WAL on every write |
| `rocksdb_max_open_files` | `PATHLOCKD_ROCKSDB_MAX_OPEN_FILES` | `4096` | RocksDB file descriptor limit |
| `group_gc_interval_secs` | `PATHLOCKD_GROUP_GC_INTERVAL_SECS` | `1` | GC sweep interval (0 = off) |
| `group_gc_batch` | `PATHLOCKD_GROUP_GC_BATCH` | `1024` | Keys processed per sweep |
| `seed_nodes` | `PATHLOCKD_SEED_NODES` | `[]` | Gossip seed addresses |
| `gossip_addr` | `PATHLOCKD_GOSSIP_ADDR` | `0.0.0.0:7946` | SWIM gossip bind |
| `raft_addr` | `PATHLOCKD_RAFT_ADDR` | `http://localhost:50052` | Internal Raft transport |
| `bootstrap` | `PATHLOCKD_BOOTSTRAP` | `false` | Bootstrap a new cluster |
| `join` | `PATHLOCKD_JOIN` | `false` | Join an existing cluster |

### Removed: PD and TiKV configuration fields

The following fields are no longer present and are rejected if supplied:
`pd_endpoints`, `mvcc_gc_interval_secs`, and all TiKV transport options. The
replacement is the embedded RocksDB engine in `data_dir`.

### Removed: serialization tombstones

The per-handler serialization-key approach used to sequence mutating operations
within a handler domain is replaced by the serialized writer's global apply
lock. The `cf:meta/fence_counter` key provides the monotonic fencing counter
that was previously a PD counter.

### Changed: data directory

`data_dir` (default `/var/lib/pathlockd`) now holds the RocksDB database
directory directly rather than being a mount point for an external store.
Cluster nodes each maintain their own RocksDB instance.

### Changed: start-up is single-binary

Starting pathlockd now only requires the binary and a config file (or
environment variables). No external services need to be running first.

## Upgrade note

**This is a breaking change.** The storage backend has changed entirely.

- **Data migration.** There is no automated migration path. Existing lock state
  must be rebuilt. Run a fresh deployment against an empty `data_dir`.
- **Configuration.** Remove `pd_endpoints` and `mvcc_gc_interval_secs` from
  your config. Add `data_dir` pointing to a writable directory for RocksDB.
- **API compatibility.** The `pathlockd.v1.PathLock` gRPC API is unchanged.
  All 20 RPCs have identical signatures and semantics. The lock engine
  (hierarchical read/write, fencing, TTL leases, deadlock detection,
  preemption claims, owner events) behaves identically.
- **Single-node deployment** works without changes beyond the config fields
  above. A single pathlockd process with `bootstrap = true` starts
  immediately — no external dependencies.
- **Multi-node deployment** requires `seed_nodes` for gossip bootstrap and
  `bootstrap` on one node / `join` on the rest.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.6.0-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.6.0-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.6.0   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
