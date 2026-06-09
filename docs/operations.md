# pathlockd Operations Guide

## Overview

pathlockd is a self-contained distributed lock service. Each node runs an embedded
Multi-Raft stack with RocksDB for durable storage. Cluster discovery uses
SWIM/foca; lock correctness is provided by Raft log order, linearizable reads,
TTL leases, and fencing tokens.

## Configuration

Configuration is loaded from lowest to highest precedence:

1. Built-in defaults
2. TOML config file (`--config <path>` or `PATHLOCKD_CONFIG` env)
3. Individual `PATHLOCKD_*` environment variables

### Required settings

| Field | Description | Example |
|---|---|---|
| `node_id` | Stable, unique node identifier | `pathlockd-0` |
| `data_dir` | Persistent storage for RocksDB groups | `/var/lib/pathlockd` |
| `listen` | gRPC listen address | `0.0.0.0:50051` |

### Cluster settings

| Field | Default | Description |
|---|---|---|
| `group_count` | `256` | Number of Raft groups (shards) |
| `replication_factor` | `3` | Voters per Raft group (must be odd) |
| `seed_nodes` | `[]` | Bootstrap seed nodes for SWIM gossip |
| `bootstrap` | `false` | Set to `true` on the first node to create a new cluster |
| `join` | `false` | Set to `true` when joining an existing cluster |

### Storage settings

| Field | Default | Description |
|---|---|---|
| `rocksdb_wal_sync` | `true` | Sync WAL on every write (set to `false` for throughput) |
| `rocksdb_max_open_files` | `4096` | RocksDB max open files |
| `raft_snapshot_interval_entries` | `10000` | Entries between snapshots |
| `raft_snapshot_min_log_entries` | `5000` | Minimum log entries before snapshot |

### Example config

```toml
listen = "0.0.0.0:50051"
node_id = "pathlockd-0"
data_dir = "/var/lib/pathlockd"
public_addr = "http://pathlockd-0.pathlockd:50051"
raft_addr = "http://pathlockd-0.pathlockd:50052"
gossip_addr = "0.0.0.0:7946"
seed_nodes = ["pathlockd-0.pathlockd:7946", "pathlockd-1.pathlockd:7946", "pathlockd-2.pathlockd:7946"]
group_count = 256
replication_factor = 3
group_gc_interval_secs = 1
group_gc_batch = 1024
event_buffer = 8192
request_timeout_ms = 30000
log_level = "info"
```

## Running

### Single-node mode

```bash
pathlockd --config pathlockd.toml
```

The node opens its local RocksDB at `data_dir/groups/g000001/db` and serves
gRPC on the configured `listen` address.

### Multi-node cluster

**Bootstrap the first node:**

```bash
pathlockd --config pathlockd.toml  # with bootstrap = true
```

**Join additional nodes:**

```bash
pathlockd --config pathlockd.toml  # with join = true, seed_nodes populated
```

## Health checks

Health status reports ready when:

- RocksDB opened all local groups
- Gossip/SWIM started
- Internal Raft transport started
- Local node has joined the cluster
- `g_sys` has a known leader
- Enough groups have leader/quorum

```bash
pathlockd --health-check
# Returns exit code 0 if ready, 1 otherwise
```

For external probes, call the `Health` RPC:

```bash
grpcurl -plaintext localhost:50051 pathlockd.v1.PathLock/Health
```

## Observability

### Tracing

Set `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to point at an OTLP collector
(e.g., Jaeger, Grafana Tempo, or an OpenTelemetry Collector).

```bash
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://otel-collector:4317
```

Spans are emitted for every gRPC request with:
- `rpc.service`, `rpc.method`
- `grpc.status_code`
- Request duration

### Metrics

Set `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` to point at an OTLP metrics
collector (e.g., Prometheus via OpenTelemetry Collector).

```bash
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://otel-collector:4317
```

Metrics emitted:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pathlockd.grpc.server.requests` | Counter | `rpc.service`, `rpc.method`, `grpc.status_code` | Completed gRPC requests |
| `pathlockd.grpc.server.errors` | Counter | `rpc.service`, `rpc.method`, `grpc.status_code` | Non-OK gRPC requests |
| `pathlockd.grpc.server.duration` | Histogram | `rpc.service`, `rpc.method`, `grpc.status_code` | Request latency (ms) |
| `pathlockd.gc.sweeps` | Counter | `success` | GC sweeps completed |
| `pathlockd.gc.reclaimed` | Counter | `success` | Expired keys reclaimed |
| `pathlockd.gc.duration` | Histogram | `success` | GC sweep duration (ms) |

Disable OTel SDK with:

```bash
export OTEL_SDK_DISABLED=true
```

## Data directory layout

```
<data_dir>/
  groups/
    g000001/
      db/               # RocksDB with all column families
    g000002/
      db/
    ...
    sys/
      db/               # System group for fencing tokens
```

Each `db/` directory contains a RocksDB database with these column families:

| CF | Content |
|---|---|
| `meta` | Raft vote, membership, last_applied |
| `raft_log` | Raft log entries |
| `write_locks` | `path -> LockRecord` |
| `read_locks` | `path:NUL:owner -> LockRecord` |
| `fences` | `path -> FenceRecord` |
| `claims` | `path -> ClaimRecord` |
| `desc_write` | `ancestor:NUL:path -> ExpiringIndexRecord` |
| `desc_read` | `ancestor:NUL:path:NUL:owner -> ExpiringIndexRecord` |
| `desc_claim` | `ancestor:NUL:path -> ExpiringIndexRecord` |
| `owner_alive` | `owner -> AliveRecord` |
| `owner_holds` | `owner:NUL:mode:NUL:path -> OwnedLockRecord` |
| `wait_edges` | `owner -> WaitEdgeRecord` |
| `expiry` | `be64(expires_at):NUL:kind:NUL:primary_key -> ExpiryRecord` |

## Tuning

### GC tuning

- `group_gc_interval_secs`: How often each group sweeps expired entries. Default `1`.
- `group_gc_batch`: Keys processed per sweep. Default `1024`.

Set `group_gc_interval_secs = 0` to disable active GC (lazy expiry still applies).

### Raft tuning

- `raft_max_inflight`: Max in-flight proposals per group. Default `256`.
- `raft_snapshot_interval_entries`: Snapshot after this many entries. Default `10000`.

### Concurrency tuning

- `max_concurrent_requests_per_connection`: Per-HTTP/2 connection limit. Default `256`.
- `request_timeout_ms`: Server-side deadline per RPC. Default `30000`.

### Lock domain cardinality

- `group_count` should exceed the number of hot lock domains by at least 4x.
  Each domain maps to exactly one Raft group via HRW hashing.
- Multi-domain acquires are rejected in the current version.

## Troubleshooting

### Node won't start

- Check `data_dir` exists and is writable
- Verify `node_id` is unique across the cluster
- Verify `replication_factor` is odd and <= cluster size

### High lock latency

- Check if a single lock domain is receiving excessive traffic (hot group)
- Consider increasing `group_count` if many domains share few groups
- Check RocksDB I/O: ensure `data_dir` is on fast storage (NVMe)

### GC not reclaiming

- Verify `group_gc_interval_secs > 0`
- Check logs for GC sweep errors
- Lazy expiry is the correctness backstop; active GC is housekeeping

### Memory usage

- `rocksdb_max_open_files`: Lower this if file descriptor limits are tight
- `event_buffer`: Per-subscriber event queue depth; large values increase memory
