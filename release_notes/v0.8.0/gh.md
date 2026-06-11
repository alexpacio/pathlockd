Multi-raft clustering with elastic membership. pathlockd is now a
distributed system: nodes form a cluster via SWIM gossip, lock state is
sharded across Raft groups, and membership self-heals without operator
intervention.

## Changes

### Added: multi-raft engine

Lock state is now partitioned across `group_count` (default 32) Raft groups
using Rendezvous Hashing (HRW). A special system group (`SYS_GROUP`) holds
cluster-global state: the monotonic fencing counter, deadlock wait-graph, and
the membership directory. All groups share one RocksDB instance (group `g`'s
keyspace lives under a prefix); starting or stopping a group is cheap —
no per-group files.

### Added: SWIM gossip (foca)

Nodes discover each other and detect failures via SWIM over UDP. Gossip
provides _hints_ only — which nodes exist, their addresses, and whether they
look alive. Correctness (group membership, lock state, owner liveness) is
always decided by Raft. Nodes announce themselves on startup and can join an
existing cluster by contacting any seed address (`join_addr` config).

### Added: elastic membership controller

A decentralized reconciler runs on every node and converges group voter sets
toward the HRW-selected desired set without central coordination:

1. Missing desired voters are added as learners (openraft replicates state
   into them).
2. Once all desired voters are present and a quorum is alive, joint consensus
   migrates the voter set.
3. Leadership is periodically transferred toward each group's HRW-first live
   voter, spreading write load across the cluster.
4. The sys group additionally keeps every stable node as a **sys learner**,
   so all nodes hold a local replica of the directory for stale-tolerable
   local reads.

Safety rails: nodes are only counted as stable after `stability_window_secs`
(default 30 s) continuously up; voters are only evicted after
`eviction_window_secs` (default 60 s) gone **and** the change preserves a
live majority. Membership changes are rate-limited by
`max_concurrent_reconciles`.

### Added: client-command routing

The request router maps each lock domain to its owning Raft group via HRW
(same deterministic function as placement) and forwards writes to the current
group leader. Non-leader nodes transparently proxy commands using the new
internal `RaftTransport` service (`Forward`, `ForwardRead`). Linearizable
reads go through a read-barrier before forwarding.

### Added: internal Raft transport gRPC service

A new internal proto (`pathlockd_raft.proto`) defines node-to-node RPCs:
openraft protocol messages (`AppendEntries`, `Vote`, `TransferLeader`,
`InstallSnapshot`), leader forwarding (`Forward`, `ForwardRead`), and drain
control (`SetDraining`). All groups multiplex over one HTTP/2 channel per
peer via group-tagged frames.

### Added: SetClaim / ClearClaim RPCs

A claim reserves a path for a claimant without granting the lock. New
acquires by other owners that overlap the claimed path are refused with
`preempt_claimed` while existing holders drain. The claimant's own acquire
consumes the claim atomically; a crashed claimant's reservation expires with
its TTL. This lets a pure waiter reserve its queued path before it holds any
lease.

### Added: graceful drain

Nodes can be marked draining via `SetDraining`. Reconcilers migrate groups
off draining nodes and transfer leaderships away; clearing the flag cancels
the drain. Useful for rolling restarts and scale-in.

### Added: cluster live tests

`tests/cluster_live.rs` and `tests/cluster_tests.rs` bring up real
multi-node topologies in-process and exercise leader failover, membership
convergence, command forwarding, and routing correctness.

## Upgrade notes

The wire format and storage layout changed substantially. **A clean data
directory is required when upgrading from v0.7.x.** There is no in-place
migration path.

Configuration additions (all optional with safe defaults):

| Key | Default | Description |
|-----|---------|-------------|
| `group_count` | 32 | Number of Raft shard groups |
| `replication_factor` | 3 | Desired voters per group |
| `stability_window_secs` | 30 | Time a node must be up before it is eligible as a voter |
| `eviction_window_secs` | 60 | Time a node must be absent before it is evicted from a group |
| `max_concurrent_reconciles` | 4 | Membership changes in flight per tick |
| `gossip_addr` | — | UDP address this node advertises for SWIM |
| `join_addr` | — | Seed address to contact on first boot (omit for single-node) |

**Known limitation:** a voter restarting with a wiped disk retains its node
identity but has lost its vote; in pathological timing it could double-vote
within one term. Recommended recovery: rejoin with a fresh node ID
(StatefulSet replica with a new ordinal) and let the reconciler evict the old
identity after `eviction_window_secs`.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.8.0-linux-amd64.tar.gz` - optimized, stripped release binary.
- `pathlockd-0.8.0-linux-amd64-debug.tar.gz` - unoptimized binary with debug info.
- `SHA256SUMS` - checksums.

Tarballs are dynamically linked (`glibc` + `libssl3`). For a self-contained,
multi-platform deployment use the container image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.8.0   # amd64 + arm64
```
