Read-only lock inspection: path, owner, and cluster-wide dump.

## Changes

### Added read-only inspection to the PathLock service

Three new RPCs on the production `pathlockd.v1.PathLock` service expose live lock
state for operators and tooling. All are pure reads: they filter by owner
liveness so the reported view matches what would actually block, but they never
mutate the keyspace — dead-owner entries are left for active acquires and
background GC to reclaim.

- **`InspectPath`** — path-centric snapshot of one exact lock path: the live
  write owner, the live point read owners, the persisted fencing token (which
  can outlive the lock, since a fence's TTL is `max(lease, 1 day)`), and any live
  preemption claim.

- **`ListOwnerLocks`** — owner-centric: every lock recorded in an owner's owner
  set, parsed back into `(path, mode)` pairs, plus whether the owner's liveness
  lease is still present.

- **`DumpLocks`** — cluster-wide, paginated dump of every live lock, one
  `(owner, mode, path, fence?)` entry per holding. It walks the `fslock:alive:`
  owner index `owner_page` owners at a time (server default 64, hard cap 512) and
  expands each owner's set in its own snapshot. Because every lock — read or
  write — is recorded in exactly one owner set, the union over owners is the
  complete set of live locks. Pass the opaque `cursor` from each response back to
  fetch the next page until `done` is true.

Supporting internals: `store::scan_alive_owners` adds a paged scan over the
`fslock:alive:` index, and `engine::inspect_path` / `list_owner_locks` /
`dump_locks` implement the read paths over the same bounded, retrying
transactions used elsewhere. The dump is best-effort by design — each owner is
read in its own snapshot, so a page is a near-real-time view rather than a single
global instant. In-transaction set enumeration stays bounded by
`MAX_SET_ENUM_MEMBERS`, so an oversized owner or read set surfaces as gRPC
`RESOURCE_EXHAUSTED` rather than an unbounded scan.

New unit test: `mode_to_proto_round_trips` confirms the engine `Mode` ↔ protobuf
`Mode` mapping used by the inspection responses is symmetric.

## Upgrade note

This release is purely additive. The existing `pathlockd.v1.PathLock` RPCs and
lock semantics are unchanged, and no TiKV keyspace migration is required. Clients
built against v0.4.1 keep working as-is; regenerate your protobuf stubs to pick
up `InspectPath`, `ListOwnerLocks` and `DumpLocks`.

The new endpoints are read-only and liveness-filtered — they never modify lock
state — so they are safe to call against a production cluster.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.4.2-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.4.2-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.4.2   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
