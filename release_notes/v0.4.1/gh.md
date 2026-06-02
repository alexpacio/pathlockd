Subtree-aware preemption claims and bounded set scans.

## Changes

### Fix: preemption claims now block covering ancestor writes

Previously a `set_claim` on `h:/a/b` could be defeated by a different owner
acquiring a write lock on the ancestor `h:/a`, because the conflict check only
looked at path-exact and descendant write/read locks — it did not consult claims
held below the requested path.

This release introduces a `claimdesc` index: `set_claim` registers the claimed
path under every ancestor key, and `acquire` for a write lock now checks
`find_descendant_claim_conflict` before granting the lock. A write acquire that
covers a claimed descendant is blocked with `REASON_PREEMPT_CLAIMED` unless the
requesting owner is the claimant itself.

When the claimant successfully acquires a covering ancestor write lock,
`remove_owned_descendant_claims` consumes all of the claimant's own descendant
claims atomically within the same transaction. Read acquires remain point-only
and are unaffected by claims on descendant paths.

New regression test: `descendant_preemption_claim_blocks_ancestor_write`
verifies that an ancestor write is blocked by a live descendant claim, that
reads bypass this restriction, and that the claimant's covering acquire consumes
the claim so unrelated owners can re-acquire afterwards.

### Fix: set scans inside transactions are now bounded

Descendant conflict checks, owner renewal (`renew`), `release_all`, and
`force_release` previously called an unbounded `smembers` that could enumerate
every member of a TiKV set in one transaction snapshot. For a root or busy
subtree lock, an owner with many paths, or a large read set this caused a
scaling cliff in memory pressure, transaction size, and latency.

The following changes cap in-transaction set enumeration:

- **`MAX_SET_ENUM_MEMBERS = 65 536`** — hard limit on live set members read in
  one transactional scan. Exceeding it surfaces as a typed
  `SetScanLimitExceeded` error that maps to gRPC `RESOURCE_EXHAUSTED`.

- **`smembers_limited`** — all `smembers` calls in the engine now go through
  this bounded variant. No code path silently enumerates beyond the limit.

- **`sismember` is a point-lookup** — the previous implementation loaded the
  whole set to check membership. It now fetches the single member key directly,
  reducing per-check I/O from O(set-size) to O(1).

- **`has_live_member` early-exit scan** — emptiness checks that previously
  called `scard(key) == 0` (full scan) now use a scan that returns as soon as
  one live member is found, avoiding full enumeration on non-empty sets.

New unit test: `engine_err_maps_set_scan_limit_to_resource_exhausted` confirms
that `SetScanLimitExceeded` is translated to `tonic::Code::ResourceExhausted`
at the service boundary.

## Upgrade note

The production `pathlockd.v1.PathLock` API is unchanged. No TiKV keyspace
migration is required.

Deployments that hold root or very wide subtree locks, or owners with more than
65 536 live lock entries, will now receive `RESOURCE_EXHAUSTED` on operations
that would have previously succeeded (slowly). These cases should be treated as
architectural anti-patterns; split the lock granularity rather than raising the
limit.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.4.1-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.4.1-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.4.1   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
