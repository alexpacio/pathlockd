Scalability and TiKV hardening release.

## Changes

- **Lease expiry now uses PD time** - pathlockd now derives lease timestamps
  from TiKV/PD's timestamp oracle physical time instead of each daemon's local
  wall clock. This prevents cross-instance clock skew from expiring a still-live
  lock early or extending a dead one longer than intended.

- **Logical sets are stored as per-member TiKV keys** - read sets, owner sets,
  and descendant indexes now use one TiKV key per member instead of rewriting a
  single encoded set value. This removes the largest hot-value growth point for
  busy roots, hot read paths, and large owner leases. Existing legacy set values
  are read for compatibility and migrated opportunistically by mutating set
  operations.

- **Fencing tokens no longer use a global TiKV counter hot key** -
  `IncrFencingToken` now returns a monotonic PD TSO version. Tokens remain
  cluster-wide ordered and positive, but they are no longer small consecutive
  integers from `fslock:fencing:counter`.

- **Retry behavior is more selective** - bounded transaction retry now targets
  transient TiKV conditions such as locks, write conflicts, region churn,
  unavailable/deadline gRPC responses, and related temporary failures. Permanent
  client/data errors fail faster instead of burning the whole retry budget and
  encouraging another client retry.

- **Server overload controls** - the daemon now applies a configurable
  server-side request timeout, per-connection concurrency limit, and load
  shedding. Defaults are `request_timeout_ms = 30000` and
  `max_concurrent_requests_per_connection = 256`; both can be set in TOML or via
  `PATHLOCKD_REQUEST_TIMEOUT_MS` and
  `PATHLOCKD_MAX_CONCURRENT_REQUESTS_PER_CONNECTION`.

- **Active GC is less aggressive by default** - the default GC sweep interval is
  now 60 seconds instead of 1 second. Lazy expiry still enforces correctness; the
  background sweep is storage cleanup only.

- **Per-owner event fan-out** - `Subscribe` streams now register in a per-owner
  registry and only wake for events addressed to that owner. Local fan-out now
  scales with subscribers for the affected owner instead of every subscriber on
  the instance.

- **Revoke claim input validation** - optional preemption claims on
  `RequestRevoke` now require `claim_path` and `claimant_owner_id` together,
  validate the claim path, and cap `claim_ttl_ms` at 60 seconds. A zero claim TTL
  still uses the short default.

- **Deadlock walk cap tightened** - `DetectCycle.max_depth` is clamped to 64 to
  bound one advisory wait-chain walk and keep a single request from holding a
  long-lived TiKV snapshot for too long.

## Upgrade note

No manual TiKV keyspace migration is required. New set members are written under
`fslock:setm:*`; old encoded `Stored::Set` keys remain readable and are migrated
the next time the corresponding logical set is mutated. Expired member keys are
reclaimed by the existing GC sweep.

`IncrFencingToken` now returns PD TSO versions, so callers must treat fencing
tokens as opaque monotonically increasing `int64` values. Code that assumed
small consecutive integers should be adjusted. The debug fencing-counter RPCs
still operate on the legacy counter key for test/fault-injection scenarios, but
they no longer control public token issuance.

Two new config fields are available:

```toml
request_timeout_ms = 30000
max_concurrent_requests_per_connection = 256
```

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.2.7-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.2.7-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.2.7   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
