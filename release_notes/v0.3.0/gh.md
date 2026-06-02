OpenTelemetry observability, TiKV MVCC GC, and legacy format removal.

## Changes

### OpenTelemetry tracing and metrics

- **OTLP traces and metrics export** — a new `src/otel.rs` module wires
  `opentelemetry-sdk` into the existing `tracing` stack. Every gRPC handler is
  wrapped in an instrumented span; trace context is propagated from incoming
  requests via W3C `traceparent`. Metrics include a per-sweep GC counter and
  latency histogram.

- **Zero-config for operators without an APM backend** — the SDK path is active
  only when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the per-signal variant) is set.
  Setting `OTEL_SDK_DISABLED=true` or omitting the endpoint falls back to the
  plain `tracing-subscriber` formatter used in earlier releases. No TOML key is
  required.

- **Graceful shutdown** — `TelemetryGuard::shutdown()` flushes the in-flight
  OTLP exporter queue before the process exits. Errors are logged as warnings
  rather than masking the server shutdown result.

### TiKV transactional MVCC GC

- **`mvcc_gc_once`** — pathlockd can now advance PD's global safepoint on a
  configurable interval. This is required for standalone TiKV clusters without a
  TiDB coordinator: without it, MVCC tombstones accumulate indefinitely and
  cluster scan performance degrades over time.

- **New config fields** — `mvcc_gc_interval_secs` (default `300`) and
  `mvcc_gc_safe_point_retention_secs` (default `600`). Setting
  `mvcc_gc_interval_secs = 0` disables the sweep for clusters where another
  component (TiDB, a custom GC worker) already advances the safepoint.
  `mvcc_gc_safe_point_retention_secs` must be at least `2x request_timeout_ms`;
  startup validation rejects unsafe combinations.

### Multi-replica GC lease coordination

- **`try_acquire_gc_lease`** — both the logical GC sweep and the MVCC GC sweep
  now acquire a cluster-wide lease (stored at `pathlockd:gc:<name>`, outside the
  `fslock:` data range) before running. Only the replica that holds the lease
  executes the expensive cluster-wide scan; others log a debug-level skip and
  continue. The lease TTL is 30 s; replicas that restart or fail release it
  implicitly via lazy expiry. This replaces the per-replica unconditional sweep
  that caused write amplification in multi-node deployments.

### `del_set` correctness fix

- **`Tx::del_set`** — a new store primitive that deletes the set's member-key
  range instead of the parent key. All call sites in the engine that previously
  used `Tx::del` on set-typed keys (`rd:*`, `own:*`, `idx:*`) now use
  `del_set`. The previous code left orphaned member keys in TiKV whenever an
  empty set was cleaned up, causing stale members to reappear after a restart
  until the GC sweep reclaimed them.

### Wait-edge parser hardening

- **`parse_wait_edge` now returns `Result`** — malformed V1-prefix wait edges
  (wrong length fields, out-of-bounds slices) are now propagated as errors
  rather than silently falling back to the legacy bare-owner form. This surfaces
  storage corruption instead of hiding it behind a misleading owner string.

### Removed legacy compatibility code

- **`Stored::Set` variant removed** — the inline set format predates the
  member-key layout introduced in v0.2.x. The automatic migration code
  (`load_legacy_set`, `migrate_legacy_set`, `set_exp`) and the runtime fallback
  in `load_set` have been deleted. Any cluster that was running a recent v0.2.x
  release will have already migrated all set keys on first write; nodes that
  skipped migration will encounter decode errors on the affected keys.

- **Legacy wait-edge fallback removed** — `legacy_wait_edge` is gone; a
  versioned wait edge that fails to parse is now an error.

- **Counter string-coercion removed** — `parse_counter_string` (which allowed
  `Stored::Str` values to be treated as counters) is removed. The only valid
  counter type is `Stored::Counter`.

### End-to-end test infrastructure

- **`tests/e2e_stress.rs` and `scripts/test-e2e-stress.sh`** — a new test
  harness that starts two pathlockd replicas in peer mode, verifies cross-replica
  event fan-out, and runs concurrent GC stress under load. Previously the
  integration suite only tested a single node.

## Upgrade note

**If you are upgrading from v0.2.9**, no TiKV keyspace migration is required.
All lock metadata, serialization tombstones, and fencing counters remain valid.

**If you are upgrading from v0.1.x or an early v0.2.x build that predates the
member-key set layout**, run a full GC sweep with the old binary before upgrading
to ensure all legacy `Stored::Set` values are migrated. Upgrading directly may
cause decode errors on any key that was never mutated after the member-key layout
was introduced.

MVCC GC is **enabled by default** (`mvcc_gc_interval_secs = 300`). If your
cluster already has a TiDB coordinator or a custom GC worker managing the global
safepoint, set `mvcc_gc_interval_secs = 0` to avoid duplicate safepoint
advancement.

This release does not change the protobuf API or lock semantics.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.3.0-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.3.0-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.3.0   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
