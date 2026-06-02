Production hardening, debug-surface removal, and deeper TiKV state verification.

## Changes

### Removed the gRPC debug surface

- **`PathLockDebug` has been removed from the protobuf API** - the test-only
  fault-injection service and its messages are no longer generated, mounted, or
  documented. The daemon now exposes only the production `PathLock` service.

- **`PATHLOCKD_ENABLE_DEBUG` / `enable_debug` removed** - the daemon no longer
  accepts a config switch that can publish destructive fault-injection RPCs.
  Example config, README and internal docs have been updated accordingly.

- **Fault-injection helpers are internal only** - engine-level test helpers were
  renamed from `debug_*` to explicit `inject_*` / `inspect_*` names and remain
  available to integration tests without being exposed over the network.

### Process-stability hardening

- **Background GC loops are supervised** - logical GC and TiKV MVCC GC are now
  split into guarded pass functions. A panic inside a background sweep is logged
  and the task continues on the next tick instead of silently dying.

- **Health probes are stricter and IPv6-safe** - `--health-check` now parses the
  configured listen address as a socket address, maps unspecified binds to
  loopback (`0.0.0.0` -> `127.0.0.1`, `[::]` -> `[::1]`), and reports invalid
  listen addresses directly.

- **Event fan-out no longer panics on poisoned registry locks** - the per-owner
  subscription registry recovers from a poisoned mutex and logs the recovery.
  Subscriber queue sizes are also bounded so a bad `event_buffer` cannot trip
  Tokio channel construction.

- **Storage encode path is fallible** - `Stored` serialization now returns an
  error instead of relying on an `expect`, keeping unexpected encode failures on
  the normal error path.

- **Config validation tightened** - `event_buffer` must be in a safe bounded
  range and `gc_page` is capped, so dangerous startup knobs fail fast instead of
  creating runtime surprises.

- **Tracing filter validation tightened** - invalid `PATHLOCKD_LOG_LEVEL` /
  `log_level` filters now produce a clear startup error.

### Stronger e2e and safety coverage

- **New TiKV state-transition e2e test** - `tests/e2e_state.rs` drives a real
  daemon over gRPC, then reads TiKV directly and decodes `fslock:*` values to
  prove acquire, renew, TTL extension, fencing, read/write membership,
  descendant indexes, wait edges, release cleanup, lazy expiry and GC drain all
  happen in storage.

- **New `scripts/test-e2e-state.sh` runner** - runs the state-transition suite in
  the dev compose network, matching the existing integration/e2e script style.

- **New safety/deadlock e2e harness** - `tests/e2e_stress.rs` now includes
  adversarial hierarchical contention, manufactured deadlock cycles, fencing
  assertions, an in-memory mutual-exclusion oracle and post-run transient-state
  drain checks. `scripts/test-e2e-safety.sh` runs this suite with tunable load.

- **`PATHLOCKD_EXTRA_ENV` forwarding fixed** - the shared Docker test runner now
  forwards extra environment flags assembled by higher-level scripts, so e2e
  tunables actually reach the container.

## Upgrade note

This release removes the `pathlockd.v1.PathLockDebug` protobuf service. Any
external tooling that called debug RPCs such as `Flush`, `ExpireOwner`,
`DeleteLockKey`, `SetFence`, or `OwnedPaths` must be removed or replaced with
test-only direct integration helpers.

The production `pathlockd.v1.PathLock` API and lock semantics are unchanged.
No TiKV keyspace migration is required.

`PATHLOCKD_ENABLE_DEBUG` and the TOML `enable_debug` key are no longer accepted.
Remove them from deployment manifests and config files before upgrading.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.4.0-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.4.0-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.4.0   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
