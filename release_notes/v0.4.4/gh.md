Storage and self-healing observability metrics.

## Changes

### Added: storage & self-healing observability metrics

Three new OpenTelemetry metrics make the keyspace and the self-healing paths
visible to monitoring:

- **`pathlockd.locks.live`** (gauge, attribute `class`) — a per-class census of
  live `fslock:` keys: `write`, `read`, `alive`, `owner`, `index`, `fence`,
  `wait`, `claim`, `other`. This is the per-type "how many locks are held" view
  TiKV's own metrics cannot give (TiKV counts keys per column family, not per
  key-prefix). It is computed as a side effect of the logical GC sweep — the sweep
  already visits and decodes every key, so the only added cost is classification —
  and it counts only non-expired keys, so a lapsed-but-unswept lock drops out
  immediately. Requires logical GC enabled (`gc_interval_secs > 0`, the default).
- **`pathlockd.stale_lock.resolved`** (counter) — stranded transaction locks
  rolled back by the stale-lock resolver. Normally flat at zero; any increase
  means the resolver caught an orphan.
- **`pathlockd.gc.skipped_chunks`** (counter) — GC chunks skipped after a delete
  error while the sweep continued. Normally zero; a non-zero rate flags
  poisoned/orphaned keys being worked around.

`gc_once` now returns a `GcSweep { reclaimed, failed_chunks, census }` instead of a
bare reclaimed count; this is an internal API change with no effect on the gRPC
surface.

## Upgrade note

The `pathlockd.v1.PathLock` API is unchanged. No TiKV keyspace migration is
required.

The new metrics are emitted automatically when OpenTelemetry is enabled (via
`OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_SERVICE_NAME`). The live-lock census
requires logical GC enabled (`gc_interval_secs > 0`, the default).

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.4.4-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.4.4-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.4.4   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
