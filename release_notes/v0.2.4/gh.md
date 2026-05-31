Input validation fix for malformed protobuf enum values.

## Changes

- **Invalid protobuf enum integers now return `InvalidArgument`** — the
  gRPC service previously decoded `mode` and `state` enum fields by comparing
  the incoming integer against a single known value and silently falling back
  to the other variant for everything else. An unknown `mode` integer was
  treated as `Write`, and an unknown `state` integer was treated as `New`.
  A misbehaving or malicious client could therefore acquire a write lock (or
  flip lock state) by sending an out-of-range enum value, with no error
  surfaced. `to_mode`/`to_state` now decode via `try_from` and reject any
  value outside the defined enum, returning a gRPC `InvalidArgument` status
  (`invalid mode value N` / `invalid lock state value N`). This validation is
  applied across the acquire, release, and debug paths.

## Upgrade note

The daemon binary, wire protocol, and on-disk keyspace format are unchanged;
no migration is needed. The only behavioural change is that requests carrying
an undefined `mode` or `state` enum integer are now rejected with
`InvalidArgument` instead of being coerced to a default variant. Well-formed
clients are unaffected.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.2.4-linux-amd64.tar.gz` — optimized, stripped release binary (x86-64-v3).
- `pathlockd-0.2.4-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `SHA256SUMS` — checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
images:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.2.4   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (≈ 2015+). It will crash with
> `Illegal instruction` on older hardware.
