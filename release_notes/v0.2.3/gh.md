Docker container fixes and release tooling cleanup.

## Changes

- **Distroless base image corrected** — the runtime stage now uses
  `gcr.io/distroless/cc-debian13` instead of `distroless/base-debian13`.
  The `base` variant omits the C/C++ runtime libraries (`libgcc_s`,
  `libstdc++`) that Rust binaries link against, causing the container to fail
  at startup. The `cc` variant includes those libraries and matches the intent
  of the original distroless switch.

- **x86-64-v3 optimization baked into Docker builds** — `scripts/release.sh
  --docker` now passes `--build-arg RUSTFLAGS="-C target-cpu=x86-64-v3"` to
  the builder stage automatically, so the container image is always compiled
  with the same microarch tuning as the binary tarballs. Previously, the flag
  had to be injected manually.

- **x86-64-v4 Docker variant removed** — the `--docker-v4` flag and its
  AVX-512-optimized `:VERSION-x86-64-v4` image tag are gone. The extra variant
  added release complexity for a rarely-used target; operators who need
  AVX-512 tuning can build a local image with a custom `RUSTFLAGS` build arg.

- **`--docker-force` flag added to `release.sh`** — replaces the removed
  `--docker-v4` slot. Pass `--docker-force` to rebuild and push an image tag
  that already exists in GHCR (the default behaviour still skips existing
  tags).

## Upgrade note

The daemon binary and on-disk keyspace format are unchanged; no migration is
needed. The fix only affects the container image. If you are running the
0.2.2 container image, pull the updated 0.2.3 image — the 0.2.2 container
would have failed to start due to missing C runtime libraries.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.2.3-linux-amd64.tar.gz` — optimized, stripped release binary (x86-64-v3).
- `pathlockd-0.2.3-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `SHA256SUMS` — checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
images:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.2.3   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (≈ 2015+). It will crash with
> `Illegal instruction` on older hardware.
