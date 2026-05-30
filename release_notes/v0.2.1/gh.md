arm64 container images and optional local Docker builds.

## Changes

- **linux/arm64 container images** — the generic image is now a multi-platform
  manifest covering both `linux/amd64` and `linux/arm64` (Apple Silicon M1–M4,
  AWS Graviton). The x86-64-v4 optimized image remains amd64-only (that ISA
  does not exist on ARM).

- **Docker build opt-in in release script** — `scripts/release.sh` no longer
  builds or pushes container images by default. Pass `--docker` to build both
  flavors locally and push them to GHCR. Without the flag, the script focuses
  on binary artifacts and the GitHub release; container images are published
  automatically by the GitHub Actions workflow on tag push (no change there).

- **`docker buildx` for local builds** — the `--docker` path now uses
  `docker buildx` with a dedicated `pathlockd-builder` builder instance,
  enabling multi-platform builds (`linux/amd64,linux/arm64`) from any host.
  The builder is created automatically if it doesn't already exist.

## Upgrade note

No binary or on-disk format changes; a 0.2.0 keyspace is fully compatible
with 0.2.1.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.2.1-linux-amd64.tar.gz` — optimized, stripped release binary (generic x86-64).
- `pathlockd-0.2.1-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `SHA256SUMS` — checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
images:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.2.1             # amd64 + arm64
docker pull ghcr.io/alexpacio/pathlockd:0.2.1-x86-64-v4   # amd64 / AVX-512 only
```
