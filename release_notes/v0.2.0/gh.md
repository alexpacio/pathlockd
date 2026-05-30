Dependency refresh, x86-64-v4 container image, and automated GHCR publishing.

## Changes

- **All dependencies updated and pinned** — every entry in `Cargo.toml` is now
  locked to an exact version (`=x.y.z`), eliminating silent drift on rebuilds.
  Notable version jumps:
  - `tonic` / `tonic-prost` / `tonic-prost-build` `0.12` → `0.14.6` (prost
    codegen split into the new `tonic-prost` / `tonic-prost-build` crates).
  - `prost` `0.13` → `0.14.3`.
  - `tikv-client` `0.3` → `0.4.0`.
  - `bincode` `1.3` → `2.0.1` (new `serde`-feature API; see upgrade note below).
  - `toml` `0.8` → `1.1.2`.
  - `thiserror` `1` → `2.0.18`.
  - `tokio` `1` → `1.52.3`, `clap` `4` → `4.6.1`, `anyhow` `1` → `1.0.102`.

- **x86-64-v4 container image** — a second image compiled with
  `-C target-cpu=x86-64-v4` (AVX-512, BMI2, POPCNT, MOVBE) is now published
  alongside the generic build on every release. Use it on modern Xeon / EPYC
  hosts for a free ~5–15 % throughput gain. The binary will `SIGILL` on older
  CPUs — verify first with `grep -c avx512 /proc/cpuinfo`.

- **Automated GHCR publishing via GitHub Actions** — a new
  [Docker publish workflow](.github/workflows/docker-publish.yml) fires on
  every `v*` tag and builds both images in parallel, pushing to
  `ghcr.io/alexpacio/pathlockd`. No PAT or extra secrets required; the
  workflow uses the built-in `GITHUB_TOKEN`.

## Upgrade note

**`bincode` wire format changed.** Values serialized by `0.1.3` (bincode 1.x)
are not readable by `0.2.0` (bincode 2.x). Flush the TiKV keyspace before
upgrading a running deployment:

```bash
# drain all locks (clients must release or let leases expire), then:
tikv-ctl --pd-endpoints=<pd>:2379 unsafe-destroy-range \
  --from-hex 00 --to-hex FF
```

Or bring up a fresh keyspace and let clients re-acquire their locks.
A 0.1.3 keyspace used only for development (no durable state worth keeping)
can simply be wiped with `docker compose down -v && docker compose up`.

## Artifacts (Linux x86_64 / amd64 only)

- `pathlockd-0.2.0-linux-amd64.tar.gz` — optimized, stripped release binary (generic x86-64).
- `pathlockd-0.2.0-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `SHA256SUMS` — checksums.

Both are **dynamically linked** (`glibc` + `libssl3` required at runtime). For
a self-contained deployment use the container images:

```
docker pull ghcr.io/alexpacio/pathlockd:0.2.0            # generic
docker pull ghcr.io/alexpacio/pathlockd:0.2.0-x86-64-v4  # AVX-512 optimized
```
