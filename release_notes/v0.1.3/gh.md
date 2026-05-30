Release automation improvements.

## Changes

- **GHCR image push** — `scripts/release.sh` now builds and pushes the
  container image to `ghcr.io/<user>/pathlockd:<version>` (and `:latest`)
  as part of every release. Authentication uses the existing `gh` session
  (`gh auth token | docker login ghcr.io`) — no separate PAT required.
- **Idempotent re-runs** — re-running the script after a dry run no longer
  rebuilds from scratch: dist tarballs are skipped when all three files
  (`*.tar.gz`, `*-debug.tar.gz`, `SHA256SUMS`) are already present in
  `dist/<tag>/`, and the Docker build is skipped when the image for that
  version already exists locally.
- **Auto-bump `Cargo.toml`** — in non-dry-run mode the script patches the
  `[package] version` field and updates `Cargo.lock` (`cargo update
  --package pathlockd`), then commits the change automatically. In
  `--dry-run` mode the version must already match the tag (no side effects).

## Upgrade note

No binary or on-disk format changes; a 0.1.2 keyspace is fully compatible
with 0.1.3.

## Artifacts (Linux x86_64 / amd64 only)
- `pathlockd-0.1.3-linux-amd64.tar.gz` — optimized, stripped release binary.
- `pathlockd-0.1.3-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `SHA256SUMS` — checksums.

Both are **dynamically linked** (built on a Debian/glibc system); they need
`glibc` and `libssl3` (+ `ca-certificates`) at runtime. For a self-contained
deployment, use the container image instead:

```
docker pull ghcr.io/alexpacio/pathlockd:0.1.3
```
