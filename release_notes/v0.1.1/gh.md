First hardening release on the 0.1 line.

## Highlights
- **Correctness:** set members now expire **per member**, fixing a latent
  mutual-exclusion bug where a short-lived lock could shorten a read set or
  descendant index below a longer-lived one and admit two writers into
  overlapping subtrees.
- **Scalability:** the serialization key is now **sharded per handler**, so
  disjoint handlers commit in parallel instead of through one global key.
- **Robustness:** server-side input validation (`ttl_ms` must be `> 0` and
  `≤ 7 days`, paths must be normalized, `DetectCycle.max_depth` clamped),
  retry-exhaustion mapped to `Unavailable`, jittered backoff, and a bounded
  per-peer event forwarder.
- **Ops:** the debug service is mounted only when explicitly enabled; the
  container image runs **non-root** with a `HEALTHCHECK` (via a new
  `--health-check` self-probe).
- **Docs:** new [usage guide for building a user-space virtual filesystem](https://github.com/alexpacio/pathlockd/blob/v0.1.1/docs/usage-virtual-filesystem.md).

## ⚠️ Upgrade note
The on-disk value encoding changed (per-member set expiry). Run against a
**fresh / flushed** keyspace — do not point 0.1.1 at a 0.1.0 keyspace.

## Roadmap to 1.0.0 (not yet implemented)
Metrics, CI, authentication/authorization + TLS, and multitenancy are planned
for the final `1.0.0` release.

## Artifacts (Linux x86_64 / amd64 only)
- `pathlockd-0.1.1-linux-amd64.tar.gz` — optimized, stripped release binary.
- `pathlockd-0.1.1-linux-amd64-debug.tar.gz` — unoptimized binary with debug info.
- `SHA256SUMS` — checksums.

Both are **dynamically linked** (built on a Debian/glibc system); they need
`glibc` and `libssl3` (+ `ca-certificates`) at runtime, matching the Docker
runtime image. For a self-contained deployment, use the container image instead.
