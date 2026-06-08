Orphaned-lock resilience: cancellation-safe commits, active stale-lock resolution, and bounded prewrite TTL.

## Changes

### Fix: mutating RPCs are now cancellation-safe

tonic's server-side request timeout (configured via `request_timeout_ms`) cancels
the handler future on expiry. When a cancellation landed mid-`Tx::commit()` —
inside the prewrite phase of TiKV's two-phase commit — the transaction was dropped
without rolling back the lock intents already sent to TiKV, leaving orphaned
prewrite locks that pinned the region's resolved timestamp indefinitely.

All state-mutating RPC handlers (`acquire`, `release`, `release_all`, `renew`,
`force_release`, `assert_fencing`, `detect_cycle`, `is_blocking`, `set_wait_edge`,
`clear_wait_edge`, and the `set_claim` inside `request_revoke`) are now executed
via a new `run_detached()` helper that wraps each operation in a `tokio::spawn`
task. Tonic cancels the handler future; the spawned task is never aborted, so the
underlying transaction always reaches a terminal commit or rollback — only the
response is lost on cancellation. Pure reads and the fencing token increment (which
only reads the PD oracle) are unchanged.

### Fix: GC sweep continues past `TxnNotFound` chunks

`gc_once` previously aborted the entire sweep on the first chunk-level error, which
meant a single stranded prewrite lock (surfaced as `TxnNotFound` by TiKV's lock
resolver) would halt reclamation of the rest of the keyspace. The sweep now skips a
failing chunk with a `warn` log and continues; at the end it emits a summary warning
with the skipped-chunk count alongside the number of keys reclaimed. Real
infrastructure failures (scan errors, connection loss) still propagate and abort the
pass.

### Added: active stale-lock resolver

A new background task (`spawn_stale_lock_resolver`) runs on a configurable interval
and actively rolls back prewrite locks older than a configurable grace window before
MVCC GC's much larger retention period would. It calls tikv-client's
`cleanup_locks` over the entire `fslock:` key range with a safepoint of
`now − grace_ms`, which resolves `TxnNotFound` cases without error — the lock is
simply gone. Per-lock errors that do occur are logged as warnings rather than
aborting the sweep.

Like the logical and MVCC GC tasks, the resolver uses `try_acquire_gc_lease` so
only one replica in a multi-instance deployment runs the sweep at a time.

Two new config fields control it:

| Field | Default | Env var |
|---|---|---|
| `stale_lock_resolve_interval_secs` | `10` | `PATHLOCKD_STALE_LOCK_RESOLVE_INTERVAL_SECS` |
| `stale_lock_grace_secs` | `60` | `PATHLOCKD_STALE_LOCK_GRACE_SECS` |

Set `stale_lock_resolve_interval_secs = 0` to disable. Startup validation rejects
any configuration where `stale_lock_grace_secs * 1000 < request_timeout_ms` to
prevent the resolver from rolling back legitimately in-flight transactions.

### Fix: orphaned prewrite locks now self-expire within ~3 s

Optimistic TiKV transactions auto-heartbeat by default, re-bumping a prewrite lock's
TTL every ~10 s and keeping a stranded lock alive for 20 s or more. `Tx::begin` and
`begin_warn` now set `HeartbeatOption::NoHeartbeat`. Without heartbeating, an
abandoned prewrite lock keeps its initial `DEFAULT_LOCK_TTL` (~3 s) and
self-expires, after which any foreground operation or the stale-lock resolver can
clear it without a manual resolve call. The previous misleading comment implying
optimistic transactions "hold no locks" has been corrected.

The three fixes are complementary: cancellation safety removes the main source of
strandings; bounded prewrite TTL makes any residual orphan (e.g. from a daemon crash
mid-commit) self-heal within ~3 s; and the active resolver plus GC resilience ensure
cleanup keeps running and catches anything that slips through.

## Upgrade note

The `pathlockd.v1.PathLock` API is unchanged. No TiKV keyspace migration is
required.

The two new config fields default to safe values (`interval = 10 s`,
`grace = 60 s`) and are active by default; set `stale_lock_resolve_interval_secs
= 0` in your TOML or via env var to opt out.

If your deployment uses a `request_timeout_ms` larger than 60 000 (60 s), increase
`stale_lock_grace_secs` to at least `ceil(request_timeout_ms / 1000)` before
upgrading — startup will refuse to start with an unsafe combination.

## Artifacts (Linux amd64 and arm64)

- `pathlockd-0.4.3-linux-amd64.tar.gz` - optimized, stripped release binary
  (x86-64-v3).
- `pathlockd-0.4.3-linux-amd64-debug.tar.gz` - unoptimized binary with debug
  info.
- `SHA256SUMS` - checksums.

Tarballs are built on the release host and dynamically linked (`glibc` +
`libssl3`). For a self-contained, multi-platform deployment use the container
image:

```bash
docker pull ghcr.io/alexpacio/pathlockd:0.4.3   # amd64 (x86-64-v3+) + arm64
```

> **Note:** the `amd64` image is compiled with `-C target-cpu=x86-64-v3` and
> requires a Haswell-class CPU or newer (about 2015+). It will crash with
> `Illegal instruction` on older hardware.
