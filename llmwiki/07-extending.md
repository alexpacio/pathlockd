# Extending pathlockd

## Add a new primitive (RPC)

1. **Proto** (`proto/pathlockd.proto`): add the request/response messages and the
   `rpc` to the `PathLock` service. Keep enum values prefixed with the enum name
   (`FOO_STATUS_OK`) so prost strips the prefix to clean variants.
2. **Engine** (`src/engine.rs`): add a `pub fn foo_inner<T: StoreTxn>(tx: &mut T,
   args) -> Result<FooOutcome>` that is synchronous and deterministic. The Raft
   state machine will call it during apply. Encode logical results as a value
   enum, never as `Err`.
3. **Command** (`src/raft/command.rs`): add a new variant to the `Command` enum
   carrying the request payload, and a new variant to `ApplyResponse` for the
   outcome. This is what gets serialized into the Raft log and deserialized in
   `apply()`.
4. **State machine** (`src/raft/state_machine.rs`): add a match arm in `apply()`
   that calls the engine function and returns the outcome.
5. **Router** (`src/cluster/router.rs`): add a public async method that builds the
   command and sends it to the appropriate Raft group leader.
6. **Service** (`src/service.rs`): implement the trait method, validating inputs
   (`check_id` / `check_ttl` / `check_path`), calling the router, and publishing
   any events.
7. **Test** (`tests/engine_tests.rs`): assert the outcome value against the
   in-process RocksDB state machine.
8. **Client** (`pathlockd-nodejs-client`): copy the updated `.proto` into the
   client package (`proto/`), add a typed wrapper method, rebuild. **The bundled
   proto must stay in sync** — a stale client proto silently drops new fields.

## Touching the data model

- Keys are defined by the builder functions in `store_keys.rs`. New per-owner or
  per-path data should use the existing column family constants and key layout
  patterns (e.g. `wr_key`, `rd_prefix`, `member_key`). Column families are
  defined as constants in `store_keys.rs`; add new ones to `ALL_CFS`.
- New values use the `StoredRecord` enum in `store_rocksdb.rs`:
  `StoredRecord::Str { v, exp }` for expiring string values,
  `StoredRecord::Counter { v }` for monotonic counters. Values are
  bincode-encoded.
- Anything that should expire needs an `exp` and must be read through the
  `StoreTxn` trait's expiry-aware helpers. For set-like data with members of
  independent lifetimes, keep the per-member expiry model (member-key prefix
  pattern: key\0member) — never a single set-wide expiry.

## Concurrency rules of thumb

- All mutating primitives go through the Raft state machine's serialized apply
  path. The `WriteBatch` is committed atomically — no optimistic retry loops or
  per-handler serialization keys are needed.
- Read-only observability operations (`inspect_path`, `list_owner_locks`,
  `dump_locks`, `detect_cycle`, `is_blocking`) use a `RocksDbTxn` snapshot
  wrapper. They skip the Raft apply path entirely.
- Never hold results across operations; compute inside the `StoreTxn` scope.

## Events

- Publish an event only at the point ownership actually changes, and only via
  the broadcaster so the per-owner filter and peer fan-out apply.
- Remember subscriptions are per-owner: only that owner will ever see the event.
  Cross-owner coordination must go through state the other side can poll.

## Gotchas

- `get_ancestors` is byte/`/`-based and assumes normalized paths
  (`handler:/a/b`, no trailing slash). Normalize on the client; the service also
  rejects clearly malformed paths (`check_path`: no handler, unrooted, `//`,
  `.`/`..`, trailing slash) as a backstop, but does not canonicalize.
- Fencing tokens must stay monotonic; never write a lower fence for a path.
- Keep fault-injection helpers internal to tests; do not add a gRPC debug
  surface.
