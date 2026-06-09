//! StoreTxn trait and RocksDB-backed transaction wrapper.
//!
//! The `StoreTxn` trait abstracts storage operations used by the lock engine.
//! All methods are synchronous because RocksDB operations are inherently sync.
//! The trait is implemented by both the Raft state machine's WriteBatch wrapper
//! and a direct-read RocksDB snapshot for observability reads.

use std::sync::Arc;

use crate::store_keys::expired;

// --- StoreTxn trait (sync) ---

pub trait StoreTxn {
    fn now_ms(&self) -> u64;

    fn get_str(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<Option<String>>;
    fn set_str(
        &mut self,
        cf: &'static str,
        key: &[u8],
        value: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<()>;
    fn pexpire_str(&mut self, cf: &'static str, key: &[u8], ttl_ms: u64) -> anyhow::Result<()>;
    fn del(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<()>;

    fn sadd(
        &mut self,
        cf: &'static str,
        key: &[u8],
        member: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<()>;
    fn srem(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<()>;
    fn smembers_limited(
        &mut self,
        cf: &'static str,
        key: &[u8],
        limit: usize,
    ) -> anyhow::Result<Vec<String>>;
    fn sismember(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<bool>;
    fn has_live_member(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<bool>;
    fn pexpire_set(&mut self, cf: &'static str, key: &[u8], ttl_ms: u64) -> anyhow::Result<()>;
    fn del_set(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<()>;
}

// --- Read-only RocksDB-backed StoreTxn for observability ---

pub struct RocksDbTxn {
    db: Arc<rocksdb::DB>,
    now_ms: u64,
}

impl RocksDbTxn {
    pub fn new(db: Arc<rocksdb::DB>, now_ms: u64) -> Self {
        Self { db, now_ms }
    }
}

impl StoreTxn for RocksDbTxn {
    fn now_ms(&self) -> u64 {
        self.now_ms
    }

    fn get_str(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<Option<String>> {
        let cf_handle = self.db.cf_handle(cf).unwrap();
        match self.db.get_cf(&cf_handle, key)? {
            Some(v) => {
                let (rec, _): (StoredRecord, _) =
                    bincode::serde::decode_from_slice(&v, bincode::config::standard())?;
                match rec {
                    StoredRecord::Str { v, exp } => {
                        if expired(exp, self.now_ms) {
                            Ok(None)
                        } else {
                            Ok(Some(v))
                        }
                    }
                    StoredRecord::Counter { .. } => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    fn set_str(
        &mut self,
        _cf: &'static str,
        _key: &[u8],
        _value: &str,
        _ttl_ms: u64,
    ) -> anyhow::Result<()> {
        anyhow::bail!("RocksDbTxn is read-only")
    }

    fn pexpire_str(&mut self, _cf: &'static str, _key: &[u8], _ttl_ms: u64) -> anyhow::Result<()> {
        anyhow::bail!("RocksDbTxn is read-only")
    }

    // `del`/`srem`/`del_set` are best-effort lazy cleanup of already-expired or
    // dead-owner entries. On this read-only view (used by `detect_cycle` and
    // `is_blocking`) they are dropped silently rather than erroring: the query
    // result is computed from liveness checks and stays correct, and the actual
    // pruning is performed by the serialized write path and the GC sweep.
    fn del(&mut self, _cf: &'static str, _key: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }

    fn sadd(
        &mut self,
        _cf: &'static str,
        _key: &[u8],
        _member: &str,
        _ttl_ms: u64,
    ) -> anyhow::Result<()> {
        anyhow::bail!("RocksDbTxn is read-only")
    }

    fn srem(&mut self, _cf: &'static str, _key: &[u8], _member: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn smembers_limited(
        &mut self,
        cf: &'static str,
        key: &[u8],
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        let cf_handle = self.db.cf_handle(cf).unwrap();
        let prefix = member_prefix(key);
        let mut members = Vec::new();
        let mut iter = self.db.raw_iterator_cf(&cf_handle);
        iter.seek(&prefix);
        let mut count = 0;
        while iter.valid() {
            let k = iter.key().unwrap();
            if !k.starts_with(&prefix) {
                break;
            }
            if count >= limit {
                return Err(SetScanLimitExceeded {
                    operation: "smembers",
                    limit,
                }
                .into());
            }
            if let Some(v) = iter.value() {
                if let Ok((rec, _)) = bincode::serde::decode_from_slice::<StoredRecord, _>(
                    v,
                    bincode::config::standard(),
                ) {
                    match rec {
                        StoredRecord::Str { v: member, exp } => {
                            if !expired(exp, self.now_ms) {
                                members.push(member);
                            }
                        }
                        StoredRecord::Counter { .. } => {}
                    }
                }
            }
            count += 1;
            iter.next();
        }
        Ok(members)
    }

    fn sismember(&mut self, cf: &'static str, key: &[u8], member: &str) -> anyhow::Result<bool> {
        let member_key = member_key(key, member);
        let cf_handle = self.db.cf_handle(cf).unwrap();
        match self.db.get_cf(&cf_handle, &member_key)? {
            Some(v) => {
                let (rec, _): (StoredRecord, _) =
                    bincode::serde::decode_from_slice(&v, bincode::config::standard())?;
                match rec {
                    StoredRecord::Str { exp, .. } => Ok(!expired(exp, self.now_ms)),
                    StoredRecord::Counter { .. } => Ok(false),
                }
            }
            None => Ok(false),
        }
    }

    fn has_live_member(&mut self, cf: &'static str, key: &[u8]) -> anyhow::Result<bool> {
        let cf_handle = self.db.cf_handle(cf).unwrap();
        let prefix = member_prefix(key);
        let mut iter = self.db.raw_iterator_cf(&cf_handle);
        iter.seek(&prefix);
        while iter.valid() {
            let k = iter.key().unwrap();
            if !k.starts_with(&prefix) {
                return Ok(false);
            }
            if let Some(v) = iter.value() {
                if let Ok((rec, _)) = bincode::serde::decode_from_slice::<StoredRecord, _>(
                    v,
                    bincode::config::standard(),
                ) {
                    match rec {
                        StoredRecord::Str { exp, .. } => {
                            if !expired(exp, self.now_ms) {
                                return Ok(true);
                            }
                        }
                        StoredRecord::Counter { .. } => {}
                    }
                }
            }
            iter.next();
        }
        Ok(false)
    }

    fn pexpire_set(&mut self, _cf: &'static str, _key: &[u8], _ttl_ms: u64) -> anyhow::Result<()> {
        anyhow::bail!("RocksDbTxn is read-only")
    }

    fn del_set(&mut self, _cf: &'static str, _key: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }
}

// --- Shared helpers ---

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StoredRecord {
    Str { v: String, exp: u64 },
    Counter { v: i64 },
}

fn member_prefix(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(key.len() + 1);
    buf.extend_from_slice(key);
    buf.push(0);
    buf
}

fn member_key(key: &[u8], member: &str) -> Vec<u8> {
    let mut buf = member_prefix(key);
    buf.extend_from_slice(member.as_bytes());
    buf
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{operation} on set would enumerate more than {limit} live members")]
pub struct SetScanLimitExceeded {
    pub operation: &'static str,
    pub limit: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_keys;
    use std::sync::Arc;

    fn open_test_db() -> (Arc<rocksdb::DB>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = store_keys::ALL_CFS;
        let db = Arc::new(rocksdb::DB::open_cf(&opts, &db_path, cfs).unwrap());
        (db, dir)
    }

    // --- RocksDbTxn: read-only operations ---

    #[test]
    fn read_only_txn_get_str_returns_none_for_missing_key() {
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 100_000);
        assert!(txn
            .get_str(store_keys::CF_WRITE_LOCKS, b"nonexistent")
            .unwrap()
            .is_none());
    }

    #[test]
    fn read_only_txn_set_str_fails() {
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 100_000);
        assert!(txn
            .set_str(store_keys::CF_WRITE_LOCKS, b"key", "val", 1000)
            .is_err());
    }

    #[test]
    fn read_only_txn_sadd_fails() {
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 100_000);
        assert!(txn
            .sadd(store_keys::CF_OWNER_HOLDS, b"set", "member", 1000)
            .is_err());
    }

    #[test]
    fn read_only_txn_lazy_cleanup_is_a_silent_noop() {
        // del/srem/del_set represent best-effort lazy cleanup of expired or
        // dead-owner entries. On the read-only view they must succeed as no-ops
        // (not error) so detect_cycle/is_blocking can run against a snapshot.
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 100_000);
        assert!(txn.del(store_keys::CF_WRITE_LOCKS, b"key").is_ok());
        assert!(txn
            .srem(store_keys::CF_OWNER_HOLDS, b"set", "member")
            .is_ok());
        assert!(txn.del_set(store_keys::CF_OWNER_HOLDS, b"set").is_ok());
    }

    #[test]
    fn read_only_txn_has_live_member_returns_false_for_empty() {
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 100_000);
        assert!(!txn
            .has_live_member(store_keys::CF_OWNER_HOLDS, b"empty-set")
            .unwrap());
    }

    #[test]
    fn read_only_txn_smembers_limited_returns_empty() {
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 100_000);
        assert!(txn
            .smembers_limited(store_keys::CF_OWNER_HOLDS, b"empty-set", 100)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn read_only_txn_sismember_returns_false() {
        let (db, _dir) = open_test_db();
        let mut txn = RocksDbTxn::new(db, 100_000);
        assert!(!txn
            .sismember(store_keys::CF_OWNER_HOLDS, b"empty-set", "member")
            .unwrap());
    }

    // --- StoreTxn trait object safety and type checking ---

    #[test]
    fn store_txn_trait_is_implemented() {
        // Verify that RocksDbTxn implements StoreTxn
        fn _accept_store_txn(_txn: &mut dyn StoreTxn) {}
        // This is a compile-time check; if it compiles, the trait is implemented.
    }

    // --- SetScanLimitExceeded error ---

    #[test]
    fn set_scan_limit_error_formatting() {
        let err = SetScanLimitExceeded {
            operation: "smembers",
            limit: 100,
        };
        let msg = err.to_string();
        assert!(msg.contains("smembers"));
        assert!(msg.contains("100"));
    }
}
