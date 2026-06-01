// A nested TxnNotFound is the same transient condition as the
// top-level `Error::TxnNotFound` arm above: the referenced txn's
// primary lock was already resolved / TTL-expired, so a retry on a
// fresh snapshot clears it. TiKV surfaces it this way from
// commit/lock-resolve (often inside `MultipleKeyErrors`).