//! pathlockd — fast, scalable, opinionated path-based distributed locking
//! primitives for developers building user-space virtual filesystems.
//!
//! Durable lock metadata is replicated by embedded Multi-Raft groups and stored
//! locally in RocksDB. Cluster discovery uses SWIM/foca; lock correctness is
//! provided by Raft log order, linearizable reads, TTL leases, and fencing tokens.

pub mod proto {
    tonic::include_proto!("pathlockd.v1");
}

pub mod cluster;
pub mod config;
pub mod engine;
pub mod events;
pub mod otel;
pub mod raft;
pub mod service;
pub mod store_keys;
pub mod store_rocksdb;
