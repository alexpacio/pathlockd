//! Readiness checks based on the serialized writer's actual ability to
//! accept and apply commands.

use std::time::Duration;

use crate::cluster::router::Router;

/// How long the health probe waits for a no-op command to round-trip the
/// writer queue. A writer wedged behind a RocksDB stall or a runaway command
/// fails this, turning the node not-ready so the orchestrator can act —
/// previously health stayed green while the write path was frozen.
const WRITER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Health status for the local node.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub ready: bool,
    pub detail: String,
}

impl HealthStatus {
    pub fn ready() -> Self {
        Self {
            ready: true,
            detail: "ready".into(),
        }
    }

    pub fn not_ready(reason: impl Into<String>) -> Self {
        Self {
            ready: false,
            detail: reason.into(),
        }
    }
}

/// Check whether the local node is ready to serve: the writer must be
/// unpoisoned and drain a probe command within the timeout.
pub async fn check_ready(router: &Router) -> HealthStatus {
    if !router.writer_healthy() {
        return HealthStatus::not_ready("writer poisoned by WAL sync failure");
    }
    match router.probe_writer(WRITER_PROBE_TIMEOUT).await {
        Ok(()) => HealthStatus::ready(),
        Err(e) => HealthStatus::not_ready(format!("writer probe failed: {e}")),
    }
}
