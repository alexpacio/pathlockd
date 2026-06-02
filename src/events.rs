//! Event fan-out for the per-owner lifecycle stream (release / kill / revoke).
//!
//! Within a single instance, each `Subscribe` stream registers a bounded mpsc
//! sender in a per-owner registry keyed by owner id, and an event is routed only
//! to the senders registered for *its* owner. Routing is one map lookup, so an
//! event reaches just the handful of streams that asked for that owner — cost
//! scales with that owner's subscribers, not the instance-wide subscriber count.
//! This replaces a single global broadcast channel, where every subscription
//! woke for every event instance-wide only to discard the ones addressed to
//! other owners (O(subscribers × events) wakeups, and slow subscribers lagging
//! the shared ring).
//!
//! Across instances, an event is best-effort forwarded to configured peers'
//! `PublishEvent` RPC so an event raised on instance A reaches the owner's
//! subscription on instance B. The client-side recheck timer is the correctness
//! backstop, so a dropped peer message only costs latency, never safety.
//!
//! Peer fan-out uses one long-lived forwarder task per peer draining a bounded
//! queue (not a task per event), so a slow or dead peer can neither pile up
//! tasks nor stall the request path: a full queue simply drops the event, and
//! each forward RPC carries a timeout.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};

use crate::proto::{path_lock_client::PathLockClient, Event, EventType, PublishEventRequest};

/// Per-peer forwarder queue depth. Events are tiny and infrequent; if a peer is
/// slow enough to fill this, we drop (the client-side recheck is the backstop).
const PEER_QUEUE: usize = 1024;
/// Timeout applied to each peer `PublishEvent` RPC (connect and per-call).
const PEER_RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// Hard cap for each subscriber queue. Tokio's bounded mpsc rejects capacities
/// above its semaphore limit; keep config inside a sane operational range before
/// channel construction can panic.
const MAX_SUBSCRIBER_QUEUE: usize = 1_000_000;

#[derive(Clone)]
pub struct Broadcaster {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Arc<Registry>,
    peer_txs: Vec<mpsc::Sender<Event>>,
}

/// Live subscriptions keyed by owner id. An event is delivered by looking up its
/// owner and pushing to that owner's senders only, so delivery cost scales with
/// the subscribers for *that* owner, not the instance-wide subscriber count.
struct Registry {
    subs: Mutex<HashMap<String, Vec<SubSender>>>,
    next_id: AtomicU64,
    /// Per-subscriber queue depth. A subscriber only ever queues its own owner's
    /// events, so this fills only if that one client stalls; an overflow drops
    /// (the client recheck is the correctness backstop). tokio's bounded mpsc
    /// allocates on demand, so a large depth costs memory only when backlogged.
    capacity: usize,
}

/// One subscriber's sender plus the id used to remove exactly it on drop (an
/// owner may hold more than one subscription).
struct SubSender {
    id: u64,
    tx: mpsc::Sender<Event>,
}

impl Registry {
    fn new(capacity: usize) -> anyhow::Result<Arc<Self>> {
        if capacity == 0 {
            anyhow::bail!("event_buffer must be > 0");
        }
        if capacity > MAX_SUBSCRIBER_QUEUE {
            anyhow::bail!("event_buffer too large (max {MAX_SUBSCRIBER_QUEUE})");
        }
        Ok(Arc::new(Self {
            subs: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            capacity,
        }))
    }

    fn lock_subs(&self) -> MutexGuard<'_, HashMap<String, Vec<SubSender>>> {
        self.subs.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("event registry mutex was poisoned; recovering registry state");
            poisoned.into_inner()
        })
    }

    /// Deliver `ev` to the live subscribers for its owner, if any.
    fn route(&self, ev: &Event) {
        let subs = self.lock_subs();
        if let Some(list) = subs.get(&ev.owner_id) {
            for s in list {
                // Non-blocking: never stall the publish path. A full or closed
                // queue drops the event; closed senders are reaped by the owning
                // Subscription's Drop, not here.
                let _ = s.tx.try_send(ev.clone());
            }
        }
    }

    /// Register a new subscription for `owner` and hand back its stream.
    fn register(self: &Arc<Self>, owner: &str) -> Subscription {
        let (tx, rx) = mpsc::channel(self.capacity);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.lock_subs()
            .entry(owner.to_string())
            .or_default()
            .push(SubSender { id, tx });
        Subscription {
            rx,
            registry: Arc::clone(self),
            owner: owner.to_string(),
            id,
        }
    }

    /// Drop one subscription's sender (from `Subscription::drop`), and the owner
    /// entry entirely once its last subscriber leaves, so the map cannot grow
    /// without bound as clients come and go.
    fn unregister(&self, owner: &str, id: u64) {
        let mut subs = self.lock_subs();
        if let Some(list) = subs.get_mut(owner) {
            list.retain(|s| s.id != id);
            if list.is_empty() {
                subs.remove(owner);
            }
        }
    }
}

/// A live per-owner event subscription. Yields the owner's `Event`s; on drop it
/// unregisters itself so a disconnected client leaves no dangling sender behind.
pub struct Subscription {
    rx: mpsc::Receiver<Event>,
    registry: Arc<Registry>,
    owner: String,
    id: u64,
}

impl Stream for Subscription {
    type Item = Event;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Subscription is Unpin (every field is), so poll the receiver directly.
        self.get_mut().rx.poll_recv(cx)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.registry.unregister(&self.owner, self.id);
    }
}

impl Broadcaster {
    pub fn new(capacity: usize, peer_endpoints: &[String]) -> anyhow::Result<Self> {
        let registry = Registry::new(capacity)?;
        let mut peer_txs = Vec::new();
        for ep in peer_endpoints {
            let endpoint = Endpoint::from_shared(ep.clone())
                .map_err(|e| anyhow::anyhow!("invalid peer endpoint {ep}: {e}"))?
                .timeout(PEER_RPC_TIMEOUT)
                .connect_timeout(PEER_RPC_TIMEOUT);
            let channel = endpoint.connect_lazy();
            let (ptx, prx) = mpsc::channel(PEER_QUEUE);
            tokio::spawn(peer_forwarder(channel, prx));
            peer_txs.push(ptx);
        }
        Ok(Self {
            inner: Arc::new(Inner { registry, peer_txs }),
        })
    }

    /// Register a `Subscribe` stream for `owner`; it receives only that owner's
    /// events.
    pub fn subscribe(&self, owner: &str) -> Subscription {
        self.inner.registry.register(owner)
    }

    /// Publish an event that originated on this instance: deliver locally and
    /// enqueue to each peer's forwarder (best-effort, non-blocking).
    pub fn publish_local(&self, ev: Event) {
        self.inner.registry.route(&ev);
        for ptx in &self.inner.peer_txs {
            // try_send: if a peer's queue is full we drop rather than block the
            // request path; the client recheck timer is the correctness backstop.
            let _ = ptx.try_send(ev.clone());
        }
    }

    /// Publish an event forwarded from a peer: deliver locally only (do not
    /// re-forward, which would loop).
    pub fn publish_from_peer(&self, ev: Event) {
        self.inner.registry.route(&ev);
    }

    pub fn released(&self, owner: &str) {
        self.publish_local(Event {
            r#type: EventType::Released as i32,
            owner_id: owner.to_string(),
        });
    }

    pub fn killed(&self, owner: &str) {
        self.publish_local(Event {
            r#type: EventType::Killed as i32,
            owner_id: owner.to_string(),
        });
    }

    pub fn revoke(&self, owner: &str) {
        self.publish_local(Event {
            r#type: EventType::Revoke as i32,
            owner_id: owner.to_string(),
        });
    }
}

/// One long-lived task per peer: drains its bounded queue and forwards each
/// event via `PublishEvent`. The lazy channel reconnects under the hood; a
/// failed or timed-out send is dropped (best-effort). The task ends when the
/// `Broadcaster` (and thus the sender) is dropped.
async fn peer_forwarder(channel: Channel, mut rx: mpsc::Receiver<Event>) {
    let mut client = PathLockClient::new(channel);
    while let Some(ev) = rx.recv().await {
        let owner_id = ev.owner_id.clone();
        let event_type = ev.r#type;
        if let Err(e) = client
            .publish_event(PublishEventRequest { event: Some(ev) })
            .await
        {
            tracing::debug!(
                owner_id = %owner_id,
                event_type,
                error = %e,
                "peer event forward failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broadcaster_rejects_invalid_subscriber_queue_sizes() {
        assert!(Broadcaster::new(0, &[]).is_err());
        assert!(Broadcaster::new(MAX_SUBSCRIBER_QUEUE + 1, &[]).is_err());
        assert!(Broadcaster::new(1, &[]).is_ok());
    }
}
