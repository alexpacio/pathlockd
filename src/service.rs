//! gRPC service implementation: maps the protobuf surface onto the engine and
//! publishes release/kill/revoke events at exactly the points the engine
//! mutates ownership.

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use futures::StreamExt;
use tikv_client::TransactionClient;
use tonic::{Request, Response, Status};

use crate::engine::{
    self, AcquireArgs, AcquireOutcome, AssertOutcome, CycleOutcome, LockReq, RelReq, RenewOutcome,
};
use crate::events::Broadcaster;
use crate::proto::{
    self, path_lock_debug_server::PathLockDebug, path_lock_server::PathLock, AcquireRequest,
    AcquireResponse, AcquireStatus, AssertFencingRequest, AssertFencingResponse, AssertStatus,
    ClearWaitEdgeRequest, ClearWaitEdgeResponse, CycleKind, DebugAck, DeleteLockKeyRequest,
    DetectCycleRequest, DetectCycleResponse, Event, ExpireOwnerRequest, FlushRequest,
    FlushResponse, ForceReleaseRequest, ForceReleaseResponse, GetFenceRequest, GetFenceResponse,
    GetFencingCounterRequest, GetFencingCounterResponse, GetWriteOwnerRequest,
    GetWriteOwnerResponse, HealthRequest, HealthResponse, IncrFencingTokenRequest,
    IncrFencingTokenResponse, IsBlockingRequest, IsBlockingResponse, IsOwnerAliveRequest,
    IsOwnerAliveResponse, OwnedPathsRequest, OwnedPathsResponse, PublishEventRequest,
    PublishEventResponse, ReleaseAllRequest, ReleaseLocksRequest, ReleaseResponse, RenewRequest,
    RenewResponse, RenewStatus, RequestRevokeRequest, RequestRevokeResponse, SetFenceRequest,
    SetFencingCounterRequest, SetWaitEdgeRequest, SetWaitEdgeResponse, SetWriteOwnerRequest,
    SubscribeRequest,
};

fn internal<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(e.to_string())
}

/// Map an engine error to a gRPC status. A transient TiKV error that survived
/// the bounded retry budget becomes `Unavailable` so the client backs off and
/// retries; anything else is a genuine internal fault — logged in full, but
/// reported to the client without the internal detail.
fn engine_err(e: anyhow::Error) -> Status {
    if crate::store::is_retryable(&e) {
        Status::unavailable("storage temporarily unavailable (contention/region churn); retry")
    } else {
        tracing::error!(error = %e, "internal error serving request");
        Status::internal("internal error")
    }
}

// --- request validation (defensive backstop; clients are expected to send
// already-normalized paths and sane leases) ---

/// Upper bound on a lease TTL. Leases are normally seconds to minutes; this just
/// guards against an absurd value (and a `0` that would mean "never expires").
const MAX_TTL_MS: u64 = 7 * 86_400_000; // 7 days
const MAX_ID_LEN: usize = 1024;
const MAX_PATH_LEN: usize = 4096;
const MAX_PATHS_PER_REQUEST: usize = 1024;
/// A preemption claim is just a short bridge from revoke publication to winner
/// acquire. Keep the upper bound tight so a bad caller cannot reserve a subtree
/// for a whole lease window.
const MAX_CLAIM_TTL_MS: u64 = 60_000;
/// Hard cap on a deadlock-detection walk so a client can't request an unbounded
/// scan. Each step is several sequential TiKV round-trips inside one advisory
/// transaction, so a high cap would let a single request pin a worker and age
/// its snapshot for seconds while real wait-chains are short. Hitting the cap
/// returns `Truncated` and the client's recheck simply re-walks, so this bounds
/// one pass without affecting correctness. `DetectCycle.max_depth` is clamped to
/// this rather than rejected.
const MAX_CYCLE_DEPTH: u32 = 64;

#[allow(clippy::result_large_err)]
fn check_id(label: &str, id: &str) -> Result<(), Status> {
    if id.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{label} must not be empty"
        )));
    }
    if id.len() > MAX_ID_LEN {
        return Err(Status::invalid_argument(format!(
            "{label} too long (max {MAX_ID_LEN} bytes)"
        )));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn check_ttl(ttl_ms: u64) -> Result<(), Status> {
    if ttl_ms == 0 {
        return Err(Status::invalid_argument(
            "ttl_ms must be > 0 (a 0 TTL would create a lock that never expires)",
        ));
    }
    if ttl_ms > MAX_TTL_MS {
        return Err(Status::invalid_argument(format!(
            "ttl_ms too large (max {MAX_TTL_MS} ms)"
        )));
    }
    Ok(())
}

/// Validate a path form `"<handler>:<normalizedPath>"`. Rejects the shapes that
/// would silently break containment (no handler, non-rooted path, `//`, `.`/`..`
/// segments, trailing slash on a non-root path) so a malformed path fails fast
/// instead of locking a node that conflicts with nothing.
#[allow(clippy::result_large_err)]
fn check_path(path: &str) -> Result<(), Status> {
    if path.is_empty() || path.len() > MAX_PATH_LEN {
        return Err(Status::invalid_argument("path empty or too long"));
    }
    let colon = path.find(':').ok_or_else(|| {
        Status::invalid_argument(format!(
            "path must be \"<handler>:<normalizedPath>\": {path}"
        ))
    })?;
    let handler = &path[..colon];
    let p = &path[colon + 1..];
    if handler.is_empty() || handler.contains('/') {
        return Err(Status::invalid_argument(format!(
            "path has an empty or invalid handler: {path}"
        )));
    }
    if !p.starts_with('/') {
        return Err(Status::invalid_argument(format!(
            "normalized path must start with '/': {path}"
        )));
    }
    if p == "/" {
        return Ok(()); // root
    }
    if p.ends_with('/') {
        return Err(Status::invalid_argument(format!(
            "normalized path must not end with '/': {path}"
        )));
    }
    for seg in p[1..].split('/') {
        if seg.is_empty() {
            return Err(Status::invalid_argument(format!(
                "normalized path has an empty segment ('//'): {path}"
            )));
        }
        if seg == "." || seg == ".." {
            return Err(Status::invalid_argument(format!(
                "normalized path has a '.'/'..' segment: {path}"
            )));
        }
    }
    Ok(())
}

/// The conflict reasons `IsBlocking` knows how to re-check — the lock-held
/// reasons a waiter blocks on. `is_blocking_inner` reads the two `*read_locked`
/// reasons as a read re-check, `preempt_claimed` as a claim re-check, and the
/// rest as a write re-check, so an unrecognized value would silently fall
/// through to the write path; reject it.
const BLOCKING_REASONS: [&str; 6] = [
    "ancestor_locked",
    "write_locked",
    "read_locked",
    "descendant_write_locked",
    "descendant_read_locked",
    engine::REASON_PREEMPT_CLAIMED,
];

#[allow(clippy::result_large_err)]
fn check_blocking_reason(reason: &str) -> Result<(), Status> {
    if BLOCKING_REASONS.contains(&reason) {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "unknown is_blocking reason {reason:?} (expected one of {BLOCKING_REASONS:?})"
        )))
    }
}

#[allow(clippy::result_large_err)]
fn check_write_fencing_token(fencing_token: i64) -> Result<(), Status> {
    if fencing_token <= 0 {
        return Err(Status::invalid_argument(
            "fencing_token must be > 0 for write locks",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn check_event(ev: &Event) -> Result<(), Status> {
    check_id("event.owner_id", &ev.owner_id)?;
    match proto::EventType::try_from(ev.r#type) {
        Ok(proto::EventType::Released | proto::EventType::Killed | proto::EventType::Revoke) => {
            Ok(())
        }
        Err(_) => Err(Status::invalid_argument(format!(
            "invalid event type value {}",
            ev.r#type
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn to_mode(i: i32) -> Result<engine::Mode, Status> {
    match proto::Mode::try_from(i) {
        Ok(proto::Mode::Read) => Ok(engine::Mode::Read),
        Ok(proto::Mode::Write) => Ok(engine::Mode::Write),
        Err(_) => Err(Status::invalid_argument(format!("invalid mode value {i}"))),
    }
}

#[allow(clippy::result_large_err)]
fn to_state(i: i32) -> Result<engine::State, Status> {
    match proto::LockState::try_from(i) {
        Ok(proto::LockState::Held) => Ok(engine::State::Held),
        Ok(proto::LockState::New) => Ok(engine::State::New),
        Err(_) => Err(Status::invalid_argument(format!(
            "invalid lock state value {i}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// PathLock service
// ---------------------------------------------------------------------------

pub struct PathLockService {
    pub client: Arc<TransactionClient>,
    pub broadcaster: Broadcaster,
}

impl PathLockService {
    pub fn new(client: Arc<TransactionClient>, broadcaster: Broadcaster) -> Self {
        Self {
            client,
            broadcaster,
        }
    }
}

type EventStream = Pin<Box<dyn Stream<Item = Result<Event, Status>> + Send>>;
const PATH_LOCK_SERVICE: &str = "pathlockd.v1.PathLock";
const PATH_LOCK_DEBUG_SERVICE: &str = "pathlockd.v1.PathLockDebug";

#[tonic::async_trait]
impl PathLock for PathLockService {
    async fn acquire(
        &self,
        request: Request<AcquireRequest>,
    ) -> Result<Response<AcquireResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "Acquire",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                check_ttl(req.ttl_ms)?;
                if req.requests.len() + req.release_requests.len() > MAX_PATHS_PER_REQUEST {
                    return Err(Status::invalid_argument(format!(
                        "too many paths in one request (max {MAX_PATHS_PER_REQUEST})"
                    )));
                }
                for r in &req.requests {
                    check_path(&r.path)?;
                }
                for r in &req.release_requests {
                    check_path(&r.path)?;
                }
                if req
                    .requests
                    .iter()
                    .any(|r| to_mode(r.mode).is_ok_and(|mode| mode == engine::Mode::Write))
                {
                    check_write_fencing_token(req.fencing_token)?;
                }
                let requests: Vec<LockReq> = req
                    .requests
                    .iter()
                    .map(|r| {
                        Ok(LockReq {
                            path: r.path.clone(),
                            mode: to_mode(r.mode)?,
                            state: to_state(r.state)?,
                        })
                    })
                    .collect::<Result<_, Status>>()?;
                let release_requests: Vec<RelReq> = req
                    .release_requests
                    .iter()
                    .map(|r| {
                        Ok(RelReq {
                            path: r.path.clone(),
                            mode: to_mode(r.mode)?,
                        })
                    })
                    .collect::<Result<_, Status>>()?;
                let had_release = !release_requests.is_empty();

                let args = AcquireArgs {
                    owner_id: req.owner_id.clone(),
                    ttl_ms: req.ttl_ms,
                    requests,
                    fencing_token: req.fencing_token,
                    release_requests,
                };

                let outcome = engine::acquire(&self.client, args)
                    .await
                    .map_err(engine_err)?;
                let resp = match outcome {
                    AcquireOutcome::Ok => {
                        // RELEASED is published only when an inline release actually ran and
                        // the caller asked for it.
                        if had_release && req.emit_release {
                            self.broadcaster.released(&req.owner_id);
                        }
                        AcquireResponse {
                            status: AcquireStatus::Ok as i32,
                            ..Default::default()
                        }
                    }
                    AcquireOutcome::Conflict {
                        path,
                        owner,
                        reason,
                    } => AcquireResponse {
                        status: AcquireStatus::Conflict as i32,
                        path,
                        owner,
                        reason,
                    },
                    AcquireOutcome::Lost { path, reason } => AcquireResponse {
                        status: AcquireStatus::Lost as i32,
                        path,
                        owner: String::new(),
                        reason,
                    },
                };
                Ok(Response::new(resp))
            },
        )
        .await
    }

    async fn release(
        &self,
        request: Request<ReleaseLocksRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "Release",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                if req.requests.len() > MAX_PATHS_PER_REQUEST {
                    return Err(Status::invalid_argument(format!(
                        "too many paths in one request (max {MAX_PATHS_PER_REQUEST})"
                    )));
                }
                for r in &req.requests {
                    check_path(&r.path)?;
                }
                let reqs: Vec<RelReq> = req
                    .requests
                    .iter()
                    .map(|r| {
                        Ok(RelReq {
                            path: r.path.clone(),
                            mode: to_mode(r.mode)?,
                        })
                    })
                    .collect::<Result<_, Status>>()?;
                engine::release(&self.client, &req.owner_id, &reqs, req.del_wait_key)
                    .await
                    .map_err(engine_err)?;
                // Release always publishes RELEASED for the owner.
                self.broadcaster.released(&req.owner_id);
                Ok(Response::new(ReleaseResponse {}))
            },
        )
        .await
    }

    async fn release_all(
        &self,
        request: Request<ReleaseAllRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "ReleaseAll",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                engine::release_all(&self.client, &req.owner_id, req.del_wait_key)
                    .await
                    .map_err(engine_err)?;
                self.broadcaster.released(&req.owner_id);
                Ok(Response::new(ReleaseResponse {}))
            },
        )
        .await
    }

    async fn renew(
        &self,
        request: Request<RenewRequest>,
    ) -> Result<Response<RenewResponse>, Status> {
        crate::otel::observe_rpc(PATH_LOCK_SERVICE, "Renew", request, |request| async move {
            let req = request.into_inner();
            check_id("owner_id", &req.owner_id)?;
            check_ttl(req.ttl_ms)?;
            let outcome = engine::renew(&self.client, &req.owner_id, req.ttl_ms)
                .await
                .map_err(engine_err)?;
            let resp = match outcome {
                RenewOutcome::Ok => RenewResponse {
                    status: RenewStatus::Ok as i32,
                    ..Default::default()
                },
                RenewOutcome::Lost { path, reason } => RenewResponse {
                    status: RenewStatus::Lost as i32,
                    path,
                    reason,
                },
            };
            Ok(Response::new(resp))
        })
        .await
    }

    async fn force_release(
        &self,
        request: Request<ForceReleaseRequest>,
    ) -> Result<Response<ForceReleaseResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "ForceRelease",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("victim_id", &req.victim_id)?;
                engine::force_release(&self.client, &req.victim_id)
                    .await
                    .map_err(engine_err)?;
                self.broadcaster.killed(&req.victim_id);
                Ok(Response::new(ForceReleaseResponse {}))
            },
        )
        .await
    }

    async fn assert_fencing(
        &self,
        request: Request<AssertFencingRequest>,
    ) -> Result<Response<AssertFencingResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "AssertFencing",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                if req.paths.len() > MAX_PATHS_PER_REQUEST {
                    return Err(Status::invalid_argument(format!(
                        "too many paths in one request (max {MAX_PATHS_PER_REQUEST})"
                    )));
                }
                for p in &req.paths {
                    check_path(p)?;
                }
                if !req.paths.is_empty() {
                    check_write_fencing_token(req.fencing_token)?;
                }
                let outcome = engine::assert_fencing(
                    &self.client,
                    &req.owner_id,
                    req.fencing_token,
                    &req.paths,
                )
                .await
                .map_err(engine_err)?;
                let resp = match outcome {
                    AssertOutcome::Ok => AssertFencingResponse {
                        status: AssertStatus::Ok as i32,
                        ..Default::default()
                    },
                    AssertOutcome::Fail { path, reason } => AssertFencingResponse {
                        status: AssertStatus::Fail as i32,
                        path,
                        reason,
                    },
                };
                Ok(Response::new(resp))
            },
        )
        .await
    }

    async fn detect_cycle(
        &self,
        request: Request<DetectCycleRequest>,
    ) -> Result<Response<DetectCycleResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "DetectCycle",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("start_owner_id", &req.start_owner_id)?;
                let depth = req.max_depth.min(MAX_CYCLE_DEPTH);
                let outcome = engine::detect_cycle(&self.client, &req.start_owner_id, depth)
                    .await
                    .map_err(engine_err)?;
                let resp = match outcome {
                    CycleOutcome::None => DetectCycleResponse {
                        kind: CycleKind::None as i32,
                        chain: vec![],
                    },
                    CycleOutcome::Cycle(chain) => DetectCycleResponse {
                        kind: CycleKind::Found as i32,
                        chain,
                    },
                    CycleOutcome::Truncated(chain) => DetectCycleResponse {
                        kind: CycleKind::Truncated as i32,
                        chain,
                    },
                };
                Ok(Response::new(resp))
            },
        )
        .await
    }

    async fn is_blocking(
        &self,
        request: Request<IsBlockingRequest>,
    ) -> Result<Response<IsBlockingResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "IsBlocking",
            request,
            |request| async move {
                let req = request.into_inner();
                check_path(&req.conflict_path)?;
                check_id("conflict_owner", &req.conflict_owner)?;
                check_blocking_reason(&req.reason)?;
                let blocking = engine::is_blocking(
                    &self.client,
                    &req.conflict_path,
                    &req.conflict_owner,
                    &req.reason,
                )
                .await
                .map_err(engine_err)?;
                Ok(Response::new(IsBlockingResponse { blocking }))
            },
        )
        .await
    }

    async fn incr_fencing_token(
        &self,
        request: Request<IncrFencingTokenRequest>,
    ) -> Result<Response<IncrFencingTokenResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "IncrFencingToken",
            request,
            |_request| async move {
                let token = engine::incr_fencing_token(&self.client)
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(IncrFencingTokenResponse { token }))
            },
        )
        .await
    }

    async fn set_wait_edge(
        &self,
        request: Request<SetWaitEdgeRequest>,
    ) -> Result<Response<SetWaitEdgeResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "SetWaitEdge",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                check_id("conflict_owner", &req.conflict_owner)?;
                check_ttl(req.ttl_ms)?;
                let metadata = if req.conflict_path.is_empty() && req.reason.is_empty() {
                    None
                } else if req.conflict_path.is_empty() || req.reason.is_empty() {
                    return Err(Status::invalid_argument(
                        "conflict_path and reason must be provided together",
                    ));
                } else {
                    check_path(&req.conflict_path)?;
                    check_blocking_reason(&req.reason)?;
                    Some(engine::WaitEdgeMetadata {
                        conflict_path: req.conflict_path,
                        reason: req.reason,
                    })
                };
                engine::set_wait_edge(
                    &self.client,
                    &req.owner_id,
                    &req.conflict_owner,
                    req.ttl_ms,
                    metadata.as_ref(),
                )
                .await
                .map_err(engine_err)?;
                Ok(Response::new(SetWaitEdgeResponse {}))
            },
        )
        .await
    }

    async fn clear_wait_edge(
        &self,
        request: Request<ClearWaitEdgeRequest>,
    ) -> Result<Response<ClearWaitEdgeResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "ClearWaitEdge",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                engine::clear_wait_edge(&self.client, &req.owner_id)
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(ClearWaitEdgeResponse {}))
            },
        )
        .await
    }

    async fn is_owner_alive(
        &self,
        request: Request<IsOwnerAliveRequest>,
    ) -> Result<Response<IsOwnerAliveResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "IsOwnerAlive",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                let alive = engine::is_owner_alive(&self.client, &req.owner_id)
                    .await
                    .map_err(engine_err)?;
                Ok(Response::new(IsOwnerAliveResponse { alive }))
            },
        )
        .await
    }

    async fn request_revoke(
        &self,
        request: Request<RequestRevokeRequest>,
    ) -> Result<Response<RequestRevokeResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "RequestRevoke",
            request,
            |request| async move {
                let req = request.into_inner();
                check_id("owner_id", &req.owner_id)?;
                // Plant the preemption claim (if requested) BEFORE publishing the
                // revoke, so the claim is durably visible by the time the victim reacts
                // and its manager tries to re-acquire. A failure here is non-fatal: the
                // revoke itself (plus client-side backoff) still resolves the deadlock,
                // just without the extra race protection.
                let wants_claim = !req.claim_path.is_empty() || !req.claimant_owner_id.is_empty();
                if wants_claim {
                    if req.claim_path.is_empty() || req.claimant_owner_id.is_empty() {
                        return Err(Status::invalid_argument(
                            "claim_path and claimant_owner_id must be provided together",
                        ));
                    }
                    check_path(&req.claim_path)?;
                    check_id("claimant_owner_id", &req.claimant_owner_id)?;
                    if req.claim_ttl_ms > MAX_CLAIM_TTL_MS {
                        return Err(Status::invalid_argument(format!(
                    "claim_ttl_ms too large (max {MAX_CLAIM_TTL_MS} ms; use 0 for the default)"
                )));
                    }
                    if let Err(e) = engine::set_claim(
                        &self.client,
                        &req.claim_path,
                        &req.claimant_owner_id,
                        req.claim_ttl_ms,
                    )
                    .await
                    {
                        tracing::warn!(
                            owner_id = %req.owner_id,
                            claim_path = %req.claim_path,
                            claimant = %req.claimant_owner_id,
                            error = %e,
                            "fslock: failed to plant preemption claim; proceeding with revoke only"
                        );
                    }
                }
                self.broadcaster.revoke(&req.owner_id);
                Ok(Response::new(RequestRevokeResponse {}))
            },
        )
        .await
    }

    type SubscribeStream = EventStream;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "Subscribe",
            request,
            |request| async move {
                // A subscription is bound to one owner id and receives only that owner's
                // events — the registry routes by owner id, so this stream is woken only
                // for its own events, never the whole instance's traffic.
                let owner = request.into_inner().owner_id;
                check_id("owner_id", &owner)?;
                let stream: Self::SubscribeStream =
                    Box::pin(self.broadcaster.subscribe(&owner).map(Ok::<Event, Status>));
                Ok(Response::new(stream))
            },
        )
        .await
    }

    async fn publish_event(
        &self,
        request: Request<PublishEventRequest>,
    ) -> Result<Response<PublishEventResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "PublishEvent",
            request,
            |request| async move {
                let ev = request
                    .into_inner()
                    .event
                    .ok_or_else(|| Status::invalid_argument("event is required"))?;
                check_event(&ev)?;
                self.broadcaster.publish_from_peer(ev);
                Ok(Response::new(PublishEventResponse {}))
            },
        )
        .await
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_SERVICE,
            "Health",
            request,
            |_request| async move {
                // Readiness: confirm we can open and close a TiKV transaction.
                let resp = match self.client.begin_optimistic().await {
                    Ok(mut txn) => {
                        let _ = txn.rollback().await;
                        HealthResponse {
                            ok: true,
                            detail: "ready".into(),
                        }
                    }
                    Err(e) => HealthResponse {
                        ok: false,
                        detail: format!("tikv unreachable: {e}"),
                    },
                };
                Ok(Response::new(resp))
            },
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// PathLockDebug service
// ---------------------------------------------------------------------------

pub struct DebugService {
    pub client: Arc<TransactionClient>,
    pub enabled: bool,
}

impl DebugService {
    pub fn new(client: Arc<TransactionClient>, enabled: bool) -> Self {
        Self { client, enabled }
    }

    #[allow(clippy::result_large_err)]
    fn guard(&self) -> Result<(), Status> {
        if self.enabled {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "debug service disabled (set PATHLOCKD_ENABLE_DEBUG=1)",
            ))
        }
    }
}

#[tonic::async_trait]
impl PathLockDebug for DebugService {
    async fn flush(&self, r: Request<FlushRequest>) -> Result<Response<FlushResponse>, Status> {
        crate::otel::observe_rpc(PATH_LOCK_DEBUG_SERVICE, "Flush", r, |_r| async move {
            self.guard()?;
            let deleted = crate::store::flush_all(&self.client)
                .await
                .map_err(internal)?;
            Ok(Response::new(FlushResponse { deleted }))
        })
        .await
    }

    async fn expire_owner(
        &self,
        r: Request<ExpireOwnerRequest>,
    ) -> Result<Response<DebugAck>, Status> {
        crate::otel::observe_rpc(PATH_LOCK_DEBUG_SERVICE, "ExpireOwner", r, |r| async move {
            self.guard()?;
            engine::debug_expire_owner(&self.client, &r.into_inner().owner_id)
                .await
                .map_err(internal)?;
            Ok(Response::new(DebugAck {}))
        })
        .await
    }

    async fn delete_lock_key(
        &self,
        r: Request<DeleteLockKeyRequest>,
    ) -> Result<Response<DebugAck>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_DEBUG_SERVICE,
            "DeleteLockKey",
            r,
            |r| async move {
                self.guard()?;
                let req = r.into_inner();
                let owner = if req.owner_id.is_empty() {
                    None
                } else {
                    Some(req.owner_id)
                };
                engine::debug_delete_lock_key(&self.client, &req.path, to_mode(req.mode)?, owner)
                    .await
                    .map_err(internal)?;
                Ok(Response::new(DebugAck {}))
            },
        )
        .await
    }

    async fn set_write_owner(
        &self,
        r: Request<SetWriteOwnerRequest>,
    ) -> Result<Response<DebugAck>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_DEBUG_SERVICE,
            "SetWriteOwner",
            r,
            |r| async move {
                self.guard()?;
                let req = r.into_inner();
                engine::debug_set_write_owner(&self.client, &req.path, &req.owner_id)
                    .await
                    .map_err(internal)?;
                Ok(Response::new(DebugAck {}))
            },
        )
        .await
    }

    async fn get_write_owner(
        &self,
        r: Request<GetWriteOwnerRequest>,
    ) -> Result<Response<GetWriteOwnerResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_DEBUG_SERVICE,
            "GetWriteOwner",
            r,
            |r| async move {
                self.guard()?;
                let owner = engine::debug_get_write_owner(&self.client, &r.into_inner().path)
                    .await
                    .map_err(internal)?;
                Ok(Response::new(GetWriteOwnerResponse {
                    exists: owner.is_some(),
                    owner_id: owner.unwrap_or_default(),
                }))
            },
        )
        .await
    }

    async fn set_fence(&self, r: Request<SetFenceRequest>) -> Result<Response<DebugAck>, Status> {
        crate::otel::observe_rpc(PATH_LOCK_DEBUG_SERVICE, "SetFence", r, |r| async move {
            self.guard()?;
            let req = r.into_inner();
            engine::debug_set_fence(&self.client, &req.path, req.value)
                .await
                .map_err(internal)?;
            Ok(Response::new(DebugAck {}))
        })
        .await
    }

    async fn get_fence(
        &self,
        r: Request<GetFenceRequest>,
    ) -> Result<Response<GetFenceResponse>, Status> {
        crate::otel::observe_rpc(PATH_LOCK_DEBUG_SERVICE, "GetFence", r, |r| async move {
            self.guard()?;
            let value = engine::debug_get_fence(&self.client, &r.into_inner().path)
                .await
                .map_err(internal)?;
            Ok(Response::new(GetFenceResponse {
                exists: value.is_some(),
                value: value.unwrap_or(0),
            }))
        })
        .await
    }

    async fn set_fencing_counter(
        &self,
        r: Request<SetFencingCounterRequest>,
    ) -> Result<Response<DebugAck>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_DEBUG_SERVICE,
            "SetFencingCounter",
            r,
            |r| async move {
                self.guard()?;
                engine::debug_set_fencing_counter(&self.client, r.into_inner().value)
                    .await
                    .map_err(internal)?;
                Ok(Response::new(DebugAck {}))
            },
        )
        .await
    }

    async fn get_fencing_counter(
        &self,
        r: Request<GetFencingCounterRequest>,
    ) -> Result<Response<GetFencingCounterResponse>, Status> {
        crate::otel::observe_rpc(
            PATH_LOCK_DEBUG_SERVICE,
            "GetFencingCounter",
            r,
            |_r| async move {
                self.guard()?;
                let value = engine::debug_get_fencing_counter(&self.client)
                    .await
                    .map_err(internal)?;
                Ok(Response::new(GetFencingCounterResponse { value }))
            },
        )
        .await
    }

    async fn owned_paths(
        &self,
        r: Request<OwnedPathsRequest>,
    ) -> Result<Response<OwnedPathsResponse>, Status> {
        crate::otel::observe_rpc(PATH_LOCK_DEBUG_SERVICE, "OwnedPaths", r, |r| async move {
            self.guard()?;
            let (members, alive) =
                engine::debug_owned_paths(&self.client, &r.into_inner().owner_id)
                    .await
                    .map_err(internal)?;
            Ok(Response::new(OwnedPathsResponse { members, alive }))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_invalid(r: Result<(), Status>) -> bool {
        matches!(r, Err(ref e) if e.code() == tonic::Code::InvalidArgument)
    }

    #[test]
    fn check_ttl_rejects_zero_and_huge() {
        assert!(is_invalid(check_ttl(0))); // 0 = never expires
        assert!(is_invalid(check_ttl(MAX_TTL_MS + 1)));
        assert!(check_ttl(1).is_ok());
        assert!(check_ttl(10_000).is_ok());
        assert!(check_ttl(MAX_TTL_MS).is_ok());
    }

    #[test]
    fn check_id_rejects_empty_and_overlong() {
        assert!(is_invalid(check_id("owner_id", "")));
        assert!(is_invalid(check_id(
            "owner_id",
            &"x".repeat(MAX_ID_LEN + 1)
        )));
        assert!(check_id("owner_id", "owner-42").is_ok());
    }

    #[test]
    fn check_path_accepts_normalized_forms() {
        assert!(check_path("h:/").is_ok()); // root
        assert!(check_path("h:/a").is_ok());
        assert!(check_path("google_drive:/a/b/c").is_ok());
    }

    #[test]
    fn check_path_rejects_unsafe_shapes() {
        assert!(is_invalid(check_path(""))); // empty
        assert!(is_invalid(check_path("noseparator"))); // no handler ':'
        assert!(is_invalid(check_path(":/x"))); // empty handler
        assert!(is_invalid(check_path("h:relative"))); // not rooted
        assert!(is_invalid(check_path("h:/a/"))); // trailing slash (non-root)
        assert!(is_invalid(check_path("h:/a//b"))); // empty segment
        assert!(is_invalid(check_path("h:/a/../b"))); // dot-dot segment
        assert!(is_invalid(check_path("h:/a/./b"))); // dot segment
    }

    #[test]
    fn check_path_distinguishes_trailing_slash() {
        // The footgun this guards: "h:/a" and "h:/a/" used to be distinct,
        // non-conflicting lock nodes. Now the latter is rejected outright.
        assert!(check_path("h:/a").is_ok());
        assert!(is_invalid(check_path("h:/a/")));
    }

    #[test]
    fn check_blocking_reason_accepts_known_rejects_unknown() {
        for r in BLOCKING_REASONS {
            assert!(check_blocking_reason(r).is_ok(), "{r} should be accepted");
        }
        // A real conflict reason that is not a "blocked on a held lock" condition.
        assert!(is_invalid(check_blocking_reason("stale_fencing_token")));
        assert!(is_invalid(check_blocking_reason("")));
        assert!(is_invalid(check_blocking_reason("garbage")));
    }

    #[test]
    fn check_write_fencing_token_rejects_non_positive() {
        assert!(is_invalid(check_write_fencing_token(0)));
        assert!(is_invalid(check_write_fencing_token(-1)));
        assert!(check_write_fencing_token(1).is_ok());
    }

    #[test]
    fn check_event_accepts_known_types_and_owner() {
        for kind in [
            proto::EventType::Released,
            proto::EventType::Killed,
            proto::EventType::Revoke,
        ] {
            assert!(check_event(&Event {
                r#type: kind as i32,
                owner_id: "owner-42".into(),
            })
            .is_ok());
        }
    }

    #[test]
    fn check_event_rejects_empty_owner_and_unknown_type() {
        assert!(is_invalid(check_event(&Event {
            r#type: proto::EventType::Released as i32,
            owner_id: String::new(),
        })));
        assert!(is_invalid(check_event(&Event {
            r#type: 99,
            owner_id: "owner-42".into(),
        })));
    }

    #[test]
    fn enum_decoding_rejects_unknown_values() {
        assert_eq!(
            to_mode(proto::Mode::Write as i32).unwrap(),
            engine::Mode::Write
        );
        assert_eq!(
            to_mode(proto::Mode::Read as i32).unwrap(),
            engine::Mode::Read
        );
        assert!(is_invalid(to_mode(99).map(|_| ())));

        assert_eq!(
            to_state(proto::LockState::New as i32).unwrap(),
            engine::State::New
        );
        assert_eq!(
            to_state(proto::LockState::Held as i32).unwrap(),
            engine::State::Held
        );
        assert!(is_invalid(to_state(99).map(|_| ())));
    }
}
